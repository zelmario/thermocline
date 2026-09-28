package router

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// A collation decides which strings are one value. Under en/strength 2,
// Bob@x.com and bob@x.com are one key, and a unique index rejects the second.
//
// The registry derives its id from the value's bytes, and those two spellings
// have different bytes, so it made two keys where mongod makes one. Measured on
// a tiered collection whose archived document held Bob@x.com, the router
// accepted bob@x.com that mongod rejects.
//
// Reproducing the comparison here would mean reimplementing ICU, which is the
// same trap constrains avoids for a partial index's filter. The server is asked
// instead: one indexed lookup against the archive, saying the collation out
// loud, which is the same comparison mongod would have made.
//
// Only an index that carries a collation pays for this, and an index inherits
// its collection's default one, so the spec says so either way.

// collatedArchiveClash reports whether a live archived document other than
// owner already holds this key under idx's collation.
//
// It fails closed the other way from most of this file: if the archive cannot
// be asked, the write is not refused. Rejecting a write because bookkeeping is
// unreachable is a visible failure caused by the proxy, and the case this
// guards is one the fast tier's own index cannot see anyway.
func (r *Router) collatedArchiveClash(ctx context.Context, ns string, idx uniqueIndex, vals []any, owner bson.RawValue) bool {
	if !r.collatedArchiveClashNow(ctx, ns, idx, vals, owner) {
		return false
	}
	// About to refuse, so asked again once mirroring has caught up: a document
	// deleted a moment ago still holds its value in the archive until its
	// tombstone lands. See clashIsFresh.
	r.clashIsFresh(ctx, ns)
	return r.collatedArchiveClashNow(ctx, ns, idx, vals, owner)
}

// collatedArchiveClashNow is collatedArchiveClash asked of the archive as it
// stands.
func (r *Router) collatedArchiveClashNow(ctx context.Context, ns string, idx uniqueIndex, vals []any, owner bson.RawValue) bool {
	if len(idx.collation) == 0 || len(vals) != len(idx.fields) {
		return false
	}
	if !r.archiveCanAnswerCollated(ctx, ns, idx) {
		return false
	}

	filter := collatedClashFilter(idx, vals, owner)

	// More than one, because the archive is behind the fast tier by however
	// long mirroring takes. A document that has just moved off this value still
	// looks like it holds it there, and taking only the first match could hand
	// back that one and hide a real clash sitting behind it. That is the same
	// staleness claimIsLive resolves for a byte key, and the same answer
	// applies: a document the fast tier holds has already been judged by its
	// own index, so only an archive-only one is a cross tier clash.
	//
	// The cap is what one key can honestly be expected to match. Reaching it
	// means the constraint was already broken by more documents than this, and
	// refusing on the first archive-only one among them is right either way.
	db, coll := splitNS(ns)
	cur, err := r.cold.Database(db).Collection(coll).Find(ctx, filter,
		options.Find().
			SetCollation(collationFromSpec(idx.collation)).
			SetProjection(bson.D{{Key: "_id", Value: 1}}).
			SetLimit(collatedCandidates),
	)
	if err != nil {
		r.log.Warn("could not ask the archive about a collated unique key, not enforcing it across tiers",
			"ns", ns, "index", idx.name, "err", err,
			"effect", "a duplicate of an archived document could be inserted, spelled differently")
		return false
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	for cur.Next(ctx) {
		id, lerr := bson.Raw(cur.Current).LookupErr("_id")
		if lerr != nil {
			return true
		}
		// Only a document the fast tier holds with another value is passed
		// over: its archived copy is stale, and the fast tier's own index has
		// the current one. A document the fast tier holds with this value used
		// to be passed over too, as already judged by that index, and the
		// judgement does not last until the write lands: an eviction sweep can
		// take that exact document off the fast tier in between, and then
		// nothing refuses the second spelling. Measured on 2026-09-28 in the
		// two-application shop, with the evictor sweeping 200 orders at a
		// time: "ref003955" was accepted beside an archived "Ref003955" whose
		// fast tier copy the evictor removed that second. Refusing here is the
		// answer mongod gives while the fast tier holds it, and the only
		// correct one once it does not.
		if !r.fastTierMovedOff(ctx, ns, idx, vals, id) {
			return true
		}
	}
	return false
}

// fastTierMovedOff reports whether the fast tier holds this document with a
// value other than vals for the index, which is what makes its archived copy
// stale. A document it does not hold, one it holds with the value, and a
// question that could not be asked are all false.
func (r *Router) fastTierMovedOff(ctx context.Context, ns string, idx uniqueIndex, vals []any, id bson.RawValue) bool {
	if !r.fastTierHolds(ctx, ns, id) {
		return false
	}
	holds := bson.D{{Key: "_id", Value: id}}
	for i, f := range idx.fields {
		holds = append(holds, bson.E{Key: f, Value: vals[i]})
	}
	if scope := indexScope(idx); len(scope) > 0 {
		holds = append(holds, bson.E{Key: "$and", Value: scope})
	}
	db, coll := splitNS(ns)
	err := r.hot.Database(db).Collection(coll).FindOne(ctx, holds,
		options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}}).
			SetCollation(collationFromSpec(idx.collation))).Err()
	return errors.Is(err, mongo.ErrNoDocuments)
}

// collatedClashFilter is what collatedArchiveClash asks the archive: live
// documents holding this key, other than the one that already owns it, among
// the documents the index actually indexes.
//
// A separate function so the last of those three can be checked without an
// archive to ask. A partial or sparse index constrains only some documents, and
// one outside it does not hold the key however plainly it holds the value, in
// any collation. Written without that, a unique index carrying both a collation
// and a partialFilterExpression refused an insert mongod takes: measured on
// {email: 1} unique, en/strength 2, partial on {active: true}, with
// {_id: 1, email: "Bob@x.com", active: false} archived and off the fast tier, an
// insert of {email: "bob@x.com", active: true} came back E11000 from the router
// and was accepted by mongod on an untiered collection carrying the same index.
func collatedClashFilter(idx uniqueIndex, vals []any, owner bson.RawValue) bson.D {
	filter := bson.D{meta.NotArchivedDeleted()}
	for i, f := range idx.fields {
		// A missing field is indexed as null, exactly as keyID encodes it.
		if vals[i] == nil {
			filter = append(filter, bson.E{Key: f, Value: nil})
			continue
		}
		filter = append(filter, bson.E{Key: f, Value: vals[i]})
	}
	if !owner.IsZero() {
		// The document that already holds this value is allowed to keep it.
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$ne", Value: owner}}})
	}
	// The same narrowing the two byte-key lookups do, from the same function.
	if scope := indexScope(idx); len(scope) > 0 {
		filter = append(filter, bson.E{Key: "$and", Value: scope})
	}
	return filter
}

// collatedCandidates bounds how many archived documents one key is compared
// against. See collatedArchiveClash.
const collatedCandidates = 8

// collationFromSpec turns an index's reported collation into one a query can
// carry. The report includes the version of the collator that built the index,
// which is not something a query may ask for.
func collationFromSpec(spec bson.Raw) *options.Collation {
	var c struct {
		Locale          string `bson:"locale"`
		CaseLevel       bool   `bson:"caseLevel"`
		CaseFirst       string `bson:"caseFirst"`
		Strength        int    `bson:"strength"`
		NumericOrdering bool   `bson:"numericOrdering"`
		Alternate       string `bson:"alternate"`
		MaxVariable     string `bson:"maxVariable"`
		Normalization   bool   `bson:"normalization"`
		Backwards       bool   `bson:"backwards"`
	}
	if err := bson.Unmarshal(spec, &c); err != nil {
		return nil
	}
	return &options.Collation{
		Locale:          c.Locale,
		CaseLevel:       c.CaseLevel,
		CaseFirst:       c.CaseFirst,
		Strength:        c.Strength,
		NumericOrdering: c.NumericOrdering,
		Alternate:       c.Alternate,
		MaxVariable:     c.MaxVariable,
		Normalization:   c.Normalization,
		Backwards:       c.Backwards,
	}
}

// archiveCanAnswerCollated reports whether the archive can answer the lookup
// above from an index, and says so once when it cannot.
//
// The lookup is by the index's own fields, and an adopted archive is the
// original cluster, so it usually carries the very index this is about. An
// archive the feeder built has only _id_, because nothing creates an
// application's indexes there. Running the lookup anyway would put a collection
// scan on every insert into that collection, which is a far worse thing to do
// quietly than the under-enforcement it would be fixing.
//
// A query with a collation can only use an index built with the same one, so
// the collation has to match and not merely be present.
func (r *Router) archiveCanAnswerCollated(ctx context.Context, ns string, idx uniqueIndex) bool {
	key := ns + "|" + idx.name
	r.brackets.mu.RLock()
	ans, asked := r.brackets.collated[key]
	r.brackets.mu.RUnlock()
	// Asked again once the answer is old enough. See collatedRecheckNo: an
	// operator who creates the index the warning below asks for has to be able
	// to see it take effect.
	if asked && ans.good() {
		return ans.usable
	}
	usable := false

	db, coll := splitNS(ns)
	cur, err := r.cold.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		// Do not remember a failure to ask as an answer.
		return false
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	want := collationFromSpec(idx.collation)
	for cur.Next(ctx) {
		var spec struct {
			Key       bson.Raw `bson:"key"`
			Collation bson.Raw `bson:"collation"`
		}
		if cur.Decode(&spec) != nil || len(spec.Collation) == 0 {
			continue
		}
		elems, kerr := spec.Key.Elements()
		if kerr != nil || len(elems) == 0 || elems[0].Key() != idx.fields[0] {
			continue
		}
		have := collationFromSpec(spec.Collation)
		if have == nil || want == nil || have.Locale != want.Locale || have.Strength != want.Strength {
			continue
		}
		usable = true
		break
	}

	r.brackets.mu.Lock()
	if r.brackets.collated == nil {
		r.brackets.collated = map[string]collatedAnswer{}
	}
	if r.brackets.collatedWarned == nil {
		r.brackets.collatedWarned = map[string]bool{}
	}
	r.brackets.collated[key] = collatedAnswer{usable: usable, asked: time.Now()}
	// Once per index, not once every time the answer is asked again: a
	// misconfiguration nobody has fixed should not fill the log.
	warn := !usable && !r.brackets.collatedWarned[key]
	if warn {
		r.brackets.collatedWarned[key] = true
	}
	r.brackets.mu.Unlock()

	if warn {
		r.log.Warn("not enforcing a collated unique index across tiers",
			"ns", ns, "index", idx.name,
			"reason", "the value has to be compared in the index's own collation, which means asking the "+
				"archive, and the archive has no index on "+idx.fields[0]+" with that collation",
			"effect", "a duplicate of an archived document can be inserted, spelled differently; asking "+
				"anyway would scan the whole archived collection on every insert",
			"action", "create the same index on the archive, with the same collation")
	}
	return usable
}
