package router

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
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
	if len(idx.collation) == 0 || len(vals) != len(idx.fields) {
		return false
	}
	if !r.archiveCanAnswerCollated(ctx, ns, idx) {
		return false
	}

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
		if !r.fastTierHolds(ctx, ns, id) {
			return true
		}
	}
	return false
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
