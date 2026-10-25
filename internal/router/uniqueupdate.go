package router

import (
	"context"
	"fmt"
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/wire"
)

// Inserts are not the only way a document comes to hold a unique value. An
// update can move one onto a value whose owner has been evicted, and the fast
// tier's unique index cannot see that owner any more than it could for an
// insert. It accepts the write, and the collection ends up with two live
// documents sharing a key: one in the archive, one in the fast tier.
//
// So the same registry that guards inserts guards updates. The difference is
// that the key being claimed belongs to a document that does not exist yet in
// the shape it is about to take, so it has to be worked out first: what the
// matched documents hold now, with the update's own changes laid over the top.
//
// Not every update can be worked out that way. $inc on an indexed field, a
// $rename, an aggregation pipeline update: their result depends on evaluation
// this router does not do, and guessing is not an option when the answer decides
// whether a write is a duplicate. For those the fast tier is asked to apply the
// statement inside a transaction, which is read back and then rolled back, so
// the real value is known before the client's own command is allowed to run.
//
// Only when that fails as well is the write let through with a warning, which is
// the same stance the insert path takes when the registry is unreachable.
// Refusing writes because a bookkeeping collection cannot answer a question is a
// worse failure than the one being prevented.

// maxKeyedUpdate caps how many matched documents one statement will have keys
// computed for. Past this the guard steps aside rather than walk an unbounded
// range on the write path.
const maxKeyedUpdate = 10000

// maxKeyedUpdateBytes is the same cap in bytes. See matchedForStatement.
const maxKeyedUpdateBytes = 256 << 20

// updateStatement is one update, however the client packaged it.
type updateStatement struct {
	filter bson.Raw
	update bson.RawValue
	upsert bool
	multi  bool

	// collation decides what equal means for the filter, so anything that asks
	// the same filter of the other tier has to ask it the same way.
	collation bson.Raw

	// sort is findAndModify's, which decides which document it acts on.
	sort         bson.Raw
	arrayFilters bson.RawValue
}

// updateShape is what a statement will do to the fields of a unique index.
type updateShape struct {
	// replacement is the whole new document, for an update that replaces rather
	// than modifies. Its own fields are the answer, with no existing document
	// needed.
	replacement bson.Raw
	isReplace   bool

	set     map[string]bson.RawValue
	unset   map[string]bool
	touched map[string]bool
	opaque  bool
}

// parseUpdate reads a statement's update document into an updateShape.
func parseUpdate(u bson.RawValue) updateShape {
	sh := updateShape{
		set:     map[string]bson.RawValue{},
		unset:   map[string]bool{},
		touched: map[string]bool{},
	}
	if u.Type == bson.TypeArray {
		// An aggregation pipeline update. The new value of a field is whatever
		// the pipeline computes, which is not something to reimplement here.
		sh.opaque = true
		return sh
	}
	doc, ok := u.DocumentOK()
	if !ok {
		sh.opaque = true
		return sh
	}
	elems, err := doc.Elements()
	if err != nil {
		sh.opaque = true
		return sh
	}

	operators := false
	for _, e := range elems {
		if strings.HasPrefix(e.Key(), "$") {
			operators = true
			break
		}
	}
	if !operators {
		sh.isReplace = true
		sh.replacement = doc
		return sh
	}

	for _, e := range elems {
		sub, sok := e.Value().DocumentOK()
		if !sok {
			sh.opaque = true
			continue
		}
		fields, ferr := sub.Elements()
		if ferr != nil {
			sh.opaque = true
			continue
		}
		for _, f := range fields {
			sh.touched[f.Key()] = true
			switch e.Key() {
			case "$set", "$setOnInsert":
				sh.set[f.Key()] = f.Value()
			case "$unset":
				sh.unset[f.Key()] = true
			case "$rename":
				// Touches the destination as well as the source.
				if dst, dok := f.Value().StringValueOK(); dok {
					sh.touched[dst] = true
				}
			}
		}
	}
	return sh
}

// relates reports whether writing field a changes what field b holds. Equal
// names obviously do, and so do dotted paths where one contains the other.
func relates(a, b string) bool {
	return a == b ||
		strings.HasPrefix(a, b+".") ||
		strings.HasPrefix(b, a+".")
}

// affects reports whether the statement can change idx's key at all, or move
// the document into or out of the set of documents idx covers.
//
// It reads the update document alone, which is the whole answer for a statement
// that modifies a document that exists. It is not the answer for an upsert:
// the document mongod creates inherits the filter's equality fields too, so
// updateOne({email, date}, {$setOnInsert: {name}}, {upsert: true}) writes a key
// the update document never mentions. Callers check an upsert whatever this
// says. Measured on the pilot: that upsert was accepted against a key a live
// archived comment held, the write was acknowledged, the archive refused to
// mirror it with E11000, and a read of the key answered 1 where two documents
// existed.
func (sh updateShape) affects(idx uniqueIndex) bool {
	if sh.isReplace {
		return true
	}
	// A pipeline update does not say which fields it writes, so it has to be
	// assumed to write all of them. Deciding it affects nothing would skip the
	// check entirely, which is how a pipeline could quietly take an archived
	// document's key.
	if sh.opaque {
		return true
	}
	for _, group := range [][]string{idx.fields, idx.partialFields} {
		for _, f := range group {
			for t := range sh.touched {
				if relates(t, f) {
					return true
				}
			}
		}
	}
	return false
}

// determinable reports whether the new key can be worked out without evaluating
// the update the way the server would.
func (sh updateShape) determinable(idx uniqueIndex) bool {
	if sh.isReplace {
		return true
	}
	if sh.opaque {
		return false
	}
	for _, f := range idx.fields {
		for t := range sh.touched {
			if !relates(t, f) {
				continue
			}
			// Only a write to exactly this field, by an operator whose result is
			// the value itself, can be read off the command.
			if t != f {
				return false
			}
			_, isSet := sh.set[t]
			if !isSet && !sh.unset[t] {
				return false
			}
		}
	}
	return true
}

// newKeyValues works out what idx's fields will hold after the statement runs.
// existing is the document being updated, or nil for a document an upsert is
// about to create.
// Values, plural, per field. A field that holds an array puts one entry per
// element into the index, so what a document contributes to one field is a list
// and not a value.
//
// It used to be one value read with lookupPath, and lookupPath cannot cross an
// array: it answers "element not found" for items.sku over an array of
// subdocuments, so every such document was keyed as null here. That agreed with
// the insert path while the insert path was wrong in the same way, and stopped
// agreeing the moment the insert path learned to walk arrays. The two then
// computed different keys for the same document, and an update to one whose
// items.sku the archive has nothing to do with was refused as a duplicate.
//
// Found by the fuzzer at ten rounds of a hundred and twenty operations, on a
// compound partial unique index over items.sku and n. The reference accepted the
// update and the tiered side answered E11000, and every comparison after it
// disagreed because the two collections had parted company.
func (sh updateShape) newKeyValues(idx uniqueIndex, existing bson.Raw, filter bson.Raw) [][]any {
	per := make([][]any, len(idx.fields))
	for i, f := range idx.fields {
		switch {
		case sh.isReplace:
			per[i] = fieldValues(sh.replacement, f)
		default:
			if v, ok := sh.set[f]; ok {
				per[i] = openValue(v)
				continue
			}
			if sh.unset[f] {
				per[i] = []any{nil}
				continue
			}
			if existing != nil {
				per[i] = fieldValues(existing, f)
				continue
			}
			// An upsert with nothing matched: the document mongod is about to
			// create also inherits the filter's equality fields.
			if v, ok := equalityInFilter(filter, f); ok {
				per[i] = openValue(v)
				continue
			}
			per[i] = []any{nil}
		}
	}
	return per
}

// prospective builds the document the statement will leave behind, so a sparse
// or partial index can be evaluated against what the document is about to
// become rather than against what it is now. A document can move into or out of
// such an index by being updated, and the key has to follow it.
//
// It reports false when the result cannot be assembled without evaluating the
// update, which is the same line drawn by determinable.
func (sh updateShape) prospective(existing bson.Raw, filter bson.Raw) (bson.Raw, bool) {
	if sh.isReplace {
		return sh.replacement, true
	}
	if sh.opaque {
		return nil, false
	}
	for k := range sh.set {
		if strings.Contains(k, ".") {
			return nil, false
		}
	}
	for k := range sh.unset {
		if strings.Contains(k, ".") {
			return nil, false
		}
	}

	base := existing
	if base == nil {
		// An upsert with nothing matched starts from the filter's equality
		// fields, which is what mongod carries into the document it creates.
		base = filter
	}
	elems, err := base.Elements()
	if err != nil {
		return nil, false
	}

	out := make(bson.D, 0, len(elems)+len(sh.set))
	seen := map[string]bool{}
	for _, e := range elems {
		k := e.Key()
		if strings.HasPrefix(k, "$") {
			continue
		}
		if existing == nil {
			// Only equality survives from a filter into an upserted document.
			v, ok := equalityInFilter(base, k)
			if !ok {
				continue
			}
			seen[k] = true
			if sh.unset[k] {
				continue
			}
			if sv, isSet := sh.set[k]; isSet {
				out = append(out, bson.E{Key: k, Value: sv})
				continue
			}
			out = append(out, bson.E{Key: k, Value: v})
			continue
		}
		seen[k] = true
		if sh.unset[k] {
			continue
		}
		if sv, isSet := sh.set[k]; isSet {
			out = append(out, bson.E{Key: k, Value: sv})
			continue
		}
		out = append(out, bson.E{Key: k, Value: e.Value()})
	}
	for k, v := range sh.set {
		if seen[k] {
			continue
		}
		out = append(out, bson.E{Key: k, Value: v})
	}

	raw, merr := bson.Marshal(out)
	if merr != nil {
		return nil, false
	}
	return raw, true
}

func lookupPath(doc bson.Raw, field string) (bson.RawValue, bool) {
	v, err := doc.LookupErr(strings.Split(field, ".")...)
	if err != nil {
		return bson.RawValue{}, false
	}
	return v, true
}

// filterDepth bounds the walk through $and and $or. Nothing an application
// writes nests deeper, and the filter comes from a client.
const filterDepth = 10

// equalityInFilter pulls a field's value out of a filter when the filter pins it
// to one value, which is what an upsert carries into the document it creates.
//
// What counts as pinning is mongod's answer and not ours, so it was measured
// rather than reasoned: every row of
// TestEqualityInFilterMatchesWhatMongodCarries was run as an upsert against
// mongod 8.0 and judged by the document that came back. Four spellings it
// carries were being read as pinning nothing:
//
//	{f: {$in: [v]}}          creates f
//	{f: {$all: [v]}}         creates f
//	{$and: [{f: v}, ...]}    creates f, at any nesting
//	{$or: [{f: v}]}          creates f, with exactly one clause
//
// The cost of missing one is paid twice, because this function is shared. The
// reserved field guard asks it whether an upsert would write _tcDeletedAt, so a
// missed spelling is a live document wearing a tombstone's mark. The unique key
// registry asks it which keys the upsert is about to take, so a missed spelling
// is a cross-tier duplicate admitted with nothing claimed. Sharing the reader
// was meant to stop the two drifting apart, and it did: they had one hole
// rather than two.
func equalityInFilter(filter bson.Raw, field string) (bson.RawValue, bool) {
	return equalityInFilterAt(filter, field, 0)
}

func equalityInFilterAt(filter bson.Raw, field string, depth int) (bson.RawValue, bool) {
	if len(filter) == 0 || depth > filterDepth {
		return bson.RawValue{}, false
	}
	if v, err := filter.LookupErr(field); err == nil {
		return pinnedValue(v)
	}
	// A nested field is pinned by naming the path or by naming the subdocument,
	// and mongod creates and indexes it either way. Measured against 8.0 with a
	// unique index on meta.email: {"meta.email": v}, {meta: {email: v}} and
	// {meta: {$eq: {email: v}}} all upserted a document the index held. Only the
	// first was being read, so an upsert taking a nested key by one of the other
	// two created the document with nothing claimed.
	for at := 0; at < len(field); at++ {
		if field[at] != '.' {
			continue
		}
		v, err := filter.LookupErr(field[:at])
		if err != nil {
			continue
		}
		inner, pinned := pinnedValue(v)
		if !pinned {
			continue
		}
		doc, isDoc := inner.DocumentOK()
		if !isDoc {
			continue
		}
		if eq, found := equalityInFilterAt(doc, field[at+1:], depth+1); found {
			return eq, true
		}
	}
	// $and applies every clause, so an equality in any of them reaches the
	// document. $or applies one of them, so it pins only when there is one.
	for _, op := range []string{"$and", "$or"} {
		v, err := filter.LookupErr(op)
		if err != nil {
			continue
		}
		raw, ok := v.ArrayOK()
		if !ok {
			continue
		}
		vals, verr := raw.Values()
		if verr != nil || len(vals) == 0 {
			continue
		}
		if op == "$or" && len(vals) != 1 {
			continue
		}
		for _, cv := range vals {
			cd, cok := cv.DocumentOK()
			if !cok {
				continue
			}
			if eq, found := equalityInFilterAt(cd, field, depth+1); found {
				return eq, true
			}
		}
	}
	return bson.RawValue{}, false
}

// pinnedValue reports the one value a field's own clause pins it to.
func pinnedValue(v bson.RawValue) (bson.RawValue, bool) {
	doc, ok := v.DocumentOK()
	if !ok {
		// A plain value, which includes an array: that is an exact match on the
		// whole array and mongod creates the field holding it.
		return v, true
	}
	// {field: {$eq: x}} pins it.
	if eq, eerr := doc.LookupErr("$eq"); eerr == nil {
		return eq, true
	}
	// So do $in and $all with exactly one value, which is the pair this missed.
	// More than one pins nothing, and a regex pins nothing even alone: measured,
	// mongod leaves the field out of the document for both.
	// Whatever else is in the clause. Restricting this to a clause with nothing
	// else in it looked like the safe reading and is the wrong one: measured,
	// {f: {$in: [v], $ne: v}} creates f holding v, and so does
	// {f: {$all: [v], $gt: "a"}}. mongod builds the document from the equality
	// clauses and does not care that the result would not match the filter it
	// came from. Reading only the lone form would leave the key unclaimed,
	// which is the direction that admits a cross-tier duplicate.
	for _, op := range []string{"$in", "$all"} {
		av, aerr := doc.LookupErr(op)
		if aerr != nil {
			continue
		}
		raw, arrOK := av.ArrayOK()
		if !arrOK {
			return bson.RawValue{}, false
		}
		vals, verr := raw.Values()
		if verr != nil || len(vals) != 1 || vals[0].Type == bson.TypeRegex {
			return bson.RawValue{}, false
		}
		return vals[0], true
	}
	if elems, eerr := doc.Elements(); eerr == nil {
		for _, e := range elems {
			if strings.HasPrefix(e.Key(), "$") {
				return bson.RawValue{}, false
			}
		}
	}
	// A subdocument with no operators in it, which is an exact match on the
	// whole subdocument.
	return v, true
}

// guardUpdateKeys reserves the unique keys a statement is about to move onto,
// before the fast tier is allowed to run it.
func (c *conn) guardUpdateKeys(ctx context.Context, msg *wire.Message, db, name string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}

	coll, stmts, ok := updateStatements(cmd, msg, name)
	if !ok || len(stmts) == 0 {
		return nil, false
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}
	idxs := c.r.uniqueIndexes(ctx, ns)
	if len(idxs) == 0 {
		return nil, false
	}

	var claimed []string
	for s, st := range stmts {
		sh := parseUpdate(st.update)
		for i := range idxs {
			if !sh.affects(idxs[i]) && !st.upsert {
				continue
			}
			clash, kids, cerr := c.claimStatement(ctx, ns, idxs[i], sh, st)
			if out, done := c.refusedAsBehind(ctx, msg, cerr, append(claimed, kids...)); done {
				return out, true
			}
			if cerr != nil {
				c.r.releaseKeys(ctx, claimed)
				c.log.Warn("could not check unique keys, letting the update through",
					"ns", ns, "err", cerr)
				return nil, false
			}
			claimed = append(claimed, kids...)
			if clash {
				c.r.releaseKeys(ctx, claimed)
				// mongod refuses an update for its own reasons before it ever
				// reaches an index: $[] on a path that does not exist, $inc on
				// a string. The key was read off the command without running
				// it, so a statement that could never have written was answered
				// E11000 where mongod says "The path 'tags' must exist", found
				// by the fuzzer on 2026-10-07. A client that reads 11000 as
				// "taken, pick another" acts on an answer MongoDB never gives.
				//
				// So before saying duplicate, the fast tier runs the statement
				// in a transaction that is thrown away. If it fails on its own,
				// the command goes through and fails the same way, writing
				// nothing. One statement only: with more, forwarding would run
				// the ones around it, and those are not all checked here.
				if len(stmts) == 1 && c.failsOnItsOwn(ctx, ns, coll, st) {
					c.log.Debug("an update whose key is taken fails for another reason first, letting mongod say which",
						"ns", ns, "index", idxs[i].name)
					return nil, false
				}
				c.r.Stats.DuplicateKeys.Add(1)
				c.log.Debug("update would duplicate an archived document",
					"ns", ns, "index", idxs[i].name)
				// With more than one statement nothing is forwarded, and a
				// reply of ok: 1 with one write error at s said the others had
				// run: an ordered update of [A, B] whose B takes an archived
				// key applied neither, and the client was told A had gone in.
				// mongod applies A. Applying it here means a trimmed command
				// and its reply put back together, which this does not do, so
				// the whole command is refused, saying nothing was applied.
				if len(stmts) > 1 {
					c.r.Stats.Refused.Add(1)
					c.cls = ClassRefused
					out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
						fmt.Sprintf("thermocline: refusing this update because statement %d would take a key "+
							"held by a document in the archive (index %s), which mongod would refuse with "+
							"E11000, and this router cannot apply the other statements without it. Nothing "+
							"was applied. Send that statement on its own to get the duplicate key error, and "+
							"the others separately.", s, idxs[i].name))
					if rerr != nil {
						return nil, false
					}
					return out, true
				}
				out, rerr := c.r.updateDuplicateKeyReply(msg, name, ns, idxs[i].name, s)
				if rerr != nil {
					return nil, false
				}
				return out, true
			}
		}
	}

	// Held until the reply comes back, so a write the fast tier rejects gives
	// its keys back instead of blocking that value until something collides.
	c.pendingKeys = append(c.pendingKeys, claimed...)
	return nil, false
}

// claimStatement claims the key every document this statement touches will end
// up with.
func (c *conn) claimStatement(ctx context.Context, ns string, idx uniqueIndex, sh updateShape, st updateStatement) (clash bool, claimed []string, err error) {
	_, coll := splitNS(ns)

	docs, ids, tooMany, merr := c.matchedForStatement(ctx, ns, st)
	if merr != nil {
		return false, nil, merr
	}
	if tooMany {
		c.log.Warn("too many documents to check against the key registry, letting the update through",
			"ns", ns, "limit", maxKeyedUpdate)
		return false, nil, nil
	}

	// The new value of an indexed field cannot always be read off the command.
	// When it cannot, the fast tier is asked to work it out in a transaction
	// that is thrown away, rather than the write being waved through.
	if !sh.determinable(idx) {
		after, eerr := c.evaluateUpdate(ctx, ns, coll, dryRunStatement(st, ids), ids)
		if eerr != nil {
			c.log.Warn("could not evaluate this update against the key registry, letting it through",
				"ns", ns, "index", idx.name, "err", eerr,
				"effect", "a duplicate of an archived document could be created")
			return false, nil, nil
		}
		c.log.Debug("evaluated an update the router cannot read the result of",
			"ns", ns, "index", idx.name, "matched", len(ids), "evaluated", len(after))
		return c.claimEvaluated(ctx, ns, idx, after)
	}

	statement := map[string]string{}
	for _, doc := range docs {
		id, _ := doc.LookupErr("_id")
		if !c.constrainsAfter(ctx, ns, idx, sh, doc, st.filter) {
			continue
		}
		// Every key the post-image takes. See multikey.go.
		kids, tuples := keysFromFieldValues(ns, idx, sh.newKeyValues(idx, doc, st.filter))
		for k, kid := range kids {
			if sameStatementHolds(statement, kid, id) {
				continue
			}
			got, terr := c.r.tryClaim(ctx, ns, idx, kid, id, tuples[k])
			if terr != nil {
				return false, claimed, terr
			}
			if !got {
				return true, claimed, nil
			}
			claimed = append(claimed, kid)
			statement[kid] = idKey(id)
		}
	}

	// Nothing matched. An upsert is still about to create a document, and that
	// document needs its key reserved just as an insert would.
	if len(docs) == 0 && st.upsert {
		if !c.constrainsAfter(ctx, ns, idx, sh, nil, st.filter) {
			return false, claimed, nil
		}
		kids, tuples := keysFromFieldValues(ns, idx, sh.newKeyValues(idx, nil, st.filter))
		owner, _ := lookupPath(st.filter, "_id")
		for k, kid := range kids {
			got, terr := c.r.tryClaim(ctx, ns, idx, kid, owner, tuples[k])
			if terr != nil {
				return false, claimed, terr
			}
			if !got {
				return true, claimed, nil
			}
			claimed = append(claimed, kid)
		}
	}
	return false, claimed, nil
}

// matchedForStatement reads the documents a statement will act on, in the order
// and number the server would pick them.
func (c *conn) matchedForStatement(ctx context.Context, ns string, st updateStatement) (docs []bson.Raw, ids []bson.RawValue, tooMany bool, err error) {
	dbName, coll := splitNS(ns)

	opts := options.Find()
	if !st.multi {
		opts.SetLimit(1)
	} else {
		opts.SetLimit(maxKeyedUpdate + 1)
	}
	// findAndModify picks by sort, so the same sort has to decide which
	// document is examined here or a different one gets checked.
	if len(st.sort) > 0 {
		opts.SetSort(st.sort)
	}
	// And in the statement's own collation, which is what the field's comment
	// on updateStatement asks for and this read was not doing. mongod runs the
	// update under it; this read ran under the collection's default, so the two
	// picked different documents and the keys of the ones missed here were
	// never claimed.
	//
	// The default is deliberately not filled in when the statement carries
	// none: this asks the fast tier, whose collection has its own collation,
	// and mongod applies that to a query that names none. Only the explicit
	// half is missing, which is the half that overrides it.
	//
	// Measured on a unique index over email, the archive alone holding
	// {_id: 2, email: "taken@x.com"} and the fast tier holding
	// {_id: 1, name: "ALICE"}: an update of {name: "alice"} setting that same
	// address, carrying en/strength 2, was accepted by the router and refused
	// E11000 by an untiered reference. The archive then held taken@x.com on
	// two live documents.
	if len(st.collation) > 0 {
		if c := collationFromSpec(st.collation); c != nil {
			opts.SetCollation(c)
		}
	}
	filter := st.filter
	if len(filter) == 0 {
		filter = bson.Raw{}
	}
	cur, ferr := c.r.hot.Database(dbName).Collection(coll).Find(ctx, filter, opts)
	if ferr != nil {
		return nil, nil, false, ferr
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	held := 0
	for cur.Next(ctx) {
		// In bytes as well as documents: every matched document is held, and
		// then read back again after the dry run, so ten thousand large ones is
		// gigabytes twice over. Past either bound the guard steps aside, as it
		// already does for the count.
		held += len(cur.Current)
		if len(docs) >= maxKeyedUpdate || held > maxKeyedUpdateBytes {
			return nil, nil, true, nil
		}
		doc := make(bson.Raw, len(cur.Current))
		copy(doc, cur.Current)
		docs = append(docs, doc)
		if id, ierr := doc.LookupErr("_id"); ierr == nil {
			ids = append(ids, id)
		}
	}
	return docs, ids, false, cur.Err()
}

// dryRunStatement rewrites a statement to act on exactly the documents already
// matched, so the dry run and the real write cannot pick different ones.
func dryRunStatement(st updateStatement, ids []bson.RawValue) bson.D {
	if len(ids) == 0 {
		out := bson.D{
			{Key: "q", Value: st.filter},
			{Key: "u", Value: st.update},
			{Key: "upsert", Value: st.upsert},
			{Key: "multi", Value: false},
		}
		if !st.arrayFilters.IsZero() {
			out = append(out, bson.E{Key: "arrayFilters", Value: st.arrayFilters})
		}
		// The filter is the client's own here, so it is matched the client's
		// way. Not below: there it is an _id list already chosen, and a
		// collation could widen a string _id to documents nobody matched.
		if len(st.collation) > 0 {
			out = append(out, bson.E{Key: "collation", Value: st.collation})
		}
		return out
	}
	out := bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}},
		{Key: "u", Value: st.update},
		{Key: "multi", Value: len(ids) > 1},
	}
	if !st.arrayFilters.IsZero() {
		out = append(out, bson.E{Key: "arrayFilters", Value: st.arrayFilters})
	}
	return out
}

// failsOnItsOwn reports whether the fast tier refuses this statement for a
// reason of its own, asked in a transaction that is always aborted. It is the
// statement as the client sent it, filter, multi and collation included, so the
// answer is the one the real command would get.
//
// Only a write error from the statement counts. A write conflict is about the
// moment, not the statement, and an error running the dry run at all says
// nothing either way; both answer false and leave the refusal standing, which
// is what happened before this was asked.
func (c *conn) failsOnItsOwn(ctx context.Context, ns, coll string, st updateStatement) bool {
	dbName, _ := splitNS(ns)
	stmt := bson.D{
		{Key: "q", Value: st.filter},
		{Key: "u", Value: st.update},
		{Key: "upsert", Value: st.upsert},
		{Key: "multi", Value: st.multi},
	}
	if !st.arrayFilters.IsZero() {
		stmt = append(stmt, bson.E{Key: "arrayFilters", Value: st.arrayFilters})
	}
	if len(st.collation) > 0 {
		stmt = append(stmt, bson.E{Key: "collation", Value: st.collation})
	}
	// findAndModify's, which decides the document it acts on and so whether
	// the update can apply to it. An update statement takes a sort from 8.0,
	// the release both tiers are held to.
	if len(st.sort) > 0 && !st.multi {
		stmt = append(stmt, bson.E{Key: "sort", Value: st.sort})
	}
	sess, err := c.r.hot.StartSession()
	if err != nil {
		return false
	}
	defer sess.EndSession(context.WithoutCancel(ctx))

	failed := false
	_ = mongo.WithSession(ctx, sess, func(sc context.Context) error {
		if serr := sess.StartTransaction(); serr != nil {
			return serr
		}
		defer func() { _ = sess.AbortTransaction(context.WithoutCancel(sc)) }()
		derr := c.r.hot.Database(dbName).RunCommand(sc, bson.D{
			{Key: "update", Value: coll},
			{Key: "updates", Value: bson.A{stmt}},
		}).Err()
		if code, ok := dryRunWriteError(derr); ok {
			failed = code != writeConflictCode
		}
		return nil
	})
	return failed
}

// writeConflictCode is WriteConflict, which a dry run in its own transaction can
// meet for reasons that belong to the moment and not to the statement.
const writeConflictCode = 112

// dryRunWriteError is the code of the first write error a dry run met.
//
// The driver does not return a reply that carries write errors: RunCommand
// turns them into a WriteException, so they arrive here as an error and never
// as a reply to read writeErrors from. Measured on 2026-10-07, when the first
// version of failsOnItsOwn read the reply and so never saw one.
func dryRunWriteError(err error) (int, bool) {
	var we mongo.WriteException
	if !errors.As(err, &we) || len(we.WriteErrors) == 0 {
		return 0, false
	}
	return we.WriteErrors[0].Code, true
}

// evaluateUpdate works out what a statement will produce, by having the fast
// tier apply it inside a transaction and then rolling that transaction back.
//
// $inc on an indexed field, a $rename, a pipeline update: the result depends on
// evaluation this router does not do, and guessing is not an option when the
// answer decides whether a write is a duplicate. So the server is asked. The
// transaction is always aborted, so nothing the dry run writes survives it, and
// the client's own command is what actually runs afterwards.
//
// It returns the documents as they would be, for keys to be computed from.
func (c *conn) evaluateUpdate(ctx context.Context, ns, coll string, stmt any, ids []bson.RawValue) ([]bson.Raw, error) {
	dbName, _ := splitNS(ns)
	sess, err := c.r.hot.StartSession()
	if err != nil {
		return nil, err
	}
	defer sess.EndSession(context.WithoutCancel(ctx))

	var out []bson.Raw
	rerr := mongo.WithSession(ctx, sess, func(sc context.Context) error {
		if serr := sess.StartTransaction(); serr != nil {
			return serr
		}
		defer func() { _ = sess.AbortTransaction(context.WithoutCancel(sc)) }()

		var reply bson.Raw
		if derr := c.r.hot.Database(dbName).RunCommand(sc, bson.D{
			{Key: "update", Value: coll},
			{Key: "updates", Value: bson.A{stmt}},
		}).Decode(&reply); derr != nil {
			// A write error in the dry run is the fast tier's own answer, and
			// the client's command will get the same one. Nothing to check.
			// It arrives as an error, not in the reply: see dryRunWriteError.
			// Read from the reply, it never matched, and every such update
			// was logged as one that "could create a duplicate".
			if code, ok := dryRunWriteError(derr); ok && code != writeConflictCode {
				return nil
			}
			return derr
		}

		want := ids
		if up, uerr := reply.LookupErr("upserted"); uerr == nil {
			if arr, ok := up.ArrayOK(); ok {
				if vals, verr := arr.Values(); verr == nil {
					for _, v := range vals {
						if d, dok := v.DocumentOK(); dok {
							if id, ierr := d.LookupErr("_id"); ierr == nil {
								want = append(want, id)
							}
						}
					}
				}
			}
		}
		if len(want) == 0 {
			return nil
		}

		cur, ferr := c.r.hot.Database(dbName).Collection(coll).Find(sc,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: want}}}})
		if ferr != nil {
			return ferr
		}
		defer func() { _ = cur.Close(context.WithoutCancel(sc)) }()
		for cur.Next(sc) {
			doc := make(bson.Raw, len(cur.Current))
			copy(doc, cur.Current)
			out = append(out, doc)
		}
		return cur.Err()
	})
	if rerr != nil {
		return nil, rerr
	}
	return out, nil
}

// claimEvaluated claims the keys of documents whose post-update shape had to be
// computed by the fast tier rather than read off the command.
func (c *conn) claimEvaluated(ctx context.Context, ns string, idx uniqueIndex, docs []bson.Raw) (clash bool, claimed []string, err error) {
	dbName, _ := splitNS(ns)
	statement := map[string]string{}
	for _, doc := range docs {
		if !c.r.constrains(ctx, dbName, idx, doc) {
			continue
		}
		id, _ := doc.LookupErr("_id")
		kids, tuples := docKeys(ns, idx, doc)
		for k, kid := range kids {
			if sameStatementHolds(statement, kid, id) {
				continue
			}
			got, terr := c.r.tryClaim(ctx, ns, idx, kid, id, tuples[k])
			if terr != nil {
				return false, claimed, terr
			}
			if !got {
				return true, claimed, nil
			}
			claimed = append(claimed, kid)
			statement[kid] = idKey(id)
		}
	}
	return false, claimed, nil
}

// sameStatementHolds reports whether an earlier document of the same statement
// has claimed this key, for a later one that would take the same new value.
//
// mongod applies the earlier document and refuses the later one with its own
// unique index, so the later one is left for mongod to refuse rather than sent
// to the registry. Sent there it found the earlier document's fresh claim, whose
// owner does not hold the value yet because the update has not run, read it as
// stale and took it over, and the document that did take the value was left
// with a claim pointing at one that never did. Measured on 2026-09-28 with
// updateMany({g:1}, {$set:{email:"x@x"}}) over two documents: see
// releaseUnheld for the other half of it.
func sameStatementHolds(statement map[string]string, kid string, id bson.RawValue) bool {
	owner, taken := statement[kid]
	return taken && owner != idKey(id)
}

// constrainsAfter reports whether idx will index the document once the
// statement has run. A plain unique index always does; a sparse or partial one
// has to be judged against the document the update is about to produce.
func (c *conn) constrainsAfter(ctx context.Context, ns string, idx uniqueIndex, sh updateShape, existing bson.Raw, filter bson.Raw) bool {
	if !idx.sparse && len(idx.partial) == 0 {
		return true
	}
	dbName, _ := splitNS(ns)
	after, ok := sh.prospective(existing, filter)
	if !ok {
		c.log.Warn("cannot tell whether this update lands inside a sparse or partial unique index, not enforcing it",
			"ns", ns, "index", idx.name,
			"effect", "a duplicate of an archived document could be created")
		return false
	}
	return c.r.constrains(ctx, dbName, idx, after)
}

// updateStatements pulls the statements out of whichever command carried them.
func updateStatements(cmd bson.Raw, msg *wire.Message, name string) (string, []updateStatement, bool) {
	switch name {
	case "update":
		coll, ok := stringField(cmd, "update")
		if !ok {
			return "", nil, false
		}
		raw := msg.AllOf("updates")
		out := make([]updateStatement, 0, len(raw))
		for _, st := range raw {
			u, uerr := st.LookupErr("u")
			if uerr != nil {
				continue
			}
			s := updateStatement{update: u}
			if q, qerr := st.LookupErr("q"); qerr == nil {
				s.filter, _ = q.DocumentOK()
			}
			if v, verr := st.LookupErr("upsert"); verr == nil {
				s.upsert, _ = v.BooleanOK()
			}
			if v, verr := st.LookupErr("multi"); verr == nil {
				s.multi, _ = v.BooleanOK()
			}
			if v, verr := st.LookupErr("arrayFilters"); verr == nil {
				s.arrayFilters = v
			}
			// Per statement, which is where update carries it. Never read
			// before, so the field this struct declares for it was always nil
			// and every read of it below asked the wrong question.
			s.collation = docField(st, "collation")
			out = append(out, s)
		}
		return coll, out, true

	case "findAndModify", "findandmodify":
		coll, ok := stringField(cmd, "findAndModify")
		if !ok {
			if coll, ok = stringField(cmd, "findandmodify"); !ok {
				return "", nil, false
			}
		}
		// A findAndModify that only removes writes no key.
		if v, verr := cmd.LookupErr("remove"); verr == nil {
			if b, bok := v.BooleanOK(); bok && b {
				return "", nil, false
			}
		}
		u, uerr := cmd.LookupErr("update")
		if uerr != nil {
			return "", nil, false
		}
		s := updateStatement{update: u}
		if q, qerr := cmd.LookupErr("query"); qerr == nil {
			s.filter, _ = q.DocumentOK()
		}
		if v, verr := cmd.LookupErr("upsert"); verr == nil {
			s.upsert, _ = v.BooleanOK()
		}
		if v, verr := cmd.LookupErr("sort"); verr == nil {
			s.sort, _ = v.DocumentOK()
		}
		if v, verr := cmd.LookupErr("arrayFilters"); verr == nil {
			s.arrayFilters = v
		}
		// Top level, which is where findAndModify carries it, unlike update.
		s.collation = docField(cmd, "collation")
		return coll, []updateStatement{s}, true
	}
	return "", nil, false
}

// updateDuplicateKeyReply is the error the server would have produced if the
// document holding this key had still been in the fast tier.
func (r *Router) updateDuplicateKeyReply(msg *wire.Message, name, ns, index string, stmt int) (*wire.Message, error) {
	errmsg := "E11000 duplicate key error collection: " + ns + " index: " + index +
		" :: thermocline: this update would take a key held by a document in the archive, " +
		"where the fast tier's unique index cannot see it"

	if name == "update" {
		return wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, bson.D{
			{Key: "n", Value: int32(0)},
			{Key: "nModified", Value: int32(0)},
			{Key: "writeErrors", Value: bson.A{bson.D{
				{Key: "index", Value: int32(stmt)},
				{Key: "code", Value: int32(11000)},
				{Key: "errmsg", Value: errmsg},
			}}},
			{Key: "ok", Value: 1.0},
		})
	}
	// findAndModify reports a duplicate key as a failed command, not as a write
	// error inside a successful one.
	return wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "code", Value: int32(11000)},
		{Key: "codeName", Value: "DuplicateKey"},
		{Key: "errmsg", Value: errmsg},
	})
}
