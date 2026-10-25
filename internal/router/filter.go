package router

import (
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// idFromFilter extracts an equality match on _id, if the filter is one.
//
// Only an exact id can be resolved on a miss. Anything else would mean running
// the query against the archive to find out whether the empty answer was true,
// which is the work the fast tier exists to avoid.
//
// Both spellings count: {_id: x} and {_id: {$eq: x}}.
// filterIsOnlyID reports whether a filter says nothing but which _id it wants.
//
// An _id equality names at most one document, which is why a point read is
// exempt from the completeness guard. It does not say that document matches the
// statement: a filter is its _id and everything else in it, and resolving a miss
// from the _id alone finds a document the statement would never have touched.
//
// Outside a transaction that produced a wrong answer, fixed once already by
// answering a point read with the query the client sent. Inside one it produces
// something worse than a wrong answer. The router copies the document up and
// asks the driver to replay the transaction so it can see it; the replay runs,
// the statement still matches nothing, the same _id resolves to the same
// unmatching document, and round it goes. Measured by the differential harness:
// the transaction body ran 446 times and then the driver gave up after its two
// minute budget, so a transaction MongoDB would have committed failed, and the
// application was told the write did not happen. Which was true, and it was the
// router that made it true.
//
// Only the top level. An _id inside $and or $or is not found by idFromFilter
// either, so such a filter already goes down the path that judges the whole of
// it.
func filterIsOnlyID(filter bson.Raw) bool {
	elems, err := filter.Elements()
	if err != nil || len(elems) != 1 {
		return false
	}
	return elems[0].Key() == "_id"
}

func idFromFilter(filter bson.Raw) (bson.RawValue, bool) {
	if len(filter) == 0 {
		return bson.RawValue{}, false
	}
	val, err := filter.LookupErr("_id")
	if err != nil {
		return bson.RawValue{}, false
	}

	if doc, ok := val.DocumentOK(); ok {
		// A document here is ambiguous: {$eq: x} is an operator, {a: 1} is an
		// _id that happens to be a document. MongoDB tells them apart by the
		// leading $, and so must this. Reading every document as an operator
		// meant a compound _id was never recognised as naming one document, so
		// an update to an archived one matched nothing and reported success.
		elems, eerr := doc.Elements()
		if eerr != nil {
			return bson.RawValue{}, false
		}
		if len(elems) == 0 || !strings.HasPrefix(elems[0].Key(), "$") {
			return val, true
		}
		eq, err := doc.LookupErr("$eq")
		if err != nil {
			// An operator other than $eq, so this is a range or a set rather
			// than one document.
			return bson.RawValue{}, false
		}
		return eq, true
	}
	return val, true
}

// lowerTimeBound returns the earliest instant a filter can match, when that can
// be read from the filter itself, in the dimension the namespace is tiered by.
//
// The field matters and used to be ignored. A completeness claim says the fast
// tier holds every document newer than an instant, and "newer" is measured the
// way the rules measure age: a date field, or the timestamp inside a default
// ObjectId. A bound on any other date is a bound in a different dimension and
// says nothing about which documents are here.
//
// Measured on a collection tiered by createdAt, claimed complete for the last
// thirty days, holding one recent order on the fast tier and a two year old one
// in the archive, both shipped yesterday. The query
// {shippedAt: {$gte: two days ago}} was read as bounded two days back, judged
// inside the claim, answered from the fast tier alone, and returned 1 where the
// archive holds 2. No error, and the number looked reasonable.
//
// So the bound has to come from the field the age comes from. Anything else is
// unbounded, which sends the read to the archive: slower, and right.
func lowerTimeBound(filter bson.Raw, ageField string, known bool) (time.Time, bool) {
	return boundInFilter(filter, ageField, known, 0)
}

// filterDepthLimit bounds how far $and and $or nesting is followed.
//
// mongod enforces a maximum BSON depth of 100, but this reads the client's
// filter before mongod has seen it, so a document nested deeper than anything
// it would accept still arrives here first. A hundred is past any filter an
// application writes and short of anything that could exhaust the stack.
// joinedCollections bounds itself for the same reason.
//
// Past the limit, nothing is read, which is the same answer as a filter
// carrying no bound: the query goes to the archive.
const filterDepthLimit = 100

func boundInFilter(filter bson.Raw, ageField string, known bool, depth int) (time.Time, bool) {
	if depth > filterDepthLimit {
		return time.Time{}, false
	}
	if len(filter) == 0 || !known {
		// Nothing says how this namespace ages, so nothing can say which
		// dimension a claim about it is in.
		return time.Time{}, false
	}
	elems, err := filter.Elements()
	if err != nil {
		return time.Time{}, false
	}

	// An empty ageField means the age is the timestamp inside the _id, which is
	// the shape a rule with no date_field uses.
	want := ageField
	if want == "" {
		want = "_id"
	}

	var best time.Time
	var found bool
	take := func(t time.Time) {
		// The tightest bound wins: any of them being inside the complete range
		// is enough to make the whole answer complete.
		if !found || t.After(best) {
			best, found = t, true
		}
	}

	for _, e := range elems {
		// $and means every branch holds, so a bound in any one of them bounds
		// the whole query. Worth reading, because that is how an ORM writes a
		// filter: {$and: [{status: "open"}, {createdAt: {$gte: T}}]} is the same
		// query as the flat one beside it, and without this it was unbounded and
		// went to the archive however complete the fast tier was.
		//
		// Only $and and $or. $nor is about documents that do not match, and
		// $not inverts the range rather than narrowing it. Reading a bound out
		// of either would claim the query cannot reach further back than it can.
		if e.Key() == "$and" {
			arr, aerr := e.Value().ArrayOK()
			if !aerr {
				continue
			}
			vals, verr := arr.Values()
			if verr != nil {
				continue
			}
			for _, v := range vals {
				doc, dok := v.DocumentOK()
				if !dok {
					continue
				}
				if t, ok := boundInFilter(doc, ageField, known, depth+1); ok {
					take(t)
				}
			}
			continue
		}

		// $or is satisfied by any one branch, so it is bounded only when every
		// branch is, and then at the earliest of them: a matching document
		// satisfies at least one branch and is therefore no older than that
		// branch's bound. One branch carrying none makes the whole thing reach
		// as far back as that branch does.
		//
		// This is how alternatives are written, by hand and by an ORM, and
		// leaving it out sent a query bounded in every branch to the archive.
		if e.Key() == "$or" {
			if t, ok := earliestBranchBound(e.Value(), ageField, known, depth+1); ok {
				take(t)
			}
			continue
		}

		if e.Key() != want {
			continue
		}
		if t, ok := boundOf(e.Value(), ageType(ageField)); ok {
			take(t)
		}
	}
	return best, found
}

// earliestBranchBound reads the bound on an $or: the earliest of its branches'
// bounds, and only when every branch has one.
func earliestBranchBound(val bson.RawValue, ageField string, known bool, depth int) (time.Time, bool) {
	arr, ok := val.ArrayOK()
	if !ok {
		return time.Time{}, false
	}
	vals, err := arr.Values()
	if err != nil || len(vals) == 0 {
		// An empty $or matches nothing, which mongod refuses anyway. Nothing to
		// read either way.
		return time.Time{}, false
	}
	var earliest time.Time
	for i, v := range vals {
		doc, dok := v.DocumentOK()
		if !dok {
			return time.Time{}, false
		}
		t, tok := boundInFilter(doc, ageField, known, depth)
		if !tok {
			return time.Time{}, false
		}
		if i == 0 || t.Before(earliest) {
			earliest = t
		}
	}
	return earliest, true
}

// ageType is the BSON type a rule ages by: the date in date_field, or the
// ObjectId of an _id when there is none.
//
// A bound of the other type is not a bound. mongod brackets a range by type,
// and the claim covers the type its rule ages by: the evictor and the warmer
// select dates for a date field and ObjectIds for _id. So {createdAt: {$gte:
// ObjectId(...)}} read as a date bound inside the claim answered from the fast
// tier, which holds none of the adopted documents whose createdAt is an
// ObjectId, where MongoDB returns them.
func ageType(ageField string) bson.Type {
	if ageField == "" {
		return bson.TypeObjectID
	}
	return bson.TypeDateTime
}

// boundOf reads a lower bound from one filter value, of the type the rule ages
// by.
//
// Every operator here is one the document has to satisfy along with the others
// beside it, so the latest of the bounds they give is the bound on the query.
func boundOf(val bson.RawValue, typ bson.Type) (time.Time, bool) {
	// An exact ObjectId or date is its own lower bound.
	if t, ok := timeOf(val, typ); ok {
		return t, true
	}

	doc, ok := val.DocumentOK()
	if !ok {
		return time.Time{}, false
	}

	var best time.Time
	var found bool
	take := func(t time.Time) {
		if !found || t.After(best) {
			best, found = t, true
		}
	}

	// $eq is the same statement as the bare value beside it, and a driver
	// writes one or the other depending on how the query was built. Reading
	// only the bare form sent an ORM's query to the archive and the hand
	// written one to the fast tier.
	for _, op := range []string{"$gte", "$gt", "$eq"} {
		v, err := doc.LookupErr(op)
		if err != nil {
			continue
		}
		if t, ok := timeOf(v, typ); ok {
			take(t)
		}
	}

	// $in is bounded at its earliest member, and only when every member is an
	// instant. One that is not matches documents whose age field holds
	// something else, and no claim about time covers those: MongoDB brackets a
	// range predicate by type, and an equality list is not bracketed at all.
	if v, err := doc.LookupErr("$in"); err == nil {
		if t, ok := earliestOf(v, typ); ok {
			take(t)
		}
	}

	return best, found
}

// earliestOf reads the earliest instant from an $in list, or false when the
// list is empty or holds anything that is not one.
func earliestOf(val bson.RawValue, typ bson.Type) (time.Time, bool) {
	arr, ok := val.ArrayOK()
	if !ok {
		return time.Time{}, false
	}
	vals, err := arr.Values()
	if err != nil || len(vals) == 0 {
		return time.Time{}, false
	}
	var earliest time.Time
	for i, v := range vals {
		t, ok := timeOf(v, typ)
		if !ok {
			return time.Time{}, false
		}
		if i == 0 || t.Before(earliest) {
			earliest = t
		}
	}
	return earliest, true
}

// timeOf reads an instant from an ObjectId or a date, of type typ only.
func timeOf(val bson.RawValue, typ bson.Type) (time.Time, bool) {
	if val.Type != typ {
		return time.Time{}, false
	}
	switch val.Type {
	case bson.TypeObjectID:
		oid, ok := val.ObjectIDOK()
		if !ok {
			return time.Time{}, false
		}
		return oid.Timestamp().UTC(), true
	case bson.TypeDateTime:
		t, ok := val.TimeOK()
		if !ok {
			return time.Time{}, false
		}
		return t.UTC(), true
	default:
		return time.Time{}, false
	}
}

// stringField reads a string field from a command document.
func stringField(doc bson.Raw, key string) (string, bool) {
	val, err := doc.LookupErr(key)
	if err != nil {
		return "", false
	}
	return val.StringValueOK()
}

// replyOK reports whether a command succeeded.
func replyOK(reply bson.Raw) bool {
	val, err := reply.LookupErr("ok")
	if err != nil {
		return false
	}
	switch val.Type {
	case bson.TypeDouble:
		return val.Double() == 1
	case bson.TypeInt32:
		return val.Int32() == 1
	case bson.TypeInt64:
		return val.Int64() == 1
	case bson.TypeBoolean:
		return val.Boolean()
	default:
		return false
	}
}

// firstBatch returns the documents of a find reply.
func firstBatch(reply bson.Raw) ([]bson.RawValue, bool) {
	cursor, err := reply.LookupErr("cursor")
	if err != nil {
		return nil, false
	}
	cdoc, ok := cursor.DocumentOK()
	if !ok {
		return nil, false
	}
	batch, err := cdoc.LookupErr("firstBatch")
	if err != nil {
		return nil, false
	}
	arr, ok := batch.ArrayOK()
	if !ok {
		return nil, false
	}
	vals, err := arr.Values()
	if err != nil {
		return nil, false
	}
	return vals, true
}

// matched returns how many documents a write command matched.
func matched(reply bson.Raw) int {
	val, err := reply.LookupErr("n")
	if err != nil {
		return -1
	}
	switch val.Type {
	case bson.TypeInt32:
		return int(val.Int32())
	case bson.TypeInt64:
		return int(val.Int64())
	case bson.TypeDouble:
		return int(val.Double())
	default:
		return -1
	}
}

// writeFailed reports whether a write reply carries per-statement errors, which
// a command can do while still reporting ok:1.
func writeFailed(reply bson.Raw) bool {
	val, err := reply.LookupErr("writeErrors")
	if err != nil {
		return false
	}
	arr, ok := val.ArrayOK()
	if !ok {
		return false
	}
	vals, err := arr.Values()
	return err == nil && len(vals) > 0
}

// ageFieldOf is the field a namespace's age is read from, and whether anything
// says. Everything that judges a query against a completeness claim asks this,
// so the claim and the bound are always read in the same dimension.
func (r *Router) ageFieldOf(ns string) (string, bool) {
	return r.cfg.AgeField(ns)
}
