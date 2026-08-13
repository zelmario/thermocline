package router

import (
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
func idFromFilter(filter bson.Raw) (bson.RawValue, bool) {
	if len(filter) == 0 {
		return bson.RawValue{}, false
	}
	val, err := filter.LookupErr("_id")
	if err != nil {
		return bson.RawValue{}, false
	}

	if doc, ok := val.DocumentOK(); ok {
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
// be read from the filter itself.
//
// Two shapes are recognised: a bound on _id, which carries a timestamp when it
// is an ObjectId, and a bound on a date field. A query with neither is
// unbounded and could match anything, including documents that have left the
// fast tier.
func lowerTimeBound(filter bson.Raw) (time.Time, bool) {
	if len(filter) == 0 {
		return time.Time{}, false
	}
	elems, err := filter.Elements()
	if err != nil {
		return time.Time{}, false
	}

	var best time.Time
	var found bool
	for _, e := range elems {
		t, ok := boundOf(e.Value())
		if !ok {
			continue
		}
		// The tightest bound wins: any of them being inside the complete range
		// is enough to make the whole answer complete.
		if !found || t.After(best) {
			best, found = t, true
		}
	}
	return best, found
}

// boundOf reads a lower bound from one filter value.
func boundOf(val bson.RawValue) (time.Time, bool) {
	// An exact ObjectId or date is its own lower bound.
	if t, ok := timeOf(val); ok {
		return t, true
	}

	doc, ok := val.DocumentOK()
	if !ok {
		return time.Time{}, false
	}
	for _, op := range []string{"$gte", "$gt"} {
		v, err := doc.LookupErr(op)
		if err != nil {
			continue
		}
		if t, ok := timeOf(v); ok {
			return t, true
		}
	}
	return time.Time{}, false
}

// timeOf reads an instant from an ObjectId or a date.
func timeOf(val bson.RawValue) (time.Time, bool) {
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
