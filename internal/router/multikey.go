package router

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A unique index on a field that holds an array is multikey, and one document
// puts one entry per element into it. So no two documents may share *any*
// element, and a registry that computes one key per document is comparing the
// wrong thing.
//
// Measured before this. A document holding tags: ["x","y"] mirrored and then
// taken off the fast tier, and a second holding tags: ["y","z"] inserted: the
// router accepted it, the fast tier accepted it, and the archive refused the
// mirrored insert with E11000 dup key { tags: "y" }. The document then sits on
// the fast tier, absent from the archive, and the evictor will not remove it
// because the archive cannot confirm it.
//
// The other half of the same mistake was quieter and pointed the other way. A
// unique index on a dotted path into an array, "items.sku", cannot be read by
// looking the path up: the driver answers "element not found" for a path that
// crosses an array, so *every* such document was keyed as null and the first one
// claimed that key for the namespace. Every insert after it was refused as a
// duplicate of a document it has nothing in common with.
//
// So the keys of a document are a list, worked out the way mongod works them
// out: descend into an array wherever the path meets one, and expand the value
// at the end of the path if that is an array too.

// fieldValues is every value one document contributes for one index field.
//
// A missing field is nil, which is the null key MongoDB indexes it as, and which
// keyIDFromValues already encodes. An empty array is undefined, which is what
// MongoDB indexes it as, and two documents holding one are duplicates of each
// other while neither is a duplicate of a document missing the field.
//
// Those are measured against mongod rather than read out of the manual, and so
// is every other corner here. See the head of multikey_test.go for the six
// answers and what was asked to get them.
func fieldValues(doc bson.Raw, field string) []any {
	parts := strings.Split(field, ".")
	v, err := doc.LookupErr(parts[0])
	if err != nil {
		return []any{nil}
	}
	return descend(v, parts[1:])
}

// descend walks the rest of a path from one value, opening arrays as it goes.
func descend(v bson.RawValue, rest []string) []any {
	if arr, ok := v.ArrayOK(); ok {
		vals, verr := arr.Values()
		if verr != nil {
			return []any{nil}
		}
		if len(rest) == 0 {
			if len(vals) == 0 {
				return []any{bson.Undefined{}}
			}
			// One level. MongoDB indexes an array inside an array as a value
			// rather than opening it again.
			out := make([]any, 0, len(vals))
			for _, e := range vals {
				out = append(out, e)
			}
			return out
		}
		var out []any
		for _, e := range vals {
			out = append(out, descend(e, rest)...)
		}
		if len(out) == 0 {
			// An empty array part way along a path. Measured: for items.sku,
			// {items: []} and a document with no items at all are duplicates of
			// each other, so both are the null key.
			return []any{nil}
		}
		return out
	}
	if len(rest) == 0 {
		return []any{v}
	}
	sub, ok := v.DocumentOK()
	if !ok {
		// The path cannot continue through this, which indexes as null.
		return []any{nil}
	}
	nv, err := sub.LookupErr(rest[0])
	if err != nil {
		return []any{nil}
	}
	return descend(nv, rest[1:])
}

// keyTuples is every combination of values one document contributes to one
// index, in field order.
//
// Empty when the document cannot be indexed at all. MongoDB refuses to index
// parallel arrays: a compound index cannot take two array fields from one
// document, and the write fails on the fast tier before anything here matters.
// Claiming nothing is right for that, because there is nothing to protect.
func keyTuples(idx uniqueIndex, doc bson.Raw) [][]any {
	per := make([][]any, len(idx.fields))
	arrays := 0
	for i, f := range idx.fields {
		per[i] = fieldValues(doc, f)
		if len(per[i]) > 1 {
			arrays++
		}
		if len(per[i]) == 0 {
			per[i] = []any{nil}
		}
	}
	if arrays > 1 {
		return nil
	}
	return crossOne(per)
}

// crossOne expands the one field that may have several values, which is all a
// legal multikey index can have.
func crossOne(per [][]any) [][]any {
	n := 1
	for _, vs := range per {
		if len(vs) > n {
			n = len(vs)
		}
	}
	out := make([][]any, 0, n)
	for k := 0; k < n; k++ {
		tuple := make([]any, len(per))
		for i, vs := range per {
			if len(vs) == 1 {
				tuple[i] = vs[0]
				continue
			}
			tuple[i] = vs[k]
		}
		out = append(out, tuple)
	}
	return out
}

// docKeys is every registry id one document takes in one index, with the values
// each was built from.
//
// Deduplicated, because MongoDB puts one entry in the index for a value however
// many times the array repeats it, and claiming the same id twice in one batch
// would have the second look like somebody else holding it.
func docKeys(ns string, idx uniqueIndex, doc bson.Raw) ([]string, [][]any) {
	return keysFromTuples(ns, idx, keyTuples(idx, doc))
}

// keysFromFieldValues is docKeys for values worked out some other way, which is
// what the update path has to do: the document that will hold the key does not
// exist yet, so its values come from the update, from the document being changed
// and from the filter.
//
// It takes the values per field rather than one each, for the same reason
// fieldValues returns a list: a field holding an array contributes one of them
// per element. The update path used to hand over one value per field and it was
// the null key for anything behind an array, which stopped agreeing with the
// insert path the moment that one learned to walk them.
func keysFromFieldValues(ns string, idx uniqueIndex, per [][]any) ([]string, [][]any) {
	if len(per) != len(idx.fields) {
		return nil, nil
	}
	arrays := 0
	for i := range per {
		if len(per[i]) == 0 {
			per[i] = []any{nil}
		}
		if len(per[i]) > 1 {
			arrays++
		}
	}
	if arrays > 1 {
		return nil, nil
	}
	return keysFromTuples(ns, idx, crossOne(per))
}

// openValue expands one value the way the end of a path is expanded.
func openValue(v any) []any {
	rv, ok := v.(bson.RawValue)
	if !ok {
		return []any{v}
	}
	arr, aok := rv.ArrayOK()
	if !aok {
		return []any{v}
	}
	vals, err := arr.Values()
	if err != nil {
		return []any{v}
	}
	if len(vals) == 0 {
		return []any{bson.Undefined{}}
	}
	out := make([]any, 0, len(vals))
	for _, e := range vals {
		out = append(out, e)
	}
	return out
}

func keysFromTuples(ns string, idx uniqueIndex, tuples [][]any) ([]string, [][]any) {
	if len(tuples) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(tuples))
	ids := make([]string, 0, len(tuples))
	kept := make([][]any, 0, len(tuples))
	for _, t := range tuples {
		id, ok := keyIDFromValues(ns, idx, t)
		if !ok {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
		kept = append(kept, t)
	}
	return ids, kept
}
