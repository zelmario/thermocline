package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The three places that ask whether a document holds a key have to agree on
// which documents an index indexes. Two get it from indexScope directly; this
// is the third, which was written without it and refused writes mongod accepts.
func TestCollatedClashFilterNarrowsToWhatTheIndexIndexes(t *testing.T) {
	partial, err := bson.Marshal(bson.D{{Key: "active", Value: true}})
	if err != nil {
		t.Fatal(err)
	}

	has := func(f bson.D, key string) bool {
		for _, e := range f {
			if e.Key == key {
				return true
			}
		}
		return false
	}

	plain := uniqueIndex{name: "email_1", fields: []string{"email"}, collation: bson.Raw{1}}
	f := collatedClashFilter(plain, []any{"bob@x.com"}, bson.RawValue{})
	if has(f, "$and") {
		t.Errorf("a plain index narrows nothing, got %v", f)
	}
	if !has(f, "email") {
		t.Errorf("the value is not in the filter: %v", f)
	}

	withPartial := plain
	withPartial.partial = bson.Raw(partial)
	f = collatedClashFilter(withPartial, []any{"bob@x.com"}, bson.RawValue{})
	if !has(f, "$and") {
		t.Errorf("a partial index must narrow to its filter, got %v", f)
	}

	sparse := plain
	sparse.sparse = true
	f = collatedClashFilter(sparse, []any{"bob@x.com"}, bson.RawValue{})
	if !has(f, "$and") {
		t.Errorf("a sparse index must narrow to the documents it indexes, got %v", f)
	}

	// The owner keeps its own value, and a missing field is a null key, both
	// of which the archive lookup relied on before this was extracted.
	owner := bson.RawValue{Type: bson.TypeInt32, Value: []byte{1, 0, 0, 0}}
	f = collatedClashFilter(plain, []any{nil}, owner)
	if !has(f, "_id") {
		t.Errorf("the owner must be excluded: %v", f)
	}
	for _, e := range f {
		if e.Key == "email" && !isEqOf(e.Value, nil) {
			t.Errorf("a missing field must be looked up as {$eq: null}, got %v", e.Value)
		}
	}
}

// A key value comes from the client's document, and a document of operators or
// a regex placed bare in a filter is read as a query: {email: {$ne: null}}
// matched every archived email. Each value is compared with $eq, literally.
func TestKeyValuesStayLiteral(t *testing.T) {
	plain := uniqueIndex{fields: []string{"email"}}
	for _, v := range []any{
		bson.D{{Key: "$ne", Value: nil}},
		bson.Regex{Pattern: ".*"},
		"bob@x.com",
	} {
		f := collatedClashFilter(plain, []any{v}, bson.RawValue{})
		for _, e := range f {
			if e.Key == "email" && !isEqOf(e.Value, v) {
				t.Errorf("value %v went into the filter as %v, not {$eq: value}", v, e.Value)
			}
		}
	}
}

func isEqOf(v any, want any) bool {
	d, ok := v.(bson.D)
	if !ok || len(d) != 1 || d[0].Key != "$eq" {
		return false
	}
	a, _ := bson.Marshal(bson.D{{Key: "v", Value: d[0].Value}})
	b, _ := bson.Marshal(bson.D{{Key: "v", Value: want}})
	return string(a) == string(b)
}
