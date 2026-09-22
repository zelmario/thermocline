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
		if e.Key == "email" && e.Value != nil {
			t.Errorf("a missing field must be looked up as null, got %v", e.Value)
		}
	}
}
