package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Every rule below was checked against PSMDB 8.0 by putting the documents into a
// collection with the unique index on it and seeing which pairs it called
// duplicates, rather than read out of the manual. The six answers were: two
// empty arrays collide; an empty array and a missing field do not; two missing
// fields collide; for a dotted path an empty array at the prefix and a missing
// field *do* collide; two documents holding [[1,2]] collide, so the inner array
// is a value; and parallel arrays are error 171 rather than an index entry.

func mkRaw(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return bson.Raw(b)
}

func keysOf(t *testing.T, idx uniqueIndex, d bson.D) []string {
	t.Helper()
	ids, vals := docKeys("shop.k", idx, mkRaw(t, d))
	if len(ids) != len(vals) {
		t.Fatalf("%d ids and %d value tuples", len(ids), len(vals))
	}
	return ids
}

// One key per element, which is what a multikey index holds. Everything else in
// this file is a corner of that.
func TestMultikeyOneKeyPerElement(t *testing.T) {
	idx := uniqueIndex{name: "tags_1", fields: []string{"tags"}}
	got := keysOf(t, idx, bson.D{{Key: "tags", Value: bson.A{"x", "y"}}})
	if len(got) != 2 {
		t.Fatalf("tags [x y] gave %d keys, want 2", len(got))
	}
	one := keysOf(t, idx, bson.D{{Key: "tags", Value: "y"}})
	if len(one) != 1 {
		t.Fatalf("a scalar gave %d keys, want 1", len(one))
	}
	// The element and the scalar are the same key, or an array and a plain
	// value holding the same thing would not collide and mongod says they do.
	if got[1] != one[0] {
		t.Errorf("element y is %s and scalar y is %s, want the same", got[1], one[0])
	}
}

// mongod puts one entry in the index however many times the array repeats a
// value, and claiming the same id twice would have the second look like
// somebody else holding it.
func TestMultikeyDeduplicates(t *testing.T) {
	idx := uniqueIndex{name: "tags_1", fields: []string{"tags"}}
	got := keysOf(t, idx, bson.D{{Key: "tags", Value: bson.A{"y", "y", "y"}}})
	if len(got) != 1 {
		t.Errorf("[y y y] gave %d keys, want 1", len(got))
	}
}

// An empty array indexes as undefined, so two documents holding one are
// duplicates of each other, and neither is a duplicate of a document missing
// the field. Measured on mongod: [] then [] is 11000, [] then missing is ok.
func TestMultikeyEmptyArrayIsOneKey(t *testing.T) {
	idx := uniqueIndex{name: "tags_1", fields: []string{"tags"}}
	a := keysOf(t, idx, bson.D{{Key: "tags", Value: bson.A{}}})
	b := keysOf(t, idx, bson.D{{Key: "tags", Value: bson.A{}}})
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("empty arrays gave %d and %d keys, want 1 each", len(a), len(b))
	}
	if a[0] != b[0] {
		t.Errorf("two empty arrays gave %s and %s, want the same key", a[0], b[0])
	}
	missing := keysOf(t, idx, bson.D{{Key: "other", Value: 1}})
	if len(missing) != 1 {
		t.Fatalf("a missing field gave %d keys, want 1", len(missing))
	}
	if missing[0] == a[0] {
		t.Errorf("an empty array and a missing field share key %s, and mongod keys them apart", a[0])
	}
}

// A missing field is null, and two documents missing it are duplicates. That
// was true before arrays were understood and has to stay true. Measured.
func TestMultikeyMissingFieldIsOneNullKey(t *testing.T) {
	idx := uniqueIndex{name: "tags_1", fields: []string{"tags"}}
	a := keysOf(t, idx, bson.D{{Key: "other", Value: 1}})
	b := keysOf(t, idx, bson.D{{Key: "other", Value: 2}})
	if len(a) != 1 || a[0] != b[0] {
		t.Errorf("two documents missing the field gave %v and %v", a, b)
	}
}

// One level. MongoDB indexes an array inside an array as a value rather than
// opening it again: measured, two documents holding [[1,2]] are 11000.
func TestMultikeyOpensOneLevel(t *testing.T) {
	idx := uniqueIndex{name: "tags_1", fields: []string{"tags"}}
	got := keysOf(t, idx, bson.D{{Key: "tags", Value: bson.A{bson.A{1, 2}, "z"}}})
	if len(got) != 2 {
		t.Errorf("[[1 2] z] gave %d keys, want 2", len(got))
	}
}

// A dotted path that crosses an array. The driver cannot look this up at all,
// which is why every such document used to be keyed as null and the first one
// to arrive claimed that key for the whole namespace.
func TestMultikeyDottedPathThroughArray(t *testing.T) {
	idx := uniqueIndex{name: "items.sku_1", fields: []string{"items.sku"}}
	got := keysOf(t, idx, bson.D{{Key: "items", Value: bson.A{
		bson.D{{Key: "sku", Value: "a"}},
		bson.D{{Key: "sku", Value: "b"}},
	}}})
	if len(got) != 2 {
		t.Fatalf("items.sku over two subdocuments gave %d keys, want 2", len(got))
	}
	other := keysOf(t, idx, bson.D{{Key: "items", Value: bson.A{
		bson.D{{Key: "sku", Value: "c"}},
	}}})
	for _, g := range got {
		if g == other[0] {
			t.Errorf("sku c shares a key with one of a or b")
		}
	}
	// An element without the field is a null key, as mongod indexes it.
	mixed := keysOf(t, idx, bson.D{{Key: "items", Value: bson.A{
		bson.D{{Key: "sku", Value: "a"}},
		bson.D{{Key: "qty", Value: 1}},
	}}})
	if len(mixed) != 2 {
		t.Errorf("one sku and one element without it gave %d keys, want 2", len(mixed))
	}
}

// A compound index takes the array from one field and the scalar from the other.
func TestMultikeyCompoundWithOneArray(t *testing.T) {
	idx := uniqueIndex{name: "g_1_tags_1", fields: []string{"g", "tags"}}
	got := keysOf(t, idx, bson.D{
		{Key: "g", Value: "one"},
		{Key: "tags", Value: bson.A{"x", "y"}},
	})
	if len(got) != 2 {
		t.Fatalf("one scalar and two elements gave %d keys, want 2", len(got))
	}
	// A different scalar with the same elements is a different pair of keys.
	other := keysOf(t, idx, bson.D{
		{Key: "g", Value: "two"},
		{Key: "tags", Value: bson.A{"x", "y"}},
	})
	for _, a := range got {
		for _, b := range other {
			if a == b {
				t.Errorf("g one and g two share key %s", a)
			}
		}
	}
}

// MongoDB refuses to index parallel arrays, so the write fails on the fast tier
// before anything here matters and there is nothing to protect. Measured: the
// insert is error 171, CannotIndexParallelArrays.
func TestMultikeyParallelArraysClaimNothing(t *testing.T) {
	idx := uniqueIndex{name: "a_1_b_1", fields: []string{"a", "b"}}
	got := keysOf(t, idx, bson.D{
		{Key: "a", Value: bson.A{1, 2}},
		{Key: "b", Value: bson.A{3, 4}},
	})
	if len(got) != 0 {
		t.Errorf("parallel arrays gave %d keys, want none", len(got))
	}
}

// The update path has to reach the same keys as the insert path for the same
// document, or an update to a document is judged against keys the insert of it
// never claimed.
//
// This is the one that broke. The update path read a field with a plain lookup,
// which cannot cross an array, so items.sku was the null key there and the real
// elements here. They agreed while both were wrong and stopped agreeing the
// moment this side learned to walk arrays: the fuzzer found it as an update
// refused for a duplicate the reference had no trouble with.
func TestMultikeyUpdatePathMatchesDocKeys(t *testing.T) {
	for _, tc := range []struct {
		name string
		idx  uniqueIndex
		doc  bson.D
	}{
		{"an array field", uniqueIndex{name: "tags_1", fields: []string{"tags"}},
			bson.D{{Key: "tags", Value: bson.A{"x", "y"}}}},
		{"a path through an array", uniqueIndex{name: "items.sku_1", fields: []string{"items.sku"}},
			bson.D{{Key: "items", Value: bson.A{
				bson.D{{Key: "sku", Value: "a"}},
				bson.D{{Key: "sku", Value: "b"}},
			}}}},
		{"compound, one of them behind an array",
			uniqueIndex{name: "items.sku_1_n_1", fields: []string{"items.sku", "n"}},
			bson.D{
				{Key: "items", Value: bson.A{bson.D{{Key: "sku", Value: "a"}}, bson.D{{Key: "sku", Value: "b"}}}},
				{Key: "n", Value: 7},
			}},
		{"a scalar, which must not change", uniqueIndex{name: "email_1", fields: []string{"email"}},
			bson.D{{Key: "email", Value: "a@x"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := mkRaw(t, tc.doc)
			fromDoc, _ := docKeys("shop.k", tc.idx, d)
			// What an update that touches none of the indexed fields computes.
			var sh updateShape
			fromUpdate, _ := keysFromFieldValues("shop.k", tc.idx, sh.newKeyValues(tc.idx, d, nil))
			if len(fromDoc) == 0 {
				t.Fatalf("the document gave no keys at all")
			}
			if len(fromDoc) != len(fromUpdate) {
				t.Fatalf("the document gave %d keys and the update path %d", len(fromDoc), len(fromUpdate))
			}
			for i := range fromDoc {
				if fromDoc[i] != fromUpdate[i] {
					t.Errorf("key %d differs: document %s, update path %s", i, fromDoc[i], fromUpdate[i])
				}
			}
		})
	}
}
