package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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
// the field.
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
// was true before arrays were understood and has to stay true.
func TestMultikeyMissingFieldIsOneNullKey(t *testing.T) {
	idx := uniqueIndex{name: "tags_1", fields: []string{"tags"}}
	a := keysOf(t, idx, bson.D{{Key: "other", Value: 1}})
	b := keysOf(t, idx, bson.D{{Key: "other", Value: 2}})
	if len(a) != 1 || a[0] != b[0] {
		t.Errorf("two documents missing the field gave %v and %v", a, b)
	}
}

// One level. MongoDB indexes an array inside an array as a value rather than
// opening it again.
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
// before anything here matters and there is nothing to protect.
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

// The values worked out by the update path expand the same way, or an update
// that sets an array would be judged on one key while the insert of the same
// document is judged on all of them.
func TestMultikeyValueKeysMatchDocKeys(t *testing.T) {
	idx := uniqueIndex{name: "tags_1", fields: []string{"tags"}}
	d := mkRaw(t, bson.D{{Key: "tags", Value: bson.A{"x", "y"}}})
	fromDoc, _ := docKeys("shop.k", idx, d)
	v, err := d.LookupErr("tags")
	if err != nil {
		t.Fatal(err)
	}
	fromVals, _ := valueKeys("shop.k", idx, []any{v})
	if len(fromDoc) != len(fromVals) {
		t.Fatalf("document gave %d keys and values gave %d", len(fromDoc), len(fromVals))
	}
	for i := range fromDoc {
		if fromDoc[i] != fromVals[i] {
			t.Errorf("key %d differs: %s and %s", i, fromDoc[i], fromVals[i])
		}
	}
}
