package meta

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// rawValueFor marshals v and returns it as the _id value a change event carries.
func rawValueFor(t *testing.T, v any) bson.RawValue {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "_id", Value: v}})
	if err != nil {
		t.Fatalf("marshal %v: %v", v, err)
	}
	rv, err := bson.Raw(raw).LookupErr("_id")
	if err != nil {
		t.Fatalf("lookup _id: %v", err)
	}
	return rv
}

// MongoDB allows any BSON type as an _id, so DocID has to survive all of them
// and never collide across namespaces.
func TestDocIDIsStableAndDistinct(t *testing.T) {
	oid := bson.NewObjectID()

	values := []struct {
		name string
		val  any
	}{
		{"objectid", oid},
		{"string", "order-1"},
		{"int32", int32(1)},
		{"int64", int64(1)},
		{"compound", bson.D{{Key: "tenant", Value: "acme"}, {Key: "seq", Value: 7}}},
		{"binary", bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}},
	}

	seen := make(map[string]string)
	for _, v := range values {
		id := DocID("app.orders", rawValueFor(t, v.val))

		if again := DocID("app.orders", rawValueFor(t, v.val)); again != id {
			t.Errorf("%s: DocID is not stable: %q then %q", v.name, id, again)
		}
		if prev, dup := seen[id]; dup {
			t.Errorf("%s collides with %s, both produced %q", v.name, prev, id)
		}
		seen[id] = v.name

		// The same document id in another collection must be a different key,
		// otherwise one collection's eviction notice would silence another's.
		if other := DocID("app.invoices", rawValueFor(t, v.val)); other == id {
			t.Errorf("%s: DocID does not separate namespaces", v.name)
		}
	}
}

// int32(1) and int64(1) compare equal in MongoDB but are different BSON types.
// Encoding the type byte keeps them apart, which is what we want for a key.
func TestDocIDSeparatesNumericTypes(t *testing.T) {
	a := DocID("app.orders", rawValueFor(t, int32(1)))
	b := DocID("app.orders", rawValueFor(t, int64(1)))
	if a == b {
		t.Errorf("int32(1) and int64(1) produced the same DocID %q", a)
	}
}

func TestTombstoneSetsTheField(t *testing.T) {
	raw, err := bson.Marshal(Tombstone(bson.NewObjectID().Timestamp()))
	if err != nil {
		t.Fatalf("marshal tombstone: %v", err)
	}
	set, err := bson.Raw(raw).LookupErr("$set")
	if err != nil {
		t.Fatalf("tombstone has no $set: %v", err)
	}
	if _, err := set.Document().LookupErr(TombstoneField); err != nil {
		t.Errorf("tombstone $set does not contain %s: %v", TombstoneField, err)
	}
}

// One pair of tiers can carry two Thermocline deployments, and the second one's
// bookkeeping database is not named in the first one's config. Reporting it to
// an operator as application data tells them to tier somebody's checkpoints.
func TestIsBookkeeping(t *testing.T) {
	cases := []struct {
		name  string
		colls []string
		want  bool
	}{
		{"another deployment's bookkeeping", []string{"checkpoints", "watermarks", "positions", "residency"}, true},
		{"the key registry on the fast tier", []string{"keys"}, true},
		{"one bookkeeping collection alone", []string{"markers"}, true},
		{"an application database", []string{"orders", "customers"}, false},
		// The one that matters: a database carrying both is somebody's data
		// with an unlucky name in it, and staying quiet about it would hide the
		// collections an application cannot see.
		{"application data beside a lookalike", []string{"checkpoints", "orders"}, false},
		{"an empty database says nothing either way", nil, false},
	}
	for _, c := range cases {
		if got := IsBookkeeping(c.colls); got != c.want {
			t.Errorf("%s: IsBookkeeping(%v) = %v, want %v", c.name, c.colls, got, c.want)
		}
	}
}
