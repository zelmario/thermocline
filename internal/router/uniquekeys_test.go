package router

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A key sometimes has to be claimed for a document whose _id is not known yet:
// an upsert creates a document mongod names itself, and an insert can arrive
// without one for the same reason.
//
// The registry row was written by marshalling keyClaim, and bson cannot marshal
// a zero RawValue at all. That did not fail the write, it failed the claim, and
// the guard's answer to a registry it cannot write is to let the write through.
// So the one case with no owner was the one case nothing was enforced.
func TestClaimRowWithoutAnOwner(t *testing.T) {
	now := time.Now().UTC()

	// The shape that used to be written, kept here as the reason claimRow
	// exists rather than as something the code still does.
	if _, err := bson.Marshal(keyClaim{ID: "k", NS: "db.coll", Index: "email_1", Doc: bson.RawValue{}, At: now}); err == nil {
		t.Fatal("marshalling a claim with no owner used to fail, and this test is why claimRow exists")
	}

	raw, err := bson.Marshal(claimRow("k", "db.coll", "email_1", bson.RawValue{}, now))
	if err != nil {
		t.Fatalf("a claim with no owner does not marshal: %v", err)
	}
	if _, lerr := bson.Raw(raw).LookupErr("doc"); lerr == nil {
		t.Error("a claim with no owner carries a doc field, which claimIsLive would read as an owner to re-read")
	}
	for _, f := range []string{"_id", "ns", "idx", "at"} {
		if _, lerr := bson.Raw(raw).LookupErr(f); lerr != nil {
			t.Errorf("the row is missing %s", f)
		}
	}

	// With an owner it is recorded, because that is what makes a claim
	// resolvable by re-reading the document that made it.
	owner := bson.RawValue{Type: bson.TypeInt32, Value: []byte{7, 0, 0, 0}}
	raw, err = bson.Marshal(claimRow("k", "db.coll", "email_1", owner, now))
	if err != nil {
		t.Fatalf("a claim with an owner does not marshal: %v", err)
	}
	got, lerr := bson.Raw(raw).LookupErr("doc")
	if lerr != nil {
		t.Fatal("a claim with an owner did not record it")
	}
	if !got.Equal(owner) {
		t.Errorf("recorded owner %v, want %v", got, owner)
	}
}

// The id a claim is written under has to be readable back into the fields it
// was claimed over, because that is what tells an index of the same name over
// different fields from the same index.
//
// Round-tripped rather than asserted against a literal, so that changing how a
// key id is built breaks this test instead of quietly turning the check that
// reads it into one that always says "unknown".
func TestKeyFieldsRoundTripThroughTheID(t *testing.T) {
	cases := []struct {
		name   string
		idx    uniqueIndex
		vals   []any
		fields []string
	}{
		{"one field", uniqueIndex{name: "email_1", fields: []string{"email"}},
			[]any{"a@b.c"}, []string{"email"}},
		{"compound, in order", uniqueIndex{name: "a_1_b_1", fields: []string{"a", "b"}},
			[]any{1, "x"}, []string{"a", "b"}},
		{"a dotted path", uniqueIndex{name: "p_1", fields: []string{"addr.zip"}},
			[]any{"1000"}, []string{"addr.zip"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, ok := keyIDFromValues("shop.orders", c.idx, c.vals)
			if !ok {
				t.Fatal("no id was built")
			}
			got := keyFieldsOf(id, "shop.orders|"+c.idx.name+"|")
			if !sameFields(got, c.fields) || len(got) != len(c.fields) {
				t.Fatalf("read back %v, want %v", got, c.fields)
			}
		})
	}
}

// A collection name may contain the separator, so the prefix is rebuilt rather
// than parsed. Measured here rather than reasoned: splitting this id on "|"
// gives a namespace of "shop.a" and an index of "b".
func TestKeyFieldsSurviveASeparatorInTheName(t *testing.T) {
	ns := "shop.a|b"
	idx := uniqueIndex{name: "email_1", fields: []string{"email"}}
	id, ok := keyIDFromValues(ns, idx, []any{"a@b.c"})
	if !ok {
		t.Fatal("no id was built")
	}
	if got := keyFieldsOf(id, ns+"|"+idx.name+"|"); len(got) != 1 || got[0] != "email" {
		t.Fatalf("read back %v, want [email]", got)
	}
	// And the wrong prefix, which is what parsing would produce, does not
	// decode into a field list that would be believed.
	if got := keyFieldsOf(id, "shop.a|b|"); len(got) == 1 && got[0] == "email" {
		t.Fatal("a prefix built by parsing read back the right fields, so this test proves nothing")
	}
}

func TestSameFields(t *testing.T) {
	cases := []struct {
		name          string
		live, claimed []string
		want          bool
	}{
		{"the same one", []string{"a"}, []string{"a"}, true},
		{"a different one", []string{"a"}, []string{"b"}, false},
		{"reordered, which is a different index to mongod",
			[]string{"a", "b"}, []string{"b", "a"}, false},
		{"narrowed", []string{"a"}, []string{"a", "b"}, false},
		{"unknown, which must not warn", []string{"a"}, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := sameFields(c.live, c.claimed); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

// Whether the archive can answer the question a write asks of it is decided by
// field names, not by a rendered key document.
//
// The first version compared bson.Raw.String() of mongod's stored key against
// one marshalled here. mongod picks its own numeric type for a direction and Go
// picks another, so that comparison is about spelling: an index the archive has
// reads as missing and the operator is told to build what they already have.
func TestServedByAnArchiveIndex(t *testing.T) {
	cases := []struct {
		name   string
		fields []string
		have   [][]string
		want   bool
	}{
		{"exactly it", []string{"email"}, [][]string{{"_id"}, {"email"}}, true},
		{"nothing like it", []string{"email"}, [][]string{{"_id"}, {"name"}}, false},
		{"no indexes at all", []string{"email"}, nil, false},
		{"a compound index it leads", []string{"a"}, [][]string{{"a", "b"}}, true},
		{"a compound index it does not lead", []string{"b"}, [][]string{{"a", "b"}}, false},
		{"the compound itself", []string{"a", "b"}, [][]string{{"a", "b", "c"}}, true},
		{"the same fields the other way round, which cannot seek",
			[]string{"a", "b"}, [][]string{{"b", "a"}}, false},
		{"longer than anything there", []string{"a", "b"}, [][]string{{"a"}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := servedByAnArchiveIndex(c.fields, c.have); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}
