package router

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The rule is about a name, and it took three attempts to write because each
// version was a list of the cases somebody thought of. These are the cases that
// were missing from one of those lists, kept as a table so the next widening
// has to answer them.

func TestReservedTopLevel(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		doc  bson.D
		want string
	}{
		{"an ordinary document", bson.D{{Key: "_id", Value: 1}, {Key: "n", Value: 2}}, ""},
		{"the tombstone mark", bson.D{{Key: "_id", Value: 1}, {Key: "_tcDeletedAt", Value: now}}, "_tcDeletedAt"},
		{"the body-dropped mark", bson.D{{Key: "_id", Value: 1}, {Key: "_tcBodyDropped", Value: true}}, "_tcBodyDropped"},
		// Any type hides a document, because the predicate that excludes
		// tombstones is {$exists: false}. A string, a number and an array all
		// count, and reading only for a date would have missed $inc and $push.
		{"the mark holding a string", bson.D{{Key: "_tcDeletedAt", Value: "no"}}, "_tcDeletedAt"},
		{"the mark holding a number", bson.D{{Key: "_tcDeletedAt", Value: 0}}, "_tcDeletedAt"},
		{"the mark holding an array", bson.D{{Key: "_tcDeletedAt", Value: bson.A{1}}}, "_tcDeletedAt"},
		// Nested is not the same field: no predicate here reads it.
		{"nested under another field", bson.D{{Key: "inner", Value: bson.D{{Key: "_tcDeletedAt", Value: now}}}}, ""},
		{"a name that merely starts the same", bson.D{{Key: "_tcDeletedAtX", Value: 1}}, ""},
		{"an empty document", bson.D{}, ""},
	}
	for _, c := range cases {
		got, bad := reservedTopLevel(raw(t, c.doc))
		if c.want == "" && bad {
			t.Errorf("%s: refused on %q, want allowed", c.name, got)
		}
		if c.want != "" && got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestReservedInUpdate(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		u    bson.D
		want bool
	}{
		{"$set of it", bson.D{{Key: "$set", Value: bson.D{{Key: "_tcDeletedAt", Value: now}}}}, true},
		{"$setOnInsert of it", bson.D{{Key: "$setOnInsert", Value: bson.D{{Key: "_tcDeletedAt", Value: now}}}}, true},
		// The two that the first allow-list missed, measured through the router
		// before the rule was widened: both wrote the mark and were accepted.
		{"$currentDate of it", bson.D{{Key: "$currentDate", Value: bson.D{{Key: "_tcDeletedAt", Value: true}}}}, true},
		{"$min of it", bson.D{{Key: "$min", Value: bson.D{{Key: "_tcDeletedAt", Value: now}}}}, true},
		{"$inc of it", bson.D{{Key: "$inc", Value: bson.D{{Key: "_tcDeletedAt", Value: 1}}}}, true},
		{"$push of it", bson.D{{Key: "$push", Value: bson.D{{Key: "_tcDeletedAt", Value: 1}}}}, true},
		// $rename names its target in the value, which is the one operator that
		// writes a field the keys never mention.
		{"$rename onto it", bson.D{{Key: "$rename", Value: bson.D{{Key: "v", Value: "_tcDeletedAt"}}}}, true},
		{"$rename away from it", bson.D{{Key: "$rename", Value: bson.D{{Key: "_tcDeletedAt", Value: "v"}}}}, false},
		// $unset is how the field comes off a document that has one, so it has
		// to stay allowed or the state has no way out.
		{"$unset of it", bson.D{{Key: "$unset", Value: bson.D{{Key: "_tcDeletedAt", Value: ""}}}}, false},
		{"an ordinary $set", bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "shipped"}}}}, false},
		{"an ordinary $inc", bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 1}}}}, false},
		// A replacement document is read at its top level, like an insert.
		{"a replacement carrying it", bson.D{{Key: "_id", Value: 1}, {Key: "_tcDeletedAt", Value: now}}, true},
		{"an ordinary replacement", bson.D{{Key: "_id", Value: 1}, {Key: "n", Value: 2}}, false},
	}
	for _, c := range cases {
		u := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: raw(t, c.u)}
		got, bad := reservedInUpdate(u)
		if bad != c.want {
			t.Errorf("%s: refused=%v (%q), want %v", c.name, bad, got, c.want)
		}
	}
}

func TestReservedInPipeline(t *testing.T) {
	now := time.Now()
	stages := func(d ...bson.D) bson.RawValue {
		arr := bson.A{}
		for _, s := range d {
			arr = append(arr, s)
		}
		b, err := bson.Marshal(bson.D{{Key: "p", Value: arr}})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		v, err := bson.Raw(b).LookupErr("p")
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		return v
	}
	cases := []struct {
		name string
		v    bson.RawValue
		want bool
	}{
		{"a pipeline $set of it", stages(bson.D{{Key: "$set", Value: bson.D{{Key: "_tcDeletedAt", Value: now}}}}), true},
		{"a pipeline $addFields of it", stages(bson.D{{Key: "$addFields", Value: bson.D{{Key: "_tcDeletedAt", Value: now}}}}), true},
		// $project can introduce a field as much as $set can, which the first
		// version of the stage reader did not consider.
		{"a pipeline $project of it", stages(bson.D{{Key: "$project", Value: bson.D{{Key: "_tcDeletedAt", Value: "$v"}}}}), true},
		{"an ordinary pipeline", stages(bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: 1}}}}), false},
		{"a later stage carrying it", stages(
			bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: 1}}}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "_tcDeletedAt", Value: now}}}}), true},
	}
	for _, c := range cases {
		got, bad := reservedInUpdate(c.v)
		if bad != c.want {
			t.Errorf("%s: refused=%v (%q), want %v", c.name, bad, got, c.want)
		}
	}
}
