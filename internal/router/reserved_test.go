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
		// An operator's keys are paths, so a dotted one writes the mark's own
		// name at the top level: {"_tcDeletedAt.x": 1} makes _tcDeletedAt a
		// subdocument, and the predicate that hides a tombstone is
		// {$exists: false}, which a subdocument satisfies as well as a date.
		{"$set of a path under it", bson.D{{Key: "$set", Value: bson.D{{Key: "_tcDeletedAt.x", Value: 1}}}}, true},
		{"$inc of a path under it", bson.D{{Key: "$inc", Value: bson.D{{Key: "_tcDeletedAt.n", Value: 1}}}}, true},
		{"$rename onto a path under it", bson.D{{Key: "$rename", Value: bson.D{{Key: "v", Value: "_tcDeletedAt.x"}}}}, true},
		{"$unset of a path under it", bson.D{{Key: "$unset", Value: bson.D{{Key: "_tcDeletedAt.x", Value: ""}}}}, false},
		// The prefix has to be the whole first path component, or an ordinary
		// field whose name starts with the same letters is refused.
		{"a field merely starting the same", bson.D{{Key: "$set", Value: bson.D{{Key: "_tcDeletedAtX", Value: 1}}}}, false},
		{"an ordinary dotted path", bson.D{{Key: "$set", Value: bson.D{{Key: "meta.x", Value: 1}}}}, false},
		// A replacement document is read at its top level, like an insert.
		{"a replacement carrying it", bson.D{{Key: "_id", Value: 1}, {Key: "_tcDeletedAt", Value: now}}, true},
		{"an ordinary replacement", bson.D{{Key: "_id", Value: 1}, {Key: "n", Value: 2}}, false},
		// An insert's and a replacement's keys are names, not paths: mongod
		// stores this as one field with a dot in its name, which the predicate
		// does not read. Refusing it would be this proxy inventing a rule.
		{"a replacement with a dotted name", bson.D{{Key: "_id", Value: 1}, {Key: "_tcDeletedAt.x", Value: 1}}, false},
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
		// A replacing stage says what the whole document becomes, so it writes
		// the field as surely as $set does. The two spellings take their
		// argument at different depths: $replaceWith takes the document, and
		// $replaceRoot wraps it in newRoot.
		{"a pipeline $replaceWith of it", stages(
			bson.D{{Key: "$replaceWith", Value: bson.D{
				{Key: "_id", Value: "$_id"},
				{Key: "_tcDeletedAt", Value: now}}}}), true},
		{"a pipeline $replaceRoot of it", stages(
			bson.D{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: bson.D{
				{Key: "_id", Value: "$_id"},
				{Key: "_tcDeletedAt", Value: now}}}}}}), true},
		{"an ordinary $replaceRoot", stages(
			bson.D{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: bson.D{
				{Key: "_id", Value: "$_id"},
				{Key: "seen", Value: 1}}}}}}), false},
		// newRoot need not be a literal document: an expression that evaluates
		// to one is the documented form, and $mergeObjects is how a pipeline
		// adds a field while keeping the rest.
		// $setField names the field it writes in a value rather than as a key,
		// which is the $rename shape again one level down.
		{"a $replaceWith setting the field by name", stages(
			bson.D{{Key: "$replaceWith", Value: bson.D{
				{Key: "$setField", Value: bson.D{
					{Key: "field", Value: "_tcDeletedAt"},
					{Key: "input", Value: "$$ROOT"},
					{Key: "value", Value: now}}}}}}), true},
		{"a $replaceWith setting an ordinary field", stages(
			bson.D{{Key: "$replaceWith", Value: bson.D{
				{Key: "$setField", Value: bson.D{
					{Key: "field", Value: "seen"},
					{Key: "input", Value: "$$ROOT"},
					{Key: "value", Value: 1}}}}}}), false},
		// $literal hands its argument through without evaluating it, so the
		// document it yields shows its keys exactly as a literal one does.
		// Mutation testing found this arm untested: removing it left the suite
		// green.
		{"a $replaceWith of a $literal document", stages(
			bson.D{{Key: "$replaceWith", Value: bson.D{
				{Key: "$literal", Value: bson.D{
					{Key: "_id", Value: 1},
					{Key: "_tcDeletedAt", Value: now}}}}}}), true},
		{"a $replaceWith of an ordinary $literal", stages(
			bson.D{{Key: "$replaceWith", Value: bson.D{
				{Key: "$literal", Value: bson.D{
					{Key: "_id", Value: 1},
					{Key: "seen", Value: 1}}}}}}), false},
		// An expression whose keys come from data is not read, and not refused
		// either. This pins the residue that docs/limitations.md names, so
		// changing it is a decision rather than an accident.
		{"a $replaceWith of a field path is not read", stages(
			bson.D{{Key: "$replaceWith", Value: "$sub"}}), false},
		{"a $replaceRoot merging it in", stages(
			bson.D{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: bson.D{
				{Key: "$mergeObjects", Value: bson.A{
					"$$ROOT",
					bson.D{{Key: "_tcDeletedAt", Value: now}}}}}}}}}), true},
	}
	for _, c := range cases {
		got, bad := reservedInUpdate(c.v)
		if bad != c.want {
			t.Errorf("%s: refused=%v (%q), want %v", c.name, bad, got, c.want)
		}
	}
}

// An upsert's filter is a write path, which is the part the first three
// versions of this guard all missed: they read the update and nothing else.
// mongod builds the document an upsert creates from the filter's equality
// fields, so the rule here is exactly "what survives into that document", and
// it is the same rule the unique key registry reads a filter by.
func TestReservedInUpsertFilter(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name   string
		filter bson.D
		want   string
	}{
		{"an ordinary filter", bson.D{{Key: "k", Value: 1}}, ""},
		{"equality on the mark", bson.D{{Key: "_tcDeletedAt", Value: now}}, "_tcDeletedAt"},
		{"equality on the body-dropped mark", bson.D{{Key: "_tcBodyDropped", Value: true}}, "_tcBodyDropped"},
		{"the $eq spelling", bson.D{{Key: "_tcDeletedAt", Value: bson.D{{Key: "$eq", Value: now}}}}, "_tcDeletedAt"},
		{"the mark beside ordinary fields", bson.D{{Key: "k", Value: 1}, {Key: "_tcDeletedAt", Value: now}}, "_tcDeletedAt"},
		// A range predicate is not carried into the created document, so
		// refusing it would be the guard asserting more than it guards
		// against. Proved against mongod: the upsert created {k: 3, a: 1}.
		{"a range predicate on it", bson.D{{Key: "_tcDeletedAt", Value: bson.D{{Key: "$gt", Value: now}}}}, ""},
		// And an $in of one value is not a range predicate, which is what this
		// case assumed when it sat here expecting "". Measured against mongod
		// 8.0 the same way the line above was: the upsert created the field,
		// so the mark went onto a live document and every read that fell
		// through to the archive then skipped it.
		{"an $in of one value on it", bson.D{{Key: "_tcDeletedAt", Value: bson.D{{Key: "$in", Value: bson.A{now}}}}}, "_tcDeletedAt"},
		{"an $in of two values on it", bson.D{{Key: "_tcDeletedAt", Value: bson.D{{Key: "$in", Value: bson.A{now, now}}}}}, ""},
		{"an $all of one value on it", bson.D{{Key: "_tcDeletedAt", Value: bson.D{{Key: "$all", Value: bson.A{now}}}}}, "_tcDeletedAt"},
		// $and is how an ORM writes almost every filter it generates.
		{"the mark inside an $and", bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "k", Value: 1}},
			bson.D{{Key: "_tcDeletedAt", Value: now}}}}}, "_tcDeletedAt"},
		{"an ordinary $and", bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "k", Value: 1}}, bson.D{{Key: "j", Value: 2}}}}}, ""},
		{"a nested field of that name", bson.D{{Key: "inner", Value: bson.D{{Key: "_tcDeletedAt", Value: now}}}}, ""},
		{"an empty filter", bson.D{}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := bson.Marshal(c.filter)
			if err != nil {
				t.Fatal(err)
			}
			got, bad := reservedInUpsertFilter(raw)
			if c.want == "" {
				if bad {
					t.Fatalf("refused %v for %s, and mongod does not write that field", c.filter, got)
				}
				return
			}
			if !bad || got != c.want {
				t.Fatalf("got %q %v, want %q true", got, bad, c.want)
			}
		})
	}
}

// $merge's own pipeline. whenMatched is a string in every ordinary merge and a
// list of stages when somebody wants to decide the update per document, and
// reading only the outer pipeline's stages saw neither.
func TestReservedInMergeWhenMatched(t *testing.T) {
	cases := []struct {
		name  string
		stage bson.D
		want  string
	}{
		{"whenMatched merge", bson.D{{Key: "$merge", Value: bson.D{
			{Key: "into", Value: "t"}, {Key: "whenMatched", Value: "merge"}}}}, ""},
		{"a pipeline setting the mark", bson.D{{Key: "$merge", Value: bson.D{
			{Key: "into", Value: "t"},
			{Key: "whenMatched", Value: bson.A{bson.D{{Key: "$set", Value: bson.D{{Key: "_tcDeletedAt", Value: "$$NOW"}}}}}}}}}, "_tcDeletedAt"},
		{"a pipeline adding the body-dropped mark", bson.D{{Key: "$merge", Value: bson.D{
			{Key: "into", Value: "t"},
			{Key: "whenMatched", Value: bson.A{bson.D{{Key: "$addFields", Value: bson.D{{Key: "_tcBodyDropped", Value: true}}}}}}}}}, "_tcBodyDropped"},
		{"the mark in the second stage of it", bson.D{{Key: "$merge", Value: bson.D{
			{Key: "into", Value: "t"},
			{Key: "whenMatched", Value: bson.A{
				bson.D{{Key: "$set", Value: bson.D{{Key: "ok", Value: 1}}}},
				bson.D{{Key: "$set", Value: bson.D{{Key: "_tcDeletedAt", Value: "$$NOW"}}}}}}}}}, "_tcDeletedAt"},
		{"an ordinary pipeline", bson.D{{Key: "$merge", Value: bson.D{
			{Key: "into", Value: "t"},
			{Key: "whenMatched", Value: bson.A{bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}}}}}, ""},
		// whenNotMatched only ever names a mode, and reading a string as a
		// pipeline is the shape that panics rather than the shape that misses.
		{"whenNotMatched insert", bson.D{{Key: "$merge", Value: bson.D{
			{Key: "into", Value: "t"}, {Key: "whenNotMatched", Value: "insert"}}}}, ""},
		{"a $merge naming its target as a string", bson.D{{Key: "$merge", Value: "t"}}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := bson.Marshal(c.stage)
			if err != nil {
				t.Fatal(err)
			}
			got, bad := reservedInStage(raw)
			if c.want == "" {
				if bad {
					t.Fatalf("refused %s, which writes no mark", got)
				}
				return
			}
			if !bad || got != c.want {
				t.Fatalf("got %q %v, want %q true", got, bad, c.want)
			}
		})
	}
}
