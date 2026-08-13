package router

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestIDFromFilter(t *testing.T) {
	oid := bson.NewObjectID()

	tests := []struct {
		name   string
		filter bson.D
		want   bool
		why    string
	}{
		{"plain equality", bson.D{{Key: "_id", Value: oid}}, true,
			"the common shape of a lookup by id"},
		{"explicit $eq", bson.D{{Key: "_id", Value: bson.D{{Key: "$eq", Value: oid}}}}, true,
			"drivers and ORMs write it both ways"},
		{"equality with extra criteria", bson.D{
			{Key: "_id", Value: oid}, {Key: "status", Value: "open"}}, true,
			"still at most one document, so a miss can be resolved by id"},

		{"no _id at all", bson.D{{Key: "customerId", Value: 4711}}, false,
			"could match many documents, so a miss cannot be resolved this way"},
		{"$in matches several", bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{oid}}}}}, false,
			"a set of ids is not one document"},
		{"a range over _id", bson.D{
			{Key: "_id", Value: bson.D{{Key: "$gte", Value: oid}}}}, false,
			"a range is not one document"},
		{"empty filter", bson.D{}, false, "matches everything"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := rawDoc(t, tt.filter)
			_, got := idFromFilter(raw)
			if got != tt.want {
				t.Errorf("idFromFilter = %v, want %v: %s", got, tt.want, tt.why)
			}
		})
	}
}

// The completeness guard turns on this: can we prove the query only reaches
// into the range the fast tier holds completely? Reading no bound has to mean
// "unbounded", never "bounded at zero".
func TestLowerTimeBound(t *testing.T) {
	when := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	oid := bson.NewObjectIDFromTimestamp(when)

	tests := []struct {
		name   string
		filter bson.D
		want   bool
		why    string
	}{
		{"$gte on _id", bson.D{
			{Key: "_id", Value: bson.D{{Key: "$gte", Value: oid}}}}, true,
			"an ObjectId bound carries a timestamp"},
		{"$gt on a date field", bson.D{
			{Key: "createdAt", Value: bson.D{{Key: "$gt", Value: when}}}}, true,
			"an explicit date bound"},
		{"a bound plus other criteria", bson.D{
			{Key: "status", Value: "open"},
			{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: when}}}}, true,
			"the bound still applies to the whole query"},

		{"no temporal bound", bson.D{{Key: "status", Value: "open"}}, false,
			"this query could match anything, including archived documents"},
		{"only an upper bound", bson.D{
			{Key: "createdAt", Value: bson.D{{Key: "$lt", Value: when}}}}, false,
			"an upper bound says nothing about how far back the query reaches"},
		{"empty filter", bson.D{}, false, "matches everything"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := lowerTimeBound(rawDoc(t, tt.filter))
			if ok != tt.want {
				t.Fatalf("lowerTimeBound found=%v, want %v: %s", ok, tt.want, tt.why)
			}
			if ok && got.Before(when.Add(-time.Second)) {
				t.Errorf("bound = %v, want about %v", got, when)
			}
		})
	}
}

// With several bounds, the tightest one decides. Any of them landing inside the
// complete range makes the whole answer complete.
func TestLowerTimeBoundTakesTheTightest(t *testing.T) {
	early := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	filter := rawDoc(t, bson.D{
		{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: early}}},
		{Key: "updatedAt", Value: bson.D{{Key: "$gte", Value: late}}},
	})

	got, ok := lowerTimeBound(filter)
	if !ok {
		t.Fatal("no bound found")
	}
	if !got.Equal(late) {
		t.Errorf("bound = %v, want the tightest bound %v", got, late)
	}
}

func TestReplyOK(t *testing.T) {
	cases := []struct {
		val  any
		want bool
	}{
		{1.0, true}, {int32(1), true}, {int64(1), true},
		{0.0, false}, {int32(0), false},
	}
	for _, c := range cases {
		raw := rawDoc(t, bson.D{{Key: "ok", Value: c.val}})
		if got := replyOK(raw); got != c.want {
			t.Errorf("replyOK(ok=%v) = %v, want %v", c.val, got, c.want)
		}
	}
	if replyOK(rawDoc(t, bson.D{{Key: "errmsg", Value: "boom"}})) {
		t.Error("a reply with no ok field was treated as success")
	}
}

// A write that matched nothing is the signal that a document may be archived.
// Confusing "matched zero" with "field missing" would either miss real cases or
// invent them, so the two are kept distinct.
func TestMatched(t *testing.T) {
	cases := []struct {
		name string
		doc  bson.D
		want int
	}{
		{"matched one", bson.D{{Key: "n", Value: int32(1)}}, 1},
		{"matched none", bson.D{{Key: "n", Value: int32(0)}}, 0},
		{"n as int64", bson.D{{Key: "n", Value: int64(3)}}, 3},
		{"no n field", bson.D{{Key: "ok", Value: 1.0}}, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matched(rawDoc(t, c.doc)); got != c.want {
				t.Errorf("matched = %d, want %d", got, c.want)
			}
		})
	}
}

func TestFirstBatch(t *testing.T) {
	full := rawDoc(t, bson.D{{Key: "cursor", Value: bson.D{
		{Key: "id", Value: int64(0)},
		{Key: "firstBatch", Value: bson.A{bson.D{{Key: "_id", Value: 1}}}},
	}}})
	batch, ok := firstBatch(full)
	if !ok || len(batch) != 1 {
		t.Errorf("firstBatch = %d docs, ok=%v, want 1 doc", len(batch), ok)
	}

	empty := rawDoc(t, bson.D{{Key: "cursor", Value: bson.D{
		{Key: "id", Value: int64(0)},
		{Key: "firstBatch", Value: bson.A{}},
	}}})
	batch, ok = firstBatch(empty)
	if !ok {
		t.Fatal("an empty batch should still parse, it is the miss signal")
	}
	if len(batch) != 0 {
		t.Errorf("firstBatch = %d docs, want 0", len(batch))
	}
}
