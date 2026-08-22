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
			{Key: "_id", Value: bson.D{{Key: "$gte", Value: oid}}}}, false,
			"this namespace ages by createdAt, so an _id bound is a bound in another dimension"},
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
			got, ok := lowerTimeBound(rawDoc(t, tt.filter), "createdAt", true)
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
//
// Among bounds on the age field. A tighter bound on some other date is not a
// tighter bound on the same thing, and taking it used to be how a query on an
// unrelated field was judged complete: see
// TestLowerTimeBoundOnlyReadsTheAgeField.
func TestLowerTimeBoundTakesTheTightest(t *testing.T) {
	early := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	// $gt is read after $gte, so both spellings on the age field are seen.
	filter := rawDoc(t, bson.D{
		{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: late}}},
		{Key: "updatedAt", Value: bson.D{{Key: "$gte", Value: early}}},
	})

	got, ok := lowerTimeBound(filter, "createdAt", true)
	if !ok {
		t.Fatal("no bound found")
	}
	if !got.Equal(late) {
		t.Errorf("bound = %v, want the age field's bound %v", got, late)
	}

	// And the other way round, which is the case that mattered: a tighter bound
	// on another field must not raise it.
	filter = rawDoc(t, bson.D{
		{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: early}}},
		{Key: "updatedAt", Value: bson.D{{Key: "$gte", Value: late}}},
	})
	got, ok = lowerTimeBound(filter, "createdAt", true)
	if !ok {
		t.Fatal("no bound found")
	}
	if !got.Equal(early) {
		t.Errorf("bound = %v, want the age field's own bound %v, not another field's", got, early)
	}
}

// The field a bound is read from decides whether it means anything.
//
// A completeness claim says the fast tier holds every document newer than an
// instant, measured the way the rules measure age. A bound on any other date is
// a bound in a different dimension.
//
// Measured on a collection tiered by createdAt, claimed complete for the last
// thirty days, holding one recent order on the fast tier and a two year old one
// in the archive, both shipped yesterday: {shippedAt: {$gte: two days ago}} was
// read as bounded, judged inside the claim, and answered 1 where the archive
// holds 2.
func TestLowerTimeBoundOnlyReadsTheAgeField(t *testing.T) {
	when := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	oid := bson.NewObjectIDFromTimestamp(when)
	bound := func(f bson.D) bson.Raw { return rawDoc(t, f) }

	byCreatedAt := bson.D{{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: when}}}}
	byShippedAt := bson.D{{Key: "shippedAt", Value: bson.D{{Key: "$gte", Value: when}}}}
	byID := bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: oid}}}}

	cases := []struct {
		name     string
		filter   bson.D
		ageField string
		known    bool
		want     bool
	}{
		{"the age field itself", byCreatedAt, "createdAt", true, true},
		{"another date field entirely", byShippedAt, "createdAt", true, false},
		{"an _id bound where age comes from a date field", byID, "createdAt", true, false},
		{"an _id bound where age comes from the _id", byID, "", true, true},
		{"a date field where age comes from the _id", byCreatedAt, "", true, false},
		{"nothing says how this namespace ages", byCreatedAt, "", false, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, got := lowerTimeBound(bound(c.filter), c.ageField, c.known)
			if got != c.want {
				t.Errorf("bounded = %v, want %v", got, c.want)
			}
		})
	}
}
