package cache

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The example this was built from. A count of users in one country returns
// 3,512,345 for five minutes, then 3,512,344 for fifteen, then 3,512,346 for
// ten. The shortest stretch is five minutes, so the suggestion is four and a
// half: inside the evidence, never equal to it.
func TestTheWorkedExample(t *testing.T) {
	a := NewAdvisor(100, 100, time.Hour)
	k := &clock{t: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)}
	a.now = k.now

	const shape, ns = "count shop.users query={country:?}", "shop.users"
	const uy = uint64(0x55590000) // the parameter hash for "UY"; any stable value does

	run := func(result uint64, minutes int) {
		for i := 0; i < minutes; i++ {
			a.Observe(shape, ns, uy, result)
			k.add(time.Minute)
		}
	}
	run(3512345, 5)
	run(3512344, 15)
	run(3512346, 10)

	rows := a.Report(10)
	if len(rows) != 1 {
		t.Fatalf("advice rows = %d, want 1", len(rows))
	}
	got := rows[0]
	if got.Stable != 5*time.Minute {
		t.Errorf("stable = %s, want 5m", got.Stable)
	}
	if want := 4*time.Minute + 30*time.Second; got.Suggested != want {
		t.Errorf("suggested = %s, want %s", got.Suggested, want)
	}
	if got.Changes != 2 {
		t.Errorf("changes = %d, want 2", got.Changes)
	}
}

// A shape asked once with each of many values has no repetition to exploit, and
// saying so is as useful as a suggestion.
func TestAShapeWithNoRepeatsIsNotAdvised(t *testing.T) {
	a := NewAdvisor(100, 100, time.Hour)
	k := &clock{t: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)}
	a.now = k.now

	for i := 0; i < 50; i++ {
		a.Observe("find shop.orders filter={_id:?}", "shop.orders", uint64(i), uint64(i))
		k.add(time.Second)
	}
	if rows := a.Report(10); len(rows) != 0 {
		t.Fatalf("advised %d queries that never repeated", len(rows))
	}
}

// Ranking, because the point is to find the queries that would benefit most.
// A query asked constantly whose answer holds a minute beats a rare one whose
// answer holds an hour.
func TestTheReportRanksByWhatWouldBeSaved(t *testing.T) {
	a := NewAdvisor(100, 100, 2*time.Hour)
	k := &clock{t: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)}
	a.now = k.now

	for i := 0; i < 200; i++ {
		a.Observe("count shop.hot query={}", "shop.hot", 1, 7)
		k.add(time.Second)
	}
	for i := 0; i < 2; i++ {
		a.Observe("count shop.cold query={}", "shop.cold", 1, 9)
		k.add(time.Hour)
	}
	rows := a.Report(10)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].NS != "shop.hot" {
		t.Errorf("first row is %s, want shop.hot: it is asked constantly", rows[0].NS)
	}
}

// The shape is what makes two executions the same query, and the parameters are
// what makes them different.
func TestShapeSeparatesTheQueryFromItsValues(t *testing.T) {
	raw := func(d bson.D) bson.Raw {
		b, err := bson.Marshal(d)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}
	uy := raw(bson.D{
		{Key: "count", Value: "users"},
		{Key: "query", Value: bson.D{{Key: "country", Value: "UY"}}},
		{Key: "$db", Value: "shop"},
	})
	ar := raw(bson.D{
		{Key: "count", Value: "users"},
		{Key: "query", Value: bson.D{{Key: "country", Value: "AR"}}},
		{Key: "$db", Value: "shop"},
	})
	other := raw(bson.D{
		{Key: "count", Value: "users"},
		{Key: "query", Value: bson.D{{Key: "city", Value: "UY"}}},
		{Key: "$db", Value: "shop"},
	})

	s1, p1 := Shape("count", "shop.users", uy)
	s2, p2 := Shape("count", "shop.users", ar)
	s3, _ := Shape("count", "shop.users", other)

	if s1 != s2 {
		t.Errorf("the same query with different values got different shapes:\n%s\n%s", s1, s2)
	}
	if p1 == p2 {
		t.Error("different values hashed the same")
	}
	if s1 == s3 {
		t.Error("filtering on a different field is a different query")
	}
	if want := "count shop.users query={country:?}"; s1 != want {
		t.Errorf("shape = %q, want %q", s1, want)
	}
	// The session is not part of the question.
	withSession := raw(bson.D{
		{Key: "count", Value: "users"},
		{Key: "query", Value: bson.D{{Key: "country", Value: "UY"}}},
		{Key: "lsid", Value: bson.D{{Key: "id", Value: "abc"}}},
		{Key: "$clusterTime", Value: bson.D{{Key: "t", Value: int64(7)}}},
		{Key: "$db", Value: "shop"},
	})
	if s4, _ := Shape("count", "shop.users", withSession); s4 != s1 {
		t.Errorf("the session changed the shape:\n%s\n%s", s1, s4)
	}
}

// The dashboard asks twice a second and this walks every shape and every
// parameter set, then sorts them. A report a second old is the same report.
func TestTheReportIsNotRecomputedForEveryLook(t *testing.T) {
	a := NewAdvisor(100, 100, time.Hour)
	k := &clock{t: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)}
	a.now = k.now

	for i := 0; i < 4; i++ {
		a.Observe("count shop.users query={}", "shop.users", 1, 7)
		k.add(time.Second)
	}
	first := a.Report(10)
	if len(first) != 1 {
		t.Fatalf("rows = %d, want 1", len(first))
	}

	// Nothing observed, no time passed: the same rows come back without the
	// work being done again.
	if a.cached == nil {
		t.Fatal("the report was not held at all")
	}
	held := a.cached
	_ = a.Report(10)
	if &held[0] != &a.cached[0] {
		t.Error("the report was worked out again for a second look in the same second")
	}

	// A new observation makes it stale, because a report that cannot change is
	// worse than one that costs something.
	a.Observe("count shop.users query={}", "shop.users", 1, 8)
	if a.cached != nil {
		t.Error("an observation left a stale report standing")
	}

	// And a second later it is worked out again regardless.
	_ = a.Report(10)
	k.add(2 * time.Second)
	before := a.cachedAt
	_ = a.Report(10)
	if !a.cachedAt.After(before) {
		t.Error("the report was never refreshed after it went stale")
	}
}
