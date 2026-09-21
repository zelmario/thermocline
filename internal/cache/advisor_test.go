package cache

import (
	"strconv"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
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

// A driver asks config.collections constantly and the answers are perfectly
// stable, so they win a ranking meant to tell an operator which of *their*
// queries to cache. Measured on the pilot: the top twenty rows were that one
// shape, twenty times, and no application query appeared at all.
func TestAdvisorIgnoresHousekeeping(t *testing.T) {
	for _, ns := range []string{
		"config.collections", "admin.system.users", "local.oplog.rs", meta.DB + ".watermarks",
	} {
		if !Housekeeping(ns) {
			t.Errorf("%s should not be advised on", ns)
		}
	}
	for _, ns := range []string{"app.orders", "sample_mflix.comments", "configuration.things"} {
		if Housekeeping(ns) {
			t.Errorf("%s is the application's and must be advised on", ns)
		}
	}

	a := NewAdvisor(100, 100, time.Minute)
	// On a clock this test moves, not the wall clock it used to run on. Ten
	// observations in a tight loop span microseconds, and since the cache grew
	// a MinTTL floor the advisor will not recommend a period that short, so the
	// row this test looks for disappeared for a reason that has nothing to do
	// with housekeeping. Seconds apart is what an application's repeated query
	// actually looks like.
	k := &clock{t: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)}
	a.now = k.now
	for i := 0; i < 10; i++ {
		a.Observe("find config.collections filter={_id:?}", "config.collections", 1, 7)
		a.Observe("find app.orders filter={customer:?}", "app.orders", 1, 7)
		k.add(time.Second)
	}
	rows := a.Report(10)
	for _, r := range rows {
		if r.NS == "config.collections" {
			t.Fatalf("the report advises on %s, which crowds out the queries an operator can act on", r.NS)
		}
	}
	if len(rows) == 0 {
		t.Fatal("the application's query was dropped along with the housekeeping")
	}
}

// What the advisor costs once it is full, which is the state a real deployment
// reaches and no benchmark had measured.
//
// evictShapeIfFull picks the oldest shape by scanning every shape it holds, and
// it runs on every insert once the table is at max_queries. So a workload with
// more distinct shapes than the cap pays a full scan per cacheable read, and
// the default cap is 1000. The advisor runs by default, whether or not anything
// is cached, so this is paid by deployments that never turn the cache on.
//
// Run as: a cap of 1000, then observe shapes that are always new.
func BenchmarkAdvisorObserveWhenFull(b *testing.B) {
	const cap = 1000
	a := NewAdvisor(cap, 50, time.Hour)
	// Fill it, so every Observe below evicts.
	for i := 0; i < cap; i++ {
		a.Observe("shape"+strconv.Itoa(i), "shop.orders", uint64(i), uint64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Observe("new"+strconv.Itoa(i), "shop.orders", uint64(i), uint64(i))
	}
}

// And the same advisor when it is not full, which is what every existing
// measurement of this code was taken on.
func BenchmarkAdvisorObserveWhenNotFull(b *testing.B) {
	a := NewAdvisor(1<<20, 50, time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Observe("new"+strconv.Itoa(i), "shop.orders", uint64(i), uint64(i))
	}
}

// The advisor must not recommend what the cache would refuse to hold.
//
// MinTTL was added to the cache on 2026-09-18: an answer whose observed
// stability is shorter than a millisecond is learned from and not stored,
// because serving a hit costs 365us and the entry would expire before anything
// could read it. The advisor kept suggesting those, so `cline cache` could list
// a query as worth caching for 180 microseconds, and turning the cache on for
// it would hold nothing.
//
// Two places deciding one thing, which is the shape that drifts.
func TestAdvisorDoesNotSuggestWhatTheCacheWillNotHold(t *testing.T) {
	a := NewAdvisor(100, 50, time.Hour)
	k := &clock{t: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)}
	a.now = k.now

	// A query that keeps giving the same answer, looked at twice a few hundred
	// nanoseconds apart: real repetition, far too brief to be worth holding.
	a.Observe("find shop.orders {a: ?}", "shop.orders", 1, 99)
	k.add(200 * time.Nanosecond)
	a.Observe("find shop.orders {a: ?}", "shop.orders", 1, 99)

	for _, adv := range a.Report(10) {
		if adv.Suggested < MinTTL {
			t.Errorf("advised caching %q for %v, which is below the cache's own floor of %v",
				adv.Shape, adv.Suggested, MinTTL)
		}
	}

	// The control, in the same advisor: a query stable for a minute is still
	// advised, or the floor has swallowed the feature.
	a2 := NewAdvisor(100, 50, time.Hour)
	k2 := &clock{t: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)}
	a2.now = k2.now
	a2.Observe("find shop.totals {}", "shop.totals", 7, 42)
	k2.add(time.Minute)
	a2.Observe("find shop.totals {}", "shop.totals", 7, 42)
	if len(a2.Report(10)) == 0 {
		t.Error("a query stable for a minute produced no advice at all")
	}
}

// What one eviction costs, as the cap grows. The other two benchmarks measure
// the average over many inserts, which is the throughput question and is fine:
// a batch clears a tenth of the table, so a hundred inserts pay one scan.
//
// This is the other question, and it is the one an operator creates. Eviction
// builds a slice of every shape and sorts it, holding the advisor's lock, and
// every read on the router waits behind that. `router.cache.max_queries` is a
// number in a file with no upper bound on it, and "cache more" is the obvious
// thing to type.
//
// So this times the single Observe that evicts, with the refill outside the
// timer, at caps an operator might actually write.
func BenchmarkAdvisorEvictionPause(b *testing.B) {
	for _, cap := range []int{1000, 10000, 100000, 1000000} {
		b.Run(strconv.Itoa(cap), func(b *testing.B) {
			a := NewAdvisor(cap, 50, time.Hour)
			for i := 0; i < cap; i++ {
				a.Observe("shape"+strconv.Itoa(i), "shop.orders", uint64(i), uint64(i))
			}
			seq := cap
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// The eviction dropped a tenth, so put a tenth back before
				// timing the insert that fills it again. Outside the timer,
				// because the refill is not what is being measured.
				b.StopTimer()
				for len(a.shapes) < cap-1 {
					seq++
					a.Observe("fill"+strconv.Itoa(seq), "shop.orders", uint64(seq), uint64(seq))
				}
				seq++
				b.StartTimer()
				a.Observe("evict"+strconv.Itoa(seq), "shop.orders", uint64(seq), uint64(seq))
			}
		})
	}
}
