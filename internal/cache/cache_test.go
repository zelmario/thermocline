package cache

import (
	"strconv"
	"testing"
	"time"
)

// clock lets a test move time the way the cache experiences it, since every
// decision here is about how long something held still.
type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }

func newTestCache(max int, maxTTL time.Duration) (*Cache, *clock) {
	c := New(max, maxTTL)
	k := &clock{t: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)}
	c.now = k.now
	return c, k
}

// A query seen once has no evidence behind it, and evidence is the whole idea.
func TestNothingIsCachedWithoutEvidence(t *testing.T) {
	c, _ := newTestCache(10, time.Hour)
	c.Observe("q", "shop.orders", []byte("3512345"))
	if _, ok := c.Get("q"); ok {
		t.Fatal("a query seen once was cached")
	}
	if ttl := c.TTLOf("q"); ttl != 0 {
		t.Fatalf("ttl = %s, want 0", ttl)
	}
}

// A query whose answer never changes is the one a cache exists for, and the
// first version of this never cached it: it learned only from changes. How long
// the answer in hand has already held is evidence too.
func TestAnAnswerThatNeverChangesIsStillLearned(t *testing.T) {
	c, k := newTestCache(10, time.Hour)
	c.Observe("q", "shop.orders", []byte("3512345"))
	k.add(10 * time.Minute)
	c.Observe("q", "shop.orders", []byte("3512345"))

	if got, want := c.TTLOf("q"), 9*time.Minute; got != want {
		t.Fatalf("ttl = %s, want %s", got, want)
	}
	if _, ok := c.Get("q"); !ok {
		t.Error("an answer that had held for ten minutes was not cached")
	}
}

// And a change still counts against it, even a much later one.
func TestAShortLifeLowersATTLLearnedFromALongOne(t *testing.T) {
	c, k := newTestCache(10, time.Hour)
	c.Observe("q", "shop.orders", []byte("a"))
	k.add(time.Hour)
	c.Observe("q", "shop.orders", []byte("a"))
	if c.TTLOf("q") != 54*time.Minute {
		t.Fatalf("ttl = %s, want 54m", c.TTLOf("q"))
	}
	// It changes, then changes again a second later.
	c.Observe("q", "shop.orders", []byte("b"))
	k.add(time.Second)
	c.Observe("q", "shop.orders", []byte("c"))
	if got := c.TTLOf("q"); got > time.Second {
		t.Fatalf("ttl = %s, want no more than the second it held", got)
	}
}

// The worked example: a count that holds for five minutes should be cached for
// four and a half.
func TestTTLIsNineTenthsOfWhatWasObserved(t *testing.T) {
	c, k := newTestCache(10, time.Hour)
	c.Observe("q", "shop.orders", []byte("3512345"))
	k.add(5 * time.Minute)
	c.Observe("q", "shop.orders", []byte("3512345"))
	// It changed, so five minutes of stability is now evidence.
	c.Observe("q", "shop.orders", []byte("3512344"))

	if got, want := c.TTLOf("q"), 4*time.Minute+30*time.Second; got != want {
		t.Fatalf("ttl = %s, want %s", got, want)
	}
	reply, ok := c.Get("q")
	if !ok || string(reply) != "3512344" {
		t.Fatalf("cached = %q, %v", reply, ok)
	}
	k.add(4 * time.Minute)
	if _, ok := c.Get("q"); !ok {
		t.Error("the entry expired before its learned ttl")
	}
	k.add(time.Minute)
	if _, ok := c.Get("q"); ok {
		t.Error("the entry outlived its learned ttl")
	}
}

// A query that usually holds for an hour and sometimes for a second is a query
// that changes every second, as far as anybody reading it is concerned.
func TestTheShortestObservationWins(t *testing.T) {
	c, k := newTestCache(10, time.Hour)
	c.Observe("q", "shop.orders", []byte("a"))
	k.add(time.Hour)
	c.Observe("q", "shop.orders", []byte("a"))
	c.Observe("q", "shop.orders", []byte("b"))
	k.add(2 * time.Second)
	c.Observe("q", "shop.orders", []byte("b"))
	c.Observe("q", "shop.orders", []byte("c"))

	if got, want := c.TTLOf("q"), 1800*time.Millisecond; got != want {
		t.Fatalf("ttl = %s, want %s", got, want)
	}
}

// A ceiling, because a query that has not changed since the router started is
// not proof that it never will.
func TestTheCeilingApplies(t *testing.T) {
	c, k := newTestCache(10, time.Minute)
	c.Observe("q", "shop.orders", []byte("a"))
	k.add(10 * time.Hour)
	c.Observe("q", "shop.orders", []byte("a"))
	c.Observe("q", "shop.orders", []byte("b"))
	if got := c.TTLOf("q"); got != time.Minute {
		t.Fatalf("ttl = %s, want the ceiling of 1m", got)
	}
}

// A write through the router is not a timer running out, it is the answer
// becoming wrong. The entry goes immediately, and so does what was learned:
// stability measured before a write says nothing about after it.
func TestAWriteDropsTheAnswerAndTheEvidence(t *testing.T) {
	c, k := newTestCache(10, time.Hour)
	c.Observe("q", "shop.orders", []byte("a"))
	k.add(5 * time.Minute)
	c.Observe("q", "shop.orders", []byte("a"))
	c.Observe("q", "shop.orders", []byte("b"))
	if _, ok := c.Get("q"); !ok {
		t.Fatal("nothing was cached to invalidate")
	}

	if n := c.Invalidate("shop.orders"); n != 1 {
		t.Errorf("invalidated %d entries, want 1", n)
	}
	if _, ok := c.Get("q"); ok {
		t.Error("the answer survived a write to its collection")
	}
	if ttl := c.TTLOf("q"); ttl != 0 {
		t.Errorf("the learned ttl survived a write: %s", ttl)
	}
	// And a write to somebody else's collection leaves it alone.
	c.Observe("q2", "shop.other", []byte("x"))
	k.add(time.Minute)
	c.Observe("q2", "shop.other", []byte("x"))
	c.Observe("q2", "shop.other", []byte("y"))
	c.Invalidate("shop.orders")
	if _, ok := c.Get("q2"); !ok {
		t.Error("a write to one collection dropped another's answer")
	}
}

// Bounded, because a router serving ten thousand distinct queries must not hold
// ten thousand answers.
func TestItStaysWithinItsBound(t *testing.T) {
	c, k := newTestCache(3, time.Hour)
	for _, q := range []string{"a", "b", "c", "d", "e"} {
		c.Observe(q, "shop.orders", []byte(q))
		k.add(time.Second)
	}
	if got := c.Len(); got > 3 {
		t.Fatalf("holding %d queries, want at most 3", got)
	}
}

// Counting entries is not a memory bound. A thousand replies of a megabyte each
// is a gigabyte, and nothing in a count says which of those it is.
func TestTheMemoryBudgetIsInBytes(t *testing.T) {
	big := make([]byte, 200*1024)
	c, k := newTestCache(1000, time.Hour)
	c.maxBytes = 1024 * 1024 // room for five of them

	for i := 0; i < 20; i++ {
		key := "q" + string(rune('a'+i))
		c.Observe(key, "shop.orders", big)
		k.add(time.Second)
		c.Observe(key, "shop.orders", big) // held a second, so now it is cached
		k.add(time.Second)
	}
	if got := c.Bytes(); got > 1024*1024 {
		t.Fatalf("holding %d bytes, over the budget of %d", got, 1024*1024)
	}
	if c.Bytes() == 0 {
		t.Fatal("holding nothing at all, which is a different bug")
	}
}

// One reply larger than the whole budget must not be held, and must not stop
// the query being learned from either.
func TestAnOversizedAnswerIsLearnedFromButNotHeld(t *testing.T) {
	huge := make([]byte, MaxEntryBytes+1)
	c, k := newTestCache(10, time.Hour)

	c.Observe("q", "shop.orders", huge)
	k.add(time.Minute)
	c.Observe("q", "shop.orders", huge)

	if _, ok := c.Get("q"); ok {
		t.Error("an oversized answer was held")
	}
	if c.Bytes() != 0 {
		t.Errorf("holding %d bytes of something too large to hold", c.Bytes())
	}
	if c.TTLOf("q") == 0 {
		t.Error("nothing was learned about a query just because its answer is large")
	}
	if c.Rejected == 0 {
		t.Error("the rejection was not counted, so an idle-looking cache says nothing about why")
	}
}

// The collections an answer depends on are not always known the first time it
// is seen.
//
// cacheDeps works them out by reading the catalog: the view chain the query
// runs through, what those views join, what the pipeline joins. Every one of
// those reads can come back short. A listCollections that fails leaves a view
// looking like an ordinary collection, and the answer is then hung from the
// namespace the command named and nothing else.
//
// The router asks again on the next read of the same query and gets the fuller
// set, and until this it made no difference: the index was written once, when
// the key was new, and never revisited. A write to the collection discovered
// second did not drop the entry, so the cache went on serving an answer the
// data behind it had moved out from under.
func TestADependencyLearnedLaterStillInvalidates(t *testing.T) {
	c, k := newTestCache(10, time.Hour)

	// First look: only the collection the command named, which is what a
	// catalog read that came back short leaves behind.
	c.ObserveResultOn("q", []string{"shop.v"}, []byte("a"), 1)
	k.add(10 * time.Minute)
	// Second look: the view's base is known this time.
	c.ObserveResultOn("q", []string{"shop.v", "shop.orders"}, []byte("a"), 1)

	if _, ok := c.Get("q"); !ok {
		t.Fatal("the answer was not cached, so this test proves nothing")
	}
	if n := c.Invalidate("shop.orders"); n != 1 {
		t.Fatalf("a write to shop.orders dropped %d entries, want 1", n)
	}
	if _, ok := c.Get("q"); ok {
		t.Error("the cache still answered a query whose data had just been written")
	}
}

// byKey is bounded by max_queries and byNS was bounded by nothing.
//
// Invalidate deletes the entry itself, and leaving the reference behind is safe
// because a reference that finds nothing does nothing. Safe is not free: the
// key stays in the set of every other namespace it was hung from, for a query
// that may never be asked again, and those sets are the one thing here that no
// setting bounds. A router answering many distinct queries against a collection
// that is written to accumulates them for as long as it runs.
func TestInvalidateLeavesNoDeadReferences(t *testing.T) {
	c, k := newTestCache(1000, time.Hour)

	for i := 0; i < 100; i++ {
		key := string(rune('a'+i%26)) + string(rune('a'+i/26))
		c.ObserveResultOn(key, []string{"shop.v", "shop.orders"}, []byte("a"), 1)
		k.add(time.Minute)
		c.ObserveResultOn(key, []string{"shop.v", "shop.orders"}, []byte("a"), 1)
	}
	c.Invalidate("shop.orders")

	if n := c.Len(); n != 0 {
		t.Fatalf("%d queries still watched after the write, want 0", n)
	}
	if n := c.refs(); n != 0 {
		t.Errorf("%d dead references left behind, want 0", n)
	}
}

// A cache told to hold nothing holds the default instead, rather than holding
// nothing at all.
//
// config.Load fills MaxBytes in before it unmarshals, so a file that turns the
// cache on without naming a budget still gets 64 MB and this guard never fires
// for it. A Config built in code has no such help, and the failure it prevents
// is the quiet one: a cache that is on, reports itself on, and stores nothing
// because every answer is larger than its budget.
func TestZeroByteBudgetMeansTheDefault(t *testing.T) {
	for _, budget := range []int{0, -1, -(1 << 20)} {
		c := NewWithBytes(100, budget, time.Minute)
		if c.maxBytes != DefaultMaxBytes {
			t.Errorf("NewWithBytes(budget=%d) kept maxBytes=%d, want the default %d",
				budget, c.maxBytes, DefaultMaxBytes)
		}
	}
	// And a budget that was asked for is honoured, or the guard could swallow
	// every value and still pass the cases above.
	if c := NewWithBytes(100, 4096, time.Minute); c.maxBytes != 4096 {
		t.Errorf("NewWithBytes(budget=4096) kept maxBytes=%d, want 4096", c.maxBytes)
	}
}

// An entry whose TTL is shorter than the work of storing it can never be read,
// and storing it is pure cost: the whole reply is copied, the byte accounting
// moves, and evictForBytes may then walk every key in the cache.
//
// It happens whenever the second sighting of a query follows the first closely,
// which two connections of a pool asking the same question at the same moment
// do by definition. The TTL is derived from how long the answer has been seen
// to hold, and at that point it has held for the gap between the two reads.
//
// Found by a router test that failed once in a few hundred runs with "nothing
// was cached, so this test proves nothing": under -count it ran fast enough
// that the gap was a few hundred nanoseconds, and the entry expired between
// being stored and being read.
func TestAnEntryTooShortLivedToReadIsNotStored(t *testing.T) {
	c, k := newTestCache(10, time.Hour)
	reply := []byte("an answer worth 9 bytes")

	c.Observe("q", "shop.orders", reply)
	k.add(200 * time.Nanosecond)
	c.Observe("q", "shop.orders", reply)

	if _, ok := c.Get("q"); ok {
		t.Error("a 200ns-old answer was served from the cache")
	}
	if c.Bytes() != 0 {
		t.Errorf("the cache is holding %d bytes for an answer nothing can read", c.Bytes())
	}

	// And the control: the same query with a gap worth caching is held.
	c2, k2 := newTestCache(10, time.Hour)
	c2.Observe("q", "shop.orders", reply)
	k2.add(time.Minute)
	c2.Observe("q", "shop.orders", reply)
	if _, ok := c2.Get("q"); !ok {
		t.Error("an answer that held for a minute was not cached")
	}
}

// What the MinTTL floor saves, on the shape that produced it: the second
// sighting of a query arriving close behind the first, which is what two
// connections of a pool asking the same question at one moment look like.
//
// The real clock on purpose. The gap is whatever the machine makes it, which is
// the point: below the floor the reply is not copied and the byte accounting
// and eviction scan do not run.
func BenchmarkObserveSecondSightingCloseBehind(b *testing.B) {
	reply := make([]byte, 64*1024)
	for i := range reply {
		reply[i] = byte(i)
	}
	c := NewWithBytes(1000, 64<<20, time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := strconv.Itoa(i)
		c.Observe(k, "shop.orders", reply)
		c.Observe(k, "shop.orders", reply)
	}
}
