package cache

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// How many times a query has to be asked before the cache answers one, and what
// it settles at afterwards.
//
// This is the number that decides whether the feature is worth turning on, and
// it was not written down anywhere. A pilot measured a 7% hit rate and the
// conclusion drawn was that the workload did not repeat itself. It did: it
// asked each question about three and a half times, and the first hit is the
// fourth ask. The workload stopped one short of the warm-up.
//
// Why three misses. The first ask has no evidence of anything and stores
// nothing. The second derives a TTL from the one interval it has seen, 90% of
// it, so the entry expires just before the third ask arrives. The third stores
// again with a TTL drawn from the longer span it has now watched, and that one
// reaches the fourth. From there the entry is re-validated whenever it expires,
// which is the single miss in each run of hits: a hit deliberately does not
// refresh the entry's age, or the TTL would stop bounding anything.
//
// Asserted as properties rather than as the exact string, which would break on
// any change to the policy without saying whether the change was bad. The
// pattern is printed so a reader can see the shape.
func TestHowLongBeforeTheCacheAnswers(t *testing.T) {
	const asks = 20
	const gap = 10 * time.Second

	// changeEvery 0 means the answer never changes; n means a new one every n
	// asks; 1 means it is different every time it is looked at.
	run := func(changeEvery int) (pattern string, hits int, firstHit int) {
		clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		c := NewWithBytes(100, DefaultMaxBytes, time.Hour)
		c.now = func() time.Time { return clock }

		firstHit = -1
		var sb strings.Builder
		for i := 0; i < asks; i++ {
			answer := []byte("v0")
			if changeEvery > 0 {
				answer = fmt.Appendf(nil, "v%d", i/changeEvery)
			}
			if _, ok := c.Get("k"); ok {
				hits++
				if firstHit < 0 {
					firstHit = i + 1
				}
				sb.WriteByte('H')
			} else {
				sb.WriteByte('.')
				c.Observe("k", "shop.orders", answer)
			}
			clock = clock.Add(gap)
		}
		return sb.String(), hits, firstHit
	}

	t.Run("a stable answer is served from the fourth ask", func(t *testing.T) {
		pattern, hits, first := run(0)
		t.Logf("never changes:     %s  %d/%d hits", pattern, hits, asks)
		if first != 4 {
			t.Errorf("first hit on ask %d, want 4: the warm-up is what a workload has to outlast", first)
		}
		// Well over half, and rising: the entry's TTL grows as the answer goes
		// on holding still.
		if hits < asks/2 {
			t.Errorf("%d hits of %d for an answer that never changes", hits, asks)
		}
	})

	t.Run("an answer that changes every look is never served", func(t *testing.T) {
		pattern, hits, _ := run(1)
		t.Logf("changes every ask: %s  %d/%d hits", pattern, hits, asks)
		if hits != 0 {
			t.Errorf("%d hits on a query whose answer is different every time it is looked at; "+
				"each of those served something the database had already replaced", hits)
		}
	})

	t.Run("an answer that holds for a while is served within it", func(t *testing.T) {
		pattern, hits, first := run(4)
		t.Logf("changes every 4th: %s  %d/%d hits", pattern, hits, asks)
		if first != 4 {
			t.Errorf("first hit on ask %d, want 4", first)
		}
		// The point of the estimate: a query whose answer holds for four looks
		// is nearly as cacheable as one that never changes, because the TTL is
		// drawn from how long the answer actually held.
		if hits < asks/2 {
			t.Errorf("%d hits of %d for an answer holding four asks at a time", hits, asks)
		}
	})
}
