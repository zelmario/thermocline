package router

import (
	"testing"
	"time"

	"github.com/zelmario/thermocline/internal/watermark"
)

// Whether a claim read that just failed should be attempted again.
//
// It should not, and the number is why: one claim read against an address
// nothing answers on costs 30.002 seconds, the driver's default server
// selection timeout, which nothing here shortens. Before this, every tiered
// read during an archive outage paid that and was then refused.
func TestClaimReadFailedRecently(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		entry  watermarkEntry
		cached bool
		want   bool
	}{
		{"never failed", watermarkEntry{fetched: now.Add(-time.Hour)}, true, false},
		{"failed a moment ago", watermarkEntry{lastFail: now.Add(-time.Second)}, true, true},
		{"the last moment of the window", watermarkEntry{lastFail: now.Add(-completenessTTL + time.Millisecond)}, true, true},
		{"long enough ago to try again", watermarkEntry{lastFail: now.Add(-completenessTTL)}, true, false},
		{"nothing cached at all", watermarkEntry{lastFail: now.Add(-time.Second)}, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := claimReadFailedRecently(c.entry, c.cached, now); got != c.want {
				t.Fatalf("claimReadFailedRecently = %v, want %v", got, c.want)
			}
		})
	}
}

// What may be answered from a claim the archive can no longer be asked for
// again. router.claim_grace is the one setting here that trades a guarantee for
// availability, so the conditions on it are worth pinning one at a time.
func TestClaimWithinGrace(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	good := func(age time.Duration) watermarkEntry {
		return watermarkEntry{mark: watermark.Mark{Claimed: true}, known: true, fetched: now.Add(-age)}
	}

	cases := []struct {
		name   string
		entry  watermarkEntry
		cached bool
		grace  time.Duration
		want   bool
	}{
		{"off by default, however fresh the claim", good(time.Second), true, 0, false},
		{"a negative grace is off too", good(time.Second), true, -time.Minute, false},
		{"inside the ordinary TTL", good(time.Second), true, time.Minute, true},
		{"inside the grace, past the TTL", good(30 * time.Second), true, time.Minute, true},
		{"the last moment of the grace", good(completenessTTL + time.Minute - time.Millisecond), true, time.Minute, true},
		{"the grace has run out", good(completenessTTL + time.Minute), true, time.Minute, false},
		{"nothing cached, so nothing to hold over", good(time.Second), false, time.Minute, false},
		{
			// A router that started during the outage read the claim and
			// failed. That failure is not a claim, and answering from the zero
			// Mark would report every namespace unclaimed as if it were known.
			"the cached entry records a failed read",
			watermarkEntry{known: false, fetched: now.Add(-time.Second)},
			true, time.Minute, false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := claimWithinGrace(c.entry, c.cached, c.grace, now); got != c.want {
				t.Fatalf("claimWithinGrace = %v, want %v", got, c.want)
			}
		})
	}
}
