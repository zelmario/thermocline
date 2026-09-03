package router

import (
	"strconv"
	"testing"
	"time"
)

// Which collections a starting router may drop as litter.
//
// A tiered $out assembles its result in a scratch collection inside the
// application's own database and renames it over the target. Every failing path
// drops it; a process that dies does not take one, so the collection stays for
// ever because nothing else looks at it. The name carries the nanosecond the
// aggregation started, which is what makes the age readable without trusting
// anything else, and the age is what keeps this from dropping another router's
// work in progress.
func TestScratchAge(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	stamp := func(d time.Duration) string {
		return "tmp.thermocline.out." + strconv.FormatInt(now.Add(-d).UnixNano(), 10)
	}

	cases := []struct {
		name    string
		coll    string
		wantAge time.Duration
		wantOK  bool
	}{
		{"one this router just made", stamp(time.Second), time.Second, true},
		{"one left an hour ago", stamp(time.Hour), time.Hour, true},
		{"one left a week ago", stamp(7 * 24 * time.Hour), 7 * 24 * time.Hour, true},
		// Anything without a readable stamp is left alone: it is either from a
		// version that named these differently or not ours, and dropping a
		// collection on a guess about its name is not a trade worth making.
		{"no stamp at all", "tmp.thermocline.out.", 0, false},
		{"a stamp that is not a number", "tmp.thermocline.out.later", 0, false},
		{"a negative stamp", "tmp.thermocline.out.-5", 0, false},
		{"the application's own collection", "orders", 0, false},
		{"something merely similar", "tmp.thermoclinex.out.1", 0, false},
		{"a collection the application called tmp", "tmp.orders", 0, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			age, ok := scratchAge(c.coll, now)
			if ok != c.wantOK {
				t.Fatalf("scratchAge(%q) ok = %v, want %v", c.coll, ok, c.wantOK)
			}
			if ok && age != c.wantAge {
				t.Errorf("age = %v, want %v", age, c.wantAge)
			}
		})
	}

	// And the threshold itself, which is the whole safety of this: a scratch
	// collection another router is filling right now must not be dropped.
	if age, _ := scratchAge(stamp(time.Minute), now); age >= scratchLeftoverAge {
		t.Errorf("a minute-old scratch collection reads as litter at %v", age)
	}
	if age, _ := scratchAge(stamp(2*time.Hour), now); age < scratchLeftoverAge {
		t.Errorf("a two hour old scratch collection does not read as litter at %v", age)
	}
}
