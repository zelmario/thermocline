package router

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/checkpoint"
)

// The bar a read has to clear before it is answered from the archive. The cases
// that matter are the ones where the two clocks disagree: the whole reason a
// cluster time was introduced is that the wall-clock reading is the feeder's
// and the one it is compared against is the router's.
func TestCaughtUp(t *testing.T) {
	read := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	before := read.Add(-time.Second)
	after := read.Add(time.Second)

	ts := func(sec, inc uint32) bson.Timestamp { return bson.Timestamp{T: sec, I: inc} }
	packed := func(sec, inc uint32) uint64 { return checkpoint.PackTS(ts(sec, inc)) }

	cases := []struct {
		name    string
		at      time.Time
		ts      bson.Timestamp
		untilTS uint64
		want    bool
	}{
		{"both bars cleared", after, ts(100, 5), packed(100, 5), true},
		{"the clock is behind", before, ts(100, 5), packed(100, 5), false},
		// The bug this was written for. The feeder's clock runs ahead of the
		// router's, so the wall-clock bar passes for a position that has not
		// mirrored the write this read is about. Before the cluster time was
		// required, this answered from an archive missing that write.
		{"the feeder's clock is ahead of its mirroring", after, ts(100, 4), packed(100, 5), false},
		{"mirroring is past the read's cluster time", after, ts(100, 6), packed(100, 5), true},
		{"the increment is what separates them", after, ts(100, 5), packed(100, 6), false},
		{"a later second clears an earlier one", after, ts(101, 1), packed(100, 9), true},
		// Both degradations, which must not stall an upgrade.
		{"a feeder from before this publishes no cluster time", after, bson.Timestamp{}, packed(100, 5), true},
		{"a router that has forwarded no reply has none to clear", after, ts(100, 4), 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := reading{at: tc.at, ts: tc.ts, known: true}
			if got := caughtUp(r, read, tc.untilTS, 0, false); got != tc.want {
				t.Fatalf("caughtUp = %v, want %v", got, tc.want)
			}
		})
	}
}

// The archive gets what is left of the caller's deadline, not the whole of it
// a second time. Measured before this: a client asking for 800ms waited 300ms
// for mirroring, the archive was handed 800ms of its own, and the answer came
// back successfully after 1117ms.
func TestWithRemainingTime(t *testing.T) {
	cmdWith := func(ms int64) bson.D {
		return bson.D{{Key: "find", Value: "c"}, {Key: "maxTimeMS", Value: ms}, {Key: "filter", Value: bson.D{}}}
	}
	maxTimeOf := func(d bson.D) interface{} {
		for _, e := range d {
			if e.Key == "maxTimeMS" {
				return e.Value
			}
		}
		return nil
	}

	t.Run("the wait is deducted", func(t *testing.T) {
		got := withRemainingTime(cmdWith(800), 800*time.Millisecond, 300*time.Millisecond)
		if maxTimeOf(got) != int64(500) {
			t.Fatalf("maxTimeMS = %v, want 500", maxTimeOf(got))
		}
	})

	// Zero would be read by MongoDB as "no limit", which is the opposite of
	// what an exhausted deadline means, so the floor is one millisecond and the
	// server decides.
	t.Run("an exhausted deadline never becomes no deadline", func(t *testing.T) {
		got := withRemainingTime(cmdWith(800), 800*time.Millisecond, 900*time.Millisecond)
		if maxTimeOf(got) != int64(1) {
			t.Fatalf("maxTimeMS = %v, want 1", maxTimeOf(got))
		}
	})

	t.Run("a caller who set no deadline is left alone", func(t *testing.T) {
		in := bson.D{{Key: "find", Value: "c"}}
		got := withRemainingTime(in, 0, 300*time.Millisecond)
		if maxTimeOf(got) != nil {
			t.Fatalf("a deadline appeared from nowhere: %v", maxTimeOf(got))
		}
	})
}

// The poller paces itself to the cadence the feeder promised, which is in the
// position record, because polling faster than the position changes cannot make
// it newer and the reads land on the tier this is trying to speed up. Measured
// on the shop: polling every 5ms against a 20ms cadence was worse than every
// 25ms on throughput, p50, p95 and p99 alike.
func TestPollIntervalPacesToTheFeeder(t *testing.T) {
	cases := []struct {
		name    string
		everyMS int64
		want    time.Duration
	}{
		{"the cadence itself", 20, 20 * time.Millisecond},
		{"and a slower one is clamped", 80, checkpointPollMax},
		{"never below the floor", 1, checkpointPollMin},
		{"never above the ceiling", 10000, checkpointPollMax},
		{"unknown means the old interval", 0, checkpointPollMax},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &checkpointCache{last: reading{everyMS: tc.everyMS}}
			if got := c.pollInterval(); got != tc.want {
				t.Fatalf("pollInterval = %v, want %v", got, tc.want)
			}
		})
	}
}

// The wall-clock bar compares an instant the feeder wrote against one this
// router took, and two hosts do not agree about what time it is.
//
// It is the direction that makes it worth fixing. A router running ahead of its
// feeder never clears the bar, so every read that falls through waits the whole
// archive_wait and then answers stale anyway: slow and stale, the pair the
// barrier exists to avoid, and the log line it produces is the one a genuinely
// lagging feeder produces. Nothing distinguishes them.
//
// Both instants are put in the archive's frame first. The feeder publishes how
// far ahead of the archive it is, this side measures the same for itself, and
// the archive is then a reference neither has to trust the other about.
func TestCaughtUpAcrossTwoClocks(t *testing.T) {
	read := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

	// Mirroring is genuinely up to date: in the archive's frame the feeder's
	// position is a second past the instant this read began.
	//
	//   archive     12:00:00      the reference
	//   router      12:00:05      five seconds ahead, so `read` reads 12:00:05
	//   feeder      11:59:58      two behind, so its position reads 11:59:59
	//
	// Raw, that is 11:59:59 against 12:00:05 and the bar never clears.
	const routerSkew = 5 * time.Second
	const feederSkew = -2 * time.Second
	position := read.Add(-routerSkew).Add(time.Second).Add(feederSkew)

	got := reading{at: position, known: true, skew: feederSkew, skewKnown: true}

	t.Run("with both offsets it clears", func(t *testing.T) {
		if !caughtUp(got, read, 0, routerSkew, true) {
			t.Fatalf("mirroring is a second past this read and the bar did not clear")
		}
	})

	t.Run("and a feeder that is genuinely behind still does not", func(t *testing.T) {
		behind := got
		behind.at = read.Add(-routerSkew).Add(-time.Second).Add(feederSkew)
		if caughtUp(behind, read, 0, routerSkew, true) {
			t.Fatalf("mirroring is a second short of this read and the bar cleared")
		}
	})

	t.Run("a feeder that publishes no offset is compared raw", func(t *testing.T) {
		// The upgrade case: correcting one side and not the other would be
		// worse than correcting neither.
		old := got
		old.skewKnown = false
		if caughtUp(old, read, 0, routerSkew, true) {
			t.Fatalf("one-sided correction was applied")
		}
	})

	t.Run("a router that has not measured yet compares raw", func(t *testing.T) {
		// Which is this defect: uncorrected, a healthy feeder never clears the
		// bar. It is asserted rather than left implied, because a correction
		// applied on one side only would be worse than none.
		if caughtUp(got, read, 0, 0, false) {
			t.Fatalf("the feeder's offset was applied on its own")
		}
	})

	t.Run("the cluster-time bar still binds", func(t *testing.T) {
		// Correcting the clocks does not weaken the sound bar: mirroring is
		// past this read by the wall clock and short of it by cluster time.
		withTS := got
		withTS.ts = bson.Timestamp{T: 100, I: 4}
		if caughtUp(withTS, read, checkpoint.PackTS(bson.Timestamp{T: 100, I: 5}), routerSkew, true) {
			t.Fatalf("a corrected wall clock cleared a read the cluster time says is not mirrored")
		}
	})
}
