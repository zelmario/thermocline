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
			if got := caughtUp(tc.at, tc.ts, read, tc.untilTS); got != tc.want {
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
// position record. A flat interval was the first version and it was wrong in
// both directions: two milliseconds against a feeder publishing every twenty is
// five hundred reads a second of one document for latency the cadence hides,
// and a feeder that promises nothing must not be polled as though it promised
// everything.
func TestPollIntervalPacesToTheFeeder(t *testing.T) {
	cases := []struct {
		name    string
		everyMS int64
		want    time.Duration
	}{
		{"a quarter of the cadence", 20, 5 * time.Millisecond},
		{"and of a slower one", 80, 20 * time.Millisecond},
		{"never below the floor", 4, checkpointPollMin},
		{"never above the ceiling", 10000, checkpointPollMax},
		{"unknown means the old interval", 0, checkpointPollMax},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &checkpointCache{everyMS: tc.everyMS}
			if got := c.pollInterval(); got != tc.want {
				t.Fatalf("pollInterval = %v, want %v", got, tc.want)
			}
		})
	}
}
