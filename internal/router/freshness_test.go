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
