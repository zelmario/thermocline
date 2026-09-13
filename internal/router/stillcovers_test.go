package router

import (
	"testing"
	"time"

	"github.com/zelmario/thermocline/internal/watermark"
)

// A cursor outlives the decision that let the fast tier answer it, and this is
// the decision that is made again for every getMore: does the claim the cursor
// was authorised by still cover what it covered?
//
// Getting it wrong in one direction is the shape this whole file exists to
// stop: the fast tier answered the first batch because it was complete for the
// range, the evictor took the documents, and the rest of the cursor comes back
// short with no error on it. Measured when it was found, on forty documents
// with a claim covering all of history: the cursor delivered 3 of 40.
//
// testevict.sh covers the ordinary case end to end, and needs a gate, an
// evictor and forty documents to reach it. The boundary cases below are not
// among them, and each is one line here.
func TestStillCovers(t *testing.T) {
	t0 := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	opened := nsClaim{ns: "shop.orders", from: t0, claimed: true}

	cases := []struct {
		name string
		now  watermark.Mark
		ok   bool
		want bool
		why  string
	}{
		{"unchanged", watermark.Mark{From: t0, Claimed: true}, true, true,
			"nothing moved, so nothing was removed under the cursor"},
		{"widened", watermark.Mark{From: t0.Add(-time.Hour), Claimed: true}, true, true,
			"the fast tier holds more than when the cursor opened, which takes nothing away"},
		{"narrowed by a second", watermark.Mark{From: t0.Add(time.Second), Claimed: true}, true, false,
			"the evictor raises the claim to the boundary it then evicts against"},
		{"narrowed by an hour", watermark.Mark{From: t0.Add(time.Hour), Claimed: true}, true, false,
			"the obvious case"},
		{"withdrawn", watermark.Mark{From: t0, Claimed: false}, true, false,
			"a withdrawn claim covers nothing, and From alone would read it as the widest claim there is"},
		{"withdrawn with a zero From", watermark.Mark{Claimed: false}, true, false,
			"the zero time is the whole of history, so reading From without Claimed inverts this"},
		{"could not be read", watermark.Mark{From: t0, Claimed: true}, false, false,
			"an unreadable claim is not evidence the claim still holds"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := stillCovers(opened, c.now, c.ok); got != c.want {
				t.Errorf("stillCovers = %v, want %v: %s", got, c.want, c.why)
			}
		})
	}
}

// A cursor opened against a claim covering all of history, which is the state
// the original defect was measured in: From is the zero time, so every
// narrowing moves it forward.
func TestStillCoversFromAllOfHistory(t *testing.T) {
	opened := nsClaim{ns: "shop.orders", claimed: true}
	now := watermark.Mark{From: time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC), Claimed: true}
	if stillCovers(opened, now, true) {
		t.Error("a claim raised off the zero time has narrowed, and this said it had not")
	}
	if !stillCovers(opened, watermark.Mark{Claimed: true}, true) {
		t.Error("an unmoved claim on all of history was read as narrowed")
	}
}
