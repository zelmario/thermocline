package router

import "testing"

// The score turns speculation on once most recent point reads of a namespace
// miss, and back off once they hit, within specScoreLimit either way so a long
// run of one kind is undone by a short run of the other.
func TestPointSpecScore(t *testing.T) {
	r := &Router{}
	const ns = "shop.orders"
	if got := r.specScore(ns).Load(); got != 0 {
		t.Fatalf("a namespace starts at %d, want 0: nothing is speculated until reads have missed", got)
	}
	for i := 0; i < 100; i++ {
		r.notePointRead(ns, true)
	}
	if got := r.specScore(ns).Load(); got != specScoreLimit {
		t.Fatalf("after 100 misses the score is %d, want the limit %d", got, specScoreLimit)
	}
	for i := 0; i < specScoreLimit; i++ {
		r.notePointRead(ns, false)
	}
	if got := r.specScore(ns).Load(); got > 0 {
		t.Fatalf("after %d hits in a row it still speculates, score %d", specScoreLimit, got)
	}
	for i := 0; i < 100; i++ {
		r.notePointRead(ns, false)
	}
	if got := r.specScore(ns).Load(); got != -specScoreLimit {
		t.Fatalf("after 100 hits the score is %d, want %d", got, -specScoreLimit)
	}
	if got := r.specScore("shop.other").Load(); got != 0 {
		t.Fatalf("another namespace moved with this one: %d", got)
	}
}
