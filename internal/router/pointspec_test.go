package router

import (
	"math/rand"
	"testing"
)

// The rate turns speculation on once most recent point reads of a namespace
// miss, and off within a few hits, so a long run of one kind is undone by a
// short run of the other.
func TestPointSpecScore(t *testing.T) {
	r := &Router{}
	const ns = "shop.orders"
	if speculates(r.specScore(ns).Load()) {
		t.Fatal("a namespace speculates before any read has missed")
	}
	for i := 0; i < 100; i++ {
		r.notePointRead(ns, true)
	}
	if !speculates(r.specScore(ns).Load()) {
		t.Fatalf("after 100 misses it does not speculate, rate %d", r.specScore(ns).Load())
	}
	for i := 0; i < 5; i++ {
		r.notePointRead(ns, false)
	}
	if speculates(r.specScore(ns).Load()) {
		t.Fatalf("after 5 hits in a row it still speculates, rate %d", r.specScore(ns).Load())
	}
	if speculates(r.specScore("shop.other").Load()) {
		t.Fatal("another namespace moved with this one")
	}
}

// How often a namespace speculates at a given miss rate: rarely when half its
// point reads miss, which is what staging saw, nearly always when nine in ten
// do.
func TestSpeculatesOnlyWhenMostMiss(t *testing.T) {
	for _, c := range []struct {
		miss     float64
		min, max float64
	}{
		{0.5, 0, 0.05},
		{0.9, 0.8, 1},
	} {
		r := &Router{}
		ns := "db.c"
		rnd := rand.New(rand.NewSource(1))
		spec := 0
		const n = 100000
		for i := 0; i < n; i++ {
			if speculates(r.specScore(ns).Load()) {
				spec++
			}
			r.notePointRead(ns, rnd.Float64() < c.miss)
		}
		if f := float64(spec) / n; f < c.min || f > c.max {
			t.Errorf("with %.0f%% of reads missing it speculated on %.1f%%, want %.0f%% to %.0f%%",
				c.miss*100, f*100, c.min*100, c.max*100)
		}
	}
}
