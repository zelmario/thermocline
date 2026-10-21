package main

import (
	"math/rand"
	"testing"
)

// The visits come out the shape planVisit says: about two purchases in a
// hundred visits, most steps looking around, a support lookup in a hundred.
func TestVisitShape(t *testing.T) {
	s := &store{sessions: true}
	p := s.newShopper(1)
	p.rng = rand.New(rand.NewSource(1))
	const visits = 200000
	steps := map[string]int{}
	total := 0
	for v := 0; v < visits; v++ {
		p.plan = p.plan[:0]
		p.planVisit()
		for _, name := range p.plan {
			steps[name]++
			total++
		}
	}
	conv := float64(steps["checkout"]) / visits
	look := float64(steps["browse a category"]+steps["product page"]+steps["filter by price"]) / float64(total)
	support := float64(steps["old order (support)"]) / visits
	t.Logf("%.2f steps a visit; checkout in %.2f%% of visits; looking around %.0f%% of steps; %v",
		float64(total)/visits, 100*conv, 100*look, steps)
	if conv < 0.015 || conv > 0.03 {
		t.Errorf("checkout in %.2f%% of visits, want about 2%%", 100*conv)
	}
	if look < 0.75 {
		t.Errorf("looking around is %.0f%% of steps, want most of them", 100*look)
	}
	if support < 0.008 || support > 0.012 {
		t.Errorf("support in %.2f%% of visits, want about 1%%", 100*support)
	}
}
