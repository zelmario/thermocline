package router

import (
	"testing"
	"time"

	driverevent "go.mongodb.org/mongo-driver/v2/event"
)

func topo(kinds ...string) *driverevent.TopologyDescriptionChangedEvent {
	e := &driverevent.TopologyDescriptionChangedEvent{}
	for _, k := range kinds {
		e.NewDescription.Servers = append(e.NewDescription.Servers, driverevent.ServerDescription{Kind: k})
	}
	return e
}

func TestPrimaryWatch(t *testing.T) {
	var none *PrimaryWatch
	if none.withoutPrimarySince(time.Now().Add(-time.Hour)) {
		t.Fatal("a router with no watch read as having lost its primary")
	}

	w := NewPrimaryWatch()
	began := time.Now().Add(-time.Hour)
	// The driver's first description knows nobody yet.
	w.changed(topo("Unknown", "Unknown", "Unknown"))
	if w.withoutPrimarySince(began) {
		t.Fatal("the description before the first hello read as a lost primary")
	}
	w.changed(topo("RSPrimary", "RSSecondary", "RSSecondary"))
	if w.withoutPrimarySince(began) {
		t.Fatal("finding the primary at startup read as a lost primary")
	}

	beforeLoss := time.Now()
	w.changed(topo("Unknown", "RSSecondary", "RSSecondary"))
	if !w.withoutPrimarySince(time.Now()) {
		t.Fatal("no primary now, and the watch did not say so")
	}
	w.changed(topo("RSSecondary", "RSPrimary", "RSSecondary"))
	if !w.withoutPrimarySince(beforeLoss) {
		t.Fatal("a window that closed after the wait began went unseen")
	}
	if w.withoutPrimarySince(time.Now().Add(time.Millisecond)) {
		t.Fatal("a window that closed before the wait began was counted")
	}
	// A description that changes something else keeps the time it came back.
	back := w.backAt.Load()
	w.changed(topo("RSSecondary", "RSPrimary", "RSSecondary"))
	if w.backAt.Load() != back {
		t.Fatal("a description with the same primary moved the time it came back")
	}
}
