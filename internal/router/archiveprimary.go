package router

import (
	"sync/atomic"
	"time"

	driverevent "go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
)

// PrimaryWatch remembers when the archive last had no primary, as the driver
// saw it.
//
// The freshness barrier asks whether the archive has a primary only when a wait
// runs out, and that question has no memory. Measured on the local pair on
// 2026-10-09: rsCold had no primary for about 11 seconds, a read through the
// router began 3 seconds into that, and the primary came back a few hundred
// milliseconds before the read's five seconds were up. Asked then, the archive
// had a primary, so the wait gave up as it would for a slow feeder, and the
// read answered without the write it made 5 seconds earlier. The feeder had not
// been able to publish anything for the whole wait. The same check passed on
// other runs where the window ended a little later, which is how it got through
// as passing at first.
//
// The driver already knows: measured with a probe beside the router, it
// reported the lost primary within 60ms of the step-down and the new one within
// 60ms of the election, because servers push topology changes to it. This keeps
// that, at no cost to a read.
type PrimaryWatch struct {
	had    atomic.Bool
	seen   atomic.Bool
	backAt atomic.Int64 // unix nanoseconds of the last change back to one
}

// NewPrimaryWatch returns a watch that has seen nothing yet, which reads as an
// archive that has never been without a primary.
func NewPrimaryWatch() *PrimaryWatch { return &PrimaryWatch{} }

// Monitor is passed to the archive client's options.
func (w *PrimaryWatch) Monitor() *driverevent.ServerMonitor {
	return &driverevent.ServerMonitor{TopologyDescriptionChanged: w.changed}
}

func (w *PrimaryWatch) changed(e *driverevent.TopologyDescriptionChangedEvent) {
	has := false
	for _, s := range e.NewDescription.Servers {
		if s.Kind == description.ServerKindRSPrimary.String() {
			has = true
			break
		}
	}
	// The first description the driver builds has no primary yet because it
	// has not heard from anyone, which is not a primary going away.
	if !w.seen.Load() {
		if has {
			w.seen.Store(true)
			w.had.Store(true)
		}
		return
	}
	if w.had.Swap(has) == has {
		return
	}
	if has {
		w.backAt.Store(time.Now().UnixNano())
	}
}

// withoutPrimarySince reports whether the archive was without a primary at any
// moment after t.
func (w *PrimaryWatch) withoutPrimarySince(t time.Time) bool {
	if w == nil || !w.seen.Load() {
		return false
	}
	if !w.had.Load() {
		return true
	}
	return w.backAt.Load() > t.UnixNano()
}

// WatchArchivePrimary gives the router what the archive client's monitor saw.
func (r *Router) WatchArchivePrimary(w *PrimaryWatch) *Router {
	r.archivePrimary = w
	return r
}
