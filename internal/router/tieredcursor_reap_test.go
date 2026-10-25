package router

import (
	"testing"
	"time"
)

// mongod times a cursor out ten minutes after its last use. The guard on a
// tiered cursor was reaped ten minutes after it opened, so a cursor still
// being read lost it and its later getMores were forwarded unchecked.
func TestTieredCursorIdlesFromItsLastUse(t *testing.T) {
	var reg tieredCursors
	opened := time.Now().Add(-11 * time.Minute)
	reg.add(1, tieredRead{at: opened})
	reg.add(2, tieredRead{at: opened})
	reg.touch(1)

	reg.reapIdle(10 * time.Minute)

	if _, ok := reg.get(1); !ok {
		t.Error("a cursor opened eleven minutes ago and read a moment ago was reaped")
	}
	if _, ok := reg.get(2); ok {
		t.Error("a cursor idle for eleven minutes was kept")
	}
}

// A cursor opened with noCursorTimeout is never timed out by mongod, and a
// batch job pausing on one for longer than ten minutes still reads it. Its
// guard was reaped like any other and the getMores after the pause went to
// the fast tier unchecked.
func TestTieredCursorWithNoTimeoutIsKept(t *testing.T) {
	var reg tieredCursors
	reg.add(1, tieredRead{at: time.Now().Add(-time.Hour), pinned: true})
	reg.reapIdle(10 * time.Minute)
	if _, ok := reg.get(1); !ok {
		t.Error("a noCursorTimeout cursor idle for an hour was reaped")
	}
}
