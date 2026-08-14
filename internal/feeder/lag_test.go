package feeder

import (
	"testing"
	"time"
)

// Lag is measured from the last applied event, so on a database that goes quiet
// it climbs forever while the archive is in fact perfectly up to date. The
// evictor reads that number, fails closed, and stops evicting for good, which
// looks exactly like a hang. An empty change stream poll means everything has
// been applied, so it has to reset the measurement.
func TestIdleFeederReportsNoLag(t *testing.T) {
	f := testFeeder()

	f.lastEventAt = time.Now().Add(-90 * time.Minute)
	if f.Lag() < time.Hour {
		t.Fatalf("setup: wanted a large lag to start from, got %v", f.Lag())
	}

	f.markCaughtUp()
	if got := f.Lag(); got > time.Second {
		t.Errorf("Lag after catching up = %v, want about zero: a quiet database must not look like a feeder hours behind", got)
	}
}

// A feeder that has never seen an event reports no lag rather than a lag
// measured from the zero time, which would be thousands of years.
func TestFreshFeederReportsNoLag(t *testing.T) {
	if got := testFeeder().Lag(); got != 0 {
		t.Errorf("Lag before any event = %v, want 0", got)
	}
}
