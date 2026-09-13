package checkpoint

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The feeder publishes its position several times a second, and the router uses
// that record to decide whether mirroring is running at all. A feeder judged
// stopped turns the freshness barrier off: reads that fall through to the
// archive answer at once, count a stale read and warn.
//
// So the cost of the two mistakes is not symmetric. Calling a stopped feeder
// alive costs one read the archive_wait it would have paid anyway. Calling a
// running feeder stopped removes the guarantee the barrier exists to give,
// while the feeder is doing its job, and the warning names the wrong cause.
//
// The test that used to make that decision subtracted the reader's clock from a
// stamp written by the feeder's, with a two second floor at the shipped
// cadence. Two hosts three seconds apart is ordinary, and it is the second
// mistake it makes.
func TestLivenessWatchesTheRecordMove(t *testing.T) {
	// These are the readings where the clocks cannot be reconciled: a feeder too
	// old to publish its offset from the archive, or a reader that has not
	// managed a reading of its own yet. Watching the record move is all there is
	// to go on. The reconciled path is the test below.
	// The cadence the feeder ships with, so the window is its two second floor.
	const every = int64(20)

	moving := func(n int) PositionRecord {
		// Whatever savedAt says, each reading differs from the last, which is
		// a publication that happened between the two.
		return PositionRecord{
			SavedAt: time.Unix(1000, 0).Add(time.Duration(n) * 20 * time.Millisecond),
			At:      time.Unix(1000, 0).Add(time.Duration(n) * 20 * time.Millisecond),
			TS:      bson.Timestamp{T: 1000, I: uint32(n)},
			EveryMS: every,
		}
	}

	t.Run("a feeder whose clock is behind this one is still alive", func(t *testing.T) {
		var l Liveness
		// The reader's clock is a minute ahead of the feeder's. Every stamp the
		// feeder writes therefore looks a minute old here, and the wall-clock
		// test calls a healthy feeder stopped for ever.
		now := time.Unix(1060, 0)
		if !l.Observe(moving(1), now, 0, false) {
			t.Fatalf("first sight of a record with a skewed stamp read as stopped")
		}
		for i := 2; i < 10; i++ {
			now = now.Add(20 * time.Millisecond)
			if !l.Observe(moving(i), now, 0, false) {
				t.Fatalf("reading %d: a record that moved read as stopped", i)
			}
		}
	})

	t.Run("a feeder that stops publishing is stopped, after a window", func(t *testing.T) {
		var l Liveness
		now := time.Unix(1060, 0)
		l.Observe(moving(1), now, 0, false)
		last := moving(2)
		now = now.Add(20 * time.Millisecond)
		if !l.Observe(last, now, 0, false) {
			t.Fatalf("a record that moved read as stopped")
		}
		// It stops here. Inside the window there is no evidence yet.
		now = now.Add(1900 * time.Millisecond)
		if !l.Observe(last, now, 0, false) {
			t.Errorf("called stopped after 1.9s, inside the two second window")
		}
		now = now.Add(200 * time.Millisecond)
		if l.Observe(last, now, 0, false) {
			t.Errorf("a record that has not moved for over two seconds still reads as alive")
		}
	})

	t.Run("the window follows the cadence the feeder published", func(t *testing.T) {
		var l Liveness
		rec := moving(1)
		rec.EveryMS = 5000 // four missed publications is twenty seconds
		now := time.Unix(2000, 0)
		l.Observe(rec, now, 0, false)
		if !l.Observe(rec, now.Add(19*time.Second), 0, false) {
			t.Errorf("called stopped after 19s with a 5s cadence, which is one missed publication short")
		}
		if l.Observe(rec, now.Add(21*time.Second), 0, false) {
			t.Errorf("still alive after 21s with a 5s cadence")
		}
	})

	t.Run("a record left behind hours ago is stopped on sight", func(t *testing.T) {
		// The case the wall clock is still allowed to decide: a router started
		// while the feeder has been down for hours should not make the first
		// read that falls through wait the whole barrier to learn it.
		var l Liveness
		rec := moving(1)
		if l.Observe(rec, rec.SavedAt.Add(2*time.Hour), 0, false) {
			t.Errorf("a position two hours old read as alive on first sight")
		}
	})

	t.Run("a record stamped seconds ago is alive on sight", func(t *testing.T) {
		var l Liveness
		rec := moving(1)
		if !l.Observe(rec, rec.SavedAt.Add(3*time.Second), 0, false) {
			t.Errorf("a position three seconds old read as stopped on first sight, which is the skew this exists to survive")
		}
	})

	t.Run("a stamp from the future is alive, which is the safe direction", func(t *testing.T) {
		var l Liveness
		rec := moving(1)
		if !l.Observe(rec, rec.SavedAt.Add(-time.Hour), 0, false) {
			t.Errorf("a feeder whose clock is ahead read as stopped")
		}
	})
}

// With both offsets to hand there is nothing to watch for. The feeder publishes
// how far ahead of the archive its own clock is, the reader knows its own, and
// the record's age is then exact whatever either machine believes the time to
// be. That is what a feeder stopping has to be decided by: waiting a window
// before believing it costs every read that falls through in the meantime the
// whole archive_wait, and a router that has just started has no window yet.
func TestLivenessDatesTheRecordExactly(t *testing.T) {
	// This host is 30s ahead of the archive, the feeder 5s behind it, and the
	// shipped cadence puts the staleness window at its two second floor.
	const mine = 30 * time.Second
	theirs := int64(-5000)
	archiveNow := time.Unix(3000, 0)
	now := archiveNow.Add(mine)

	rec := func(ageInArchiveFrame time.Duration) PositionRecord {
		return PositionRecord{
			SavedAt: archiveNow.Add(-ageInArchiveFrame).Add(time.Duration(theirs) * time.Millisecond),
			At:      archiveNow.Add(-ageInArchiveFrame),
			TS:      bson.Timestamp{T: 3000, I: 1},
			EveryMS: 20,
			SkewMS:  &theirs,
		}
	}

	t.Run("a feeder publishing now is alive on the first look", func(t *testing.T) {
		var l Liveness
		if !l.Observe(rec(0), now, mine, true) {
			t.Fatalf("a position published this instant read as stopped")
		}
	})

	t.Run("and one that stopped four seconds ago is stopped on the first look", func(t *testing.T) {
		// No window of watching first. A router started while the feeder was
		// down has nothing to watch, and every read in the meantime pays.
		var l Liveness
		if l.Observe(rec(4*time.Second), now, mine, true) {
			t.Fatalf("a position four seconds old read as alive")
		}
	})

	t.Run("thirty-five seconds of skew is what this is for", func(t *testing.T) {
		// The contrast, on one record published this instant. This host is 30s
		// ahead of the archive and the feeder is 5s behind it, so subtracting
		// the raw stamps says the record is 35 seconds old and the feeder has
		// stopped. Every read that falls through then answers without waiting
		// for mirroring, and warns about a feeder that is doing its job.
		r := rec(0)
		// What the rule used to be, spelled out rather than described.
		if wasAlive := now.Sub(r.SavedAt) <= staleAfter(r.EveryMS); wasAlive {
			t.Errorf("subtracting the raw stamps no longer calls this stopped, so the case has moved")
		}
		var l Liveness
		if !l.Observe(r, now, mine, true) {
			t.Errorf("reconciled, a record published this instant read as stopped")
		}
	})

	t.Run("a record that moves is alive whatever its stamp says", func(t *testing.T) {
		// A feeder whose clock has stopped while it goes on mirroring. The
		// arithmetic says older and older; the record says otherwise, and the
		// record is the evidence.
		var l Liveness
		frozen := rec(time.Hour)
		l.Observe(frozen, now, mine, true)
		frozen.TS.I++
		if !l.Observe(frozen, now.Add(time.Second), mine, true) {
			t.Fatalf("a record that moved between two readings read as stopped")
		}
	})
}
