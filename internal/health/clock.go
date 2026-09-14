package health

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Almost everything here is decided by comparing instants, and the instants do
// not all come from the same machine.
//
// There are three answers to that, and which one applies depends on who reads
// the instant.
//
// Where the archive reads it, the archive writes it. Everything whose lifetime
// its TTL monitor decides is stamped as the row is written, with $currentDate
// or $$NOW: the eviction and warming announcements, the record of this system's
// own DDL, the residency rows and the hold inside them. The eviction
// announcement is the one that used to destroy data. It was stamped by the
// evictor and expired by the archive, so an evictor running behind had its
// announcements expiring early, and far enough behind they were gone before the
// feeder read the delete they explained: the feeder took that delete for the
// application's own, the archive tombstoned its copy, and the document was gone
// from both tiers with nothing said. There is nothing to correct there now
// because there is nothing to compare.
//
// Where another daemon reads it, both sides say where they stand against the
// archive and the instants are put in its frame before they are subtracted.
// That is Skew below, taken over a round trip on a one-minute cadence: the
// feeder publishes its reading in the position record, and the router, the
// evictor and this package each take their own. Mirroring lag, the freshness
// barrier, how old a published position is and how long ago a daemon reported
// are all worked out that way.
//
// What is left is the windows measured against the application's own document
// dates, a warm window and an eviction cutoff. A skewed clock shifts those by
// the difference rather than breaking them, and nothing but a correct clock
// fixes it.
//
// Nothing here can correct a clock. It reports one.

// AbsDuration is how far apart two instants are, whichever way round they came
// out. Every caller here is asking "do these two clocks disagree by more than
// X", and the sign is a separate question with a separate answer.
func AbsDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// SkewWarn is the difference worth mentioning: past this the residency holds and
// the windows are visibly not what the configuration says.
const SkewWarn = 30 * time.Second

// Skew is how far a local clock is ahead of a server's, positive when local is
// ahead.
func Skew(ctx context.Context, c *mongo.Client) (time.Duration, error) {
	var res struct {
		LocalTime time.Time `bson:"localTime"`
	}
	// Timed around the call, so the round trip is not read as skew. The
	// server's instant is somewhere inside the window; the middle is the best
	// estimate of it and the error is at most half the round trip.
	before := time.Now()
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&res)
	after := time.Now()
	if err != nil {
		return 0, fmt.Errorf("read the server's clock: %w", err)
	}
	if res.LocalTime.IsZero() {
		return 0, fmt.Errorf("the server did not report a clock")
	}
	middle := before.Add(after.Sub(before) / 2)
	return middle.Sub(res.LocalTime), nil
}

// LagFrom is how far behind a published instant is, with both clocks accounted
// for when they are known.
//
// The two instants come from two machines: the feeder stamps the position with
// its own clock and whoever reads it is holding its own. Clamping at zero used
// to be the whole treatment, and it only hides the direction that produces a
// negative number. The other direction produces a positive one, which nothing
// can tell from real lag: a host thirty seconds ahead of its feeder reads
// thirty seconds of mirroring lag on a deployment where nothing is behind, and
// that number gates eviction and is what the alert rules watch.
//
// Both sides talk to the archive, so both can say where they stand against that
// one clock. With either offset missing the raw instants are subtracted, which
// is what this did before.
//
// Still clamped. The correction is measured over a round trip and is good to a
// fraction of a millisecond, not exact, and a lag of minus one millisecond is
// not a small number, it is a nonsense one.
func LagFrom(now, at time.Time, mine, theirs time.Duration, known bool) time.Duration {
	if known {
		now = now.Add(-mine)
		at = at.Add(-theirs)
	}
	return max(now.Sub(at), 0)
}

// skewRemeasure is how often the archive's clock is reread. Clocks drift
// slowly, and a daemon that watched them closely would pay a round trip per
// sample to learn nothing.
const skewRemeasure = time.Minute

// archiveSkew is how far this host's clock is ahead of the archive's, and
// whether it has ever been measured.
//
// This one is taken in line rather than in the background, unlike the router's,
// because a sample is already several round trips to both tiers and is taken on
// a sweep's cadence rather than a read's.
func (c *Checker) archiveSkew(ctx context.Context) (time.Duration, bool) {
	c.skewMu.Lock()
	defer c.skewMu.Unlock()
	if c.skewKnown && time.Since(c.skewAt) < skewRemeasure {
		return c.skew, true
	}
	if !c.skewKnown && !c.skewAt.IsZero() && time.Since(c.skewAt) < skewRemeasure {
		// A cluster that refuses the reading is not asked again every sample.
		return 0, false
	}
	skew, err := Skew(ctx, c.cold)
	c.skewAt = time.Now()
	if err != nil {
		// The last reading stands, which is a better answer than none.
		return c.skew, c.skewKnown
	}
	c.skew, c.skewKnown = skew, true
	return skew, true
}
