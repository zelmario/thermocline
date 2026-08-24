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
// The one that destroys data is the eviction announcement. The evictor stamps
// it with its own clock and the archive's TTL monitor decides when it has
// expired, so an evictor running behind the archive has its announcements
// expiring early. Far enough behind and they are gone before the feeder reads
// the delete they explain, which the feeder then takes for the application's
// own: the archive tombstones its copy and the document is gone from both
// tiers, with nothing said anywhere.
//
// The others are milder and worth knowing about anyway. A residency hold is
// written by the router and read by the evictor, so skew there evicts a
// document that was just promoted and it is fetched again. A warm window and an
// eviction cutoff are both measured against document dates, so a skewed clock
// shifts the window rather than breaking it.
//
// Nothing here can correct a clock. It reports one, in the words that say which
// direction is dangerous.

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
