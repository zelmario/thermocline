package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmar/thermocline/internal/meta"
)

// A checkpoint answers two questions that have nothing to do with each other,
// and answering both with one write made one of them expensive.
//
// The first is durability: where does the feeder resume after a restart? That
// costs re-mirrored work when it is stale, mirroring is idempotent, and so
// saving it every few seconds is exactly right.
//
// The second is freshness: how far has mirroring got, right now? A read that
// falls through to the archive waits on this before it answers, because the
// archive trails the fast tier and returning a document the caller already
// deleted is worse than being slow.
//
// Tying the second to the first made every fallback read wait for the next
// checkpoint, whether or not the feeder had anything to do. Measured on an idle
// database with the feeder perfectly caught up, a fallback read took 1.14 s
// with checkpoint_every at 1s and 4.53 s at the shipped default of 5s, while
// the same read with the wait disabled took 1.8 ms. The wait was not waiting
// for mirroring. It was waiting for the next checkpoint write.
//
// Worse, at the default the wait exceeded archive_wait, which also defaults to
// 5s, so reads gave up and answered stale anyway: slow and stale, the two
// things the barrier exists to avoid.
//
// So the position gets its own record and its own cadence. It carries no resume
// token, just an instant, which makes it small enough to write many times a
// second without anyone noticing.

// PositionRecord is how far mirroring has got.
type PositionRecord struct {
	ID string `bson:"_id"`
	// At is the cluster time through which every event has been applied to the
	// archive. It is written after the batch lands, never before.
	At      time.Time `bson:"at"`
	SavedAt time.Time `bson:"savedAt"`
	// EveryMS is the cadence the publisher promised, so a reader can judge
	// whether silence means "behind" or "gone" without being told how this
	// feeder is configured.
	EveryMS int64 `bson:"everyMS,omitempty"`
}

// Positions reads and writes mirroring positions.
type Positions struct {
	coll *mongo.Collection
}

// NewPositions returns a position store backed by the cold tier.
func NewPositions(cold *mongo.Client) *Positions {
	return &Positions{coll: cold.Database(meta.DB).Collection(meta.PositionsColl)}
}

// Save publishes how far mirroring has got.
//
// The write concern is deliberately w:1. This is a hint that makes reads fast,
// not a durability record: if it is lost the feeder republishes it within a
// tick, and the durable checkpoint is what recovery actually uses. Paying for
// majority acknowledgement several times a second to speed up reads would be a
// poor trade.
func (p *Positions) Save(ctx context.Context, id string, at time.Time, every time.Duration) error {
	if at.IsZero() {
		return nil
	}
	_, err := p.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "at", Value: at},
			{Key: "savedAt", Value: time.Now().UTC()},
			{Key: "everyMS", Value: every.Milliseconds()},
		}}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save position %q: %w", id, err)
	}
	return nil
}

// At returns how far mirroring has got, and whether a position exists.
//
// A missing position is not an error. A feeder from before this existed, or one
// with positions turned off, publishes nothing here and the caller falls back
// to the checkpoint, which is where this information used to live.
func (p *Positions) At(ctx context.Context, id string) (PositionRecord, bool, error) {
	var rec PositionRecord
	err := p.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&rec)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return PositionRecord{}, false, nil
	}
	if err != nil {
		return PositionRecord{}, false, fmt.Errorf("read position %q: %w", id, err)
	}
	return rec, !rec.At.IsZero(), nil
}

// Live reports whether the publisher still looks alive.
//
// A position left behind by a feeder that has stopped would otherwise be read
// as "mirroring is behind", and every fallback read would wait the full
// archive_wait before answering. That turns a stopped background daemon into a
// slow database, which is exactly what the wait is not supposed to do. Four
// missed publications is the line; the record carries the cadence so this does
// not have to guess it.
func (rec PositionRecord) Live(now time.Time) bool {
	every := time.Duration(rec.EveryMS) * time.Millisecond
	if every <= 0 {
		every = time.Second
	}
	stale := 4 * every
	if stale < 2*time.Second {
		stale = 2 * time.Second
	}
	return now.Sub(rec.SavedAt) <= stale
}
