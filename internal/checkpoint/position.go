package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
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
	// At is the wall-clock instant through which every event has been applied
	// to the archive. It is written after the batch lands, never before.
	//
	// It is kept for a reader that has nothing better, and it is the weaker of
	// the two: it is the *feeder's* clock, and whoever compares against it is
	// reading their own. On separate hosts the barrier is then only as sound as
	// the skew between two machines nothing keeps in step.
	At      time.Time `bson:"at"`
	SavedAt time.Time `bson:"savedAt"`
	// TS is the same position as a cluster time, and is the one to compare
	// against when it is there.
	//
	// A cluster time is the replica set's own logical clock, so the feeder and
	// the router read it from the same place and no clock of their own comes
	// into it. It is exact where the wall-clock reading is a claim: it comes
	// either from an event the feeder applied or from the resume token of an
	// empty poll, which is MongoDB saying "nothing exists up to here".
	//
	// Absent on a record written by a feeder from before this, which is the
	// only reason At survives.
	TS bson.Timestamp `bson:"ts,omitempty"`
	// EveryMS is the cadence the publisher promised, so a reader can judge
	// whether silence means "behind" or "gone" without being told how this
	// feeder is configured.
	EveryMS int64 `bson:"everyMS,omitempty"`
}

// PackTS makes a cluster time comparable as a single number.
//
// A BSON timestamp orders by seconds first and by increment second, which is
// exactly what the two halves packed into a uint64 do. Comparing the seconds
// alone would treat every event inside one second as simultaneous, and that
// rounding is what once made the published position appear to move backwards.
func PackTS(ts bson.Timestamp) uint64 { return uint64(ts.T)<<32 | uint64(ts.I) }

// UnpackTS is PackTS backwards, for logging a value that was kept packed.
func UnpackTS(v uint64) bson.Timestamp {
	return bson.Timestamp{T: uint32(v >> 32), I: uint32(v)}
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
func (p *Positions) Save(ctx context.Context, id string, at time.Time, ts bson.Timestamp, every time.Duration) error {
	if at.IsZero() && ts.T == 0 {
		return nil
	}
	set := bson.D{
		{Key: "savedAt", Value: time.Now().UTC()},
		{Key: "everyMS", Value: every.Milliseconds()},
	}
	if !at.IsZero() {
		set = append(set, bson.E{Key: "at", Value: at})
	}
	if ts.T != 0 {
		set = append(set, bson.E{Key: "ts", Value: ts})
	}
	_, err := p.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: set}},
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
	return rec, !rec.At.IsZero() || rec.TS.T != 0, nil
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
