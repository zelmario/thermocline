// Package checkpoint persists change stream resume tokens.
//
// Checkpoints live on the cold tier on purpose. The token describes a position
// in the fast tier's oplog, and the whole point of storing it is to survive the
// fast tier going away.
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

// Record is one saved position.
type Record struct {
	ID      string    `bson:"_id"`
	Token   bson.Raw  `bson:"token"`
	SavedAt time.Time `bson:"savedAt"`
	// EventAt is the cluster time of the last event applied, used to report lag.
	EventAt time.Time `bson:"eventAt,omitempty"`
	// SkewMS is how far the feeder's clock was ahead of the archive's when it
	// last looked, which is what makes EventAt comparable on another machine.
	//
	// The same field the position record carries and for the same reason: lag
	// is worked out by subtracting EventAt from a clock that is not the
	// feeder's. This record is the fallback for a deployment with
	// position_every set to zero, which is a supported setting, and without
	// this that deployment is the one place the correction does not reach.
	//
	// Absent on a record written by a feeder from before it, and on one that
	// has not managed to read the archive's clock. A reader without it
	// subtracts raw instants, which is what everything did before.
	SkewMS *int64 `bson:"skewMS,omitempty"`
	// StartAt is where a feeder with no token starts reading, written by
	// cline backfill in place of a token the oplog no longer holds.
	//
	// Clearing the checkpoint instead restarted mirroring from whenever the
	// feeder next started, so every write between the backfill's copy and that
	// moment reached neither: on the Hetzner staging pair on 2026-10-12 that
	// was ten seconds of a running shop, and an update made in it left the
	// archive holding the version before. Starting from just before the copy
	// replays what the copy may have missed, and replaying is safe because
	// every event is applied as an upsert or a mark. The first token the
	// feeder saves removes it.
	StartAt *bson.Timestamp `bson:"startAt,omitempty"`
	// NS and TS name the event the token was taken from, and are absent for a
	// token that names no event: MongoDB's post-batch token past entries this
	// stream filtered out.
	//
	// mongod resumes from an event's token only if that event still passes the
	// stream's filter, and answers "the resume token was not found" otherwise.
	// Taking a namespace out of feeder.namespaces does exactly that to a
	// checkpoint whose last event was on it, and the feeder reported a rolled
	// oplog with the oplog minutes old: measured on the Hetzner staging pair on
	// 2026-10-14. Knowing which namespace the token names is what tells that
	// apart from a fast tier that lost the event, and TS is where to resume.
	NS string          `bson:"ns,omitempty"`
	TS *bson.Timestamp `bson:"ts,omitempty"`
}

// Skew is how far the publisher's clock was ahead of the archive's when it last
// looked, and whether it ever managed to look.
func (r Record) Skew() (time.Duration, bool) {
	if r.SkewMS == nil {
		return 0, false
	}
	return time.Duration(*r.SkewMS) * time.Millisecond, true
}

// Store reads and writes checkpoints.
type Store struct {
	coll *mongo.Collection
}

// New returns a Store backed by the cold tier.
func New(cold *mongo.Client) *Store {
	return &Store{coll: cold.Database(meta.DB).Collection(meta.Checkpoints)}
}

// Load returns the saved token for id. A missing checkpoint is not an error;
// it returns a nil token, meaning "start from now".
func (s *Store) Load(ctx context.Context, id string) (bson.Raw, *bson.Timestamp, error) {
	var rec Record
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&rec)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("load checkpoint %q: %w", id, err)
	}
	return rec.Token, rec.StartAt, nil
}

// StartFrom replaces the checkpoint with one that has no token and starts
// reading at at. See Record.StartAt. wall is at's own instant, recorded as
// eventAt so lag is reported from where mirroring will resume rather than as
// missing.
func (s *Store) StartFrom(ctx context.Context, id string, at bson.Timestamp, wall time.Time) error {
	_, err := s.coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{
		{Key: "_id", Value: id},
		{Key: "startAt", Value: at},
		{Key: "savedAt", Value: time.Now().UTC()},
		{Key: "eventAt", Value: wall.UTC()},
	}, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("set checkpoint %q to start at %v: %w", id, at, err)
	}
	return nil
}

// Save writes the token for id. eventAt may be zero when the stream has not
// delivered an event yet. ns and ts name the event the token was taken from,
// and ns is empty for a token that names none. See Record.NS.
func (s *Store) Save(ctx context.Context, id string, token bson.Raw, eventAt time.Time,
	skew time.Duration, skewKnown bool, ns string, ts bson.Timestamp) error {
	set := bson.D{
		{Key: "token", Value: token},
		{Key: "savedAt", Value: time.Now().UTC()},
	}
	unset := bson.D{{Key: "startAt", Value: ""}}
	if ns != "" {
		set = append(set, bson.E{Key: "ns", Value: ns}, bson.E{Key: "ts", Value: ts})
	} else {
		unset = append(unset, bson.E{Key: "ns", Value: ""}, bson.E{Key: "ts", Value: ""})
	}
	if !eventAt.IsZero() {
		set = append(set, bson.E{Key: "eventAt", Value: eventAt})
	}
	if skewKnown {
		set = append(set, bson.E{Key: "skewMS", Value: skew.Milliseconds()})
	}

	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: set}, {Key: "$unset", Value: unset}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save checkpoint %q: %w", id, err)
	}
	return nil
}

// EventAt returns the cluster time of the last event the feeder applied, and
// whether a checkpoint exists at all.
//
// It is how anything other than the feeder finds out how far behind mirroring
// is. A read served from the archive is only as current as this instant.
func (s *Store) EventAt(ctx context.Context, id string) (time.Time, bool, error) {
	rec, ok, err := s.Mark(ctx, id)
	return rec.EventAt, ok, err
}

// Mark returns the whole checkpoint, for a caller that needs more than the
// instant: how far behind mirroring is cannot be worked out from EventAt alone
// without knowing whose clock it was read on. See Record.SkewMS.
func (s *Store) Mark(ctx context.Context, id string) (Record, bool, error) {
	var rec Record
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&rec)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("read checkpoint %q: %w", id, err)
	}
	return rec, !rec.EventAt.IsZero(), nil
}
