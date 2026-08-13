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

	"github.com/zelmar/thermocline/internal/meta"
)

// Record is one saved position.
type Record struct {
	ID      string    `bson:"_id"`
	Token   bson.Raw  `bson:"token"`
	SavedAt time.Time `bson:"savedAt"`
	// EventAt is the cluster time of the last event applied, used to report lag.
	EventAt time.Time `bson:"eventAt,omitempty"`
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
func (s *Store) Load(ctx context.Context, id string) (bson.Raw, error) {
	var rec Record
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&rec)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint %q: %w", id, err)
	}
	return rec.Token, nil
}

// Save writes the token for id. eventAt may be zero when the stream has not
// delivered an event yet.
func (s *Store) Save(ctx context.Context, id string, token bson.Raw, eventAt time.Time) error {
	set := bson.D{
		{Key: "token", Value: token},
		{Key: "savedAt", Value: time.Now().UTC()},
	}
	if !eventAt.IsZero() {
		set = append(set, bson.E{Key: "eventAt", Value: eventAt})
	}

	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: set}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save checkpoint %q: %w", id, err)
	}
	return nil
}
