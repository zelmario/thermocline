// Package watermark records how far back the fast tier is complete.
//
// CompleteFrom means every live document newer than that instant is present in
// the fast tier, so a query bounded inside the range can be answered there
// alone. Everything that changes what the fast tier holds has to keep this
// honest, in both directions:
//
//   - The warmer lowers it as it fills contiguous ranges from the archive.
//   - The evictor raises it before removing anything, because a document
//     removed from inside the claimed range makes the claim false.
//
// Getting the second one wrong is the dangerous half. An overstated watermark
// makes range queries return short results and report success.
package watermark

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

// Store reads and writes watermarks on the cold tier.
type Store struct {
	coll *mongo.Collection
}

// New returns a Store backed by the cold tier.
func New(cold *mongo.Client) *Store {
	return &Store{coll: cold.Database(meta.DB).Collection(meta.Watermarks)}
}

// Load returns the recorded completeness for ns. A missing watermark is not an
// error; it returns the zero time, meaning nothing is claimed.
func (s *Store) Load(ctx context.Context, ns string) (time.Time, error) {
	var wm meta.Watermark
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: ns}}).Decode(&wm)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("load watermark for %s: %w", ns, err)
	}
	return wm.CompleteFrom.UTC(), nil
}

// Set records completeness unconditionally. Used by the warmer, which knows
// exactly how far it has filled.
func (s *Store) Set(ctx context.Context, ns string, from time.Time) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: ns}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "completeFrom", Value: from.UTC()},
			{Key: "updatedAt", Value: time.Now().UTC()},
		}}},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("save watermark for %s: %w", ns, err)
	}
	return nil
}

// RaiseTo moves the watermark later, never earlier, and reports whether it
// changed anything.
//
// The evictor calls this before removing documents older than `from`. Raising
// the watermark shrinks what the fast tier claims to hold completely, which is
// the safe direction: the worst case is a redundant lookup in the archive.
func (s *Store) RaiseTo(ctx context.Context, ns string, from time.Time) (bool, error) {
	res, err := s.coll.UpdateOne(ctx,
		bson.D{
			{Key: "_id", Value: ns},
			{Key: "completeFrom", Value: bson.D{{Key: "$lt", Value: from.UTC()}}},
		},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "completeFrom", Value: from.UTC()},
			{Key: "updatedAt", Value: time.Now().UTC()},
		}}})
	if err != nil {
		return false, fmt.Errorf("raise watermark for %s: %w", ns, err)
	}
	if res.MatchedCount > 0 {
		return true, nil
	}

	// No existing record matched. Either the watermark is already later, which
	// needs nothing, or there is no watermark at all, which needs one.
	err = s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: ns}}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return true, s.Set(ctx, ns, from)
	}
	if err != nil {
		return false, fmt.Errorf("raise watermark for %s: %w", ns, err)
	}
	return false, nil
}
