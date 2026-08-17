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

	"github.com/zelmario/thermocline/internal/meta"
)

// Mark is what the store knows about one namespace.
//
// Claimed is the whole point of this type. "The fast tier is complete back to
// From" and "nothing has ever been recorded" are different states, and
// collapsing them into a zero time is what let an empty fast tier answer range
// queries as though it held everything. A fast tier that genuinely holds all of
// history says so with Claimed true and From at the zero time, which is a claim
// somebody made rather than the absence of one.
type Mark struct {
	From    time.Time
	Claimed bool
}

// Complete reports whether a query reaching back to bound can be answered from
// the fast tier alone. An unclaimed namespace is never complete.
func (m Mark) Complete(bound time.Time) bool {
	return m.Claimed && !bound.Before(m.From)
}

// CoversAll reports whether the fast tier claims every document, however old.
//
// This is what makes a query with no lower time bound answerable. Such a query
// can reach arbitrarily far back, so a watermark part way through history
// cannot cover it, and only a claim on all of history can.
func (m Mark) CoversAll() bool {
	return m.Claimed && m.From.IsZero()
}

// Store reads and writes watermarks on the cold tier.
type Store struct {
	coll *mongo.Collection
}

// New returns a Store backed by the cold tier.
func New(cold *mongo.Client) *Store {
	return &Store{coll: cold.Database(meta.DB).Collection(meta.Watermarks)}
}

// Load returns the recorded completeness for ns. A missing watermark is not an
// error; it returns an unclaimed Mark, meaning nothing is guaranteed.
func (s *Store) Load(ctx context.Context, ns string) (Mark, error) {
	var wm meta.Watermark
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: ns}}).Decode(&wm)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Mark{}, nil
	}
	if err != nil {
		return Mark{}, fmt.Errorf("load watermark for %s: %w", ns, err)
	}
	return Mark{From: wm.CompleteFrom.UTC(), Claimed: true}, nil
}

// Clear withdraws the claim for ns, so nothing is guaranteed until something
// records a new one.
//
// This is the recovery path after the fast tier loses documents by any route
// the evictor did not drive: a restore, a rebuild, a dropped collection, an
// accident. The old claim describes a tier that no longer exists, and leaving
// it in place makes the router answer from data that is not there.
func (s *Store) Clear(ctx context.Context, ns string) (bool, error) {
	res, err := s.coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: ns}})
	if err != nil {
		return false, fmt.Errorf("clear watermark for %s: %w", ns, err)
	}
	return res.DeletedCount > 0, nil
}

// All returns every recorded watermark, for reporting.
func (s *Store) All(ctx context.Context) (map[string]Mark, error) {
	cur, err := s.coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list watermarks: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	out := map[string]Mark{}
	for cur.Next(ctx) {
		var wm meta.Watermark
		if err := cur.Decode(&wm); err != nil {
			return nil, fmt.Errorf("list watermarks: %w", err)
		}
		out[wm.NS] = Mark{From: wm.CompleteFrom.UTC(), Claimed: true}
	}
	return out, cur.Err()
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
