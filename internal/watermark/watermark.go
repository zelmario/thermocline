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
	// IndexGen counts unique index changes on this namespace, so a router can
	// tell that another one changed them without a second round trip.
	//
	// It rides on this document because this document is already read on the
	// hot path, once per ClaimCacheTTL per namespace, by every router that
	// serves the namespace. A unique index created on one router used to go
	// unenforced by the others for up to indexRefresh, a minute, and closing
	// that properly needs a broadcast nothing here has. This narrows it to the
	// claim's own five seconds and costs no round trip at all.
	//
	// Carried whether or not the namespace is Claimed: a DDL can happen before
	// anything has warmed, and dropping the number then would leave the other
	// routers on their old listing for the full minute.
	IndexGen int64

	// Dimension is the field the claim was measured in, and Measured says
	// whether the row recorded one at all.
	//
	// "The fast tier holds every live document newer than From" is only a
	// statement once you know what newer means. It is measured in the field the
	// rules tier by, and until this was recorded the row carried the instant
	// alone: change a rule's date_field and every existing claim is re-read in a
	// dimension it was never made in. Measured on twenty orders whose shippedAt
	// is thirty days after their createdAt, claimed complete from 15 January in
	// createdAt: with the field changed to shippedAt the router answered 6 where
	// the archive holds 20, and reported success.
	//
	// Measured is false for rows written before this existed. Those are used as
	// they always were, because refusing every claim on an upgrade would be a
	// worse failure than the one being fixed, and the next write stamps them.
	Dimension string
	Measured  bool
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

// ClaimCacheTTL is how long a reader may reuse a claim it has read.
//
// The router caches one, because reading it per query would put an archive
// round trip on the hot path. That makes the claim a value with two copies, and
// the copies disagree for as long as this.
//
// Which way they disagree is what matters. The evictor only ever raises a
// claim, narrowing it, so a stale copy is always the more permissive one: a
// router holding it believes the fast tier covers more than it does. Anything
// that writes a claim and then removes what it described has to let this much
// time pass in between, or every reader with a cached copy answers from a tier
// that no longer holds it.
//
// Measured with a fast tier claiming all of history and an eviction sweep
// removing 905 of 2,000 documents: the router answered countDocuments with
// 1,095 where the archive holds 2,000, and answered 2,000 again seven seconds
// later. No error either time.
const ClaimCacheTTL = 5 * time.Second

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
	var raw bson.Raw
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: ns}}).Decode(&raw)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Mark{}, nil
	}
	if err != nil {
		return Mark{}, fmt.Errorf("load watermark for %s: %w", ns, err)
	}

	return markOf(raw, ns)
}

// markOf reads one row into a Mark.
//
// The claim is the completeFrom field, not the row. Claimed used to come from
// the row existing, and completeFrom was decoded into a plain time.Time, so a
// row carrying nothing but its _id read as "complete from the zero time", which
// is the most permissive claim this system can hold: every range query on that
// namespace answered from the fast tier alone, however far back it reaches.
// Measured through `cline watermark` on a row inserted with no other field:
// "complete for all of history".
//
// Nothing here wrote such a row, which is why it was never seen, and that is
// exactly what made it worth closing: the next thing to $set a field on this
// collection with upsert:true would have created one, and so would an $unset, a
// hand-edited row, or a backup restored from an older schema. A row without the
// field is not a claim.
//
// One function for both readers on purpose. Load and All had the same two lines
// written out twice, and a fix to one of them would have left the other saying
// every row is a claim.
func markOf(raw bson.Raw, ns string) (Mark, error) {
	// Read before the claim, and returned even when there is no claim.
	var gen int64
	if g, gerr := raw.LookupErr("indexGen"); gerr == nil {
		if n, ok := g.AsInt64OK(); ok {
			gen = n
		}
	}
	v, lerr := raw.LookupErr("completeFrom")
	if lerr != nil {
		return Mark{IndexGen: gen}, nil
	}
	// Present but not a date is a different matter and stays an error: the
	// caller reports the claim as unreadable and refuses, rather than guessing
	// at what somebody meant.
	t, ok := v.TimeOK()
	if !ok {
		return Mark{}, fmt.Errorf("watermark for %s: completeFrom is %s, not a date",
			ns, v.Type.String())
	}
	m := Mark{From: t.UTC(), Claimed: true, IndexGen: gen}
	if d, derr := raw.LookupErr("dimension"); derr == nil {
		if name, ok := d.StringValueOK(); ok {
			m.Dimension, m.Measured = name, true
		}
	}
	return m, nil
}

// BumpIndexGen records that this namespace's unique indexes have changed.
//
// Upserted, because a namespace can have its indexes changed before anything
// has claimed it complete, and the row this creates carries no completeFrom and
// is therefore not a claim. That is exactly why Claimed had to stop meaning
// "the row exists" before this could be written: see markOf.
func (s *Store) BumpIndexGen(ctx context.Context, ns string) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: ns}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "indexGen", Value: 1}}}},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("record an index change for %s: %w", ns, err)
	}
	return nil
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

// ClearDatabase withdraws every claim for collections in one database, and
// returns how many it removed.
//
// For dropDatabase, which removes them all at once and names none of them. A
// claim is keyed by namespace, so the ones to withdraw are exactly those whose
// key begins with the database and a dot.
//
// A range rather than a regular expression: no escaping to get wrong, and the
// _id index answers it directly. The upper bound is the byte after '.', so it
// stops at the end of this database's names and reaches no further.
func (s *Store) ClearDatabase(ctx context.Context, db string) (int64, error) {
	res, err := s.coll.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{
		{Key: "$gte", Value: db + "."},
		{Key: "$lt", Value: db + "/"},
	}}})
	if err != nil {
		return 0, fmt.Errorf("clear watermarks for %s: %w", db, err)
	}
	return res.DeletedCount, nil
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
		var raw bson.Raw
		if err := cur.Decode(&raw); err != nil {
			return nil, fmt.Errorf("list watermarks: %w", err)
		}
		ns, ok := raw.Lookup("_id").StringValueOK()
		if !ok {
			continue
		}
		// A row that is not a claim is left out of the listing rather than
		// reported as one covering all of history.
		mark, merr := markOf(raw, ns)
		if merr != nil {
			return nil, fmt.Errorf("list watermarks: %w", merr)
		}
		if !mark.Claimed {
			continue
		}
		out[ns] = mark
	}
	return out, cur.Err()
}

// Set records completeness unconditionally. Used by the warmer, which knows
// exactly how far it has filled.
func (s *Store) Set(ctx context.Context, ns string, from time.Time, dimension string) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: ns}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "completeFrom", Value: from.UTC()},
			{Key: "dimension", Value: dimension},
			{Key: "updatedAt", Value: time.Now().UTC()},
		}}},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("save watermark for %s: %w", ns, err)
	}
	return nil
}

// LowerFrom moves the claim to from, but only if it still says what the caller
// last saw: was when claimed is true, or no claim at all when it is false. It
// reports whether it wrote.
//
// For the warmer, which lowers the claim one batch at a time over a run that can
// last hours, while the evictor raises it to its cutoff on every sweep and then
// deletes what fell below. A plain Set from the warmer lands on top of a raise,
// and puts the claim back over documents the evictor has already removed.
// Measured with a warm reaching past the eviction cutoff: the evictor raised the
// claim, removed 74 documents, and the warmer's next batch wrote the claim back
// below them; reads then trusted a claim the fast tier was 74 documents short of
// until the next sweep raised it again.
//
// Compared against the value the caller itself last wrote, not one read at the
// start of a run, so it fails only when something else has moved the claim since.
func (s *Store) LowerFrom(ctx context.Context, ns string, from time.Time, dimension string, was time.Time, claimed bool) (bool, error) {
	filter := bson.D{{Key: "_id", Value: ns}}
	if claimed {
		filter = append(filter, bson.E{Key: "completeFrom", Value: was.UTC()})
	} else {
		filter = append(filter, bson.E{Key: "completeFrom", Value: bson.D{{Key: "$exists", Value: false}}})
	}
	res, err := s.coll.UpdateOne(ctx, filter,
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "completeFrom", Value: from.UTC()},
			{Key: "dimension", Value: dimension},
			{Key: "updatedAt", Value: time.Now().UTC()},
		}}},
		options.UpdateOne().SetUpsert(!claimed))
	if err != nil {
		// No claim expected and one found: the upsert's insert collides with
		// the row that holds it, which is the same answer as matching nothing.
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("lower watermark for %s: %w", ns, err)
	}
	return res.MatchedCount > 0 || res.UpsertedCount > 0, nil
}

// RaiseTo moves the watermark later, never earlier, and reports whether it
// changed anything.
//
// The evictor calls this before removing documents older than `from`. Raising
// the watermark shrinks what the fast tier claims to hold completely, which is
// the safe direction: the worst case is a redundant lookup in the archive.
//
// It narrows a claim and never makes one. A namespace with no watermark claims
// nothing, and that is the state every adoption starts in: the archive holds
// the collection, the fast tier has never held any of it, and no warm has run.
// Creating a claim there says the fast tier holds everything since the eviction
// boundary about a tier that holds nothing, and a sweep that scans zero
// documents and evicts zero was enough to do it.
//
// Measured on 4,000 orders in the archive and an empty fast tier: before the
// sweep the router answered a thirty day range with 240, the archive's own
// answer; after one sweep, a router started afterwards answered 0 and reported
// success. The router already running kept the older claim it had cached, which
// is why this waited for a restart to show itself.
//
// The claim belongs to whatever put documents there: the warmer, which knows
// how far back it filled, or an operator who has checked, with
// `cline watermark --complete-all`.
func (s *Store) RaiseTo(ctx context.Context, ns string, from time.Time, dimension string) (bool, error) {
	res, err := s.coll.UpdateOne(ctx,
		bson.D{
			{Key: "_id", Value: ns},
			{Key: "completeFrom", Value: bson.D{{Key: "$lt", Value: from.UTC()}}},
		},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "completeFrom", Value: from.UTC()},
			{Key: "dimension", Value: dimension},
			{Key: "updatedAt", Value: time.Now().UTC()},
		}}})
	if err != nil {
		return false, fmt.Errorf("raise watermark for %s: %w", ns, err)
	}
	if res.MatchedCount > 0 {
		return true, nil
	}

	// No existing record matched. Either the watermark is already later, or
	// there is no watermark at all. Neither needs anything: there is no claim
	// to narrow, and one made here would be about documents nothing has put on
	// the fast tier.
	return false, nil
}
