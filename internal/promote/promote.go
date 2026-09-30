// Package promote moves documents from the archive into the fast tier without
// changing them.
//
// Every promotion used to be written as an upsert carrying $setOnInsert, which
// gave the semantics the design needs: insert this document, unless the fast
// tier already holds it, in which case its copy is the newer one and must not be
// overwritten.
//
// It also silently rewrote the document. MongoDB builds a document created by
// update modifiers from the field paths the modifiers name, and it applies them
// in lexicographic order, so the document that landed in the fast tier had its
// fields sorted. Measured on {_id, zeta, alpha, mid, name} held only in the
// archive: the archive stored that order, a read served from the archive
// returned that order, and the moment the document was copied up the fast tier
// held {_id, alpha, mid, name, zeta}. The feeder then mirrored that copy back
// down, so the archive's faithful version was overwritten by the reordered one
// and the original order was gone from both tiers.
//
// BSON is an ordered format and MongoDB preserves field order on insert, so an
// application that round-trips a document, compares two serialisations, or
// signs one, sees a document it did not write. Nothing else in this system
// reorders anything, and a tiering layer that quietly rewrites every document
// it ever serves is not a drop-in.
//
// An ordinary insert has exactly the semantics the upsert was chosen for and
// preserves the document byte for byte. A duplicate key error is not a failure
// here, it is the "already present" case, which is what $setOnInsert expressed
// by matching and doing nothing.
package promote

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// One inserts a single document unless the fast tier already holds it, and
// reports whether this call is what put it there.
//
// Validation is bypassed for the same reason the feeder bypasses it going the
// other way: a validator is a rule about what the application may write, and
// this is not the application writing. It is a document the archive already
// holds being placed where a read can reach it faster. Refusing it here would
// mean a collection whose validator was added after some of its documents were
// written could never serve those documents from the fast tier, and every read
// of one would fall through to the archive for ever.
func One(ctx context.Context, coll *mongo.Collection, doc bson.Raw) (bool, error) {
	_, err := coll.InsertOne(ctx, doc, options.InsertOne().SetBypassDocumentValidation(true))
	if err == nil {
		return true, nil
	}
	if mongo.IsDuplicateKeyError(err) {
		// The fast tier gained this document while we were reading the archive.
		// Its copy is the newer one, so leaving it alone is the whole point.
		return false, nil
	}
	return false, err
}

// Many inserts every document the fast tier does not already hold, and returns
// how many it actually inserted.
//
// The count is the number moved, not the number looked at. Callers depend on
// the difference: a zero means the fast tier was not missing any of them, and a
// transaction waiting to be told that replayed itself forever when the count
// included documents that were already there.
// It also reports the positions of the documents it did not insert, because the
// caller has usually already announced that a promotion is coming for each of
// them. A document the fast tier already held produces no insert and therefore
// no echo, so an announcement left standing for it is spent on whatever write
// comes next. That is not hypothetical: it silently swallowed the mirror of an
// updateMany, and the archive kept the old version of half the documents.
func Many(ctx context.Context, coll *mongo.Collection, docs []bson.Raw) (int, []int, error) {
	if len(docs) == 0 {
		return 0, nil, nil
	}
	models := make([]mongo.WriteModel, len(docs))
	for i, d := range docs {
		models[i] = mongo.NewInsertOneModel().SetDocument(d)
	}
	// Unordered, so one document the fast tier already holds does not stop the
	// rest from being moved.
	res, err := coll.BulkWrite(ctx, models,
		options.BulkWrite().SetOrdered(false).SetBypassDocumentValidation(true))
	inserted := 0
	if res != nil {
		inserted = int(res.InsertedCount)
	}
	if err == nil {
		return inserted, nil, nil
	}
	if skipped, ok := duplicatePositions(err); ok {
		return inserted, skipped, nil
	}
	return inserted, nil, fmt.Errorf("promote into %s: %w", coll.Name(), err)
}

// duplicatePositions reports which documents the fast tier already held, and
// whether that was the only thing that went wrong.
func duplicatePositions(err error) ([]int, bool) {
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) {
		return nil, false
	}
	if bwe.WriteConcernError != nil {
		return nil, false
	}
	if len(bwe.WriteErrors) == 0 {
		return nil, false
	}
	out := make([]int, 0, len(bwe.WriteErrors))
	for _, we := range bwe.WriteErrors {
		if we.Code != duplicateKey {
			return nil, false
		}
		out = append(out, we.Index)
	}
	return out, true
}

const duplicateKey = 11000

// Held reports which of these _ids the fast tier already holds, position for
// position.
//
// A copy-up asks before it records anything, because what it records is a
// promotion: a residency record saying this system is about to put the document
// on the fast tier, which a change stream reader takes as the reason an insert
// of that _id is not the application's. Recorded for a document the fast tier
// already held, no insert follows, and the record hides the application's own
// earlier insert of it from any stream read in the next thirty seconds instead.
// Measured on 2026-09-30: an order placed through the router, its customer's
// orders warmed by a transaction a moment later, and the order's insert missing
// from a stream read from before it, 4 of 4.
//
// And the insert is left out for those documents, which is the other half: a
// document the fast tier held when asked is one the application may delete
// before the insert runs, and inserting the archive's copy then puts back what
// it deleted.
//
// Matched back by the _id's bytes. An _id the fast tier holds under a
// different type or case compares equal to mongod and not here, so it is
// reported absent and inserted, and the duplicate key then leaves it alone as
// it always has.
func Held(ctx context.Context, coll *mongo.Collection, ids []bson.RawValue) ([]bool, error) {
	out := make([]bool, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	cur, err := coll.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("ask the fast tier which documents it holds: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	have := map[string]bool{}
	for cur.Next(ctx) {
		if id, lerr := cur.Current.LookupErr("_id"); lerr == nil {
			have[heldKey(id)] = true
		}
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("ask the fast tier which documents it holds: %w", err)
	}
	for i, id := range ids {
		out[i] = have[heldKey(id)]
	}
	return out, nil
}

func heldKey(id bson.RawValue) string {
	return string(append([]byte{byte(id.Type)}, id.Value...))
}
