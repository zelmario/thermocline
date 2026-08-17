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
func One(ctx context.Context, coll *mongo.Collection, doc bson.Raw) (bool, error) {
	_, err := coll.InsertOne(ctx, doc)
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
func Many(ctx context.Context, coll *mongo.Collection, docs []bson.Raw) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	models := make([]mongo.WriteModel, len(docs))
	for i, d := range docs {
		models[i] = mongo.NewInsertOneModel().SetDocument(d)
	}
	// Unordered, so one document the fast tier already holds does not stop the
	// rest from being moved.
	res, err := coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	inserted := 0
	if res != nil {
		inserted = int(res.InsertedCount)
	}
	if err == nil {
		return inserted, nil
	}
	if onlyDuplicates(err) {
		return inserted, nil
	}
	return inserted, fmt.Errorf("promote into %s: %w", coll.Name(), err)
}

// onlyDuplicates reports whether a bulk write failed for no reason other than
// documents the fast tier already held.
func onlyDuplicates(err error) bool {
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) {
		return false
	}
	if bwe.WriteConcernError != nil {
		return false
	}
	if len(bwe.WriteErrors) == 0 {
		return false
	}
	for _, we := range bwe.WriteErrors {
		if we.Code != duplicateKey {
			return false
		}
	}
	return true
}

const duplicateKey = 11000
