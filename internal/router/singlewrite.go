package router

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// A write that names one document by something other than its _id fell between
// the two paths that resolve a miss.
//
// interceptWrite acts on the way back, but only understood a filter pinning an
// exact _id. guardMultiWrite acts before the write and takes any filter, but
// only for statements that name many documents: multi:true for an update,
// limit:0 for a delete. deleteOne({email:"d@x"}) is neither, so the fast tier
// matched nothing, reported zero, and the archived document stayed where it was.
//
// Measured on a document held only in the archive: deleteOne by a field
// reported deletedCount 0 and the document was still readable through the
// router afterwards; updateOne by a field reported matched 0 and modified
// nothing; findOneAndDelete by a field returned null and left the document
// intact. All three reported success.
//
// For a delete that is the worst answer the system can give. An erasure request
// that reports success without erasing is the one failure nobody can audit their
// way out of afterwards, because the evidence that it did not happen is the
// record itself, sitting in a tier the application cannot see.
//
// It is resolved the same way the _id case is, and for the same reason: the fast
// tier matching nothing means it applied nothing, so the statement can simply be
// run again once the document it was about is here.

// resolveMissByFilter finds the document a single-document write would have
// acted on, had the fast tier held it.
//
// It returns the _id to copy up. Everything after that is the path the _id case
// already takes, which is deliberate: the copy-up, the transaction replay and
// the retry are all the same problem once the document is named.
func (c *conn) resolveMissByFilter(ctx context.Context, ns string, filter, sort bson.Raw) (bson.RawValue, bool, error) {
	// A collection Thermocline does not tier is whole on the fast tier, so its
	// zero is the truth and no archive lookup is owed.
	if !c.r.cfg.Manages(ns) {
		return bson.RawValue{}, false, nil
	}

	// A fast tier holding everything this filter can reach has nothing archived
	// that is not also here, so matching nothing here is matching nothing
	// anywhere. This is the same claim the read path trusts, and it keeps the
	// cost of this at zero for a collection that has been fully warmed.
	if mark, known := c.r.completeness(ctx, ns); known && mark.Claimed {
		if bound, bounded := lowerTimeBound(filter); bounded && mark.Complete(bound) {
			return bson.RawValue{}, false, nil
		} else if !bounded && mark.CoversAll() {
			return bson.RawValue{}, false, nil
		}
	}

	return c.r.oneArchivedMatch(ctx, ns, filter, sort)
}

// oneArchivedMatch returns the _id of one live archived document matching
// filter, choosing between several the way the caller's own sort asks.
//
// A single-document write acts on one of the documents it matches, and which
// one is MongoDB's choice unless a sort says otherwise. The fast tier matched
// nothing here, so every document the statement matches is in the archive, and
// the archive's own first match is therefore the document the statement would
// have found with nothing tiered underneath it.
func (r *Router) oneArchivedMatch(ctx context.Context, ns string, filter, sort bson.Raw) (bson.RawValue, bool, error) {
	db, coll := splitNS(ns)
	opts := options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})
	if len(sort) > 0 {
		opts.SetSort(sort)
	}
	var doc bson.Raw
	err := r.cold.Database(db).Collection(coll).FindOne(ctx, liveOnly(bson.RawValue{
		Type:  bson.TypeEmbeddedDocument,
		Value: filter,
	}), opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		// Not in the archive either, so matching nothing was the truth.
		return bson.RawValue{}, false, nil
	}
	if err != nil {
		return bson.RawValue{}, false, err
	}
	id, lerr := doc.LookupErr("_id")
	if lerr != nil {
		return bson.RawValue{}, false, lerr
	}
	return id, true, nil
}
