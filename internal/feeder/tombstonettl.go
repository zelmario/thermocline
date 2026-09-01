package feeder

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// tombstoneTTLIndex is the name of the archive-side index that expires the
// copies of documents the application deleted.
const tombstoneTTLIndex = "tc_tombstone_ttl"

// settleTombstoneTTL puts the archive's tombstone TTL index into the state
// feeder.tombstone_retention asks for, once per namespace per run.
//
// The mark is a date, so the whole expiry is one TTL index on it: mongod removes
// the copy that long after the delete, and a document without the mark is not
// touched because a TTL index ignores documents whose key is missing or is not a
// date. No sweep of our own, and nothing to get wrong on a namespace this feeder
// never sees.
//
// Zero retention drops the index rather than skipping the namespace. An operator
// who sets the knob and then removes it is asking for the audit trail back, and
// leaving mongod quietly expiring copies would make the config a lie.
//
// Failures warn and are remembered as settled anyway: retention is a property of
// the deployment, not of this batch, and refusing to mirror because an index
// could not be created would trade an unbounded archive for a stopped one.
func (f *Feeder) settleTombstoneTTL(ctx context.Context, ns string) {
	if f.ttlSettled == nil {
		f.ttlSettled = map[string]bool{}
	}
	if f.ttlSettled[ns] {
		return
	}
	f.ttlSettled[ns] = true

	db, coll := splitNS(ns)
	if db == "" || coll == "" {
		return
	}
	idx := f.cold.Database(db).Collection(coll).Indexes()
	secs := int32(f.cfg.TombstoneRetention.D().Seconds())

	if secs <= 0 {
		if err := dropTombstoneTTL(ctx, idx); err != nil {
			f.log.Warn("could not drop the tombstone expiry index",
				"ns", ns, "err", err,
				"note", "deleted documents may still be expiring in the archive; drop "+
					tombstoneTTLIndex+" by hand to stop it")
		}
		return
	}

	// createIndexes creates the collection when it does not exist, and an archive
	// collection made that way carries none of the fast tier's options. A
	// collation cannot be added to a collection afterwards at any time, so
	// creating it here would decide how every query against it compares strings,
	// for ever, from a housekeeping index.
	//
	// It can happen: a delete on a namespace the archive has never received
	// produces a tombstone op that matches nothing, and this used to run before
	// asking. Left unsettled instead, so the next tombstone in that namespace
	// tries again once there is something to index.
	if exists, err := f.archiveHas(ctx, db, coll); err != nil || !exists {
		f.ttlSettled[ns] = false
		if err != nil {
			f.log.Warn("could not tell whether the archive holds this collection yet",
				"ns", ns, "err", err)
		}
		return
	}

	model := mongo.IndexModel{
		Keys:    bson.D{{Key: meta.TombstoneField, Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(secs).SetName(tombstoneTTLIndex),
	}
	_, err := idx.CreateOne(ctx, model)
	if err == nil {
		f.log.Info("deleted documents expire from the archive",
			"ns", ns, "after", f.cfg.TombstoneRetention.String())
		return
	}
	// A retention that changed value collides with the index already there,
	// which is the ordinary case on the second setting rather than an error.
	if !optionsConflict(err) {
		f.log.Warn("could not create the tombstone expiry index",
			"ns", ns, "err", err,
			"note", "deleted documents stay in the archive until this succeeds")
		return
	}
	if err := dropTombstoneTTL(ctx, idx); err != nil {
		f.log.Warn("could not replace the tombstone expiry index", "ns", ns, "err", err)
		return
	}
	if _, err := idx.CreateOne(ctx, model); err != nil {
		f.log.Warn("could not recreate the tombstone expiry index", "ns", ns, "err", err)
		return
	}
	f.log.Info("deleted documents now expire from the archive on a new schedule",
		"ns", ns, "after", f.cfg.TombstoneRetention.String())
}

// archiveHas reports whether the archive already holds the collection.
//
// One filtered listCollections, which reads the catalog rather than the data.
func (f *Feeder) archiveHas(ctx context.Context, db, coll string) (bool, error) {
	names, err := f.cold.Database(db).ListCollectionNames(ctx,
		bson.D{{Key: "name", Value: coll}})
	if err != nil {
		return false, err
	}
	return len(names) > 0, nil
}

// dropTombstoneTTL removes the index and treats its absence as success.
func dropTombstoneTTL(ctx context.Context, idx mongo.IndexView) error {
	err := idx.DropOne(ctx, tombstoneTTLIndex)
	if err == nil || indexMissing(err) {
		return nil
	}
	return err
}

// indexMissing reports the IndexNotFound that a drop of an absent index gives,
// and the NamespaceNotFound of an archive collection that does not exist yet.
func indexMissing(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Code == 27 || ce.Code == 26
	}
	return false
}

// optionsConflict reports an existing index with the same name and different
// options, which is what a changed retention looks like.
func optionsConflict(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		// IndexOptionsConflict, IndexKeySpecsConflict.
		return ce.Code == 85 || ce.Code == 86
	}
	return false
}
