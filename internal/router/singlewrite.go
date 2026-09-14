package router

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/wire"
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
		af, known := c.r.ageFieldOf(ns)
		if bound, bounded := lowerTimeBound(filter, af, known); bounded && mark.Complete(bound) {
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

// A sort says which of several matches the caller means, and the fast tier
// holds a subset.
//
// interceptWrite and interceptFindAndModify repair a write that matched
// nothing: the fast tier applied nothing, so the statement can be run again
// once the document is here. A sorted findAndModify that matched *something*
// never reaches them. The fast tier answered with its own first match, which is
// the first of the part it happens to hold, and the reply is a document the
// caller did not ask for.
//
// findOneAndDelete({}, {sort: {seq: 1}}) on a collection whose oldest documents
// are in the archive deletes the oldest one still cached. Nothing reports
// anything: a document matched, one document was modified, and the count is
// right.
//
// So the archive is asked which document the sort really means, and that one is
// brought up before the write runs. Before, not after: the fast tier has
// already applied the update by the time a reply could be inspected, and there
// is no way to take it back.
//
// One document is enough. The archive holds every live document the fast tier
// holds, so its best match by this sort is the collection's, and a document the
// fast tier has that the archive has not seen yet can only be one the fast tier
// would have picked anyway.
//
// findAndModify is the only single-document write that carries a sort. update
// and delete statements have no sort field, so the document they act on is
// whichever one matched, and matching nothing is the case already handled.
//
// Two of the shortcuts below hold only while a reply is coming. Without a sort
// any match will do, and an _id names one document outright: both of those send
// the command on, and interceptFindAndModify puts it right afterwards if the
// fast tier turned out to hold nothing. An unacknowledged findAndModify has no
// reply and nothing runs afterwards, so for those this guard is the only
// chance, and it takes neither shortcut. See the note above guardMultiWrite.
func (c *conn) guardSortedFindAndModify(ctx context.Context, msg *wire.Message, db string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	coll, ok := stringField(cmd, "findAndModify")
	if !ok {
		if coll, ok = stringField(cmd, "findandmodify"); !ok {
			return nil, false
		}
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}

	repliable := !msg.MoreToCome()

	// No sort means any match will do, which is what MongoDB itself promises,
	// so the fast tier's answer is as good as the archive's. As good, as long
	// as it has one: a fast tier holding no match at all answers nothing, and
	// unacknowledged there is no reply for interceptFindAndModify to correct.
	// oneArchivedMatch takes an empty sort, which is that same any-match.
	sortDoc, hasSort := cmd.Lookup("sort").DocumentOK()
	// An empty sort document is no sort: it names no order, so it cannot name
	// a document. Read once here so the two places below agree about it.
	sorted := hasSort && len(sortDoc) > 0
	if !sorted && repliable {
		return nil, false
	}

	var fdoc bson.Raw
	if qval, qerr := cmd.LookupErr("query"); qerr == nil {
		fdoc, _ = qval.DocumentOK()
	}
	// A filter pinned to one _id names its document outright, and a sort cannot
	// change which one that is. That settles which document is meant, not
	// whether the fast tier holds it, and only a reply settles the second half.
	// Unacknowledged, the walk below asks the archive for that same _id and
	// brings it up, which is what interceptFindAndModify would have done.
	if _, hasID := idFromFilter(fdoc); hasID && repliable {
		return nil, false
	}

	// Without a sort any match will do, so a fast tier that already matches
	// something ends it: only a fast tier matching nothing can lose the write.
	// With a sort the question is which document, not whether there is one, and
	// the fast tier's own best match is exactly what cannot be trusted.
	if !sorted {
		if has, herr := c.r.fastTierMatches(ctx, ns, fdoc, docField(cmd, "collation")); herr == nil && has {
			return nil, false
		}
	}

	if mark, known := c.r.completeness(ctx, ns); known && mark.Claimed {
		af, aok := c.r.ageFieldOf(ns)
		if bound, bounded := lowerTimeBound(fdoc, af, aok); bounded && mark.Complete(bound) {
			return nil, false
		} else if !bounded && mark.CoversAll() {
			return nil, false
		}
	}

	id, found, err := c.r.oneArchivedMatch(ctx, ns, fdoc, sortDoc)
	if err != nil {
		c.log.Warn("could not ask the archive which document this sort means, so the fast tier's answer stands",
			"ns", ns, "err", err)
		return nil, false
	}
	if !found {
		return nil, false
	}

	// Usually it is already here, and asking is much cheaper than finding out
	// by trying. A copy-up records residency, reads the whole document out of
	// the archive and then discovers the insert is a duplicate; this is one
	// indexed lookup by _id. It matters because the shape that reaches this
	// code most is a queue polled in a loop, where every poll is a sorted
	// findAndModify against the same collection.
	if c.r.fastTierHolds(ctx, ns, id) {
		return nil, false
	}

	copied, cerr := c.r.copyUp(ctx, ns, id)
	if cerr != nil {
		c.log.Warn("could not bring up the document this sort means, so the fast tier's answer stands",
			"ns", ns, "err", cerr)
		return nil, false
	}
	if !copied {
		// Already here, so the fast tier's own sort reaches it.
		return nil, false
	}

	c.r.Stats.WriteMisses.Add(1)
	c.cls = ClassMiss
	if inTransaction(cmd) {
		// The copy-up is an ordinary write outside the transaction, so this
		// transaction's snapshot was taken before it landed. Only replaying the
		// whole transaction makes the document visible to it.
		c.r.Stats.TxnReplays.Add(1)
		c.cls = ClassTxnWarm
		out, terr := c.r.replayTransaction(msg.Header.RequestID, msg.Header.RequestID,
			"the document this sort names in "+ns+" was in the archive and has been brought back to the fast tier")
		if terr != nil {
			return nil, false
		}
		return out, true
	}
	c.log.Debug("brought up the document a sorted findAndModify names", "ns", ns)
	return nil, false
}

// fastTierHolds reports whether the fast tier already has this document.
//
// Only ever used to skip work. A false answer costs a copy-up that turns out to
// be unnecessary, which is what would have happened anyway, so an error here is
// reported as "not held" and the caller carries on.
func (r *Router) fastTierHolds(ctx context.Context, ns string, id bson.RawValue) bool {
	db, coll := splitNS(ns)
	err := r.hot.Database(db).Collection(coll).
		FindOne(ctx, bson.D{{Key: "_id", Value: id}},
			options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).Err()
	return err == nil
}
