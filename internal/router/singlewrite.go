package router

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/zelmario/thermocline/internal/checkpoint"
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
func (c *conn) resolveMissByFilter(ctx context.Context, ns string, filter, sort, collation bson.Raw) (bson.RawValue, bool, error) {
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

	return c.r.oneArchivedMatch(ctx, ns, filter, sort, collation)
}

// oneArchivedMatch returns the _id of one live archived document matching
// filter, choosing between several the way the caller's own sort asks.
//
// A single-document write acts on one of the documents it matches, and which
// one is MongoDB's choice unless a sort says otherwise. The fast tier matched
// nothing here, so every document the statement matches is in the archive, and
// the archive's own first match is therefore the document the statement would
// have found with nothing tiered underneath it.
func (r *Router) oneArchivedMatch(ctx context.Context, ns string, filter, sort, collation bson.Raw) (bson.RawValue, bool, error) {
	if id, found, ok := r.archivedMatchUnwaited(ctx, ns, filter, sort, collation); ok {
		return id, found, nil
	}
	// Which document the write acts on is decided here, so the archive is
	// asked once mirroring has caught up, as a read would be. Otherwise its
	// first match can be a document the application has just deleted, and the
	// write then matches nothing where MongoDB would have found the next one.
	// The copy-up that follows waits too, but by then the choice is made.
	if err := r.archiveIsFresh(ctx, ns, 0, "choosing a document for a write from the archive before mirroring caught up",
		"a single-document write may act on, or miss because of, a document deleted in the last moments"); err != nil {
		return bson.RawValue{}, false, err
	}
	db, coll := splitNS(ns)
	opts := options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})
	if len(sort) > 0 {
		opts.SetSort(sort)
	}
	// In the comparison the fast tier already made: the caller's own collation
	// when the command carried one, the collection's when it did not. See
	// askingCollation.
	//
	// It decides both halves of this lookup and the sort is the sharper one.
	// The filter decides whether the archive holds a match at all, so getting
	// it wrong reports a single-document write as matching nothing. The sort
	// decides which of several matches the caller meant, and a collation is
	// what orders strings, so getting that wrong does not lose the write, it
	// puts it on a different document. Two lines above this call the fast tier
	// was already being asked with the collation, and this was not.
	if ec, ok := r.askingCollation(ctx, ns, collation); ok {
		if c := collationFromSpec(ec); c != nil {
			opts.SetCollation(c)
		}
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
		if has, herr := c.r.fastTierMatchesHeld(ctx, ns, fdoc, docField(cmd, "collation")); herr == nil && has {
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

	t0 := time.Now()
	id, found, err := c.r.oneArchivedMatch(ctx, ns, fdoc, sortDoc, docField(cmd, "collation"))
	c.r.Metrics.RecordStage(stageFamMatch, time.Since(t0))
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
	//
	// Held as well as asked. The answer is relied on until mongod runs the
	// sort, and an eviction sweep can take this document in between: the fast
	// tier's sort then picks the next one, and the client's command modifies
	// a record it did not name. holdWhatIsThere holds it, and reports it
	// absent if an eviction already under way takes it, so it is copied up.
	{
		db, coll := splitNS(ns)
		t1 := time.Now()
		have, _, herr := c.r.holdWhatIsThere(ctx, ns, c.r.hot.Database(db).Collection(coll), []bson.RawValue{id})
		c.r.Metrics.RecordStage(stageFamHold, time.Since(t1))
		if herr == nil && have[0] {
			return nil, false
		}
	}

	t2 := time.Now()
	copied, cerr := c.r.copyUp(ctx, ns, id)
	c.r.Metrics.RecordStage(stageFamCopyUp, time.Since(t2))
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

// unwaitedCandidates is how many of the archive's best matches are asked for,
// so that one changed since the feeder's position can be passed over.
const unwaitedCandidates = 8

// archivedMatchUnwaited chooses the document oneArchivedMatch would, without
// waiting for mirroring first. ok false means it could not, and the caller
// waits as before.
//
// The wait made the archive's best match current. The fast tier's oplog says
// instead which of the archive's best few have changed since the feeder's
// position. One that was inserted or updated since is on the fast tier, every
// write lands there, so the fast tier's own sort weighs it and it can be passed
// over. One that was deleted may have been evicted rather than deleted, and is
// still live, so that is not decided here. The first candidate nothing has
// touched is current in the archive, and it is the one. Shipping the oldest
// order waited here and again in its copy-up, measured at 29.3 ms against
// 8.2 ms on the old cluster alone.
func (r *Router) archivedMatchUnwaited(ctx context.Context, ns string, filter, sort, collation bson.Raw) (bson.RawValue, bool, bool) {
	if len(collation) > 0 || r.cfg.ArchiveWait.D() <= 0 || r.cfg.FeederID == "" {
		return bson.RawValue{}, false, false
	}
	if _, has := r.defaultCollation(ctx, ns); has {
		return bson.RawValue{}, false, false
	}
	got := r.recentPosition(ctx)
	if !got.known || got.ts.IsZero() {
		return bson.RawValue{}, false, false
	}
	until := checkpoint.UnpackTS(r.seenOpTime.Load())
	if until.IsZero() {
		return bson.RawValue{}, false, false
	}
	behind := checkpoint.PackTS(until) > checkpoint.PackTS(got.ts)
	if behind && !r.windowCheckable(ctx, got.ts, until) {
		return bson.RawValue{}, false, false
	}

	db, coll := splitNS(ns)
	opts := options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}).SetLimit(unwaitedCandidates)
	if len(sort) > 0 {
		opts.SetSort(sort)
	}
	cur, err := r.cold.Database(db).Collection(coll).Find(ctx, liveOnly(bson.RawValue{
		Type:  bson.TypeEmbeddedDocument,
		Value: filter,
	}), opts)
	if err != nil {
		return bson.RawValue{}, false, false
	}
	var ids []bson.RawValue
	for cur.Next(ctx) {
		id, err := cur.Current.LookupErr("_id")
		if err != nil {
			_ = cur.Close(ctx)
			return bson.RawValue{}, false, false
		}
		ids = append(ids, bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)})
	}
	cerr := cur.Err()
	_ = cur.Close(ctx)
	if cerr != nil {
		return bson.RawValue{}, false, false
	}
	if len(ids) == 0 {
		// Nothing matching in the archive, which mirroring cannot change: it
		// is behind only by writes that land on the fast tier first, and a
		// document the fast tier has that the archive has not seen yet is the
		// fast tier's own sort to pick. What a lag leaves in the archive is
		// more, a deleted document still live, never less.
		return bson.RawValue{}, false, true
	}
	if !behind {
		return ids[0], true, true
	}
	touched, err := r.oplogTouchedBy(ctx, ns, got.ts, until, ids)
	if err != nil {
		return bson.RawValue{}, false, false
	}
	for _, id := range ids {
		switch touched[idKeyOf(id)] {
		case "d", "c":
			return bson.RawValue{}, false, false
		case "i", "u":
			continue
		}
		return id, true, true
	}
	return bson.RawValue{}, false, false
}

// oplogTouchedBy is oplogTouched saying which of ids each entry touched and
// how: "i", "u" or "d" by _id, and "c" under every key when a command (a
// transaction's applyOps, DDL) ran, since it could hide any of them.
func (r *Router) oplogTouchedBy(ctx context.Context, ns string, from, until bson.Timestamp, ids []bson.RawValue) (map[string]string, error) {
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	arr := make(bson.A, len(ids))
	for i, id := range ids {
		arr[i] = id
	}
	// Inserts as well: one inserted since is on the fast tier, whose own sort
	// weighs it, so it can be passed over. See archivedMatchUnwaited.
	direct, inner := touchesIDs(ns, arr, true)
	filter := oplogWindow(ns, from, direct, inner)
	var out struct {
		Cursor struct {
			FirstBatch []bson.Raw `bson:"firstBatch"`
		} `bson:"cursor"`
	}
	err := r.hot.Database("local").RunCommand(cctx, bson.D{
		{Key: "find", Value: "oplog.rs"},
		{Key: "filter", Value: filter},
		{Key: "projection", Value: bson.D{{Key: "op", Value: 1}, {Key: "o._id", Value: 1}, {Key: "o2._id", Value: 1}}},
		{Key: "batchSize", Value: int32(1000)},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}, {Key: "afterClusterTime", Value: until}}},
	}, options.RunCmd().SetReadPreference(readpref.Primary())).Decode(&out)
	if err != nil {
		return nil, err
	}
	// Any command that matched, an applyOps holding something other than a
	// plain insert of another document, DDL, a commit, is taken as touching
	// every candidate: which of them it touched is not worth reading out.
	touched := map[string]string{}
	for _, e := range out.Cursor.FirstBatch {
		op, _ := e.Lookup("op").StringValueOK()
		if op == "c" {
			for _, id := range ids {
				touched[idKeyOf(id)] = "c"
			}
			return touched, nil
		}
		id, err := e.LookupErr("o2", "_id")
		if err != nil {
			id, err = e.LookupErr("o", "_id")
		}
		if err != nil {
			continue
		}
		k := idKeyOf(id)
		// A delete outranks anything else that happened to the same _id.
		if touched[k] != "d" {
			touched[k] = op
		}
	}
	return touched, nil
}

func idKeyOf(v bson.RawValue) string { return string(rune(v.Type)) + string(v.Value) }
