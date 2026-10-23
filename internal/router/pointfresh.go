package router

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/zelmario/thermocline/internal/wire"

	"github.com/zelmario/thermocline/internal/checkpoint"
)

// A read of one document by _id that the fast tier did not have goes to the
// archive, and used to wait out the freshness barrier first: until mirroring
// had passed every write this router had seen, on every collection. On the
// store benchmark that wait was most of the read. An old order looked up for
// support took 16.0ms at the median against 1.2ms on MongoDB alone, while the
// order itself had not been written for months.
//
// For this read only one write can make the archive's answer wrong. The fast
// tier has just said it does not hold the document, and every write that puts
// a document somewhere puts it on the fast tier: an insert lands there, an
// update copies an archived document up before it changes it, and the evictor
// removes a copy only once the archive holds the same one. So the archive's
// copy is the truth unless the document was deleted and the tombstone has not
// been mirrored yet. That is the case the barrier exists for: a document
// deleted and then found again.
//
// A delete is in the fast tier's oplog, so the question can be asked there
// instead of waited for. Every entry after the feeder's published position is
// one the archive may not reflect yet, and that is a few dozen entries; a ts
// bound on the oplog seeks rather than scans, so asking is one indexed read on
// the fast tier. The oplog holds every router's and every client's writes, not
// only this router's, which is what the barrier promised and what this keeps.
//
// It reads at afterClusterTime seenOpTime, the same bar the barrier waits for,
// so a delete acknowledged before this read began is visible to it even if it
// was acknowledged with w:1 and an earlier write still holds a hole in the
// oplog.
//
// Anything it cannot reason about waits as before: a command entry in the
// window, because a delete can sit inside applyOps or a transaction, a feeder
// whose position carries no cluster time, a window the oplog no longer reaches
// back to or that holds more than windowMaxEntries, and any error.

// pointCheckWindow is the gap between the feeder's position and this router's
// cluster time that is checked without first asking how much oplog it holds.
const pointCheckWindow = 2 * time.Second

// windowMaxEntries is the most oplog entries a window may hold and still be
// checked rather than waited for. More means the feeder is behind, and a scan
// of that much oplog on the read path is not cheaper than the wait it saves.
const windowMaxEntries = 500

// windowCheckable reports whether the fast tier's oplog after from, up to
// until, can be asked what changed instead of waiting for mirroring.
//
// A gap in seconds alone said "behind" when the feeder was idle. A feeder's
// position moves with an event or an empty poll's resume token, and on an idle
// replica set nothing moves the cluster time but the periodic no-op writer,
// every ten seconds by default. So the first write after a few quiet seconds
// opened a gap of seconds holding one entry, the write itself, and the read
// behind it waited. Measured on testfresh: one run in five refused its merged
// read this way, at 2.4 seconds and one entry.
//
// So a wider gap is measured in entries, and the oplog must still reach back
// to from: an entry at or before it, or the window's entries are not all
// there. Both are cheap, the first entry and a ts bound being seeks.
func (r *Router) windowCheckable(ctx context.Context, from, until bson.Timestamp) bool {
	if time.Duration(until.T-from.T)*time.Second <= pointCheckWindow {
		return true
	}
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	oplog := r.hot.Database("local", options.Database().SetReadPreference(readpref.Primary())).Collection("oplog.rs")
	tsOnly := bson.D{{Key: "ts", Value: 1}}
	var first struct {
		TS bson.Timestamp `bson:"ts"`
	}
	if err := oplog.FindOne(cctx, bson.D{}, options.FindOne().
		SetSort(bson.D{{Key: "$natural", Value: 1}}).SetProjection(tsOnly)).Decode(&first); err != nil {
		return false
	}
	if checkpoint.PackTS(first.TS) > checkpoint.PackTS(from) {
		return false
	}
	cur, err := oplog.Find(cctx, bson.D{{Key: "ts", Value: bson.D{{Key: "$gt", Value: from}}}}, options.Find().
		SetProjection(tsOnly).SetLimit(windowMaxEntries+1).SetBatchSize(windowMaxEntries+1))
	if err != nil {
		return false
	}
	defer cur.Close(cctx)
	n := 0
	for cur.Next(cctx) {
		n++
	}
	return cur.Err() == nil && n <= windowMaxEntries
}


// pointReadInParallel answers a read of one document by _id from the archive
// with the oplog check and the archive read run at the same time. ok false
// means the check did not clear it, or the read was not one batch, and the
// caller waits for mirroring and reads again, as every read did before.
//
// The two were one after the other, and the read is all round trips: the fast
// tier's miss, the check, the archive. On the Docker test server a point read
// took 1.65ms through the router against 0.41ms on the archive alone, and the
// check went to the oplog for the entries its stream had not reached in two
// reads of three. They do not depend on each other once the window is read,
// and it is read first: from is where the archive is current to, so the archive
// read that follows holds everything mirrored up to it.
func (c *conn) pointReadInParallel(ctx context.Context, msg *wire.Message, db, coll string, id bson.RawValue,
	rewritten bson.D, batchSize int, budget time.Duration, count bool) (*wire.Message, bool) {
	ns := db + "." + coll
	from, until, ok := c.r.pointWindow(ctx)
	if !ok {
		return nil, false
	}
	fresh := make(chan bool, 1)
	go func() { fresh <- c.r.noDeletePendingIn(ctx, ns, id, from, until) }()
	cur, err := c.r.cold.Database(db).RunCommandCursor(ctx, withRemainingTime(rewritten, budget, 0))
	var batch bson.A
	more := false
	if err == nil {
		batch, more, err = readBatch(ctx, cur, batchSize)
		_ = cur.Close(ctx)
	}
	if !<-fresh || err != nil || more {
		return nil, false
	}
	c.r.Stats.PointReadsUnwaited.Add(1)
	if count {
		c.r.Stats.Archived.Add(1)
		c.cls = ClassArchive
	}
	out, err := cursorReply(msg.Header.RequestID, msg.Header.RequestID, 0, ns, "firstBatch", batch)
	if err != nil {
		return nil, false
	}
	return out, true
}

// noDeletePending reports whether the archive's copy of the document with this
// _id is certainly not one the application has deleted and whose tombstone is
// still being mirrored. False means wait for mirroring. See the top of this
// file for why a delete is the only write that can make that copy wrong.
func (r *Router) noDeletePending(ctx context.Context, ns string, id bson.RawValue) bool {
	from, until, ok := r.pointWindow(ctx)
	if !ok {
		return false
	}
	return r.noDeletePendingIn(ctx, ns, id, from, until)
}

// pointWindow is the window a point read checks: from the feeder's published
// position, which is where the archive is current to, to the newest cluster
// time this router has seen. Read before the archive is, so everything
// mirrored up to from is in what the archive then returns. ok false means a
// point read cannot be checked and waits.
func (r *Router) pointWindow(ctx context.Context) (from, until bson.Timestamp, ok bool) {
	if r.cfg.ArchiveWait.D() <= 0 || r.cfg.FeederID == "" {
		return from, until, false
	}
	got := r.recentPosition(ctx)
	if !got.known || got.ts.IsZero() {
		return from, until, false
	}
	until = checkpoint.UnpackTS(r.seenOpTime.Load())
	if until.IsZero() {
		return from, until, false
	}
	return got.ts, until, true
}

// noDeletePendingIn is noDeletePending for a window already read.
func (r *Router) noDeletePendingIn(ctx context.Context, ns string, id bson.RawValue, from, until bson.Timestamp) bool {
	if checkpoint.PackTS(until) <= checkpoint.PackTS(from) {
		// Mirrored past every write this router has seen already.
		return true
	}
	if touched, rest, known := r.touches.ask(ns, from, until, []bson.RawValue{id}, true); known {
		if touched || checkpoint.PackTS(rest) >= checkpoint.PackTS(until) {
			r.Stats.OplogChecksInMemory.Add(1)
			return !touched
		}
		// Only what the stream has not reached yet is read.
		from = rest
		r.Stats.OplogChecksTail.Add(1)
	} else {
		r.Stats.OplogChecksQueried.Add(1)
	}
	if !r.windowCheckable(ctx, from, until) {
		return false
	}
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	// Only a delete matters here, at the top level or inside an applyOps:
	// see the top of this file. An applyOps of inserts and updates, which is
	// what an insertMany is on 8.0, cannot hide one.
	filter := oplogWindow(ns, from,
		bson.A{bson.D{{Key: "ns", Value: ns}, {Key: "op", Value: "d"}, {Key: "o._id", Value: id}}},
		bson.D{{Key: "ns", Value: ns}, {Key: "op", Value: bson.D{{Key: "$nin", Value: bson.A{"i", "u"}}}}})
	var out struct {
		Cursor struct {
			FirstBatch []bson.Raw `bson:"firstBatch"`
		} `bson:"cursor"`
	}
	err := r.hot.Database("local").RunCommand(cctx, bson.D{
		{Key: "find", Value: "oplog.rs"},
		{Key: "filter", Value: filter},
		{Key: "projection", Value: bson.D{{Key: "ts", Value: 1}}},
		{Key: "limit", Value: int32(1)},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}, {Key: "afterClusterTime", Value: until}}},
	}, options.RunCmd().SetReadPreference(readpref.Primary())).Decode(&out)
	return err == nil && len(out.Cursor.FirstBatch) == 0
}
