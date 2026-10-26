package router

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/wire"
)

// A read by _id that the fast tier does not hold is two round trips in a row:
// the fast tier's empty answer, then the archive. On a namespace where most
// such reads miss, the archive is asked at the same time as the fast tier, and
// its answer is already in hand when the fast tier's empty one arrives.
//
// On the Docker rig on 2026-10-24 the fast tier's part of an archived point
// read was 0.36ms of 1.21ms. Every hop there is inflated by the box's CPU
// sleep states (see the stage timings in pointfresh.go), but the hop itself is
// what a real server pays too, and this removes one from the sequence.
//
// It changes nothing about what the answer is allowed to be, and three things
// hold that in place:
//
//   - The window's start, the feeder's position, is read before the archive
//     is asked, as pointReadInParallel does, so the archive's answer holds
//     everything mirrored up to it.
//   - The window's end is this router's cluster time read after the fast
//     tier's reply, not before the archive was asked. It is later, so the
//     oplog check looks at more, never less.
//   - An empty answer from the archive is thrown away and the read goes the
//     ordinary way. The archive was asked before the fast tier answered, and
//     an eviction in between would otherwise read as a document that does not
//     exist.
//
// A found document is the truth for the same reasons pointfresh.go gives: the
// fast tier has said it does not hold it, every write that puts a document
// somewhere puts it there, and the oplog check rules out a delete the archive
// has not seen.
//
// What is tested and what is not. Taking out the oplog check of the window's
// first part brought back 20 of 20 deleted documents in testfresh, so the
// suites do reach this path. The second part, which catches an eviction made
// between the archive read and the fast tier's answer, is reasoned and not
// tested: that is a window of under a millisecond, and nothing outside the
// router can place an eviction inside it.
//
// Measured on the Docker rig on 2026-10-24, same mix: open one of my orders
// 1.45ms to 1.18ms at p50, an old order 1.47ms to 1.20ms, the whole archived
// point read 1.21ms to 1.03ms on average; 11 of 2,471 speculated reads wasted.
//
// What it costs is an archive read for every point read the fast tier
// answers while this is on. It is on for a namespace only while most of its
// recent point reads missed: see specScore.

// pointSpec is one archive read made ahead of the fast tier's answer.
type pointSpec struct {
	ns   string
	id   bson.RawValue
	from bson.Timestamp
	// early is the oplog check of the window from the feeder's position to
	// this router's cluster time when the command arrived, run beside the two
	// reads; takeSpec checks only what the fast tier's reply added after it.
	early     bson.Timestamp
	earlyDone chan bool
	done      chan specResult
}

type specResult struct {
	batch bson.A
	more  bool
	err   error
}

// specScoreScale is a whole miss rate in specScore's units, parts in ten
// thousand.
const specScoreScale = 10000

// specScore is a namespace's recent point read miss rate, a moving average in
// parts of specScoreScale: each read moves it a sixteenth of the way towards
// all or none. See speculates for the rate that asks the archive ahead.
func (r *Router) specScore(ns string) *atomic.Int32 {
	if v, ok := r.pointSpecScores.Load(ns); ok {
		return v.(*atomic.Int32)
	}
	v, _ := r.pointSpecScores.LoadOrStore(ns, new(atomic.Int32))
	return v.(*atomic.Int32)
}

// speculates says the recent miss rate is high enough to ask the archive
// ahead: three reads in four.
//
// It was a score up one per miss and down one per hit, speculating while
// positive. Half the reads that reach it missing is a fair coin, and a fair
// coin's walk spends half its time above zero, so the archive was asked ahead
// of half the reads with an even chance of wasting each. Measured on staging,
// YCSB latest over 100M records adopting from an empty fast tier: 1.0 million
// reads speculated, 499,781 wasted, an archive read each on a volume already
// 99% busy, about a tenth of everything that archive read. The gain from
// speculating is one round trip on a read that misses; the cost of a wasted
// one is a disk read on the tier with the least to spare.
func speculates(rate int32) bool { return rate >= specScoreScale*3/4 }

func (r *Router) notePointRead(ns string, missed bool) {
	s := r.specScore(ns)
	var target int32
	if missed {
		target = specScoreScale
	}
	for {
		old := s.Load()
		next := old + (target-old)/16
		if next == old && old != target {
			next = target
		}
		if s.CompareAndSwap(old, next) {
			return
		}
	}
}

// startPointSpec asks the archive for a point read before the fast tier has
// answered it, when the namespace's recent point reads say it will be needed.
// It leaves c.spec set for pointReadInParallel to take, or nil.
func (c *conn) startPointSpec(ctx context.Context, msg *wire.Message, db string) {
	c.spec, c.mspec = nil, nil
	if msg.Header.OpCode != wire.OpMsg {
		return
	}
	raw, err := msg.CommandDoc()
	if err != nil {
		return
	}
	// The command's values point into the client's message, which is reused
	// for the next one, and the archive read can outlive this command when
	// the fast tier answers first.
	cmd := append(bson.Raw(nil), raw...)
	coll, ok := stringField(cmd, "find")
	if !ok {
		return
	}
	ns := db + "." + coll
	fdoc, _ := cmd.Lookup("filter").DocumentOK()
	id, hasID := idFromFilter(fdoc)
	if !hasID {
		// A sorted read with a limit may be merged from both tiers instead.
		// See mergespec.go.
		c.startMergeSpec(ctx, cmd, db, ns, msg.Header.RequestID)
		return
	}
	// Only a filter that is the _id alone. The oplog check asks whether the
	// document was deleted, and that is the only write that can make the
	// archive's copy wrong when the fast tier does not hold it. With more in the
	// filter, the fast tier can hold it and not match, after an update the
	// archive has not seen yet. See answerPointRead.
	if !filterIsOnlyID(fdoc) || !speculates(c.r.specScore(ns).Load()) || inTransaction(cmd) {
		return
	}
	if _, honourable := archiveLevel(cmd); !honourable {
		return
	}
	if _, viaView, tiered := c.r.tieredBase(ctx, ns); !tiered || viaView || c.r.isViewNS(ctx, ns, viaView, tiered) {
		return
	}
	// The cached count, which costs nothing and refuses nothing: the read
	// itself still passes refuseIfNotSuperset before it can take this answer.
	if c.r.notSupersetCached(ns) != 0 {
		return
	}
	from, early, ok := c.r.pointWindow(ctx)
	if !ok {
		return
	}
	collation, _ := c.r.defaultCollation(ctx, ns)
	hintPred, keepHint := c.r.hintPredicate(ctx, ns, cmd)
	rewritten, batchSize, err := rewriteForArchive(cmd, collation, hintPred, keepHint)
	if err != nil {
		return
	}
	budget := maxTimeBudget(cmd)
	arrived := c.cmdStarted
	spec := &pointSpec{ns: ns, id: id, from: from, early: early,
		earlyDone: make(chan bool, 1), done: make(chan specResult, 1)}
	c.r.Stats.PointReadsSpeculated.Add(1)
	go func() {
		spec.earlyDone <- c.r.noDeletePendingIn(context.WithoutCancel(ctx), ns, id, from, early)
	}()
	go func() {
		// Not the command's context: the read may finish after the command
		// has, and cancelling an operation in flight closes its connection.
		actx := context.WithoutCancel(ctx)
		if budget > 0 {
			var cancel context.CancelFunc
			actx, cancel = context.WithTimeout(actx, budget)
			defer cancel()
		}
		var res specResult
		ts := time.Now()
		c.r.Metrics.RecordStage(stagePointSpecStart, ts.Sub(arrived))
		defer func() { c.r.Metrics.RecordStage(stagePointSpecRead, time.Since(ts)) }()
		cur, err := c.r.cold.Database(db).RunCommandCursor(actx, withRemainingTime(rewritten, budget, 0))
		if err == nil {
			res.batch, res.more, err = readBatch(actx, cur, batchSize)
			_ = cur.Close(actx)
		}
		res.err = err
		spec.done <- res
	}()
	c.spec = spec
}

// takeSpec answers the point read from the archive read started ahead of it,
// when there is one for this document. ok false means there was none, or it
// found nothing, or the oplog check did not clear it, and the read goes the
// way it would have.
func (c *conn) takeSpec(ctx context.Context, msg *wire.Message, ns string, id bson.RawValue, count bool) (*wire.Message, bool) {
	spec := c.spec
	c.spec = nil
	if spec == nil || spec.ns != ns || spec.id.Type != id.Type || !bytes.Equal(spec.id.Value, id.Value) {
		return nil, false
	}
	t0 := time.Now()
	// The rest of the window: what this router's cluster time moved to by the
	// fast tier's reply. A delete anywhere from the feeder's position to here
	// is in one of the two parts. This part is the one that catches an
	// eviction made after the archive was asked, which is why it has to end
	// after the fast tier's answer and not before.
	fresh := make(chan bool, 1)
	go func() {
		until, ok := c.r.seenUntil()
		if !ok {
			fresh <- false
			return
		}
		if checkpoint.PackTS(until) <= checkpoint.PackTS(spec.early) {
			fresh <- true
			return
		}
		fresh <- c.r.noDeletePendingIn(ctx, ns, id, spec.early, until)
	}()
	res := <-spec.done
	c.r.Metrics.RecordStage(stagePointArchive, time.Since(t0))
	early, late := <-spec.earlyDone, <-fresh
	c.r.Metrics.RecordStage(stagePointCheck, time.Since(t0))
	if !early || !late || res.err != nil || res.more || len(res.batch) == 0 {
		c.r.Stats.PointSpecWasted.Add(1)
		return nil, false
	}
	c.r.Stats.PointReadsUnwaited.Add(1)
	c.r.Stats.PointSpecUsed.Add(1)
	if count {
		c.r.Stats.Archived.Add(1)
		c.cls = ClassArchive
	}
	out, err := cursorReply(msg.Header.RequestID, msg.Header.RequestID, 0, ns, "firstBatch", res.batch)
	if err != nil {
		return nil, false
	}
	return out, true
}

// seenUntil is the end of a point read's window: the newest cluster time this
// router has seen, read now, after the fast tier's reply was noted.
func (r *Router) seenUntil() (bson.Timestamp, bool) {
	until := checkpoint.UnpackTS(r.seenOpTime.Load())
	return until, !until.IsZero()
}
