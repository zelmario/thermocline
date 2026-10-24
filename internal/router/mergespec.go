package router

import (
	"context"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/wire"
)

// A merged read (see mergedread.go) asked the fast tier, and only once its
// answer was in did it ask the archive. Both are needed whenever the fast tier
// cannot prove it holds the whole answer, which on an adopted cluster is most
// of "my orders". On a namespace where most sorted reads end up merged, the
// archive is asked as the command arrives, the same way a point read is in
// pointspec.go, and for the same reasons the answer does not change:
//
//   - The feeder's position is read before the archive is asked, so the
//     archive's answer holds everything mirrored up to it.
//   - The oplog window ends at this router's cluster time after the fast
//     tier's reply. It is checked in two parts, up to the time the command
//     arrived, beside both reads, and the rest once the reply is in. The
//     second part is what catches a document the archive returned that was
//     evicted, updated or deleted after the archive was asked: any of those is
//     an entry in it, and the read takes the ordinary path.
//   - The ordinary path asks the fast tier first, so a document it did not
//     hold had left it before the archive was asked, and an eviction leaves
//     only what the archive already holds. Asked ahead, the archive may be
//     read before a document is mirrored, and the fast tier after it is
//     evicted: in neither half. An eviction is a delete in the window, so any
//     delete of the namespace in it, of whatever _id, sends the read to the
//     ordinary path, even when the archive returned nothing.
//
// The merge itself, and every condition on it, is mergedFind's: this only
// supplies the archive's half sooner.

// mergeScoreLimit bounds the score, as specScoreLimit does for point reads.
const mergeScoreLimit = 8

// mergeScore is how a namespace's recent sorted reads went: down one for each
// that arrived, up two for each that went on to be merged, so a namespace whose
// reads are mostly merged climbs and one whose fast tier mostly answers alone
// falls.
func (r *Router) mergeScore(ns string) *atomic.Int32 {
	if v, ok := r.mergeSpecScores.Load(ns); ok {
		return v.(*atomic.Int32)
	}
	v, _ := r.mergeSpecScores.LoadOrStore(ns, new(atomic.Int32))
	return v.(*atomic.Int32)
}

func (r *Router) noteMerge(ns string, delta int32) {
	s := r.mergeScore(ns)
	for {
		old := s.Load()
		next := old + delta
		if next > mergeScoreLimit {
			next = mergeScoreLimit
		}
		if next < -mergeScoreLimit {
			next = -mergeScoreLimit
		}
		if next == old || s.CompareAndSwap(old, next) {
			return
		}
	}
}

// mergeSpec is the archive's half of a merged read, asked ahead.
type mergeSpec struct {
	ns    string
	reqID int32
	from  bson.Timestamp
	early bson.Timestamp
	tc0   *tailCheck
	done  chan mergeSpecResult
}

type mergeSpecResult struct {
	docs []bson.Raw
	err  error
}

func (c *conn) startMergeSpec(ctx context.Context, cmd bson.Raw, db, ns string, reqID int32) {
	plan, ok := mergePlanOf(cmd)
	if !ok || inTransaction(cmd) {
		return
	}
	if _, viaView, tiered := c.r.tieredBase(ctx, ns); !tiered || viaView || c.r.isViewNS(ctx, ns, viaView, tiered) {
		return
	}
	worth := c.r.mergeScore(ns).Load() > 0
	c.r.noteMerge(ns, -1)
	if !worth || c.r.notSupersetCached(ns) != 0 {
		return
	}
	if _, has := c.r.defaultCollation(ctx, ns); has {
		return
	}
	got := c.r.recentPosition(ctx)
	if !got.known || got.ts.IsZero() {
		return
	}
	rewritten, _, err := rewriteForArchive(cmd, nil, nil, false)
	if err != nil {
		return
	}
	spec := &mergeSpec{ns: ns, reqID: reqID, from: got.ts,
		early: checkpoint.UnpackTS(c.r.seenOpTime.Load()), done: make(chan mergeSpecResult, 1)}
	if checkpoint.PackTS(spec.early) > checkpoint.PackTS(spec.from) {
		spec.tc0 = c.r.startTailCheck(context.WithoutCancel(ctx), ns, spec.from, spec.early)
	}
	c.r.Stats.MergesSpeculated.Add(1)
	go func() {
		actx := context.WithoutCancel(ctx)
		var res mergeSpecResult
		cur, err := c.r.cold.Database(db).RunCommandCursor(actx, rewritten)
		if err == nil {
			for len(res.docs) < plan.limit && cur.Next(actx) {
				res.docs = append(res.docs, append(bson.Raw(nil), cur.Current...))
			}
			err = cur.Err()
			_ = cur.Close(actx)
		}
		res.err = err
		spec.done <- res
	}()
	c.mspec = spec
}

// takeMergeSpec is the archive's half of mergedFind from the read asked ahead,
// with the oplog check of the whole window done. ok false means there is none
// for this read, and mergedFind asks the archive itself; refused means the
// window could not be cleared and the read takes the ordinary path.
func (c *conn) takeMergeSpec(ctx context.Context, ns string, msg *wire.Message) (archived []bson.Raw, ok, refused bool, why string) {
	spec := c.mspec
	c.mspec = nil
	if spec == nil || spec.ns != ns || spec.reqID != msg.Header.RequestID {
		return nil, false, false, ""
	}
	until := checkpoint.UnpackTS(c.r.seenOpTime.Load())
	if until.IsZero() {
		return nil, true, true, "no cluster time seen yet"
	}
	behind := checkpoint.PackTS(until) > checkpoint.PackTS(spec.from)
	if behind && !c.r.windowCheckable(ctx, spec.from, until) {
		return nil, true, true, "the feeder is further behind than the oplog is read for"
	}
	lateFrom := spec.from
	if checkpoint.PackTS(spec.early) > checkpoint.PackTS(lateFrom) {
		lateFrom = spec.early
	}
	var tc1 *tailCheck
	late := checkpoint.PackTS(until) > checkpoint.PackTS(lateFrom)
	if late {
		tc1 = c.r.startTailCheck(ctx, ns, lateFrom, until)
	}
	ta := time.Now()
	res := <-spec.done
	c.r.Metrics.RecordStage(stageMergeArchive, time.Since(ta))
	if res.err != nil {
		return nil, true, true, "the archive read failed: " + res.err.Error()
	}
	if behind {
		ids := make(bson.A, 0, len(res.docs))
		for _, d := range res.docs {
			id, err := d.LookupErr("_id")
			if err != nil {
				return nil, true, true, "an archived document has no _id"
			}
			ids = append(ids, id)
		}
		// Any delete in the window counts, not only of what the archive
		// returned. See the comment at the top.
		if checkpoint.PackTS(spec.early) > checkpoint.PackTS(spec.from) {
			if touched, err := c.r.oplogTouchedWith(ctx, spec.tc0, ns, spec.from, spec.early, ids, true); err != nil || touched {
				c.r.Stats.MergeFallbacks.Add(1)
				return nil, true, true, "something the archive returned, or a document it did not hold yet, changed since the feeder's position"
			}
		}
		if late {
			if touched, err := c.r.oplogTouchedWith(ctx, tc1, ns, lateFrom, until, ids, true); err != nil || touched {
				c.r.Stats.MergeFallbacks.Add(1)
				return nil, true, true, "something the archive returned, or a document it did not hold yet, changed since the feeder's position"
			}
		}
	}
	c.r.Stats.MergesSpecUsed.Add(1)
	return res.docs, true, false, ""
}
