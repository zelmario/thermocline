package router

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/livecache"
)

// The merged read, the unwaited point read and the live cache's merged fill
// each asked the fast tier's oplog whether a document they were about to take
// from the archive had been updated or deleted since the feeder's position.
// One find on local.oplog.rs per read, scanning every entry since that
// position: measured on the Docker test server under 1,500 writes a second,
// 0.8ms at 32 entries and 3.7ms at 3,000, on the primary every client is
// waiting on. The 200 million order adoption made about 185 of them a second,
// and the fast tier's primary went from 128% to 158% CPU against the run that
// waited instead.
//
// The tracker keeps the same answer in memory. A change stream on the fast
// tier carries every update, replace and delete of a tiered namespace, by _id
// and cluster time, and every command that could hide one. A question about a
// window it has seen to the end of is answered here, with no round trip. One
// it has not, because the stream is behind the read or the window starts
// before the tracker could see, still goes to the oplog.
//
// What it records is what the oplog check counts: updates and deletes by _id,
// compared the way mongod's _id index compares (idKey), and anything else that
// names the namespace or its database, DDL above all, which counts as touching
// every document in it. Inserts are left out, as the oplog check leaves them
// out: see touchesIDs.

// touchRetention is how far back the tracker can answer. A window starting
// earlier goes to the oplog, as does one wider than windowCheckable allows.
const touchRetention = time.Minute

// touchMaxIDs bounds the documents remembered. Past it the oldest half of the
// retention is forgotten early, which only sends more windows to the oplog.
const touchMaxIDs = 2_000_000

// touchWaitFor is how long a question waits for the stream to reach the end of
// its window before going to the oplog. The stream sees majority-committed
// writes only, so a read right behind a w:1 write can be ahead of it.
const touchWaitFor = 10 * time.Millisecond

type touchMark struct {
	upd, del uint64
}

type touchTracker struct {
	// waits and waitedNS count the questions that waited for the stream to
	// reach the end of their window, and how long they waited in all.
	waits, waitedNS atomic.Int64
	mu              sync.Mutex
	cond            *sync.Cond
	// applied is how far the stream has been read, start where it began,
	// horizon how far back what is held reaches. All packed.
	applied, start, horizon uint64
	// inclusive says every event at applied itself has been seen, not only
	// those before it. See advance.
	inclusive bool
	ids       map[string]map[string]touchMark
	ns        map[string]uint64
	db        map[string]uint64
	n         int
}

func newTouchTracker() *touchTracker {
	t := &touchTracker{ids: map[string]map[string]touchMark{}, ns: map[string]uint64{}, db: map[string]uint64{}}
	t.cond = sync.NewCond(&t.mu)
	return t
}

// startAt sets where the stream began, which may be before its first batch.
func (t *touchTracker) startAt(at uint64) {
	t.mu.Lock()
	if t.start == 0 {
		t.start = at
	}
	t.mu.Unlock()
}

// advance records that the stream has been read through at. inclusive says
// every event at at itself has been seen too, which holds after a batch ending
// on an event outside a transaction: that event is the only oplog entry at its
// time. It does not hold inside a transaction, whose events share one time and
// may be split across batches, nor for an empty poll's token, a mark one past
// the last entry where a later write can land.
func (t *touchTracker) advance(at uint64, inclusive bool) {
	t.mu.Lock()
	if at > t.applied || (at == t.applied && inclusive && !t.inclusive) {
		t.applied, t.inclusive = at, inclusive
		if t.start == 0 {
			t.start = at
		}
		t.prune()
		t.cond.Broadcast()
	}
	t.mu.Unlock()
}

// past says the stream has seen every event up to and including u. Called
// with mu held.
func (t *touchTracker) past(u uint64) bool {
	return t.applied > u || (t.applied == u && t.inclusive)
}

// touch records one event.
func (t *touchTracker) touch(op, db, coll string, id bson.RawValue, at uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.start == 0 {
		// Nothing is answered before the stream's first position.
		return
	}
	switch {
	case coll == "":
		if at > t.db[db] {
			t.db[db] = at
		}
	case op == "update" || op == "replace" || op == "delete":
		if id.Type == 0 {
			t.markNS(db+"."+coll, at)
			return
		}
		m := t.ids[db+"."+coll]
		if m == nil {
			m = map[string]touchMark{}
			t.ids[db+"."+coll] = m
		}
		k := idKey(id)
		mk, had := m[k]
		if op == "delete" {
			mk.del = max(mk.del, at)
		} else {
			mk.upd = max(mk.upd, at)
		}
		m[k] = mk
		if !had {
			t.n++
		}
	case op == "insert":
	default:
		t.markNS(db+"."+coll, at)
	}
}

func (t *touchTracker) markNS(ns string, at uint64) {
	if at > t.ns[ns] {
		t.ns[ns] = at
	}
}

// prune forgets what is older than the retention, or older than half of it
// when too much is held. Called with mu held.
func (t *touchTracker) prune() {
	keep := touchRetention
	if t.n > touchMaxIDs {
		keep /= 2
	}
	at := checkpoint.UnpackTS(t.applied)
	if at.T <= uint32(keep/time.Second) {
		return
	}
	cut := checkpoint.PackTS(bson.Timestamp{T: at.T - uint32(keep/time.Second)})
	if cut <= t.horizon+uint64(1)<<32 && t.n <= touchMaxIDs {
		// At most once a second.
		return
	}
	for ns, m := range t.ids {
		for k, mk := range m {
			if mk.upd <= cut && mk.del <= cut {
				delete(m, k)
				t.n--
			}
		}
		if len(m) == 0 {
			delete(t.ids, ns)
		}
	}
	t.horizon = max(t.horizon, cut)
}

// question says whether any of ids in ns may have changed in (from, until]:
// updated or deleted when deletesOnly is false, deleted when it is true, or
// anything done to the namespace or its database. known false means the
// tracker cannot say, and the oplog has to be asked.
func (t *touchTracker) question(ns string, from, until bson.Timestamp, ids []bson.RawValue, deletesOnly bool) (touched, known bool) {
	if t == nil {
		return false, false
	}
	f, u := checkpoint.PackTS(from), checkpoint.PackTS(until)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.start == 0 || f < t.start || f < t.horizon {
		return false, false
	}
	if !t.past(u) {
		t0 := time.Now()
		defer func() { t.waits.Add(1); t.waitedNS.Add(int64(time.Since(t0))) }()
		deadline := t0.Add(touchWaitFor)
		timer := time.AfterFunc(touchWaitFor, func() {
			t.mu.Lock()
			t.cond.Broadcast()
			t.mu.Unlock()
		})
		for !t.past(u) && time.Now().Before(deadline) {
			t.cond.Wait()
		}
		timer.Stop()
		if !t.past(u) {
			return false, false
		}
	}
	db, _ := splitNS(ns)
	if t.ns[ns] > f || t.db[db] > f {
		return true, true
	}
	m := t.ids[ns]
	for _, id := range ids {
		mk, ok := m[idKey(id)]
		if !ok {
			continue
		}
		if mk.del > f || (!deletesOnly && mk.upd > f) {
			return true, true
		}
	}
	return false, true
}

// followTouches keeps the tracker fed for as long as ctx lives.
func (r *Router) followTouches(ctx context.Context) {
	for ctx.Err() == nil {
		err := r.followTouchesOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		// A stream that ended may have missed events: start again knowing
		// nothing, so nothing is answered from a gap.
		r.touches.mu.Lock()
		r.touches.applied, r.touches.start, r.touches.horizon = 0, 0, 0
		r.touches.ids, r.touches.ns, r.touches.db, r.touches.n = map[string]map[string]touchMark{}, map[string]uint64{}, map[string]uint64{}, 0
		r.touches.mu.Unlock()
		r.log.Warn("the stream that answers the oplog check in memory stopped, starting it again",
			"err", err, "meanwhile", "merged reads ask the oplog, as they did before")
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (r *Router) followTouchesOnce(ctx context.Context) error {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "operationType", Value: bson.D{{Key: "$ne", Value: "insert"}}}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "operationType", Value: 1}, {Key: "ns", Value: 1}, {Key: "to", Value: 1}, {Key: "documentKey._id", Value: 1}, {Key: "clusterTime", Value: 1}, {Key: "txnNumber", Value: 1},
		}}},
	}
	opts := options.ChangeStream().SetMaxAwaitTime(5 * time.Millisecond).SetShowExpandedEvents(true)
	// From a retention ago rather than from now. The window a read asks about
	// starts at the feeder's position, which on an idle feeder stands still
	// and can be well before this router started: opened at now, the tracker
	// could say nothing about it.
	var from *bson.Timestamp
	if now, ok := r.fastTierClock(ctx); ok && now.T > uint32(touchRetention/time.Second) {
		from = &bson.Timestamp{T: now.T - uint32(touchRetention/time.Second)}
		opts.SetStartAtOperationTime(from)
	}
	cs, err := r.hot.Watch(ctx, pipeline, opts)
	if err != nil && from != nil {
		// An oplog that does not reach that far back: from now, then.
		from = nil
		cs, err = r.hot.Watch(ctx, pipeline, options.ChangeStream().SetMaxAwaitTime(5*time.Millisecond).SetShowExpandedEvents(true))
	}
	if err != nil {
		return err
	}
	if from != nil {
		// Every event after it is on its way, so a window from it is known
		// once the stream has been read to the window's end.
		r.touches.startAt(checkpoint.PackTS(*from))
	}
	defer func() { _ = cs.Close(context.WithoutCancel(ctx)) }()
	for {
		if cs.TryNext(ctx) {
			var ev struct {
				Op string `bson:"operationType"`
				NS struct {
					DB   string `bson:"db"`
					Coll string `bson:"coll"`
				} `bson:"ns"`
				To struct {
					DB   string `bson:"db"`
					Coll string `bson:"coll"`
				} `bson:"to"`
				Key struct {
					ID bson.RawValue `bson:"_id"`
				} `bson:"documentKey"`
				At  bson.Timestamp `bson:"clusterTime"`
				Txn *int64         `bson:"txnNumber"`
			}
			if err := cs.Decode(&ev); err != nil {
				return err
			}
			// A rename onto a tiered namespace names the source, which may
			// not be tiered.
			if ev.To.Coll != "" && r.cfg.Manages(ev.To.DB+"."+ev.To.Coll) {
				r.touches.touch(ev.Op, ev.To.DB, ev.To.Coll, bson.RawValue{}, checkpoint.PackTS(ev.At))
			}
			if ev.NS.Coll == "" || r.cfg.Manages(ev.NS.DB+"."+ev.NS.Coll) {
				id := ev.Key.ID
				id.Value = append([]byte(nil), id.Value...)
				r.touches.touch(ev.Op, ev.NS.DB, ev.NS.Coll, id, checkpoint.PackTS(ev.At))
			}
			if cs.RemainingBatchLength() == 0 {
				r.touches.advance(checkpoint.PackTS(ev.At), ev.Txn == nil)
				if at, ok := livecache.TokenTime(cs.ResumeToken()); ok {
					r.touches.advance(checkpoint.PackTS(at), false)
				}
			}
			continue
		}
		if err := cs.Err(); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if at, ok := livecache.TokenTime(cs.ResumeToken()); ok {
			r.touches.advance(checkpoint.PackTS(at), false)
		}
	}
}

// fastTierClock is the fast tier primary's clock, a ping's operationTime.
func (r *Router) fastTierClock(ctx context.Context) (bson.Timestamp, bool) {
	raw, err := r.hot.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}},
		options.RunCmd().SetReadPreference(readpref.Primary())).Raw()
	if err != nil {
		return bson.Timestamp{}, false
	}
	t, i, ok := raw.Lookup("operationTime").TimestampOK()
	return bson.Timestamp{T: t, I: i}, ok
}

func (r *Router) touchWaits() int64 {
	if r.touches == nil {
		return 0
	}
	return r.touches.waits.Load()
}

func (r *Router) touchWaitedUS() int64 {
	if r.touches == nil {
		return 0
	}
	return r.touches.waitedNS.Load() / 1000
}
