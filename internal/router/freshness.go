package router

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/wire"
)

// A read the fast tier cannot answer is sent to the archive, and the archive
// trails the fast tier by the feeder's mirroring lag. That makes such a read as
// fresh as a secondary read, which is not the same promise the fast tier makes,
// and the difference is visible in two ways:
//
//   - a write that has already returned to the client is not in the archive yet,
//     so a query that should match it does not;
//   - a document the client has already deleted is still live in the archive,
//     because its tombstone has not been mirrored, so a query returns it.
//
// The second is worse than staleness. Deleting a document and then finding it
// again is not a slow answer, it is a wrong one.
//
// So before answering from the archive the router waits for mirroring to reach
// the instant the read began. Every write that completed before then is in the
// archive by the time it is read. While the feeder keeps up, which is the
// normal case, the checkpoint is already past that instant and the wait returns
// immediately.
//
// "The instant the read began" used to be the only bar, and it is a reading of
// this router's clock compared against a reading of the feeder's. On separate
// hosts that is only as sound as the skew between two machines nothing keeps in
// step, and the dangerous direction is silent: a feeder whose clock runs ahead
// publishes a position past writes it has not mirrored, and a read that trusts
// it misses a write that had already returned to the client.
//
// So a cluster time is required as well, whenever both sides have one. It is
// the replica set's own logical clock, so neither daemon's clock comes into it:
// the feeder publishes the cluster time it has mirrored through, and the router
// keeps the newest operationTime the fast tier has reported, which every reply
// carries and which is therefore free. Any write already acknowledged to a
// client happened at or before that instant, so mirroring past it is mirroring
// past that write.
//
// Both bars, not either. Requiring both can only make the wait stricter, and it
// degrades to exactly the old behaviour when a feeder from before this
// publishes no cluster time, or when this router has forwarded no reply to take
// one from.

// checkpointPoll is how often the checkpoint is re-read while waiting.
const checkpointPoll = 25 * time.Millisecond

// checkpointFresh is how long a checkpoint reading is reused. Many reads can
// fall through at once, and they would otherwise all query the same small
// document on the archive at the same time.
const checkpointFresh = 20 * time.Millisecond

type checkpointCache struct {
	mu      sync.Mutex
	at      time.Time      // the feeder's position, by the clock
	ts      bson.Timestamp // and as a cluster time, when it published one
	known   bool
	fetched time.Time // when this reading was taken
}

// noteOpTime remembers the newest cluster time the fast tier has reported.
//
// It is strictly at or after every write the fast tier has already
// acknowledged, which is what makes it the right thing for a fallback read to
// wait on.
func (r *Router) noteOpTime(reply *wire.Message) {
	if reply == nil {
		return
	}
	doc, err := reply.ReplyDoc()
	if err != nil {
		return
	}
	t, i, ok := doc.Lookup("operationTime").TimestampOK()
	if !ok {
		return
	}
	want := checkpoint.PackTS(bson.Timestamp{T: t, I: i})
	for {
		got := r.seenOpTime.Load()
		if want <= got || r.seenOpTime.CompareAndSwap(got, want) {
			return
		}
	}
}

// mirroredThrough returns how far mirroring has got, reusing a recent reading.
func (r *Router) mirroredThrough(ctx context.Context) (time.Time, bson.Timestamp, bool) {
	r.ckptCache.mu.Lock()
	if !r.ckptCache.fetched.IsZero() && time.Since(r.ckptCache.fetched) < checkpointFresh {
		at, ts, known := r.ckptCache.at, r.ckptCache.ts, r.ckptCache.known
		r.ckptCache.mu.Unlock()
		return at, ts, known
	}
	r.ckptCache.mu.Unlock()

	// The position first. It says the same thing the checkpoint's eventAt says,
	// published on its own much faster cadence, and it is the difference
	// between this wait costing a couple of milliseconds and costing a whole
	// checkpoint interval. The checkpoint remains the fallback, for a feeder
	// that publishes no position.
	var at time.Time
	var ts bson.Timestamp
	var known bool
	rec, ok, err := r.positions.At(ctx, r.cfg.FeederID)
	if err == nil && ok && rec.Live(time.Now()) {
		at, ts, known = rec.At, rec.TS, true
	} else {
		at, known, err = r.checkpoints.EventAt(ctx, r.cfg.FeederID)
	}
	if err != nil {
		return time.Time{}, bson.Timestamp{}, false
	}

	r.ckptCache.mu.Lock()
	r.ckptCache.at, r.ckptCache.ts, r.ckptCache.known, r.ckptCache.fetched = at, ts, known, time.Now()
	r.ckptCache.mu.Unlock()
	return at, ts, known
}

// waitForArchive blocks until mirroring has reached until, and reports whether
// it got there.
//
// A false return is not a reason to refuse the read. The archive still holds
// every document; it is merely behind, which is the same guarantee this read
// had before any of this existed. The caller says so in the log rather than
// turning a slow answer into an error.
func (r *Router) waitForArchive(ctx context.Context, until time.Time) bool {
	wait := r.cfg.ArchiveWait.D()
	if wait <= 0 || r.cfg.FeederID == "" {
		return true
	}

	// Fixed before the loop: a cluster time read later would include writes made
	// while this read was already waiting, and this read never promised to see
	// those.
	untilTS := r.seenOpTime.Load()

	deadline := time.Now().Add(wait)
	for {
		at, ts, known := r.mirroredThrough(ctx)
		if known && caughtUp(at, ts, until, untilTS) {
			return true
		}
		// No checkpoint at all means nothing is mirroring. Waiting the full
		// timeout on every read would turn a stopped feeder into an outage, so
		// this gives up at once and lets the caller answer and warn.
		if !known {
			return false
		}
		if !time.Now().Before(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(checkpointPoll):
		}
	}
}

// archiveReadIsFresh waits for mirroring and counts what it had to do, so the
// two archive read paths behave the same way and report it the same way.
//
// budget is the caller's own maxTimeMS, or zero when they set none. The wait
// must not outlast it: a client that asked for an answer within fifty
// milliseconds and got one after five hundred was told its deadline did not
// apply here, which is not the proxy's decision to make.
func (c *conn) archiveReadIsFresh(ctx context.Context, ns string, started time.Time, budget time.Duration) {
	if budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	if c.r.waitForArchive(ctx, started) {
		return
	}
	c.r.Stats.StaleReads.Add(1)
	c.log.Warn("answering from the archive before mirroring caught up",
		"ns", ns,
		"waited", c.r.cfg.ArchiveWait.String(),
		"effect", "this answer may miss a very recent write, or include a very recently deleted document")
}

// caughtUp decides whether mirroring has reached what this read needs.
//
// Both bars have to clear, and that is the whole point. The wall-clock one is
// what this always did: the feeder's published instant against the instant the
// read began. The cluster-time one is the sound one, and it is the binding
// bar on any system with traffic, because a busy router has a fresh
// operationTime to clear while a feeder that has fallen behind does not.
//
// The cluster-time bar passes when either side has nothing to say. A feeder
// from before this publishes no cluster time, and a router that has forwarded
// no reply has taken none; in both cases the wall-clock comparison is the whole
// answer, exactly as it was before. Refusing instead would turn an upgrade into
// a stall.
func caughtUp(at time.Time, ts bson.Timestamp, until time.Time, untilTS uint64) bool {
	if at.Before(until) {
		return false
	}
	if untilTS == 0 || ts.T == 0 {
		return true
	}
	return checkpoint.PackTS(ts) >= untilTS
}
