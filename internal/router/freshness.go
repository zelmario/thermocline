package router

import (
	"context"
	"sync"
	"time"
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

// checkpointPoll is how often the checkpoint is re-read while waiting.
const checkpointPoll = 25 * time.Millisecond

// checkpointFresh is how long a checkpoint reading is reused. Many reads can
// fall through at once, and they would otherwise all query the same small
// document on the archive at the same time.
const checkpointFresh = 20 * time.Millisecond

type checkpointCache struct {
	mu      sync.Mutex
	at      time.Time // the feeder's position
	known   bool
	fetched time.Time // when this reading was taken
}

// mirroredThrough returns how far mirroring has got, reusing a recent reading.
func (r *Router) mirroredThrough(ctx context.Context) (time.Time, bool) {
	r.ckptCache.mu.Lock()
	if !r.ckptCache.fetched.IsZero() && time.Since(r.ckptCache.fetched) < checkpointFresh {
		at, known := r.ckptCache.at, r.ckptCache.known
		r.ckptCache.mu.Unlock()
		return at, known
	}
	r.ckptCache.mu.Unlock()

	// The position first. It says the same thing the checkpoint's eventAt says,
	// published on its own much faster cadence, and it is the difference
	// between this wait costing a couple of milliseconds and costing a whole
	// checkpoint interval. The checkpoint remains the fallback, for a feeder
	// that publishes no position.
	var at time.Time
	var known bool
	rec, ok, err := r.positions.At(ctx, r.cfg.FeederID)
	if err == nil && ok && rec.Live(time.Now()) {
		at, known = rec.At, true
	} else {
		at, known, err = r.checkpoints.EventAt(ctx, r.cfg.FeederID)
	}
	if err != nil {
		return time.Time{}, false
	}

	r.ckptCache.mu.Lock()
	r.ckptCache.at, r.ckptCache.known, r.ckptCache.fetched = at, known, time.Now()
	r.ckptCache.mu.Unlock()
	return at, known
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

	deadline := time.Now().Add(wait)
	for {
		at, known := r.mirroredThrough(ctx)
		if known && !at.Before(until) {
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
