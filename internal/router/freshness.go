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

// checkpointPoll bounds how often the position is re-read while anything is
// waiting on it. The interval used is a quarter of the cadence the feeder
// promised, between these two.
//
// A quarter, because polling faster than the thing being watched changes only
// the load. The feeder says how often it publishes in the position record
// itself, so this tunes to the deployment rather than to a guess: a feeder
// publishing every 20ms is polled every 5ms, and one publishing every second is
// polled four times in it.
//
// It matters because one goroutine polls however many reads are waiting, but
// that goroutine runs for as long as any of them do. Two milliseconds against a
// feeder that publishes every 20ms is five hundred reads a second of one small
// document, for four milliseconds of latency that the cadence hides anyway.
const (
	checkpointPollMin = 2 * time.Millisecond
	checkpointPollMax = 25 * time.Millisecond
)

// pollInterval is how long to wait before reading the position again.
func (c *checkpointCache) pollInterval() time.Duration {
	c.mu.Lock()
	every := c.everyMS
	c.mu.Unlock()
	if every <= 0 {
		// A feeder that promises no cadence gets the slowest of these, which is
		// what this did before it paced itself. Treating unknown as "as fast as
		// possible" would poll a deployment that never asked for it.
		return checkpointPollMax
	}
	d := time.Duration(every) * time.Millisecond / 4
	if d < checkpointPollMin {
		return checkpointPollMin
	}
	if d > checkpointPollMax {
		return checkpointPollMax
	}
	return d
}

// checkpointIdle is how long the poller keeps going after the last waiter
// leaves. Reads that fall through arrive in bursts, so stopping the instant one
// finishes would pay the first read of the next burst a full poll to start it
// up again.
const checkpointIdle = time.Second

// checkpointCache is the newest reading of how far mirroring has got, and the
// way waiters are told it has moved.
//
// It used to be a cache with a freshness window, which every waiter consulted
// and refreshed for itself. That put two quantisations in front of a read that
// was already waiting on the feeder's: the interval each waiter polled at, and
// the window a stale reading was reused for. One poller and a broadcast removes
// both, and removes the load of one query per waiting read at the same time.
type checkpointCache struct {
	mu      sync.Mutex
	at      time.Time      // the feeder's position, by the clock
	ts      bson.Timestamp // and as a cluster time, when it published one
	known   bool
	fetched time.Time // when this reading was taken
	// everyMS is the cadence the feeder promised, which is what this side
	// paces itself against. Zero until a position has been read.
	everyMS int64

	// updated is closed and replaced every time the reading changes, so a
	// waiter can take it, check the reading, and block on it without a race:
	// anything published between the check and the wait has already closed the
	// channel it is about to wait on.
	updated chan struct{}
	// waiters is how many reads are blocked on this. The poller runs while it
	// is above zero and for checkpointIdle afterwards.
	waiters int
	polling bool
}

// waitChan returns the channel that will be closed on the next reading.
func (c *checkpointCache) waitChan() chan struct{} {
	if c.updated == nil {
		c.updated = make(chan struct{})
	}
	return c.updated
}

// publish records a reading and wakes everything waiting on it.
func (c *checkpointCache) publish(at time.Time, ts bson.Timestamp, known bool, everyMS int64) {
	c.mu.Lock()
	c.at, c.ts, c.known, c.fetched = at, ts, known, time.Now()
	if everyMS > 0 {
		c.everyMS = everyMS
	}
	if c.updated != nil {
		close(c.updated)
		c.updated = nil
	}
	c.mu.Unlock()
}

// poll keeps the reading current while reads are waiting on it.
func (r *Router) poll(ctx context.Context) {
	defer func() {
		r.ckptCache.mu.Lock()
		r.ckptCache.polling = false
		r.ckptCache.mu.Unlock()
	}()
	idleSince := time.Time{}
	for {
		at, ts, known, everyMS := r.readPosition(ctx)
		r.ckptCache.publish(at, ts, known, everyMS)

		r.ckptCache.mu.Lock()
		n := r.ckptCache.waiters
		r.ckptCache.mu.Unlock()
		if n > 0 {
			idleSince = time.Time{}
		} else if idleSince.IsZero() {
			idleSince = time.Now()
		} else if time.Since(idleSince) > checkpointIdle {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(r.ckptCache.pollInterval()):
		}
	}
}

// startPolling makes sure the poller is running.
func (r *Router) startPolling(ctx context.Context) {
	r.ckptCache.mu.Lock()
	if r.ckptCache.polling {
		r.ckptCache.mu.Unlock()
		return
	}
	r.ckptCache.polling = true
	r.ckptCache.mu.Unlock()
	// Detached: the poller outlives the read that started it, and serves the
	// reads that follow.
	go r.poll(context.WithoutCancel(ctx))
}

// noteOpTime remembers the newest cluster time the fast tier has reported.
//
// It is strictly at or after every write the fast tier has already
// acknowledged, which is what makes it the right thing for a fallback read to
// wait on.
//
// **Only ever a fast tier reply.** The two tiers are separate clusters with
// independent logical clocks, and a cold tier reading here would be compared
// against the feeder's fast tier one, which is a comparison between two
// unrelated counters. It holds today because every path that answers from the
// archive or the cache sends its reply and returns before this is reached, so
// the only thing that gets here is what was read from the upstream connection.
// Anything that moves this call earlier, or lets an archive answer fall through
// to it, breaks the barrier silently.
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
	if !r.ckptCache.fetched.IsZero() {
		at, ts, known := r.ckptCache.at, r.ckptCache.ts, r.ckptCache.known
		r.ckptCache.mu.Unlock()
		return at, ts, known
	}
	r.ckptCache.mu.Unlock()
	// Nothing has been read yet, so this first caller fetches it rather than
	// waiting a poll to find out.
	at, ts, known, everyMS := r.readPosition(ctx)
	r.ckptCache.publish(at, ts, known, everyMS)
	return at, ts, known
}

// readPosition asks the archive how far mirroring has got.
func (r *Router) readPosition(ctx context.Context) (time.Time, bson.Timestamp, bool, int64) {
	// The position first. It says the same thing the checkpoint's eventAt says,
	// published on its own much faster cadence, and it is the difference
	// between this wait costing a couple of milliseconds and costing a whole
	// checkpoint interval. The checkpoint remains the fallback, for a feeder
	// that publishes no position.
	var at time.Time
	var ts bson.Timestamp
	var known bool
	var everyMS int64
	rec, ok, err := r.positions.At(ctx, r.cfg.FeederID)
	if err == nil && ok && rec.Live(time.Now()) {
		at, ts, known, everyMS = rec.At, rec.TS, true, rec.EveryMS
	} else {
		at, known, err = r.checkpoints.EventAt(ctx, r.cfg.FeederID)
	}
	if err != nil {
		return time.Time{}, bson.Timestamp{}, false, 0
	}
	return at, ts, known, everyMS
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

	r.ckptCache.mu.Lock()
	r.ckptCache.waiters++
	r.ckptCache.mu.Unlock()
	defer func() {
		r.ckptCache.mu.Lock()
		r.ckptCache.waiters--
		r.ckptCache.mu.Unlock()
	}()
	r.startPolling(ctx)

	for {
		// Taken before the reading is checked, so a position published between
		// the check and the wait wakes this rather than being missed.
		r.ckptCache.mu.Lock()
		next := r.ckptCache.waitChan()
		r.ckptCache.mu.Unlock()

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
		left := time.Until(deadline)
		if left <= 0 {
			return false
		}
		// Woken by the poller rather than by a timer of its own, so the wait is
		// as short as the position is new. The deadline is still the bound.
		timer := time.NewTimer(left)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-next:
			timer.Stop()
		case <-timer.C:
			return false
		}
	}
}

// archiveReadIsFresh waits for mirroring and counts what it had to do, so the
// two archive read paths behave the same way and report it the same way.
//
// budget is the caller's own maxTimeMS, or zero when they set none. The wait
// takes at most half of it, and the read that follows is given what is left: a
// client that asked for an answer within fifty milliseconds and got one after
// five hundred was told its deadline did not apply here, which is not the
// proxy's decision to make, and one that waited out its whole deadline here
// would reach the archive with nothing to spend.
func (c *conn) archiveReadIsFresh(ctx context.Context, ns string, started time.Time, budget time.Duration) {
	if budget > 0 {
		// Half, not all of it. Waiting out the whole deadline honours it and
		// starves the read that deadline was set for: the archive would then
		// be asked with a millisecond left and answer MaxTimeMSExpired, which
		// turns a feeder that is merely behind into failed queries. That is
		// the thing this barrier exists not to do. Splitting it means neither
		// half can starve the other, and a caller who set no deadline is
		// unaffected.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget/2)
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
