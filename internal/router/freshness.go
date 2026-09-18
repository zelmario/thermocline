package router

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/health"
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
// waiting on it. The interval used is the cadence the feeder promised, between
// these two.
//
// The cadence itself, not a fraction of it, and that took a measurement to
// settle because the obvious reasoning is wrong. Polling faster than the
// position changes cannot make it newer, it can only add reads to the archive,
// and the archive is the tier already answering the queries this is trying to
// speed up.
//
// A microbenchmark says the opposite, and did: one client on an idle system
// measured 30ms at a 25ms interval against 10ms at 2ms, because granularity is
// all there is to see when nothing is competing. The shop, eight clients
// against a 5,000,000 order archive, said this at a 20ms cadence:
//
//	poll every  5ms   71/s   p50 84ms   p95 282ms   p99 464ms
//	poll every 25ms   74/s   p50 77ms   p95 166ms   p99 245ms
//
// Less polling was better on every one of them, and the tails by half. The
// feeder says how often it publishes in the position record itself, so this
// tunes to the deployment rather than to a guess.
const (
	checkpointPollMin = 2 * time.Millisecond
	checkpointPollMax = 25 * time.Millisecond
)

// pollInterval is how long to wait before reading the position again.
func (c *checkpointCache) pollInterval() time.Duration {
	c.mu.Lock()
	every := c.last.everyMS
	c.mu.Unlock()
	if every <= 0 {
		// A feeder that promises no cadence gets the slowest of these, which is
		// what this did before it paced itself. Treating unknown as "as fast as
		// possible" would poll a deployment that never asked for it.
		return checkpointPollMax
	}
	d := time.Duration(every) * time.Millisecond
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
	last    reading
	fetched time.Time // when this reading was taken

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

// reading is everything one look at the published position says.
type reading struct {
	at    time.Time      // the feeder's position, by the feeder's clock
	ts    bson.Timestamp // and as a cluster time, when it published one
	known bool
	// everyMS is the cadence the feeder promised, which is what this side
	// paces itself against. Zero until a position has been read.
	everyMS int64
	// skew is how far the feeder's clock was ahead of the archive's when it
	// last looked, which is what makes at comparable with an instant taken
	// here. See caughtUp.
	skew      time.Duration
	skewKnown bool
}

// waitChan returns the channel that will be closed on the next reading.
func (c *checkpointCache) waitChan() chan struct{} {
	if c.updated == nil {
		c.updated = make(chan struct{})
	}
	return c.updated
}

// publish records a reading and wakes everything waiting on it.
func (c *checkpointCache) publish(r reading) {
	c.mu.Lock()
	everyMS := c.last.everyMS
	c.last, c.fetched = r, time.Now()
	if c.last.everyMS <= 0 {
		// A reading that says nothing about the cadence does not unsay what the
		// last one did, since this side paces itself against it.
		c.last.everyMS = everyMS
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
		r.ckptCache.publish(r.readPosition(ctx))

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
	r.goSafely("polling mirroring's position", func() { r.poll(context.WithoutCancel(ctx)) })
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

// mirroredThroughCached returns what is already known about mirroring, and
// never goes and finds out.
//
// For callers that must not disturb the read path. mirroredThrough publishes
// whatever it fetches into the same cache the barrier reads, including a
// failure, and publish sets fetched either way, so a caller that fetched and
// failed leaves "nothing is mirroring" in the cache. The poller only runs while
// reads are waiting on it, so on a router with no fall-through traffic that
// answer stands until one arrives, and the first one then gives up at once and
// answers stale.
//
// A metrics scrape is exactly such a caller: it runs on a timer whether or not
// anything is reading, and an archive it cannot reach is the state it most
// needs to report. Reporting it must not also cause it.
func (r *Router) mirroredThroughCached() (reading, bool) {
	r.ckptCache.mu.Lock()
	defer r.ckptCache.mu.Unlock()
	if r.ckptCache.fetched.IsZero() {
		return reading{}, false
	}
	return r.ckptCache.last, true
}

// mirroredThrough returns how far mirroring has got, reusing a recent reading.
func (r *Router) mirroredThrough(ctx context.Context) reading {
	r.ckptCache.mu.Lock()
	if !r.ckptCache.fetched.IsZero() {
		last := r.ckptCache.last
		r.ckptCache.mu.Unlock()
		return last
	}
	r.ckptCache.mu.Unlock()
	// Nothing has been read yet, so this first caller fetches it rather than
	// waiting a poll to find out.
	got := r.readPosition(ctx)
	r.ckptCache.publish(got)
	return got
}

// readPosition asks the archive how far mirroring has got.
func (r *Router) readPosition(ctx context.Context) reading {
	// The position first. It says the same thing the checkpoint's eventAt says,
	// published on its own much faster cadence, and it is the difference
	// between this wait costing a couple of milliseconds and costing a whole
	// checkpoint interval. The checkpoint remains the fallback, for a feeder
	// that publishes no position.
	var got reading
	mine, mineKnown := r.archiveSkew(ctx)
	rec, ok, err := r.positions.At(ctx, r.cfg.FeederID)
	switch {
	case err == nil && ok && r.posLive.Observe(rec, time.Now(), mine, mineKnown):
		got.at, got.ts, got.known, got.everyMS = rec.At, rec.TS, true, rec.EveryMS
		got.skew, got.skewKnown = rec.Skew()
	case err == nil && ok:
		// A position that exists and has stopped moving is evidence, not an
		// absence: this feeder was publishing and has stopped. Falling back to
		// its checkpoint would read as "mirroring is a little behind" and every
		// read would then wait out the whole barrier for a feeder that is never
		// going to catch up.
		//
		// Measured with the feeder stopped and a checkpoint left behind: 40
		// point reads took 185 seconds through the router against 97ms straight
		// at the archive, which is 4.6 seconds each, the whole archive_wait,
		// every time. Saying "nothing is mirroring" instead answers at once and
		// warns, which is what the barrier already does when there is no
		// checkpoint at all.
		return reading{everyMS: rec.EveryMS}
	default:
		// No position record at all, which is a feeder too old to publish one
		// or one with position_every set to zero. Its checkpoint is the only
		// signal there is, and it carries the same offset for the same reason.
		var rec checkpoint.Record
		var ok bool
		rec, ok, err = r.checkpoints.Mark(ctx, r.cfg.FeederID)
		got.at, got.known = rec.EventAt, ok
		got.skew, got.skewKnown = rec.Skew()
	}
	if err != nil {
		return reading{}
	}
	return got
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

		got := r.mirroredThrough(ctx)
		if got.known && r.caughtUp(ctx, got, until, untilTS) {
			return true
		}
		// No checkpoint at all means nothing is mirroring. Waiting the full
		// timeout on every read would turn a stopped feeder into an outage, so
		// this gives up at once and lets the caller answer and warn.
		if !got.known {
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
	suppressed, say := c.r.noteStaleRead()
	if !say {
		return
	}
	c.log.Warn("answering from the archive before mirroring caught up",
		"ns", ns,
		"waited", c.r.cfg.ArchiveWait.String(),
		"suppressed_since_last", suppressed,
		"effect", "this answer may miss a very recent write, or include a very recently deleted document")
	c.explainStale(ctx, started)
}

// noteStaleRead decides whether this stale read gets a log line, and reports how
// many went unlogged since the last one did.
//
// Rate limited because the flood arrives exactly when the log has to stay
// readable. A feeder that has fallen behind sends every fall-through read down
// this path, and on the 2026-09-18 run more than eight reads in ten fell
// through: that is a line per read at the rate of traffic, a few hundred bytes
// each, gigabytes a day, burying whatever else the router has to say about the
// outage.
//
// Nothing is lost by it. thermocline_stale_reads_total counts every one exactly,
// which is what an alert reads, and the line now carries how many it stands for.
// A single stale read on a healthy deployment still logs immediately, because
// the gate has been open since the last one, and that is the case the warning
// was written for.
func (r *Router) noteStaleRead() (suppressed int64, say bool) {
	now := time.Now().UnixNano()
	last := r.staleSaidAt.Load()
	if now-last < int64(explainEvery) || !r.staleSaidAt.CompareAndSwap(last, now) {
		r.staleSuppressed.Add(1)
		return 0, false
	}
	return r.staleSuppressed.Swap(0), true
}

// explainEvery bounds how often the line below is written. See explainStale.
const explainEvery = 10 * time.Second

// explainStale says which of the two bars the wait gave up on.
//
// The warning above says "waited 5s" and nothing else, which leaves an operator
// with two quantities and no way to tell which one moved. They are not
// interchangeable: caughtUp holds a wall-clock bar and a cluster-time bar, and
// its own comment says the cluster-time one is the binding bar on any system
// with traffic. The metric an alert watches, thermocline_mirroring_lag_seconds,
// is the wall-clock one.
//
// Measured on the 2026-09-18 store run: eleven stale reads inside one second
// while that gauge, sampled every five seconds, read 24ms three seconds before
// and 14ms five seconds after, and the feeder logged nothing at all. The event
// was real and no number an operator graphs went anywhere near it.
//
// Read after the fact rather than captured inside the wait, so it costs a
// cached lookup on a path that has already given up and changes nothing about
// the decision. That the bar has often cleared by the time this runs is itself
// worth seeing: it says the wait ended a moment early rather than the feeder
// being stopped.
func (c *conn) explainStale(ctx context.Context, started time.Time) {
	// No gate of its own: noteStaleRead has already claimed this interval for
	// the pair, so this runs exactly when the warning above does.
	got := c.r.mirroredThrough(ctx)
	if !got.known {
		c.log.Warn("and mirroring has published no position at all",
			"note", "nothing is mirroring, so no read can clear the barrier; check the feeder is running")
		return
	}
	seen := c.r.seenOpTime.Load()
	feeder := checkpoint.PackTS(got.ts)
	mine, mineKnown := c.r.archiveSkew(ctx)
	at, until := got.at, started
	if mineKnown && got.skewKnown {
		at = at.Add(-got.skew)
		until = until.Add(-mine)
	}
	c.log.Warn("which bar the freshness wait gave up on",
		"wall_clock_short_by", until.Sub(at).Round(time.Millisecond).String(),
		"cluster_time_short", seen != 0 && got.ts.T != 0 && feeder < seen,
		"feeder_ts", feeder, "router_seen_ts", seen,
		"frames_aligned", mineKnown && got.skewKnown,
		"note", "a negative wall_clock_short_by with cluster_time_short true is the ordinary "+
			"shape: the feeder's position is recent but does not yet cover an operationTime this "+
			"router has already handed a client, and only the cluster-time bar can see that")
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
//
// The wall-clock bar is the one that needs care, because the two instants were
// read from two machines. `at` is the feeder's clock and `until` is this one,
// and a router running a few seconds ahead of its feeder never clears the bar:
// every read that falls through waits the whole archive_wait and then answers
// stale anyway, which is slow and stale, the pair this barrier exists to avoid.
// Nothing said so, because the answer it gives is the one a genuinely lagging
// feeder gives.
//
// So both instants are put in the archive's frame before they are compared. The
// feeder publishes how far ahead of the archive its own clock is, this side
// measures the same thing for itself, and the archive's clock is then the one
// reference neither daemon has to trust the other about. When either offset is
// missing the raw instants are compared, which is what this did before.
func (r *Router) caughtUp(ctx context.Context, got reading, until time.Time, untilTS uint64) bool {
	mine, ok := r.archiveSkew(ctx)
	return caughtUp(got, until, untilTS, mine, ok)
}

// caughtUp is the decision itself, with both offsets already in hand.
func caughtUp(got reading, until time.Time, untilTS uint64, mine time.Duration, mineKnown bool) bool {
	at := got.at
	if mineKnown && got.skewKnown {
		at = at.Add(-got.skew)
		until = until.Add(-mine)
	}
	if at.Before(until) {
		return false
	}
	if untilTS == 0 || got.ts.T == 0 {
		return true
	}
	return checkpoint.PackTS(got.ts) >= untilTS
}

// archiveSkew is how far this host's clock is ahead of the archive's, and
// whether it has ever been measured.
//
// It never waits for the answer. The reading is a round trip to the archive and
// the callers are a read deciding whether to wait and a metrics scrape, so
// blocking either on the archive would put the archive's latency in front of
// work that had no reason to touch it, and holding the lock across the call
// would put every other caller behind it too. The measurement runs in the
// background and the previous answer is given meanwhile, which for the first
// call means "not known" and a comparison of raw instants, exactly as before.
//
// Reread on the same cadence the feeder rereads its own, and for the same
// reason: clocks drift slowly, and asking several times a second would pay a
// round trip to learn nothing.
func (r *Router) archiveSkew(ctx context.Context) (time.Duration, bool) {
	r.clock.mu.Lock()
	fresh := r.clock.known && time.Since(r.clock.at) < skewRemeasure
	// A cluster that refuses the reading is not asked again on every read, and
	// one measurement is enough to have in flight at a time.
	backOff := !r.clock.known && !r.clock.at.IsZero() && time.Since(r.clock.at) < time.Second
	start := !fresh && !backOff && !r.clock.measuring
	if start {
		r.clock.measuring = true
	}
	skew, known := r.clock.skew, r.clock.known
	r.clock.mu.Unlock()

	if start {
		r.goSafely("reading the archive's clock", func() {
			r.measureArchiveSkew(context.WithoutCancel(ctx))
		})
	}
	return skew, known
}

// noteArchiveSkew records a reading somebody else took.
func (r *Router) noteArchiveSkew(skew time.Duration) {
	r.clock.mu.Lock()
	defer r.clock.mu.Unlock()
	r.clock.skew, r.clock.known, r.clock.at = skew, true, time.Now()
}

// measureArchiveSkew takes the reading and records it.
func (r *Router) measureArchiveSkew(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, skewTimeout)
	defer cancel()
	skew, err := health.Skew(ctx, r.cold)

	r.clock.mu.Lock()
	defer r.clock.mu.Unlock()
	r.clock.measuring = false
	r.clock.at = time.Now()
	if err != nil {
		// The last reading stands. It is minutes of drift out of date at worst,
		// which is a better answer than none.
		return
	}
	if r.clock.known && health.AbsDuration(skew-r.clock.skew) > health.SkewWarn {
		r.log.Warn("this host's clock moved against the archive's",
			"was", r.clock.skew.Round(time.Millisecond).String(),
			"now", skew.Round(time.Millisecond).String(),
			"effect", "reads that fall through to the archive judge mirroring against this")
	}
	r.clock.skew, r.clock.known = skew, true
}

// skewTimeout bounds the reading. It is a hello on an existing pool; taking
// longer than this means the archive has troubles that this is not the place to
// wait out.
const skewTimeout = 5 * time.Second

// skewRemeasure is how often each side rereads the archive's clock.
const skewRemeasure = time.Minute

// warnAboutClocks reports a host clock that disagrees with either tier.
//
// Three things in this system are decided by comparing instants that were read
// on different machines, and this is the one place a human is told before any
// of them misbehaves:
//
//   - the freshness barrier, which compares the feeder's published position
//     against the instant a read began, and answers stale or waits the whole
//     archive_wait when the two hosts disagree;
//   - thermocline_mirroring_lag_seconds, which is the number the alert rules
//     watch and reads as lag that is not there;
//   - the eviction announcements, which expire on the archive's TTL and are
//     what the feeder needs to tell an eviction from the application's own
//     delete. That one destroys data. See internal/health's own note.
//
// The offsets are corrected for where they can be, and the correction is
// measured over a round trip on a cadence, so it is a good answer rather than a
// perfect one. A host that is minutes out is still worth fixing rather than
// working around.
func (r *Router) warnAboutClocks(ctx context.Context) {
	for _, tier := range []struct {
		name   string
		client *mongo.Client
	}{{"the archive", r.cold}, {"the fast tier", r.hot}} {
		if tier.client == nil {
			continue
		}
		skew, err := health.Skew(ctx, tier.client)
		if err != nil {
			r.log.Debug("could not read a tier's clock", "tier", tier.name, "err", err)
			continue
		}
		if tier.client == r.cold {
			// Kept rather than read and thrown away. It is the same reading
			// the barrier and the lag metric use, so a start primes them and
			// the first scrape has a number in it.
			r.noteArchiveSkew(skew)
		}
		if health.AbsDuration(skew) <= health.SkewWarn {
			continue
		}
		ahead := "ahead of"
		if skew < 0 {
			ahead = "behind"
		}
		r.log.Warn("this host's clock disagrees with a tier's",
			"tier", tier.name,
			"by", health.AbsDuration(skew).Round(time.Millisecond).String(),
			"direction", "this host is "+ahead+" "+tier.name,
			"effect", "mirroring lag is measured against this, and so is the wait before a read falls through to the archive",
			"fix", "run an NTP client on every host that runs a cline daemon")
	}
}

// archiveNowFallback is this instant on the archive's clock, as well as this
// process can say, for comparing against something another daemon stamped
// there. It falls back to this host's own reading when no comparison has been
// managed, which is what everything did before there was one.
func (r *Router) archiveNowFallback(ctx context.Context) time.Time {
	now := time.Now().UTC()
	if skew, ok := r.archiveSkew(ctx); ok {
		return now.Add(-skew)
	}
	return now
}
