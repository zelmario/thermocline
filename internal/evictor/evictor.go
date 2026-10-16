// Package evictor removes documents from the fast tier once the archive holds
// them.
//
// It is the only supported way to remove data from the fast tier, and it earns
// that by announcing every document before deleting it. The feeder reads those
// announcements to tell an eviction apart from an application delete, which is
// what keeps the archive complete without also resurrecting deleted records.
//
// Nothing is removed unless the archive is confirmed to hold that specific
// document, and nothing is removed at all while mirroring is behind.
package evictor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/health"
	"github.com/zelmario/thermocline/internal/lease"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/watermark"
)

// Evictor removes eligible documents from one fast tier.
type Evictor struct {
	hot    *mongo.Client
	cold   *mongo.Client
	cfg    config.EvictorConfig
	check  *health.Checker
	log    *slog.Logger
	dryRun bool

	// maxHealthWait bounds how long a paused sweep waits for the cluster to
	// recover. Zero waits indefinitely, which is what a long-running daemon
	// should do. A one-shot run sets it so the command actually returns.
	maxHealthWait time.Duration

	// total accumulates across sweeps, because a counter that resets every pass
	// tells an alert nothing.
	total Stats

	marks   *watermark.Store
	limiter *limiter
	// hold is what keeps two sweeps from overlapping, and heldAt paces its
	// renewal. See RunOnce and keepLease.
	hold   *lease.Lease
	heldAt time.Time

	// unagedSeen records which namespaces have already been counted for
	// documents the rule cannot age, so the scan that costs happens once per
	// process rather than once per sweep. See warnAboutUnaged.
	// clockOnce keeps the comparison with the archive's clock to one per run:
	// it is a property of the machines, not of a sweep.
	clockOnce sync.Once

	unagedMu   sync.Mutex
	unagedSeen map[string]bool

	// skew is how far this clock is from the archive's, as last measured. It is
	// published rather than only logged, because it is the difference that loses
	// documents: see checkClock.
	skewMu    sync.Mutex
	skew      time.Duration
	skewKnown bool
}

// lastSkew reports the most recent clock comparison with the archive.
func (e *Evictor) lastSkew() (time.Duration, bool) {
	e.skewMu.Lock()
	defer e.skewMu.Unlock()
	return e.skew, e.skewKnown
}

// keepLease renews this sweep's claim, at most as often as the lease asks for.
//
// A failure that is not the lease being gone is left alone: the archive being
// unreachable stops the sweep at its next write anyway, and stopping here would
// report it as somebody else sweeping, which it is not.
func (e *Evictor) keepLease(ctx context.Context) error {
	if e.dryRun || time.Since(e.heldAt) < lease.Renew {
		return nil
	}
	if err := e.hold.Keep(ctx); err != nil {
		if errors.Is(err, lease.ErrLost) {
			e.log.Warn("another sweep took over, stopping this one", "err", err)
			return err
		}
		e.log.Debug("could not renew the evictor lease", "err", err)
		return nil
	}
	e.heldAt = time.Now()
	return nil
}

// archiveNow is this instant on the archive's clock, as well as this process
// can say. It falls back to its own when no comparison has been managed, which
// is what everything did before there was one.
func (e *Evictor) archiveNow() time.Time {
	now := time.Now().UTC()
	if skew, ok := e.lastSkew(); ok {
		return now.Add(-skew)
	}
	return now
}

// SetMaxHealthWait bounds how long RunOnce waits for an unhealthy cluster
// before giving up. Zero, the default, waits indefinitely.
func (e *Evictor) SetMaxHealthWait(d time.Duration) { e.maxHealthWait = d }

// publish records what this evictor has done so far, so it can be alerted on.
//
// Cumulative rather than per pass, because that is what a counter means. The
// interesting alert is not any of these numbers, it is the time since the last
// pass: an evictor that has stopped, or is failing closed on mirroring lag,
// looks exactly like a healthy one from outside until somebody notices it has
// not run.
func (e *Evictor) publish(ctx context.Context, s Stats) {
	e.total.Scanned += s.Scanned
	e.total.Evicted += s.Evicted
	e.total.Held += s.Held
	e.total.Unconfirmed += s.Unconfirmed
	e.total.Backoffs += s.Backoffs
	counters := map[string]int64{
		"scanned":     int64(e.total.Scanned),
		"evicted":     int64(e.total.Evicted),
		"held":        int64(e.total.Held),
		"unconfirmed": int64(e.total.Unconfirmed),
		"backoffs":    int64(e.total.Backoffs),
	}
	var gauges map[string]float64
	if skew, ok := e.lastSkew(); ok {
		// Published every pass, because a clock drifts while a daemon runs and
		// the log said this once at startup and never again.
		gauges = map[string]float64{"clock_skew_seconds": skew.Seconds()}
	}
	if err := meta.PublishDaemonReport(ctx, e.cold, "evictor", s.DryRun, counters, gauges); err != nil {
		e.log.Warn("could not publish evictor statistics", "err", err)
	}
}

// Stats reports one run. On a dry run, Evicted counts what would have been
// removed.
type Stats struct {
	Scanned     int
	Evicted     int
	Held        int
	Unconfirmed int
	Backoffs    int
	DryRun      bool
	// Paused says the sweep stopped part way because an operator paused
	// eviction while it ran.
	Paused bool
}

// LogArgs returns the sweep summary as slog key-value pairs. The evicted count
// is labelled differently on a dry run, because "evicted=40" in a log somebody
// greps six months from now should not describe documents that are still there.
func (s Stats) LogArgs() []any {
	removed := "evicted"
	if s.DryRun {
		removed = "would_evict"
	}
	return []any{
		"scanned", s.Scanned,
		removed, s.Evicted,
		"held", s.Held,
		"unconfirmed", s.Unconfirmed,
		"backoffs", s.Backoffs,
	}
}

// New builds an Evictor. It does not contact either cluster.
func New(hot, cold *mongo.Client, cfg config.EvictorConfig, dryRun bool, log *slog.Logger) *Evictor {
	return &Evictor{
		hot:     hot,
		cold:    cold,
		cfg:     cfg,
		check:   health.New(hot, cold, cfg.FeederID),
		log:     log,
		dryRun:  dryRun,
		marks:   watermark.New(cold),
		limiter: newLimiter(cfg.Rate),
		hold:    lease.New(cold, "evictor", cfg.FeederID),
	}
}

// RunOnce walks every rule once and returns when there is nothing left to
// remove at the current boundary.
//
// One sweep at a time across the whole deployment, which the lease is what
// enforces. Two sweeps overlapping announce the same eviction twice and delete
// it once, so a count is left standing on the notice, and the next delete of
// that _id -- the application's own, whenever it comes -- is read as the
// eviction that never happened: the archive keeps a live copy of a document its
// owner deleted. The shape that arranges it is ordinary, an operator running
// cline evict --once while the daemon is up.
//
// A pass rather than a process, because that is the thing that must not
// overlap. Two evictors taking turns are two ordinary sweeps.
//
// A dry run takes nothing. It announces nothing and removes nothing, so it
// cannot do the damage, and making it wait for a daemon's pass would make the
// one command an operator reaches for when they want to understand the system
// the one command that will not answer.
func (e *Evictor) RunOnce(ctx context.Context) (Stats, error) {
	if !e.dryRun {
		if err := e.hold.Take(ctx); err != nil {
			return Stats{DryRun: e.dryRun}, err
		}
		e.heldAt = time.Now()
		defer func() {
			if err := e.hold.Release(context.WithoutCancel(ctx)); err != nil {
				e.log.Debug("could not release the evictor lease", "err", err)
			}
		}()
	}

	// An operator hold comes first, before any candidate is even looked at.
	// See meta.EvictionPausedMarker for the case it exists for: an archive that
	// has been restored can hold an older version of a document, which the
	// per-document check confirms as present, and the fast tier's newer copy is
	// then removed.
	if rec, paused, err := meta.EvictionPaused(ctx, e.cold); err != nil {
		e.log.Warn("could not read the eviction hold, not evicting", "err", err)
		return Stats{DryRun: e.dryRun}, nil
	} else if paused {
		e.log.Warn("eviction is paused by an operator, removing nothing",
			"since", rec.Since, "reason", rec.Reason,
			"remedy", "cline resume --config <file> when the archive is trustworthy again")
		s := Stats{DryRun: e.dryRun}
		e.publish(ctx, s)
		return s, nil
	}

	e.checkClock(ctx)

	total := Stats{DryRun: e.dryRun}
	if e.dryRun {
		e.log.Info("dry run, nothing will be removed")
	}

	// Fixed here, so the boundary a claim is narrowed to is the same one the
	// sweep then evicts against. Computing it again inside the rule would move
	// it forward by however long the wait took, and evict a slice the claim was
	// never narrowed past.
	boundaries := make(map[string]time.Time, len(e.cfg.Rules))
	for _, rule := range e.cfg.Rules {
		boundaries[rule.NS] = cutoff(time.Now(), rule.OlderThan.D())
	}

	// Every claim narrowed first, then one wait, then the removals.
	//
	// The wait is what lets readers holding a cached claim re-read it before
	// anything it covered is removed, and it has to come after the raise. Doing
	// it inside each rule means paying it once per rule: a deployment tiering
	// twenty namespaces would spend two minutes of every sweep waiting, and an
	// evictor on a one minute cycle would never keep up. Raising them all first
	// makes one wait serve all of them, because by the time the first removal
	// happens every claim has been narrowed and the window has passed.
	if err := e.narrowClaims(ctx, boundaries); err != nil {
		return total, err
	}

	for _, rule := range e.cfg.Rules {
		s, err := e.runRule(ctx, rule, boundaries[rule.NS])
		total.Scanned += s.Scanned
		total.Evicted += s.Evicted
		total.Held += s.Held
		total.Unconfirmed += s.Unconfirmed
		total.Backoffs += s.Backoffs
		if err != nil {
			return total, err
		}
		if s.Paused {
			break
		}
	}
	e.publish(ctx, total)
	return total, nil
}

// Run walks every rule repeatedly, pausing between sweeps.
func (e *Evictor) Run(ctx context.Context, every time.Duration) error {
	for {
		stats, err := e.RunOnce(ctx)
		var held *lease.ErrHeld
		switch {
		case errors.As(err, &held), errors.Is(err, lease.ErrLost):
			// Somebody else is sweeping. Skipping is right where stopping is
			// not: the other sweep is doing this one's work, and a daemon that
			// exits because an operator ran a sweep by hand is a fast tier that
			// fills up. ErrLost is the same thing found part way through a
			// sweep rather than at the start of one.
			e.log.Warn("another sweep is running, skipping this one", "err", err)
		case err != nil && !errors.Is(err, context.Canceled):
			return err
		}
		e.log.Info("sweep complete", stats.LogArgs()...)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(every):
		}
	}
}

func (e *Evictor) runRule(ctx context.Context, rule config.EvictionRule, boundary time.Time) (Stats, error) {
	// Every way out of a sweep waits for the delete still running behind it.
	// The lease is held until this returns, and cline pause takes the lease to
	// know no sweep is in progress, so a delete outliving the sweep would be
	// one the operator was told had stopped.
	var behind removal
	stats, err := e.sweepRule(ctx, rule, boundary, &behind)
	n, werr := behind.wait()
	stats.Evicted += n
	if err == nil {
		err = werr
	}
	return stats, err
}

func (e *Evictor) sweepRule(ctx context.Context, rule config.EvictionRule, boundary time.Time, behind *removal) (Stats, error) {
	var stats Stats
	var after bson.RawValue
	db, coll := splitNS(rule.NS)
	hotColl := e.hot.Database(db).Collection(coll)

	// The same rule the warmer refuses, for the same reason. With no date_field
	// the age comes from the timestamp inside an ObjectId _id, and MongoDB
	// brackets range predicates by type, so against any other _id type the
	// candidate query matches nothing. The evictor then sweeps for ever,
	// reports success every time and tiers nothing at all.
	if rule.UsesObjectIDTime() {
		wrong, err := meta.IDsAreNotObjectIDs(ctx, e.cold, db, coll)
		if err != nil {
			return stats, err
		}
		if wrong {
			e.log.Error("not evicting a namespace this rule cannot address",
				"ns", rule.NS,
				"reason", "the rule takes each document's age from the timestamp inside its _id, and this "+
					"collection's _id values are not ObjectIds",
				"effect", "the range this rule asks for matches no document, so this namespace is never tiered "+
					"and the fast tier grows without bound",
				"action", "set date_field on this rule to the field that carries the document's date")
			return stats, nil
		}
	}
	e.warnAboutUnaged(ctx, rule, hotColl)

	e.log.Info("evicting",
		"ns", rule.NS,
		"older_than", rule.OlderThan.String(),
		"cutoff", boundary.Format(time.RFC3339),
		"selector", selectorName(rule))

	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		// The lease is renewed here rather than only taken at the start of the
		// sweep, for the reason the health check below is taken here: a sweep
		// can run for hours, and one that is not renewed looks abandoned after
		// thirty seconds. A second evictor would then take it and the two would
		// announce the same eviction twice, which is what the lease is for.
		//
		// Losing it ends the sweep rather than the daemon. Somebody else is
		// doing this work now, and the next pass will find out whether they
		// still are.
		if err := e.keepLease(ctx); err != nil {
			return stats, err
		}

		// Health gates every batch, not every sweep. A sweep can run for hours
		// and the cluster's condition changes underneath it.
		if backedOff, err := e.waitForHealth(ctx); err != nil {
			return stats, err
		} else if backedOff {
			stats.Backoffs++
		}

		filter, err := candidateFilter(rule, boundary, after)
		if err != nil {
			return stats, err
		}

		cands, err := e.scan(ctx, hotColl, rule.NS, filter)
		if err != nil {
			return stats, err
		}
		if len(cands) == 0 {
			return stats, nil
		}
		stats.Scanned += len(cands)
		after = cands[len(cands)-1].id

		out, err := e.classify(ctx, rule.NS, cands)
		if err != nil {
			return stats, err
		}
		stats.Held += out.held
		stats.Unconfirmed += out.unconfirmed

		if out.unconfirmed > 0 {
			// Not fatal: the feeder may simply not have caught up with these
			// particular documents. They are reconsidered next sweep.
			e.log.Warn("documents not in the archive, leaving them in place",
				"ns", rule.NS, "count", out.unconfirmed)
		}

		// Asked again before every batch, not only when the sweep began. An
		// operator pauses eviction to restore the archive, and a sweep already
		// running went on removing documents it had confirmed against the
		// archive that was about to be replaced. See runPause, which waits for
		// a sweep in progress to reach this check.
		if rec, paused, perr := meta.EvictionPaused(ctx, e.cold); perr != nil || paused {
			if perr != nil {
				e.log.Warn("could not read the eviction hold, stopping this sweep", "err", perr)
			} else {
				e.log.Warn("eviction was paused during this sweep, stopping it",
					"ns", rule.NS, "since", rec.Since, "reason", rec.Reason)
			}
			stats.Paused = true
			return stats, nil
		}
		if len(out.evict) > 0 {
			removed, err := e.evictBehind(ctx, rule.NS, hotColl, out.evict, behind)
			if err != nil {
				return stats, err
			}
			// What was removed, not what was offered: a document changed since it
			// was mirrored is offered and stays, and counting it here reported
			// twenty evicted by a sweep that had removed nothing.
			stats.Evicted += removed
			e.limiter.spend(ctx, len(out.evict))
		}

		if len(cands) < e.cfg.BatchSize {
			return stats, nil
		}
	}
}

// scan reads one batch of candidates in _id order so the sweep is resumable and
// cannot revisit the same documents.
func (e *Evictor) scan(ctx context.Context, coll *mongo.Collection, ns string, filter bson.D) ([]candidate, error) {
	cur, err := coll.Find(ctx, filter,
		options.Find().
			SetSort(bson.D{{Key: "_id", Value: 1}}).
			SetLimit(int64(e.cfg.BatchSize)).
			SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("scan %s: %w", ns, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var cands []candidate
	for cur.Next(ctx) {
		id, err := cur.Current.LookupErr("_id")
		if err != nil {
			return nil, fmt.Errorf("scan %s: document without _id", ns)
		}
		// Copy: cur.Current is reused on the next iteration.
		raw := bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)}
		cands = append(cands, candidate{id: raw, docID: meta.DocID(ns, raw)})
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", ns, err)
	}
	return cands, nil
}

// classify applies the residency hold and the archive confirmation, in two
// round trips for the whole batch.
func (e *Evictor) classify(ctx context.Context, ns string, cands []candidate) (outcome, error) {
	docIDs := make([]string, len(cands))
	ids := make([]bson.RawValue, len(cands))
	for i, c := range cands {
		docIDs[i] = c.docID
		ids[i] = c.id
	}

	held, err := e.residencyHolds(ctx, docIDs)
	if err != nil {
		return outcome{}, err
	}
	archived, err := e.archived(ctx, ns, cands, ids)
	if err != nil {
		return outcome{}, err
	}
	return partition(cands, held, archived), nil
}

// residencyHolds returns the documents whose stay in the fast tier has not
// expired. Age makes a document a candidate; recent use overrides it.
//
// Asked in the archive's frame, because that is the frame the hold was written
// in. A router or warmer promotes a document and stamps the hold from the
// archive's own clock; this process may be on a third machine, and its own
// clock is what would otherwise decide whether that hold has passed. Ahead by
// more than the hold and every hold reads as expired, so a document fetched a
// moment ago is evicted and the next read fetches it again, which is the whole
// thing residency exists to stop. The reading is taken against the archive
// every pass already, for the announcements.
func (e *Evictor) residencyHolds(ctx context.Context, docIDs []string) (map[string]bool, error) {
	cur, err := e.cold.Database(meta.DB).Collection(meta.Residency).Find(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}},
		{Key: "hotUntil", Value: bson.D{{Key: "$gt", Value: e.archiveNow()}}},
	}, options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("read residency: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	held := make(map[string]bool)
	for cur.Next(ctx) {
		var rec struct {
			ID string `bson:"_id"`
		}
		if err := cur.Decode(&rec); err != nil {
			return nil, fmt.Errorf("decode residency: %w", err)
		}
		held[rec.ID] = true
	}
	return held, cur.Err()
}

// archived returns the documents the cold tier actually holds, live. This is
// the check that makes eviction safe, so it queries the archive directly rather
// than trusting any bookkeeping.
//
// Live is the word that matters. A tombstoned copy is a record that the
// application deleted this document, not a copy of it: reads filter it out, so
// counting it here would confirm the archive holds something no read will ever
// return.
//
// That state is reachable and costs the document. An application deletes a
// record, which tombstones the archive's copy, and then writes the same _id
// again, which is ordinary for a natural key or a reprocessed record. Until the
// feeder mirrors the second write the fast tier holds the only live copy and
// the archive holds a tombstone. Measured without this filter: one sweep and
// the document was on neither tier, and the router answered GONE for a document
// the application had just written.
func (e *Evictor) archived(ctx context.Context, ns string, cands []candidate, ids []bson.RawValue) (map[string]bool, error) {
	db, coll := splitNS(ns)
	cur, err := e.cold.Database(db).Collection(coll).Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}, meta.NotArchivedDeleted()},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("confirm archive copies for %s: %w", ns, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	present := make(map[string]bool, len(cands))
	for cur.Next(ctx) {
		id, err := cur.Current.LookupErr("_id")
		if err != nil {
			continue
		}
		present[meta.DocID(ns, id)] = true
	}
	return present, cur.Err()
}

// evict announces the batch, then removes it.
//
// The order is the contract. The announcement is acknowledged by the cold tier
// before the delete reaches the fast tier, so by the time the feeder observes
// the delete the notice is already there to explain it. Reverse the two and
// every eviction looks like an application delete.
func (e *Evictor) evict(ctx context.Context, ns string, hotColl *mongo.Collection, docs []candidate) (int, error) {
	if e.dryRun {
		e.log.Info("would evict", "ns", ns, "count", len(docs))
		return 0, nil
	}
	var behind removal
	removed, err := e.evictBehind(ctx, ns, hotColl, docs, &behind)
	n, werr := behind.wait()
	removed += n
	if err == nil {
		err = werr
	}
	return removed, err
}

// removal is one step's delete from the fast tier, running while the next
// step is confirmed and announced.
//
// Measured on the staging pair, 200,000 documents in one sweep: 41.4s, of
// which the deletes on the fast tier were 25.6s and the announcements on the
// archive 11.1s, one after the other. They are two replica sets, and nothing
// orders one step's delete against the next step's announcement: the steps are
// disjoint ranges of _id, and the order that matters, announce before delete,
// is per document and kept inside each step.
//
// One step deep, no more. The next step's delete starts only once this one
// has finished, so the feeder sees the deletes in sweep order as before, and
// at most two steps' documents are held at once.
type removal struct {
	done    chan struct{}
	removed int
	err     error
}

func (r *removal) start(f func() (int, error)) {
	r.done = make(chan struct{})
	go func() {
		defer close(r.done)
		r.removed, r.err = f()
	}()
}

// wait returns what the delete behind removed, and nothing when none is
// running.
func (r *removal) wait() (int, error) {
	if r.done == nil {
		return 0, nil
	}
	<-r.done
	n, err := r.removed, r.err
	*r = removal{}
	return n, err
}

// evictBehind confirms and announces each step, then leaves its delete running
// in behind and returns. What it reports removed is what the deletes it waited
// for removed; the caller waits for the last one.
func (e *Evictor) evictBehind(ctx context.Context, ns string, hotColl *mongo.Collection, docs []candidate, behind *removal) (int, error) {
	chunks, err := e.bySize(ctx, hotColl, docs)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, chunk := range chunks {
		present, err := e.prepareChunk(ctx, ns, hotColl, chunk)
		if err != nil {
			return removed, err
		}
		n, werr := behind.wait()
		removed += n
		if werr != nil {
			// The sweep stops here, so this step's delete is never sent and
			// its announcements would stand for documents still on the fast
			// tier. A standing one turns an application delete within the hour
			// into an eviction, and the archive keeps what the application
			// deleted. Unlike the short delete removeChunk leaves alone, nothing of this
			// step was removed, so taking all of them back is exact.
			if len(present) > 0 {
				if err := e.withdraw(context.WithoutCancel(ctx), present); err != nil {
					e.log.Warn("could not withdraw announcements for documents not evicted",
						"ns", ns, "count", len(present), "err", err)
				}
			}
			return removed, werr
		}
		if len(present) == 0 {
			continue
		}
		behind.start(func() (int, error) { return e.removeChunk(ctx, ns, hotColl, present) })
	}
	return removed, nil
}

// evictChunkBytes bounds how much document data one step of an eviction holds.
// See bySize.
const evictChunkBytes = 64 << 20

// bySize splits a batch into steps of at most evictChunkBytes of document
// data, by asking the fast tier for each candidate's size first.
//
// Eviction compares every candidate whole on both tiers, since 0be762a, and a
// batch is up to batch_size documents: at the default 500 and near the 16MB
// limit that is gigabytes from each tier held at once. The sizes cost one
// indexed query that returns two small fields a document. A candidate the fast
// tier no longer holds is dropped here, as presentOnFastTier would drop it.
func (e *Evictor) bySize(ctx context.Context, hotColl *mongo.Collection, docs []candidate) ([][]candidate, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	ids := make([]bson.RawValue, len(docs))
	for i, d := range docs {
		ids[i] = d.id
	}
	cur, err := hotColl.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{
			{Key: "_id", Value: 1},
			{Key: "s", Value: bson.D{{Key: "$bsonSize", Value: "$$ROOT"}}},
		}))
	if err != nil {
		return nil, fmt.Errorf("size the batch before evicting: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	ns := e.nsOf(docs)
	size := make(map[string]int64, len(docs))
	for cur.Next(ctx) {
		id, derr := cur.Current.LookupErr("_id")
		if derr != nil {
			continue
		}
		raw := bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)}
		var n int64
		if v, lerr := cur.Current.LookupErr("s"); lerr == nil {
			if i, ok := v.AsInt64OK(); ok {
				n = i
			}
		}
		size[meta.DocID(ns, raw)] = n
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("size the batch before evicting: %w", err)
	}
	var chunks [][]candidate
	var step []candidate
	var held int64
	for _, d := range docs {
		n, ok := size[d.docID]
		if !ok {
			continue
		}
		if len(step) > 0 && held+n > evictChunkBytes {
			chunks = append(chunks, step)
			step, held = nil, 0
		}
		step = append(step, d)
		held += n
	}
	if len(step) > 0 {
		chunks = append(chunks, step)
	}
	return chunks, nil
}

// prepareChunk is the first half of one step of evict: confirm and announce.
// It returns the documents announced and still free to remove, for
// removeChunk.
func (e *Evictor) prepareChunk(ctx context.Context, ns string, hotColl *mongo.Collection, docs []candidate) ([]candidate, error) {

	// Announce only what is actually there, so the announcements and the
	// deletes cover the same documents.
	//
	// Without this the two sets drift: something can remove a candidate between
	// the scan and here, and the delete then reports fewer removals than were
	// announced. The first attempt at handling that took the surplus back from
	// an arbitrary prefix of the batch, because a bulk delete does not say which
	// id it missed. That withdrew announcements belonging to documents it had
	// just evicted, and the feeder tombstoned them: 141 documents lost across
	// two collections on the pilot, by the fix meant to stop exactly that.
	present, err := e.presentOnFastTier(ctx, hotColl, docs)
	if err != nil {
		return nil, err
	}
	if len(present) == 0 {
		return nil, nil
	}
	current, err := e.currentInArchive(ctx, ns, present)
	if err != nil {
		return nil, err
	}
	if changed := len(present) - len(current); changed > 0 {
		e.log.Info("not evicting documents changed since they were mirrored",
			"ns", ns, "count", changed,
			"reason", "the archive does not yet hold the version the fast tier does",
			"effect", "they stay on the fast tier until a later sweep finds the archive caught up")
	}
	present = current
	if len(present) == 0 {
		return nil, nil
	}
	if err := e.announce(ctx, present); err != nil {
		return nil, err
	}
	// And holds asked again, after announcing, for the other half of a
	// handshake with the router. A router about to let mongod answer an insert
	// of an _id both tiers hold writes a hold on that document and then looks
	// for this announcement; this writes the announcement and then looks for
	// the hold. Whichever is second sees the first, so an eviction and that
	// insert cannot both go ahead. Without it the router could read the fast
	// tier's copy, this could delete it, and mongod would then accept the
	// insert and the feeder mirror it over the archived document. See
	// router.archivedIDClashesNow.
	return e.dropHeldAfterAnnouncing(ctx, ns, present)
}

// removeChunk is the second half: delete what prepareChunk announced, and end
// the holds it was chosen under.
func (e *Evictor) removeChunk(ctx context.Context, ns string, hotColl *mongo.Collection, present []candidate) (int, error) {

	ids := make([]bson.RawValue, len(present))
	docIDs := make([]string, len(present))
	for i, d := range present {
		ids[i] = d.id
		docIDs[i] = d.docID
	}
	// Each delete insists on the exact document the archive was compared
	// against, so one changed in the moment since then is left alone rather
	// than removed with only its older version archived. $literal because the
	// document is the application's and a string in it starting with $ would
	// otherwise be read as a field path.
	//
	// Under the simple collation, whatever the collection's default. $eq in
	// $expr compares strings by the collection's collation, so in one created
	// with {locale: "en", strength: 2} a document the application had changed
	// from "bob" to "Bob" still equalled the "bob" that was archived, and the
	// newer version was deleted with only the older one kept. Measured on a
	// plain mongod: that filter deletes the changed document under the
	// collection's collation and leaves it under the simple one.
	models := make([]mongo.WriteModel, len(present))
	for i, d := range present {
		filter := bson.D{
			{Key: "_id", Value: d.id},
			{Key: "$expr", Value: bson.D{{Key: "$eq", Value: bson.A{"$$ROOT", bson.D{{Key: "$literal", Value: d.doc}}}}}},
		}
		models[i] = mongo.NewDeleteOneModel().SetFilter(filter).SetCollation(simpleCollation)
	}
	res, err := meta.ParallelBulkWrite(ctx, hotColl, models)
	if err != nil {
		return 0, fmt.Errorf("delete from fast tier %s: %w", ns, err)
	}
	e.log.Debug("evicted", "ns", ns, "deleted", res.DeletedCount)
	if int(res.DeletedCount) < len(present) {
		// Still possible, in the moment between the check above and this
		// delete, and nothing here can say which id it was. The announcements
		// stay where they are.
		//
		// Leaving one standing costs a later application delete of that same
		// document being read as an eviction, so the archive keeps a record it
		// should have marked deleted, and the notice expires within the hour.
		// Taking one back wrongly costs a document evicted from the fast tier
		// and tombstoned in the archive, which is gone from both. Of the two,
		// keep the data.
		e.log.Info("fewer documents to evict than were there a moment ago",
			"ns", ns, "announced", len(present), "deleted", res.DeletedCount,
			"note", "something else removed them first; the announcements are left standing, "+
				"because taking back the wrong one loses a document")
	}

	// The hold is over once the document has left the fast tier, and the record
	// is ended rather than deleted. That is the whole difference between a
	// change stream that is right and one that is not.
	//
	// Deleting it took away the evidence that this document's copy-up inserts
	// were promotions. A consumer reading its stream later still has those
	// inserts ahead of it, and the router decides what to hide when it hands
	// them over: with the record gone, or recreated by a later copy-up whose
	// promotedAt is newer than the insert being read, the copy-up arrives as an
	// application insert. Measured with the fuzzer, seed 90801: four of them in
	// one round, every promotedAt one to two seconds after the event it was
	// judging.
	//
	// Ending it releases the hold exactly as deleting did -- residencyHolds
	// wants hotUntil in the future, and this puts it in the past -- and the row
	// then goes on its own expiry, which outlives the hold by NoticeTTL. The
	// same thing the feeder's dropResidency does for a document the application
	// deleted, and the third and last instance of one pattern: the evidence a
	// change stream reader needs was being destroyed the moment nothing else
	// needed it.
	//
	// "In the past" is decided by whoever reads it, so the instant comes from
	// the archive rather than from here. See meta.HoldIsOver.
	//
	// What is left is a bound rather than a guarantee: a consumer more than a
	// TTL pass behind reads the insert with no record to recognise it by. That
	// is the bound the row already had once the application deleted a document,
	// and it is a minute where it used to be nothing at all.
	//
	// Only the holds that have lapsed, because those are the ones this batch
	// was chosen under. The holds were checked when the batch was scanned, and
	// a router can copy one of these documents up, or refresh a lapsed hold,
	// between that check and here. Its hold is then running, and it is not this
	// eviction's to end: ending it says the document left the fast tier before
	// the router's copy-up put it back, so the copy-up's own insert reads as the
	// application writing that _id again.
	//
	// Measured on 2026-09-25 with two applications behind two routers: a hold
	// placed at 27.754, this delete at 27.794 and this update at 27.822, the
	// other router's copy-up at 27.833, and both applications' change streams
	// shown two seeded orders as orders just placed.
	//
	// A running hold left open is what the router's copy-up needed. The
	// document has gone and will be copied up again, which costs a read, and
	// its insert falls inside the promotion that record describes.
	//
	// Judged on the archive's clock, which is the one hotUntil was written in.
	//
	// Failing here is untidy rather than dangerous, so it only warns.
	if _, err := e.cold.Database(meta.DB).Collection(meta.Residency).UpdateMany(ctx,
		bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}},
			{Key: "$or", Value: bson.A{
				bson.D{{Key: "hotUntil", Value: bson.D{{Key: "$exists", Value: false}}}},
				bson.D{{Key: "$expr", Value: bson.D{{Key: "$lte", Value: bson.A{"$hotUntil", "$$NOW"}}}}},
			}},
		},
		meta.HoldIsOver(time.Now().UTC())); err != nil {
		e.log.Warn("could not end the residency records of evicted documents", "ns", ns, "err", err)
	}
	return int(res.DeletedCount), nil
}

// presentOnFastTier narrows a batch to the documents the fast tier still holds.
//
// One indexed lookup by _id, so that what is announced and what is deleted are
// the same set. See evict for what happens when they are not.
func (e *Evictor) presentOnFastTier(ctx context.Context, hotColl *mongo.Collection, docs []candidate) ([]candidate, error) {
	ids := make([]bson.RawValue, len(docs))
	for i, d := range docs {
		ids[i] = d.id
	}
	// Whole documents, not only the _id: what the fast tier holds is what the
	// archive's copy has to equal. See currentInArchive.
	cur, err := hotColl.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return nil, fmt.Errorf("check the fast tier before evicting: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	here := make(map[string]bson.Raw, len(docs))
	for cur.Next(ctx) {
		id, derr := cur.Current.LookupErr("_id")
		if derr != nil {
			continue
		}
		raw := bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)}
		here[meta.DocID(e.nsOf(docs), raw)] = bson.Raw(append([]byte(nil), cur.Current...))
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("check the fast tier before evicting: %w", err)
	}

	out := docs[:0:0]
	for _, d := range docs {
		if doc, ok := here[d.docID]; ok {
			d.doc = doc
			out = append(out, d)
		}
	}
	return out, nil
}

// currentInArchive narrows a batch to the documents whose archived copy is the
// one the fast tier holds now, byte for byte.
//
// Confirming the archive holds a document by _id was not enough. A document
// updated a moment ago has an older version in the archive until the feeder
// mirrors the update, and evicting it then removed the only copy of the new
// version: the feeder's lookup for the update event came back empty, it
// skipped the event on the grounds that a delete was coming, and the delete
// that came was the announced eviction. Measured on 2026-09-25 with the feeder
// held back: twenty old documents each updated once through the router, one
// sweep, and afterwards the router answered the old value for all twenty.
// Twenty acknowledged writes, lost.
//
// Byte for byte because the archive's copy is the fast tier's document
// replaced verbatim, so equal bytes are the same version, and anything else is
// a version this eviction cannot prove the archive has. Those stay, and a
// later sweep takes them once mirroring has caught up.
func (e *Evictor) currentInArchive(ctx context.Context, ns string, docs []candidate) ([]candidate, error) {
	db, coll := splitNS(ns)
	ids := make([]bson.RawValue, len(docs))
	for i, d := range docs {
		ids[i] = d.id
	}
	cur, err := e.cold.Database(db).Collection(coll).Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}, meta.NotArchivedDeleted()})
	if err != nil {
		return nil, fmt.Errorf("read the archive's copies before evicting: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	archived := make(map[string]bson.Raw, len(docs))
	for cur.Next(ctx) {
		id, derr := cur.Current.LookupErr("_id")
		if derr != nil {
			continue
		}
		raw := bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)}
		archived[meta.DocID(ns, raw)] = bson.Raw(append([]byte(nil), cur.Current...))
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("read the archive's copies before evicting: %w", err)
	}
	out := docs[:0:0]
	for _, d := range docs {
		if a, ok := archived[d.docID]; ok && bytes.Equal(a, d.doc) {
			out = append(out, d)
		}
	}
	return out, nil
}

// nsOf reads the namespace back off a batch, which every candidate carries
// inside its docID.
func (e *Evictor) nsOf(docs []candidate) string {
	if len(docs) == 0 {
		return ""
	}
	ns, _, _ := strings.Cut(docs[0].docID, "|")
	return ns
}

func (e *Evictor) announce(ctx context.Context, docs []candidate) error {
	docIDs := make([]string, len(docs))
	for i, d := range docs {
		docIDs[i] = d.docID
	}
	return meta.AnnounceEviction(ctx, e.cold, docIDs)
}

func (e *Evictor) withdraw(ctx context.Context, docs []candidate) error {
	docIDs := make([]string, len(docs))
	for i, d := range docs {
		docIDs[i] = d.docID
	}
	return meta.WithdrawEviction(ctx, e.cold, docIDs)
}

// ErrUnhealthy is returned when the cluster did not become fit for eviction
// within the allowed wait.
var ErrUnhealthy = errors.New("cluster not healthy enough to evict")

// waitForHealth blocks until every signal is inside its limit. It reports
// whether it had to wait at all.
//
// A dry run never blocks. It changes nothing, so refusing to report what would
// happen because the oplog is short would make the flag useless exactly when an
// operator is trying to understand the system.
func (e *Evictor) waitForHealth(ctx context.Context) (bool, error) {
	const pollEvery = 5 * time.Second
	const remindEvery = time.Minute

	if e.dryRun {
		reading := e.check.Sample(ctx)
		if verdict := health.Evaluate(reading, e.cfg.Backoff, e.cfg.MaxFeederLag.D()); !verdict.OK {
			e.log.Warn("eviction would be paused right now, continuing anyway because this is a dry run",
				"reason", verdict.Reason, "unavailable", reading.Missing)
		}
		return false, nil
	}

	var (
		waited     bool
		start      = time.Now()
		lastNotice time.Time
	)
	for {
		// The sweep is still this process's while it waits, so the claim on it
		// is kept up. A pause for mirroring lag lasts as long as mirroring
		// takes to catch up, which is far longer than the lease outlives a
		// holder, and a second evictor picking the work up while this one sits
		// here waiting for the same cluster would help nobody.
		if err := e.keepLease(ctx); err != nil {
			return waited, err
		}

		reading := e.check.Sample(ctx)
		verdict := health.Evaluate(reading, e.cfg.Backoff, e.cfg.MaxFeederLag.D())
		if verdict.OK {
			if waited {
				e.log.Info("resuming eviction", "paused_for", time.Since(start).Round(time.Second))
			}
			return waited, nil
		}

		// Repeat the reason periodically. A pause that logs once and then goes
		// quiet is indistinguishable from a hang, which is how an operator ends
		// up killing a daemon that was doing exactly the right thing.
		if !waited || time.Since(lastNotice) >= remindEvery {
			e.log.Warn("eviction paused",
				"reason", verdict.Reason,
				"waiting_for", time.Since(start).Round(time.Second),
				"unavailable", reading.Missing)
			lastNotice = time.Now()
		}
		waited = true

		if e.maxHealthWait > 0 && time.Since(start) >= e.maxHealthWait {
			return waited, fmt.Errorf("%w after %s: %s",
				ErrUnhealthy, time.Since(start).Round(time.Second), verdict.Reason)
		}

		select {
		case <-ctx.Done():
			return waited, ctx.Err()
		case <-time.After(pollEvery):
		}
	}
}

func splitNS(ns string) (db, coll string) {
	db, coll, _ = strings.Cut(ns, ".")
	return db, coll
}

func selectorName(rule config.EvictionRule) string {
	if rule.UsesObjectIDTime() {
		return "_id timestamp"
	}
	return rule.DateField
}

// warnAboutUnaged says so when the fast tier holds documents this rule can
// never remove.
//
// The whole-namespace version of this is refused above: a rule with no
// date_field against non-ObjectId _ids matches nothing at all. The partial
// version is quieter and reads as success. MongoDB brackets a range predicate
// by type, so a cutoff on a date selects only documents whose field holds a
// date, and one whose field is missing, null or a string is invisible to every
// sweep. Measured on a fast tier of three documents, one properly dated and two
// not: "sweep complete, scanned 1, evicted 1", and the two that cannot be aged
// stayed where they were with nothing mentioning them.
//
// That is a fast tier that grows without bound while every sweep reports
// success, which is the same failure the check above exists to prevent, one
// field along.
//
// Counted once per rule per process rather than per sweep. The predicate cannot
// use an index, so it is a scan, and a scan on the fast tier every few seconds
// would cost more than the warning is worth. Once at startup matches the check
// above it, and a number that changes is a restart away from being seen.
func (e *Evictor) warnAboutUnaged(ctx context.Context, rule config.EvictionRule, hotColl *mongo.Collection) {
	if rule.UsesObjectIDTime() {
		return
	}
	e.unagedMu.Lock()
	if e.unagedSeen == nil {
		e.unagedSeen = map[string]bool{}
	}
	seen := e.unagedSeen[rule.NS]
	e.unagedSeen[rule.NS] = true
	e.unagedMu.Unlock()
	if seen {
		return
	}

	n, err := hotColl.CountDocuments(ctx, bson.D{{Key: rule.DateField, Value: bson.D{
		{Key: "$not", Value: bson.D{{Key: "$type", Value: "date"}}},
	}}})
	if err == nil && n > 0 {
		e.log.Warn("the fast tier holds documents this rule can never remove",
			"ns", rule.NS, "not_dated_by_"+rule.DateField, n,
			"reason", "a cutoff on a date selects only documents whose field holds a date, because "+
				"MongoDB brackets a range predicate by type; one whose "+rule.DateField+" is missing, "+
				"null or a string is invisible to every sweep",
			"effect", "these stay on the fast tier however old they are, so it grows by that much and "+
				"every sweep goes on reporting success",
			"action", "give them a date in "+rule.DateField+", or remove them by hand once the archive "+
				"is confirmed to hold them")
	}

	// And the ones bracketing lets through, which are excluded on purpose. See
	// candidateFilter: a document whose age field is an array is older than the
	// cutoff and newer than it at once, because a range predicate on an array
	// matches when any element does. Removing one leaves a short answer inside
	// the range the claim promises.
	//
	// Reported separately from the count above, because the reason and the
	// action are different: those cannot be selected at all, and these could be
	// and must not be.
	na, aerr := hotColl.CountDocuments(ctx, bson.D{{Key: rule.DateField, Value: bson.D{
		{Key: "$type", Value: "array"},
	}}})
	if aerr != nil || na == 0 {
		return
	}
	e.log.Warn("the fast tier holds documents this rule will not remove, because their age is an array",
		"ns", rule.NS, "array_"+rule.DateField, na,
		"reason", "a range predicate on an array matches when any element matches, so a document "+
			"holding [old, new] in "+rule.DateField+" is older than the cutoff and newer than it "+
			"at the same time",
		"effect", "removing one would leave a read bounded inside the completeness claim short by "+
			"that document, with no error on it, so these stay where they are and the fast tier "+
			"grows by that much",
		"action", "give each document one date in "+rule.DateField+", or age this namespace by a "+
			"field that holds one")
}

// settleClaim waits long enough for every reader holding the previous claim to
// have re-read it. See watermark.ClaimCacheTTL.
//
// The margin is a second, because the two clocks involved are different
// machines' and the TTL is measured from when a reader fetched rather than from
// when this wrote.
func (e *Evictor) settleClaim(ctx context.Context) error {
	if e.dryRun {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(watermark.ClaimCacheTTL + time.Second):
		return nil
	}
}

// narrowClaims raises every rule's completeness watermark to the boundary this
// sweep will evict against, and then waits for readers to have seen it.
//
// Before removing anything, and in that order. A claim says the fast tier holds
// everything newer than an instant; the documents about to be removed are older
// than that instant, so the claim has to stop covering them first. Doing it this
// way round also means an interrupted sweep leaves a claim that understates what
// is there and never one that overstates it, which is the direction that cannot
// produce a wrong answer.
//
// The wait afterwards is the other half, and the reason it is here rather than
// inside each rule. A router caches the claim, so for ClaimCacheTTL after the
// raise its copy is the older and wider one, and removing in that window means
// it answers from a fast tier that no longer holds what it believes. Waiting
// once for the whole sweep serves every rule, because by the time the first
// removal happens every claim has already been narrowed.
func (e *Evictor) narrowClaims(ctx context.Context, boundaries map[string]time.Time) error {
	if e.dryRun {
		return nil
	}
	raisedAny := false
	for _, rule := range e.cfg.Rules {
		boundary, ok := boundaries[rule.NS]
		if !ok {
			continue
		}
		raised, err := e.marks.RaiseTo(ctx, rule.NS, boundary, rule.DateField)
		if err != nil {
			return err
		}
		if raised {
			raisedAny = true
			e.log.Info("completeness watermark raised to the eviction boundary",
				"ns", rule.NS, "complete_from", boundary.Format(time.RFC3339))
		}
	}
	if !raisedAny {
		return nil
	}
	return e.settleClaim(ctx)
}

// checkClock says how far this machine's clock is from the archive's, once.
//
// It used to be the one that destroyed data. An eviction announcement was
// stamped here and expired by the archive's TTL monitor, so a clock running
// behind had its announcements expiring early, and far enough behind they were
// gone before the feeder read the delete they explained: the feeder took that
// delete for the application's own, the archive tombstoned its copy, and the
// document was gone from both tiers. The archive stamps those rows now, as it
// writes them, so the clock that starts the hour is the clock that ends it and
// this one does not come into it.
//
// What is left is real and smaller. Every eviction rule's cutoff is worked out
// here, against the dates in the application's own documents, so a clock that
// is out by an hour evicts an hour's worth of the wrong documents: the window
// shifts rather than breaking. Nothing else here judges another daemon's
// instant by this clock any more.
//
// Reported rather than refused. A clock can be corrected while this runs, and a
// sweep that stops because of one is a fast tier that fills up instead.
func (e *Evictor) checkClock(ctx context.Context) {
	skew, err := health.Skew(ctx, e.cold)
	if err != nil {
		e.log.Debug("could not compare this clock with the archive's", "err", err)
		return
	}
	e.skewMu.Lock()
	e.skew, e.skewKnown = skew, true
	e.skewMu.Unlock()

	// Measured every pass and published every pass; said out loud once, because
	// a clock that disagrees disagrees on every sweep and the log is not where
	// an alert should live.
	e.clockOnce.Do(func() {
		if health.AbsDuration(skew) <= health.SkewWarn {
			e.log.Debug("clocks agree with the archive", "skew", skew.Round(time.Millisecond).String())
			return
		}
		e.log.Warn("this clock and the archive's do not agree",
			"skew", skew.Round(time.Millisecond).String(),
			"note", "every eviction cutoff is worked out here, against the dates in your own "+
				"documents, so the window this sweep uses is out by about this much",
			"action", "run NTP on every host that runs a cline daemon")
	})
}

// simpleCollation is byte-wise string comparison, for the checks that must see
// a change of case as a change.
var simpleCollation = &options.Collation{Locale: "simple"}

// dropHeldAfterAnnouncing takes out of a just-announced batch the documents a
// hold was placed on since the batch was chosen, and takes their announcements
// back. Taking one back is right here, unlike after a delete, because these are
// never deleted: the announcement is withdrawn for exactly the documents left
// alone.
func (e *Evictor) dropHeldAfterAnnouncing(ctx context.Context, ns string, present []candidate) ([]candidate, error) {
	docIDs := make([]string, len(present))
	for i, d := range present {
		docIDs[i] = d.docID
	}
	held, err := e.residencyHolds(ctx, docIDs)
	if err != nil {
		return nil, err
	}
	if len(held) == 0 {
		return present, nil
	}
	keep := present[:0:0]
	var back []string
	for _, d := range present {
		if held[d.docID] {
			back = append(back, d.docID)
			continue
		}
		keep = append(keep, d)
	}
	if err := meta.WithdrawEviction(ctx, e.cold, back); err != nil {
		return nil, fmt.Errorf("withdraw announcements for documents held since: %w", err)
	}
	e.log.Info("not evicting documents a hold was placed on while the batch was announced",
		"ns", ns, "count", len(back))
	return keep, nil
}
