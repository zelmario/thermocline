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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/health"
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
	if err := meta.PublishDaemonReport(ctx, e.cold, "evictor", s.DryRun, counters); err != nil {
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
	}
}

// RunOnce walks every rule once and returns when there is nothing left to
// remove at the current boundary.
func (e *Evictor) RunOnce(ctx context.Context) (Stats, error) {
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

	total := Stats{DryRun: e.dryRun}
	if e.dryRun {
		e.log.Info("dry run, nothing will be removed")
	}

	for _, rule := range e.cfg.Rules {
		s, err := e.runRule(ctx, rule)
		total.Scanned += s.Scanned
		total.Evicted += s.Evicted
		total.Held += s.Held
		total.Unconfirmed += s.Unconfirmed
		total.Backoffs += s.Backoffs
		if err != nil {
			return total, err
		}
	}
	e.publish(ctx, total)
	return total, nil
}

// Run walks every rule repeatedly, pausing between sweeps.
func (e *Evictor) Run(ctx context.Context, every time.Duration) error {
	for {
		stats, err := e.RunOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
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

func (e *Evictor) runRule(ctx context.Context, rule config.EvictionRule) (Stats, error) {
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

	boundary := cutoff(time.Now(), rule.OlderThan.D())
	e.log.Info("evicting",
		"ns", rule.NS,
		"older_than", rule.OlderThan.String(),
		"cutoff", boundary.Format(time.RFC3339),
		"selector", selectorName(rule))

	// Raise the completeness watermark before removing anything.
	//
	// Everything older than the boundary is about to leave the fast tier, so it
	// cannot go on claiming to hold that range completely. Doing this first,
	// rather than after, means an interrupted sweep still leaves an honest
	// watermark: it may understate what is there, and it will never overstate
	// it. An overstated watermark makes range queries return short results and
	// report success, which is the failure this system exists to avoid.
	if !e.dryRun {
		raised, err := e.marks.RaiseTo(ctx, rule.NS, boundary)
		if err != nil {
			return stats, err
		}
		if raised {
			e.log.Info("completeness watermark raised to the eviction boundary",
				"ns", rule.NS, "complete_from", boundary.Format(time.RFC3339))
		}
	}

	for {
		if err := ctx.Err(); err != nil {
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

		if len(out.evict) > 0 {
			if err := e.evict(ctx, rule.NS, hotColl, out.evict); err != nil {
				return stats, err
			}
			stats.Evicted += len(out.evict)
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
func (e *Evictor) residencyHolds(ctx context.Context, docIDs []string) (map[string]bool, error) {
	cur, err := e.cold.Database(meta.DB).Collection(meta.Residency).Find(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}},
		{Key: "hotUntil", Value: bson.D{{Key: "$gt", Value: time.Now().UTC()}}},
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
func (e *Evictor) evict(ctx context.Context, ns string, hotColl *mongo.Collection, docs []candidate) error {
	if e.dryRun {
		e.log.Info("would evict", "ns", ns, "count", len(docs))
		return nil
	}

	if err := e.announce(ctx, docs); err != nil {
		return err
	}

	ids := make([]bson.RawValue, len(docs))
	for i, d := range docs {
		ids[i] = d.id
	}
	res, err := hotColl.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return fmt.Errorf("delete from fast tier %s: %w", ns, err)
	}
	e.log.Debug("evicted", "ns", ns, "deleted", res.DeletedCount)

	// Residency is meaningless once the document has left the fast tier.
	// Failing here is untidy rather than dangerous, so it only warns.
	docIDs := make([]string, len(docs))
	for i, d := range docs {
		docIDs[i] = d.docID
	}
	if _, err := e.cold.Database(meta.DB).Collection(meta.Residency).
		DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}}}); err != nil {
		e.log.Warn("could not clear residency records", "ns", ns, "err", err)
	}
	return nil
}

func (e *Evictor) announce(ctx context.Context, docs []candidate) error {
	now := time.Now().UTC()
	models := make([]mongo.WriteModel, len(docs))
	for i, d := range docs {
		models[i] = mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: d.docID}}).
			SetReplacement(meta.EvictionNotice{ID: d.docID, At: now}).
			SetUpsert(true)
	}

	// Unordered and idempotent, so a retry after a partial failure is safe.
	_, err := e.cold.Database(meta.DB).Collection(meta.Evicting).
		BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("announce eviction: %w", err)
	}
	return nil
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
