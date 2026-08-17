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

	marks   *watermark.Store
	limiter *limiter
}

// SetMaxHealthWait bounds how long RunOnce waits for an unhealthy cluster
// before giving up. Zero, the default, waits indefinitely.
func (e *Evictor) SetMaxHealthWait(d time.Duration) { e.maxHealthWait = d }

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

// archived returns the documents the cold tier actually holds. This is the
// check that makes eviction safe, so it queries the archive directly rather
// than trusting any bookkeeping.
func (e *Evictor) archived(ctx context.Context, ns string, cands []candidate, ids []bson.RawValue) (map[string]bool, error) {
	db, coll := splitNS(ns)
	cur, err := e.cold.Database(db).Collection(coll).Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
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
