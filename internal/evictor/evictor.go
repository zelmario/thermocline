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
	"sync"
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

	// unagedSeen records which namespaces have already been counted for
	// documents the rule cannot age, so the scan that costs happens once per
	// process rather than once per sweep. See warnAboutUnaged.
	// clockOnce keeps the comparison with the archive's clock to one per run:
	// it is a property of the machines, not of a sweep.
	clockOnce sync.Once

	unagedMu   sync.Mutex
	unagedSeen map[string]bool
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

func (e *Evictor) runRule(ctx context.Context, rule config.EvictionRule, boundary time.Time) (Stats, error) {
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
		return err
	}
	if len(present) == 0 {
		return nil
	}
	if err := e.announce(ctx, present); err != nil {
		return err
	}

	ids := make([]bson.RawValue, len(present))
	docIDs := make([]string, len(present))
	for i, d := range present {
		ids[i] = d.id
		docIDs[i] = d.docID
	}
	res, err := hotColl.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return fmt.Errorf("delete from fast tier %s: %w", ns, err)
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

	// Residency is meaningless once the document has left the fast tier.
	// Failing here is untidy rather than dangerous, so it only warns.
	if _, err := e.cold.Database(meta.DB).Collection(meta.Residency).
		DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}}}); err != nil {
		e.log.Warn("could not clear residency records", "ns", ns, "err", err)
	}
	return nil
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
	cur, err := hotColl.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("check the fast tier before evicting: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	here := make(map[string]struct{}, len(docs))
	for cur.Next(ctx) {
		id, derr := cur.Current.LookupErr("_id")
		if derr != nil {
			continue
		}
		raw := bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)}
		here[meta.DocID(e.nsOf(docs), raw)] = struct{}{}
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("check the fast tier before evicting: %w", err)
	}

	out := docs[:0:0]
	for _, d := range docs {
		if _, ok := here[d.docID]; ok {
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
	if err != nil || n == 0 {
		return
	}
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
		raised, err := e.marks.RaiseTo(ctx, rule.NS, boundary)
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
// The direction that destroys data is behind: an eviction announcement is
// stamped here and expired by the archive's TTL monitor, so a clock running
// behind has its announcements expiring early, and far enough behind they are
// gone before the feeder reads the delete they explain. It then takes that
// delete for the application's own, the archive tombstones its copy, and the
// document is gone from both tiers. See health.Skew and meta.NoticeTTL.
//
// Reported rather than refused. A clock can be corrected while this runs, and a
// sweep that stops because of one is a fast tier that fills up instead.
func (e *Evictor) checkClock(ctx context.Context) {
	e.clockOnce.Do(func() {
		skew, err := health.Skew(ctx, e.cold)
		if err != nil {
			e.log.Debug("could not compare this clock with the archive's", "err", err)
			return
		}
		behind := -skew
		switch {
		case behind >= meta.NoticeTTL:
			e.log.Error("this clock is far enough behind the archive's to lose documents",
				"behind", behind.Round(time.Second).String(),
				"notice_ttl", meta.NoticeTTL.String(),
				"reason", "an eviction announcement is stamped here and expired by the archive, so "+
					"one written now is already older than the archive keeps them",
				"effect", "the feeder reads every eviction as the application's own delete and "+
					"tombstones the archive's copy, losing the document from both tiers",
				"action", "fix the clock on this machine before letting a sweep run")
		case skew > health.SkewWarn || behind > health.SkewWarn:
			e.log.Warn("this clock and the archive's do not agree",
				"skew", skew.Round(time.Millisecond).String(),
				"note", "eviction announcements are stamped here and expired there, and residency "+
					"holds are written by the router and read here",
				"action", "run NTP on both, or expect the windows to differ from the configuration "+
					"by about this much")
		default:
			e.log.Debug("clocks agree with the archive", "skew", skew.Round(time.Millisecond).String())
		}
	})
}
