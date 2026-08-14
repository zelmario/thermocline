// Package warmer fills the fast tier from the archive.
//
// Traffic alone fills the fast tier sparsely, one document at a time. That
// serves a lookup by id and is useless for a range query, because holding 12 of
// a customer's 40 orders looks exactly like holding all of them. The warmer
// fills contiguous ranges instead, which is what lets the router state a
// guarantee: everything newer than this instant is present here.
//
// It walks newest first, so the guarantee extends further back with every
// batch, and the watermark it records doubles as the resume point.
package warmer

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

	"github.com/zelmar/thermocline/internal/config"
	"github.com/zelmar/thermocline/internal/health"
	"github.com/zelmar/thermocline/internal/meta"
	"github.com/zelmar/thermocline/internal/watermark"
)

// Warmer copies documents from the archive into the fast tier.
type Warmer struct {
	hot    *mongo.Client
	cold   *mongo.Client
	cfg    config.WarmerConfig
	check  *health.Checker
	log    *slog.Logger
	dryRun bool

	// unannounced is the feeder's delete policy. Under "ignore" the archive
	// keeps deleted documents readable, which makes a count comparison between
	// the tiers meaningless.
	unannounced config.UnannouncedDeletes

	marks   *watermark.Store
	limiter *limiter
}

// Stats reports one run.
type Stats struct {
	Scanned  int
	Warmed   int
	Present  int
	Backoffs int
	DryRun   bool
}

// LogArgs returns the summary as slog key-value pairs, labelled honestly on a
// dry run.
func (s Stats) LogArgs() []any {
	copied := "warmed"
	if s.DryRun {
		copied = "would_warm"
	}
	return []any{
		"scanned", s.Scanned,
		copied, s.Warmed,
		"already_present", s.Present,
		"backoffs", s.Backoffs,
	}
}

// New builds a Warmer. It does not contact either cluster.
func New(hot, cold *mongo.Client, cfg config.WarmerConfig, unannounced config.UnannouncedDeletes, dryRun bool, log *slog.Logger) *Warmer {
	return &Warmer{
		hot:         hot,
		cold:        cold,
		cfg:         cfg,
		unannounced: unannounced,
		check:       health.New(hot, cold, ""),
		log:         log,
		dryRun:      dryRun,
		marks:       watermark.New(cold),
		limiter:     newLimiter(cfg.Rate),
	}
}

// Run walks every rule until each namespace is complete back to its target.
func (w *Warmer) Run(ctx context.Context) (Stats, error) {
	total := Stats{DryRun: w.dryRun}
	if w.dryRun {
		w.log.Info("dry run, nothing will be copied")
	}

	// Announce that a warm is in progress so the feeder suppresses the echo.
	// Without this, every document copied up is mirrored straight back down
	// into the archive it was just read from.
	stop := w.startHeartbeat(ctx)
	defer stop()

	for _, rule := range w.cfg.Rules {
		s, err := w.runRule(ctx, rule)
		total.Scanned += s.Scanned
		total.Warmed += s.Warmed
		total.Present += s.Present
		total.Backoffs += s.Backoffs
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// verifyClaim checks that the fast tier still holds everything its watermark
// says it does, and reports how far short it is.
//
// Telling a hole from a legitimate deletion looked like it needed a
// document-by-document comparison, and it does not. The archive is upsert only:
// a document the application deleted is tombstoned there, not removed. So the
// documents live in the archive are exactly the documents the fast tier is
// claiming to hold, and two counts answer the question.
//
// This matters because the alternative is silence. A fast tier missing half a
// range still answers queries, still returns documents, and still reports
// success. Nothing else in the system would ever notice.
func (w *Warmer) verifyClaim(ctx context.Context, rule config.WarmRule, from time.Time) (short bool, hotN, coldN int64, err error) {
	// With this policy the archive keeps deleted documents readable, so it
	// legitimately holds more than the fast tier and the comparison means
	// nothing. Worse, acting on it would warm deleted documents back to life.
	if w.unannounced == config.Ignore {
		return false, 0, 0, nil
	}

	hotN, coldN, err = w.countBothInWindow(ctx, rule, from)
	if err != nil || hotN >= coldN {
		return false, hotN, coldN, err
	}

	// A delete applied to the fast tier is tombstoned in the archive a moment
	// later, so during that moment the archive legitimately shows more. Look
	// again before discarding a claim over it.
	select {
	case <-ctx.Done():
		return false, hotN, coldN, ctx.Err()
	case <-time.After(recheckDelay):
	}
	hotN, coldN, err = w.countBothInWindow(ctx, rule, from)
	if err != nil {
		return false, hotN, coldN, err
	}
	return hotN < coldN, hotN, coldN, nil
}

// recheckDelay is how long verifyClaim waits before believing a shortfall.
const recheckDelay = 3 * time.Second

// countBothInWindow counts what each tier holds at or after from.
func (w *Warmer) countBothInWindow(ctx context.Context, rule config.WarmRule, from time.Time) (int64, int64, error) {
	filter, err := windowFilter(rule, from)
	if err != nil {
		return 0, 0, err
	}
	db, coll := splitNS(rule.NS)

	hotN, err := w.hot.Database(db).Collection(coll).CountDocuments(ctx, filter)
	if err != nil {
		return 0, 0, fmt.Errorf("count %s on the fast tier: %w", rule.NS, err)
	}
	coldN, err := w.cold.Database(db).Collection(coll).
		CountDocuments(ctx, append(filter, meta.NotArchivedDeleted()))
	if err != nil {
		return 0, 0, fmt.Errorf("count %s in the archive: %w", rule.NS, err)
	}
	return hotN, coldN, nil
}

// staleClaim reports whether a recorded watermark describes a fast tier that no
// longer exists.
//
// Nothing tells the warmer that the fast tier was restored, rebuilt or emptied.
// Those leave no change stream event behind, so the claim simply outlives the
// data it described, and the warmer used to trust it and do nothing at all. The
// decisive signal is cheap: a fast tier holding nothing whatsoever in a window
// the archive can fill is a fast tier that lost its contents. verifyClaim
// catches the subtler case of a range that is merely short.
func (w *Warmer) staleClaim(ctx context.Context, rule config.WarmRule, floor time.Time) (bool, error) {
	hotHas, err := w.anyInWindow(ctx, w.hot, rule, floor)
	if err != nil || hotHas {
		return false, err
	}
	return w.anyInWindow(ctx, w.cold, rule, floor)
}

// anyInWindow asks one tier whether it holds a single document at or after
// floor. One indexed lookup, no count.
func (w *Warmer) anyInWindow(ctx context.Context, client *mongo.Client, rule config.WarmRule, floor time.Time) (bool, error) {
	filter, err := windowFilter(rule, floor)
	if err != nil {
		return false, err
	}
	db, coll := splitNS(rule.NS)
	err = client.Database(db).Collection(coll).
		FindOne(ctx, filter, options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("probe %s: %w", rule.NS, err)
	}
	return true, nil
}

func (w *Warmer) runRule(ctx context.Context, rule config.WarmRule) (Stats, error) {
	var stats Stats
	now := time.Now().UTC()
	floor := now.Add(-rule.Back.D())

	saved, err := w.marks.Load(ctx, rule.NS)
	if err != nil {
		return stats, err
	}

	if saved.Claimed {
		stale, err := w.staleClaim(ctx, rule, floor)
		if err != nil {
			return stats, err
		}
		if stale {
			w.log.Warn("discarding a completeness claim the fast tier cannot support",
				"ns", rule.NS,
				"claimed_complete_from", saved.From.Format(time.RFC3339),
				"found", "the fast tier holds nothing in this window while the archive does",
				"note", "the fast tier was rebuilt, restored or emptied; warming the whole window again")
			if !w.dryRun {
				if _, err := w.marks.Clear(ctx, rule.NS); err != nil {
					return stats, err
				}
			}
			saved = watermark.Mark{}
		}
	}

	// The window is empty of neither tier, but it can still be full of holes.
	if saved.Claimed {
		short, hotN, coldN, verr := w.verifyClaim(ctx, rule, saved.From)
		switch {
		case verr != nil:
			// Not being able to check is not a reason to stop warming.
			w.log.Warn("could not verify the completeness claim", "ns", rule.NS, "err", verr)
		case short:
			w.log.Warn("discarding a completeness claim the fast tier cannot support",
				"ns", rule.NS,
				"claimed_complete_from", saved.From.Format(time.RFC3339),
				"fast_tier", hotN, "archive", coldN, "missing", coldN-hotN,
				"note", "documents are missing from a range claimed complete; warming it again")
			if !w.dryRun {
				if _, cerr := w.marks.Clear(ctx, rule.NS); cerr != nil {
					return stats, cerr
				}
			}
			saved = watermark.Mark{}
		}
	}

	start := resumePoint(saved, now)

	w.log.Info("warming",
		"ns", rule.NS,
		"back", rule.Back.String(),
		"from", start.Format(time.RFC3339),
		"to", floor.Format(time.RFC3339),
		"selector", selectorName(rule))

	if !start.After(floor) {
		w.log.Info("already complete", "ns", rule.NS, "complete_from", start.Format(time.RFC3339))
		return stats, nil
	}

	// Resume by starting just past the last completed point.
	before := bson.RawValue{}
	if saved.Claimed && saved.From.Before(now) {
		before = objectIDBound(saved.From)
	}

	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if backedOff, err := w.waitForHealth(ctx); err != nil {
			return stats, err
		} else if backedOff {
			stats.Backoffs++
		}

		filter, err := sourceFilter(rule, floor, before)
		if err != nil {
			return stats, err
		}

		docs, err := w.read(ctx, rule.NS, filter)
		if err != nil {
			return stats, err
		}
		if len(docs) == 0 {
			// Nothing older left inside the window, so the whole range is done.
			if err := w.marks.Set(ctx, rule.NS, floor); err != nil {
				return stats, err
			}
			w.log.Info("complete", "ns", rule.NS, "complete_from", floor.Format(time.RFC3339))
			return stats, nil
		}
		stats.Scanned += len(docs)
		before = docs[len(docs)-1].id

		written, err := w.write(ctx, rule.NS, docs)
		if err != nil {
			return stats, err
		}
		stats.Warmed += written
		stats.Present += len(docs) - written

		// The watermark only moves after the batch is durable in the fast tier.
		// Moving it first would claim completeness for documents still in
		// flight, which is the one mistake this component must not make.
		oldest, err := w.oldestOf(docs, rule)
		if err != nil {
			return stats, err
		}
		if !w.dryRun {
			if err := w.marks.Set(ctx, rule.NS, watermarkAfter(oldest)); err != nil {
				return stats, err
			}
		}

		w.limiter.spend(ctx, len(docs))

		if len(docs) < w.cfg.BatchSize {
			if !w.dryRun {
				if err := w.marks.Set(ctx, rule.NS, floor); err != nil {
					return stats, err
				}
			}
			w.log.Info("complete", "ns", rule.NS, "complete_from", floor.Format(time.RFC3339))
			return stats, nil
		}
	}
}

// sourceDoc is one archived document about to be copied up.
type sourceDoc struct {
	id  bson.RawValue
	raw bson.Raw
}

// read pulls one batch out of the archive, newest first.
func (w *Warmer) read(ctx context.Context, ns string, filter bson.D) ([]sourceDoc, error) {
	db, coll := splitNS(ns)
	cur, err := w.cold.Database(db).Collection(coll).Find(ctx, filter,
		options.Find().
			SetSort(bson.D{{Key: "_id", Value: -1}}).
			SetLimit(int64(w.cfg.BatchSize)))
	if err != nil {
		return nil, fmt.Errorf("read archive %s: %w", ns, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var docs []sourceDoc
	for cur.Next(ctx) {
		raw := append(bson.Raw(nil), cur.Current...)
		id, err := raw.LookupErr("_id")
		if err != nil {
			return nil, fmt.Errorf("read archive %s: document without _id", ns)
		}
		docs = append(docs, sourceDoc{
			id:  bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)},
			raw: raw,
		})
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("read archive %s: %w", ns, err)
	}
	return docs, nil
}

// write copies a batch into the fast tier and records residency for it.
// It returns how many documents were actually inserted.
func (w *Warmer) write(ctx context.Context, ns string, docs []sourceDoc) (int, error) {
	if w.dryRun {
		w.log.Info("would warm", "ns", ns, "count", len(docs))
		return len(docs), nil
	}

	// Announce first, for the same reason the evictor does: the feeder must be
	// able to recognise the resulting inserts before it sees them.
	if err := w.announce(ctx, ns, docs); err != nil {
		return 0, err
	}

	db, coll := splitNS(ns)
	models := make([]mongo.WriteModel, len(docs))
	for i, d := range docs {
		// $setOnInsert, never a replace. The fast tier is authoritative: if it
		// already holds this document, its copy is the newer one and the
		// archive's must not overwrite it.
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: d.id}}).
			SetUpdate(bson.D{{Key: "$setOnInsert", Value: d.raw}}).
			SetUpsert(true)
	}
	res, err := w.hot.Database(db).Collection(coll).
		BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, fmt.Errorf("warm %s: %w", ns, err)
	}

	// These documents are back in the fast tier, so their eviction notices are
	// now false. See meta.ClearEvictionNotices for what leaving them does.
	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = meta.DocID(ns, d.id)
	}
	if err := meta.ClearEvictionNotices(ctx, w.cold, ids); err != nil {
		w.log.Warn("warmed but could not clear eviction notices", "ns", ns, "err", err)
	}

	if err := w.recordResidency(ctx, ns, docs); err != nil {
		return int(res.UpsertedCount), err
	}
	w.log.Debug("warmed", "ns", ns, "inserted", res.UpsertedCount, "already_present", res.MatchedCount)
	return int(res.UpsertedCount), nil
}

// recordResidency protects warmed documents from the evictor for a while.
// Without it, a warm followed by an eviction sweep undoes itself.
func (w *Warmer) recordResidency(ctx context.Context, ns string, docs []sourceDoc) error {
	until := time.Now().UTC().Add(w.cfg.Residency.D())
	models := make([]mongo.WriteModel, len(docs))
	for i, d := range docs {
		docID := meta.DocID(ns, d.id)
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: docID}}).
			SetUpdate(bson.D{{Key: "$set", Value: bson.D{
				{Key: "ns", Value: ns},
				{Key: "doc", Value: d.id},
				{Key: "hotUntil", Value: until},
			}}}).
			SetUpsert(true)
	}
	_, err := w.cold.Database(meta.DB).Collection(meta.Residency).
		BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("record residency for %s: %w", ns, err)
	}
	return nil
}

// announce tells the feeder these inserts are coming, so it does not mirror
// them back into the archive they came from.
func (w *Warmer) announce(ctx context.Context, ns string, docs []sourceDoc) error {
	now := time.Now().UTC()
	models := make([]mongo.WriteModel, len(docs))
	for i, d := range docs {
		id := meta.DocID(ns, d.id)
		models[i] = mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: id}}).
			SetReplacement(meta.WarmingNotice{ID: id, At: now}).
			SetUpsert(true)
	}
	_, err := w.cold.Database(meta.DB).Collection(meta.Warming).
		BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("announce warming: %w", err)
	}
	return nil
}

// startHeartbeat keeps the warmer-active marker fresh and returns a stop func.
func (w *Warmer) startHeartbeat(ctx context.Context) func() {
	if w.dryRun {
		return func() {}
	}
	markers := w.cold.Database(meta.DB).Collection(meta.Markers)
	beat := func(c context.Context) {
		_, _ = markers.UpdateOne(c,
			bson.D{{Key: "_id", Value: meta.WarmerActiveMarker}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: time.Now().UTC()}}}},
			options.UpdateOne().SetUpsert(true))
	}
	beat(ctx)

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				beat(ctx)
			}
		}
	}()

	return func() {
		close(done)
		// The marker is left to go stale rather than deleted. When a warm
		// finishes, the feeder is still working through the inserts it just
		// produced, and clearing the marker now would let that tail be mirrored
		// back into the archive. Letting it age out keeps suppression in place
		// for the grace period the feeder needs to catch up.
	}
}

func (w *Warmer) waitForHealth(ctx context.Context) (bool, error) {
	// The warmer only adds to the fast tier, so mirroring lag is irrelevant to
	// its safety. Passing zero disables that check; the cluster limits still
	// apply, since a bulk copy can hurt the fast tier just as a bulk delete can.
	const pollEvery = 5 * time.Second
	var waited bool
	for {
		reading := w.check.Sample(ctx)
		reading.Missing = dropMissing(reading.Missing, "feeder lag")
		verdict := health.Evaluate(reading, w.cfg.Backoff, 0)
		if verdict.OK {
			if waited {
				w.log.Info("resuming warm")
			}
			return waited, nil
		}
		if !waited {
			w.log.Warn("warming paused", "reason", verdict.Reason)
		}
		waited = true
		if w.dryRun {
			return waited, nil
		}
		select {
		case <-ctx.Done():
			return waited, ctx.Err()
		case <-time.After(pollEvery):
		}
	}
}

func (w *Warmer) oldestOf(docs []sourceDoc, rule config.WarmRule) (time.Time, error) {
	last := docs[len(docs)-1]
	if rule.UsesObjectIDTime() {
		if last.id.Type != bson.TypeObjectID {
			return time.Time{}, fmt.Errorf("rule %q reads age from _id but found a %v _id: set date_field",
				rule.NS, last.id.Type)
		}
		return last.id.ObjectID().Timestamp().UTC(), nil
	}
	val, err := last.raw.LookupErr(rule.DateField)
	if err != nil {
		return time.Time{}, fmt.Errorf("rule %q: document has no %s field", rule.NS, rule.DateField)
	}
	t, ok := val.TimeOK()
	if !ok {
		return time.Time{}, fmt.Errorf("rule %q: %s is not a date", rule.NS, rule.DateField)
	}
	return t.UTC(), nil
}

// objectIDBound builds an _id cursor for a point in time.
func objectIDBound(at time.Time) bson.RawValue {
	oid := bson.NewObjectIDFromTimestamp(at)
	raw, err := bson.Marshal(bson.D{{Key: "_id", Value: oid}})
	if err != nil {
		return bson.RawValue{}
	}
	v, err := bson.Raw(raw).LookupErr("_id")
	if err != nil {
		return bson.RawValue{}
	}
	return v
}

func dropMissing(missing []string, name string) []string {
	out := missing[:0]
	for _, m := range missing {
		if m != name {
			out = append(out, m)
		}
	}
	return out
}

func splitNS(ns string) (db, coll string) {
	db, coll, _ = strings.Cut(ns, ".")
	return db, coll
}

func selectorName(rule config.WarmRule) string {
	if rule.UsesObjectIDTime() {
		return "_id timestamp"
	}
	return rule.DateField
}
