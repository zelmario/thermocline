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

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/health"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/promote"
	"github.com/zelmario/thermocline/internal/watermark"
)

// Warmer copies documents from the archive into the fast tier.
type Warmer struct {
	hot  *mongo.Client
	cold *mongo.Client
	cfg  config.WarmerConfig
	// feederID and positions are how the warmer knows the archive has been
	// told everything the fast tier has done. See mirroredToNow.
	feederID  string
	positions *checkpoint.Positions
	check     *health.Checker
	log       *slog.Logger
	dryRun    bool

	// unannounced is the feeder's delete policy. Under "ignore" the archive
	// keeps deleted documents readable, which makes a count comparison between
	// the tiers meaningless.
	unannounced config.UnannouncedDeletes

	// total accumulates across runs, for the same reason the evictor keeps one:
	// a counter that resets every pass tells an alert nothing.
	total Stats

	marks   *watermark.Store
	limiter *limiter
	// indexer gives the fast tier the archive's indexes, so a tier this fills
	// can seek for the reads the archive was already seeking for. See
	// promote/indexes.go.
	indexer *promote.Indexer
}

// publish records what this warmer has done so far.
//
// The number worth alerting on is the time since the last run. A warmer that has
// stopped leaves the fast tier's completeness claim slowly going out of date,
// and nothing else in the system notices.
func (w *Warmer) publish(ctx context.Context, s Stats) {
	w.total.Scanned += s.Scanned
	w.total.Warmed += s.Warmed
	w.total.Present += s.Present
	w.total.Backoffs += s.Backoffs
	counters := map[string]int64{
		"scanned":         int64(w.total.Scanned),
		"warmed":          int64(w.total.Warmed),
		"already_present": int64(w.total.Present),
		"backoffs":        int64(w.total.Backoffs),
	}
	if err := meta.PublishDaemonReport(ctx, w.cold, "warmer", s.DryRun, counters, nil); err != nil {
		w.log.Warn("could not publish warmer statistics", "err", err)
	}
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
func New(hot, cold *mongo.Client, cfg config.WarmerConfig, unannounced config.UnannouncedDeletes, feederID string, dryRun bool, log *slog.Logger) *Warmer {
	return &Warmer{
		hot:         hot,
		cold:        cold,
		cfg:         cfg,
		feederID:    feederID,
		positions:   checkpoint.NewPositions(cold),
		unannounced: unannounced,
		check:       health.New(hot, cold, ""),
		log:         log,
		dryRun:      dryRun,
		marks:       watermark.New(cold),
		limiter:     newLimiter(cfg.Rate),
		indexer:     promote.NewIndexer(hot, cold, log),
	}
}

// Run walks every rule until each namespace is complete back to its target.
func (w *Warmer) Run(ctx context.Context) (Stats, error) {
	total := Stats{DryRun: w.dryRun}
	if w.dryRun {
		w.log.Info("dry run, nothing will be copied")
	}

	// No announcement and no wait. The feeder used to look for a warm's echoes
	// only while a heartbeat said a warm was running, so a warm announced
	// itself and then waited out the feeder's cache before copying anything.
	// The feeder asks on every flush that holds an insert now, which is what
	// makes a copy-up it did not know about come out right. See
	// Feeder.suppressPromotionEchoes.

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
	w.publish(ctx, total)
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

// stillHolds reports whether the fast tier holds the window that is about to be
// claimed, asked at the moment of claiming rather than at the start of the run.
//
// A claim describes the fast tier as it is now, not as it was when this warm
// began copying. An evictor working on the same namespace takes documents away
// while the warm is filling, and their windows overlap whenever the hot window
// reaches further back than the eviction cutoff.
//
// Measured with a warm reaching back fifteen years against an evictor removing
// anything over a day old, both on sample_training.trips: the warm finished its
// scan and claimed all of history, and by then the evictor had the fast tier
// down to 4,400 documents of the 10,000 the archive holds. A client counting the
// collection through the router was answered 4,400, as a success, because the
// claim said the fast tier held everything.
//
// Refusing to strengthen the claim leaves whatever the evictor last narrowed it
// to, which is the honest description of a tier being emptied. The next warm
// makes the claim again if it is true by then.
//
// What is left is the gap between this answer and the write that follows it, and
// it is worth saying why that is not closed with a compare and set on the
// watermark. The evictor narrows the claim, then waits settleClaim, then
// deletes: the window here would have to span that wait, which is
// watermark.ClaimCacheTTL and a second, for the fast tier to lose a document
// between this check passing and the claim being written. A compare and set
// would have to read the watermark immediately before writing it to be any
// tighter than that, and getting the expected value from the start of the run
// instead would refuse to claim on any namespace whose evictor sweeps during a
// warm, which is all of them. A warmer that never records a claim sends every
// read to the archive, so that cure is worse than this disease.
func (w *Warmer) stillHolds(ctx context.Context, rule config.WarmRule, from time.Time) bool {
	if w.dryRun {
		return true
	}
	// Under this policy the archive keeps deleted documents readable, so it
	// legitimately holds more than the fast tier and the comparison says
	// nothing. Same reasoning as verifyClaim.
	if w.unannounced == config.Ignore {
		return true
	}

	hotN, coldN, err := w.countBothInWindow(ctx, rule, from)
	if err != nil {
		// Not being able to check is not a reason to claim more than was
		// proved, and not a reason to fail the warm either.
		w.log.Warn("could not check the fast tier before recording a claim",
			"ns", rule.NS, "err", err, "effect", "leaving the claim as it was")
		return false
	}
	if hotN >= coldN {
		return true
	}
	// Two very different things bring the counts apart, and an operator sent
	// after the wrong one loses an afternoon. Documents can leave while the
	// warm fills, which is an evictor working the same namespace. Or the rule
	// may not be able to address them at all: a rule ages a collection by a
	// date field, and a document written before that field existed has none, so
	// no window ever selects it and no warm will ever copy it up.
	//
	// The second one never resolves. Saying "documents left the fast tier"
	// about it sends somebody looking for an evictor that is not there.
	if n := w.unagedInArchive(ctx, rule); n > 0 {
		w.log.Warn("not claiming a window, because the archive holds documents this rule cannot address",
			"ns", rule.NS,
			"fast_tier", hotN, "archive", coldN, "missing", coldN-hotN,
			"not_dated_by_"+rule.DateField, n,
			"why", "a rule ages a collection by a date field, and a document whose "+rule.DateField+
				" is missing, null or not a date is selected by no window, because MongoDB brackets "+
				"a range predicate by type; no warm will ever copy those up",
			"effect", "this claim is never going to be made, so queries with no lower time bound "+
				"go on being answered by the archive",
			"action", "give those documents the field, or tier this namespace by the _id timestamp "+
				"instead, or accept that it is read from the archive")
		return false
	}
	w.log.Warn("not claiming a window the fast tier no longer holds",
		"ns", rule.NS,
		"fast_tier", hotN, "archive", coldN, "missing", coldN-hotN,
		"why", "documents left the fast tier while this warm was filling it, which is what an "+
			"evictor whose cutoff falls inside the warm window does",
		"effect", "the claim is left where it was, so these reads fall through to the archive "+
			"and stay correct")
	return false
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

// progressEvery is how often a running warm says where it has got to. Slow
// enough to stay out of the way of a warm that finishes in seconds, often
// enough that one running for hours is visibly alive.
const progressEvery = 30 * time.Second

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

	// Before the documents, not after: an index built on an empty collection
	// costs nothing and one built on a full one costs a scan. See indexes.go
	// for why a warmed tier has none of its own.
	if !w.dryRun {
		w.indexer.Ensure(ctx, rule.NS)
	}

	// A rule with no date_field reads a document's age from the timestamp
	// inside its _id, and against any other _id type it addresses nothing at
	// all. Warming such a rule copies nothing, concludes the window is done and
	// records a claim over a fast tier it never filled, which is the one
	// mistake this component must not make. See meta.IDsAreNotObjectIDs.
	if rule.UsesObjectIDTime() {
		db, coll := splitNS(rule.NS)
		wrong, err := meta.IDsAreNotObjectIDs(ctx, w.cold, db, coll)
		if err != nil {
			return stats, err
		}
		if wrong {
			w.log.Error("not warming a namespace this rule cannot address",
				"ns", rule.NS,
				"reason", "the rule takes each document's age from the timestamp inside its _id, and this "+
					"collection's _id values are not ObjectIds",
				"effect", "the range this rule asks for matches no document, so warming it would copy "+
					"nothing and then claim the fast tier holds everything",
				"action", "set date_field on this rule to the field that carries the document's date")
			return stats, nil
		}
	}

	w.log.Info("warming",
		"ns", rule.NS,
		"back", rule.Back.String(),
		"from", start.Format(time.RFC3339),
		"to", floor.Format(time.RFC3339),
		"selector", selectorName(rule))
	w.warnIfTheArchiveCannotSortThis(ctx, rule)

	if !start.After(floor) {
		w.log.Info("already complete", "ns", rule.NS, "complete_from", start.Format(time.RFC3339))
		return stats, nil
	}

	lastReport := time.Now()

	// Resume by starting just past the last completed point, expressed in the
	// same quantity the rule tiers by. Turning a date watermark into an _id
	// bound is what made a widened window scan nothing and then claim it. See
	// scanCursor.
	before := scanCursor{}
	if saved.Claimed && saved.From.Before(now) {
		if rule.UsesObjectIDTime() {
			before.id = objectIDBound(saved.From)
		} else {
			before.at, before.atSet = saved.From, true
		}
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

		if !w.dryRun && !w.mirroredToNow(ctx) {
			w.log.Warn("not warming while mirroring is behind the fast tier",
				"ns", rule.NS,
				"waited", mirroredWait.String(),
				"why", "the archive may still hold as live a document the application has just deleted, "+
					"and copying it up would undo the delete",
				"effect", "this rule stops here for this run and resumes on the next one")
			return stats, nil
		}
		docs, cut, err := w.read(ctx, rule.NS, filter, scanSort(rule))
		if err != nil {
			return stats, err
		}
		if len(docs) == 0 {
			// Nothing older left inside the window, so the whole range is done.
			if err := w.claimComplete(ctx, rule, floor); err != nil {
				return stats, err
			}
			return stats, nil
		}
		stats.Scanned += len(docs)
		last := docs[len(docs)-1]
		before.id = last.id
		if !rule.UsesObjectIDTime() {
			at, aerr := w.oldestOf(docs, rule)
			if aerr != nil {
				return stats, aerr
			}
			before.at, before.atSet = at, true
		}

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
			if err := w.marks.Set(ctx, rule.NS, watermarkAfter(oldest), rule.DateField); err != nil {
				return stats, err
			}
		}

		w.limiter.spend(ctx, len(docs))

		// Progress, at a rate a person can read.
		//
		// The only per batch output was at debug, so a warm said "warming" and
		// then nothing until "warm complete". A first warm of a large adopted
		// collection runs for hours: measured, 184,521 documents took 672
		// seconds, so tens of millions is most of a day. Silence for that long
		// is indistinguishable from a stall, and the reasonable thing an
		// operator does about a stall is restart it, which throws away nothing
		// but starts the whole scan again.
		//
		// How far back it has reached matters more than the count, because the
		// window is a span of time and that is what says how much is left.
		if !w.dryRun && time.Since(lastReport) >= progressEvery {
			lastReport = time.Now()
			w.log.Info("warming",
				"ns", rule.NS,
				"warmed", stats.Warmed,
				"already_present", stats.Present,
				"reached_back_to", oldest.UTC().Format(time.RFC3339),
				"target", floor.UTC().Format(time.RFC3339))
		}

		// Short and not cut for size: the range has run out. See read.
		if len(docs) < w.cfg.BatchSize && !cut {
			if !w.dryRun {
				if err := w.claimComplete(ctx, rule, floor); err != nil {
					return stats, err
				}
			} else {
				w.log.Info("complete", "ns", rule.NS, "complete_from", floor.Format(time.RFC3339))
			}
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
// warmBatchBytes bounds how much document data one batch holds. See read.
const warmBatchBytes = 32 << 20

// read fetches one batch from the archive, and reports whether it stopped
// early because the batch reached warmBatchBytes rather than because the range
// ran out.
//
// A batch was batch_size documents whatever their size, and all of it was held
// at once. Measured on 2026-09-26: warming 60 documents of 12MB peaked at
// 1,874MB, and the default batch of 200 near the 16MB limit is several times
// that. Stopping early is only safe because the caller is told: the range is
// exhausted when a batch comes back short, and a batch cut short by bytes is
// not the end of anything. Read as the end, it would claim the fast tier
// complete over documents never copied.
func (w *Warmer) read(ctx context.Context, ns string, filter bson.D, sort bson.D) ([]sourceDoc, bool, error) {
	db, coll := splitNS(ns)
	cur, err := w.cold.Database(db).Collection(coll).Find(ctx, filter,
		options.Find().
			SetSort(sort).
			SetLimit(int64(w.cfg.BatchSize)))
	if err != nil {
		return nil, false, fmt.Errorf("read archive %s: %w", ns, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var docs []sourceDoc
	held := 0
	for cur.Next(ctx) {
		raw := append(bson.Raw(nil), cur.Current...)
		id, err := raw.LookupErr("_id")
		if err != nil {
			return nil, false, fmt.Errorf("read archive %s: document without _id", ns)
		}
		docs = append(docs, sourceDoc{
			id:  bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)},
			raw: raw,
		})
		held += len(raw)
		if held >= warmBatchBytes && len(docs) < w.cfg.BatchSize {
			return docs, true, nil
		}
	}
	if err := cur.Err(); err != nil {
		return nil, false, fmt.Errorf("read archive %s: %w", ns, err)
	}
	return docs, false, nil
}

// write copies a batch into the fast tier and records residency for it.
// It returns how many documents were actually inserted.
func (w *Warmer) write(ctx context.Context, ns string, docs []sourceDoc) (int, error) {
	if w.dryRun {
		w.log.Info("would warm", "ns", ns, "count", len(docs))
		return len(docs), nil
	}

	db, coll := splitNS(ns)
	hot := w.hot.Database(db).Collection(coll)

	// The documents the fast tier already holds are held there and nothing more:
	// no notice, no promotion recorded, no insert. See promote.Held for what
	// each of those did to a document the warm never copied. A fast tier that
	// cannot be asked leaves the batch whole, which is how this ran before.
	ids := make([]bson.RawValue, len(docs))
	for i, d := range docs {
		ids[i] = d.id
	}
	//
	// Held, and then asked whether an eviction already under way takes them
	// anyway, because the claim written after this batch describes them. A
	// sweep that read the holds before this one was written deletes the
	// document, and the claim then covers a document the fast tier does not
	// hold. See promote.HoldPresent.
	asked := false
	if have, left, holdErr, herr := promote.HoldPresent(ctx, hot, w.cold, ns, ids, w.cfg.Residency.D()); herr != nil {
		w.log.Debug("could not ask the fast tier which documents it holds, warming all of them",
			"ns", ns, "err", herr)
	} else {
		asked = true
		if holdErr != nil {
			w.log.Warn("could not hold documents the fast tier already has against eviction",
				"ns", ns, "err", holdErr)
		}
		if len(left) > 0 {
			want := make([]bson.RawValue, len(left))
			for j, i := range left {
				want[j] = ids[i]
			}
			found, rerr := promote.RereadArchived(ctx, w.cold.Database(db).Collection(coll), want)
			if rerr != nil {
				return 0, fmt.Errorf("read again the documents an eviction took during a warm: %w", rerr)
			}
			for _, i := range left {
				if d, ok := found[promote.RawKey(ids[i])]; ok {
					docs[i].raw = d
				} else {
					// Deleted by the application since the read: nothing to warm.
					have[i] = true
				}
			}
		}
		absent := make([]sourceDoc, 0, len(docs))
		for i, d := range docs {
			if !have[i] {
				absent = append(absent, d)
			}
		}
		docs = absent
		if len(docs) == 0 {
			return 0, nil
		}
	}

	// Announce first, for the same reason the evictor does: the feeder must be
	// able to recognise the resulting inserts before it sees them.
	if err := w.announce(ctx, ns, docs); err != nil {
		return 0, err
	}

	// An insert, never a replace. The fast tier is authoritative: if it already
	// holds this document, its copy is the newer one and the archive's must not
	// overwrite it, which a duplicate key achieves without rewriting the
	// document the way $setOnInsert did. See package promote.
	raw := make([]bson.Raw, len(docs))
	for i, d := range docs {
		raw[i] = d.raw
	}
	// Residency is recorded before the write, not after.
	//
	// These writes are what a client's change stream sees, and it sees them
	// immediately. The router hides a warmed document's insert by recognising
	// it as a promotion, and it recognises it by the residency record, so
	// recording afterwards leaves a window in which the application is told a
	// years-old document was just created. Recording first for a warm that then
	// fails costs nothing: residency for a document that is not there protects
	// nothing.
	if err := w.recordResidency(ctx, ns, docs, asked); err != nil {
		return 0, err
	}

	inserted, skipped, err := promote.Many(ctx, hot, raw)
	if err != nil {
		return 0, fmt.Errorf("warm %s: %w", ns, err)
	}

	// The documents the fast tier already held produce no insert, so the echo
	// announced for each of them has to be taken back.
	//
	// The skipped positions used to be discarded here, on the grounds that they
	// matter only to callers that announce an echo per document and the warmer
	// announced through _thermocline.warming instead. That stopped being true
	// the moment the residency record started carrying an echo so that a warm
	// could be told from an application's insert. A notice left standing is
	// spent on whatever write reaches that document next and that mirror is
	// dropped, so the archive keeps the older version with nothing saying so --
	// which is the one outcome this system exists to prevent, arrived at by
	// fixing something else.
	if len(skipped) > 0 {
		stale := make([]string, 0, len(skipped))
		for _, i := range skipped {
			if i >= 0 && i < len(docs) {
				stale = append(stale, meta.DocID(ns, docs[i].id))
			}
		}
		if werr := meta.WithdrawEchoes(ctx, w.cold, stale); werr != nil {
			w.log.Warn("could not withdraw the echoes for documents the fast tier already held",
				"ns", ns, "count", len(stale), "err", werr,
				"effect", "one later write to each of these may not reach the archive")
		}
	}

	// The eviction notices for these documents are deliberately left alone.
	//
	// Clearing them looks right, because a document back in the fast tier is not
	// leaving it. It is right about the next eviction and wrong about the one
	// already in flight: if the feeder has not applied that delete yet, the
	// notice it needs is gone by the time it looks, and it tombstones a document
	// nothing deleted. The warm's own insert does not repair it either, because
	// the feeder suppresses that as an echo of this copy-up.
	//
	// The feeder consumes a notice as it applies the delete it describes, so one
	// cannot outlive its eviction. See meta.AnnounceEviction.

	w.log.Debug("warmed", "ns", ns, "inserted", inserted, "already_present", len(raw)-inserted)
	return inserted, nil
}

// recordResidency protects warmed documents from the evictor for a while, and
// records that this system put them there.
//
// Without the hold, a warm followed by an eviction sweep undoes itself.
//
// Without the rest of the row, a warmed document reached the application's
// change stream as an insert. The router recognises a copy-up by the residency
// record the copy-up wrote, and this wrote a hold and nothing else -- no
// promotedAt, no echo -- so there was nothing to recognise. Every document the
// warmer moved was announced to any client watching as a brand new insert of a
// record the archive may have held for years, and the warmer's whole job is
// moving documents in bulk. meta.ResidencyUpdate writes what the router's own
// copy-up writes, so both are recognised the same way.
//
// asked says the fast tier was asked and held none of docs, so each is a
// promotion for certain. See meta.PromotionUpdate.
func (w *Warmer) recordResidency(ctx context.Context, ns string, docs []sourceDoc, asked bool) error {
	now := time.Now().UTC()
	update := meta.ResidencyUpdate
	if asked {
		update = meta.PromotionUpdate
	}
	models := make([]mongo.WriteModel, len(docs))
	for i, d := range docs {
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: meta.DocID(ns, d.id)}}).
			SetUpdate(update(ns, d.id, now, w.cfg.Residency.D())).
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
	models := make([]mongo.WriteModel, len(docs))
	for i, d := range docs {
		id := meta.DocID(ns, d.id)
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: id}}).
			// An update rather than a replacement, so the instant can come from
			// the archive rather than from here. The row holds nothing else, so
			// this writes exactly what the replacement did.
			//
			// It is read twice by another process: by the archive's TTL monitor,
			// which decides when the row goes, and by the feeder, which asks
			// whether a warm was announced recently enough to be this insert's.
			// Both of those are somewhere else, and the archive's clock is the
			// one they share with this. See meta.AnnounceEviction.
			SetUpdate(bson.D{{Key: "$currentDate", Value: bson.D{{Key: "at", Value: true}}}}).
			SetUpsert(true)
	}
	_, err := w.cold.Database(meta.DB).Collection(meta.Warming).
		BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("announce warming: %w", err)
	}
	return nil
}

func (w *Warmer) waitForHealth(ctx context.Context) (bool, error) {
	// Mirroring lag is not judged here as a health signal, so zero disables
	// that check. It is judged per batch instead, and as a question of
	// correctness rather than load: see mirroredToNow, which exists because
	// "the warmer only adds to the fast tier, so lag is irrelevant" was wrong.
	// The cluster limits still apply, since a bulk copy can hurt the fast tier
	// just as a bulk delete can.
	const pollEvery = 5 * time.Second
	var waited bool
	for {
		reading := w.check.Sample(ctx)
		reading.Missing = dropMissing(reading.Missing, "feeder lag")
		verdict := health.Evaluate(reading, w.cfg.Backoff, 0)
		// Free disk is the warmer's alone. It fills the fast tier, which is
		// smaller than the archive by design, and a warm window is bounded by
		// time rather than by size: a window chosen from the age of the data
		// instead of the size of the tier fills the disk. On the first day of
		// an adoption the evictor cannot make room either, because a new
		// replica set's oplog window is its own age. The evictor is not held
		// back on this signal, because it is what frees the space.
		if verdict.OK && w.cfg.MinFreeDisk > 0 && !reading.IsMissingSignal("free disk") &&
			reading.FreeDisk < w.cfg.MinFreeDisk {
			verdict = health.Verdict{Reason: fmt.Sprintf(
				"the fast tier's filesystem is %.1f%% free, limit %.1f%%",
				reading.FreeDisk*100, w.cfg.MinFreeDisk*100)}
		}
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

// claimComplete records what the fast tier can now answer, which is not always
// the window that was warmed.
//
// A watermark says "complete for everything newer than T". A query carrying no
// lower time bound reaches arbitrarily far back, so no such watermark can cover
// it and every one of those reads falls through to the archive, however full
// the fast tier is. Only a claim on all of history covers them: see
// watermark.Mark.CoversAll, which has said so all along.
//
// That is the whole of an adoption. The warm window is chosen to cover the data
// the collection has, the warm copies every document up, and the claim it
// leaves still refuses the shape of query an application asks most: give me
// this customer's orders. Measured on a 50,000 order shop whose entire orders
// collection had just been warmed into the fast tier: 41% of the run still
// answered from the archive, 284 operations per second, order history 62.74ms
// at p50. Declaring the same tier complete for all of history, changing nothing
// else, gave 100% from the fast tier, 1,384 per second, and 2.65ms. Nobody
// would have looked, because the fast tier held every document and the warmer
// had reported success.
//
// So when the archive holds nothing older than the window, the fast tier holds
// every document there is and this says so. When it does hold something older,
// the boundary claim is the honest one and the cost of it is named, because an
// operator reading "complete" has no other way to learn that unbounded queries
// are still going to the archive.
func (w *Warmer) claimComplete(ctx context.Context, rule config.WarmRule, floor time.Time) error {
	older, err := olderThanFilter(rule, floor)
	if err != nil {
		return err
	}
	db, coll := splitNS(rule.NS)
	left, err := w.cold.Database(db).Collection(coll).CountDocuments(ctx, older, options.Count().SetLimit(1))
	if err != nil {
		// Not fatal, and not a reason to claim more than was proved. The
		// boundary claim is always sound; only the stronger one needs proof.
		w.log.Warn("could not tell whether the archive holds anything older than the warm",
			"ns", rule.NS, "err", err, "effect", "claiming the window only, which is the safe direction")
		left = 1
	}

	if left == 0 {
		if !w.stillHolds(ctx, rule, time.Time{}) {
			return nil
		}
		if err := w.marks.Set(ctx, rule.NS, time.Time{}, rule.DateField); err != nil {
			return err
		}
		w.log.Info("complete for all of history", "ns", rule.NS,
			"why", "the archive holds no live document older than this warm, so the fast tier holds every one",
			"effect", "queries with no lower time bound are answered by the fast tier instead of the archive")
		return nil
	}

	if !w.stillHolds(ctx, rule, floor) {
		return nil
	}
	if err := w.marks.Set(ctx, rule.NS, floor, rule.DateField); err != nil {
		return err
	}
	w.log.Info("complete", "ns", rule.NS, "complete_from", floor.Format(time.RFC3339),
		"note", "the archive still holds documents older than this, so a query with no lower time bound "+
			"cannot be answered by the fast tier and falls through",
		"action", "warm further back, or declare it with: cline watermark --ns "+rule.NS+" --complete-all "+
			"if the fast tier already holds every document")
	return nil
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

// unagedInArchive counts the live documents the archive holds that this rule
// cannot read an age from.
//
// Not just a missing field. MongoDB brackets a range predicate by type, so a
// window on a date selects only documents whose field holds a date: a string
// date, a null, and an absent field are all equally invisible to it, and
// equally never warmed. Measured on four documents, one of each shape:
// {$exists: false} found one of the three that no window selects, and
// {$not: {$type: "date"}} found all three.
//
// Only ever used to explain a refusal, so a rule that reads age from the _id has
// nothing to report: every document has one. An error is reported as none,
// because failing to explain a warning is not worth failing a warm over.
func (w *Warmer) unagedInArchive(ctx context.Context, rule config.WarmRule) int64 {
	if rule.UsesObjectIDTime() {
		return 0
	}
	db, coll := splitNS(rule.NS)
	n, err := w.cold.Database(db).Collection(coll).CountDocuments(ctx, bson.D{
		meta.NotArchivedDeleted(),
		{Key: rule.DateField, Value: bson.D{
			{Key: "$not", Value: bson.D{{Key: "$type", Value: "date"}}},
		}},
	})
	if err != nil {
		return 0
	}
	return n
}

// warnIfTheArchiveCannotSortThis says when a rule's walk of the archive has no
// index to walk.
//
// The warm reads the archive newest first, and for a rule that ages by a date
// field the sort is {field: -1, _id: -1}. Nothing copies an index to the
// archive, so unless somebody built one there the sort has nothing to use, and
// mongod answers every batch by reading the whole collection and sorting it.
//
// Per batch, not per pass, which is what makes it quadratic: measured on 200,003
// archived documents with batch_size 200, one warm pass sent a thousand finds
// and the profiled ones each examined 200,000 documents. The archive spent
// 16.75 seconds inside the 255 that fitted in a capped system.profile.
//
// The obvious remedy is the wrong one and that is why this names the index. An
// index on the field alone still leaves a blocking sort, because the tiebreak
// on _id is not in it: the same pass then examined 19,039 documents per batch
// instead of 200,000, which is better and still wrong. With {field: -1, _id: -1}
// there is no sort stage, 100 documents are examined per batch, and the archive
// stops being the thing the warm waits for: 36ms in total against 16,750.
//
// Said once per rule per pass, which is a listIndexes against a daemon that is
// about to read the whole window anyway.
func (w *Warmer) warnIfTheArchiveCannotSortThis(ctx context.Context, rule config.WarmRule) {
	if rule.UsesObjectIDTime() {
		// The sort is on _id alone, which every collection indexes.
		return
	}
	db, coll := splitNS(rule.NS)
	cur, err := w.cold.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		// The archive not answering is this pass's problem in louder ways.
		w.log.Debug("could not list the archive's indexes for this rule", "ns", rule.NS, "err", err)
		return
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	for cur.Next(ctx) {
		key, ok := cur.Current.Lookup("key").DocumentOK()
		if !ok {
			continue
		}
		if indexServesNewestFirst(key, rule.DateField) {
			return
		}
	}
	w.log.Warn("the archive has no index for the order this warm reads it in",
		"ns", rule.NS, "date_field", rule.DateField,
		"meaning", "every batch is answered by reading the whole collection and sorting it, so the "+
			"cost of one pass grows with the square of the collection rather than with the "+
			"window being warmed",
		"measured", "200,000 documents examined per batch at 200,003 archived, against 100 with "+
			"the index below; and an index on the field alone still sorts, at 19,039",
		"action", "build {"+rule.DateField+": -1, _id: -1} on the archive, which is the order this "+
			"reads in; the field on its own is not enough because the tiebreak on _id is not in it")
}

// indexServesNewestFirst reports whether an index can answer the warm's sort
// without a sorting stage.
//
// The sort is {field: -1, _id: -1}. An index answers it when those are its first
// two fields in that order and its directions are either the same as the sort's
// or the exact opposite, because mongod can walk an index backwards but cannot
// mix directions.
func indexServesNewestFirst(key bson.Raw, field string) bool {
	elems, err := key.Elements()
	if err != nil || len(elems) < 2 {
		return false
	}
	if elems[0].Key() != field || elems[1].Key() != "_id" {
		return false
	}
	a, aok := numericDirection(elems[0].Value())
	b, bok := numericDirection(elems[1].Value())
	if !aok || !bok {
		// A text or geospatial key, which is not an order at all.
		return false
	}
	return a == b
}

// numericDirection reads an index key's direction, whatever numeric type mongod
// stored it as.
func numericDirection(v bson.RawValue) (int, bool) {
	switch v.Type {
	case bson.TypeInt32:
		return sign(int(v.Int32())), true
	case bson.TypeInt64:
		return sign(int(v.Int64())), true
	case bson.TypeDouble:
		return sign(int(v.Double())), true
	}
	return 0, false
}

func sign(n int) int {
	if n < 0 {
		return -1
	}
	return 1
}

// mirroredWait bounds how long one batch waits for mirroring. See mirroredToNow.
const mirroredWait = 30 * time.Second

// mirroredToNow waits until the feeder has mirrored everything the fast tier
// had written when this was called, and reports whether it got there.
//
// The warmer copies archived documents the fast tier does not hold, and a
// document the application deleted a moment ago is exactly that: gone from the
// fast tier at once, and live in the archive until its tombstone lands. A
// batch read in between copies it back and the delete is undone. This used to
// be waived on the grounds that "the warmer only adds to the fast tier, so
// mirroring lag is irrelevant to its safety", and adding is the harm.
// Measured on 2026-09-25 with the feeder held back: 25 documents deleted
// through the router, one warm, and the warm copied all 25 back; the router
// then answered 25 of 25 documents the application had deleted, where MongoDB
// answers none. With this, the warm waits, says why, and copies none.
//
// The fast tier's own last write is the bar, read here rather than assumed,
// and the feeder's published cluster time has to reach it. Once per batch, so
// a long warm is never working from a view older than one batch.
func (w *Warmer) mirroredToNow(ctx context.Context) bool {
	if w.feederID == "" {
		return true
	}
	var h struct {
		LastWrite struct {
			OpTime struct {
				TS bson.Timestamp `bson:"ts"`
			} `bson:"opTime"`
		} `bson:"lastWrite"`
	}
	if err := w.hot.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&h); err != nil {
		return false
	}
	want := checkpoint.PackTS(h.LastWrite.OpTime.TS)
	deadline := time.Now().Add(mirroredWait)
	for {
		rec, ok, err := w.positions.At(ctx, w.feederID)
		if err == nil && ok && checkpoint.PackTS(rec.TS) >= want {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}
