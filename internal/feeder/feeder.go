// Package feeder mirrors writes from the fast tier into the cold tier.
//
// It tails the fast tier's change stream and applies every insert, update and
// replace to the cold tier as an upsert. It never deletes and never drops. That
// single restriction is what makes the cold tier a complete archive rather than
// a second copy of whatever state the fast tier happens to be in, and it is why
// removing a document from the fast tier does not remove it from the archive.
package feeder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/watermark"
)

// ErrUnannouncedDelete is returned when policy is "halt" and a delete arrived
// that the evictor did not announce.
var ErrUnannouncedDelete = errors.New("unannounced delete on the fast tier")

// ErrResumePointLost is returned when the fast tier's oplog no longer reaches
// back to where mirroring got to.
//
// It stops the feeder, and that is the point. Reopening cannot help: the events
// between the checkpoint and the start of the oplog are gone, so every write the
// application made in that window is on the fast tier and not in the archive,
// and no amount of retrying will fetch them. Left to retry, the feeder logs a
// driver error once a second for ever while the archive receives nothing, which
// is the quietest possible way to have a hole in it.
//
// Stopping is louder. A daemon that exits is noticed by whatever runs it; a
// repeating log line is noticed by nobody.
var ErrResumePointLost = errors.New("the fast tier's oplog no longer reaches the mirroring checkpoint")

// Feeder mirrors one fast tier into one cold tier.
type Feeder struct {
	hot   *mongo.Client
	cold  *mongo.Client
	cfg   config.FeederConfig
	cp    *checkpoint.Store
	pos   *checkpoint.Positions
	marks *watermark.Store
	log   *slog.Logger

	// buffered work, flushed together
	pending []pendingOp
	// Where mirroring's time goes, since the rate it sustains is a ceiling on
	// how fast the fast tier can take writes before every fall-through read
	// waits. End-to-end timing on a loaded machine varied by 70% between runs
	// of the same config, which is more than any change worth making, so the
	// split is measured inside one run instead. See reportRate.
	pace mirrorPace

	// spentNotices are the eviction announcements this run has used and not yet
	// written off, held until the checkpoint covering those events is saved.
	//
	// Writing them off sooner breaks the idempotence the checkpoint cadence
	// rests on: the events come round again after a restart, nothing is left to
	// say those deletes were evictions, and the replay tombstones every one.
	// Held here, they also stop two deletes of one document inside a single
	// window both spending the same announcement, which is what the archive's
	// own count cannot see. See flushAndCheckpoint.
	spentNotices map[string]int
	// claimedInBatch are the announcements the batch being assembled has taken.
	// They join spentNotices only once that batch is in the archive, so an
	// attempt that failed leaves nothing behind for a retry to under-count
	// against.
	claimedInBatch map[string]int
	// lag of the most recent event applied
	lastEventAt time.Time
	// lastEventTS is the same position as a cluster time, kept whole.
	// lastEventAt is that instant with its sub-second half thrown away, which
	// is fine for reporting lag to a human and useless to a read deciding
	// whether it has to wait. See resumetoken.go for why this is the sound one.
	lastEventTS bson.Timestamp
	lastToken   bson.Raw
	lastSaved   time.Time
	// lastPublished paces the position write, which is far more frequent than
	// the checkpoint because a fallback read waits on it.
	lastPublished time.Time

	// warnedReserved remembers which namespaces have already been reported for
	// carrying Thermocline's reserved field, so one application doing it does
	// not fill the log.
	warnedReserved map[string]bool

	// cached view of whether a warmer is running, see echo.go
	warmActive    bool
	warmCheckedAt time.Time

	// The same pair for the router's promotions. See promotion echoes in
	// echo.go for what they are and why suppressing them has to be one-shot.
	promoteActive    bool
	promoteCheckedAt time.Time
}

// withdrawCompleteness drops the fast tier's completeness claim for ns.
//
// A failure here is logged rather than returned. The alternative is stopping
// mirroring over a bookkeeping write, and a feeder that keeps running is worth
// more than a watermark that is briefly wrong: the warmer checks the claim
// against reality as well, so this is the first of two defences, not the only
// one.
func (f *Feeder) withdrawCompleteness(ctx context.Context, ns string) {
	if f.marks == nil {
		// A Feeder assembled without an archive connection, which only happens
		// in tests that exercise event classification on its own.
		return
	}
	existed, err := f.marks.Clear(ctx, ns)
	if err != nil {
		f.log.Error("could not withdraw the completeness claim after a drop",
			"ns", ns, "err", err,
			"note", "the router may answer range queries from a fast tier that no longer holds them; "+
				"clear it with: cline watermark --ns "+ns+" --clear")
		return
	}
	if existed {
		f.log.Warn("withdrew the completeness claim, the fast tier no longer holds this namespace",
			"ns", ns, "note", "range queries fall through to the archive until the warmer refills it")
	}
}

// withdrawDatabase drops every completeness claim for one database.
//
// Logged rather than returned, for the same reason as withdrawCompleteness:
// mirroring is worth more than a bookkeeping write, and the warmer checks the
// claim against reality as a second defence.
func (f *Feeder) withdrawDatabase(ctx context.Context, db string) {
	if f.marks == nil || db == "" {
		return
	}
	n, err := f.marks.ClearDatabase(ctx, db)
	if err != nil {
		f.log.Error("could not withdraw the completeness claims after a database was dropped",
			"db", db, "err", err,
			"note", "the router may answer range queries from a fast tier that no longer holds them; "+
				"clear them with: cline watermark --ns <db.collection> --clear")
		return
	}
	if n > 0 {
		f.log.Warn("withdrew every completeness claim in a dropped database",
			"db", db, "claims", n,
			"note", "range queries fall through to the archive until the warmer refills it")
	}
}

// pendingOp is one decoded change waiting to be written.
type pendingOp struct {
	ns   string
	key  bson.Raw // documentKey, always {_id: ...}
	id   bson.RawValue
	doc  bson.Raw // full document, nil for deletes
	kind action

	// wasInsert records that the event was an insert rather than an update or a
	// replace. Both arrive here as an upsert, because that is how they are
	// mirrored, but only an insert can be the router copying a document up, and
	// only inserts may be suppressed as promotion echoes.
	wasInsert bool
}

// changeEvent is the subset of a change stream document the feeder reads.
type changeEvent struct {
	ID            bson.Raw `bson:"_id"`
	OperationType string   `bson:"operationType"`
	NS            struct {
		DB   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	DocumentKey  bson.Raw       `bson:"documentKey"`
	FullDocument bson.Raw       `bson:"fullDocument"`
	ClusterTime  bson.Timestamp `bson:"clusterTime"`
}

// New builds a Feeder. It does not contact either cluster.
func New(hot, cold *mongo.Client, cfg config.FeederConfig, log *slog.Logger) *Feeder {
	return &Feeder{
		hot:   hot,
		cold:  cold,
		cfg:   cfg,
		cp:    checkpoint.New(cold),
		pos:   checkpoint.NewPositions(cold),
		marks: watermark.New(cold),
		log:   log,
	}
}

// EnsureIndexes creates the bookkeeping indexes the feeder and evictor need.
// Safe to call on every start.
func (f *Feeder) EnsureIndexes(ctx context.Context) error {
	// Eviction notices expire on their own. They only have to outlive feeder
	// lag, and the config refuses a max_feeder_lag that reaches this far. See
	// meta.NoticeTTL, which is where the two are tied together.
	ttl := int32(meta.NoticeTTL.Seconds())
	_, err := f.cold.Database(meta.DB).Collection(meta.Evicting).Indexes().
		CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(ttl).SetName("tc_notice_ttl"),
		})
	if err != nil {
		return fmt.Errorf("create evicting ttl index: %w", err)
	}

	// Warming notices expire the same way, for the same reason.
	_, err = f.cold.Database(meta.DB).Collection(meta.Warming).Indexes().
		CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(ttl).SetName("tc_warm_ttl"),
		})
	if err != nil {
		return fmt.Errorf("create warming ttl index: %w", err)
	}
	return nil
}

// Run tails the fast tier until ctx is cancelled. It reopens the stream on
// recoverable errors, resuming from the last saved checkpoint.
func (f *Feeder) Run(ctx context.Context) error {
	if err := f.EnsureIndexes(ctx); err != nil {
		return err
	}
	// Before the first event, not after: a capped collection destroys archive
	// history as it rolls, and by the time that shows up the history is gone.
	if err := f.checkCollectionKinds(ctx); err != nil {
		return err
	}

	for {
		err := f.runOnce(ctx)
		switch {
		case err == nil, errors.Is(err, context.Canceled):
			return nil
		case errors.Is(err, ErrUnannouncedDelete), errors.Is(err, ErrResumePointLost):
			return err
		}

		f.log.Error("change stream ended, reopening", "err", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
}

// warnAboutUnmirrorableHead names namespaces whose documents the archive is
// never going to receive.
//
// A change stream reports changes from now, so anything already on the fast
// tier when the feeder first starts is not mirrored by starting it. Whether
// that matters depends entirely on which way round the deployment is, and the
// feeder can tell by looking:
//
// In an adoption the archive is the cluster being adopted and already holds
// everything, so there is nothing to say. In the other shape, a new archive
// beside a fast tier that already has data, the archive is missing those
// documents and no part of this system will fetch them: the warmer copies the
// archive *to* the fast tier, which is the opposite direction, and mirroring
// only carries what changes from here.
//
// This used to advise running the warmer, which cannot do it. Measured: ten
// documents on the fast tier, an empty archive, the feeder started and the
// warmer run exactly as the note said, and the archive still held none of them.
//
// Nothing is lost by it, because the evictor never removes a document the
// archive does not hold. What is lost is the premise: the archive is supposed
// to hold everything, and here it does not.
func (f *Feeder) warnAboutUnmirrorableHead(ctx context.Context) {
	var stranded []string
	for _, ns := range f.cfg.Namespaces {
		db, coll := splitNS(ns)
		if db == "" || coll == "" {
			continue
		}
		hot, herr := f.hot.Database(db).Collection(coll).CountDocuments(ctx, bson.D{}, options.Count().SetLimit(1))
		if herr != nil || hot == 0 {
			continue
		}
		cold, cerr := f.cold.Database(db).Collection(coll).CountDocuments(ctx, bson.D{}, options.Count().SetLimit(1))
		if cerr != nil || cold > 0 {
			continue
		}
		stranded = append(stranded, ns)
	}
	if len(stranded) == 0 {
		return
	}
	sort.Strings(stranded)
	f.log.Warn("the fast tier holds documents the archive has none of, and mirroring will not fetch them",
		"namespaces", stranded,
		"meaning", "a change stream reports changes from now, and the warmer copies the archive to the "+
			"fast tier, which is the other direction; nothing here reads the fast tier's history",
		"effect", "the archive is not the complete copy the rest of this system assumes, though nothing "+
			"is lost: the evictor never removes a document the archive does not hold",
		"action", "copy these collections into the archive before relying on it, then restart the feeder")
}

// runOnce opens a stream and pumps it until it fails or ctx ends.
func (f *Feeder) runOnce(ctx context.Context) error {
	token, err := f.cp.Load(ctx, f.cfg.ID)
	if err != nil {
		return err
	}

	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetMaxAwaitTime(f.cfg.MaxAwait)
	if len(token) > 0 {
		// StartAfter resumes past an invalidate, ResumeAfter does not.
		opts.SetStartAfter(token)
		f.log.Info("resuming from checkpoint", "id", f.cfg.ID)
	} else {
		f.log.Warn("no checkpoint, mirroring from now on",
			"id", f.cfg.ID,
			"note", "a change stream reports changes from now, so whatever the fast tier already "+
				"holds is not mirrored by starting this")
		f.warnAboutUnmirrorableHead(ctx)
	}

	stream, err := f.hot.Watch(ctx, f.pipeline(), opts)
	if err != nil {
		if len(token) > 0 && resumePointGone(err) {
			// The instant the checkpoint marks, so the log says how much is
			// missing rather than only that something is.
			at, _, _ := f.cp.EventAt(ctx, f.cfg.ID)
			f.log.Error("the fast tier's oplog no longer reaches where mirroring got to",
				"id", f.cfg.ID, "checkpoint", at.UTC().Format(time.RFC3339), "err", err,
				"meaning", "every write the application made between that checkpoint and the "+
					"start of the oplog is on the fast tier and not in the archive",
				"effect", "reopening cannot fetch them, so the feeder stops rather than run on "+
					"with a hole in the archive nothing would mention again",
				"action", "copy the affected namespaces from the fast tier into the archive, then "+
					"clear the checkpoint; clearing it on its own makes mirroring start from now "+
					"and hides the gap instead of closing it")
			return fmt.Errorf("%w: %v", ErrResumePointLost, err)
		}
		return fmt.Errorf("open change stream: %w", err)
	}
	defer func() { _ = stream.Close(context.WithoutCancel(ctx)) }()

	f.log.Info("mirroring", "namespaces", f.namespaceDescription())

	for {
		if ctx.Err() != nil {
			return f.flush(context.WithoutCancel(ctx))
		}

		// TryNext blocks up to MaxAwaitTime, so an idle stream still paces the
		// loop and flushes at least that often.
		if f.pace.since.IsZero() {
			f.pace.since = time.Now()
		}
		waitFrom := time.Now()
		got := stream.TryNext(ctx)
		f.pace.waiting += time.Since(waitFrom)
		if got {
			readFrom := time.Now()
			if err := f.handle(ctx, stream.Current); err != nil {
				return err
			}
			f.pace.reading += time.Since(readFrom)
			f.pace.events++
			f.lastToken = stream.ResumeToken()
			if len(f.pending) >= f.cfg.BatchSize {
				writeFrom := time.Now()
				if err := f.flushAndCheckpoint(ctx); err != nil {
					return err
				}
				f.pace.writing += time.Since(writeFrom)
			}
			continue
		}
		if err := stream.Err(); err != nil {
			// Flush what we have before giving the stream up, so a reopen does
			// not repeat work we already decoded.
			if ferr := f.flush(context.WithoutCancel(ctx)); ferr != nil {
				f.log.Error("flush before reopen failed", "err", ferr)
			}
			return err
		}

		// Idle. Everything the fast tier has written is in the archive, so say
		// so before checkpointing: a quiet database must not look like a feeder
		// that has fallen hours behind.
		//
		// The resume token has to be taken here as well. It otherwise only moves
		// when an event arrives, and the checkpoint write is skipped when there
		// is no token, so on a silent database nothing would be saved and the
		// stale timestamp would sit there growing. MongoDB advances the token
		// past an empty batch precisely so a reader can record progress it made
		// without seeing anything, which also means a long quiet spell does not
		// leave the feeder resuming from an ancient position.
		tok := stream.ResumeToken()
		if len(tok) > 0 {
			f.lastToken = tok
		}
		if err := f.flush(ctx); err != nil {
			return err
		}
		f.markCaughtUp(tok)
		if err := f.flushAndCheckpoint(ctx); err != nil {
			return err
		}
	}
}

// pipeline restricts the stream to the operations and namespaces we mirror.
// mirrorPace accumulates where one stretch of mirroring spent its time.
type mirrorPace struct {
	events  int
	waiting time.Duration // blocked on the change stream having something
	reading time.Duration // decoding events and buffering the work they imply
	writing time.Duration // putting that work in the archive, all of the below
	asking  time.Duration // asking the archive what to suppress, before writing
	bulk    time.Duration // the bulk writes themselves
	since   time.Time
}

// reportRate says how fast mirroring is going and what the time went on, once
// per checkpoint. Debug, because it is a question asked while tuning rather
// than something an idle deployment needs in its log every few seconds.
//
// waiting is the honest idle measure: a feeder that has caught up sits in
// TryNext, so a high share there means there was nothing to mirror rather than
// that mirroring is slow.
func (f *Feeder) reportRate() {
	if f.pace.events == 0 || f.pace.since.IsZero() {
		return
	}
	elapsed := time.Since(f.pace.since)
	if elapsed <= 0 {
		return
	}
	f.log.Debug("mirroring pace",
		"events", f.pace.events,
		"per_second", int(float64(f.pace.events)/elapsed.Seconds()),
		"waiting_ms", f.pace.waiting.Milliseconds(),
		"reading_ms", f.pace.reading.Milliseconds(),
		"writing_ms", f.pace.writing.Milliseconds(),
		"asking_ms", f.pace.asking.Milliseconds(),
		"bulk_ms", f.pace.bulk.Milliseconds(),
		"elapsed_ms", elapsed.Milliseconds())
	f.pace = mirrorPace{since: time.Now()}
}

func (f *Feeder) pipeline() mongo.Pipeline {
	match := bson.D{
		{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{
			// Mirrored.
			"insert", "update", "replace", "delete",
			// Never mirrored, but let through so they can be logged. A dropped
			// collection on the fast tier is exactly the moment an operator
			// needs to hear that the archive kept its copy.
			"drop", "dropDatabase", "rename",
		}}}},
		{Key: "ns.db", Value: bson.D{{Key: "$nin", Value: bson.A{
			"admin", "local", "config", meta.DB,
		}}}},
	}

	if len(f.cfg.Namespaces) > 0 {
		var want bson.A
		for _, ns := range f.cfg.Namespaces {
			db, coll := splitNS(ns)
			want = append(want, bson.D{
				{Key: "ns.db", Value: db},
				{Key: "ns.coll", Value: coll},
			})
		}
		match = append(match, bson.E{Key: "$or", Value: want})
	}

	return mongo.Pipeline{{{Key: "$match", Value: match}}}
}

// handle decodes one event and buffers the work it implies.
func (f *Feeder) handle(ctx context.Context, raw bson.Raw) error {
	var ev changeEvent
	if err := bson.Unmarshal(raw, &ev); err != nil {
		return fmt.Errorf("decode change event: %w", err)
	}
	if skipNamespace(ev.NS.DB) {
		return nil
	}
	f.noteEventTime(time.Unix(int64(ev.ClusterTime.T), 0).UTC())
	f.noteEventTS(ev.ClusterTime)

	ns := ev.NS.DB + "." + ev.NS.Coll

	// Classify before touching documentKey. DDL events do not carry one, and
	// looking it up first turns "somebody dropped a collection" into a
	// confusing warning about a missing _id.
	act, decided := classifyOp(ev.OperationType)
	if decided && act == actionSkip {
		f.log.Warn("ignoring operation that is never mirrored",
			"ns", ns, "op", ev.OperationType,
			"note", "the archive keeps its copy")
		// The archive keeps its copy, but the fast tier just lost the whole
		// collection, so any claim that it holds a range completely is now
		// false. Withdraw it: the archive still has the documents, and the
		// router must go there for them instead of answering from nothing.
		if ev.OperationType == "drop" {
			f.withdrawCompleteness(ctx, ns)
		}
		// And the same one level up. dropDatabase names no collection, so the
		// line above never fired for it and every claim in that database stayed
		// exactly as it was: the router went on answering bounded reads from a
		// fast tier holding nothing, and returning empty, while the archive
		// still held the documents. The twin of the case above, and the same
		// silent short answer it exists to prevent.
		if ev.OperationType == "dropDatabase" {
			f.withdrawDatabase(ctx, ev.NS.DB)
		}
		return nil
	}

	id, err := ev.DocumentKey.LookupErr("_id")
	if err != nil {
		f.log.Warn("event without _id, skipping", "ns", ns, "op", ev.OperationType)
		return nil
	}

	if !decided {
		// A delete. Whether it is an eviction is resolved in bulk at flush
		// time, because one lookup per delete would be far too slow while the
		// evictor is running.
		f.pending = append(f.pending, pendingOp{
			ns: ns, key: ev.DocumentKey, id: id, kind: actionDeletePending,
		})
		return nil
	}

	if len(ev.FullDocument) == 0 {
		// updateLookup found nothing, so the document was deleted between the
		// change and our read. The delete event is behind this one in the
		// stream and will be handled then.
		return nil
	}
	f.pending = append(f.pending, pendingOp{
		ns: ns, key: ev.DocumentKey, id: id, doc: ev.FullDocument, kind: actionUpsert,
		wasInsert: ev.OperationType == "insert",
	})
	return nil
}

// resumePointGone reports whether MongoDB refused the resume token, so
// mirroring cannot carry on from where it stopped.
//
// Two codes and several wordings. ChangeStreamHistoryLost is the specific one.
// ChangeStreamFatalError is what a server says when the token is simply not
// there any more: measured against PSMDB 8.0, "cannot resume stream; the resume
// token was not found". So the code narrows it to a fatal change stream error
// and the word resume in the message decides, rather than matching one phrasing
// that a release can change.
//
// A token from another cluster lands here too, and belongs here: it names a
// position this oplog has never had, mirroring cannot resume from it, and the
// operator has the same hole to think about either way.
func resumePointGone(err error) bool {
	var ce mongo.CommandError
	if !errors.As(err, &ce) {
		return false
	}
	if ce.Code == 286 || ce.Name == "ChangeStreamHistoryLost" {
		return true
	}
	if ce.Code == 280 || ce.Name == "ChangeStreamFatalError" {
		return strings.Contains(strings.ToLower(ce.Message), "resume")
	}
	return false
}
