// Package feeder mirrors writes from the fast tier into the cold tier.
//
// It tails the fast tier's change stream and applies every insert, update and
// replace to the cold tier as an upsert. It never issues a delete or a drop
// against the cold tier: an application's delete is mirrored as a mark on the
// archived copy, _tcDeletedAt, and the router filters marked documents out of
// every read, so the client sees the document gone while the archive keeps it.
//
// The restriction is on this component, not on the application. Deleting works
// exactly as it always did. What it buys is that removing a document from the
// fast tier does not remove it from the archive, which is what makes the cold
// tier a complete archive rather than a second copy of whatever state the fast
// tier happens to be in, and what stops one accident there destroying both.
package feeder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/lease"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/watermark"
)

// ErrUnannouncedDelete is returned when policy is "halt" and a delete arrived
// that the evictor did not announce.
var ErrUnannouncedDelete = errors.New("unannounced delete on the fast tier")

// ErrUnmirrorableKind is returned when a collection this feeder is mirroring
// turns out to be a kind that mirroring damages. See checkNamespaceKind.
var ErrUnmirrorableKind = errors.New("a namespace this feeder mirrors cannot be mirrored")

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
	// hold is what stops a second feeder with this id running beside this one.
	// Two of them read every delete twice, and an eviction is announced once:
	// see internal/lease.
	hold   *lease.Lease
	heldAt time.Time
	log    *slog.Logger

	// buffered work, flushed together
	pending []pendingOp

	// How far this host's clock is ahead of the archive's, as last measured,
	// and when that was. Published with the position: see clockSkew.
	skew      time.Duration
	skewAt    time.Time
	skewKnown bool
	clockOnce sync.Once

	// kindSeen is the namespaces whose collection kind has been checked at
	// least once since this feeder started. Read and written only from the
	// stream goroutine. See checkNamespaceKind.
	kindSeen map[string]bool
	// kindRechecked is when that answer was last thrown away and asked again,
	// per namespace, which happens when deletes arrive that nothing announced.
	// Same goroutine. See recheckKindOnUnannounced.
	kindRechecked map[string]time.Time

	// saidWritesBlocked stops the line about blocked writes repeating every
	// second while the block is held. Cleared when a write actually lands,
	// which is the only honest sign the block is gone: the reopen loop logs a
	// resume on every attempt, including the ones about to fail again.
	saidWritesBlocked bool

	// unmirroredNS is the namespaces this feeder has given up a document in,
	// loaded from the archive at startup so a restart does not forget. Same
	// goroutine as everything else here.
	//
	// It exists so that clearing costs nothing in the ordinary case: with no
	// such document anywhere, which is every healthy deployment, no batch asks
	// the archive about one.
	unmirroredNS map[string]bool

	// counters are what this feeder has done since it started, published into
	// the archive so the router can export them. The evictor and the warmer
	// have done this since 2026-09-03 and the feeder had not, which left the
	// one daemon whose failures stop mirroring reporting them only to a log
	// line. See publishCounters.
	counters feederCounters
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
	spentNotices map[string][]bson.Timestamp
	// claimedInBatch are the announcements the batch being assembled has taken.
	// They join spentNotices only once that batch is in the archive, so an
	// attempt that failed leaves nothing behind for a retry to under-count
	// against.
	claimedInBatch map[string][]bson.Timestamp
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
	// pendingSince is when the oldest event in pending was buffered, and zero
	// while pending is empty. See dueToFlush.
	pendingSince time.Time

	// warnedReserved remembers which namespaces have already been reported for
	// carrying Thermocline's reserved field, so one application doing it does
	// not fill the log.
	warnedReserved map[string]bool

	// ttlSettled remembers which archive collections have had the tombstone TTL
	// index put in the state the config asks for. One namespace is one round
	// trip per feeder run, and only namespaces that actually tombstone.
	ttlSettled map[string]bool

	// noTxn is set once the archive has refused a transaction for not being a
	// replica set, and flushes are written one namespace at a time from then on.
	// See writeFlush.
	noTxn bool
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

// hasUnmirrored reports whether this feeder has given up a document in this
// namespace.
func (f *Feeder) hasUnmirrored(ns string) bool { return f.unmirroredNS[ns] }

// noteUnmirrored remembers that it has.
func (f *Feeder) noteUnmirrored(ns string) {
	if f.unmirroredNS == nil {
		f.unmirroredNS = map[string]bool{}
	}
	f.unmirroredNS[ns] = true
}

// forgetUnmirrored is called when every row for a namespace has been cleared.
func (f *Feeder) forgetUnmirrored(ns string) { delete(f.unmirroredNS, ns) }

// loadUnmirrored reads the rows written by whatever ran before this feeder.
//
// Without it a restarted feeder would never clear a row it did not write
// itself, and the namespace would stay refused after the duplicate was
// resolved. A failure here is a warning rather than a stop: the rows still do
// their work in the router, and the only cost is that clearing waits for the
// next feeder that can read them.
func (f *Feeder) loadUnmirrored(ctx context.Context) {
	byNS, err := meta.UnmirroredNamespaces(ctx, f.cold)
	if err != nil {
		f.log.Warn("could not read which documents the archive has refused",
			"err", err,
			"effect", "reads of those namespaces stay refused until a feeder can read these rows")
		return
	}
	for ns, n := range byNS {
		f.noteUnmirrored(ns)
		f.log.Warn("the archive holds none of a document the fast tier does, from before this feeder started",
			"ns", ns, "documents", n,
			"effect", "reads of this namespace that fall through to the archive are refused, "+
				"because the archive is not a superset of the fast tier while this stands",
			"action", "resolve the duplicate value, then write the document again; cline preflight "+
				"lists them with the index and the colliding key")
	}
}

// pendingOp is one decoded change waiting to be written.
// feederCounters is what a feeder has done since it started. Cumulative, which
// is what a Prometheus counter wants, and touched only from the stream
// goroutine.
type feederCounters struct {
	// mirrored and tombstoned are the two things mirroring does.
	mirrored   int64
	tombstoned int64
	// unmirrorable counts documents the archive refused and this feeder gave
	// up on, which it does rather than retrying for ever. Each one is a
	// document the archive will never hold while the fast tier does, and it was
	// an ERROR in a log and nothing else. See writeBatch.
	unmirrorable int64
	// insertsOverExisting counts inserts mirrored onto an _id the archive
	// already held. Through the router an insert for an _id the archive holds
	// live is refused as a duplicate key, so this is either a write that did
	// not come through the router or an _id the application deleted and used
	// again. Measured on a shop run of 348 batches with copy-ups and evictions
	// underneath: zero. Measured on one write made straight at the fast tier's
	// mongod: one. Measured on a delete and re-insert of the same _id through
	// the router: also one, which is why this is a number to look at rather
	// than a warning to act on.
	insertsOverExisting int64
}

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

	// at is where this event sits in the fast tier's oplog.
	//
	// Carried for deletes, so that an eviction announcement spent on one can say
	// which one: the router reads the same position off the same event and can
	// then tell the evictor's delete from the application's delete of the same
	// document. And for inserts, for the mirror image of that: a residency
	// record says which inserts were its own copy-ups, so a promotion read late
	// is still recognised after the document has been evicted and promoted
	// again. See meta.ResidencyRecord.Echoed.
	at bson.Timestamp
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
		hold:  lease.New(cold, "feeder", cfg.ID),
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

	// And the record of this system's own DDL on the fast tier, which a
	// client's change stream is judged against. The same hour, because it
	// answers the same question for the same late reader: a consumer resuming
	// from a stored token needs the row to still be there. See meta.OwnDDL.
	_, err = f.cold.Database(meta.DB).Collection(meta.OwnDDL).Indexes().
		CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(ttl).SetName("tc_ownddl_ttl"),
		})
	if err != nil {
		return fmt.Errorf("create own ddl ttl index: %w", err)
	}

	// Residency rows expire at an instant they carry themselves, so the TTL is
	// zero seconds past that instant rather than a duration of its own.
	//
	// On expiresAt and not on hotUntil, and that is a fix rather than a
	// rename. hotUntil is when the hold is over; expiresAt is when the row may
	// go, an hour later. While they were one field the row was destroyed as
	// soon as the hold ended, which took with it the only evidence that a
	// copy-up's insert was a promotion, and a change stream consumer reading
	// after that was shown inserts its application never made. See
	// meta.ResidencyRecord.ExpiresAt for the measurement.
	//
	// Rows written before the field existed carry no expiresAt and the new
	// index would never remove them, so they are given one first, derived from
	// the hold they already carry. Done before the index is created and before
	// the old one is dropped, so there is no moment at which a row is immortal.
	if _, err := f.cold.Database(meta.DB).Collection(meta.Residency).UpdateMany(ctx,
		bson.D{{Key: "expiresAt", Value: bson.D{{Key: "$exists", Value: false}}}},
		mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{
			{Key: "expiresAt", Value: bson.D{{Key: "$add", Value: bson.A{
				"$hotUntil", meta.NoticeTTL.Milliseconds(),
			}}}},
		}}}}); err != nil {
		f.log.Warn("could not carry residency rows onto the new expiry field",
			"err", err,
			"note", "rows written before this version keep expiring on hotUntil until the "+
				"old index is gone, and are removed by hand or by the next start")
	}

	if _, err := f.cold.Database(meta.DB).Collection(meta.Residency).Indexes().
		CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "expiresAt", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0).SetName("tc_residency_expiry"),
		}); err != nil {
		f.log.Warn("could not create the residency expiry index",
			"err", err,
			"note", "residency rows are never cleared, so "+meta.DB+"."+meta.Residency+
				" grows slowly; mirroring is unaffected")
	} else if err := f.cold.Database(meta.DB).Collection(meta.Residency).Indexes().
		DropOne(ctx, "tc_residency_ttl"); err != nil {
		// Absent on every deployment that never ran the older version, which is
		// not a problem and must not be reported as one.
		if !isIndexNotFound(err) {
			f.log.Warn("could not drop the old residency expiry index",
				"err", err,
				"note", "it expires rows on hotUntil, which is when the hold ends rather than "+
					"when the row may go, so a change stream read late may see a copy-up as an insert")
		}
	}

	// This one warns instead of refusing to start. The notice TTLs above are
	// load-bearing: an announcement that outlives its expiry is read as an
	// application delete and the archive tombstones a live record, so a feeder
	// that cannot create them should not run. Housekeeping is not worth a
	// stopped feeder.
	return nil
}

// Run tails the fast tier until ctx is cancelled. It reopens the stream on
// recoverable errors, resuming from the last saved checkpoint.
func (f *Feeder) Run(ctx context.Context) error {
	// Before anything is mirrored, because the damage a second feeder does is
	// done by mirroring. See internal/lease.
	if err := f.hold.Take(ctx); err != nil {
		return err
	}
	defer func() {
		// Best effort, and the point of it is that a restart does not have to
		// wait the lease out.
		if err := f.hold.Release(context.WithoutCancel(ctx)); err != nil {
			f.log.Debug("could not release the feeder lease", "err", err)
		}
	}()

	if err := f.EnsureIndexes(ctx); err != nil {
		return err
	}
	// Before the first event, not after: a capped collection destroys archive
	// history as it rolls, and by the time that shows up the history is gone.
	if err := f.checkCollectionKinds(ctx); err != nil {
		return err
	}
	f.loadUnmirrored(ctx)

	for {
		err := f.runOnce(ctx)
		switch {
		case err == nil, errors.Is(err, context.Canceled):
			return nil
		case errors.Is(err, ErrUnannouncedDelete), errors.Is(err, ErrResumePointLost),
			errors.Is(err, ErrUnmirrorableKind), errors.Is(err, lease.ErrLost):
			// ErrLost belongs in that list rather than in the reopen below:
			// another process is mirroring these namespaces now, and two of
			// them is what the lease exists to prevent.
			return err
		}

		// Naming the cause when it is one whose remedy is not on the fast tier.
		// The archive refusing every write reads as a change stream problem
		// otherwise, once a second, and an operator follows that to the wrong
		// cluster: measured with write blocking turned on at the archive, twenty
		// reopens in twenty seconds and every line saying "change stream ended".
		//
		// Reopening is the right behaviour and it is left alone. Unlike a
		// document the archive will never take, this one succeeds as soon as the
		// block is lifted, and it does: measured, mirroring caught up on its own
		// and the write made during the block reached the archive.
		if writesBlocked(err) {
			if !f.saidWritesBlocked {
				f.saidWritesBlocked = true
				f.log.Error("the archive is refusing every write, because writes are blocked there",
					"err", err,
					"meaning", "an operator or a migration tool has turned on write blocking on the "+
						"archive, so nothing can be mirrored while it is on and the fast tier is "+
						"accumulating everything this feeder has not carried",
					"effect", "mirroring is stopped for every namespace and this will keep retrying, "+
						"which is right: it resumes on its own the moment the block is lifted",
					"action", "turn write blocking off on the archive: "+
						"db.adminCommand({setUserWriteBlockMode: 1, global: false})",
					"note", "said once until mirroring resumes, because it would otherwise be said "+
						"every second")
			}
		} else {
			f.log.Error("change stream ended, reopening", "err", err)
		}
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
	for _, ns := range f.namespacesToCheck(ctx) {
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
		"action", "cline backfill --ns NS --apply copies them, and can be run with this feeder "+
			"still going: it carries what happens next while the copy brings what was already there")
}

// namespacesToCheck is what this feeder mirrors, spelled out.
//
// An empty feeder.namespaces mirrors everything, and reading the list gives
// "nothing" for it, so the warning above said nothing at all for the deployment
// that tiers the most. The fast tier is the side to enumerate here because the
// question is what it holds that the archive has none of.
func (f *Feeder) namespacesToCheck(ctx context.Context) []string {
	if len(f.cfg.Namespaces) > 0 {
		return f.cfg.Namespaces
	}
	dbs, err := f.hot.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil
	}
	var out []string
	for _, db := range dbs {
		if db == "admin" || db == "local" || db == "config" || db == meta.DB {
			continue
		}
		names, lerr := f.hot.Database(db).ListCollectionNames(ctx, bson.D{})
		if lerr != nil {
			continue
		}
		for _, name := range names {
			if !strings.HasPrefix(name, "system.") {
				out = append(out, db+"."+name)
			}
		}
	}
	sort.Strings(out)
	return out
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
		if len(token) > 0 && checkpoint.ResumePointGone(err) {
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
			if len(f.pending) > 0 && f.pendingSince.IsZero() {
				f.pendingSince = readFrom
			}
			if f.dueToFlush(time.Now()) {
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

// busyFlushEvery is the longest an event waits in the buffer while the stream
// is never quiet. See dueToFlush.
const busyFlushEvery = 100 * time.Millisecond

// flushBytes is the most document data one flush carries, and so the largest
// transaction the archive is asked to commit. See writeFlush and dueToFlush.
//
// 16MB, one document at the limit. It was 64MB, and flushes of five 12MB
// documents were refused by a 1GB archive cache as TransactionTooLargeForCache
// every time. A flush that holds more than this is one large document arriving
// with smaller ones, since the check runs as each event is buffered.
const flushBytes = 16 << 20

// dueToFlush says the buffer should go to the archive now, before the stream
// has gone quiet.
//
// A full batch always goes. What was missing is the other bound. The buffer
// was otherwise written only when a poll came back empty, and a stream that is
// never empty for max_await is not an idle one: it is a feeder with work to do
// and a buffer nothing was flushing. The position is published only after a
// flush, so it stood still for as long as the batch took to fill, and at a
// trickle that is seconds.
//
// A router reads a position that has not moved for two seconds as a feeder
// that has stopped, and a stopped feeder turns the freshness barrier off, so
// every read that fell through to the archive answered at once from an archive
// that did not yet have the writes it was asked about. Measured on 2026-09-25
// with a lone feeder and one insert about every fifteen milliseconds: the
// position unchanged for up to 3,283ms. In a run with two applications behind
// two routers it was 2,872ms, both routers gave up on the barrier in the same
// millisecond, and the shops reported nine things wrong, all in that second: an
// order placed and a count that did not move, a cancelled order that came back,
// a committed transaction whose event could not be read.
//
// A hundred milliseconds is a small fraction of that two seconds and of what a
// fall-through read is prepared to wait, and it leaves a busy feeder's batches
// full: at the 2,000 to 3,700 events a second this deployment mirrors, a batch
// of 500 fills first.
func (f *Feeder) dueToFlush(now time.Time) bool {
	if len(f.pending) == 0 {
		return false
	}
	if len(f.pending) >= f.cfg.BatchSize {
		return true
	}
	// A size as well as a count, since a flush became one transaction. The
	// archive has to hold a transaction's writes in its cache until it
	// commits, and 500 documents can be anything up to 8GB. The time bound
	// above happens to keep a flush small on this box, where 1.6GB of 4MB
	// documents mirrored in 26 seconds a few documents per flush, but that is
	// the box's throughput and not a limit: one getMore hands over 16MB at once.
	// Summed here rather than kept as a running total, because a buffer of at
	// most a batch is cheap to walk and a total is one more thing to keep right.
	size := 0
	for _, op := range f.pending {
		size += len(op.doc)
	}
	if size >= flushBytes {
		return true
	}
	return !f.pendingSince.IsZero() && now.Sub(f.pendingSince) >= busyFlushEvery
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
		// Not the router's own scratch collections, which live inside the
		// application's database because a rename cannot cross databases.
		//
		// This matters only when Namespaces is empty, which is the supported
		// way to say "mirror everything": the $or below would otherwise never
		// name one. Left in, a tiered $out had every document of its temporary
		// collection mirrored into the archive, and the rename that consumes
		// that collection is never mirrored, so the archive kept a full copy of
		// every such aggregation for ever and the router's startup report named
		// it as a collection the archive holds and the fast tier does not.
		//
		// $not on a missing field matches, which is what keeps dropDatabase
		// events coming through: those carry no ns.coll at all, and the feeder
		// logs them.
		{Key: "ns.coll", Value: bson.D{{Key: "$not", Value: bson.Regex{
			Pattern: "^" + regexp.QuoteMeta(meta.ScratchPrefix),
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

// justBefore is the latest cluster time an event at ts proves mirrored: the
// one before it, not its own.
//
// Every operation of a transaction carries the same cluster time, the
// commit's, so an event at T says everything before T has been seen and not
// that everything at T has. The position used to claim T, and a flush can fall
// in the middle of a transaction: at the 500-event batch limit, or at the 100ms
// bound of a stream that is never quiet. The router's barrier waits for the
// client's cluster time, which after a commit is exactly T, so it cleared with
// half the transaction in the archive and the read answered from that half.
// Measured on 2026-09-25 with one order and 600 events in one transaction: the
// position reached the commit with 499 of the 600 events in the archive, five
// rounds of five. The two-router store run met it as a committed transaction
// whose event could not be read back.
//
// T itself is claimed once it is proved: by the next event, which is later,
// or by an empty poll's resume token, which is MongoDB saying nothing exists up
// to there. See markCaughtUp. A read waiting on a plain write therefore waits
// for one of those, which on a quiet stream is the next poll.
func justBefore(ts bson.Timestamp) bson.Timestamp {
	switch {
	case ts.I > 0:
		return bson.Timestamp{T: ts.T, I: ts.I - 1}
	case ts.T > 0:
		return bson.Timestamp{T: ts.T - 1, I: ^uint32(0)}
	}
	return ts
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
	f.noteEventTS(justBefore(ev.ClusterTime))

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
			f.clearResidency(ctx, ns)
			// The next collection under this name may be a different kind of
			// thing entirely.
			f.forgetNamespaceKind(ns)
		}
		// And the same one level up. dropDatabase names no collection, so the
		// line above never fired for it and every claim in that database stayed
		// exactly as it was: the router went on answering bounded reads from a
		// fast tier holding nothing, and returning empty, while the archive
		// still held the documents. The twin of the case above, and the same
		// silent short answer it exists to prevent.
		if ev.OperationType == "dropDatabase" {
			f.withdrawDatabase(ctx, ev.NS.DB)
			f.clearResidencyIn(ctx, ev.NS.DB)
			f.forgetNamespaceKindsIn(ev.NS.DB)
		}
		return nil
	}

	// What kind of collection this is, asked once per namespace. The startup
	// check cannot see a collection created after it ran, and mirroring a capped
	// one destroys the archive's history rather than merely failing. See
	// checkNamespaceKind.
	if kerr := f.checkNamespaceKind(ctx, ns); kerr != nil {
		return kerr
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
			at: ev.ClusterTime,
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
		at:        ev.ClusterTime,
	})
	return nil
}

// isIndexNotFound reports whether dropping an index failed because it was not
// there. A deployment that never ran the version which created it is the
// ordinary case, not a fault.
func isIndexNotFound(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Code == 27 || ce.Name == "IndexNotFound"
	}
	return strings.Contains(err.Error(), "index not found")
}
