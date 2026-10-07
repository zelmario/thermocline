package router

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/promote"
	"github.com/zelmario/thermocline/internal/watermark"
	"github.com/zelmario/thermocline/internal/wire"
)

// readArchived fetches one document from the archive.
//
// Tombstoned documents are excluded. The archive keeps records the application
// deleted, and returning one here would resurrect it in the caller's hands.
func (r *Router) readArchived(ctx context.Context, ns string, id bson.RawValue, level string) (bson.Raw, bool, error) {
	db, coll := splitNS(ns)
	coll2 := r.cold.Database(db).Collection(coll)
	// The caller's durability request follows the read onto the other cluster.
	// Without this a point lookup asking for majority was answered locally.
	if rc := archiveReadConcernFor(level); rc != nil {
		coll2 = r.cold.Database(db).Collection(coll, options.Collection().SetReadConcern(rc))
	}
	var doc bson.Raw
	err := coll2.FindOne(ctx, bson.D{
		{Key: "_id", Value: id},
		meta.NotArchivedDeleted(),
	}).Decode(&doc)

	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read archive %s: %w", ns, err)
	}
	return doc, true, nil
}

// copyUp moves one document from the archive into the fast tier and protects it
// from the next eviction sweep. It reports whether the document existed.
func (r *Router) copyUp(ctx context.Context, ns string, id bson.RawValue) (bool, error) {
	r.copyUpIsFresh(ctx, ns, 0)
	doc, found, err := r.readArchived(ctx, ns, id, "")
	if err != nil || !found {
		return false, err
	}

	// Residency is recorded before the write, not after, the same way the
	// evictor announces a removal before making it.
	//
	// The write is what a client's change stream sees, and it sees it
	// immediately. Recording afterwards leaves a window in which the insert
	// event has been delivered and nothing yet says it was a promotion, so an
	// application is told a two year old document was just created. Recording
	// first for a copy-up that then fails costs nothing: a residency record for
	// a document that is not there protects nothing.
	db, coll := splitNS(ns)

	// Asked here too, for the reasons copyUpMatching asks: a document the fast
	// tier already holds is held and nothing more, and one it does not hold is
	// a promotion without any guessing. See promote.Held and meta.PromotionUpdate.
	asked := false
	if have, left, herr := r.holdWhatIsThere(ctx, ns, r.hot.Database(db).Collection(coll), []bson.RawValue{id}); herr == nil {
		asked = true
		if have[0] {
			return true, nil
		}
		// An eviction took it while this waited, so the copy read above may be
		// older than the one the evictor confirmed. See holdWhatIsThere.
		if len(left) > 0 {
			if doc, found, err = r.readArchived(ctx, ns, id, ""); err != nil || !found {
				return false, err
			}
		}
	}
	if err := r.recordResidency(ctx, ns, id, asked); err != nil {
		r.log.Warn("could not record residency before copying up", "ns", ns, "err", err)
	}

	// Before the insert, because an insert is what creates the collection when
	// the fast tier does not hold it, and a collection created that way carries
	// none of the archive's options. A default collation decides which documents
	// match and cannot be added afterwards. Memoised inside the indexer, so this
	// is a map lookup once the fast tier has the collection. See
	// Indexer.EnsureCollection.
	r.indexer.EnsureCollection(ctx, ns)

	// An insert, never a replace and never an upsert built from modifiers. If
	// the fast tier gained this document while we were reading the archive, its
	// copy is the newer one and the duplicate key says so. See package promote
	// for why this is not written as $setOnInsert.
	inserted, err := promote.One(ctx, r.hot.Database(db).Collection(coll), doc)
	if err != nil {
		return false, fmt.Errorf("copy up into %s: %w", ns, err)
	}
	// The first document of a namespace to arrive here is the cheapest moment
	// to build that namespace's indexes: the collection is empty or nearly so.
	// Not on this read's path though. The copy-up happens inside a client's
	// query, and a collection that already holds a lot of this namespace would
	// make the client wait out an index build it did not ask for.
	//
	// Detached from the request context for the same reason: the read returns
	// immediately, and cancelling it must not abandon the build half done.
	if inserted {
		ns := ns
		r.goSafely("building the archive's indexes on the fast tier", func() {
			r.indexer.Ensure(context.WithoutCancel(ctx), ns)
		})
	}
	if !inserted {
		// The fast tier already held it, so no insert happened and no echo is
		// coming. See withdrawEcho for what leaving the notice would cost.
		r.withdrawEcho(ctx, []string{meta.DocID(ns, id)})
	}

	// The eviction notice for this document is left alone on purpose. The
	// feeder consumes one as it applies the delete it describes, and taking it
	// away here would take it from a delete the feeder has not looked at yet,
	// which is how a real eviction came to be tombstoned as an application
	// delete. See meta.AnnounceEviction.
	r.Stats.CopiedUp.Add(1)
	return true, nil
}

// copyUpAsync copies a document up behind the reply that already went out.
func (r *Router) copyUpAsync(ns string, id bson.RawValue) {
	// Detached from the client's context on purpose: the reply has been sent,
	// and the client hanging up must not abandon a half-finished copy.
	r.goSafely("copying a document up", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := r.copyUp(ctx, ns, id); err != nil {
			r.log.Warn("copy up failed", "ns", ns, "err", err)
		}
	})
}

// recordResidency keeps a copied-up document in the fast tier for a while.
// Without it the next eviction sweep removes what was just fetched, and the
// next read pays for the archive again.
//
// asked says the fast tier was asked and did not hold the document, so this is
// a promotion for certain. See meta.PromotionUpdate.
func (r *Router) recordResidency(ctx context.Context, ns string, id bson.RawValue, asked bool) error {
	now := time.Now().UTC()
	_, err := r.cold.Database(meta.DB).Collection(meta.Residency).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: meta.DocID(ns, id)}},
		residencyFor(ns, id, now, r.cfg.Residency.D(), asked),
		options.UpdateOne().SetUpsert(true))
	return err
}

// recordResidencyMany does the same for a batch, in one round trip.
//
// The single-document version is called once per document by the multi-write
// copy-up path, and every one of those is a write to the archive, which is the
// far cluster in the shape this system is built for. Measured on this box, where
// it is not far at all: an updateMany over 200 archived documents took 465 ms,
// and the 200 residency holds were most of it.
//
// The warmer has done it this way all along; this is the same shape.
func (r *Router) recordResidencyMany(ctx context.Context, ns string, ids []bson.RawValue, asked bool) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now().UTC()
	models := make([]mongo.WriteModel, 0, len(ids))
	for _, id := range ids {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: meta.DocID(ns, id)}}).
			SetUpdate(residencyFor(ns, id, now, r.cfg.Residency.D(), asked)).
			SetUpsert(true))
	}
	// Unordered: these are independent holds, and one that cannot be written
	// must not stop the rest. The error is what the caller acts on.
	_, err := r.cold.Database(meta.DB).Collection(meta.Residency).
		BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}

// residencyFor is the promotion record for a copy-up: certain when the fast
// tier was asked, and guessed from the record when it could not be.
func residencyFor(ns string, id bson.RawValue, now time.Time, hold time.Duration, asked bool) mongo.Pipeline {
	if asked {
		return meta.PromotionUpdate(ns, id, now, hold)
	}
	return meta.ResidencyUpdate(ns, id, now, hold)
}

// holdWhatIsThere asks the fast tier which of ids it holds, holds those
// against eviction, and reports which ones a copy-up may skip. See
// promote.HoldPresent for the window between asking and holding, and why
// left's documents are read from the archive again.
func (r *Router) holdWhatIsThere(ctx context.Context, ns string, hot *mongo.Collection, ids []bson.RawValue) (have []bool, left []int, err error) {
	have, left, holdErr, err := promote.HoldPresent(ctx, hot, r.cold, ns, ids, r.cfg.Residency.D())
	if holdErr != nil {
		r.log.Warn("could not hold documents the fast tier already has against eviction",
			"ns", ns, "err", holdErr)
	}
	return have, left, err
}

// ensureTieredCollection gives the fast tier the collection the way the archive
// has it, and builds its indexes when that created it.
//
// The create has to happen before the write, because a collection an insert
// makes carries no options and a collation cannot be added afterwards. The
// build has to happen after it and off this path, because it is a scan and the
// client did not ask for one.
//
// Without the second half, an insert into a tiered collection the fast tier did
// not hold left it with the archive's options and none of its indexes until
// something listed the catalog or the router was restarted. Reads of it fell
// through to the archive meanwhile, so it was slow rather than wrong, and
// nothing said how long it would stay that way.
func (r *Router) ensureTieredCollection(ctx context.Context, ns string) {
	if !r.indexer.EnsureCollection(ctx, ns) {
		return
	}
	r.goSafely("building the archive's indexes on a collection a write created", func() {
		r.indexer.Ensure(context.WithoutCancel(ctx), ns)
	})
}

// watermarkCache holds one namespace's completeness for a short while.
type watermarkCache struct {
	mu      sync.RWMutex
	entries map[string]watermarkEntry
}

type watermarkEntry struct {
	mark    watermark.Mark
	known   bool
	fetched time.Time
	// lastFail is when reading the claim last failed, kept so an unreachable
	// archive is paid for once per completenessTTL instead of once per read.
	//
	// Measured with nothing answering on the archive's address: one claim read
	// costs 30.002 seconds, the driver's default server selection timeout,
	// which no daemon here shortens. Every tiered read paid that and was then
	// refused, so an archive outage was not a router refusing reads, it was a
	// router hanging for half a minute and then refusing them. A client with a
	// socket timeout gives up first and sees nothing but a broken proxy.
	//
	// The claim's two copies already disagree for completenessTTL by design, so
	// a failure is allowed to be that stale as well: the first read after one
	// pays the timeout, the rest of that window are answered from what is held.
	lastFail time.Time
}

// completenessTTL is how long a watermark reading is reused, because reading it
// per query would put an archive round trip on the hot path.
//
// It is not merely an optimisation with a harmless amount of staleness, which
// is what this comment used to say. A stale claim is always the more permissive
// one, so anything that narrows a claim has to wait this long before acting on
// the narrowing. See watermark.ClaimCacheTTL, which is where that contract is
// written down and which this must equal.
const completenessTTL = watermark.ClaimCacheTTL

// forgetCompleteness drops a cached claim, for the moment the router itself
// makes it untrue.
//
// A drop removes every document of a collection from the fast tier, and the
// claim that it holds them all outlives it: the feeder withdraws the claim when
// it sees the event, and this router keeps its cached copy for ClaimCacheTTL
// after that. Measured on a collection claimed complete for all of history and
// dropped through the router: countDocuments answered 0 where the archive holds
// 3, with no error.
//
// A drop this router forwarded is one it knows about at once, so there is no
// reason to learn it again later. A drop applied straight to the fast tier is
// not, and that window stays: the router is not watching for it, and the answer
// corrects itself once the feeder has withdrawn the claim and this cache has
// expired.
func (r *Router) forgetCompleteness(ns string) {
	r.wmCache.mu.Lock()
	delete(r.wmCache.entries, ns)
	r.wmCache.mu.Unlock()
}

// claimReadFailedRecently reports whether reading this claim failed inside the
// last completenessTTL, in which case reading it again is not worth another
// server selection timeout.
func claimReadFailedRecently(e watermarkEntry, cached bool, now time.Time) bool {
	return cached && !e.lastFail.IsZero() && now.Sub(e.lastFail) < completenessTTL
}

// claimWithinGrace reports whether a claim the archive can no longer be asked
// for again may still be answered from.
//
// Four conditions, and each one has a reason to be here. There has to be a
// grace configured, because the whole thing is off by default. There has to be
// a cached entry, and it has to be one that was read successfully: a router
// that started during the outage has nothing to hold over, and an entry
// recording a previous failure is not a claim. And the window runs from the
// moment the claim was last read, not from the moment reading it started
// failing, so an outage that took a while to notice does not extend it.
func claimWithinGrace(e watermarkEntry, cached bool, grace time.Duration, now time.Time) bool {
	if grace <= 0 || !cached || !e.known {
		return false
	}
	return now.Sub(e.fetched) < completenessTTL+grace
}

// completeness returns what the fast tier claims to hold for ns, and whether
// the claim could be read at all.
//
// A read failure is reported as unknown rather than folded into an unclaimed
// Mark. Both refuse in strict mode, but they need different words: one is "warm
// this namespace", the other is "the cold tier is unreachable".
//
// A claim held only under router.claim_grace is reported as unknown here, so
// every caller that has not deliberately asked for the grace keeps refusing.
//
// That is the safe direction for a call site nobody thought about. The grace
// trades a window of short answers for availability, which the operator chose,
// and a short answer is the worst a read can do. A write is not bounded that
// way: an insert skips the archive's own duplicate check when the fast tier
// claims all of history, so a claim that is false lets through an _id the
// archive still holds, and the feeder then mirrors the new document over the
// archived one. Measured, before this split existed: with a grace configured
// and the archive stopped, an insert the plain router refused was accepted and
// landed on the fast tier. A few minutes of short answers is a trade worth
// offering. A few minutes in which an archived document can be destroyed is
// not, and it was not what was asked for.
func (r *Router) completeness(ctx context.Context, ns string) (watermark.Mark, bool) {
	mark, known, graced := r.completenessClaim(ctx, ns)
	if graced {
		return mark, false
	}
	return mark, known
}

// completenessForRead is completeness for the paths where believing a claim
// that has become false shortens an answer and does nothing else. Those are the
// paths router.claim_grace exists for.
func (r *Router) completenessForRead(ctx context.Context, ns string) (watermark.Mark, bool) {
	mark, known, _ := r.completenessClaim(ctx, ns)
	return mark, known
}

// completenessClaim reads the claim, saying also whether it is believed only
// because router.claim_grace is holding one that can no longer be re-read.
//
// Named in full because "claim" alone means a key claim in the registry
// everywhere else in this package.
func (r *Router) completenessClaim(ctx context.Context, ns string) (watermark.Mark, bool, bool) {
	r.wmCache.mu.RLock()
	e, ok := r.wmCache.entries[ns]
	r.wmCache.mu.RUnlock()
	if ok && time.Since(e.fetched) < completenessTTL {
		return e.mark, e.known, false
	}

	// A read of this claim failed a moment ago, so the archive is unreachable
	// and asking again costs half a minute to learn the same thing.
	if now := time.Now(); claimReadFailedRecently(e, ok, now) {
		if grace := r.cfg.ClaimGrace.D(); claimWithinGrace(e, ok, grace, now) {
			r.Stats.ClaimGraceReads.Add(1)
			return e.mark, true, true
		}
		return watermark.Mark{}, false, false
	}

	mark, err := r.marks.Load(ctx, ns)
	if err != nil {
		// An archive that cannot be reached is an archive where nothing can be
		// evicted and no claim can be narrowed, because both of those are
		// writes to it. So the claim this router last read is still true, and
		// router.claim_grace says for how much longer it may be believed. Off
		// by default; the fault it does not protect against is written up on
		// the field.
		//
		// Measured from the last successful read rather than from the failure,
		// so the window is what the operator configured and not that plus
		// however long the outage took to notice.
		//
		// The failure is recorded first and on both paths. It keeps the claim
		// and the moment it was read, because the grace window is measured from
		// that, and it is recorded before the grace is consulted because a
		// graced read that did not remember the failure would pay the 30 second
		// timeout again on the very next query, which is the cost this exists
		// to avoid.
		now := time.Now()
		held := e
		if !ok {
			held = watermarkEntry{}
		}
		held.lastFail = now
		r.rememberClaim(ns, held)

		if grace := r.cfg.ClaimGrace.D(); claimWithinGrace(e, ok, grace, now) {
			r.Stats.ClaimGraceReads.Add(1)
			r.log.Warn("answering from a completeness claim that can no longer be re-read",
				"ns", ns, "age", now.Sub(e.fetched).Truncate(time.Second).String(),
				"grace", grace.String(), "err", err,
				"reason", "router.claim_grace is set and the archive is unreachable, so no claim "+
					"can have been narrowed and none of what it covers can have been evicted",
				"effect", "if only this router is cut off, rather than the archive being down, "+
					"the evictor may be removing documents this answer counts as present",
				"action", "check whether the archive is down or this router is partitioned from it")
			return e.mark, true, true
		}
		r.log.Warn("could not read the completeness watermark", "ns", ns, "err", err)
		return watermark.Mark{}, false, false
	}
	// A claim measured in one field and read in another is not a claim about
	// anything. The row records which field it was made in, and if that is not
	// the field this namespace ages by now, the claim is withdrawn rather than
	// reinterpreted: reads fall through to the archive, which is the answer they
	// would have had before anybody claimed anything.
	//
	// Measured on twenty orders whose shippedAt is thirty days after their
	// createdAt, claimed complete from 15 January in createdAt: with the rule's
	// date_field changed to shippedAt, the router answered 6 where the archive
	// holds 20, and reported success.
	//
	// Only when the row says. Rows written before the field was recorded are
	// used exactly as they were, because refusing every claim on an upgrade is a
	// worse failure than the one this prevents, and the next write stamps them.
	if mark.Claimed && mark.Measured {
		if want, ok := r.cfg.AgeField(ns); ok && want != mark.Dimension {
			r.warnClaimDimension(ns, mark.Dimension, want)
			mark = watermark.Mark{IndexGen: mark.IndexGen}
		}
	}

	entry := watermarkEntry{mark: mark, known: true, fetched: time.Now()}

	// Another router changed this namespace's unique indexes, so the listing
	// this one is enforcing from is out of date. It rides on the claim because
	// the claim is already being read here: no extra round trip, and the window
	// where two routers disagree about what is unique shrinks from indexRefresh
	// to the claim cache's own TTL.
	//
	// Only when a previous reading exists to compare against. A first read
	// learns the number rather than reacting to it, or every router would drop
	// its listing on the first query after starting, for nothing.
	if ok && e.known && e.mark.IndexGen != mark.IndexGen {
		r.log.Info("another router changed the unique indexes here, re-reading them",
			"ns", ns, "was", e.mark.IndexGen, "now", mark.IndexGen)
		r.forgetIndexes(ns)
	}

	r.rememberClaim(ns, entry)
	return entry.mark, entry.known, false
}

func (r *Router) rememberClaim(ns string, entry watermarkEntry) {
	r.wmCache.mu.Lock()
	if r.wmCache.entries == nil {
		r.wmCache.entries = make(map[string]watermarkEntry)
	}
	r.wmCache.entries[ns] = entry
	r.wmCache.mu.Unlock()
}

// warnAboutMissingWatermarks says out loud what an empty watermarks collection
// means now: range queries get refused until something records a claim.
//
// This used to be the other way round, and it was the wrong way round. An
// absent watermark was read as "nothing has been removed, so the fast tier
// holds everything", which is true only of a fast tier that was the original
// cluster and false of every fast tier built empty. Since the cold start is the
// adoption path this project recommends, the safe reading is that an absent
// claim guarantees nothing.
func (r *Router) warnAboutMissingWatermarks(ctx context.Context) {
	// Looking for a claim rather than for a row. A row without completeFrom is
	// not a claim, and counting it as one would suppress this warning while
	// nothing is actually claimed. The same mistake as reading Claimed from the
	// row's existence, in a third place: see watermark.markOf.
	cur, err := r.cold.Database(meta.DB).Collection(meta.Watermarks).
		Find(ctx,
			bson.D{{Key: "completeFrom", Value: bson.D{{Key: "$exists", Value: true}}}},
			options.Find().SetLimit(1))
	if err != nil {
		return
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	if !cur.Next(ctx) {
		r.log.Warn("no completeness watermarks recorded",
			"meaning", "range queries will be refused, because nothing has claimed the fast tier is complete",
			"action", "run the warmer, or declare it with: cline watermark --ns NS --complete-all")
	}
}

// warnAboutArchiveWithoutHistory says so when this archive has no record of
// mirroring while the fast tier already holds tiered data.
//
// The archive is where the checkpoint lives, so the archive is what remembers
// that this deployment has ever mirrored into it. A fast tier holding documents
// for a tiered namespace, against an archive that has never heard of the
// feeder, is a pair that has not been working together, and there are only
// three ways to be in it.
//
// The third is the one nothing else catches. `cold.uri` can be repointed at a
// different cluster by a typo, a config copied from another environment, or a
// DNS name that now resolves somewhere else, and the router will serve whatever
// that cluster happens to hold. Measured on two archives with a collection of
// the same name in each: ten documents mirrored into the first, then the URI
// changed to the second and everything restarted. A client counted four and
// read "another environment" out of a collection its own archive holds ten real
// documents in. The only line in the log was "no completeness watermarks
// recorded", which a fresh deployment prints too.
//
// Nothing records the pairing, and the checkpoint is the nearest thing to a
// record there is: it is written by this deployment, into this archive, under
// this feeder's id.
func (r *Router) warnAboutArchiveWithoutHistory(ctx context.Context) {
	id := r.cfg.FeederID
	if id == "" {
		return
	}
	n, err := r.cold.Database(meta.DB).Collection(meta.Checkpoints).
		CountDocuments(ctx, bson.D{{Key: "_id", Value: id}}, options.Count().SetLimit(1))
	if err != nil || n > 0 {
		return
	}

	// Only worth saying if the fast tier has something to have mirrored. A
	// deployment that has not started yet has neither, and an adoption has a
	// full archive and an empty fast tier, so neither of those lands here.
	var holding []string
	for db, colls := range r.tieredByDatabase(ctx) {
		for _, coll := range colls {
			c, cerr := r.hot.Database(db).Collection(coll).EstimatedDocumentCount(ctx)
			if cerr != nil || c == 0 {
				continue
			}
			holding = append(holding, db+"."+coll)
			if len(holding) >= 5 {
				break
			}
		}
		if len(holding) >= 5 {
			break
		}
	}
	if len(holding) == 0 {
		return
	}
	sort.Strings(holding)

	r.log.Warn("this archive has no record of mirroring, and the fast tier already holds tiered data",
		"feeder_id", id,
		"collections", r.listFor(holding),
		"meaning", "the checkpoint is written by this deployment into this archive under this "+
			"feeder's id, and there is none, so this pair has not been working together",
		"action", "check cold.uri names the archive this deployment has been using, and feeder.id "+
			"the id it has been using; if both are right then the feeder has simply never run",
		"why", "an archive repointed at another cluster serves that cluster's data, and nothing "+
			"else here would notice: measured, a client read another environment's documents "+
			"out of a collection of the same name")
}

// warnAboutPartlyStrandedCollections names collections the archive holds more of
// than the fast tier and that nothing tiers.
//
// warnAboutStrandedCollections is the all-or-nothing form of this and catches
// the adoption shape: a collection the fast tier has never held at all. It skips
// a collection the fast tier has, and that is the case this covers, because
// having some of a collection and having it are not the same thing.
//
// The way in is one config line. Take a namespace out of `feeder.namespaces`
// and the collection stops being tiered, so it becomes the fast tier's alone by
// definition and the archive's copy stops being read. Measured on fifty
// documents with the fast tier holding the most recent ten: tiered, a client
// counted fifty and read the oldest; untiered, it counted ten and the oldest was
// missing. Nothing was said anywhere.
//
// Estimated counts, because the question is whether one side has much more of it
// and an exact answer would cost a scan of the archive. Only for collections
// both tiers hold and nothing tiers, which on an ordinary deployment is a short
// list: an untiered collection is normally the fast tier's alone.
func (r *Router) warnAboutPartlyStrandedCollections(ctx context.Context) {
	var short []string
	for _, db := range r.archiveDatabases(ctx) {
		// This system's own bookkeeping is not an application's data, and
		// another deployment's is not either. The stranded report makes the
		// same exclusion for the same reason.
		if db == meta.DB {
			continue
		}
		cold, cerr := collectionsIn(ctx, r.cold, db)
		hot, herr := collectionsIn(ctx, r.hot, db)
		if cerr != nil || herr != nil {
			continue
		}
		if meta.IsBookkeeping(collectionNames(cold)) {
			continue
		}
		for name, kind := range cold {
			ns := db + "." + name
			if kind != "collection" || hot[name] != "collection" {
				continue
			}
			if r.cfg.Manages(ns) || strings.HasPrefix(name, "system.") || meta.IsScratch(name) {
				continue
			}
			inCold, err := r.cold.Database(db).Collection(name).EstimatedDocumentCount(ctx)
			if err != nil || inCold == 0 {
				continue
			}
			inHot, err := r.hot.Database(db).Collection(name).EstimatedDocumentCount(ctx)
			if err != nil || inHot >= inCold {
				continue
			}
			short = append(short, fmt.Sprintf("%s (%d of %d)", ns, inHot, inCold))
		}
	}
	if len(short) == 0 {
		return
	}
	sort.Strings(short)
	r.log.Warn("the archive holds more of these collections than the fast tier, and nothing tiers them",
		"collections", r.listFor(short),
		"meaning", "a collection this system does not tier is the fast tier's alone, so the "+
			"archive's copy is never read and the application sees only the part the fast tier has",
		"action", "put them back in feeder.namespaces if they should be tiered; if they should not, "+
			"copy the archive's copy onto the fast tier before anything reads them",
		"why", "taking a namespace out of feeder.namespaces does this, and it is one line in a "+
			"config file away")
}

// namesInALine is how many names a warning puts in one line before it stops.
//
// A line is read by a person. Measured on a cluster with a long history of test
// runs behind it, the report above named just under three hundred collections in
// one line, which is not a report, it is a wall: nobody finds the collection
// they care about in it and nobody reads the next field either.
const namesInALine = 20

// firstFew caps a list for a log line and says how much it left out.
func firstFew(names []string) []string {
	if len(names) <= namesInALine {
		return names
	}
	out := make([]string, 0, namesInALine+1)
	out = append(out, names[:namesInALine]...)
	return append(out, fmt.Sprintf("... and %d more; cline preflight lists them all",
		len(names)-namesInALine))
}

// listFor is firstFew for a router that was asked for a report rather than
// started.
//
// The cap is right for a log line, which a person reads in passing and which
// nothing can page. It is wrong for `cline preflight`, which somebody ran on
// purpose and is reading on a terminal, and where the action every one of these
// warnings names is per collection: an adoption with three hundred collections
// the archive holds more of was told about twenty of them and had no way to see
// the rest. Measured on this box, whose archive carries the pilot's sample
// datasets: the line named 20 and said "and 269 more".
func (r *Router) listFor(names []string) []string {
	if r.reporting {
		return names
	}
	return firstFew(names)
}

// tieredNamespaces is every namespace this router tiers, as "db.coll".
//
// The list in the configuration when there is one, and the archive's own
// collections when there is not, because `feeder.namespaces` empty tiers
// everything and the archive is the superset. Every report that iterates
// r.cfg.Managed directly says nothing at all in that configuration, which is
// the deployment those reports matter most in.
func (r *Router) tieredNamespaces(ctx context.Context) []string {
	if len(r.cfg.Managed) > 0 {
		return append([]string(nil), r.cfg.Managed...)
	}
	var out []string
	for _, db := range r.archiveDatabases(ctx) {
		cold, err := collectionsIn(ctx, r.cold, db)
		if err != nil {
			continue
		}
		for name, kind := range cold {
			if kind == "collection" && !strings.HasPrefix(name, "system.") {
				out = append(out, db+"."+name)
			}
		}
	}
	sort.Strings(out)
	return out
}

// tieredByDatabase groups the namespaces this router tiers by database.
//
// `feeder.namespaces` empty is not "nothing", it is "everything except the
// system databases and this deployment's own bookkeeping", and it is a
// documented configuration. Reading r.cfg.Managed alone therefore answers
// "nothing is tiered" for the deployment that tiers the most, which is how the
// check above did nothing at all in exactly the shape it matters most in.
func (r *Router) tieredByDatabase(ctx context.Context) map[string][]string {
	byDB := map[string][]string{}
	for _, ns := range r.cfg.Managed {
		if db, coll := splitNS(ns); db != "" && coll != "" {
			byDB[db] = append(byDB[db], coll)
		}
	}
	if len(byDB) > 0 {
		return byDB
	}

	names, err := r.hot.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return byDB
	}
	for _, db := range names {
		if isSystemDatabase(db) || db == meta.DB {
			continue
		}
		hot, herr := collectionsIn(ctx, r.hot, db)
		if herr != nil {
			continue
		}
		for name, kind := range hot {
			if kind != "collection" || strings.HasPrefix(name, "system.") {
				continue
			}
			byDB[db] = append(byDB[db], name)
		}
	}
	return byDB
}

// warnAboutUnarchivedCollections names tiered collections the fast tier holds
// and the archive does not.
//
// This is the stranded case the other way round and it costs more, because the
// archive is what answers. A tiered namespace the fast tier cannot claim is
// complete is read from the archive, whatever the fast tier happens to hold, and
// the archive being the superset is what makes that right. Take its collection
// away and the read is answered from nothing: the documents are still on the
// fast tier, present and unreachable, and the reply is ok.
//
// Measured on thirty mirrored documents with the archive's copy dropped out of
// band: the fast tier still held all thirty, the client was told zero, and
// nothing in either tier's log mentioned it. An eviction sweep over the same
// namespace evicted nothing, which is the one piece of good news, so the fast
// tier's copy is not at further risk while this stands.
//
// Three ways to arrive here and the advice differs, so the line names all three
// rather than guessing: the collection was dropped on the archive, the archive
// was restored from a backup older than the collection, or nothing has ever
// mirrored this namespace because the feeder is not running. The last is why
// the count is taken: a namespace nothing has written to yet has no copy on
// either tier and is not worth a word.
func (r *Router) warnAboutUnarchivedCollections(ctx context.Context) {
	var missing []string
	for db, colls := range r.tieredByDatabase(ctx) {
		hot, herr := collectionsIn(ctx, r.hot, db)
		if herr != nil {
			continue
		}
		cold, cerr := collectionsIn(ctx, r.cold, db)
		if cerr != nil {
			continue
		}
		for _, coll := range colls {
			if hot[coll] == "" || cold[coll] != "" {
				continue
			}
			if strings.HasPrefix(coll, "system.") {
				continue
			}
			// An estimate, because the question is whether anything is there
			// at all and the exact number would cost a scan. A namespace
			// written to seconds ago and not yet mirrored lands here too, which
			// is the mirroring lag and is named in the line below.
			n, err := r.hot.Database(db).Collection(coll).EstimatedDocumentCount(ctx)
			if err != nil || n == 0 {
				continue
			}
			missing = append(missing, db+"."+coll)
		}
	}
	if len(missing) == 0 {
		return
	}
	sort.Strings(missing)
	r.log.Warn("the fast tier holds tiered collections the archive does not",
		"collections", r.listFor(missing),
		"meaning", "a tiered collection the fast tier cannot claim is complete is read from the archive, "+
			"so these answer as empty while the fast tier still holds the documents",
		"action", "check the feeder is running and mirroring; if the archive lost them, the fast tier's "+
			"copy is still there to put back, and an eviction sweep will not remove it",
		"note", "a namespace written to in the last few seconds is here because mirroring has not caught up yet")
}

// warnAboutStrandedCollections names collections the archive holds that the
// fast tier does not and that nothing tiers, because the application will not
// be able to see them at all.
//
// This is the shape of every adoption. The existing cluster becomes the archive
// carrying every collection it ever had; the fast tier is new and empty; only
// the namespaces in feeder.namespaces are ever filled into it. A collection
// left out of that list lives only on the archive, and the router forwards
// reads of it to the fast tier, correctly, because a collection Thermocline
// does not tier is the fast tier's alone. The application sees an empty
// collection where it used to see its catalogue.
//
// Silence is the failure here. The behaviour is right and the surprise is
// total, so it is said once at startup, naming them.
func (r *Router) warnAboutStrandedCollections(ctx context.Context) {
	managed := map[string]bool{}
	for _, ns := range r.cfg.Managed {
		if db, _ := splitNS(ns); db != "" {
			managed[db] = true
		}
	}

	var wholly []string
	for _, db := range r.archiveDatabases(ctx) {
		cold, cerr := collectionsIn(ctx, r.cold, db)
		hot, herr := collectionsIn(ctx, r.hot, db)
		if cerr != nil || herr != nil {
			continue
		}
		// Another deployment's bookkeeping is not this application's data, and
		// telling an operator to tier somebody's checkpoints is advice that
		// would do harm if followed. This deployment's own is already excluded
		// by name, and anything it tiers is examined whatever it is called.
		if !managed[db] && meta.IsBookkeeping(collectionNames(cold)) {
			continue
		}
		var stranded, views []string
		for name, kind := range cold {
			ns := db + "." + name
			if hot[name] != "" || r.cfg.Manages(ns) || strings.HasPrefix(name, "system.") {
				continue
			}
			// A view separately, because the advice for one is different and
			// the advice for a collection cannot be followed for a view: it
			// holds no documents, so tiering it is meaningless, and the feeder
			// refuses it for that reason.
			if kind == "view" {
				views = append(views, ns)
				continue
			}
			stranded = append(stranded, ns)
		}
		// A database the fast tier has no trace of is worse than a stranded
		// collection and reads differently to an operator checking up on it:
		// listDatabases and listCollections are answered by the fast tier, so
		// the database is not empty, it is absent. Nothing names it anywhere.
		if len(hot) == 0 && len(stranded)+len(views) > 0 {
			wholly = append(wholly, db)
		}
		if len(stranded) > 0 {
			sort.Strings(stranded)
			r.log.Warn("the archive holds collections the fast tier does not, and nothing tiers them",
				"collections", r.listFor(stranded),
				"meaning", "reads of these go to the fast tier, which has never held them, so the "+
					"application sees them empty",
				"action", "add them to feeder.namespaces to tier them, or copy them to the fast tier "+
					"if they are small enough to live there whole")
		}
		if len(views) > 0 {
			sort.Strings(views)
			r.log.Warn("the archive holds views the fast tier does not",
				"views", r.listFor(views),
				"meaning", "reads of these go to the fast tier, which has never held them, so the "+
					"application sees them empty",
				"action", "recreate them on the fast tier with createView, and tier the collections "+
					"they read from; a view cannot be tiered itself and the feeder refuses one")
		}
	}
	if len(wholly) > 0 {
		sort.Strings(wholly)
		r.log.Warn("the fast tier has no trace of these databases at all",
			"databases", wholly,
			"meaning", "listDatabases and listCollections are answered by the fast tier, so these do "+
				"not show up as empty, they do not show up",
			"action", "tier or copy what is in them, and check this list against what the "+
				"application expects to find")
	}
}

// collectionNames is the names out of what collectionsIn returned.
func collectionNames(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for name := range m {
		out = append(out, name)
	}
	return out
}

// archiveDatabases names every database on the archive worth comparing against
// the fast tier.
//
// This used to be derived from feeder.namespaces, which meant it could only see
// databases that already had something tiered in them. A database with nothing
// tiered was never looked at, and that is the worst case rather than an edge
// one: every collection in it is invisible to the application and no warning
// mentioned it.
//
// Measured on MongoDB's own sample datasets adopted as an archive, with four
// collections tiered across four databases: startup named eleven stranded
// collections and said nothing about five whole databases holding 44,000
// documents, including the one the sample data's own tutorials start with.
//
// The configured namespaces are kept in the set as well, so a database named in
// the config but missing from the archive is still examined rather than
// disappearing from the report along with the typo that caused it.
func (r *Router) archiveDatabases(ctx context.Context) []string {
	dbs := map[string]bool{}
	for _, ns := range r.cfg.Managed {
		if db, _ := splitNS(ns); db != "" {
			dbs[db] = true
		}
	}
	names, err := r.cold.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		r.log.Warn("could not list the archive's databases, so this report covers only what is tiered",
			"err", err)
	}
	for _, n := range names {
		if isSystemDatabase(n) {
			continue
		}
		dbs[n] = true
	}

	out := make([]string, 0, len(dbs))
	for db := range dbs {
		out = append(out, db)
	}
	sort.Strings(out)
	return out
}

// warnAboutMissingUsers names accounts the archive has that the fast tier does
// not, because the application will not be able to log in with them.
//
// This is certain in an adoption rather than merely possible. The existing
// cluster carries the application's users; the fast tier is a new replica set
// and carries none of them. The router authenticates against the fast tier,
// either by reading admin.system.users itself when it terminates
// authentication or by forwarding the handshake, so on day one every
// connection is refused with MongoDB's ordinary authentication failure and
// nothing connects the failure to the tier that is missing the account.
//
// Said once at startup, naming them, because the fix is a minute's work and
// finding out what to fix is the part that costs an afternoon.
//
// Reading admin.system.users needs a privilege the router's own account may not
// have when it is not terminating authentication, so a refusal here is logged
// at debug and nothing more: this is a courtesy, not a precondition.
func (r *Router) warnAboutMissingUsers(ctx context.Context) {
	cold, cerr := usersIn(ctx, r.cold)
	if cerr != nil {
		r.log.Debug("could not read the archive's users to compare them", "err", cerr)
		return
	}
	hot, herr := usersIn(ctx, r.hot)
	if herr != nil {
		r.log.Debug("could not read the fast tier's users to compare them", "err", herr)
		return
	}

	var missing []string
	for u := range cold {
		if !hot[u] {
			missing = append(missing, u)
		}
	}
	if len(missing) == 0 {
		return
	}
	sort.Strings(missing)
	r.log.Warn("the archive has accounts the fast tier does not",
		"users", missing,
		"meaning", "the router authenticates against the fast tier, so these cannot log in and the "+
			"application sees an ordinary authentication failure",
		"action", "create them on the fast tier with the same names, passwords and roles")
}

// warnAboutMissingRoles is the same courtesy for the roles those accounts hold.
//
// The action for a missing account is to create it on the fast tier with the
// same roles, and a custom role the fast tier does not have makes that step fail
// with "Could not find role: appreader@admin". Measured, and it is the ordinary
// case: an application with its own role is an application whose users all
// reference it, so the first adoption step refuses every one of them and the
// reason is a role nobody was thinking about.
//
// Built-in roles are left out. Every server has them, so naming them would bury
// the one that matters.
func (r *Router) warnAboutMissingRoles(ctx context.Context) {
	cold, cerr := rolesIn(ctx, r.cold)
	if cerr != nil {
		r.log.Debug("could not read the archive's roles to compare them", "err", cerr)
		return
	}
	hot, herr := rolesIn(ctx, r.hot)
	if herr != nil {
		r.log.Debug("could not read the fast tier's roles to compare them", "err", herr)
		return
	}

	var missing []string
	for role := range cold {
		if !hot[role] {
			missing = append(missing, role)
		}
	}
	if len(missing) == 0 {
		return
	}
	sort.Strings(missing)
	r.log.Warn("the archive has roles the fast tier does not",
		"roles", missing,
		"meaning", "creating the application's accounts on the fast tier fails while these are "+
			"missing, because an account cannot be given a role that does not exist there",
		"action", "create these first, with the same privileges, then the accounts that hold them")
}

// warnAboutOptionDrift says so when a tiered collection is not the same
// collection on both tiers.
//
// The fast tier's copy is created with the archive's options before anything
// inserts into it, which is what keeps a collation and a validator working
// after an adoption. Two things get past that. An application insert can create
// the collection first, and the comment on createWithOptions calls that the
// expected way to lose the race; and a collMod through the router afterwards
// reaches the fast tier alone, because DDL is not mirrored.
//
// Either way the two tiers stop being the same collection, and a collation
// cannot be added to one that already holds documents. Measured on a collection
// the archive created with {locale: "en", strength: 2} and the fast tier created
// by an insert: the archive answers countDocuments({name: "alice"}) with 2 and
// the fast tier with 1, so the answer depends on which tier served it.
//
// Only the options that decide answers are compared. Everything else the copy
// carries is worth carrying and not worth a warning about.
func (r *Router) warnAboutOptionDrift(ctx context.Context) {
	// One listCollections per database per tier, not one per namespace. This
	// runs before the listener opens, so what it costs is time an application
	// spends unable to connect: measured on a config naming 90 namespaces,
	// asking per namespace took the router from 1.0s to 2.1s to start, and
	// asking per database takes it back.
	byDB := map[string][]string{}
	for _, ns := range r.tieredNamespaces(ctx) {
		db, coll := splitNS(ns)
		if db == "" || coll == "" {
			continue
		}
		byDB[db] = append(byDB[db], coll)
	}
	dbs := make([]string, 0, len(byDB))
	for db := range byDB {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)

	for _, db := range dbs {
		cold, cerr := answerOptionsIn(ctx, r.cold, db)
		hot, herr := answerOptionsIn(ctx, r.hot, db)
		if cerr != nil || herr != nil {
			continue
		}
		colls := byDB[db]
		sort.Strings(colls)
		for _, coll := range colls {
			c, onCold := cold[coll]
			h, onHot := hot[coll]
			if !onCold || !onHot {
				// A collection only one tier has is a different report
				// entirely, and the catalog merge and the indexer between them
				// are what answer for it.
				continue
			}
			ns := db + "." + coll
			r.warnAboutLostConstraint(ctx, ns, db, coll)
			for _, key := range drifted(c, h) {
				// A capped archive is not a difference of opinion about an
				// option, and saying "make the two copies the same" about it is
				// advice that would destroy the fast tier too. mongod removes
				// the oldest documents of a capped collection as it fills, and
				// the archive is where the history this system exists to keep
				// is kept, so it is thrown away with nothing said and reads
				// that fall through come back short.
				//
				// Measured on 80 mirrored documents with convertToCapped run
				// straight at the archive: the conversion left 16 and the rest
				// were gone, mirroring carried on without an error of any kind,
				// and nothing but this line named it. The feeder refuses to
				// start against such a pair, which is the other half: this is
				// what a router says while one keeps running.
				if key == "capped" && cappedOption(c[key]) {
					r.log.Warn("the archive's copy of a tiered collection is capped",
						"ns", ns,
						"meaning", "a capped collection removes its own oldest documents as it "+
							"fills, and the archive is where this system keeps the history, so it "+
							"is being thrown away as new documents arrive and a read that falls "+
							"through comes back short with no error",
						"action", "copy the archive's collection into an uncapped one and rename it "+
							"back, then restart the feeder, which refuses to run against this",
						"not", "making the fast tier capped as well, which is what matching the two "+
							"copies would mean here")
					continue
				}
				action := optionAdvice[key]
				if action == "" {
					action = "make the two copies the same, or know why they differ"
				}
				r.log.Warn("a tiered collection is not the same collection on both tiers",
					"ns", ns, "option", key,
					"archive", shortOption(c[key]), "fast_tier", shortOption(h[key]),
					"meaning", "which tier answers decides the answer, and this option is part of "+
						"what the answer is: a collation decides which documents match, a validator "+
						"which writes are refused, and the rest are carried onto the fast tier's copy "+
						"when this system creates it and never looked at again",
					"action", action)
			}
		}
	}
}

// warnAboutLostConstraint says so when the archive has a unique index the fast
// tier does not.
//
// The router reads the fast tier's unique indexes to know what to protect
// across both tiers, so while this is true it accepts a value the archive then
// refuses. The feeder cannot mirror that document and says so, once, in an
// error nobody is reading at the time; the document stays on the fast tier
// alone, and the evictor will not remove it because a document the archive does
// not hold is never evicted.
//
// Measured: a unique index dropped through the router, which reaches the fast
// tier alone because DDL is not mirrored, then the duplicate the application
// was now entitled to write. Afterwards the fast tier held two documents and
// the archive held one.
//
// The indexer would put the index back on the next router start, since the
// archive still defines it, which makes this a constraint that is off now and
// on again later. Saying so is worth more than either half of that.
func (r *Router) warnAboutLostConstraint(ctx context.Context, ns, db, coll string) {
	cold, cerr := uniqueIndexNames(ctx, r.cold, db, coll)
	hot, herr := uniqueIndexNames(ctx, r.hot, db, coll)
	if cerr != nil || herr != nil {
		return
	}
	// Two lists: the ones the fast tier has no unique index for at all, and the
	// ones it has under the same name enforcing something else. The second kind
	// is worse, because the indexer repairs the first on the next start and
	// leaves the second alone: an index of that name already exists.
	var missing, differs []string
	for name, enforces := range cold {
		switch got, ok := hot[name]; {
		case !ok:
			missing = append(missing, name)
		case got != enforces:
			differs = append(differs, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		r.log.Warn("the archive has a unique index the fast tier does not",
			"ns", ns, "indexes", missing,
			"meaning", "uniqueness is enforced from the fast tier's indexes, so a value this one "+
				"refuses is accepted here and then refused by the archive, and that document stays "+
				"on the fast tier alone",
			"action", "build it on the fast tier, or drop it from the archive if the constraint is "+
				"genuinely gone")
	}
	if len(differs) > 0 {
		sort.Strings(differs)
		r.log.Warn("a unique index enforces something different on each tier",
			"ns", ns, "indexes", differs,
			"meaning", "both tiers have it and both are unique, but a partial filter or a collation "+
				"makes them cover different documents or treat different values as the same, and "+
				"uniqueness is enforced from the fast tier's, so a value the archive refuses is "+
				"accepted here and that document stays on the fast tier alone",
			"action", "recreate the fast tier's copy with the archive's partial filter and "+
				"collation, or change the archive's to match if the narrower one is what is wanted")
	}
}

// uniqueIndexNames maps each unique index's name to what it enforces.
//
// The value used to be a bool, which made the comparison above "does the fast
// tier have a unique index of this name" and nothing more. unique on its own
// does not say which documents a constraint covers: a partial filter narrows it
// to the documents the filter matches, and a collation decides which values
// count as the same. Two indexes that are both unique and differ in either
// enforce different things, and the registry protects from the fast tier's.
//
// Measured with {code: 1} unique on both tiers, the archive's filtered to
// {active: true} and the fast tier's to {active: true, vip: true}: two active
// non-vip documents sharing a code were both accepted through the router, the
// feeder logged the archive refusing the second, and afterwards the fast tier
// held two and the archive one. Nothing warned, here or in `cline indexes`,
// because both names were present and both were unique.
//
// Compared as written rather than for meaning, so {active: true} and
// {active: {$eq: true}} warn when they need not. That is the safe direction: a
// warning costs a look and silence costs the document.
func uniqueIndexNames(ctx context.Context, client *mongo.Client, db, coll string) (map[string]string, error) {
	cur, err := client.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	out := map[string]string{}
	for cur.Next(ctx) {
		if unique, ok := cur.Current.Lookup("unique").BooleanOK(); !ok || !unique {
			continue
		}
		name, ok := cur.Current.Lookup("name").StringValueOK()
		if !ok {
			continue
		}
		enforces := "unique"
		if v, lerr := cur.Current.LookupErr("partialFilterExpression"); lerr == nil {
			enforces += " partial=" + v.String()
		}
		if v, lerr := cur.Current.LookupErr("collation"); lerr == nil {
			enforces += " collation=" + v.String()
		}
		out[name] = enforces
	}
	return out, cur.Err()
}

// collOptions is one collection's options, canonical enough to compare across
// two servers. Missing is a value like any other, and missing on one tier is
// the common drift.
type collOptions map[string]string

// serverOwned are the fields listCollections reports that belong to the server
// rather than to the collection's definition. Two tiers disagreeing about them
// is not drift, it is two servers.
var serverOwned = map[string]bool{"idIndex": true, "info": true, "uuid": true, "type": true}

// optionAdvice is what to do about a difference in a particular option. The
// ones with nothing written here still get reported: the whole point is that a
// list of the options worth checking silently passes every option nobody
// thought of, which is the mistake the create path next door had already made
// once and fixed by inverting its list.
// cappedOption reads the rendered capped option the way mongod reads the flag
// itself: a boolean, or a number that is not zero. The create guard had to
// learn the same latitude, because mongod declares it safeBool and a capped
// collection can be asked for with capped: 1.
func cappedOption(rendered string) bool {
	switch rendered {
	case "", "false", "0":
		return false
	}
	return !strings.Contains(rendered, `"0"`)
}

var optionAdvice = map[string]string{
	"collation": "recreate the fast tier's copy with it, because a collation cannot be added to a " +
		"collection that already holds documents",
	"validator":        "collMod the tier that is behind",
	"validationLevel":  "collMod the tier that is behind",
	"validationAction": "collMod the tier that is behind",
	"changeStreamPreAndPostImages": "collMod the tier that is behind; a client asking for a " +
		"pre-image is refused outright by the tier that does not have it",
}

// answerOptionsIn reads, for one database, every collection option that could
// make the two tiers answer differently.
//
// Everything except the fields the server owns, rather than a list of the
// options this has thought about. That polarity is the whole of it. This used
// to compare exactly two, collation and validator, while the create path beside
// it carried every option there is except a deny-list of the ones a tiered
// collection cannot have. So an option nobody had considered travelled to the
// fast tier and was never checked again: validationAction is one, and it
// decides whether a write is refused or merely logged;
// changeStreamPreAndPostImages is another, and a client asking for a pre-image
// from the tier that does not have it gets an error rather than an event.
func answerOptionsIn(ctx context.Context, client *mongo.Client, db string) (map[string]collOptions, error) {
	cur, err := client.Database(db).ListCollections(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	out := map[string]collOptions{}
	for cur.Next(ctx) {
		name, ok := stringField(cur.Current, "name")
		if !ok {
			continue
		}
		opts := collOptions{}
		if odoc, dok := cur.Current.Lookup("options").DocumentOK(); dok {
			elems, eerr := odoc.Elements()
			if eerr != nil {
				continue
			}
			for _, e := range elems {
				key := e.Key()
				if serverOwned[key] {
					continue
				}
				if key == "collation" {
					// The server fills a collation out with every default it
					// implies, and two tiers agreeing on locale and strength
					// agree for every purpose this warning is about.
					if doc, ddok := e.Value().DocumentOK(); ddok {
						opts[key] = lookupText(doc, "locale") + "/" + lookupText(doc, "strength")
						continue
					}
				}
				opts[key] = e.Value().String()
			}
		}
		out[name] = opts
	}
	return out, cur.Err()
}

// drifted names the options two copies of one collection disagree about, in a
// stable order.
func drifted(cold, hot collOptions) []string {
	seen := map[string]bool{}
	var keys []string
	for _, m := range []collOptions{cold, hot} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	out := keys[:0]
	for _, k := range keys {
		if cold[k] != hot[k] {
			out = append(out, k)
		}
	}
	return out
}

func lookupText(doc bson.Raw, key string) string {
	v, err := doc.LookupErr(key)
	if err != nil {
		return ""
	}
	if s, ok := v.StringValueOK(); ok {
		return s
	}
	if n, ok := intOf(v); ok {
		return strconv.Itoa(n)
	}
	return v.String()
}

// shortOption keeps a log line readable when a validator is a page of schema.
func shortOption(s string) string {
	if s == "" {
		return "none"
	}
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

// warnAboutDefaultConcerns says so when the archive carries a cluster-wide read
// or write concern default that the fast tier does not.
//
// setDefaultRWConcern is a decision about durability and about what a read is
// allowed to see, made once for a whole cluster and then never mentioned again
// by the application: a driver that names no concern gets the cluster's. The
// fast tier is a new replica set and has only MongoDB's implicit defaults, and
// writes and reads through the router are served there.
//
// So an application whose old cluster answered reads at majority reads at local
// through the router, and sees writes that a rollback can still take away. It
// asked for neither and nothing tells it.
//
// Only an explicitly set default counts. Every server reports implicit ones, and
// comparing those would fire on a pair that agrees about everything anybody
// chose.
func (r *Router) warnAboutDefaultConcerns(ctx context.Context) {
	cold, cerr := defaultConcerns(ctx, r.cold)
	if cerr != nil {
		r.log.Debug("could not read the archive's default concerns to compare them", "err", cerr)
		return
	}
	hot, herr := defaultConcerns(ctx, r.hot)
	if herr != nil {
		r.log.Debug("could not read the fast tier's default concerns to compare them", "err", herr)
		return
	}

	for _, d := range concernDrift(cold, hot) {
		r.log.Warn("the archive has a cluster-wide default the fast tier does not",
			"concern", d.kind, "archive", d.archive, "fast_tier", d.fastTier,
			"meaning", "a client that names no "+d.kind+" concern gets the fast tier's, so this "+
				"changed on the day the application was pointed at the router",
			"action", "run setDefaultRWConcern on the fast tier with the same values")
	}
}

// concernDrift is the comparison behind that warning, on its own so it can be
// asked directly.
//
// The end-to-end path needs setDefaultRWConcern, which is cluster-wide: setting
// it to arrange a test changes the durability of every write on that cluster
// for as long as the test runs, and a suite that failed to put it back would
// leave every other suite running against a different guarantee. The decision
// is pure given the two tiers' answers, so it is asked here instead.
//
// Only a default the archive set *globally* counts. getDefaultRWConcern reports
// an implicit default too, and the tiers agreeing on an implicit one is them
// both being untouched rather than a difference anybody made.
func concernDrift(cold, hot tierConcerns) []struct{ kind, archive, fastTier string } {
	var out []struct{ kind, archive, fastTier string }
	for _, d := range []struct{ kind, coldSet, coldVal, hotVal string }{
		{"write", cold.writeSource, cold.write, hot.write},
		{"read", cold.readSource, cold.read, hot.read},
	} {
		if d.coldSet != "global" || d.coldVal == d.hotVal {
			continue
		}
		out = append(out, struct{ kind, archive, fastTier string }{d.kind, d.coldVal, d.hotVal})
	}
	return out
}

type tierConcerns struct {
	write, read             string
	writeSource, readSource string
}

// defaultConcerns reads one tier's cluster-wide defaults, as text, because the
// only thing done with them is comparing and printing.
func defaultConcerns(ctx context.Context, client *mongo.Client) (tierConcerns, error) {
	var raw bson.Raw
	err := client.Database("admin").
		RunCommand(ctx, bson.D{{Key: "getDefaultRWConcern", Value: 1}}).Decode(&raw)
	if err != nil {
		return tierConcerns{}, err
	}
	out := tierConcerns{write: "unset", read: "unset"}
	if v, lerr := raw.LookupErr("defaultWriteConcern"); lerr == nil {
		out.write = writeConcernText(v)
	}
	if v, lerr := raw.LookupErr("defaultReadConcern"); lerr == nil {
		if doc, ok := v.DocumentOK(); ok {
			if lv, lerr := doc.LookupErr("level"); lerr == nil {
				out.read, _ = lv.StringValueOK()
			}
		}
	}
	if v, lerr := raw.LookupErr("defaultWriteConcernSource"); lerr == nil {
		out.writeSource, _ = v.StringValueOK()
	}
	if v, lerr := raw.LookupErr("defaultReadConcernSource"); lerr == nil {
		out.readSource, _ = v.StringValueOK()
	}
	return out, nil
}

// readDeadlineDefault reads the cluster-wide deadline MongoDB 8.0 applies to a
// read that names no maxTimeMS of its own. Zero means unset.
//
// It is a cluster parameter rather than part of getDefaultRWConcern, which is
// exactly why the comparison above does not see it: that one asks the command
// that reports the concerns and this is not reported there. Same family, same
// remedy, different command.
//
// An error is the answer on a server too old to know the parameter, and on an
// account without the privilege to ask. Both tiers run the same release, so an
// old pair fails on both sides and nothing is said, which is right.
func readDeadlineDefault(ctx context.Context, client *mongo.Client) (int64, error) {
	var raw bson.Raw
	err := client.Database("admin").
		RunCommand(ctx, bson.D{{Key: "getClusterParameter", Value: "defaultMaxTimeMS"}}).Decode(&raw)
	if err != nil {
		return 0, err
	}
	arr, aerr := raw.LookupErr("clusterParameters")
	if aerr != nil {
		return 0, nil
	}
	vals, ok := arrayValues(arr)
	if !ok {
		return 0, nil
	}
	for _, v := range vals {
		doc, dok := v.DocumentOK()
		if !dok {
			continue
		}
		ro, rerr := doc.LookupErr("readOperations")
		if rerr != nil {
			continue
		}
		if n, nok := intOf64(ro); nok {
			return n, nil
		}
	}
	return 0, nil
}

// readDeadlineBypassed reports whether a cluster-wide read cap is in force
// anywhere, which is when the warning below is worth making.
//
// Pure, so it can be asked without setting anything: setting this parameter
// changes every read on that cluster for as long as it is set, which is the
// same reason concernDrift is a function rather than an end-to-end check.
func readDeadlineBypassed(cold, hot int64) bool { return cold > 0 || hot > 0 }

// warnAboutReadDeadlines says so when a cluster-wide read cap is set on either
// tier, because it does not bound the reads it is there to bound.
//
// defaultMaxTimeMS caps a read that names no maxTimeMS of its own, and an
// account holding bypassDefaultMaxTimeMS is exempt from it. The built-in root
// role holds that action and readWrite does not. With terminate_auth off a read
// the fast tier answers is proxied with the client's own credentials, so an
// ordinary application account is capped by it. A read that falls through to
// the archive is not proxied: this router performs it on its own connection,
// under its own account, and withRemainingTime deliberately adds no maxTimeMS
// when the caller set none.
//
// So the cap applies to the reads that were already fast and not to the ones
// that go to the archive, which are the slow ones it was set for. Measured on
// this pair with both tiers capped at 5ms, as a user holding readWrite alone,
// running the same 19ms self join on each side:
//
//	a collection the fast tier answers   MaxTimeMSExpired
//	a tiered collection the archive has  3000 documents, no error
//
// Uncapped, both answered 3000.
func (r *Router) warnAboutReadDeadlines(ctx context.Context) {
	cold, cerr := readDeadlineDefault(ctx, r.cold)
	if cerr != nil {
		r.log.Debug("could not read the archive's default read deadline", "err", cerr)
		return
	}
	hot, herr := readDeadlineDefault(ctx, r.hot)
	if herr != nil {
		r.log.Debug("could not read the fast tier's default read deadline", "err", herr)
		return
	}
	if !readDeadlineBypassed(cold, hot) {
		return
	}
	r.log.Warn("a cluster-wide read cap is set, and it does not reach the reads it is for",
		"parameter", "defaultMaxTimeMS.readOperations",
		"archive", cold, "fast_tier", hot,
		"meaning", "a read the fast tier answers carries the client's own credentials and is "+
			"capped by it; a read that falls through to the archive is run by this router under "+
			"its own account, which bypasses the cap if it holds bypassDefaultMaxTimeMS, as root "+
			"does, so the slow reads are the ones it does not bound",
		"action", "set maxTimeMS in the application, which travels and is honoured on both tiers, "+
			"or give the router an account without bypassDefaultMaxTimeMS")
}

// writeConcernText renders a write concern the way it is written in a config,
// rather than as the extended JSON the server reports. An operator reading this
// warning is about to retype it.
func writeConcernText(v bson.RawValue) string {
	doc, ok := v.DocumentOK()
	if !ok {
		return v.String()
	}
	out := ""
	if w, err := doc.LookupErr("w"); err == nil {
		if s, sok := w.StringValueOK(); sok {
			out = "w=" + s
		} else if n, nok := intOf(w); nok {
			out = "w=" + strconv.Itoa(n)
		}
	}
	if j, err := doc.LookupErr("j"); err == nil {
		if on, jok := j.BooleanOK(); jok && on {
			out += " j=true"
		}
	}
	if t, err := doc.LookupErr("wtimeout"); err == nil {
		if n, nok := intOf(t); nok && n > 0 {
			out += " wtimeout=" + strconv.Itoa(n)
		}
	}
	if out == "" {
		return v.String()
	}
	return out
}

// rolesIn lists role@db for every role a tier defines. Only the ones somebody
// made: admin.system.roles holds no built-ins.
func rolesIn(ctx context.Context, client *mongo.Client) (map[string]bool, error) {
	cur, err := client.Database("admin").Collection("system.roles").
		Find(ctx, bson.D{}, options.Find().SetProjection(bson.D{{Key: "role", Value: 1}, {Key: "db", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	out := map[string]bool{}
	for cur.Next(ctx) {
		var role struct {
			Role string `bson:"role"`
			DB   string `bson:"db"`
		}
		if cur.Decode(&role) != nil || role.Role == "" {
			continue
		}
		out[role.Role+"@"+role.DB] = true
	}
	return out, cur.Err()
}

// usersIn lists user@db for every account a tier holds.
func usersIn(ctx context.Context, client *mongo.Client) (map[string]bool, error) {
	cur, err := client.Database("admin").Collection("system.users").
		Find(ctx, bson.D{}, options.Find().SetProjection(bson.D{{Key: "user", Value: 1}, {Key: "db", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	out := map[string]bool{}
	for cur.Next(ctx) {
		var u struct {
			User string `bson:"user"`
			DB   string `bson:"db"`
		}
		if cur.Decode(&u) != nil || u.User == "" {
			continue
		}
		out[u.User+"@"+u.DB] = true
	}
	return out, cur.Err()
}

// ensureFastTierIndexes gives the fast tier the archive's indexes for every
// namespace this router manages, before it accepts a client.
//
// The indexer already runs from the two paths that fill the fast tier from the
// archive: the warmer before it fills a namespace, and the router the first
// time it copies a document up. Neither covers a tiered collection the
// application only ever writes to. Nothing warms it, no read of it falls
// through, so the first thing to create it on the fast tier is the
// application's own insert, and an insert is not a copy-up.
//
// That gap is not merely slow. The router reads the fast tier's unique indexes
// to know which values it has to protect across both tiers, so a fast tier
// that was never given them means neither mongod nor the key registry is
// enforcing one. Measured on an adoption: an archive carrying a unique index it
// had held for years, and through the router a value the archive already held
// and the same new value twice were all three accepted. The fast tier then held
// documents the archive's index refuses, so the feeder could not mirror them,
// and it reopened its change stream from the same checkpoint once a second for
// ever. Mirroring stopped for every namespace behind it, and an order written
// after that never reached the archive at all.
//
// Startup is both the correct moment and the cheapest one. Correct, because a
// unique index built after the duplicate has been accepted is too late and will
// not build; cheapest, because the collection this is protecting is the one the
// fast tier has never held. Before the listener opens, so no client can write
// into the window.
//
// Only namespaces the archive actually holds, which keeps a config naming a
// hundred collections to the one listCollections per database it already does.
func (r *Router) ensureFastTierIndexes(ctx context.Context) {
	byDB := map[string][]string{}
	for _, ns := range r.cfg.Managed {
		if db, _ := splitNS(ns); db != "" {
			byDB[db] = append(byDB[db], ns)
		}
	}
	// `feeder.namespaces` empty tiers everything, and reading the list alone
	// answers "nothing" for it. That is not a quieter start, it is a fast tier
	// left without the archive's unique indexes on the deployment that tiers
	// the most. The archive is the superset, so it is the list.
	everything := len(byDB) == 0
	if everything {
		for _, db := range r.archiveDatabases(ctx) {
			byDB[db] = nil
		}
	}
	for db, nss := range byDB {
		cold, err := collectionsIn(ctx, r.cold, db)
		if err != nil {
			// Nothing to do here that the read paths do not already do: the
			// indexer is still called on the first copy-up, and the warmer
			// still calls it before it fills.
			r.log.Warn("could not list the archive's collections to build its indexes on the fast tier",
				"db", db, "err", err)
			continue
		}
		if everything {
			for name := range cold {
				if !strings.HasPrefix(name, "system.") {
					nss = append(nss, db+"."+name)
				}
			}
		}
		for _, ns := range nss {
			// Only real collections: a view holds no documents, so there is
			// nothing to copy up and no index of its own to build.
			if _, coll := splitNS(ns); cold[coll] == "collection" {
				r.ensureIndexesBounded(ctx, ns)
			}
		}
	}
}

// startupIndexBudget bounds one namespace's index build during startup.
//
// Generous, because the build is real work: on an adoption whose fast tier has
// already been warmed it runs over documents rather than over an empty
// collection. Bounded, because this happens before the listener opens and a
// build that never finishes is a router that never starts, with nothing in the
// log to say why.
const startupIndexBudget = 5 * time.Minute

// ensureIndexesBounded builds one namespace's indexes with a deadline, and says
// what is missing if the deadline is reached.
//
// Startup waits for these on purpose: the router reads the fast tier's unique
// indexes to know what it must protect across the tiers, and one built after a
// duplicate has been accepted is too late. What it must not do is wait for ever.
// Index builds are the one command whose default commit quorum is every voting
// member, so a member that cannot vote used to hang this with no error at all.
// That is fixed where the command is sent, and this is the second half: whatever
// the reason, the wait ends and the reason is named.
//
// It then goes on starting rather than refusing. The window this leaves is the
// one it was already leaving before the fix, and an application in front of a
// proxy that will not start is down; the ERROR says which namespace and what is
// not being enforced, and the build usually completes on its own shortly after.
func (r *Router) ensureIndexesBounded(ctx context.Context, ns string) {
	bctx, cancel := context.WithTimeout(ctx, startupIndexBudget)
	defer cancel()

	done := make(chan struct{})
	r.goSafely("building the archive's indexes on the fast tier at startup", func() {
		defer close(done)
		r.indexer.Ensure(bctx, ns)
	})
	select {
	case <-done:
	case <-bctx.Done():
		if ctx.Err() != nil {
			return
		}
		r.log.Error("gave up waiting for the fast tier's indexes before opening the listener",
			"ns", ns, "waited", startupIndexBudget.String(),
			"effect", "if one of them is unique, neither mongod nor the key registry is enforcing "+
				"that constraint on this namespace until the build finishes",
			"likely", "an index build commits on a majority of the fast tier, so check rs.status() "+
				"there, and currentOp for a createIndexes still running",
			"action", "cline indexes reports what each tier is missing; the router is starting anyway, "+
				"because an application in front of a proxy that will not start is down")
	}
}

// collectionsIn maps one database's collection names to their kind, which is
// "view" for a view and "collection" for everything else.
//
// The kind matters because the advice differs: a collection can be tiered or
// copied whole, and a view can be neither.
func collectionsIn(ctx context.Context, client *mongo.Client, db string) (map[string]string, error) {
	cur, err := client.Database(db).ListCollections(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	out := map[string]string{}
	for cur.Next(ctx) {
		var info struct {
			Name string `bson:"name"`
			Type string `bson:"type"`
		}
		if cur.Decode(&info) != nil || info.Name == "" {
			continue
		}
		kind := info.Type
		if kind == "" {
			kind = "collection"
		}
		out[info.Name] = kind
	}
	return out, cur.Err()
}

// errorReply builds a command failure the driver will surface to the caller.
func (r *Router) errorReply(msg, reply *wire.Message, errmsg string) (*wire.Message, error) {
	return r.commandError(reply.Header.RequestID, msg.Header.RequestID, errmsg)
}

// commandError builds a failed command reply from scratch, so it can also be
// used for a command that was refused before it ever reached the fast tier and
// therefore has no reply to borrow a request id from.
// commandErrorWithCode is commandError with a server code chosen by the caller.
//
// commandError's generic 125 is right for a refusal, and wrong for a deadline:
// MaxTimeMSExpired is the one code whose retry semantics do apply, and a driver
// told CommandFailed cannot tell "your deadline ran out" from "this router will
// not do that".
func (r *Router) commandErrorWithCode(requestID, responseTo int32, errmsg string,
	code int32, codeName string) (*wire.Message, error) {
	out := bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: errmsg},
		{Key: "code", Value: code},
		{Key: "codeName", Value: codeName},
	}
	return wire.NewMsg(requestID, responseTo, out)
}

func (r *Router) commandError(requestID, responseTo int32, errmsg string) (*wire.Message, error) {
	out := bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: errmsg},
		// CommandFailed. Generic on purpose: drivers surface the message, and
		// borrowing a more specific server code would invite retry logic that
		// does not apply here.
		{Key: "code", Value: int32(125)},
		{Key: "codeName", Value: "CommandFailed"},
	}
	return wire.NewMsg(requestID, responseTo, out)
}

// relayedError is a failed command reply for work this router did on the
// client's behalf, when what failed was mongod refusing the client's own query.
//
// commandError's generic 125 is right for a refusal this router makes. It is
// wrong for an error mongod raised: a $merge whose 'on' field is missing failed
// 51132 straight to mongod and 125 through the router, found by the fuzzer on
// 2026-10-07, and a client handling 51132 never saw it. So mongod's code and
// name are kept, with this router's explanation in front of mongod's message.
//
// Not a code that says a member stopped being primary: the client could not
// retry its way past that here, because the read that failed was this router's,
// and passing it on would invite exactly the retry the generic code avoids.
func (r *Router) relayedError(requestID, responseTo int32, errmsg string, err error) (*wire.Message, error) {
	var ce mongo.CommandError
	if errors.As(err, &ce) && ce.Code != 0 && !isStateChangeCode(int(ce.Code)) {
		return r.commandErrorWithCode(requestID, responseTo, errmsg, ce.Code, ce.Name)
	}
	var we mongo.WriteException
	if errors.As(err, &we) && len(we.WriteErrors) > 0 {
		if c := we.WriteErrors[0].Code; c != 0 && !isStateChangeCode(c) {
			return r.commandErrorWithCode(requestID, responseTo, errmsg, int32(c), "")
		}
	}
	return r.commandError(requestID, responseTo, errmsg)
}

// duplicateKeyError is commandError with mongod's own duplicate key code.
//
// For the refusals that are not this router announcing a limitation but this
// router giving the answer the server would have given. A driver that handles a
// duplicate key from mongod handles this one unchanged, which a generic
// CommandFailed would not.
func (r *Router) duplicateKeyError(requestID, responseTo int32, errmsg string) (*wire.Message, error) {
	return wire.NewMsg(requestID, responseTo, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: errmsg},
		{Key: "code", Value: int32(11000)},
		{Key: "codeName", Value: "DuplicateKey"},
	})
}

func splitNS(ns string) (db, coll string) {
	db, coll, _ = strings.Cut(ns, ".")
	return db, coll
}

// withdrawEcho removes the promise that a promotion is coming.
//
// Residency is recorded before the copy-up, which is what lets a change stream
// consumer tell a promotion from an insert the moment the event arrives, and it
// announces an echo at the same time. A document the fast tier turned out to
// already hold produces no insert and therefore no echo, so the announcement has
// to be taken back. Left standing it is spent on whatever write reaches that
// document next, and that mirror is dropped: measured as an updateMany over ten
// documents where the archive kept the old version of five of them.
func (r *Router) withdrawEcho(ctx context.Context, docIDs []string) {
	if len(docIDs) == 0 {
		return
	}
	// Only the echo. promotedAt stays, and clearing it was tried and was wrong:
	// the record may describe a genuine promotion by another path, and taking
	// its timestamp away let every promotion through as an ordinary insert.
	// Measured, in the same round that found the defect this comment is about:
	// 44 events delivered where 9 were right, and nothing hidden at all.
	if err := meta.WithdrawEchoes(ctx, r.cold, docIDs); err != nil {
		r.log.Warn("could not withdraw a promotion notice", "err", err)
	}
}

// isSystemDatabase reports whether a database is the server's own or this
// project's bookkeeping, rather than the application's.
func isSystemDatabase(name string) bool {
	switch name {
	case "admin", "local", "config", meta.DB:
		return true
	}
	return false
}

// warnClaimDimension says once per namespace that a claim was made in a field
// this namespace no longer ages by.
func (r *Router) warnClaimDimension(ns, was, now string) {
	r.dimSaidMu.Lock()
	said := r.dimSaid[ns]
	if r.dimSaid == nil {
		r.dimSaid = map[string]string{}
	}
	r.dimSaid[ns] = was + "\x00" + now
	r.dimSaidMu.Unlock()
	if said == was+"\x00"+now {
		return
	}
	name := func(f string) string {
		if f == "" {
			return "the timestamp in the _id"
		}
		return f
	}
	r.log.Warn("a completeness claim was measured in a field this namespace no longer ages by",
		"ns", ns, "claim_measured_in", name(was), "rules_now_age_by", name(now),
		"meaning", "the claim says the fast tier holds every document newer than an instant, and "+
			"newer means something else now, so the claim has been withdrawn rather than read "+
			"in a dimension it was never made in",
		"effect", "reads of this namespace fall through to the archive until something claims it "+
			"again, which is the answer they would have had before anybody claimed anything",
		"action", "let the warmer claim it again in the new field, or declare it with "+
			"cline watermark once you have checked what the fast tier holds")
}

// rereadArchived replaces docs[i] for each position in left with the archive's
// current copy, and marks one the archive no longer holds live as held, so the
// caller leaves it out. See promote.HoldPresent.
func (r *Router) rereadArchived(ctx context.Context, ns string, docs []bson.Raw, raw []bson.RawValue, left []int, have []bool) error {
	db, coll := splitNS(ns)
	want := make([]bson.RawValue, len(left))
	for j, i := range left {
		want[j] = raw[i]
	}
	found, err := promote.RereadArchived(ctx, r.cold.Database(db).Collection(coll), want)
	if err != nil {
		return err
	}
	for _, i := range left {
		if d, ok := found[promote.RawKey(raw[i])]; ok {
			docs[i] = d
		} else {
			have[i] = true
		}
	}
	return nil
}
