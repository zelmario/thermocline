package router

import (
	"context"
	"errors"
	"fmt"
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
	if err := r.recordResidency(ctx, ns, id); err != nil {
		r.log.Warn("could not record residency before copying up", "ns", ns, "err", err)
	}
	r.notePromoting(ctx)

	db, coll := splitNS(ns)
	// An insert, never a replace and never an upsert built from modifiers. If
	// the fast tier gained this document while we were reading the archive, its
	// copy is the newer one and the duplicate key says so. See package promote
	// for why this is not written as $setOnInsert.
	if _, err = promote.One(ctx, r.hot.Database(db).Collection(coll), doc); err != nil {
		return false, fmt.Errorf("copy up into %s: %w", ns, err)
	}

	// The document is back in the fast tier, so any eviction notice for it is
	// now false. Leaving it would make the next application delete look like
	// another eviction, and the archive would never tombstone it.
	if err := meta.ClearEvictionNotices(ctx, r.cold, []string{meta.DocID(ns, id)}); err != nil {
		r.log.Warn("copied up but could not clear the eviction notice", "ns", ns, "err", err)
	}
	r.Stats.CopiedUp.Add(1)
	return true, nil
}

// copyUpAsync copies a document up behind the reply that already went out.
func (r *Router) copyUpAsync(ns string, id bson.RawValue) {
	// Detached from the client's context on purpose: the reply has been sent,
	// and the client hanging up must not abandon a half-finished copy.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := r.copyUp(ctx, ns, id); err != nil {
			r.log.Warn("copy up failed", "ns", ns, "err", err)
		}
	}()
}

// recordResidency keeps a copied-up document in the fast tier for a while.
// Without it the next eviction sweep removes what was just fetched, and the
// next read pays for the archive again.
func (r *Router) recordResidency(ctx context.Context, ns string, id bson.RawValue) error {
	docID := meta.DocID(ns, id)
	until := time.Now().UTC().Add(r.cfg.Residency.D())
	_, err := r.cold.Database(meta.DB).Collection(meta.Residency).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: docID}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "ns", Value: ns},
			{Key: "doc", Value: id},
			{Key: "hotUntil", Value: until},
			{Key: "promotedAt", Value: time.Now().UTC()},
			// The insert this record precedes is about to appear on the feeder's
			// change stream, and mirroring it would write the archive's own
			// document back into the archive. This says one is expected, and the
			// feeder clears it as it suppresses, so only that one insert is
			// suppressed and a later genuine one is not. See promotion echoes in
			// the feeder.
			{Key: "echoPending", Value: true},
		}}},
		options.UpdateOne().SetUpsert(true))
	return err
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
}

// completenessTTL is how long a watermark reading is reused. It changes only
// when the warmer or evictor runs, so a few seconds of staleness is harmless,
// and reading it per query would put a round trip on the hot path.
const completenessTTL = 5 * time.Second

// completeness returns what the fast tier claims to hold for ns, and whether
// the claim could be read at all.
//
// A read failure is reported as unknown rather than folded into an unclaimed
// Mark. Both refuse in strict mode, but they need different words: one is "warm
// this namespace", the other is "the cold tier is unreachable".
func (r *Router) completeness(ctx context.Context, ns string) (watermark.Mark, bool) {
	r.wmCache.mu.RLock()
	e, ok := r.wmCache.entries[ns]
	r.wmCache.mu.RUnlock()
	if ok && time.Since(e.fetched) < completenessTTL {
		return e.mark, e.known
	}

	mark, err := r.marks.Load(ctx, ns)
	entry := watermarkEntry{mark: mark, known: err == nil, fetched: time.Now()}
	if err != nil {
		r.log.Warn("could not read the completeness watermark", "ns", ns, "err", err)
	}

	r.wmCache.mu.Lock()
	if r.wmCache.entries == nil {
		r.wmCache.entries = make(map[string]watermarkEntry)
	}
	r.wmCache.entries[ns] = entry
	r.wmCache.mu.Unlock()

	return entry.mark, entry.known
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
	cur, err := r.cold.Database(meta.DB).Collection(meta.Watermarks).
		Find(ctx, bson.D{}, options.Find().SetLimit(1))
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

// singleDocReply builds a find reply carrying one document fetched from the
// archive, shaped exactly as the fast tier would have shaped it.
func (r *Router) singleDocReply(msg, reply *wire.Message, ns string, doc bson.Raw) (*wire.Message, error) {
	var v bson.D
	if err := bson.Unmarshal(doc, &v); err != nil {
		return nil, fmt.Errorf("decode archived document: %w", err)
	}

	out := bson.D{
		{Key: "cursor", Value: bson.D{
			// Cursor id zero means the results are exhausted, so the driver
			// never issues a getMore that this proxy could not answer.
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: ns},
			{Key: "firstBatch", Value: bson.A{v}},
		}},
		{Key: "ok", Value: 1.0},
	}
	return wire.NewMsg(reply.Header.RequestID, msg.Header.RequestID, out)
}

// errorReply builds a command failure the driver will surface to the caller.
func (r *Router) errorReply(msg, reply *wire.Message, errmsg string) (*wire.Message, error) {
	return r.commandError(reply.Header.RequestID, msg.Header.RequestID, errmsg)
}

// commandError builds a failed command reply from scratch, so it can also be
// used for a command that was refused before it ever reached the fast tier and
// therefore has no reply to borrow a request id from.
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

func splitNS(ns string) (db, coll string) {
	db, coll, _ = strings.Cut(ns, ".")
	return db, coll
}

// notePromoting tells the feeder that copy-ups are happening, so it starts
// looking for the echoes they produce.
//
// Throttled, because the marker is a heartbeat and not a count: writing one per
// promotion would add a cold tier write to the very path this exists to make
// cheaper. The feeder treats a marker it has not seen move for a while as
// nobody promoting, so this only has to beat often enough to stay inside that
// window.
func (r *Router) notePromoting(ctx context.Context) {
	now := time.Now()
	r.promoteMu.Lock()
	if now.Sub(r.promotedAt) < promoteBeat {
		r.promoteMu.Unlock()
		return
	}
	r.promotedAt = now
	r.promoteMu.Unlock()

	_, err := r.cold.Database(meta.DB).Collection(meta.Markers).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: meta.PromotingActiveMarker}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: now.UTC()}}}},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		// Not fatal. Failing to announce costs the redundant mirror writes this
		// was going to save, and stopping a copy-up over a heartbeat would cost
		// the client its answer.
		r.log.Warn("could not announce that documents are being promoted", "err", err)
	}
}

// promoteBeat is how often the promotion heartbeat is refreshed.
const promoteBeat = 3 * time.Second
