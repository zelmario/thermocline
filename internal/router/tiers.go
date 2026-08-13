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

	"github.com/zelmar/thermocline/internal/meta"
	"github.com/zelmar/thermocline/internal/wire"
)

// readArchived fetches one document from the archive.
//
// Tombstoned documents are excluded. The archive keeps records the application
// deleted, and returning one here would resurrect it in the caller's hands.
func (r *Router) readArchived(ctx context.Context, ns string, id bson.RawValue) (bson.Raw, bool, error) {
	db, coll := splitNS(ns)
	var doc bson.Raw
	err := r.cold.Database(db).Collection(coll).FindOne(ctx, bson.D{
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
	doc, found, err := r.readArchived(ctx, ns, id)
	if err != nil || !found {
		return false, err
	}

	db, coll := splitNS(ns)
	// $setOnInsert, never a replace. If the fast tier gained this document
	// while we were reading the archive, its copy is the newer one.
	_, err = r.hot.Database(db).Collection(coll).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$setOnInsert", Value: doc}},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return false, fmt.Errorf("copy up into %s: %w", ns, err)
	}

	// The document is back in the fast tier, so any eviction notice for it is
	// now false. Leaving it would make the next application delete look like
	// another eviction, and the archive would never tombstone it.
	if err := meta.ClearEvictionNotices(ctx, r.cold, []string{meta.DocID(ns, id)}); err != nil {
		r.log.Warn("copied up but could not clear the eviction notice", "ns", ns, "err", err)
	}
	if err := r.recordResidency(ctx, ns, id); err != nil {
		r.log.Warn("copied up but could not record residency", "ns", ns, "err", err)
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
	from    time.Time
	known   bool
	fetched time.Time
}

// completenessTTL is how long a watermark reading is reused. It changes only
// when the warmer or evictor runs, so a few seconds of staleness is harmless,
// and reading it per query would put a round trip on the hot path.
const completenessTTL = 5 * time.Second

// completeFrom returns how far back the fast tier is complete for ns.
func (r *Router) completeFrom(ctx context.Context, ns string) (time.Time, bool) {
	r.wmCache.mu.RLock()
	e, ok := r.wmCache.entries[ns]
	r.wmCache.mu.RUnlock()
	if ok && time.Since(e.fetched) < completenessTTL {
		return e.from, e.known
	}

	from, err := r.marks.Load(ctx, ns)
	entry := watermarkEntry{from: from, known: err == nil, fetched: time.Now()}
	if err != nil {
		r.log.Warn("could not read the completeness watermark", "ns", ns, "err", err)
	}

	r.wmCache.mu.Lock()
	if r.wmCache.entries == nil {
		r.wmCache.entries = make(map[string]watermarkEntry)
	}
	r.wmCache.entries[ns] = entry
	r.wmCache.mu.Unlock()

	return entry.from, entry.known
}

// warnAboutMissingWatermarks says out loud what a missing watermark means.
//
// No watermark is read as "nothing has been removed, so the fast tier holds
// everything". That is right for a fast tier that was the original cluster, and
// wrong for one that was built empty, where it would claim completeness the
// tier does not have.
func (r *Router) warnAboutMissingWatermarks(ctx context.Context) {
	cur, err := r.cold.Database(meta.DB).Collection(meta.Watermarks).
		Find(ctx, bson.D{}, options.Find().SetLimit(1))
	if err != nil {
		return
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	if !cur.Next(ctx) {
		r.log.Warn("no completeness watermarks recorded",
			"meaning", "every namespace is treated as fully present in the fast tier",
			"action", "if the fast tier was built empty, run the warmer before trusting range queries")
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
	out := bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: errmsg},
		// CommandFailed. Generic on purpose: drivers surface the message, and
		// borrowing a more specific server code would invite retry logic that
		// does not apply here.
		{Key: "code", Value: int32(125)},
		{Key: "codeName", Value: "CommandFailed"},
	}
	return wire.NewMsg(reply.Header.RequestID, msg.Header.RequestID, out)
}

func splitNS(ns string) (db, coll string) {
	db, coll, _ = strings.Cut(ns, ".")
	return db, coll
}
