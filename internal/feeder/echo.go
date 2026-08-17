package feeder

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// markerFresh is how recently the warmer must have checked in for its marker to
// count. The warmer beats every 5 seconds, so this tolerates a missed beat.
const markerFresh = 20 * time.Second

// markerCache is how long the feeder trusts its last look at the marker. The
// answer is almost always "no warmer is running", and this keeps that answer
// nearly free on the hot path.
const markerCache = 5 * time.Second

// warmingActive reports whether a warmer is currently copying documents up.
//
// A warm inserts into the fast tier, which produces change events the feeder
// would mirror straight back into the archive those documents came from. That
// echo is harmless but doubles the write load on the cold tier during exactly
// the operation that moves the most data.
//
// Suppressing it costs a lookup per batch, so it is only paid while a warm is
// actually running. The rest of the time this is a cached boolean.
func (f *Feeder) warmingActive(ctx context.Context) bool {
	if time.Since(f.warmCheckedAt) < markerCache {
		return f.warmActive
	}
	f.warmCheckedAt = time.Now()

	var marker meta.ActiveMarker
	err := f.cold.Database(meta.DB).Collection(meta.Markers).
		FindOne(ctx, bson.D{{Key: "_id", Value: meta.WarmerActiveMarker}}).Decode(&marker)
	if err != nil {
		f.warmActive = false
		return false
	}

	active := time.Since(marker.Seen) < markerFresh
	if active != f.warmActive {
		if active {
			f.log.Info("warmer detected, suppressing mirrored copies of warmed documents")
		} else {
			f.log.Info("warmer finished, resuming normal mirroring")
		}
	}
	f.warmActive = active
	return active
}

// suppressWarmEchoes drops buffered upserts that came from the warmer copying
// archived documents back into the fast tier. It reports how many it removed.
func (f *Feeder) suppressWarmEchoes(ctx context.Context) (int, error) {
	if !f.warmingActive(ctx) {
		return 0, nil
	}

	var ids []string
	for _, op := range f.pending {
		if op.kind == actionUpsert {
			ids = append(ids, meta.DocID(op.ns, op.id))
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}

	cur, err := f.cold.Database(meta.DB).Collection(meta.Warming).Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		// Not fatal. Failing to suppress costs redundant writes, and stopping
		// mirroring over a bookkeeping query would be far worse.
		f.log.Warn("could not read warming notices, mirroring them anyway", "err", err)
		return 0, nil
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	announced := make(map[string]struct{}, len(ids))
	for cur.Next(ctx) {
		var rec struct {
			ID string `bson:"_id"`
		}
		if err := cur.Decode(&rec); err == nil {
			announced[rec.ID] = struct{}{}
		}
	}

	var dropped int
	for i, op := range f.pending {
		if op.kind != actionUpsert {
			continue
		}
		if _, ok := announced[meta.DocID(op.ns, op.id)]; ok {
			f.pending[i].kind = actionSkip
			dropped++
		}
	}
	return dropped, nil
}

// The router's promotions echo too, and until now nothing stopped them.
//
// A copy-up inserts an archived document into the fast tier, which is a change
// event like any other, so the feeder mirrored it straight back into the archive
// it had just been read from. The document is identical, so nothing was wrong
// with the result; what it cost was a write to the cold tier for every document
// the fast tier ever had to fetch, which is write amplification precisely where
// a read-heavy workload over archived data lands.
//
// It was not always harmless. While a promotion was written as $setOnInsert, the
// document arrived in the fast tier with its fields reordered, and this echo is
// what carried that reordered copy back down and made it permanent.
//
// Suppressing it safely is the whole difficulty, because the obvious version
// loses data. Dropping every mirror for a document that was recently promoted
// would also drop the mirror of a genuine insert, if the application deleted
// that document and created it again in the window. The archive would then keep
// the tombstone and the new document would never reach it, which is silent loss
// of exactly the kind this project exists to prevent.
//
// So the suppression is one-shot and it is announced in advance. The router
// writes echoPending on the residency record before it inserts, the same
// ordering it already uses so a change stream consumer can tell a promotion from
// an insert. The feeder suppresses one mirror per pending flag and clears the
// flag as it does, so the next insert of that document mirrors normally however
// soon it arrives. Change events arrive in order, so the flag is consumed by the
// insert it was written for.

// promotingActive reports whether the router has copied anything up recently.
//
// Same shape as warmingActive and for the same reason: the answer is almost
// always no, and this keeps the cost of asking near zero on the hot path.
func (f *Feeder) promotingActive(ctx context.Context) bool {
	if time.Since(f.promoteCheckedAt) < markerCache {
		return f.promoteActive
	}
	f.promoteCheckedAt = time.Now()

	var marker meta.ActiveMarker
	err := f.cold.Database(meta.DB).Collection(meta.Markers).
		FindOne(ctx, bson.D{{Key: "_id", Value: meta.PromotingActiveMarker}}).Decode(&marker)
	if err != nil {
		f.promoteActive = false
		return false
	}

	active := time.Since(marker.Seen) < markerFresh
	if active != f.promoteActive {
		if active {
			f.log.Debug("router is promoting documents, suppressing the mirrored copies")
		} else {
			f.log.Debug("router has stopped promoting, resuming normal mirroring")
		}
	}
	f.promoteActive = active
	return active
}

// suppressPromotionEchoes drops buffered upserts that are the router's own
// copy-ups coming back. It reports how many it removed.
func (f *Feeder) suppressPromotionEchoes(ctx context.Context) (int, error) {
	if !f.promotingActive(ctx) {
		return 0, nil
	}

	var ids []string
	for _, op := range f.pending {
		if op.kind == actionUpsert {
			ids = append(ids, meta.DocID(op.ns, op.id))
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}

	coll := f.cold.Database(meta.DB).Collection(meta.Residency)
	cur, err := coll.Find(ctx,
		bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
			{Key: "echoPending", Value: true},
		},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		f.log.Warn("could not read promotion notices, mirroring them anyway", "err", err)
		return 0, nil
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var pending []string
	for cur.Next(ctx) {
		var rec struct {
			ID string `bson:"_id"`
		}
		if derr := cur.Decode(&rec); derr == nil {
			pending = append(pending, rec.ID)
		}
	}
	if len(pending) == 0 {
		return 0, nil
	}

	// Clear the flags before dropping anything, and drop nothing if the clear
	// fails. Suppressing a mirror while the flag survives would spend that flag
	// on a later genuine insert of the same document, which is the one outcome
	// worth avoiding: the archive would keep an older version and nothing would
	// say so. Failing the other way costs a redundant write.
	res, err := coll.UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: pending}}}},
		bson.D{{Key: "$unset", Value: bson.D{{Key: "echoPending", Value: ""}}}})
	if err != nil || res == nil {
		f.log.Warn("could not clear promotion notices, mirroring them anyway", "err", err)
		return 0, nil
	}

	claimed := make(map[string]struct{}, len(pending))
	for _, id := range pending {
		claimed[id] = struct{}{}
	}

	// One flag, one mirror, and the flag is spent on the first upsert for that
	// document rather than on every upsert for it.
	//
	// This is not a detail. A promotion, an application delete and an
	// application insert of the same id can all land in one batch, and dropping
	// every upsert matching the id then swallows the genuine insert as well as
	// the echo. The archive keeps the old document with a tombstone on it while
	// the application has been told the new one is stored. Measured before the
	// fix, in testecho.sh: the archive held v:"old", tombstoned, after v:"new"
	// had been written and acknowledged.
	//
	// Change events arrive in order, so the first upsert for the id is the
	// promotion the flag was written for, and everything after it is real work.
	kept := f.pending[:0]
	dropped := 0
	for _, op := range f.pending {
		if op.kind == actionUpsert {
			id := meta.DocID(op.ns, op.id)
			if _, ok := claimed[id]; ok {
				delete(claimed, id)
				dropped++
				continue
			}
		}
		kept = append(kept, op)
	}
	f.pending = kept
	return dropped, nil
}
