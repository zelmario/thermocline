package feeder

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmar/thermocline/internal/meta"
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
