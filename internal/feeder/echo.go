package feeder

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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

	// The archive's frame, because the warmer stamped the mark there and this
	// process need not be on the warmer's machine. Ahead of it by more than
	// markerFresh and every warm reads as "no warmer is running", so nothing is
	// suppressed. See meta.HeartbeatUpdate.
	active := f.archiveNow(ctx).Sub(marker.Seen) < markerFresh
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
//
// Everything the promotion path below had to learn applies here, and this had
// learned none of it. A notice matched any upsert rather than only an insert,
// and it was never spent, so while a warm was running it swallowed every mirror
// for a document the warmer had announced, not just the one copy-up it was
// written for.
//
// Measured on MongoDB's sample datasets adopted as an archive, on the shape
// that produces it exactly: warm sample_mflix.comments, then have a client
// update a year of them through the router, 868 documents of which the fast
// tier held the 320 the warm had just copied. All 868 were modified and the
// fast tier held all 868 marked. The archive held 548, and it stayed at 548:
// the 320 updates to warmed documents were dropped on the way and nothing said
// so. The router then answered the client 548, because a range straddling the
// watermark is read from the archive, so the write was acknowledged, present on
// one tier, invisible through the proxy and absent from the durable one. An
// eviction would have finished the job.
//
// A warm copies a document up as an insert, always, so an update or a replace
// can never be one. That is the same reasoning already written down below, and
// the same one-shot discipline follows: spend the notice, and drop nothing if
// spending it fails.
func (f *Feeder) suppressWarmEchoes(ctx context.Context) (int, error) {
	if !f.warmingActive(ctx) {
		return 0, nil
	}

	var ids []string
	// Where each warm's insert sits in the fast tier's oplog, so the residency
	// record can say afterwards which insert it accounted for. The first upsert
	// for a document is the one the notice is spent on, so the first position is
	// the one to keep; see the loop at the end of this function.
	posOf := map[string]bson.Timestamp{}
	for _, op := range f.pending {
		if op.kind == actionUpsert && op.wasInsert {
			id := meta.DocID(op.ns, op.id)
			ids = append(ids, id)
			if _, seen := posOf[id]; !seen {
				posOf[id] = op.at
			}
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}

	// Bounded in time for the reason the promotion notices are: a notice whose
	// copy-up never arrived would otherwise sit waiting for any later insert of
	// the same document and suppress that instead. A warm's own echo arrives
	// well inside this.
	coll := f.cold.Database(meta.DB).Collection(meta.Warming)
	cur, err := coll.Find(ctx,
		bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
			// In the archive's frame, because that is where the warmer's
			// announcement was stamped and this process need not be on the
			// warmer's machine. Ahead of it by more than this window and no
			// notice is ever found, so every warmed document is mirrored back
			// into the archive it came from and reaches a client's change
			// stream as an insert it never made. See f.clockSkew.
			{Key: "at", Value: bson.D{{Key: "$gte", Value: f.archiveNow(ctx).Add(-echoWindow)}}},
		},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		// Not fatal. Failing to suppress costs redundant writes, and stopping
		// mirroring over a bookkeeping query would be far worse.
		f.log.Warn("could not read warming notices, mirroring them anyway", "err", err)
		return 0, nil
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var announced []string
	for cur.Next(ctx) {
		var rec struct {
			ID string `bson:"_id"`
		}
		if err := cur.Decode(&rec); err == nil {
			announced = append(announced, rec.ID)
		}
	}
	if len(announced) == 0 {
		return 0, nil
	}

	// Spend the notices before dropping anything, and drop nothing if that
	// fails. Suppressing a mirror while the notice survives would spend it on a
	// later genuine write to the same document, which is the outcome this whole
	// mechanism exists to avoid.
	if _, derr := coll.DeleteMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: announced}}}}); derr != nil {
		f.log.Warn("could not clear warming notices, mirroring them anyway", "err", derr)
		return 0, nil
	}

	// The warm is now accounted for on the residency record as well, which is
	// where the router looks when it decides whether an insert it is handing to
	// a client was this system's own work. Spending the notice alone left the
	// record saying an echo was still expected, so a warm read late fell back
	// to a window instead of being answered exactly.
	if rerr := f.spendResidencyEchoes(ctx, announced, posOf); rerr != nil {
		f.log.Warn("could not record the warmed documents on their residency records",
			"err", rerr,
			"effect", "a client reading its change stream late may see these warms as inserts")
	}

	claimed := make(map[string]struct{}, len(announced))
	for _, id := range announced {
		claimed[id] = struct{}{}
	}

	// One notice, one mirror. Change events arrive in order, so the first
	// insert of that document is the copy-up the notice was written for and
	// everything after it is the application's own work.
	var dropped int
	for i, op := range f.pending {
		if op.kind != actionUpsert || !op.wasInsert {
			continue
		}
		id := meta.DocID(op.ns, op.id)
		if _, ok := claimed[id]; ok {
			delete(claimed, id)
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

// echoWindow is how long a promotion notice is honoured. Past it, the echo it
// described has either been mirrored already or is never coming.
const echoWindow = 60 * time.Second

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

	// The archive's frame: the mark was stamped there and this process need not
	// be on the router's machine. See warmingActive above.
	active := f.archiveNow(ctx).Sub(marker.Seen) < markerFresh
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

	// Inserts only. A promotion is an insert, always, so an update or a replace
	// can never be one, and letting them be considered is what allowed a notice
	// left behind by a copy-up that inserted nothing to swallow an ordinary
	// update's mirror instead.
	var ids []string
	// Where each promotion's insert sits in the fast tier's oplog, so the record
	// can say afterwards which insert it accounted for. The first upsert for a
	// document is the one the flag is spent on, so the first position is the one
	// to keep; see the loop at the end of this function.
	posOf := map[string]bson.Timestamp{}
	for _, op := range f.pending {
		if op.kind == actionUpsert && op.wasInsert {
			id := meta.DocID(op.ns, op.id)
			ids = append(ids, id)
			if _, seen := posOf[id]; !seen {
				posOf[id] = op.at
			}
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}

	// Notices expire, because some of them are never consumed. The feeder caches
	// its view of whether anything is being promoted for a few seconds, so an
	// echo arriving inside a stale "no" is mirrored rather than suppressed, and
	// its notice is left standing. That costs one redundant write, which is the
	// trade the cache exists to make.
	//
	// What must not happen is that notice waiting indefinitely for an insert of
	// the same document, which it would then suppress. Bounding it means a
	// leftover can only affect writes in the seconds after the promotion, and a
	// promotion's own echo arrives well inside that.
	coll := f.cold.Database(meta.DB).Collection(meta.Residency)
	cur, err := coll.Find(ctx,
		bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
			{Key: "echoPending", Value: true},
			{Key: "promotedAt", Value: bson.D{{Key: "$gte", Value: time.Now().UTC().Add(-echoWindow)}}},
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
	//
	// The position of the insert goes down in the same write as the flag comes
	// off, because the two are one fact: this promotion caused that insert. A
	// router handing the event over later reads it back and answers exactly,
	// rather than comparing the event's time against a promotedAt that may
	// describe a different promotion of the same document. See
	// meta.ResidencyRecord.Echoed.
	if err := f.spendResidencyEchoes(ctx, pending, posOf); err != nil {
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
		if op.kind == actionUpsert && op.wasInsert {
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

// spendResidencyEchoes takes the echo off these residency records and writes
// down which insert each one was spent on.
//
// Shared by both suppressors, because a warm and a copy-up are the same event to
// anybody reading a change stream: this system putting a document on the fast
// tier that the archive already held. The router has one rule for recognising
// them and therefore needs one place to read.
//
// The position rather than a clock, for the reason the eviction notice's spent
// list gives: the feeder and the router read the same field off the same event,
// so the values compare by construction.
func (f *Feeder) spendResidencyEchoes(ctx context.Context, ids []string, posOf map[string]bson.Timestamp) error {
	if len(ids) == 0 {
		return nil
	}
	coll := f.cold.Database(meta.DB).Collection(meta.Residency)
	models := make([]mongo.WriteModel, 0, len(ids))
	for _, id := range ids {
		update := bson.D{{Key: "$unset", Value: bson.D{{Key: "echoPending", Value: ""}}}}
		// A server old enough not to send a cluster time leaves nothing exact to
		// record. The flag still comes off, and the router falls back to the
		// window as it did before.
		if pos, ok := posOf[id]; ok && (pos.T != 0 || pos.I != 0) {
			update = append(update, bson.E{Key: "$push", Value: bson.D{
				{Key: "echoed", Value: bson.D{
					{Key: "$each", Value: []bson.Timestamp{pos}},
					{Key: "$slice", Value: -meta.MaxEchoRecorded},
				}},
			}})
		}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: id}}).
			SetUpdate(update))
	}
	// Unordered: these are independent records, and one that cannot be written
	// must not hold up the rest.
	_, err := coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}
