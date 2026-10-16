package feeder

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// undoResurrections removes from the fast tier the documents a copy-up put back
// after the application had deleted them.
//
// A copy-up reads the archive and then inserts what it read. A delete that
// lands between the two, from another connection or another router, removes
// the fast tier's copy and is mirrored as a tombstone, and then the insert puts
// the archived copy back: the fast tier holds a live document the application
// deleted and the archive holds it tombstoned. The router answers it from the
// fast tier from then on, the evictor never takes it, because an archived copy
// that is tombstoned is not one it will evict against, and nothing counts it.
// Measured on 2026-09-30 with a transaction warming 3,000 of one customer's
// orders through one router and one order deleted through the other: 12 of 12
// rounds with the delete inside the first 400ms, 4 of 4 up to 1.6s, and
// stale_reads 0 every time.
//
// The router cannot close that window on its own. It waits for mirroring before
// it reads, which covers a delete already acknowledged, and it could check again
// before inserting, which only narrows the window. What closes it is order, and
// the feeder is the one process that sees both writes in the fast tier's order.
// A copy-up only ever reads a document the archive holds live, so if the archive
// holds the echo's document tombstoned once every event up to and including the
// echo has been applied, an application delete came in between: the copy-up
// read before the tombstone and inserted after the delete.
//
// The same holds whoever made the copy, the router's point copy-ups, its
// copy-ups for writes and transactions, and the warmer's, because all of them
// reach the feeder as the echoes suppressed above.
//
// The removal is announced as an eviction, because it is one in every sense a
// reader cares about: the document leaves the fast tier and the archive's
// record stays as it is. The feeder reads its own delete as announced and
// writes nothing, and the router's sieve hides it from change streams, which
// never showed the echo either, so a client watching the collection saw the
// application's delete and nothing after it.
//
// Each delete insists on the exact document the echo carried, under the simple
// collation, for the reason the evictor gives: a document changed since then is
// not the copy-up's any more. That leaves one case open and it is written down
// in docs/limitations.md: a write that changes the resurrected document before
// this runs makes it the application's, and it is mirrored live.
//
// Best effort, and a failure is carried to the next flush rather than returned.
// Returning it would make the flush run again with these echoes' notices
// already spent, and the second run would mirror them into the archive and
// undo the tombstone as well.
func (f *Feeder) undoResurrections(ctx context.Context, last map[string]bson.Timestamp) {
	// Only an echo nothing came after in this flush. A delete after the echo is
	// the application deleting what the copy-up put there, which leaves the
	// fast tier without it and the archive tombstoned exactly as a resurrection
	// does, and a repair for it is not only wasted: its announcement, taken
	// back when its delete matches nothing, left a row the router's sieve read
	// as an eviction, and the application's delete went missing from its own
	// change stream. testchange caught it twice. Anything else after the echo
	// makes the document the application's, as the comment above says.
	var fresh []pendingOp
	for _, op := range f.echoes {
		if at, ok := last[meta.DocID(op.ns, op.id)]; ok && before(op.at, at) {
			continue
		}
		fresh = append(fresh, op)
	}
	todo := append(f.unresurrected, fresh...)
	f.unresurrected = nil
	f.echoes = f.echoes[:0]
	if len(todo) == 0 {
		return
	}

	// The last echo for each document, since that is the copy the fast tier can
	// still hold. One flush can carry a copy-up, an eviction and another copy-up
	// of the same _id.
	byNS := map[string]map[string]pendingOp{}
	for _, op := range todo {
		if byNS[op.ns] == nil {
			byNS[op.ns] = map[string]pendingOp{}
		}
		id := meta.DocID(op.ns, op.id)
		if prev, ok := byNS[op.ns][id]; ok && before(op.at, prev.at) {
			continue
		}
		byNS[op.ns][id] = op
	}

	for ns, ops := range byNS {
		ids := make([]bson.RawValue, 0, len(ops))
		for _, op := range ops {
			ids = append(ids, op.id)
		}
		db, coll := splitNS(ns)
		cur, err := f.cold.Database(db).Collection(coll).Find(ctx,
			bson.D{
				{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
				{Key: meta.TombstoneField, Value: bson.D{{Key: "$exists", Value: true}}},
			},
			options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
		if err != nil {
			f.log.Warn("could not ask the archive whether a copy-up put back a deleted document",
				"ns", ns, "documents", len(ids), "err", err,
				"effect", "asked again on the next flush")
			for _, op := range ops {
				f.unresurrected = append(f.unresurrected, op)
			}
			continue
		}
		var dead []pendingOp
		for cur.Next(ctx) {
			id, lerr := cur.Current.LookupErr("_id")
			if lerr != nil {
				continue
			}
			if op, ok := ops[meta.DocID(ns, id)]; ok {
				dead = append(dead, op)
			}
		}
		cerr := cur.Err()
		_ = cur.Close(context.WithoutCancel(ctx))
		if cerr != nil {
			f.log.Warn("could not ask the archive whether a copy-up put back a deleted document",
				"ns", ns, "documents", len(ids), "err", cerr,
				"effect", "asked again on the next flush")
			for _, op := range ops {
				f.unresurrected = append(f.unresurrected, op)
			}
			continue
		}
		for _, op := range dead {
			if !f.removeResurrected(ctx, op) {
				f.unresurrected = append(f.unresurrected, op)
			}
		}
	}
}

// resurrectGrace bounds removeResurrected once the feeder is stopping.
const resurrectGrace = 30 * time.Second

// removeResurrected takes one resurrected document off the fast tier, and
// reports whether it is done with it, which includes finding it already gone.
//
// From the announcement to the delete it runs on a context a shutdown does not
// cancel, for the reason the evictor's steps do: an announcement cut off from
// its delete stands for an hour, and the evictor's stood 3,000 times in twelve
// SIGTERMs before its steps did the same. See evictor.stepContext.
func (f *Feeder) removeResurrected(ctx context.Context, op pendingOp) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), resurrectGrace)
	defer cancel()
	docID := meta.DocID(op.ns, op.id)
	if err := meta.AnnounceEviction(ctx, f.cold, []string{docID}); err != nil {
		f.log.Warn("could not announce the removal of a document a copy-up put back",
			"ns", op.ns, "err", err, "effect", "tried again on the next flush")
		return false
	}
	db, coll := splitNS(op.ns)
	res, err := f.hot.Database(db).Collection(coll).DeleteOne(ctx,
		bson.D{
			{Key: "_id", Value: op.id},
			{Key: "$expr", Value: bson.D{{Key: "$eq", Value: bson.A{"$$ROOT", bson.D{{Key: "$literal", Value: op.doc}}}}}},
		},
		options.DeleteOne().SetCollation(&options.Collation{Locale: "simple"}))
	if err != nil {
		// Taken back. The next flush tries again and announces again, so one
		// left standing here was one more than the deletes coming: two
		// announcements for the retry's one delete, and the spare read the
		// application's next delete of this _id as an eviction.
		//
		// The delete may have landed despite the error. Then it reaches the
		// feeder unannounced and reads as the application's, which tombstones
		// the archive's copy, and that copy is already a tombstone: it is why
		// this document is being removed at all.
		if werr := meta.WithdrawEviction(ctx, f.cold, []string{docID}); werr != nil {
			f.log.Warn("could not withdraw an eviction announcement nothing will answer",
				"ns", op.ns, "err", werr,
				"effect", "an application delete of this _id within the hour may be read as an eviction")
		}
		f.log.Warn("could not remove a document a copy-up put back",
			"ns", op.ns, "err", err, "effect", "tried again on the next flush")
		return false
	}
	if res.DeletedCount == 0 {
		// Gone already, or changed since. Either way no delete of ours will
		// reach the stream, so the announcement is taken back: left, it would
		// make the application's next delete of this _id look like an eviction
		// and the archive would keep what the application removed.
		if werr := meta.WithdrawEviction(ctx, f.cold, []string{docID}); werr != nil {
			f.log.Warn("could not withdraw an eviction announcement nothing will answer",
				"ns", op.ns, "err", werr,
				"effect", "an application delete of this _id within the hour may be read as an eviction")
		}
		return true
	}
	f.counters.resurrectionsUndone++
	f.log.Warn("a copy-up put back a document the application had deleted, removed it again",
		"ns", op.ns, "id", op.id.String(),
		"cause", "the copy-up read the archive before the delete and inserted after it",
		"total", f.counters.resurrectionsUndone)
	return true
}

// lastPositions is where the last buffered event for each document sits in the
// fast tier's oplog, whatever kind it is.
func (f *Feeder) lastPositions() map[string]bson.Timestamp {
	out := make(map[string]bson.Timestamp, len(f.pending))
	for _, op := range f.pending {
		id := meta.DocID(op.ns, op.id)
		if at, ok := out[id]; !ok || before(at, op.at) {
			out[id] = op.at
		}
	}
	return out
}

// before reports whether oplog position a comes before b.
func before(a, b bson.Timestamp) bool {
	return a.T < b.T || (a.T == b.T && a.I < b.I)
}
