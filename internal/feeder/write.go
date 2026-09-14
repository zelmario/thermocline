package feeder

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/health"
	"github.com/zelmario/thermocline/internal/lease"
	"github.com/zelmario/thermocline/internal/meta"
)

// markCaughtUp records that the fast tier has nothing left to mirror.
//
// Lag is measured from the last event applied, which only moves when there is
// an event. On a database that goes quiet, nights and weekends included, that
// timestamp stops moving while the clock does not, so lag climbs without bound
// even though the archive is perfectly up to date. The evictor reads that lag,
// fails closed, and stops evicting for good. It looks exactly like a hang.
//
// An empty change stream poll means everything written so far has been applied,
// so the honest lag at that moment is zero.
// The cluster time comes from the poll's own resume token, which is the whole
// point. MongoDB advances that token past an empty batch precisely to say
// "nothing exists up to here", so it states what this function means instead of
// approximating it.
//
// The clock reading it replaces was wrong twice. It claimed a moment it had not
// proved: an empty poll says the server had nothing more to send when it
// answered, not that a write committed a millisecond earlier had been
// delivered. And it was the feeder's clock, published for a router to compare
// against its own, so on separate hosts the barrier was only as sound as the
// skew between them. The shop test saw the consequence once in 358 order
// placements: "placing one order moved c001889 from 18 to 18", a count that
// should have gone up by one and did not.
//
// The wall clock is still recorded alongside it. Lag is reported to humans in
// seconds and it is what a reader from before this understands, but nothing
// that has a cluster time to hand should be comparing against it.
func (f *Feeder) markCaughtUp(tok bson.Raw) {
	f.lastEventAt = time.Now().UTC()
	if ts, ok := clusterTimeFromToken(tok); ok {
		f.noteEventTS(ts)
	}
}

// noteEventTS records how far mirroring has got as a cluster time, and never
// lets that go backwards.
//
// Backwards is possible and is not a paradox: a resume token from an empty poll
// is taken before the batch that follows it is applied, and the two are
// published on different cadences. Mirroring reaching a moment is a claim that
// can only ever be strengthened.
func (f *Feeder) noteEventTS(ts bson.Timestamp) {
	if checkpoint.PackTS(ts) > checkpoint.PackTS(f.lastEventTS) {
		f.lastEventTS = ts
	}
}

// noteEventTime records how far mirroring has got, and never lets that go
// backwards.
//
// An event carries a cluster time, which is a BSON timestamp: one second of
// wall-clock resolution and an increment that only orders events inside that
// second. Converting it to an instant therefore rounds down by up to a second.
// Assigning that straight to lastEventAt made the position lurch backwards
// every time an event arrived, undoing a markCaughtUp that had just recorded
// the true instant, and a fallback read waiting on the position paid for it.
//
// Keeping the later of the two is not a stretched claim. markCaughtUp fires
// when the change stream is empty, so everything written up to that instant is
// already in the archive. An event that arrives afterwards was written
// afterwards, so mirroring it cannot make an earlier statement false.
func (f *Feeder) noteEventTime(ct time.Time) {
	if ct.After(f.lastEventAt) {
		f.lastEventAt = ct
	}
}

// flushAndCheckpoint writes buffered work and saves the resume position, but
// only saves as often as configured. Mirroring is idempotent, so repeating a
// few seconds of work after a restart is cheaper than a checkpoint write per
// event.
func (f *Feeder) flushAndCheckpoint(ctx context.Context) error {
	if err := f.flush(ctx); err != nil {
		return err
	}

	// Renewed here because this is the one place both the busy path and the
	// idle one pass through, and losing it has to stop mirroring rather than be
	// noticed at the next restart. See internal/lease.
	if time.Since(f.heldAt) >= lease.Renew {
		if err := f.hold.Keep(ctx); err != nil {
			if errors.Is(err, lease.ErrLost) {
				f.log.Error("another feeder has taken over, stopping",
					"id", f.cfg.ID, "err", err,
					"effect", "two feeders on one namespace read every delete twice, and an "+
						"eviction is announced once: the second reads a real eviction as the "+
						"application's own delete and tombstones the archive's copy",
					"action", "run one feeder per id; if this one was meant to be the survivor, "+
						"stop the other and start this again")
				return err
			}
			// Anything else is the archive being unreachable, which the writes
			// above would have failed on first. Worth a line, not worth
			// stopping for.
			f.log.Warn("could not renew the feeder lease", "err", err)
		} else {
			f.heldAt = time.Now()
		}
	}

	// The position goes out first and on its own schedule. Everything buffered
	// is now in the archive, which is exactly what a fallback read is waiting
	// to hear, and making it wait for the checkpoint's slower cadence is what
	// used to turn a two millisecond archive read into a one second one.
	f.publishPosition(ctx)

	if len(f.lastToken) == 0 {
		return nil
	}
	if time.Since(f.lastSaved) < f.cfg.CheckpointEvery {
		return nil
	}
	if err := f.cp.Save(ctx, f.cfg.ID, f.lastToken, f.lastEventAt); err != nil {
		return err
	}
	f.lastSaved = time.Now()
	f.reportRate()

	// Only now are the eviction announcements this run has used written off.
	//
	// The comment above says mirroring is idempotent, so repeating a few
	// seconds of work after a restart is cheap. That is what the whole
	// checkpoint cadence rests on, and an announcement spent on apply breaks
	// it: the events come round again, there is nothing left to say those
	// deletes were evictions, and the replay tombstones every one of them. The
	// window is one checkpoint interval, and an eviction sweep runs
	// continuously, so it is not a small one.
	//
	// Tied to the checkpoint instead, a crash before the save leaves the
	// announcements standing and the replay classifies them correctly. A crash
	// after it means those events are not replayed at all.
	if len(f.spentNotices) > 0 {
		if err := meta.SpendEvictionNotices(ctx, f.cold, f.spentNotices); err != nil {
			// An announcement left standing costs a later application delete
			// being read as an eviction, so the archive keeps a record it
			// should have marked deleted. That is the safe direction: it keeps
			// something it should not, rather than losing something it should.
			f.log.Warn("could not spend eviction notices", "err", err)
			return nil
		}
		f.spentNotices = nil
	}
	return nil
}

// flush applies every buffered operation to the cold tier.
//
// Writes are ordered on purpose. An insert followed by a delete of the same
// document can land in one batch, and an unordered bulk write is free to apply
// them the wrong way round.
func (f *Feeder) flush(ctx context.Context) error {
	if len(f.pending) == 0 {
		return nil
	}
	// Three questions to the archive before a single document is written, and
	// they are asked once per flush rather than once per event. See reportRate:
	// this is where the split says the time is going.
	askedFrom := time.Now()
	if err := f.resolveDeletes(ctx); err != nil {
		return err
	}
	if dropped, err := f.suppressWarmEchoes(ctx); err != nil {
		return err
	} else if dropped > 0 {
		f.log.Debug("suppressed warmed documents", "count", dropped)
	}
	if dropped, err := f.suppressPromotionEchoes(ctx); err != nil {
		return err
	} else if dropped > 0 {
		f.log.Debug("suppressed promoted documents", "count", dropped)
	}
	f.pace.asking += time.Since(askedFrom)

	now := time.Now().UTC()
	byNS := make(map[string][]mongo.WriteModel)
	// opsByNS mirrors byNS position for position, so a write error's index can
	// be turned back into the document it was about.
	opsByNS := make(map[string][]pendingOp)
	var order []string

	for _, op := range f.pending {
		var model mongo.WriteModel
		switch op.kind {
		case actionUpsert:
			f.warnReservedField(op)
			model = mongo.NewReplaceOneModel().
				SetFilter(op.key).
				SetReplacement(op.doc).
				SetUpsert(true)
		case actionTombstone:
			// The first tombstone in a namespace is also when its expiry index
			// is settled, so retention costs nothing on a namespace nothing
			// deletes from. See settleTombstoneTTL.
			f.settleTombstoneTTL(ctx, op.ns)
			// No upsert. If the archive never received this document there is
			// nothing to mark, and inventing an empty tombstone would be worse
			// than doing nothing.
			model = mongo.NewUpdateOneModel().
				SetFilter(op.key).
				SetUpdate(meta.Tombstone(now))
		default:
			continue
		}
		if _, seen := byNS[op.ns]; !seen {
			order = append(order, op.ns)
		}
		byNS[op.ns] = append(byNS[op.ns], model)
		opsByNS[op.ns] = append(opsByNS[op.ns], op)
	}

	// The documents the application deleted, so their residency records can go
	// with them. See dropResidency.
	var deleted []string
	for _, op := range f.pending {
		if op.kind == actionTombstone {
			deleted = append(deleted, meta.DocID(op.ns, op.id))
		}
	}

	bulkFrom := time.Now()
	defer func() { f.pace.bulk += time.Since(bulkFrom) }()
	for _, ns := range order {
		res, err := f.writeBatch(ctx, ns, byNS[ns], opsByNS[ns], now)
		if err != nil {
			return fmt.Errorf("mirror %s: %w", ns, err)
		}
		f.log.Debug("mirrored",
			"ns", ns,
			"upserted", res.UpsertedCount,
			"modified", res.ModifiedCount,
			"matched", res.MatchedCount)
	}

	// The batch is in the archive, so the announcements it claimed are now this
	// run's to spend. They are not written down yet: that waits for the
	// checkpoint covering these events. See flushAndCheckpoint.
	if len(f.claimedInBatch) > 0 {
		if f.spentNotices == nil {
			f.spentNotices = map[string][]bson.Timestamp{}
		}
		for id, at := range f.claimedInBatch {
			f.spentNotices[id] = append(f.spentNotices[id], at...)
		}
		f.claimedInBatch = nil
	}

	f.dropResidency(ctx, deleted)

	f.pending = f.pending[:0]
	return nil
}

// dropResidency removes the holds for documents the application has deleted.
//
// A residency record says a document was copied up and should stay on the fast
// tier for a while. When the application deletes the document the record
// describes nothing, and what it still does is worse than nothing: the router
// reads these records to decide whether an insert it sees on the fast tier is
// one of its own copy-ups, and a record left behind makes it hide the
// application's next insert of that _id.
//
// Measured. A document is copied up from the archive, the application deletes it
// and the client's change stream delivers the delete, and the application
// inserts it again two seconds later. The insert lands, the document is there,
// and the stream delivers **nothing at all**: an application's own write missing
// from its own change stream. The record's own comment already says what should
// happen, that only the one insert the promotion causes is suppressed and a
// later genuine one is not.
//
// Here rather than in the router, because the feeder sees every delete whichever
// way it arrived, including one applied straight to the fast tier. Only the
// application's: an eviction removes the document too, and the evictor spends
// those holds itself as it goes.
//
// What is left is a race the width of one flush. An application that deletes and
// re-inserts faster than the feeder gets through a batch can still have the
// insert hidden, which is the same window the eviction side accepts, and it is
// bounded by checkpoint_every rather than by the thirty second window it
// replaces.
//
// Best effort. Failing to remove a hold must not stop mirroring: the cost is one
// suppressed insert, and stopping the feeder costs every namespace behind it.
func (f *Feeder) dropResidency(ctx context.Context, ids []string) {
	if len(ids) == 0 {
		return
	}
	// Ended, not deleted, and the difference is a wrong answer.
	//
	// The record is two things at once: a hold saying the document should stay
	// on the fast tier, and the only evidence that the insert the copy-up caused
	// was a promotion rather than somebody writing a document. Deleting it takes
	// the hold away, which is right, and the evidence with it, which is not: a
	// consumer that has not caught up has yet to read that insert, and the
	// router decides whether to hide it when it hands it over.
	//
	// Measured. A document only the archive held, read once through the router
	// so it was copied up, deleted by the application two seconds later, and the
	// stream drained after that: the client was shown **insert then delete** for
	// a document it never inserted. The fuzzer found it first, as four extra
	// inserts in eighty operations.
	//
	// So the end is recorded and the hold released. An insert event at or before
	// that instant is the promotion's own; one after it is the application
	// writing that _id again, which is what deleting the record was for. The row
	// goes on its own TTL as it always did.
	res, err := f.cold.Database(meta.DB).Collection(meta.Residency).UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		meta.HoldIsOver(time.Now().UTC()))
	if err != nil {
		f.log.Warn("could not end the residency holds for documents the application deleted",
			"documents", len(ids), "err", err,
			"effect", "a client's change stream may not be shown its own insert if one of these "+
				"_ids is written again within thirty seconds",
			"note", "the holds expire on their own TTL, so this corrects itself")
		return
	}
	if res.ModifiedCount > 0 {
		f.log.Debug("ended residency holds for deleted documents", "count", res.ModifiedCount)
	}
}

// resolveDeletes decides, in one round trip, which buffered deletes were
// evictions and which came from the application.
func (f *Feeder) resolveDeletes(ctx context.Context) error {
	var ids []string
	var slots []int
	for i, op := range f.pending {
		if op.kind != actionDeletePending {
			continue
		}
		ids = append(ids, meta.DocID(op.ns, op.id))
		slots = append(slots, i)
	}
	if len(ids) == 0 {
		return nil
	}

	// Read the announcements with their counts, then spend them in event order.
	//
	// Nothing but this consumes one. A copy-up used to delete the notice on the
	// grounds that a document back in the fast tier is not leaving it, which is
	// true of the next eviction and false of the one already in flight: the
	// feeder then found no notice for a delete it had not applied yet and
	// tombstoned a document nothing deleted. See meta.AnnounceEviction.
	//
	// A count rather than a flag, because a document can be evicted, warmed back
	// up and evicted again inside one batch, and the second delete is as
	// announced as the first.
	announced, err := meta.ConsumeEvictionNotices(ctx, f.cold, ids)
	if err != nil {
		return err
	}

	// What is left to claim: what the archive still records, minus what this
	// process has already claimed and not yet written down. Both halves are
	// needed. Without the first, a replay after a restart finds nothing and
	// tombstones a real eviction. Without the second, two deletes of one
	// document inside a checkpoint window both spend the same announcement, and
	// the second of them is the application's.
	// Claims for this batch are staged separately and only merged once it is
	// applied, so an attempt that fails part way leaves nothing behind. The
	// halt policy returns from the middle of the loop below, and a write can
	// fail after it.
	f.claimedInBatch = map[string][]bson.Timestamp{}
	avail := make(map[string]int, len(announced))
	for id, n := range announced {
		avail[id] = n - len(f.spentNotices[id])
	}

	var unannounced int
	for n, slot := range slots {
		ok := avail[ids[n]] > 0
		if ok {
			avail[ids[n]]--
			// The position of the delete this announcement answers for, so the
			// router can recognise that one delete and not every later delete
			// of the same document.
			f.claimedInBatch[ids[n]] = append(f.claimedInBatch[ids[n]], f.pending[slot].at)
		}
		act := classifyDelete(ok, f.cfg.UnannouncedDeletes)
		if !ok {
			unannounced++
		}
		if act == actionHalt {
			op := f.pending[slot]
			return fmt.Errorf("%w: %s %s: set feeder.unannounced_deletes to tombstone or ignore to continue",
				ErrUnannouncedDelete, op.ns, meta.DocID(op.ns, op.id))
		}
		f.pending[slot].kind = act
	}

	if unannounced > 0 {
		f.log.Info("deletes not announced by the evictor",
			"count", unannounced,
			"policy", string(f.cfg.UnannouncedDeletes),
			"note", "treated as application deletes unless policy says otherwise")
	}
	return nil
}

// Lag reports how far behind the fast tier the last applied event was. It is
// the number to alert on: eviction safety, recovery, and how much data a lost
// fast tier would take with it all hang off this one value.
func (f *Feeder) Lag() time.Duration {
	if f.lastEventAt.IsZero() {
		return 0
	}
	return time.Since(f.lastEventAt)
}

// splitNS divides "db.collection" at the first dot. Collection names may
// contain dots, database names may not.
func splitNS(ns string) (db, coll string) {
	db, coll, _ = strings.Cut(ns, ".")
	return db, coll
}

func (f *Feeder) namespaceDescription() string {
	if len(f.cfg.Namespaces) == 0 {
		return "all except system and " + meta.DB
	}
	return strings.Join(f.cfg.Namespaces, ", ")
}

// compile-time guard that the policy constants stay in sync with classify.
var _ = []config.UnannouncedDeletes{config.Tombstone, config.Ignore, config.Halt}

// skewRemeasure is how often the feeder rereads the archive's clock. Clocks
// drift slowly and a daemon that watched them closely would be paying a round
// trip several times a second to learn nothing.
const skewRemeasure = time.Minute

// clockSkew reports how far this feeder's clock is ahead of the archive's.
//
// It is published with the position so that a router can put the feeder's
// instant and its own into one frame before comparing them. Both daemons talk
// to the archive, so the archive's clock is the reference neither of them has to
// trust the other about.
//
// A failed reading is not an error here. The previous one stands until it can be
// taken again, and a feeder that has never managed one publishes no skew at all,
// which readers understand as "compare the raw instants, as everything did
// before this existed".
func (f *Feeder) clockSkew(ctx context.Context) (time.Duration, bool) {
	if f.skewKnown && time.Since(f.skewAt) < skewRemeasure {
		return f.skew, true
	}
	if !f.skewKnown && time.Since(f.skewAt) < time.Second {
		// A cluster that refuses the reading should not be asked on every
		// publication, which at the default cadence is fifty times a second.
		return 0, false
	}
	skew, err := health.Skew(ctx, f.cold)
	f.skewAt = time.Now()
	if err != nil {
		f.log.Debug("could not read the archive's clock", "err", err)
		return f.skew, f.skewKnown
	}
	if f.skewKnown && health.AbsDuration(skew-f.skew) > health.SkewWarn {
		f.log.Warn("this host's clock moved against the archive's",
			"was", f.skew.Round(time.Millisecond).String(),
			"now", skew.Round(time.Millisecond).String(),
			"effect", "reads that fall through to the archive judge mirroring against this")
	}
	// Said out loud once, the way the evictor says its own. A clock that
	// disagrees disagrees on every publication, and at the default cadence that
	// is fifty log lines a second.
	f.clockOnce.Do(func() {
		if health.AbsDuration(skew) <= health.SkewWarn {
			f.log.Debug("clocks agree with the archive", "skew", skew.Round(time.Millisecond).String())
			return
		}
		f.log.Warn("this clock and the archive's do not agree",
			"skew", skew.Round(time.Millisecond).String(),
			"note", "mirroring lag is measured from an instant stamped here, and so is the wait "+
				"before a read falls through to the archive. Both are corrected for this "+
				"difference, and the correction is only as good as the reading",
			"action", "run NTP on every host that runs a cline daemon")
	})
	f.skew, f.skewKnown = skew, true
	return skew, true
}

// archiveNow is this instant on the archive's clock, as well as this process
// can say, for comparing against something the archive or another daemon
// stamped. It falls back to this host's own when no reading has been managed,
// which is what everything did before there was one.
func (f *Feeder) archiveNow(ctx context.Context) time.Time {
	now := time.Now().UTC()
	if skew, ok := f.clockSkew(ctx); ok {
		return now.Add(-skew)
	}
	return now
}

// publishPosition tells everyone else how far mirroring has got.
//
// A failure here is logged at debug and otherwise ignored. The position is a
// hint that makes fallback reads fast; losing one costs the next such read a
// wait it would have had anyway, and stopping mirroring over it would be
// absurd.
func (f *Feeder) publishPosition(ctx context.Context) {
	if f.cfg.PositionEvery <= 0 || (f.lastEventAt.IsZero() && f.lastEventTS.T == 0) {
		return
	}
	if time.Since(f.lastPublished) < f.cfg.PositionEvery {
		return
	}
	skew, known := f.clockSkew(ctx)
	if err := f.pos.Save(ctx, f.cfg.ID, f.lastEventAt, f.lastEventTS, f.cfg.PositionEvery, skew, known); err != nil {
		f.log.Debug("could not publish the mirroring position", "err", err)
		return
	}
	f.lastPublished = time.Now()
}

// bsonObjectTooLarge is what MongoDB answers when an update would push a
// document past the 16 MB limit. Measured against PSMDB 8.0: code 10334,
// "Resulting document after update is larger than 16777216".
const bsonObjectTooLarge = 10334

// duplicateKey is what MongoDB answers when a write violates a unique index.
// On the mirroring path it means the archive holds another live document with
// this value, so no retry of this document can ever succeed.
const duplicateKey = 11000

// writeBatch mirrors one namespace's batch, repairing the one failure it can
// repair rather than retrying it for ever.
//
// Document validation is bypassed here, and that is not a shortcut. A validator
// is a rule about what the application may write, and the fast tier applied it
// when the application wrote. What arrives here is a copy of a document that
// already exists, plus the tombstone this project adds to record a deletion,
// and a schema was never written with that field in mind.
//
// Measured on a tiered collection whose validator was an ordinary $jsonSchema
// with additionalProperties: false. The first delete of a document in it made
// the archive refuse the tombstone, the batch never succeeded, and the feeder
// reopened its change stream on the same event fifteen times in a few seconds.
// Mirroring stopped for every namespace behind it: a write to another
// collection made afterwards never reached the archive at all. One ordinary
// delete, and the archive stops receiving anything.
//
// A document already at the limit has no room for the tombstone field, so
// marking it deleted fails. Left alone that is two failures at once: the
// archive keeps a live copy of a document the application deleted, and a later
// read hands it back; and the batch never succeeds, so the feeder reopens its
// change stream on the same event again and again and *all* mirroring stops.
// Measured before this: thirteen identical reopens, and a 16 MB document the
// application had deleted came back through the router in full.
//
// So the body is dropped to make room for the mark. That loses the archive's
// audit copy of one document, which is worth much less than never returning a
// deleted one, and it is said out loud rather than inferred.
//
// The second is a document the archive's unique index refuses, which happens
// when the fast tier accepted a value the archive already holds. Measured on an
// adoption: 85 identical reopens in 85 seconds, and an order written through
// the router afterwards never reached the archive at all. No retry can succeed,
// because the value is taken by another live archive document, so the document
// is left out of the batch and named. It stays on the fast tier and the evictor
// will not remove it, since a document the archive does not hold is never
// evicted, so the cost is an archive that is missing one document until the
// duplicate is resolved. That is worth far less than all mirroring stopping.
func (f *Feeder) writeBatch(ctx context.Context, ns string, models []mongo.WriteModel,
	ops []pendingOp, now time.Time) (*mongo.BulkWriteResult, error) {

	db, coll := splitNS(ns)
	c := f.cold.Database(db).Collection(coll)

	// Ordered, so MongoDB stops at the first failure and reports its index.
	// One repair per pass therefore makes progress; the bound is the batch
	// itself, so a failure this does not understand still surfaces.
	for attempt := 0; attempt <= len(models); attempt++ {
		res, err := c.BulkWrite(ctx, models,
			options.BulkWrite().SetOrdered(true).SetBypassDocumentValidation(true))
		if err == nil {
			return res, nil
		}
		if i, dup, ok := refusedByUniqueIndex(err, ops); ok {
			f.log.Error("the archive refuses a document the fast tier accepted, leaving it unmirrored",
				"ns", ns,
				"id", dup.id.String(),
				"err", dup.msg,
				"reason", "another live document in the archive already holds this unique value",
				"effect", "the archive does not hold this document, and the evictor will not remove it from the fast tier",
				"action", "resolve the duplicate, then re-run the write; see cline keys --rebuild",
				"why", "retrying it can never succeed, and retrying it for ever stops mirroring every namespace behind it")
			models = append(models[:i:i], models[i+1:]...)
			ops = append(ops[:i:i], ops[i+1:]...)
			if len(models) == 0 {
				return &mongo.BulkWriteResult{}, nil
			}
			continue
		}
		op, ok := oversizedTombstone(err, ops)
		if !ok {
			return nil, err
		}
		f.log.Warn("an archived document is too large to mark deleted, dropping its body",
			"ns", ns,
			"id", op.id.String(),
			"reason", "the document is at the 16MB limit and the tombstone field does not fit",
			"effect", "the archive keeps the deletion but not the document it stood for",
			"why", "leaving it unmarked would return a deleted document to a later read, and stop all mirroring")
		if _, rerr := c.ReplaceOne(ctx, op.key, meta.MinimalTombstone(op.id, now),
			options.Replace().SetBypassDocumentValidation(true)); rerr != nil {
			return nil, fmt.Errorf("replace oversized tombstone: %w", rerr)
		}
	}
	return nil, fmt.Errorf("mirroring %s made no progress", ns)
}

// refusedByUniqueIndex reports whether a bulk write failed because the archive's
// unique index refuses one of its documents, and which one.
//
// It returns the position in the batch, because the answer is to leave that
// document out rather than to repair it: the value belongs to another live
// document in the archive and nothing the feeder can do here changes that.
func refusedByUniqueIndex(err error, ops []pendingOp) (int, refusedDoc, bool) {
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) {
		return 0, refusedDoc{}, false
	}
	for _, we := range bwe.WriteErrors {
		if we.Code != duplicateKey {
			continue
		}
		if we.Index < 0 || we.Index >= len(ops) {
			continue
		}
		return we.Index, refusedDoc{id: ops[we.Index].id, msg: we.Message}, true
	}
	return 0, refusedDoc{}, false
}

// refusedDoc is one document the archive would not take, and why.
type refusedDoc struct {
	id  bson.RawValue
	msg string
}

// oversizedTombstone reports whether a bulk write failed because a tombstone
// did not fit, and which document it was about.
func oversizedTombstone(err error, ops []pendingOp) (pendingOp, bool) {
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) {
		return pendingOp{}, false
	}
	for _, we := range bwe.WriteErrors {
		if we.Code != bsonObjectTooLarge {
			continue
		}
		if we.Index < 0 || we.Index >= len(ops) {
			continue
		}
		if op := ops[we.Index]; op.kind == actionTombstone {
			return op, true
		}
	}
	return pendingOp{}, false
}

// warnReservedField reports an application writing Thermocline's own field.
//
// A document on the fast tier carrying _tcDeletedAt is mirrored faithfully, and
// the archive's copy then looks exactly like a document the application
// deleted. Reads answered by the fast tier still return it and reads that fall
// through do not, so the collection appears to lose documents only when it is
// read past the fast tier's reach. The README calls this a name to avoid; this
// is what turns it from a name to avoid into something an operator is told
// about.
//
// Once per namespace per feeder run. The alternative to warning is refusing,
// and refusing would stop mirroring the collection, which is a larger failure
// than the one being reported.
func (f *Feeder) warnReservedField(op pendingOp) {
	if len(op.doc) == 0 || f.warnedReserved[op.ns] {
		return
	}
	field := ""
	if _, err := op.doc.LookupErr(meta.TombstoneField); err == nil {
		field = meta.TombstoneField
	} else if _, err := op.doc.LookupErr(meta.BodyDroppedField); err == nil {
		field = meta.BodyDroppedField
	}
	if field == "" {
		return
	}
	if f.warnedReserved == nil {
		f.warnedReserved = map[string]bool{}
	}
	f.warnedReserved[op.ns] = true
	f.log.Warn("a document carries a field name Thermocline reserves",
		"ns", op.ns,
		"field", field,
		"effect", "the archive's copy of this document is indistinguishable from one the application deleted, "+
			"so a read the fast tier cannot answer will not return it",
		"action", "rename the field; mirroring continues either way, and this is reported once per collection")
}
