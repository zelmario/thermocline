package feeder

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmar/thermocline/internal/config"
	"github.com/zelmar/thermocline/internal/meta"
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
func (f *Feeder) markCaughtUp() {
	f.lastEventAt = time.Now().UTC()
}

// flushAndCheckpoint writes buffered work and saves the resume position, but
// only saves as often as configured. Mirroring is idempotent, so repeating a
// few seconds of work after a restart is cheaper than a checkpoint write per
// event.
func (f *Feeder) flushAndCheckpoint(ctx context.Context) error {
	if err := f.flush(ctx); err != nil {
		return err
	}
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
	if err := f.resolveDeletes(ctx); err != nil {
		return err
	}
	if dropped, err := f.suppressWarmEchoes(ctx); err != nil {
		return err
	} else if dropped > 0 {
		f.log.Debug("suppressed warmed documents", "count", dropped)
	}

	now := time.Now().UTC()
	byNS := make(map[string][]mongo.WriteModel)
	var order []string

	for _, op := range f.pending {
		var model mongo.WriteModel
		switch op.kind {
		case actionUpsert:
			model = mongo.NewReplaceOneModel().
				SetFilter(op.key).
				SetReplacement(op.doc).
				SetUpsert(true)
		case actionTombstone:
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
	}

	for _, ns := range order {
		db, coll := splitNS(ns)
		res, err := f.cold.Database(db).Collection(coll).
			BulkWrite(ctx, byNS[ns], options.BulkWrite().SetOrdered(true))
		if err != nil {
			return fmt.Errorf("mirror %s: %w", ns, err)
		}
		f.log.Debug("mirrored",
			"ns", ns,
			"upserted", res.UpsertedCount,
			"modified", res.ModifiedCount,
			"matched", res.MatchedCount)
	}

	f.pending = f.pending[:0]
	return nil
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

	cur, err := f.cold.Database(meta.DB).Collection(meta.Evicting).Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return fmt.Errorf("look up eviction notices: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	announced := make(map[string]struct{}, len(ids))
	for cur.Next(ctx) {
		var rec struct {
			ID string `bson:"_id"`
		}
		if err := cur.Decode(&rec); err != nil {
			return fmt.Errorf("decode eviction notice: %w", err)
		}
		announced[rec.ID] = struct{}{}
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("read eviction notices: %w", err)
	}

	var unannounced int
	for n, slot := range slots {
		_, ok := announced[ids[n]]
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
