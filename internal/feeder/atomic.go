package feeder

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// writeFlush puts one flush into the archive as a single transaction.
//
// A flush is gathered by namespace and each namespace is one bulk write, which
// is what makes mirroring fast. It also reorders writes across collections: a
// flush holding an event of an earlier order, then a new order, then that
// order's event writes both events before the new order, and a reader landing
// between the two bulk writes sees an event whose order does not exist. No
// state of the fast tier ever held that. The freshness barrier does not cover
// it, because it promises only that everything up to the read's start is in
// the archive, and the archive also holds part of a flush past that point.
//
// Measured on 2026-09-25 with test/crossns, four writers each inserting an
// order and then its event, the archive polled directly: 5,438 of 14,132
// events seen without their order. The shop's activity report found it first,
// once, as an event its $lookup could not join.
//
// Writing the flush in oplog order instead was tried and measured, and it is
// the wrong fix. Batching and a global order are at odds: strictly interleaved
// writes make every run one document long, and a backlog of 20,000 such pairs
// mirrored at 222 documents a second. A transaction keeps the batching and
// takes away the partial state: a reader sees the whole flush or none of it,
// so the archive only ever holds a prefix of what the fast tier did.
//
// Everything writeBatch repairs is repaired here too, on the next attempt of
// the whole transaction rather than of one bulk write, because an error inside
// a transaction has aborted all of it.
func (f *Feeder) writeFlush(ctx context.Context, order []string,
	byNS map[string][]mongo.WriteModel, opsByNS map[string][]pendingOp,
	now time.Time) (map[string]*mongo.BulkWriteResult, error) {

	if f.noTxn {
		return f.writeEach(ctx, order, byNS, opsByNS, now)
	}

	// Copies, because a repair edits them and the caller counts from its own.
	models := make(map[string][]mongo.WriteModel, len(order))
	ops := make(map[string][]pendingOp, len(order))
	total := 0
	for _, ns := range order {
		models[ns] = append([]mongo.WriteModel(nil), byNS[ns]...)
		ops[ns] = append([]pendingOp(nil), opsByNS[ns]...)
		total += len(models[ns])
	}

	sess, err := f.cold.StartSession()
	if err != nil {
		return nil, fmt.Errorf("start a session on the archive: %w", err)
	}
	defer sess.EndSession(context.WithoutCancel(ctx))

	bulk := options.BulkWrite().SetOrdered(true).SetBypassDocumentValidation(true)
	// Each repair removes or replaces one operation, so there can be no more of
	// them than there are operations; the rest is room for transient retries.
	for attempt := 0; attempt <= total+10; attempt++ {
		out := make(map[string]*mongo.BulkWriteResult, len(order))
		failed := ""
		err := mongo.WithSession(ctx, sess, func(sc context.Context) error {
			if err := sess.StartTransaction(); err != nil {
				return err
			}
			for _, ns := range order {
				if len(models[ns]) == 0 {
					out[ns] = &mongo.BulkWriteResult{}
					continue
				}
				db, coll := splitNS(ns)
				res, err := f.cold.Database(db).Collection(coll).BulkWrite(sc, models[ns], bulk)
				if err != nil {
					failed = ns
					_ = sess.AbortTransaction(context.WithoutCancel(sc))
					return err
				}
				out[ns] = res
			}
			return commit(sc, sess)
		})
		if err == nil {
			f.saidEventTooLarge = false
			if f.saidWritesBlocked {
				f.saidWritesBlocked = false
				f.log.Info("the archive is taking writes again, mirroring has resumed")
			}
			for _, ns := range order {
				f.noteInsertsOverExisting(ns, ops[ns], out[ns])
				f.clearUnmirrored(ctx, ns, ops[ns])
			}
			return out, nil
		}
		if notReplicaSet(err) {
			f.noTxn = true
			f.log.Warn("the archive cannot run transactions, so a flush is not written atomically",
				"err", err,
				"meaning", "the archive is not a replica set",
				"effect", "for the length of one flush a reader of the archive can see a write to one "+
					"collection without an earlier write to another, such as an event without its order",
				"action", "run the archive as a replica set, which is the supported shape; a single "+
					"member replica set is enough")
			return f.writeEach(ctx, order, byNS, opsByNS, now)
		}
		// Before the transient check, so a label on it does not retry the same
		// flush for ever.
		if tooLargeForCache(err) {
			f.log.Warn("the archive could not hold this flush in one transaction, writing it a collection at a time",
				"documents", total, "err", err,
				"effect", "for the length of this one flush a reader of the archive can see a write to one "+
					"collection without an earlier write to another",
				"why", "retrying the same transaction fails the same way, and mirroring would stop for every namespace behind it")
			return f.writeEach(ctx, order, byNS, opsByNS, now)
		}
		if transient(err) {
			continue
		}
		if failed == "" {
			return nil, err
		}
		if i, dup, ok := refusedByUniqueIndex(err, ops[failed]); ok {
			f.refuse(ctx, failed, dup, now)
			models[failed] = append(models[failed][:i:i], models[failed][i+1:]...)
			ops[failed] = append(ops[failed][:i:i], ops[failed][i+1:]...)
			continue
		}
		if i, ok := oversizedTombstoneAt(err, ops[failed]); ok {
			op := ops[failed][i]
			f.log.Warn("an archived document is too large to mark deleted, dropping its body",
				"ns", failed,
				"id", op.id.String(),
				"reason", "the document is at the 16MB limit and the tombstone field does not fit",
				"effect", "the archive keeps the deletion but not the document it stood for",
				"why", "leaving it unmarked would return a deleted document to a later read, and stop all mirroring")
			models[failed][i] = mongo.NewReplaceOneModel().
				SetFilter(op.key).
				SetReplacement(meta.MinimalTombstone(op.id, now))
			continue
		}
		return nil, fmt.Errorf("mirror %s: %w", failed, err)
	}
	return nil, errors.New("mirroring made no progress")
}

// writeEach is the flush one namespace at a time, which is all an archive that
// cannot run transactions allows.
func (f *Feeder) writeEach(ctx context.Context, order []string,
	byNS map[string][]mongo.WriteModel, opsByNS map[string][]pendingOp,
	now time.Time) (map[string]*mongo.BulkWriteResult, error) {
	out := make(map[string]*mongo.BulkWriteResult, len(order))
	for _, ns := range order {
		res, err := f.writeBatch(ctx, ns, byNS[ns], opsByNS[ns], now)
		if err != nil {
			return nil, fmt.Errorf("mirror %s: %w", ns, err)
		}
		out[ns] = res
	}
	return out, nil
}

// commit commits, retrying only the commit when its outcome is unknown, which
// is what the label means: the transaction may have committed, and retrying
// the commit is safe where retrying the work would not be.
func commit(ctx context.Context, sess *mongo.Session) error {
	for i := 0; ; i++ {
		err := sess.CommitTransaction(ctx)
		var le mongo.LabeledError
		if err != nil && i < 5 && errors.As(err, &le) && le.HasErrorLabel("UnknownTransactionCommitResult") {
			continue
		}
		return err
	}
}

// tooLargeForCache says the archive's storage engine could not hold the
// transaction's writes until it committed.
//
// Measured on 2026-09-26 with 60 documents of 12MB behind a stopped feeder, on
// this box's 1GB archive cache: every flush of them failed with it, the feeder
// reopened its stream and retried the same flush, and in two minutes 13 of the
// 60 had been mirrored while mirroring fell five minutes behind for everything
// else. The flush is smaller now, see flushBytes, and this is what remains when
// even that does not fit.
func tooLargeForCache(err error) bool {
	return err != nil && strings.Contains(err.Error(), "TransactionTooLargeForCache")
}

// eventTooLarge says the change stream failed because mongod would not return
// an event past its size limit, as opposed to one of the feeder's own writes
// being too large, which the write paths deal with themselves.
func eventTooLarge(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	return strings.Contains(m, "BSONObjectTooLarge") &&
		(strings.Contains(m, "getMore") || strings.Contains(m, "aggregation"))
}

// transient says the whole transaction can be tried again as it is.
func transient(err error) bool {
	var le mongo.LabeledError
	return errors.As(err, &le) && le.HasErrorLabel("TransientTransactionError")
}

// notReplicaSet says the archive refused a transaction because it is a
// standalone mongod, IllegalOperation with the server's own words.
func notReplicaSet(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Code == 20 && strings.Contains(ce.Message, "replica set")
	}
	return strings.Contains(err.Error(), "Transaction numbers are only allowed on a replica set member")
}

// oversizedTombstoneAt is oversizedTombstone, reporting where in the batch.
func oversizedTombstoneAt(err error, ops []pendingOp) (int, bool) {
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) {
		return 0, false
	}
	for _, we := range bwe.WriteErrors {
		if we.Code != bsonObjectTooLarge || we.Index < 0 || we.Index >= len(ops) {
			continue
		}
		if ops[we.Index].kind == actionTombstone {
			return we.Index, true
		}
	}
	return 0, false
}
