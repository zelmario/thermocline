package router

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// Outside a transaction the unique key guard has to run before the write, because
// the moment a write lands it is visible and a duplicate cannot be taken back.
//
// Inside a transaction the opposite is true, and it is what makes the guard
// possible at all. Nothing the transaction writes is visible until it commits,
// so the guard can let the write happen, read what it actually produced, and
// still refuse by aborting the transaction. That removes every guess: no
// evaluating a $inc or a pipeline update, no dry run in a session of the
// router's own that cannot see the caller's uncommitted writes and deadlocks
// against their locks. The fast tier is simply asked what the document now says,
// inside the transaction that wrote it.
//
// The bug this closes: a transaction that inserted a document and then moved it
// onto a value an archived document already held. The old guard read the
// matched documents on the router's own client, could not see the one the
// transaction had just inserted, computed keys for an empty set, and waved the
// write through. Two live documents, one key, one in each tier, committed and
// reported as success.

// txnKeyCheck is what one in-flight statement left for verifyTxnKeys to check.
type txnKeyCheck struct {
	ns      string
	txn     txnFields
	targets []bson.RawValue
	armed   bool
}

// prepareTxnKeyCheck notes which documents an update inside a transaction is
// about to touch, reading them from inside that transaction so its own
// uncommitted writes are included.
//
// It deliberately never handles the command. Everything else the router does to
// an update still has to happen, above all copying up a document the statement
// matched nothing for: taking that over here would trade one silent wrong
// answer for another.
func (c *conn) prepareTxnKeyCheck(ctx context.Context, msg *wire.Message, cmd bson.Raw, db, name string, t txnFields) {
	c.txnKeys = txnKeyCheck{}

	coll, stmts, ok := updateStatements(cmd, msg, name)
	if !ok || len(stmts) == 0 {
		return
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return
	}
	if len(c.r.uniqueIndexes(ctx, ns)) == 0 {
		return
	}

	var targets []bson.RawValue
	for _, st := range stmts {
		ids, tooMany, err := c.matchedIDsInTxn(t, ns, st)
		if err != nil {
			// The transaction is intact; the router simply could not ask. That
			// is the situation the pre-flight guard already treats as "let it
			// through, and say so".
			c.log.Warn("could not read the documents this update will touch, letting it through",
				"ns", ns, "err", err,
				"effect", "a duplicate of an archived document could be created")
			return
		}
		if tooMany {
			c.log.Warn("too many documents to check against the key registry, letting the update through",
				"ns", ns, "limit", maxKeyedUpdate)
			return
		}
		targets = append(targets, ids...)
	}

	c.txnKeys = txnKeyCheck{ns: ns, txn: t, targets: targets, armed: true}
}

// verifyTxnKeys checks what the write actually produced, and refuses it by
// aborting the transaction if it duplicates a key an archived document holds.
func (c *conn) verifyTxnKeys(ctx context.Context, msg *wire.Message, reply *wire.Message, name string) (*wire.Message, bool) {
	chk := c.txnKeys
	c.txnKeys = txnKeyCheck{}
	if !chk.armed || reply == nil {
		return nil, false
	}

	rdoc, derr := reply.ReplyDoc()
	if derr != nil {
		return nil, false
	}
	// A write the fast tier refused never happened, and a reply asking for the
	// transaction to be replayed means it has not happened yet. Neither has
	// anything to check or to claim.
	if !replyOK(rdoc) || writeFailed(rdoc) {
		return nil, false
	}

	ids := append(append([]bson.RawValue(nil), chk.targets...), upsertedIDs(rdoc)...)
	if len(ids) == 0 {
		return nil, false
	}

	after, aerr := c.findInCallerTxn(chk.txn, chk.ns,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}, nil, int64(len(ids)), false)
	if aerr != nil {
		c.log.Warn("could not read back what this update produced, letting it stand",
			"ns", chk.ns, "err", aerr,
			"effect", "a duplicate of an archived document could be created")
		return nil, false
	}
	// A short read means some post-images were not seen, and a key that was
	// never computed cannot be found to clash. Say so rather than report a
	// clean check over part of the work. Deletes explain a legitimate shortfall,
	// so this is only worth a warning when documents are missing that the
	// statement did not remove.
	if len(after) < len(ids) {
		c.log.Debug("read back fewer documents than this update touched",
			"ns", chk.ns, "touched", len(ids), "read", len(after),
			"note", "documents the statement deleted are expected to be missing")
	}

	idxs := c.r.uniqueIndexes(ctx, chk.ns)
	var claimed []string
	for i := range idxs {
		clash, kids, cerr := c.claimEvaluated(ctx, chk.ns, idxs[i], after)
		claimed = append(claimed, kids...)
		if errors.Is(cerr, errArchiveBehind) {
			// The write is in the transaction already, so refusing it means
			// aborting it, as for a duplicate below. See clashIsFreshOrBehind.
			if err := c.abortCallerTxn(chk.txn); err != nil {
				c.log.Warn("could not abort a transaction whose keys could not be judged",
					"ns", chk.ns, "err", err)
			}
			if out, done := c.refusedAsBehind(ctx, msg, cerr, claimed); done {
				return out, true
			}
			return nil, false
		}
		if cerr != nil {
			c.r.releaseKeys(ctx, claimed)
			c.log.Warn("could not check unique keys, letting the update stand",
				"ns", chk.ns, "err", cerr)
			return nil, false
		}
		if !clash {
			continue
		}

		// The write is real but invisible, and it must never become visible.
		// Aborting is what makes refusing after the fact as safe as refusing
		// before it: the application cannot commit past this.
		c.r.releaseKeys(ctx, claimed)
		c.r.Stats.DuplicateKeys.Add(1)
		c.cls = ClassRefused
		if err := c.abortCallerTxn(chk.txn); err != nil {
			c.log.Warn("could not abort the transaction that would have created a duplicate",
				"ns", chk.ns, "err", err)
		}
		c.log.Debug("update inside a transaction would duplicate an archived document",
			"ns", chk.ns, "index", idxs[i].name)
		out, rerr := c.r.updateDuplicateKeyReply(msg, name, chk.ns, idxs[i].name, 0)
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	return nil, false
}

// matchedIDsInTxn reads the ids a statement will act on, in the order and number
// the server would pick them, from inside the caller's transaction.
func (c *conn) matchedIDsInTxn(t txnFields, ns string, st updateStatement) (ids []bson.RawValue, tooMany bool, err error) {
	limit := int64(1)
	if st.multi {
		limit = int64(maxKeyedUpdate) + 1
	}
	filter := st.filter
	if len(filter) == 0 {
		filter = bson.Raw{}
	}
	docs, err := c.findInCallerTxn(t, ns, filter, st.sort, limit, true)
	if err != nil {
		return nil, false, err
	}
	if len(docs) > maxKeyedUpdate {
		return nil, true, nil
	}
	for _, doc := range docs {
		if id, ierr := doc.LookupErr("_id"); ierr == nil {
			ids = append(ids, id)
		}
	}
	return ids, false, nil
}

// upsertedIDs reads the ids of documents an update created rather than matched.
// They hold keys too, and nothing matched them beforehand to notice.
func upsertedIDs(rdoc bson.Raw) []bson.RawValue {
	var out []bson.RawValue

	// update says so in an "upserted" array.
	if val, err := rdoc.LookupErr("upserted"); err == nil {
		if arr, ok := val.ArrayOK(); ok {
			if vals, verr := arr.Values(); verr == nil {
				for _, v := range vals {
					if d, dok := v.DocumentOK(); dok {
						if id, ierr := d.LookupErr("_id"); ierr == nil {
							out = append(out, id)
						}
					}
				}
			}
		}
	}
	// findAndModify says so under lastErrorObject.
	if val, err := rdoc.LookupErr("lastErrorObject"); err == nil {
		if d, ok := val.DocumentOK(); ok {
			if id, ierr := d.LookupErr("upserted"); ierr == nil {
				out = append(out, id)
			}
		}
	}
	return out
}
