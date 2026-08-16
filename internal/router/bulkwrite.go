package router

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmar/thermocline/internal/wire"
)

// MongoDB 8.0 added a second way to write, and it does not look like the first.
//
// Every other write command names its collection where the guards look for it:
// `insert: "orders"`, `update: "orders"`. bulkWrite goes to the admin database,
// lists its collections in nsInfo, and has its operations refer to them by
// position. A router reading a collection name out of the command finds nothing
// at all, decides the command touches nothing it manages, and forwards it
// untouched.
//
// What went with it was every protection this system has for writes: the unique
// key registry that stops an insert duplicating a document the fast tier can no
// longer see, and the copy-up that lets a multi-document write reach documents
// that have been evicted. Measured on a collection holding three of twelve
// documents, a bulkWrite delete reported success having removed three, and the
// other nine stayed readable in the archive. Both failures were fixed on the
// ordinary write path long ago; this command reopened them through a side door.
//
// So the ops are decomposed here and handed to exactly the same guards.

// bulkOp is one entry of a bulkWrite ops array, normalised into the shape the
// existing guards already understand.
type bulkOp struct {
	kind string // insert, update or delete
	ns   string
	doc  bson.Raw        // insert
	stmt updateStatement // update and delete
}

// parseBulkWrite reads the command into namespaces and operations.
func parseBulkWrite(cmd bson.Raw, msg *wire.Message) ([]bulkOp, bool) {
	var namespaces []string
	nsVal, err := cmd.LookupErr("nsInfo")
	if err != nil {
		return nil, false
	}
	arr, ok := nsVal.ArrayOK()
	if !ok {
		return nil, false
	}
	vals, verr := arr.Values()
	if verr != nil {
		return nil, false
	}
	for _, v := range vals {
		d, dok := v.DocumentOK()
		if !dok {
			return nil, false
		}
		ns, nok := stringField(d, "ns")
		if !nok {
			return nil, false
		}
		namespaces = append(namespaces, ns)
	}

	// ops may arrive inline or as a payload sequence, the same as the documents
	// of an ordinary insert.
	ops := msg.AllOf("ops")
	if len(ops) == 0 {
		if v, oerr := cmd.LookupErr("ops"); oerr == nil {
			if a, aok := v.ArrayOK(); aok {
				if ovals, oerr2 := a.Values(); oerr2 == nil {
					for _, o := range ovals {
						if d, dok := o.DocumentOK(); dok {
							ops = append(ops, d)
						}
					}
				}
			}
		}
	}
	if len(ops) == 0 {
		return nil, false
	}

	out := make([]bulkOp, 0, len(ops))
	for _, op := range ops {
		var b bulkOp
		switch {
		case hasKey(op, "insert"):
			b.kind = "insert"
			idx, iok := intOf(op.Lookup("insert"))
			if !iok || idx < 0 || idx >= len(namespaces) {
				return nil, false
			}
			b.ns = namespaces[idx]
			d, dok := op.Lookup("document").DocumentOK()
			if !dok {
				return nil, false
			}
			b.doc = d
		case hasKey(op, "update"):
			b.kind = "update"
			idx, iok := intOf(op.Lookup("update"))
			if !iok || idx < 0 || idx >= len(namespaces) {
				return nil, false
			}
			b.ns = namespaces[idx]
			b.stmt = updateStatement{
				filter: subDoc(op.Lookup("filter")),
				update: op.Lookup("updateMods"),
				multi:  boolField(op, "multi"),
				upsert: boolField(op, "upsert"),
			}
			b.stmt.arrayFilters = op.Lookup("arrayFilters")
		case hasKey(op, "delete"):
			b.kind = "delete"
			idx, iok := intOf(op.Lookup("delete"))
			if !iok || idx < 0 || idx >= len(namespaces) {
				return nil, false
			}
			b.ns = namespaces[idx]
			b.stmt = updateStatement{
				filter: subDoc(op.Lookup("filter")),
				multi:  boolField(op, "multi"),
			}
		default:
			// An operation shape this router does not know. Guarding what it
			// cannot read would be pretending, so the whole command is left
			// alone and said so.
			return nil, false
		}
		out = append(out, b)
	}
	return out, true
}

func hasKey(doc bson.Raw, key string) bool {
	_, err := doc.LookupErr(key)
	return err == nil
}

func boolField(doc bson.Raw, key string) bool {
	b, ok := doc.Lookup(key).BooleanOK()
	return ok && b
}

// guardBulkWrite applies the ordinary write guards to a bulkWrite.
//
// It reports true when it has answered the client itself, which happens only
// when the command is refused.
func (c *conn) guardBulkWrite(ctx context.Context, msg *wire.Message, cmd bson.Raw) (*wire.Message, bool) {
	ops, ok := parseBulkWrite(cmd, msg)
	if !ok {
		c.log.Warn("could not read this bulkWrite, forwarding it unguarded",
			"effect", "cross-tier unique keys and archived documents are not protected for it")
		return nil, false
	}

	touches := false
	for _, op := range ops {
		if c.r.cfg.Manages(op.ns) {
			touches = true
			break
		}
	}
	if !touches {
		return nil, false
	}

	_, inTxn := txnOf(cmd)

	// Documents the write has to be able to see have to be here before it runs.
	// Before, not after: repeating a write after a copy-up would apply $inc
	// twice, which is why the ordinary path does it in this order too.
	for _, op := range ops {
		if op.kind == "insert" || !c.r.cfg.Manages(op.ns) {
			continue
		}
		if len(op.stmt.filter) == 0 {
			continue
		}
		if inTxn {
			switch out, res := c.warmForTxn(ctx, op.ns, op.stmt.filter, msg.Header.RequestID, msg.Header.RequestID,
				"the documents this transaction writes"); res {
			case txnWarmReplay:
				c.r.Stats.TxnReplays.Add(1)
				c.cls = ClassTxnWarm
				return out, true
			case txnWarmComplete:
				continue
			}
			c.r.Stats.Refused.Add(1)
			c.cls = ClassRefused
			out, terr := c.r.txnCannotSpanTiers(msg.Header.RequestID, msg.Header.RequestID, op.ns,
				"a bulkWrite inside a transaction cannot reach documents the fast tier does not hold")
			if terr != nil {
				return nil, false
			}
			return out, true
		}
		if n, cerr := c.r.copyUpMatching(ctx, op.ns, op.stmt.filter); cerr != nil {
			c.r.Stats.Refused.Add(1)
			c.cls = ClassRefused
			c.log.Warn("refused a bulkWrite that would only partly apply", "ns", op.ns, "err", cerr)
			out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
				"thermocline: refusing this bulkWrite because it matches documents the fast tier does not hold, "+
					"and applying it to only some of them would report success for a change that did not happen: "+
					cerr.Error())
			if rerr != nil {
				return nil, false
			}
			return out, true
		} else if n > 0 {
			c.log.Info("copied documents up so a bulkWrite could reach them", "ns", op.ns, "documents", n)
		}
	}

	// Then the unique keys, for the same reason and in the same way as an
	// ordinary insert or update.
	var claimed []string
	for _, op := range ops {
		if !c.r.cfg.Manages(op.ns) {
			continue
		}
		idxs := c.r.uniqueIndexes(ctx, op.ns)
		if len(idxs) == 0 {
			continue
		}

		switch op.kind {
		case "insert":
			got, clashes, err := c.r.claimKeys(ctx, op.ns, []bson.Raw{op.doc}, idxs, true)
			claimed = append(claimed, got...)
			if err != nil {
				c.log.Warn("could not check unique keys for this bulkWrite, letting it through",
					"ns", op.ns, "err", err)
				continue
			}
			if len(clashes) > 0 {
				return c.refuseBulk(ctx, msg, claimed, op.ns, clashes[0].index)
			}
		case "update":
			sh := parseUpdate(op.stmt.update)
			for i := range idxs {
				if !sh.affects(idxs[i]) {
					continue
				}
				clash, kids, cerr := c.claimStatement(ctx, op.ns, idxs[i], sh, op.stmt)
				claimed = append(claimed, kids...)
				if cerr != nil {
					c.log.Warn("could not check unique keys for this bulkWrite, letting it through",
						"ns", op.ns, "err", cerr)
					continue
				}
				if clash {
					return c.refuseBulk(ctx, msg, claimed, op.ns, idxs[i].name)
				}
			}
		}
	}

	// Held until the reply, so a command the fast tier rejects gives its keys
	// back rather than blocking those values until something collides.
	c.pendingKeys = append(c.pendingKeys, claimed...)
	return nil, false
}

// refuseBulk turns down a whole bulkWrite because one of its operations would
// duplicate a key an archived document holds.
//
// The whole command, not the one operation. bulkWrite reports per-operation
// results through a cursor, and rebuilding that reply to reject one entry while
// running the rest would be inventing results the server never produced. The
// application sees a duplicate key error, which is what it would have seen from
// a single cluster, and retries without the offending operation.
func (c *conn) refuseBulk(ctx context.Context, msg *wire.Message, claimed []string, ns, index string) (*wire.Message, bool) {
	c.r.releaseKeys(ctx, claimed)
	c.r.Stats.DuplicateKeys.Add(1)
	c.cls = ClassRefused
	c.log.Debug("bulkWrite would duplicate an archived document", "ns", ns, "index", index)
	out, err := c.r.duplicateKeyReply(msg.Header.RequestID, msg.Header.RequestID, ns, index)
	if err != nil {
		return nil, false
	}
	return out, true
}
