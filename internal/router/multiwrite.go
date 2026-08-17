package router

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// A write that names many documents was only ever applied to the ones the fast
// tier happened to hold. updateMany({grp:"a"}) reported modified 3 where 13
// matched, and deleteMany reported 2 where 12 matched, both as success. The
// application is told the sweep happened. For a delete that is worse than a
// wrong number: records somebody asked to have removed stay readable.
//
// Reads solved this by going to the archive. A write cannot: the archive is
// upsert-only and the feeder is its only writer, so applying the change there
// directly would race mirroring and break the rule the whole design rests on.
//
// So the matching documents come up to the fast tier first, and then the
// client's own command runs, once, against a tier that now holds all of them.
// It has to be this way round. Running the write first and repeating it after a
// copy-up would apply $inc twice.

// maxCopyUpForWrite caps how many documents one multi-document write will drag
// up from the archive.
//
// A sweep over an unbounded range could otherwise pull the whole archive into
// the fast tier to satisfy one statement. Past this the router refuses, loudly,
// rather than doing part of the job and calling it success.
const maxCopyUpForWrite = 10000

// guardMultiWrite brings archived documents up before a multi-document write.
func (c *conn) guardMultiWrite(ctx context.Context, msg *wire.Message, db, name string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	field, filterKey := "updates", "q"
	if name == "delete" {
		field, filterKey = "deletes", "q"
	}
	coll, ok := stringField(cmd, name)
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}

	stmts := msg.AllOf(field)
	if len(stmts) == 0 {
		return nil, false
	}

	for _, st := range stmts {
		if !statementIsMulti(st, name) {
			continue
		}
		fval, ferr := st.LookupErr(filterKey)
		if ferr != nil {
			continue
		}
		fdoc, dok := fval.DocumentOK()
		if !dok {
			continue
		}

		// A filter pinned to one _id cannot reach beyond a single document, and
		// that case is already resolved on the reply.
		if _, hasID := idFromFilter(fdoc); hasID {
			continue
		}

		mark, known := c.r.completeness(ctx, ns)
		if known && mark.Claimed {
			if bound, bounded := lowerTimeBound(fdoc); bounded && mark.Complete(bound) {
				continue
			} else if !bounded && mark.CoversAll() {
				continue
			}
		}

		if inTransaction(cmd) {
			// The copy-up is an ordinary write outside the transaction, so the
			// transaction's snapshot was taken before it landed and repeating
			// the statement now would still miss. Replaying the whole
			// transaction is what makes the documents visible to it.
			switch out, res := c.warmForTxn(ctx, ns, fdoc, msg.Header.RequestID, msg.Header.RequestID,
				"the documents this transaction writes"); res {
			case txnWarmReplay:
				c.r.Stats.TxnReplays.Add(1)
				c.cls = ClassTxnWarm
				return out, true
			case txnWarmComplete:
				// Every document this statement matches is already on the fast
				// tier, so the write reaches all of them.
				continue
			}
			c.r.Stats.Refused.Add(1)
			c.cls = ClassRefused
			out, terr := c.r.txnCannotSpanTiers(msg.Header.RequestID, msg.Header.RequestID, ns,
				"a multi-document write inside a transaction cannot reach documents the fast tier does not hold")
			if terr != nil {
				return nil, false
			}
			return out, true
		}

		if c.r.cfg.Completeness == config.Permissive {
			c.log.Warn("multi-document write may touch only part of what it matches",
				"ns", ns, "command", name)
			continue
		}

		n, cerr := c.r.copyUpMatching(ctx, ns, fdoc)
		if cerr == nil {
			if n > 0 {
				c.log.Info("copied documents up so a multi-document write could reach them",
					"ns", ns, "command", name, "documents", n)
			}
			continue
		}

		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a multi-document write that would only partly apply",
			"ns", ns, "command", name, "err", cerr)
		out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: refusing this "+name+" because it matches documents the fast tier does not hold, "+
				"and applying it to only some of them would report success for a change that did not happen: "+
				cerr.Error())
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	return nil, false
}

// statementIsMulti reports whether one statement of a bulk write names more
// than a single document. Update says multi:true; delete says limit:0.
func statementIsMulti(st bson.Raw, name string) bool {
	if name == "delete" {
		val, err := st.LookupErr("limit")
		if err != nil {
			return true
		}
		n, ok := intOf(val)
		return ok && n == 0
	}
	val, err := st.LookupErr("multi")
	if err != nil {
		return false
	}
	b, ok := val.BooleanOK()
	return ok && b
}

// copyUpMatching brings every archived document matching filter into the fast
// tier, skipping the ones already there.
//
// It returns how many documents it actually moved, not how many it looked at.
// The two are different whenever the fast tier already held some of them, and
// the difference matters: a caller that reads a zero here has been told
// something true and useful, that the fast tier is not missing anything this
// filter matches. Counting documents examined instead made that answer
// impossible, and a transaction waiting for it replayed itself forever.
func (r *Router) copyUpMatching(ctx context.Context, ns string, filter bson.Raw) (int, error) {
	db, coll := splitNS(ns)
	cur, err := r.cold.Database(db).Collection(coll).Find(ctx, liveOnly(bson.RawValue{
		Type:  bson.TypeEmbeddedDocument,
		Value: filter,
	}), options.Find().SetLimit(maxCopyUpForWrite+1))
	if err != nil {
		return 0, fmt.Errorf("read the archive: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	hot := r.hot.Database(db).Collection(coll)
	models := make([]mongo.WriteModel, 0, 256)
	ids := make([]string, 0, 256)
	var raw []bson.RawValue
	copied := 0
	moved := 0

	flush := func() error {
		if len(models) == 0 {
			return nil
		}
		// Residency first, for the same reason copyUp does it first: these
		// writes are what a client's change stream sees, and it must be able
		// to tell a promotion from an insert the moment the event arrives.
		for _, id := range raw {
			if rerr := r.recordResidency(ctx, ns, id); rerr != nil {
				r.log.Warn("could not record residency before copying up", "ns", ns, "err", rerr)
				break
			}
		}

		res, err := hot.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
		if err != nil {
			return fmt.Errorf("copy up into %s: %w", ns, err)
		}
		// Only the upserts moved a document. The rest matched a copy the fast
		// tier already had, which $setOnInsert deliberately leaves alone.
		moved += int(res.UpsertedCount)
		if err := meta.ClearEvictionNotices(ctx, r.cold, ids); err != nil {
			r.log.Warn("copied up but could not clear eviction notices", "ns", ns, "err", err)
		}
		r.Stats.CopiedUp.Add(res.UpsertedCount)
		models = models[:0]
		ids = ids[:0]
		raw = raw[:0]
		return nil
	}

	for cur.Next(ctx) {
		if copied >= maxCopyUpForWrite {
			return moved, fmt.Errorf("more than %d documents would have to be brought back from the archive; "+
				"warm that range first, or narrow the filter", maxCopyUpForWrite)
		}
		doc := bson.Raw(cur.Current)
		id, lerr := doc.LookupErr("_id")
		if lerr != nil {
			continue
		}
		// $setOnInsert, never a replace: if the fast tier already holds this
		// document its copy is the authoritative one.
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: id}}).
			SetUpdate(bson.D{{Key: "$setOnInsert", Value: doc}}).
			SetUpsert(true))
		ids = append(ids, meta.DocID(ns, id))
		raw = append(raw, id)
		copied++
		if len(models) >= 256 {
			if err := flush(); err != nil {
				return moved, err
			}
		}
	}
	if err := cur.Err(); err != nil {
		return moved, fmt.Errorf("read the archive: %w", err)
	}
	if err := flush(); err != nil {
		return moved, err
	}
	return moved, nil
}
