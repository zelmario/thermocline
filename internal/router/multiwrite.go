package router

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/promote"
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

		// A filter that is nothing but an _id cannot reach beyond a single
		// document, and that case is already resolved on the reply.
		//
		// Nothing but an _id, not merely carrying one. A filter of an _id and
		// something else is resolved on the reply against the whole of itself,
		// and the reply path leaves a multi statement alone because this guard
		// is supposed to have brought its documents up first. Skipping here on
		// the _id alone left {_id: X, other: ...} with neither: no copy-up
		// before the write and no resolution after it, so an archived document
		// the statement matched was never touched and the write reported
		// success. See filterIsOnlyID.
		if _, hasID := idFromFilter(fdoc); hasID && filterIsOnlyID(fdoc) {
			continue
		}

		mark, known := c.r.completeness(ctx, ns)
		if known && mark.Claimed {
			af, aok := c.r.ageFieldOf(ns)
			if bound, bounded := lowerTimeBound(fdoc, af, aok); bounded && mark.Complete(bound) {
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

		n, cerr := c.r.copyUpMatching(ctx, ns, fdoc, copyUpOptions{
			let:       letOf(cmd),
			collation: docField(st, "collation"),
		})
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
// copyUpOptions are the parts of a command that decide which documents its
// filter matches, and therefore have to travel with the copy-up's own query.
//
// Two of them so far, and both were found the same way: a field the client sets
// that this router did not read. Variables, because a filter refers to them.
// Collation, because it decides what "equal" means, and asking the archive the
// same filter under a different collation asks a different question.
type copyUpOptions struct {
	let       bson.Raw
	collation bson.Raw
}

// docField returns a document-valued field, or nil when it is absent.
func docField(doc bson.Raw, key string) bson.Raw {
	v, err := doc.LookupErr(key)
	if err != nil {
		return nil
	}
	d, ok := v.DocumentOK()
	if !ok {
		return nil
	}
	return d
}

// letOf returns a command's let document, which its filters may refer to.
func letOf(cmd bson.Raw) bson.Raw {
	v, err := cmd.LookupErr("let")
	if err != nil {
		return nil
	}
	doc, ok := v.DocumentOK()
	if !ok {
		return nil
	}
	return doc
}

// copyUpMatching brings the archived documents a filter matches up to the fast
// tier, so the client's own write reaches all of them.
//
// let carries the command's variables, or nil when it has none. A filter may
// refer to them, and since MongoDB 5.0 an ordinary application writes one that
// does: update with let: {target: 5} and $expr comparing against $$target. This
// query is the same filter asked of the archive, so it needs the same
// variables. Without them the archive rejects the filter, the copy-up fails and
// the write is refused: safe, and a working statement turned into an error.
func (r *Router) copyUpMatching(ctx context.Context, ns string, filter bson.Raw, opts copyUpOptions) (int, error) {
	db, coll := splitNS(ns)
	// Built as a find command rather than through the driver's options, so
	// anything the client sent that decides matching can be carried across
	// verbatim rather than translated.
	find := bson.D{
		{Key: "find", Value: coll},
		{Key: "filter", Value: liveOnly(bson.RawValue{
			Type:  bson.TypeEmbeddedDocument,
			Value: filter,
		})},
		{Key: "limit", Value: int64(maxCopyUpForWrite + 1)},
	}
	if len(opts.let) > 0 {
		find = append(find, bson.E{Key: "let", Value: opts.let})
	}
	if len(opts.collation) > 0 {
		find = append(find, bson.E{Key: "collation", Value: opts.collation})
	}
	cur, err := r.cold.Database(db).RunCommandCursor(ctx, find)
	if err != nil {
		return 0, fmt.Errorf("read the archive: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	hot := r.hot.Database(db).Collection(coll)
	docs := make([]bson.Raw, 0, 256)
	ids := make([]string, 0, 256)
	var raw []bson.RawValue
	copied := 0
	moved := 0

	flush := func() error {
		if len(docs) == 0 {
			return nil
		}
		// Residency first, for the same reason copyUp does it first: these
		// writes are what a client's change stream sees, and it must be able
		// to tell a promotion from an insert the moment the event arrives.
		if rerr := r.recordResidencyMany(ctx, ns, raw); rerr != nil {
			r.log.Warn("could not record residency before copying up", "ns", ns, "err", rerr)
		}
		r.notePromoting(ctx)

		// Before the insert, for the same reason the single copy-up does it: the
		// insert is what creates the collection when the fast tier does not hold
		// it, and a collection created that way carries none of the archive's
		// options. See Indexer.EnsureCollection.
		r.ensureTieredCollection(ctx, ns)

		// Only the inserts moved a document. The rest were refused as duplicates
		// of a copy the fast tier already had, which is the case the old
		// $setOnInsert expressed by matching and doing nothing.
		n, skipped, err := promote.Many(ctx, hot, docs)
		if err != nil {
			return fmt.Errorf("copy up into %s: %w", ns, err)
		}
		moved += n
		// Residency was recorded for every document considered, which announced
		// an echo for each. The ones the fast tier already held produce no
		// insert, so those announcements have to be withdrawn or they are spent
		// on whatever write comes next. See withdrawEcho.
		if len(skipped) > 0 {
			stale := make([]string, 0, len(skipped))
			for _, i := range skipped {
				if i >= 0 && i < len(ids) {
					stale = append(stale, ids[i])
				}
			}
			r.withdrawEcho(ctx, stale)
		}
		// Eviction notices are left alone here, unlike the echo notices above.
		// The feeder consumes one as it applies the delete it describes, so
		// taking one away would take it from a delete the feeder has not looked
		// at yet and turn a real eviction into a tombstone over a live record.
		// See meta.AnnounceEviction.
		r.Stats.CopiedUp.Add(int64(n))
		docs = docs[:0]
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
		// An insert, never a replace: if the fast tier already holds this
		// document its copy is the authoritative one and the duplicate key
		// leaves it alone.
		docs = append(docs, doc)
		ids = append(ids, meta.DocID(ns, id))
		raw = append(raw, id)
		copied++
		if len(docs) >= 256 {
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
