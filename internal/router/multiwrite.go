package router

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

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
//
// It brings them up for a single-document one too, but only when the write is
// unacknowledged. A single-document statement that matched nothing is normally
// put right on the reply, by interceptWrite, which resolves the filter against
// the archive and runs the statement again against a fast tier that now holds
// the document. An unacknowledged write has no reply to put anything right on:
// the router returns as soon as the message is written. So for those the
// choice is to do the work here or to lose the write, and the shortcuts below
// are taken only where a reply will follow to take them back.
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

	// An unacknowledged write is judged here whatever it names, because nothing
	// downstream will judge it. See the note above this function.
	repliable := !msg.MoreToCome()

	for _, st := range stmts {
		multi := statementIsMulti(st, name)
		if !multi && repliable {
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
		// document, and that case is already resolved on the reply. Only when
		// there is one: unacknowledged, naming the document outright is the
		// same door as naming it by any other field, and neither is resolved
		// anywhere else.
		//
		// Nothing but an _id, not merely carrying one. A filter of an _id and
		// something else is resolved on the reply against the whole of itself,
		// and the reply path leaves a multi statement alone because this guard
		// is supposed to have brought its documents up first. Skipping here on
		// the _id alone left {_id: X, other: ...} with neither: no copy-up
		// before the write and no resolution after it, so an archived document
		// the statement matched was never touched and the write reported
		// success. See filterIsOnlyID.
		if _, hasID := idFromFilter(fdoc); hasID && filterIsOnlyID(fdoc) && repliable {
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

		// One statement acting on one document needs one document, however many
		// it matches. copyUpMatching below brings up every match, which is what
		// a multi-document statement is owed and would turn an unacknowledged
		// updateOne({status: "new"}) into a warm of the whole archive: bounded
		// at maxCopyUpForWrite, so past ten thousand matches it would refuse an
		// ordinary write instead. This is the resolution interceptWrite does on
		// the reply, moved in front of the write because there is no reply.
		//
		// Before the transaction branch below, and so outside any transaction:
		// a copy-up is invisible to a snapshot taken before it, and putting
		// that right means replaying the transaction, which means sending a
		// reply to a client that is not listening. A statement sent
		// unacknowledged inside a transaction is not a shape a driver
		// produces, every statement of one being answered, so this leaves it
		// where it was rather than inventing a behaviour for it.
		if !multi {
			c.bringUpOneForUnacked(ctx, ns, fdoc, docField(st, "collation"), name)
			continue
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
			budget:    maxTimeBudget(cmd),
		})
		if cerr == nil {
			if n > 0 {
				c.log.Info("copied documents up so a multi-document write could reach them",
					"ns", ns, "command", name, "documents", n)
			}
			continue
		}

		// A deadline that ran out is the caller's own answer, not a refusal by
		// this router, and it is reported as MongoDB reports it.
		if errors.Is(cerr, errCopyUpExpired) {
			c.log.Info("a write's own maxTimeMS ran out while bringing archived documents up",
				"ns", ns, "command", name,
				"meaning", "the write was not applied, and the caller is told what one cluster "+
					"would tell it")
			out, rerr := c.r.commandErrorWithCode(msg.Header.RequestID, msg.Header.RequestID,
				"operation exceeded time limit", maxTimeMSExpiredCode, "MaxTimeMSExpired")
			if rerr != nil {
				return nil, false
			}
			return out, true
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
	// budget is the caller's own maxTimeMS, or zero when they set none.
	//
	// Without it the copy-up is work the client cannot see and did not agree
	// to. Measured on 3,000 documents held by the archive alone: an update
	// asking for 1ms came back in 595ms, and the same update on an untiered
	// collection came back in 1ms. The error was right both times and the
	// deadline meant nothing on the tiered one, which is the opposite of what
	// maxTimeMS is for: an application sets it to shed load, and a bound that
	// is exceeded 595-fold sheds none.
	//
	// The read path has always done this, in withRemainingTime. This is the
	// same thing on the other path.
	budget time.Duration
}

// errCopyUpExpired says the caller's own deadline ran out while the archive was
// being read, so the reply has to be MongoDB's own MaxTimeMSExpired rather than
// this router's generic refusal: it is the one error code whose retry semantics
// do apply, and a driver told CommandFailed cannot tell a deadline from a
// refusal.
var errCopyUpExpired = errors.New("the caller's maxTimeMS ran out while reading the archive")

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
	// The caller's deadline, applied to the work this adds. A millisecond floor
	// rather than zero, for the reason withRemainingTime gives: MongoDB reads
	// maxTimeMS zero as "no limit", which is the opposite of an expired one.
	started := time.Now()
	if opts.budget > 0 {
		find = append(find, bson.E{Key: "maxTimeMS", Value: int64(1)})
		if ms := opts.budget.Milliseconds(); ms > 1 {
			find[len(find)-1].Value = ms
		}
	}
	cur, err := r.cold.Database(db).RunCommandCursor(ctx, find)
	if err != nil {
		if isMaxTimeExpired(err) {
			return 0, errCopyUpExpired
		}
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
		// Checked here as well as on the find, because the copy writes to the
		// fast tier as it reads and those writes are not covered by the
		// archive's own deadline.
		if opts.budget > 0 && time.Since(started) > opts.budget {
			return moved, errCopyUpExpired
		}
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

// bringUpOneForUnacked brings up the one archived document an unacknowledged
// single-document write would otherwise miss.
//
// The fast tier is asked first, and a match there ends it: a statement with no
// sort acts on any document it matches, which is all MongoDB promises, and
// asking the local tier is much cheaper than asking the archive. Only a fast
// tier that matches nothing sends this to the archive, which is exactly the
// case interceptWrite reads off a reply as matched 0.
//
// Failures are logged and the write goes on. The alternative is dropping a
// write because the archive was slow, and an unacknowledged write that reaches
// only the fast tier is the behaviour this system had all along.
func (c *conn) bringUpOneForUnacked(ctx context.Context, ns string, fdoc, collation bson.Raw, name string) {
	// An error here is not a match: it means the fast tier could not be asked,
	// and going on to the archive costs a lookup while believing it would cost
	// the write.
	if has, herr := c.r.fastTierMatches(ctx, ns, fdoc, collation); herr == nil && has {
		return
	}
	id, found, err := c.resolveMissByFilter(ctx, ns, fdoc, nil)
	if err != nil {
		c.log.Warn("could not ask the archive which document this unacknowledged write means, so it reaches only the fast tier",
			"ns", ns, "command", name, "err", err)
		return
	}
	if !found {
		return
	}
	copied, cerr := c.r.copyUp(ctx, ns, id)
	if cerr != nil {
		c.log.Warn("could not bring up the document an unacknowledged write names, so the write does not reach it",
			"ns", ns, "command", name, "err", cerr)
		return
	}
	if copied {
		c.r.Stats.WriteMisses.Add(1)
		c.cls = ClassMiss
		c.log.Debug("brought up the document an unacknowledged single-document write names", "ns", ns, "command", name)
	}
}

// isMaxTimeExpired reports whether an error is MongoDB's own deadline expiry.
//
// By code rather than by message: the text has changed between releases and
// this decides whether a client is told its deadline ran out or told something
// generic.
func isMaxTimeExpired(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Code == maxTimeMSExpiredCode
	}
	var we mongo.WriteException
	if errors.As(err, &we) && we.WriteConcernError != nil {
		return we.WriteConcernError.Code == maxTimeMSExpiredCode
	}
	return false
}

// maxTimeMSExpiredCode is MongoDB's MaxTimeMSExpired.
const maxTimeMSExpiredCode = 50
