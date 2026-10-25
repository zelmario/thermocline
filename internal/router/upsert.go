package router

import (
	"context"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/wire"
)

// An upsert is the one write shape that cannot be resolved after the fact.
//
// Every other write that matches nothing on the fast tier is put right on the
// way back: the archived document is copied up and the write is run again. An
// upsert does not wait to be put right. Matching nothing is its cue to create a
// document there and then, built from the filter's equalities and the update,
// and to report success.
//
// The document it creates carries the same _id as the archived one and almost
// none of its fields. It is not a stale copy of that record, it is a different
// record wearing its name, and the feeder mirrors it up as an upsert, so the
// archived version is overwritten by it. copyUp uses $setOnInsert precisely so a
// promotion never clobbers a newer fast tier copy, which means it cannot undo
// this either.
//
// Measured on a document held only in the archive, {_id: 5, name: "alice",
// tier: "gold", balance: 100}, with an ordinary
// updateOne({_id: 5}, {$set: {balance: 200}}, {upsert: true}): the reply said
// matched 0, upserted 1, the fast tier held {_id: 5, balance: 200}, and five
// seconds later so did the archive. name and tier were gone from both tiers.
// That is the only case in this system where data already safely archived is
// destroyed, and an upsert is how "insert or update" is written in every
// application, so it is not a corner.
//
// So it is judged before it runs. If the fast tier matches nothing but the
// archive does, the archived documents come up first, and the upsert then finds
// them and updates them, which is what the statement would have done with no
// tiering underneath it.

// guardUpsert brings archived documents up before an upsert can shadow them.
//
// It returns done=true only when a transaction has to be replayed. Otherwise it
// leaves the command to be forwarded as it always was, having made the fast tier
// hold what the statement is about to look for.
func (c *conn) guardUpsert(ctx context.Context, msg *wire.Message, db, name string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	coll, ok := stringField(cmd, name)
	if !ok && name == "findandmodify" {
		coll, ok = stringField(cmd, "findandmodify")
	}
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}

	// An upsert that matches nothing creates a document, and creates the
	// collection with it when the fast tier does not hold one. A collection
	// created by a write carries none of the archive's options, and a collation
	// cannot be added afterwards. See Indexer.EnsureCollection.
	c.r.ensureTieredCollection(ctx, ns)

	// A fast tier that holds all of history has nothing archived that is not
	// also here, so an upsert matching nothing here matches nothing anywhere and
	// creating a document is the right answer. This is the same claim the read
	// path trusts, and it keeps the cost of this guard at zero for every
	// collection that has been fully warmed.
	if mark, known := c.r.completeness(ctx, ns); known && mark.Claimed && mark.CoversAll() {
		return nil, false
	}

	for _, st := range upsertStatements(msg, cmd, name) {
		for _, filter := range st.filters() {
			if out, done := c.guardOneUpsert(ctx, msg, cmd, ns, filter, st.collation); done {
				return out, true
			}
		}
	}
	return nil, false
}

// guardOneUpsert is guardUpsert for one filter of one statement. See
// upsertStatement.filters.
func (c *conn) guardOneUpsert(ctx context.Context, msg *wire.Message, cmd bson.Raw, ns string,
	filter, collation bson.Raw) (*wire.Message, bool) {
	st := upsertStatement{filter: filter, collation: collation}
	// The fast tier deciding it has a match is the end of it: the update
	// applies there and no document is created. Asking it first also keeps
	// the archive out of the common path, where the document is hot.
	has, herr := c.r.fastTierMatchesHeld(ctx, ns, st.filter, st.collation, letOf(cmd))
	if herr != nil {
		c.log.Warn("could not check the fast tier before an upsert, letting it through",
			"ns", ns, "err", herr)
		return nil, false
	}
	if has {
		return nil, false
	}

	if inTransaction(cmd) {
		// The copy-up is an ordinary write outside the transaction, so the
		// caller's snapshot was taken before it landed and the statement
		// would still match nothing. Replaying is what makes it visible.
		switch out, res := c.warmForTxn(ctx, ns, st.filter, copyUpOptions{let: letOf(cmd), collation: st.collation, budget: maxTimeBudget(cmd)}, msg.Header.RequestID, msg.Header.RequestID,
			"the document this transaction upserts"); res {
		case txnWarmReplay:
			c.r.Stats.TxnReplays.Add(1)
			c.cls = ClassTxnWarm
			return out, true
		case txnWarmComplete:
			return nil, false
		}
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		out, terr := c.r.txnCannotSpanTiers(msg.Header.RequestID, msg.Header.RequestID, ns,
			"an upsert inside a transaction cannot reach a document the fast tier does not hold, "+
				"and creating one instead would overwrite the archived record")
		if terr != nil {
			return nil, false
		}
		return out, true
	}

	// An upsert's filter may refer to the command's let variables just as a
	// multi-document write's may, and this asks the archive that filter.
	n, cerr := c.r.copyUpMatching(ctx, ns, st.filter,
		copyUpOptions{let: letOf(cmd), collation: st.collation, budget: maxTimeBudget(cmd)})
	if cerr != nil {
		if out, isDeadline, ok := c.deadlineExpired(msg.Header.RequestID, msg.Header.RequestID, ns, "upsert", cerr); isDeadline {
			return out, ok
		}
		// Letting it through would write the stub. Refusing is the honest
		// answer: the alternative is silent loss of an archived record.
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused an upsert that could have overwritten an archived document",
			"ns", ns, "err", cerr)
		out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: refusing this upsert because the archive could not be checked, and an "+
				"upsert that does not find the archived document creates a new one in its place "+
				"and overwrites it: "+cerr.Error())
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	if n > 0 {
		c.cls = ClassMiss
		c.log.Info("copied documents up so an upsert would find them rather than replace them",
			"ns", ns, "documents", n)
	}
	return nil, false
}

// upsertFilters returns the filter of every statement in this command that would
// create a document when it matches nothing.
// upsertStatement is one upsert's filter and the collation that decides what it
// matches. The two travel together because asking the other tier the same
// filter under a different collation asks a different question.
type upsertStatement struct {
	filter    bson.Raw
	collation bson.Raw
	// update is the statement's u, or findAndModify's update: what the
	// document an upsert creates is built from besides the filter.
	update bson.RawValue
}

// filters is what the archive is asked before this upsert runs: its own
// filter, and the _id it would create when that can be read.
//
// The filter alone was not enough. An upsert that matches nothing creates a
// document whose _id comes from the filter's equality, the replacement or
// $setOnInsert, and the archive may hold that _id under a document the filter
// does not match: {_id: 5, name: "bob"} against an archived {_id: 5, name:
// "alice"}. MongoDB answers E11000 and writes nothing. Here the fast tier
// matched nothing, the archive matched nothing for the whole filter, the
// upsert created {_id: 5, name: "bob"}, and the feeder mirrored it over alice.
// Bringing _id 5 up first lets the fast tier's own _id index give mongod's
// answer.
func (st upsertStatement) filters() []bson.Raw {
	out := []bson.Raw{st.filter}
	id, ok := upsertCreatesID(st)
	if !ok {
		return out
	}
	if only, isOnly := idFromFilter(st.filter); isOnly && filterIsOnlyID(st.filter) && only.Equal(id) {
		return out
	}
	byID, err := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	if err != nil {
		return out
	}
	return append(out, byID)
}

// upsertCreatesID reads the _id an upsert creates when it matches nothing, as
// mongod builds it: the replacement's own _id, else $setOnInsert's or $set's,
// else the filter's equality on _id.
func upsertCreatesID(st upsertStatement) (bson.RawValue, bool) {
	if u, ok := st.update.DocumentOK(); ok {
		elems, _ := u.Elements()
		if len(elems) > 0 && !strings.HasPrefix(elems[0].Key(), "$") {
			if v, err := u.LookupErr("_id"); err == nil {
				return v, true
			}
		} else {
			for _, op := range []string{"$setOnInsert", "$set"} {
				if v, err := u.LookupErr(op, "_id"); err == nil {
					return v, true
				}
			}
		}
	}
	return idFromFilter(st.filter)
}

func upsertStatements(msg *wire.Message, cmd bson.Raw, name string) []upsertStatement {
	if name != "update" {
		// findAndModify carries one statement, in the command itself.
		if !boolField(cmd, "upsert") {
			return nil
		}
		doc, ok := cmd.Lookup("query").DocumentOK()
		if !ok {
			return nil
		}
		return []upsertStatement{{filter: doc, collation: docField(cmd, "collation"), update: cmd.Lookup("update")}}
	}

	var out []upsertStatement
	for _, st := range msg.AllOf("updates") {
		if !boolField(st, "upsert") {
			continue
		}
		doc, ok := st.Lookup("q").DocumentOK()
		if !ok {
			continue
		}
		out = append(out, upsertStatement{filter: doc, collation: docField(st, "collation"), update: st.Lookup("u")})
	}
	return out
}

// fastTierMatchesHeld reports whether the fast tier holds anything the filter
// selects, which is what decides whether an upsert creates a document. Every
// caller lets the statement through on a yes, and so relies on the match still
// being there when mongod runs it.
//
// That answer used to stand alone, with nothing holding the match. An eviction sweep can take the matched document between
// this answer and the write, and each caller then goes wrong its own way: an
// upsert matches nothing and creates a document, which the feeder mirrors over
// the archived record when the filter named its _id; an unacknowledged write
// misses with no reply for anything to repair; a findAndModify takes another
// document. The same window holdWhatIsThere closes for a copy-up.
//
// Held only when an eviction could reach it. The evictor raises the claim to
// its cutoff, waits for routers to read it, and then removes only documents
// older than that cutoff, so a match aged at or after the claim's bound cannot
// go before this statement runs. That keeps the hold, an archive write and an
// archive read, off the hot path: a document being upserted is usually recent.
// A match the age of which cannot be read is held.
//
// A no from the hold, the eviction having taken the match while it waited, is
// reported as no match here, and the caller goes on to the archive as it does
// for any miss.
func (r *Router) fastTierMatchesHeld(ctx context.Context, ns string, filter, collation, let bson.Raw) (bool, error) {
	match, found, err := r.fastTierMatch(ctx, ns, filter, collation, let)
	if err != nil || !found {
		return found, err
	}
	if r.notYetEvictable(ctx, ns, match) {
		return true, nil
	}
	id, lerr := match.LookupErr("_id")
	if lerr != nil {
		return true, nil
	}
	db, coll := splitNS(ns)
	have, _, herr := r.holdWhatIsThere(ctx, ns, r.hot.Database(db).Collection(coll), []bson.RawValue{id})
	if herr != nil {
		r.log.Debug("could not hold the fast tier's match against eviction", "ns", ns, "err", herr)
		return true, nil
	}
	return have[0], nil
}

// notYetEvictable reports whether the claim says no eviction can take this
// document yet. See fastTierMatchesHeld.
func (r *Router) notYetEvictable(ctx context.Context, ns string, match bson.Raw) bool {
	mark, known := r.completeness(ctx, ns)
	if !known || !mark.Claimed {
		return false
	}
	if mark.CoversAll() {
		return true
	}
	var age time.Time
	if f, ok := r.ageFieldOf(ns); ok {
		v, err := match.LookupErr(strings.Split(f, ".")...)
		if err != nil || v.Type != bson.TypeDateTime {
			return false
		}
		age = v.Time()
	} else {
		v, err := match.LookupErr("_id")
		if err != nil {
			return false
		}
		oid, ok := v.ObjectIDOK()
		if !ok {
			return false
		}
		age = oid.Timestamp()
	}
	return !age.Before(mark.From)
}

// fastTierMatch returns the _id and age field of one document the fast tier
// matches for filter.
//
// let carries the command's variables. A filter may refer to them with $expr,
// and asked without them mongod refuses it as an undefined variable, which the
// upsert guard read as "could not check" and let through: an upsert matching an
// archived document by $$t created a second record. See copyUpMatching.
func (r *Router) fastTierMatch(ctx context.Context, ns string, filter, collation, let bson.Raw) (bson.Raw, bool, error) {
	proj := bson.D{{Key: "_id", Value: 1}}
	if f, ok := r.ageFieldOf(ns); ok && f != "_id" {
		proj = append(proj, bson.E{Key: f, Value: 1})
	}
	db, coll := splitNS(ns)
	opts := options.Find().SetProjection(proj).SetLimit(1)
	if len(let) > 0 {
		opts.SetLet(let)
	}
	if len(collation) > 0 {
		// The same question the client's own statement will ask, asked the same
		// way. Without this the fast tier is searched case-sensitively for a
		// statement that is not, and an upsert that would have matched here is
		// judged as matching nothing.
		var c options.Collation
		if err := bson.Unmarshal(collation, &c); err == nil {
			opts = opts.SetCollation(&c)
		}
	}
	return findFirst(ctx, r.hot.Database(db).Collection(coll), filter, opts)
}
