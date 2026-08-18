package router

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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

	// A fast tier that holds all of history has nothing archived that is not
	// also here, so an upsert matching nothing here matches nothing anywhere and
	// creating a document is the right answer. This is the same claim the read
	// path trusts, and it keeps the cost of this guard at zero for every
	// collection that has been fully warmed.
	if mark, known := c.r.completeness(ctx, ns); known && mark.Claimed && mark.CoversAll() {
		return nil, false
	}

	for _, st := range upsertFilters(msg, cmd, name) {
		// The fast tier deciding it has a match is the end of it: the update
		// applies there and no document is created. Asking it first also keeps
		// the archive out of the common path, where the document is hot.
		has, herr := c.r.fastTierMatches(ctx, ns, st)
		if herr != nil {
			c.log.Warn("could not check the fast tier before an upsert, letting it through",
				"ns", ns, "err", herr)
			continue
		}
		if has {
			continue
		}

		if inTransaction(cmd) {
			// The copy-up is an ordinary write outside the transaction, so the
			// caller's snapshot was taken before it landed and the statement
			// would still match nothing. Replaying is what makes it visible.
			switch out, res := c.warmForTxn(ctx, ns, st, msg.Header.RequestID, msg.Header.RequestID,
				"the document this transaction upserts"); res {
			case txnWarmReplay:
				c.r.Stats.TxnReplays.Add(1)
				c.cls = ClassTxnWarm
				return out, true
			case txnWarmComplete:
				continue
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

		n, cerr := c.r.copyUpMatching(ctx, ns, st, nil)
		if cerr != nil {
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
	}
	return nil, false
}

// upsertFilters returns the filter of every statement in this command that would
// create a document when it matches nothing.
func upsertFilters(msg *wire.Message, cmd bson.Raw, name string) []bson.Raw {
	if name != "update" {
		// findAndModify carries one statement, in the command itself.
		if !boolField(cmd, "upsert") {
			return nil
		}
		doc, ok := cmd.Lookup("query").DocumentOK()
		if !ok {
			return nil
		}
		return []bson.Raw{doc}
	}

	var out []bson.Raw
	for _, st := range msg.AllOf("updates") {
		if !boolField(st, "upsert") {
			continue
		}
		doc, ok := st.Lookup("q").DocumentOK()
		if !ok {
			continue
		}
		out = append(out, doc)
	}
	return out
}

// fastTierMatches reports whether the fast tier holds anything the filter
// selects, which is what decides whether an upsert creates a document.
func (r *Router) fastTierMatches(ctx context.Context, ns string, filter bson.Raw) (bool, error) {
	db, coll := splitNS(ns)
	err := r.hot.Database(db).Collection(coll).
		FindOne(ctx, filter, options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).
		Err()
	if err == nil {
		return true, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	return false, err
}
