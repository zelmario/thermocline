package router

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmar/thermocline/internal/wire"
)

// A transaction cannot read the archive. Reading one tier inside the caller's
// snapshot and the other outside it is not a transaction any more, so a read
// that reaches past what the fast tier holds used to be refused outright.
//
// Refusing is honest but it is not transparent, and transparency is the whole
// promise: an application that worked before adopting Thermocline has to keep
// working. A point lookup already had the answer, which is that the transaction
// does not need to read the archive if the documents are moved to where it is
// already reading. Copy them up outside the transaction, then ask the driver to
// replay it. The second attempt takes a fresh snapshot, finds them on the fast
// tier, and the application never learns anything happened.
//
// The only thing that changes for a range is how much has to move, and that is
// a question with a number rather than a question of kind. Up to
// maxCopyUpForWrite documents are brought back; past that the refusal stands,
// because dragging an unbounded range out of the archive to satisfy one
// transaction is a worse answer than a clear error.

// txnWarm says what warmForTxn managed to do.
type txnWarm int

const (
	// txnWarmReplay: documents were moved up. The caller's snapshot was taken
	// before they landed, so only a replay can see them.
	txnWarmReplay txnWarm = iota
	// txnWarmComplete: nothing was missing. This is the per-query completeness
	// proof that lets the transaction proceed on the fast tier alone.
	txnWarmComplete
	// txnWarmRefuse: too much to move, or the archive could not be read.
	txnWarmRefuse
)

// warmForTxn makes the fast tier able to answer a transaction's query on its own.
//
// The watermark cannot help here. It records that a whole range was warmed, and
// copying documents up for one query does not make that true, so a replay that
// re-checked the watermark would fail exactly as the first attempt did. What is
// true after a copy-up is narrower and stronger: the archive holds no live
// document matching this filter that the fast tier lacks. That is completeness
// for this query, established per query, and it is what these two answers mean.
func (c *conn) warmForTxn(ctx context.Context, ns string, filter bson.Raw, requestID, responseTo int32, what string) (*wire.Message, txnWarm) {
	n, err := c.r.copyUpMatching(ctx, ns, filter)
	if err != nil {
		c.log.Warn("could not bring this transaction's documents up from the archive, refusing instead",
			"ns", ns, "err", err)
		return nil, txnWarmRefuse
	}
	if n == 0 {
		c.log.Debug("the fast tier already held everything this transaction matches",
			"ns", ns, "what", what)
		return nil, txnWarmComplete
	}

	c.r.Stats.WarmedForTxn.Add(1)
	c.log.Info("copied documents up so a transaction could see them",
		"ns", ns, "documents", n, "what", what)

	out, terr := c.r.replayTransaction(requestID, responseTo,
		what+" in "+ns+" were in the archive and have been brought back to the fast tier")
	if terr != nil {
		return nil, txnWarmRefuse
	}
	return out, txnWarmReplay
}

// txnFilter reads the filter a command selects documents with, whatever the
// command calls it.
func txnFilter(cmd bson.Raw, name string) (bson.Raw, bool) {
	key := "filter"
	if name == "count" || name == "distinct" || name == "findAndModify" || name == "findandmodify" {
		key = "query"
	}
	val, err := cmd.LookupErr(key)
	if err != nil {
		// A command with no filter at all reads everything, which is a filter
		// too and the one most likely to exceed the cap.
		return bson.Raw{}, true
	}
	doc, ok := val.DocumentOK()
	if !ok {
		return nil, false
	}
	return doc, true
}
