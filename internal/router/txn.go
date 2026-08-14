package router

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmar/thermocline/internal/wire"
)

// A transaction cannot span two clusters, so copying a document up and re-running
// the statement inside the caller's transaction does not work: the copy-up is an
// ordinary write outside the transaction, and the transaction's snapshot was
// taken before it landed. The statement would miss again, and the application
// would be told its update succeeded when nothing happened.
//
// The way out is the contract drivers already implement. A transaction that
// fails with the TransientTransactionError label is retried from the start,
// automatically, by every official driver's withTransaction helper. So the
// router copies the document up outside the transaction, then asks for the whole
// transaction to be replayed. The replay takes a fresh snapshot, the document is
// there, and the application never learns why.
//
// That is the difference between this and refusing: the application keeps the
// code it already has.

// transientLabel is the label drivers watch for to retry a whole transaction.
const transientLabel = "TransientTransactionError"

// inTransaction reports whether a command is part of a multi-document
// transaction.
//
// A retryable write carries lsid and txnNumber too, so those two are not enough
// on their own. autocommit:false is what marks a real transaction, and treating
// a retryable write as one would ask for a replay that the driver has no
// callback to run.
func inTransaction(cmd bson.Raw) bool {
	if _, err := cmd.LookupErr("lsid"); err != nil {
		return false
	}
	if _, err := cmd.LookupErr("txnNumber"); err != nil {
		return false
	}
	val, err := cmd.LookupErr("autocommit")
	if err != nil {
		return false
	}
	b, ok := val.BooleanOK()
	return ok && !b
}

// replayTransaction tells the driver to run the caller's transaction again.
//
// WriteConflict is the code a server uses when a transaction could not proceed
// and deserves another attempt, which is exactly the situation here. The label
// is what the driver actually acts on.
func (r *Router) replayTransaction(requestID, responseTo int32, why string) (*wire.Message, error) {
	return wire.NewMsg(requestID, responseTo, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: "thermocline: " + why + ". The transaction is being replayed so it can see it."},
		{Key: "code", Value: int32(112)},
		{Key: "codeName", Value: "WriteConflict"},
		{Key: "errorLabels", Value: bson.A{transientLabel}},
	})
}

// txnCannotSpanTiers reports a transaction that no replay will fix.
//
// A range read inside a transaction that reaches past what the fast tier holds
// cannot be resolved by copying one document up, and answering it from the
// archive would leave the transaction reading two clusters at two different
// points in time. This is deliberately not labelled transient: retrying it
// forever would turn a clear failure into a hang.
func (r *Router) txnCannotSpanTiers(requestID, responseTo int32, ns, because string) (*wire.Message, error) {
	return wire.NewMsg(requestID, responseTo, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: "thermocline: this transaction reads " + ns +
			" beyond what the fast tier holds, and a transaction cannot span both tiers, because " + because +
			". Warm that range first, or move this read outside the transaction."},
		{Key: "code", Value: int32(125)},
		{Key: "codeName", Value: "CommandFailed"},
	})
}
