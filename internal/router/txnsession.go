package router

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// Everything the router does to keep the two tiers consistent has so far run on
// its own client: key claims, dry runs, copy-ups. That is right for ordinary
// traffic and wrong inside a transaction, because the router's client cannot see
// the caller's uncommitted writes and the caller cannot see the router's.
//
// It does not have to be that way. A transaction lives on the server, keyed by
// the session id, and this router already holds a connection per client that
// carries that session. A command sent on that connection with the caller's own
// lsid and txnNumber is part of the caller's transaction, sees exactly what the
// caller sees, and takes no locks the caller does not already hold.
//
// That is what this file provides: a way to ask a question inside somebody
// else's transaction.

// txnFields are the session fields that place a command inside a transaction.
type txnFields struct {
	lsid      bson.RawValue
	txnNumber bson.RawValue
	// open is false for the command that starts the transaction. Nothing can be
	// asked before it, because the transaction does not exist yet, and sending
	// startTransaction ourselves would take the caller's place and make their
	// own command fail as a duplicate start.
	open bool
}

// txnOf reads the session fields out of a command, if it is in a transaction.
func txnOf(cmd bson.Raw) (txnFields, bool) {
	if !inTransaction(cmd) {
		return txnFields{}, false
	}
	lsid, err := cmd.LookupErr("lsid")
	if err != nil {
		return txnFields{}, false
	}
	num, err := cmd.LookupErr("txnNumber")
	if err != nil {
		return txnFields{}, false
	}
	open := true
	if val, serr := cmd.LookupErr("startTransaction"); serr == nil {
		if b, ok := val.BooleanOK(); ok && b {
			open = false
		}
	}
	return txnFields{lsid: lsid, txnNumber: num, open: open}, true
}

// inCallerTxn appends the session fields that continue the caller's transaction.
//
// readConcern, writeConcern and startTransaction are deliberately not carried
// over. MongoDB accepts them only on the command that begins a transaction, and
// repeating them on a later one is an error.
func (t txnFields) inCallerTxn(cmd bson.D, db string) bson.D {
	cmd = append(cmd,
		bson.E{Key: "lsid", Value: t.lsid},
		bson.E{Key: "txnNumber", Value: t.txnNumber},
		bson.E{Key: "autocommit", Value: false},
		bson.E{Key: "$db", Value: db},
	)
	return cmd
}

// askInCallerTxn runs one command inside the caller's transaction and returns
// its reply.
//
// It borrows the client's own connection, which is safe because the router is
// answering that client's command right now and will not read another until it
// has replied. A transaction permits one operation at a time, and this is that
// one operation.
func (c *conn) askInCallerTxn(t txnFields, db string, cmd bson.D) (bson.Raw, error) {
	msg, err := wire.NewMsg(0, 0, t.inCallerTxn(cmd, db))
	if err != nil {
		return nil, err
	}
	if err := wire.Write(c.upstream, msg); err != nil {
		c.upstreamBroke()
		return nil, fmt.Errorf("ask inside the caller's transaction: %w", err)
	}
	reply, err := wire.Read(c.upstream)
	if err != nil {
		c.upstreamBroke()
		return nil, fmt.Errorf("read inside the caller's transaction: %w", err)
	}
	doc, err := reply.ReplyDoc()
	if err != nil {
		return nil, err
	}
	if !replyOK(doc) {
		return doc, fmt.Errorf("inside the caller's transaction: %s", errmsgOf(doc))
	}
	return doc, nil
}

// findInCallerTxn reads documents inside the caller's transaction, so the answer
// includes writes the transaction has made and nobody else can see yet.
//
// The batch is taken whole: a cursor left open inside somebody else's
// transaction would be a cursor they did not create and cannot be made to close,
// and MongoDB refuses a getMore across that boundary anyway.
func (c *conn) findInCallerTxn(t txnFields, ns string, filter any, sort bson.Raw, limit int64, idsOnly bool) ([]bson.Raw, error) {
	db, coll := splitNS(ns)
	cmd := bson.D{{Key: "find", Value: coll}}
	if filter != nil {
		cmd = append(cmd, bson.E{Key: "filter", Value: filter})
	}
	if len(sort) > 0 {
		cmd = append(cmd, bson.E{Key: "sort", Value: sort})
	}
	if idsOnly {
		// The pre-read only needs to know which documents will be touched, and
		// projecting away everything else keeps a multi-statement's batch small
		// enough to come back in one piece.
		cmd = append(cmd, bson.E{Key: "projection", Value: bson.D{{Key: "_id", Value: 1}}})
	}
	if limit <= 0 {
		limit = maxKeyedUpdate
	}
	cmd = append(cmd, bson.E{Key: "limit", Value: limit})
	// batchSize must be set with singleBatch, and set to the whole limit. The
	// server otherwise returns its default first batch of 101 documents and
	// closes the cursor, so an update touching more than that had the rest of
	// its post-images silently unchecked, which is the hole this guard exists
	// to close.
	cmd = append(cmd, bson.E{Key: "batchSize", Value: limit})
	// singleBatch keeps the server from leaving a cursor behind: a cursor
	// opened inside somebody else's transaction is one they did not create and
	// cannot be made to close, and MongoDB refuses a getMore across that
	// boundary anyway.
	cmd = append(cmd, bson.E{Key: "singleBatch", Value: true})

	doc, err := c.askInCallerTxn(t, db, cmd)
	if err != nil {
		return nil, err
	}
	vals, ok := firstBatch(doc)
	if !ok {
		return nil, fmt.Errorf("no batch in the reply from %s", ns)
	}
	out := make([]bson.Raw, 0, len(vals))
	for _, v := range vals {
		d, dok := v.DocumentOK()
		if !dok {
			continue
		}
		// The reply's buffer is reused, so each document is taken away whole.
		doc := make(bson.Raw, len(d))
		copy(doc, d)
		out = append(out, doc)
	}
	return out, nil
}

// abortCallerTxn ends the caller's transaction from underneath them.
//
// This is what makes a refusal after the write as safe as a refusal before it.
// The write is already in the transaction and invisible to everyone else;
// aborting throws it away, so the duplicate the router is refusing can never
// become visible even if the application ignores the error and commits.
func (c *conn) abortCallerTxn(t txnFields) error {
	_, err := c.askInCallerTxn(t, "admin", bson.D{{Key: "abortTransaction", Value: 1}})
	return err
}

// errmsgOf reads the message off a failed reply.
func errmsgOf(doc bson.Raw) string {
	if val, err := doc.LookupErr("errmsg"); err == nil {
		if s, ok := val.StringValueOK(); ok {
			return s
		}
	}
	return "no error message"
}
