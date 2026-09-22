package router

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
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
// opts is the caller's own, and every field of it changes the answer. This
// copy-up runs the caller's filter against the archive, so it is the same query
// its sibling copy-up outside a transaction runs and it needs the same three
// things:
//
//   - collation, because a filter means different documents in a different one.
//     Matching none of them is the bad case rather than the obvious one: the
//     answer below reads that as completeness and the transaction goes on to
//     read the fast tier alone, short, with no error on it.
//   - let, because a filter reading $$variables the archive is not given is a
//     filter the archive rejects, which refuses a statement that works.
//   - budget, because the caller's maxTimeMS is a bound on the work they agreed
//     to and this is work they cannot see.
//
// It took copyUpOptions{} for all three. The type is passed whole rather than
// the fields taken one at a time, so a call site builds it exactly as the
// copy-up beside it does and the next field added reaches both.
func (c *conn) warmForTxn(ctx context.Context, ns string, filter bson.Raw, opts copyUpOptions, requestID, responseTo int32, what string) (*wire.Message, txnWarm) {
	n, err := c.r.copyUpMatching(ctx, ns, filter, opts)
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

// warmScopeForTxn brings up everything an aggregation inside a transaction
// reads, across every collection it touches.
//
// The primary collection is warmed through the pipeline's leading $match, which
// is the same bound the completeness check uses. A joined collection is warmed
// whole, because $lookup and $unionWith read by key rather than by time and
// there is no range that can be shown to be enough. That makes a join against a
// large archived collection exceed the cap and be refused, which is the honest
// outcome: the alternative is dragging the collection into the fast tier to
// answer one query.
//
// Everything is warmed before anything is replayed. Replaying after the first
// namespace would work, since each attempt makes progress, but it would cost
// one replay per collection instead of one for the statement.
func (c *conn) warmScopeForTxn(ctx context.Context, msg *wire.Message, scope aggScope) (*wire.Message, txnWarm) {
	seen := map[string]bool{}
	moved := 0

	// An aggregate carries one collation, one let and one maxTimeMS for the
	// whole pipeline, so every namespace warmed for it is warmed with the same
	// ones. See warmForTxn for what passing none does.
	var opts copyUpOptions
	if cmd, cerr := msg.CommandDoc(); cerr == nil {
		opts = copyUpOptions{
			let:       letOf(cmd),
			collation: docField(cmd, "collation"),
			budget:    maxTimeBudget(cmd),
		}
	}
	warm := func(ns string, filter bson.Raw) txnWarm {
		if ns == "" || seen[ns] || !c.r.cfg.Manages(ns) {
			return txnWarmComplete
		}
		seen[ns] = true
		n, err := c.r.copyUpMatching(ctx, ns, filter, opts)
		if err != nil {
			c.log.Warn("could not bring this transaction's documents up from the archive, refusing instead",
				"ns", ns, "err", err)
			return txnWarmRefuse
		}
		moved += n
		return txnWarmComplete
	}

	if res := warm(scope.primary, scope.match); res == txnWarmRefuse {
		return nil, txnWarmRefuse
	}
	for _, ns := range scope.secondary {
		// No filter: a joined collection has to be whole.
		if res := warm(ns, bson.Raw{}); res == txnWarmRefuse {
			return nil, txnWarmRefuse
		}
	}

	if moved == 0 {
		c.log.Debug("the fast tier already held everything this aggregation reads", "ns", scope.primary)
		return nil, txnWarmComplete
	}

	c.r.Stats.WarmedForTxn.Add(1)
	c.log.Info("copied documents up so a transaction could aggregate over them",
		"ns", scope.primary, "documents", moved, "collections", len(seen))

	out, err := c.r.replayTransaction(msg.Header.RequestID, msg.Header.RequestID,
		"the documents this transaction aggregates over in "+scope.primary+
			" were in the archive and have been brought back to the fast tier")
	if err != nil {
		return nil, txnWarmRefuse
	}
	return out, txnWarmReplay
}
