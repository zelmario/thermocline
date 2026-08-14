package router

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmar/thermocline/internal/config"
	"github.com/zelmar/thermocline/internal/wire"
)

// intercept inspects one reply and decides whether the fast tier's answer stands.
func (c *conn) intercept(ctx context.Context, msg *wire.Message, name, db string, reply *wire.Message) (*wire.Message, error) {
	switch name {
	case "find":
		return c.interceptFind(ctx, msg, db, reply)
	case "update":
		return c.interceptWrite(ctx, msg, db, reply, "update", "updates", "q")
	case "delete":
		return c.interceptWrite(ctx, msg, db, reply, "delete", "deletes", "q")
	case "findAndModify", "findandmodify":
		return c.interceptFindAndModify(ctx, msg, db, reply)
	default:
		return reply, nil
	}
}

// interceptFind handles a read that came back empty.
//
// An empty result is ambiguous: the document may not exist, or it may have been
// moved to the archive. Only the second case is a wrong answer, and only a
// lookup by id can be resolved cheaply enough to check on every miss.
func (c *conn) interceptFind(ctx context.Context, msg *wire.Message, db string, reply *wire.Message) (*wire.Message, error) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return reply, nil
	}
	coll, ok := stringField(cmd, "find")
	if !ok {
		return reply, nil
	}
	ns := db + "." + coll

	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return reply, nil
	}
	batch, ok := firstBatch(rdoc)
	if !ok {
		return reply, nil
	}

	filter, _ := cmd.LookupErr("filter")
	fdoc, _ := filter.DocumentOK()

	// A lookup by exact id matches at most one document, so it has no
	// completeness question to answer. Either the fast tier holds that document
	// or the archive does, and both cases are resolved below. Running the
	// completeness guard here would refuse point reads for old records, which
	// are precisely the reads this system exists to serve.
	id, hasID := idFromFilter(fdoc)
	if !hasID {
		return c.guardCompleteness(ctx, ns, fdoc, msg, reply)
	}
	if len(batch) > 0 {
		return reply, nil
	}

	c.r.Stats.ReadMisses.Add(1)
	doc, found, err := c.r.readArchived(ctx, ns, id)
	if err != nil {
		return nil, err
	}
	if !found {
		// Genuinely absent from both tiers. The empty answer was correct.
		return reply, nil
	}

	// Inside a transaction, answering from the archive would mix two clusters
	// into one snapshot. Copy the document up and replay instead, so the whole
	// transaction reads a fast tier that holds it.
	if inTransaction(cmd) {
		if _, cerr := c.r.copyUp(ctx, ns, id); cerr != nil {
			return nil, cerr
		}
		c.r.Stats.TxnReplays.Add(1)
		c.log.Debug("replaying a transaction after a read miss", "ns", ns)
		return c.r.replayTransaction(reply.Header.RequestID, msg.Header.RequestID,
			"this transaction read a document that had been archived, and it has now been restored to the fast tier")
	}

	// Reply first, copy up afterwards. The caller already has what it asked
	// for, and making it wait on a write it did not request would turn a 30 ms
	// miss into a slower one for no benefit.
	out, err := c.r.singleDocReply(msg, reply, ns, doc)
	if err != nil {
		return nil, err
	}
	c.r.copyUpAsync(ns, id)
	return out, nil
}

// interceptWrite handles a write that matched nothing.
//
// This is the dangerous case. An update matching no document is reported to the
// driver as success, so an application updating an archived record is told the
// write happened when nothing did. Unlike a read, this has to be resolved
// before replying: copy the document up, then let the original command run
// again against a fast tier that now holds it.
func (c *conn) interceptWrite(ctx context.Context, msg *wire.Message, db string, reply *wire.Message, cmdField, arrayField, filterField string) (*wire.Message, error) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return reply, nil
	}
	coll, ok := stringField(cmd, cmdField)
	if !ok {
		return reply, nil
	}
	ns := db + "." + coll

	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return reply, nil
	}
	if matched(rdoc) != 0 {
		return reply, nil
	}
	// One statement at a time. A bulk write that matched nothing could involve
	// many archived documents, and resolving them individually here would turn
	// one round trip into many. Those are left to the fast tier's answer.
	if msg.CountOf(arrayField) != 1 {
		return reply, nil
	}

	stmt, ok := msg.FirstOf(arrayField)
	if !ok {
		return reply, nil
	}
	fval, err := stmt.LookupErr(filterField)
	if err != nil {
		return reply, nil
	}
	fdoc, _ := fval.DocumentOK()
	id, hasID := idFromFilter(fdoc)
	if !hasID {
		return reply, nil
	}

	c.r.Stats.WriteMisses.Add(1)
	copied, err := c.r.copyUp(ctx, ns, id)
	if err != nil {
		return nil, err
	}
	if !copied {
		// Not in the archive either, so matching nothing was the truth.
		return reply, nil
	}

	// Inside a transaction the copy-up landed outside the caller's snapshot, so
	// re-running the statement here would miss again. Ask for a replay instead.
	if inTransaction(cmd) {
		c.r.Stats.TxnReplays.Add(1)
		c.log.Debug("replaying a transaction after a copy-up", "ns", ns)
		return c.r.replayTransaction(reply.Header.RequestID, msg.Header.RequestID,
			"this transaction wrote to a document that had been archived, and it has now been restored to the fast tier")
	}

	return c.retry(msg)
}

// interceptFindAndModify handles the read-and-write command the same way.
func (c *conn) interceptFindAndModify(ctx context.Context, msg *wire.Message, db string, reply *wire.Message) (*wire.Message, error) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return reply, nil
	}
	coll, ok := stringField(cmd, "findAndModify")
	if !ok {
		if coll, ok = stringField(cmd, "findandmodify"); !ok {
			return reply, nil
		}
	}
	ns := db + "." + coll

	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return reply, nil
	}
	// value null and n zero together mean nothing was found to act on.
	if v, lookErr := rdoc.LookupErr("value"); lookErr == nil && v.Type != bson.TypeNull {
		return reply, nil
	}

	qval, err := cmd.LookupErr("query")
	if err != nil {
		return reply, nil
	}
	fdoc, _ := qval.DocumentOK()
	id, hasID := idFromFilter(fdoc)
	if !hasID {
		return reply, nil
	}

	c.r.Stats.WriteMisses.Add(1)
	copied, err := c.r.copyUp(ctx, ns, id)
	if err != nil {
		return nil, err
	}
	if !copied {
		return reply, nil
	}
	if inTransaction(cmd) {
		c.r.Stats.TxnReplays.Add(1)
		c.log.Debug("replaying a transaction after a copy-up", "ns", ns)
		return c.r.replayTransaction(reply.Header.RequestID, msg.Header.RequestID,
			"this transaction acted on a document that had been archived, and it has now been restored to the fast tier")
	}
	return c.retry(msg)
}

// retry re-sends the client's original command now that the fast tier holds the
// document, and returns the fresh reply.
func (c *conn) retry(msg *wire.Message) (*wire.Message, error) {
	if err := wire.Write(c.upstream, msg); err != nil {
		return nil, fmt.Errorf("retry after copy-up: %w", err)
	}
	reply, err := wire.Read(c.upstream)
	if err != nil {
		return nil, fmt.Errorf("read retry reply: %w", err)
	}
	return reply, nil
}

// guardCompleteness decides whether a range answer from the fast tier can be
// trusted, and what to do when it cannot.
//
// The fast tier is complete only for documents newer than the watermark. A
// query reaching past it may be answered with some of the matching documents
// and no indication that the rest exist. That looks exactly like success, which
// is why the strict policy refuses rather than guesses.
func (c *conn) guardCompleteness(ctx context.Context, ns string, filter bson.Raw, msg *wire.Message, reply *wire.Message) (*wire.Message, error) {
	mark, known := c.r.completeness(ctx, ns)

	if known && mark.Claimed {
		bound, bounded := lowerTimeBound(filter)
		switch {
		case bounded && mark.Complete(bound):
			return reply, nil
		case !bounded && mark.CoversAll():
			// No lower bound, so the query may reach arbitrarily far back. Only
			// a tier claiming all of history can answer that on its own.
			return reply, nil
		}
	}

	// Everything below this line is a query the fast tier cannot be shown to
	// answer in full. The three reasons need different words, because they need
	// different things done about them.
	var because, remedy string
	switch {
	case !known:
		because = "the completeness watermark could not be read"
		remedy = "the archive is unreachable, so completeness cannot be established; retry once it is back"
	case !mark.Claimed:
		because = "nothing has recorded that the fast tier is complete for this namespace"
		remedy = "run the warmer, or if the fast tier holds all of history run: cline watermark --ns " + ns + " --complete-all"
	default:
		because = "this query reaches earlier than " + mark.From.Format(time.RFC3339) +
			", which the fast tier does not hold completely"
		remedy = "bound the query to that range, or query the archive directly"
	}

	c.r.Stats.Incomplete.Add(1)

	// A transaction is the one place the archive fallback cannot go. Reading
	// one tier inside the caller's snapshot and the other outside it is not a
	// transaction any more, and no copy-up fixes a whole range.
	if cmd, cerr := msg.CommandDoc(); cerr == nil && inTransaction(cmd) {
		c.r.Stats.Refused.Add(1)
		c.log.Warn("refused a transactional read that spans both tiers", "ns", ns, "because", because)
		return c.r.txnCannotSpanTiers(reply.Header.RequestID, msg.Header.RequestID, ns, because)
	}

	// The archive holds every document, so it answers in full what the fast
	// tier cannot. This is what keeps a query the application already had
	// working, instead of turning it into an error nobody asked for.
	if c.r.cfg.Completeness == config.Archive {
		db, coll := splitNS(ns)
		out, aerr := c.answerFromArchive(ctx, msg, db, coll)
		if aerr == nil {
			c.log.Debug("read answered from the archive", "ns", ns, "because", because)
			return out, nil
		}
		c.log.Warn("could not answer this read from the archive, refusing instead", "ns", ns, "err", aerr)
	}

	if c.r.cfg.Completeness == config.Permissive {
		c.log.Warn("answer may be short", "ns", ns, "because", because)
		return reply, nil
	}

	c.r.Stats.Refused.Add(1)
	c.log.Warn("refused a query that would return a short answer", "ns", ns, "because", because)
	return c.r.errorReply(msg, reply, fmt.Sprintf(
		"thermocline: refusing this query because %s. %s. To allow short answers instead, set router.completeness to permissive.",
		because, remedy))
}
