package router

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/wire"
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
	case "listDatabases":
		return c.interceptListDatabases(ctx, msg, reply)
	case "listCollections":
		return c.interceptListCollections(ctx, msg, db, reply)
	case "collStats":
		return c.interceptCollStats(ctx, msg, db, reply)
	case "dbStats":
		return c.interceptDBStats(ctx, msg, db, reply)
	case "aggregate":
		// A read-only newest-N pipeline is the one an aggregation defers to
		// here, because judging it early cannot use its own answer. See
		// aggnewest.go.
		if out, done := c.interceptAggNewest(ctx, msg, db, reply); done {
			return out, nil
		}
		// The fast tier ran this because a claim said it could, and an
		// aggregation returns 101 documents and a cursor for the rest. The
		// claims are registered against that cursor so a getMore can be refused
		// once they no longer hold. Nil for anything the guard did not
		// authorise, which registers nothing.
		c.noteTieredCursor(c.aggAuth, reply)
		// Otherwise only a pipeline led by $collStats, which is what
		// db.coll.stats() sends. Everything else an aggregate can be is
		// decided before it runs.
		return c.interceptAggCollStats(ctx, msg, db, reply)
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

	// A collection Thermocline does not tier lives only on the fast tier, so an
	// empty answer from it is the whole truth.
	//
	// The guard above is skipped for a point read on purpose, because a lookup
	// by exact id has no completeness question to answer, and the tiering
	// question went with it. Measured on an untiered collection whose documents
	// were still on the old cluster: countDocuments answered zero, correctly,
	// while findOne by _id returned a document from the archive and copied it
	// up. Two reads of one collection disagreeing is bad enough; the copy-up is
	// worse, because it writes into a collection nothing manages and the feeder
	// will never mirror it, so the tiers disagree about it from then on.
	//
	// This is the shape of every adoption, where the existing cluster becomes
	// the archive carrying every collection and only the tiered ones are ever
	// filled into the fast tier.
	if _, _, tiered := c.r.tieredBase(ctx, ns); !tiered {
		return reply, nil
	}

	// Linearizable cannot be honoured by another cluster, and downgrading the
	// strongest guarantee in the system without saying so is the one thing this
	// must not do.
	// The level itself is carried across by rewriteForArchive along with the
	// rest of the command, so only the refusal is decided here.
	_, honourable := archiveLevel(cmd)
	if !honourable {
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a linearizable read that only the archive can answer", "ns", ns)
		return c.r.commandError(reply.Header.RequestID, msg.Header.RequestID,
			"thermocline: this document is in the archive, and readConcern linearizable cannot be "+
				"answered from there: it promises a read confirmed on the primary that owns the "+
				"document. Warm this range, or read it with majority.")
	}

	// Inside a transaction, answering from the archive would mix two clusters
	// into one snapshot. Copy the document up and replay instead, so the whole
	// transaction reads a fast tier that holds it.
	//
	// copyUp reports whether the document was there at all, which is the same
	// question the read below has to answer, so it is asked once.
	if inTransaction(cmd) {
		// The whole filter decides, not the _id in it. Asking the archive for
		// the _id alone copies up a document this find does not match, the
		// replay finds nothing again, and the two go round until the driver
		// gives up. See filterIsOnlyID for what that was measured to cost.
		//
		// resolveMissByFilter rather than a second answerFromArchive: it asks
		// the same question in one round trip with an _id projection, it is
		// what the two write paths below already use, and it costs nothing at
		// all on a namespace whose claim covers the read.
		if !filterIsOnlyID(fdoc) {
			resolved, ok, rerr := c.resolveMissByFilter(ctx, ns, fdoc, nil)
			if rerr != nil {
				return nil, rerr
			}
			if !ok {
				// The archive does not match it either, so the empty answer
				// was correct.
				return reply, nil
			}
			id = resolved
		}
		copied, cerr := c.r.copyUp(ctx, ns, id)
		if cerr != nil {
			return nil, cerr
		}
		if !copied {
			// Genuinely absent from both tiers. The empty answer was correct.
			return reply, nil
		}
		c.r.Stats.TxnReplays.Add(1)
		c.cls = ClassTxnWarm
		c.log.Debug("replaying a transaction after a read miss", "ns", ns)
		return c.r.replayTransaction(reply.Header.RequestID, msg.Header.RequestID,
			"this transaction read a document that had been archived, and it has now been restored to the fast tier")
	}

	// The archive answers the command the client sent, not a lookup by _id.
	//
	// This used to read the document by its _id and hand it back, and an _id is
	// not the whole of a filter. Measured by the differential harness:
	// find({_id: 33, f: {$gt: 13.75}}) where document 33 holds f: 8.25 and
	// lives only in the archive. MongoDB returns nothing and the router
	// returned the document, so a query was answered with a record that does
	// not match it. A projection and a skip went the same way, both ignored by
	// a reply built here from the document alone.
	//
	// The point read keeps its exemption from the completeness guard, which is
	// what it is for: at most one document can match an _id equality, so no
	// claim about how much of history the fast tier holds is needed to answer
	// it. What the exemption never justified was answering a different
	// question. rewriteForArchive carries the filter, projection, skip, limit,
	// collation, hint and read concern across, and the freshness wait a miss
	// needs before it can treat the archive's copy as the truth happens inside
	// answerFromArchive with every other archive read.
	out, err := c.answerFromArchive(ctx, msg, db, coll, false)
	if err != nil {
		return nil, err
	}

	// Promoted only if the client actually got it. A filter that matched
	// nothing leaves nothing hot to promote, and copying up regardless would
	// write a document into the fast tier that nobody read and announce it on
	// every change stream watching the collection.
	//
	// After the reply is built, not before: the caller already has what it
	// asked for, and making it wait on a write it did not request would turn a
	// 30 ms miss into a slower one for no benefit.
	//
	// Which of the two this was is also settled here, and it follows the same
	// line: promoted makes it a miss, and not promoted makes it an ordinary
	// archive read, which is exactly how Class defines them.
	if replyCarriesDocuments(out) {
		c.r.Stats.ReadMisses.Add(1)
		c.cls = ClassMiss
		c.r.copyUpAsync(ns, id)
	} else {
		// Nothing to promote, so this was an ordinary archive read and is
		// counted as one. It used to be counted as a read miss whatever came
		// back, including when the document was in neither tier, which the
		// counter's own description says it is not: "point lookups that found
		// the document only in the archive".
		c.r.Stats.Archived.Add(1)
		c.cls = ClassArchive
	}
	return out, nil
}

// replyCarriesDocuments reports whether a reply's first batch holds anything.
func replyCarriesDocuments(reply *wire.Message) bool {
	if reply == nil {
		return false
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil {
		return false
	}
	batch, ok := firstBatch(rdoc)
	return ok && len(batch) > 0
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
	if hasID && !filterIsOnlyID(fdoc) {
		// An _id and something else. The something else decides whether the
		// document that _id names is one this statement matches, so the miss is
		// resolved against the whole filter like any other. See filterIsOnlyID.
		hasID = false
	}
	if !hasID {
		// A filter on any other field names one document without saying which,
		// and until now it was in neither resolving path. See singlewrite.go.
		// A statement naming many documents is not this case: guardMultiWrite
		// brought its documents up before the write, so a zero here is real.
		if statementIsMulti(stmt, cmdField) {
			return reply, nil
		}
		var resolved bool
		id, resolved, err = c.resolveMissByFilter(ctx, ns, fdoc, nil)
		if err != nil {
			// Reporting zero here is what an erasure request must never be
			// told, so the honest answer is that the archive could not be
			// reached, not that there was nothing to reach.
			return nil, err
		}
		if !resolved {
			return reply, nil
		}
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
	c.cls = ClassMiss

	// Inside a transaction the copy-up landed outside the caller's snapshot, so
	// re-running the statement here would miss again. Ask for a replay instead.
	if inTransaction(cmd) {
		c.r.Stats.TxnReplays.Add(1)
		c.cls = ClassTxnWarm
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
	if hasID && !filterIsOnlyID(fdoc) {
		// The same as the write path above, and for the same reason. See
		// filterIsOnlyID.
		hasID = false
	}
	if !hasID {
		// findAndModify carries its own sort, which is how the caller says
		// which of several matches it means. The archive has to be asked in
		// that same order or the wrong document comes up. See singlewrite.go.
		sort, _ := cmd.Lookup("sort").DocumentOK()
		var resolved bool
		id, resolved, err = c.resolveMissByFilter(ctx, ns, fdoc, sort)
		if err != nil {
			return nil, err
		}
		if !resolved {
			return reply, nil
		}
	}

	c.r.Stats.WriteMisses.Add(1)
	copied, err := c.r.copyUp(ctx, ns, id)
	if err != nil {
		return nil, err
	}
	if !copied {
		return reply, nil
	}
	c.cls = ClassMiss
	if inTransaction(cmd) {
		c.r.Stats.TxnReplays.Add(1)
		c.cls = ClassTxnWarm
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
		c.upstreamBroke()
		return nil, fmt.Errorf("retry after copy-up: %w", err)
	}
	reply, err := wire.Read(c.upstream)
	if err != nil {
		c.upstreamBroke()
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
	// A collection Thermocline does not tier lives only on the fast tier, so the
	// fast tier is the whole of it and its answer is authoritative. Asking for a
	// completeness claim it will never have would send the read to an archive
	// that has never held a single one of its documents.
	base, viaView, tiered := c.r.tieredBase(ctx, ns)
	if !tiered {
		return reply, nil
	}
	judged := ns
	if viaView {
		judged = base
	}

	// A read, so a claim held under router.claim_grace is allowed to answer it.
	mark, known := c.r.completenessForRead(ctx, judged)

	if known && mark.Claimed {
		// The claim that is about to authorise the fast tier, remembered for as
		// long as the cursor this query leaves behind. A cursor outlives the
		// decision and nothing used to check whether the decision still held:
		// see tieredcursor.go for what that was measured to cost.
		authorised := []nsClaim{{ns: judged, from: mark.From, claimed: mark.Claimed}}

		af, aok := c.r.ageFieldOf(judged)
		bound, bounded := lowerTimeBound(filter, af, aok)
		switch {
		case bounded && mark.Complete(bound):
			c.noteTieredCursor(authorised, reply)
			return reply, nil
		case !bounded && mark.CoversAll():
			// No lower bound, so the query may reach arbitrarily far back. Only
			// a tier claiming all of history can answer that on its own.
			c.noteTieredCursor(authorised, reply)
			return reply, nil
		}

		// A query for the newest N carries no lower bound and does not need
		// one: the fast tier's own answer can prove it holds all of them. See
		// newestfirst.go.
		cmd, cerr := msg.CommandDoc()
		rdoc, derr := reply.ReplyDoc()
		if cerr == nil && derr == nil && replyOK(rdoc) {
			af, aok := c.r.ageFieldOf(judged)
			if provenNewestFirst(cmd, rdoc, af, aok, mark) && c.r.sortSafeAges(ctx, judged, af) {
				c.r.Stats.NewestFirst.Add(1)
				// The proof needs no bound, and the rule is the same: it rests
				// on this claim, so it lasts as long as this claim does.
				c.noteTieredCursor(authorised, reply)
				return reply, nil
			}
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

	// A view read can be sent to the archive when the archive holds the view
	// defined the same way, which is the case for one this router mirrored down.
	// Otherwise the question cannot be put to it however complete it is.
	if viaView && !c.r.archiveCanAnswerView(ctx, ns) {
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a read through a view over an incomplete collection",
			"view", ns, "reads", base, "because", because)
		return c.r.refuseView(reply.Header.RequestID, msg.Header.RequestID, ns, base, because)
	}

	// A transaction is the one place the archive fallback cannot go. Reading
	// one tier inside the caller's snapshot and the other outside it is not a
	// transaction any more. So rather than reading the archive, the documents
	// are moved to the tier the transaction is already reading and it is
	// replayed. Only a range too large to move is refused.
	if cmd, cerr := msg.CommandDoc(); cerr == nil && inTransaction(cmd) {
		if filter, fok := txnFilter(cmd, "find"); fok {
			switch out, res := c.warmForTxn(ctx, ns, filter, reply.Header.RequestID, msg.Header.RequestID,
				"the documents this transaction reads"); res {
			case txnWarmReplay:
				c.r.Stats.TxnReplays.Add(1)
				c.cls = ClassTxnWarm
				return out, nil
			case txnWarmComplete:
				// The archive holds nothing matching that the fast tier lacks,
				// so the answer already in hand is the whole answer.
				return reply, nil
			}
		}
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a transactional read that spans both tiers", "ns", ns, "because", because)
		return c.r.txnCannotSpanTiers(reply.Header.RequestID, msg.Header.RequestID, ns, because)
	}

	// The archive holds every document, so it answers in full what the fast
	// tier cannot. This is what keeps a query the application already had
	// working, instead of turning it into an error nobody asked for.
	if c.r.cfg.Completeness == config.Archive {
		db, coll := splitNS(ns)
		// The fast tier ran this and may be holding the rest of it. Nothing
		// will ever ask for that rest. See superseded.go.
		if fdoc, ferr := reply.ReplyDoc(); ferr == nil {
			c.dropSupersededCursor(ctx, fdoc, db)
		}
		out, aerr := c.answerFromArchive(ctx, msg, db, coll, true)
		if aerr == nil {
			c.log.Debug("read answered from the archive", "ns", ns, "because", because)
			return out, nil
		}
		// An error the archive itself returned is MongoDB's answer to this
		// command, so it goes back as it stands. Only a failure to reach the
		// archive falls through to the refusal below.
		if out, passed := c.r.serverErrorReply(reply.Header.RequestID, msg.Header.RequestID, aerr); passed {
			c.log.Debug("the archive rejected this read; returning its own error", "ns", ns, "err", aerr)
			return out, nil
		}
		c.log.Warn("could not answer this read from the archive, refusing instead", "ns", ns, "err", aerr)
	}

	if c.r.cfg.Completeness == config.Permissive {
		c.log.Warn("answer may be short", "ns", ns, "because", because)
		return reply, nil
	}

	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a query that would return a short answer", "ns", ns, "because", because)
	return c.r.errorReply(msg, reply, fmt.Sprintf(
		"thermocline: refusing this query because %s. %s. To allow short answers instead, set router.completeness to permissive.",
		because, remedy))
}
