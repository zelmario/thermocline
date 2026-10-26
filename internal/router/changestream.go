package router

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// A change stream watches the fast tier, and the fast tier is not where the
// documents live, it is where the recently used ones are kept. So it sees this
// system's own housekeeping and reports it as though the application had done
// it:
//
//   - the evictor removes a document, and the stream says delete, for a document
//     that still exists and that a read a second later still returns;
//   - a copy-up brings one back, and the stream says insert, for a document that
//     has existed for years.
//
// An application invalidating a cache, emitting domain events or tombstoning
// rows in a warehouse acts on both. Neither is a slow answer or a refused one:
// they are events that never happened, which is the worst thing this system can
// produce.
//
// Both are already distinguishable, because two other parts of Thermocline
// needed the same distinction first. The evictor announces every document it is
// about to remove, which is how the feeder tells an eviction from an
// application delete. And every copy-up records residency, to stop the next
// sweep taking back what was just fetched. So the events can be filtered
// against the same records, and no new bookkeeping is needed for it.

// promotionWindow is how recently a document must have been promoted for an
// insert of it to be read as a copy-up rather than as a real insert.
//
// Residency records outlive the promotion by design, for as long as the
// document is meant to stay hot, so presence alone would suppress a genuine
// insert of a document that had been copied up and later deleted by the
// application. The event's own time has to be close to the promotion.
//
// The event's own time, and it was not: this was measured against the clock at
// the moment the events were handed over, which is a different instant whenever
// the client is not keeping up. An application that stores its resume token and
// comes back to it gets a backlog, and every promotion in that backlog is older
// than the window. Measured: one promotion, read at once, invisible; the same
// promotion read thirty-five seconds later arrived as an insert. Storing the
// token and resuming is the correct thing for an application to do.
const promotionWindow = 30 * time.Second

// echoOutstanding is how long an unspent echo may still be waiting for its
// insert.
//
// It matches the feeder's echoWindow, and it has to: the feeder only ever spends
// a notice whose promotion is inside that window, so an echo older than it is
// one the feeder has already decided it will never spend. Treating it as still
// outstanding waits for something that is not coming.
//
// Unbounded, it used to expire with the row, and the row used to be destroyed
// when its hold ended. Now that the row outlives the hold by an hour so a late
// reader can still recognise a copy-up, an echo nothing ever spent would go on
// hiding every insert of that _id for that hour. Which is the wrong way round:
// the branch exists to suppress a copy-up nobody has accounted for yet, not to
// hide an application's own writes an hour later.
//
// Falling past it is safe. A copy-up whose echo was never spent is still
// recognised, because its insert sits within promotionWindow of the promotion
// that caused it, and that is exactly what the comparison below asks.
const echoOutstanding = 60 * time.Second

// changeStreamRegistry remembers which cursors are change streams that need
// their events filtered, and on what namespace.
//
// Per router rather than per connection, for the same reason the archive cursor
// registry is: a driver may issue getMore from any connection in its pool.
type changeStreamRegistry struct {
	mu sync.RWMutex
	ns map[int64]string
	// held is what a cursor was sent and has not yet been given. See
	// streamheld.go.
	held map[int64]*heldStream
	// tail is, per stream, the client's stages this router applies itself,
	// or a stream it must not fill. See splitStreamPipeline.
	tail map[int64]*streamTail
}

// streamTail is what this router does to a stream's batches beyond sieving
// them: the client's own stages it runs after filling, with the stream's
// collation and let, or, with noFill, nothing filled at all.
type streamTail struct {
	stages    bson.A
	collation bson.RawValue
	let       bson.RawValue
	// noFill is a stream whose own stages read fullDocument and are left to
	// mongod. Filling after them would put back what they took out.
	noFill bool
}

// applies reports whether the tail has stages to run here.
func (t *streamTail) applies() bool { return t != nil && len(t.stages) > 0 }

// fills reports whether a null lookup may be filled from the archive.
func (t *streamTail) fills() bool { return t == nil || !t.noFill }

// setTail records what a stream's cursor is to have done here.
func (reg *changeStreamRegistry) setTail(id int64, tail *streamTail) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.tail == nil {
		reg.tail = map[int64]*streamTail{}
	}
	reg.tail[id] = tail
}

// tailOf is what setTail recorded for a stream, nil for none.
func (reg *changeStreamRegistry) tailOf(id int64) *streamTail {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	return reg.tail[id]
}

func (reg *changeStreamRegistry) add(id int64, ns string) {
	if id == 0 {
		return
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.ns == nil {
		reg.ns = map[int64]string{}
	}
	reg.ns[id] = ns
}

func (reg *changeStreamRegistry) get(id int64) (string, bool) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	ns, ok := reg.ns[id]
	return ns, ok
}

func (reg *changeStreamRegistry) drop(id int64) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	delete(reg.ns, id)
	delete(reg.held, id)
	delete(reg.tail, id)
}

// splitStreamPipeline takes the client's stages from the first that reads
// fullDocument onwards out of a change stream the fast tier is asked to open,
// for this router to apply to each batch once it has filled it.
//
// With fullDocument: "updateLookup" the lookup runs on the fast tier, and an
// update to a document evicted before the event is read comes back null, which
// fillLookups fills from the archive. That runs after mongod has applied the
// stream's own stages, so they never saw the document. Two things went wrong:
// a $project removing a field of it was undone, measured as watch([{$project:
// {"fullDocument.secret": 0}}]) handed the whole document, secret included; and
// a $match on a field of it dropped the event on mongod's null before this
// router saw it, so those updates were lost.
//
// Run here, the stages see what MongoDB's would. Only the stages mongod allows
// in a change stream are taken: anything else, a $lookup or a $merge, is left
// in the command for mongod to refuse, which it does at open, and was otherwise
// run here on the router's own client. The rest are tried once at open on an
// empty input with the stream's collation and let, so a stage mongod would
// refuse is still mongod's to refuse.
//
// A stream whose stages read fullDocument and that cannot be split, because it
// ends in $changeStreamSplitLargeEvent, which only the server can apply, or
// because the trial failed, is recorded as not to be filled: filling after
// mongod's stages put back a field a $project took out.
func (c *conn) splitStreamPipeline(ctx context.Context, msg *wire.Message, cmd bson.Raw, db string) {
	stages, ok := pipelineStages(cmd)
	if !ok || len(stages) < 2 {
		return
	}
	// A stream that asked for the document has one to fill, and its stages
	// that can see the document run after the fill.
	lookup := bytes.Contains(stages[0].Value, []byte("fullDocument"))
	// The stages mongod may keep are the leading $match stages, which leave
	// an event whole, and with a lookup only those that cannot see the
	// document: none names fullDocument, $$ROOT or $$CURRENT, or reads
	// through $expr. Everything from the first other stage on runs here.
	//
	// Two reasons, one for each half. A stage that can see the document, a
	// $redact deciding on $$ROOT or a $project dropping fullDocument, ran
	// before the fill, and the fill put back what it had taken. And a stage
	// that reshapes the event, $project of the fields a consumer wants, took
	// away the lsid the sieve reads to tell the evictor's delete, made in its
	// own session, from the application's delete of the same document: run
	// first, it left an application's delete hidden as an eviction. See
	// meta.EvictionNotice.Accepts.
	at := -1
	for i, st := range stages[1:] {
		name, _ := changeStreamStage(st)
		if name != "$match" || (lookup && !blindMatch(st)) {
			at = i + 1
			break
		}
	}
	if at < 0 {
		return
	}
	var keep *streamTail
	if lookup {
		keep = &streamTail{noFill: true}
	}
	c.csTail = keep
	tail := make(bson.A, 0, len(stages)-at)
	for _, st := range stages[at:] {
		name, legal := changeStreamStage(st)
		if !legal || name == "$changeStreamSplitLargeEvent" {
			return
		}
		tail = append(tail, st)
	}
	collation, let := cmd.Lookup("collation"), cmd.Lookup("let")
	trial := bson.D{
		{Key: "aggregate", Value: 1},
		{Key: "pipeline", Value: append(bson.A{bson.D{{Key: "$documents", Value: bson.A{}}}}, tail...)},
		{Key: "cursor", Value: bson.D{}},
	}
	trial = withStreamOptions(trial, collation, let)
	if err := c.r.hot.Database(db).RunCommand(ctx, trial).Err(); err != nil {
		return
	}
	elems, err := cmd.Elements()
	if err != nil {
		return
	}
	head := make(bson.A, 0, at)
	for _, st := range stages[:at] {
		head = append(head, st)
	}
	rewritten := make(bson.D, 0, len(elems))
	for _, e := range elems {
		if e.Key() == "pipeline" {
			rewritten = append(rewritten, bson.E{Key: "pipeline", Value: head})
			continue
		}
		rewritten = append(rewritten, bson.E{Key: e.Key(), Value: e.Value()})
	}
	out, err := wire.NewMsg(msg.Header.RequestID, 0, rewritten)
	if err != nil {
		return
	}
	c.sendInstead = out
	c.csTail = &streamTail{stages: tail, collation: collation, let: let}
}

// blindMatch reports whether a stage is a $match that cannot see a change
// event's document, so running it before the fill changes nothing.
func blindMatch(st bson.RawValue) bool {
	name, _ := changeStreamStage(st)
	if name != "$match" {
		return false
	}
	for _, w := range []string{"fullDocument", "$$ROOT", "$$CURRENT", "$expr"} {
		if bytes.Contains(st.Value, []byte(w)) {
			return false
		}
	}
	return true
}

// changeStreamStage names a stage and reports whether mongod allows it after
// $changeStream. The list is mongod's own, from the change streams manual.
func changeStreamStage(st bson.RawValue) (string, bool) {
	d, ok := st.DocumentOK()
	if !ok {
		return "", false
	}
	elems, err := d.Elements()
	if err != nil || len(elems) != 1 {
		return "", false
	}
	switch name := elems[0].Key(); name {
	case "$match", "$project", "$addFields", "$set", "$unset", "$replaceRoot", "$replaceWith",
		"$redact", "$changeStreamSplitLargeEvent":
		return name, true
	default:
		return name, false
	}
}

// withStreamOptions adds a stream's collation and let to a command run on its
// behalf, where the stream had them.
func withStreamOptions(cmd bson.D, collation, let bson.RawValue) bson.D {
	if collation.Type != 0 {
		cmd = append(cmd, bson.E{Key: "collation", Value: collation})
	}
	if let.Type != 0 {
		cmd = append(cmd, bson.E{Key: "let", Value: let})
	}
	return cmd
}

// applyStreamTail runs a stream's own stages over a batch this router filled.
// See splitStreamPipeline. The resume token is each event's _id, and a stage
// that changed it is the error mongod gives for one.
func (r *Router) applyStreamTail(ctx context.Context, db string, tail *streamTail, events bson.A) (bson.A, error) {
	if !tail.applies() || len(events) == 0 {
		return events, nil
	}
	ids := make(map[string]bool, len(events))
	docs := make(bson.A, 0, len(events))
	for _, ev := range events {
		raw, ok := ev.(bson.RawValue)
		if !ok {
			return events, nil
		}
		d, dok := raw.DocumentOK()
		if !dok {
			return events, nil
		}
		ids[string(d.Lookup("_id").Value)] = true
		docs = append(docs, d)
	}
	// $documents evaluates its argument as an expression, so the events go in
	// as a literal: an update's updatedFields has dotted names, which an
	// expression object refuses with 16412, and a string such as "$5" would be
	// read as a field path.
	pipeline := append(bson.A{bson.D{{Key: "$documents", Value: bson.D{{Key: "$literal", Value: docs}}}}}, tail.stages...)
	cmd := withStreamOptions(bson.D{
		{Key: "aggregate", Value: 1}, {Key: "pipeline", Value: pipeline}, {Key: "cursor", Value: bson.D{}},
	}, tail.collation, tail.let)
	cur, err := r.hot.Database(db).RunCommandCursor(ctx, cmd)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	out := bson.A{}
	for cur.Next(ctx) {
		d := append(bson.Raw(nil), cur.Current...)
		if !ids[string(d.Lookup("_id").Value)] {
			return nil, errResumeTokenModified
		}
		out = append(out, bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: d})
	}
	return out, cur.Err()
}

// errResumeTokenModified is mongod's 280 for a stage that changed an event's
// _id, which is its resume token.
var errResumeTokenModified = errors.New("Encountered an event whose _id field, which contains the resume token, " +
	"was modified by the pipeline. Modifying the _id field of an event makes it impossible to resume the stream " +
	"from that point. Only transformations that retain the unmodified _id field are allowed.")

// streamScope names what a change stream is watching, and reports whether any
// event it delivers could be about a collection this system tiers.
//
// A change stream on a whole database or the whole deployment sends
// aggregate: 1 rather than a collection name, so it had no namespace to
// match against the configuration and was never registered, and nothing it
// delivered was ever filtered. db.watch() is ordinary: an application
// follows several collections with one stream. Measured: the collection
// stream hid a copy-up and the database stream reported it as an insert.
//
// Every event carries its own namespace, so the wider kinds are filtered
// per event instead, and this only has to decide whether any event could
// be about a collection this system tiers.
//
// It lives here, and not inside the sieve, because the guard on the way in has
// to answer the same question. It did not: it asked Manages("") for the wider
// forms, which is false for a configured deployment and true for one tiering
// everything, so the counter and the resume warning covered a db.watch() in
// one configuration and not the other, while the sieve filtered it in both.
func (r *Router) streamScope(ns, db string) (string, bool) {
	switch {
	case ns != "":
		return ns, r.cfg.Manages(ns)
	case db == "admin":
		// The whole deployment. Which databases it will carry events for is
		// not knowable here.
		return "the whole deployment", true
	default:
		return db + ", the whole database", r.managesAnythingIn(db)
	}
}

// noteChangeStream remembers the cursor an opened change stream returned, so
// its later batches can be filtered, and filters the batch this reply
// carries.
//
// It reports whether it rewrote the reply, the same way filterChangeStream
// does, and a batch it cannot read leaves the reply exactly as it came back.
func (c *conn) noteChangeStream(ctx context.Context, cmd bson.Raw, db string, reply *wire.Message) (*wire.Message, bool) {
	ns, isCS := changeStreamCollection(cmd, db)
	if !isCS {
		return nil, false
	}
	watching, matters := c.r.streamScope(ns, db)
	if !matters {
		return nil, false
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return nil, false
	}
	id, ok := cursorIDOf(rdoc)
	if !ok {
		return nil, false
	}
	// An id of 0 is a stream mongod closed in its first reply, a resume
	// before a drop delivering its events and the invalidate at once. Its
	// batch is still sieved and still has the client's stages applied; there
	// is only no cursor to remember.
	tail := c.csTail
	c.csTail = nil
	if id != 0 {
		c.r.changeStreams.add(id, watching)
		if tail != nil {
			c.r.changeStreams.setTail(id, tail)
		}
		c.log.Info("filtering tiering events out of a change stream", "watching", watching, "cursor", id)
	}

	// And this reply's own batch, which was passed through untouched.
	//
	// A stream opened on a live collection comes back with an empty
	// firstBatch, because nothing has happened yet, and everything it ever
	// delivers arrives in a getMore. That is why registering the cursor here
	// and filtering only getMore looked complete, and passed every test that
	// opened a stream and then wrote.
	//
	// A consumer that stores its resume token and comes back to it is the
	// case that breaks it. resumeAfter, startAfter and startAtOperationTime
	// all name a position in the past, so the aggregate returns events
	// immediately and the first of them never reached the sieve at all.
	// Measured: three documents copied up by one multi-document write, read
	// back from a startAtOperationTime just before it, and all three arrived
	// as inserts the application never made -- with the residency records
	// right there, naming those exact oplog positions.
	//
	// Storing a token and resuming from it is the correct thing for a
	// consumer to do, and it is precisely the consumer this was failing.
	events, eok := firstBatch(rdoc)
	if !eok || len(events) == 0 {
		return nil, false
	}
	keep, dropped := c.r.sieveEvents(ctx, events)
	keep, token, _, changed, terr := c.cutStreamBatch(ctx, id, tail, rdoc, c.owner(cmd), keep)
	if terr != nil {
		out, rerr := c.streamTailErrorTo(reply.Header.ResponseTo, terr)
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	if dropped == 0 && !changed && token.Type == 0 {
		return nil, false
	}
	if dropped > 0 {
		c.r.Stats.EventsHidden.Add(int64(dropped))
		c.log.Info("hid tiering events from a change stream's first batch",
			"watching", watching, "hidden", dropped,
			"note", "this batch is what a consumer resuming from a stored token reads")
	}
	out, berr := replaceStreamBatch(rdoc, "firstBatch", keep, token, reply.Header.RequestID)
	if berr != nil {
		return nil, false
	}
	return out, true
}

// filterChangeStream removes this system's own housekeeping from a change
// stream batch.
//
// It reports whether it rewrote the reply. A batch it cannot read, or a lookup
// it cannot make, leaves the reply exactly as it came back: dropping events on a
// guess would be worse than passing a few through.
func (c *conn) filterChangeStream(ctx context.Context, msg *wire.Message, reply *wire.Message) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	id, ok := intOf64(cmd.Lookup("getMore"))
	if !ok {
		return nil, false
	}
	watching, watched := c.r.changeStreams.get(id)
	if !watched {
		return nil, false
	}
	c.log.Debug("filtering a change stream batch", "watching", watching, "cursor", id)

	rdoc, derr := reply.ReplyDoc()
	if derr != nil || !replyOK(rdoc) {
		return nil, false
	}
	events, eok := nextBatch(rdoc)
	if !eok || len(events) == 0 {
		return nil, false
	}
	// The id the reply carries, which is 0 once mongod has closed the cursor.
	rid, _ := cursorIDOf(rdoc)

	keep, dropped := c.r.sieveEvents(ctx, events)
	// A lookup the fast tier could not answer is asked of the archive, because
	// null means deleted and this document is not. See fillLookups.
	gid, _ := intOf64(cmd.Lookup("getMore"))
	keep, token, filled, changed, terr := c.cutStreamBatch(ctx, rid, c.r.changeStreams.tailOf(gid), rdoc, c.owner(cmd), keep)
	if terr != nil {
		out, rerr := c.streamTailError(msg, terr)
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	if dropped == 0 && !changed && token.Type == 0 {
		return nil, false
	}
	if dropped > 0 {
		c.r.Stats.EventsHidden.Add(int64(dropped))
		c.log.Info("hid tiering events from a change stream", "watching", watching, "hidden", dropped)
	}
	if filled > 0 {
		c.log.Debug("answered a change stream's document lookups from the archive",
			"watching", watching, "filled", filled)
	}

	out, berr := replaceStreamBatch(rdoc, "nextBatch", keep, token, msg.Header.RequestID)
	if berr != nil {
		return nil, false
	}
	return out, true
}

// sieveEvents returns the events an application should see, and how many were
// this system's own work.
func (r *Router) sieveEvents(ctx context.Context, events []bson.RawValue) (bson.A, int) {
	// Gather the ids to ask about, so one lookup covers the whole batch.
	var deleteIDs, insertIDs []string
	kinds := make([]string, len(events))
	docIDs := make([]string, len(events))

	// Namespaces this router made for itself, which a client watching a whole
	// database would otherwise see appear, fill and vanish. A tiered $out is
	// assembled in a scratch collection inside the application's own database,
	// because the rename that finishes it cannot cross databases. The catalog
	// listing already hides them for the same reason: a client that acts on
	// what it finds would act on that. Every kind of event, not just inserts
	// and deletes, because a scratch collection is created, written, renamed
	// and dropped, and the bookkeeping the router keeps on the fast tier is
	// written and deleted continuously.
	ours := make([]bool, len(events))

	// The namespaces of DDL events on collections this system tiers, and the
	// distinct list to ask about. This system repairs the fast tier's catalog:
	// before the first copy-up into a namespace it creates the collection there
	// with the archive's options and builds the archive's indexes on it, both
	// on the application's own namespace. An ordinary change stream does not
	// carry either, so this is only reachable with showExpandedEvents, and with
	// it a client was told its collection had just been created and its indexes
	// with it. Measured on both the collection and the database shapes of the
	// stream, from a single point read of one archived document.
	//
	// Not suppressed by kind alone, which would have been a line rather than a
	// lookup: the application creates indexes on these same collections through
	// this router, and hiding its own DDL from its own stream is worse than
	// showing an extra create. See meta.OwnDDLRecord.
	ddlNS := make([]string, len(events))
	var ddlAsk []string
	seenNS := map[string]bool{}

	for i, ev := range events {
		doc, ok := ev.DocumentOK()
		if !ok {
			continue
		}
		op, _ := lookupString(doc, "operationType")
		kinds[i] = op
		if evns, nsok := eventNS(doc); nsok {
			edb, coll := splitNS(evns)
			// The bookkeeping database as well as the scratch collections, and
			// missing it was the same oversight one level up. The unique key
			// registry is the one piece of this system's own state that lives on
			// the *fast* tier, because it has to be written where the data it
			// constrains is written -- which is exactly where a client's change
			// stream watches. A deployment-wide stream carried a claim row for
			// every unique key every write touched, in a database the same client
			// cannot even list: listDatabases has hidden it all along.
			//
			// Measured: five documents inserted through the router into a
			// collection with a unique index, and a client.watch() delivered ten
			// events, five of them the registry's.
			ours[i] = meta.IsScratch(coll) || edb == meta.DB
		}
		if isCatalogDDL(op) && !ours[i] {
			if evns, nsok := eventNS(doc); nsok && r.cfg.Manages(evns) {
				ddlNS[i] = evns
				if !seenNS[evns] {
					seenNS[evns] = true
					ddlAsk = append(ddlAsk, evns)
				}
			}
		}
		if op != "delete" && op != "insert" {
			continue
		}
		// The event's own namespace, not the cursor's: a stream on a whole
		// database carries a different one on each event, and an event about a
		// collection nothing tiers has no housekeeping behind it to hide.
		ns, nsok := eventNS(doc)
		if !nsok || !r.cfg.Manages(ns) {
			continue
		}
		key, kerr := doc.LookupErr("documentKey")
		if kerr != nil {
			continue
		}
		kdoc, kok := key.DocumentOK()
		if !kok {
			continue
		}
		idv, ierr := kdoc.LookupErr("_id")
		if ierr != nil {
			continue
		}
		docIDs[i] = meta.DocID(ns, idv)
		if op == "delete" {
			deleteIDs = append(deleteIDs, docIDs[i])
		} else {
			insertIDs = append(insertIDs, docIDs[i])
		}
	}

	evicted := r.evictionNotices(ctx, deleteIDs)
	promoted := r.promotionTimes(ctx, insertIDs)
	ownDDL := r.ownCatalogDDL(ctx, ddlAsk)
	// The old rule, kept as a floor. Whatever the events say about their own
	// time, a promotion recorded within the window still counts, so this can
	// only ever suppress more than it did and never less. That also settles the
	// clock question: promotedAt is written by a router and an event's time
	// comes from the fast tier's primary, two clocks, and skew between them can
	// only fail to widen the window, never narrow it.
	recent := time.Now().Add(-promotionWindow)

	keep := make(bson.A, 0, len(events))
	dropped := 0
	for i, ev := range events {
		switch {
		case ours[i]:
			dropped++
		case ddlNS[i] != "" && ownDDL[ddlNS[i]].Covers(eventPosition(ev)):
			// This system's own catalog repair. The event's position falls
			// inside a bracket the Indexer opened before the command and
			// closed after it, read from the fast tier's own lastWrite. See
			// meta.OwnDDLSpan for why the command's own reply cannot say.
			r.log.Debug("hiding this system's own catalog repair from a change stream",
				"ns", ddlNS[i], "event", kinds[i])
			dropped++
		case kinds[i] == "delete" && isEviction(evicted, docIDs[i], eventTime(ev), eventPosition(ev), eventSession(ev)):
			dropped++
		case kinds[i] == "insert" && isPromotion(promoted, docIDs[i], eventTime(ev), eventPosition(ev), recent):
			// Which one, and how the window judged it. An insert hidden from an
			// application's own change stream is the most expensive thing this
			// function can get wrong, and the count alone cannot be worked back
			// to a document.
			r.log.Debug("hiding an insert as a promotion",
				"doc", docIDs[i], "promotedAt", promoted[docIDs[i]].at.Format(time.RFC3339Nano),
				"echoPending", promoted[docIDs[i]].pending,
				"eventAt", eventTime(ev).Format(time.RFC3339Nano))
			dropped++
		default:
			// An insert this system has a residency record for, handed to the
			// application anyway. Either the application is writing an _id it
			// deleted earlier, which is its own news, or this is a copy-up
			// arriving as an insert nobody made.
			//
			// Logged because nothing said which. The sieve says when it hides
			// an insert and says nothing when it lets one through, so a phantom
			// reaching a consumer left no trace on this side at all and the
			// only evidence was the application noticing. Measured on
			// 2026-09-24: the shop reported an order placed that nobody placed,
			// and the record that should have accounted for it was there, with
			// a promotedAt from a later promotion of the same document.
			//
			// Only when a record exists, which is what keeps this off the path
			// every ordinary insert takes.
			if kinds[i] == "insert" && docIDs[i] != "" {
				if p, have := promoted[docIDs[i]]; have {
					r.log.Info("letting an insert through although this system has a residency record for it",
						"doc", docIDs[i],
						"promotedAt", p.at.Format(time.RFC3339Nano),
						"endedAt", p.ended.Format(time.RFC3339Nano),
						"echoPending", p.pending,
						"echoes", len(p.echoed),
						"eventAt", eventTime(ev).Format(time.RFC3339Nano),
						"eventPos", eventPosition(ev),
						"note", "an application rewriting an _id it deleted looks like this too")
				}
			}
			keep = append(keep, ev)
		}
	}
	return keep, dropped
}

// isCatalogDDL reports whether an event is one this system performs on the fast
// tier to repair its catalog.
//
// create and createIndexes, and nothing else, because those are the two this
// system does: Indexer.EnsureCollection creates the collection with the
// archive's options before the first copy-up, and Indexer.Ensure builds the
// archive's indexes on it. Nothing here drops a collection or an index on the
// fast tier for its own reasons, so a drop or a dropIndexes reaching a client
// is the application's own or somebody else's, and either way it is news.
//
// Named as a list rather than "any DDL" for that reason. A kind added here
// without something recording it would hide an event nothing accounts for.
func isCatalogDDL(op string) bool {
	return op == "create" || op == "createIndexes"
}

// ownCatalogDDL reads what this system recorded doing to these namespaces.
//
// A miss is not an error: it means nothing recorded that DDL, so the event is
// the application's and goes through. That is also what happens when the
// bookkeeping cannot be read at all, which is the same direction every other
// judgement here errs in.
func (r *Router) ownCatalogDDL(ctx context.Context, nss []string) map[string]meta.OwnDDLRecord {
	if len(nss) == 0 {
		return nil
	}
	rows, err := meta.ReadOwnDDL(ctx, r.cold, nss)
	if err != nil {
		r.log.Warn("could not read which catalog changes this system made, so a client watching "+
			"with showExpandedEvents may see them", "err", err)
		return nil
	}
	return rows
}

// eventNS is the namespace one change stream event happened in.
func eventNS(doc bson.Raw) (string, bool) {
	nsv, err := doc.LookupErr("ns")
	if err != nil {
		return "", false
	}
	nd, ok := nsv.DocumentOK()
	if !ok {
		return "", false
	}
	db, dok := stringField(nd, "db")
	coll, cok := stringField(nd, "coll")
	if !dok || !cok {
		return "", false
	}
	return db + "." + coll, true
}

// evictionWindow is how close a delete has to be to the announcement for it to
// be read as an eviction rather than as the application deleting the document.
//
// It did not need one: the notice was removed the moment the feeder applied the
// eviction, so a notice that still existed meant an eviction that had just
// happened, and presence alone answered the question. A notice now lives out
// its TTL, because a client reading its events late needs it to, and presence
// alone would then hide a genuine delete of a document that had been evicted
// and warmed back up within the hour.
//
// It is a fallback now rather than the rule. A notice the feeder has spent
// records which delete it was spent on, and that is exact; the window is what
// answers for a notice written by a feeder from before that field existed, so a
// rolling upgrade does not stop suppressing evictions for an hour.
const evictionWindow = 30 * time.Second

// evictionNote is what the sieve needs to know about one announcement.
type evictionNote struct {
	left     int
	at       time.Time
	spent    []bson.Timestamp
	sessions []string
}

// isEviction reports whether this delete is the evictor making room rather than
// the application removing a document.
//
// The window used to answer this on its own, and it swallowed the application's
// own deletes. An eviction and an application delete of the same document look
// identical in the oplog, and the only thing that tells them apart is which one
// the feeder counted the announcement against: the evictor announces, the
// feeder applies that one delete and spends the notice, and every delete after
// it is somebody else's. Measured on a fuzzer round where the evictor removed
// nine documents, the feeder applied them, and the application deleted the same
// nine twenty-two seconds later: all nine deletes were dropped from the stream,
// so a client watching the collection was never told records it had deleted
// were gone.
//
// pos is the delete's own position in the fast tier's oplog, which is the same
// value the feeder wrote down, because both read it off the same event.
//
// session names the transaction the delete was made in, "" for none. A notice
// naming sessions is the evictor's own delete only in one of them; anything
// else is the application's, announced or not. See meta.EvictionNotice.Accepts.
func isEviction(notes map[string]evictionNote, id string, at time.Time, pos bson.Timestamp, session string) bool {
	note, ok := notes[id]
	if !ok {
		return false
	}
	if !(meta.EvictionNotice{Sessions: note.sessions}).Accepts(session) {
		return false
	}
	// Unclaimed: announced, and the feeder has not applied it yet, so this
	// delete may well be the one. Genuinely ambiguous until the feeder catches
	// up, and suppressing is the side that does not tell an application its
	// data changed when it did not.
	if note.left > 0 {
		return true
	}
	// Spent, and it says which delete it was spent on.
	if known, yes := (meta.EvictionNotice{Spent: note.spent}).SpentOn(pos); known {
		return yes
	}
	// A notice from a feeder that did not record positions. The old rule, and
	// the loosest thing here: the event's instant comes from the fast tier's
	// primary and the notice's from the archive, so the window absorbs whatever
	// those two machines disagree about. It was two different machines before
	// this as well, the evictor's and the primary's; the notice moved to the
	// archive's clock because the archive's TTL monitor is what decides when it
	// is deleted, and being deleted early is how documents were lost. See
	// meta.EvictionAnnouncement.
	if at.IsZero() || note.at.IsZero() {
		return false
	}
	d := at.Sub(note.at)
	if d < 0 {
		d = -d
	}
	return d <= evictionWindow
}

// eventPosition is where an event sits in the fast tier's oplog.
//
// Its cluster time, which is exact: no two oplog entries share one, and the
// feeder reads the same field off the same event. wallTime would have done from
// MongoDB 6.0 and this supports 4.2.
// eventSession names the transaction an event was made in, "" for none.
func eventSession(ev bson.RawValue) string {
	doc, ok := ev.DocumentOK()
	if !ok {
		return ""
	}
	lsid, ok := doc.Lookup("lsid").DocumentOK()
	if !ok {
		return ""
	}
	return meta.SessionHex(lsid)
}

func eventPosition(ev bson.RawValue) bson.Timestamp {
	doc, ok := ev.DocumentOK()
	if !ok {
		return bson.Timestamp{}
	}
	v, err := doc.LookupErr("clusterTime")
	if err != nil {
		return bson.Timestamp{}
	}
	t, i, tok := v.TimestampOK()
	if !tok {
		return bson.Timestamp{}
	}
	return bson.Timestamp{T: t, I: i}
}

// evictionNotices reads the announcements for these documents.
func (r *Router) evictionNotices(ctx context.Context, ids []string) map[string]evictionNote {
	out := map[string]evictionNote{}
	if len(ids) == 0 {
		return out
	}
	cur, err := r.cold.Database(meta.DB).Collection(meta.Evicting).Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		// Nothing is hidden when the answer cannot be had. An application
		// seeing an eviction is bad; an application missing a real delete
		// because a lookup failed would be worse.
		r.log.Warn("could not read eviction announcements, letting the events through", "err", err)
		return out
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	for cur.Next(ctx) {
		var rec meta.EvictionNotice
		if cur.Decode(&rec) == nil {
			out[rec.ID] = evictionNote{left: rec.Left(), at: rec.At.UTC(), spent: rec.Spent, sessions: rec.Sessions}
		}
	}
	return out
}

// isPromotion reports whether this insert is a copy-up rather than the
// application creating a document.
func isPromotion(promoted map[string]promotion, id string, at time.Time, pos bson.Timestamp, recent time.Time) bool {
	p, ok := promoted[id]
	if !ok {
		return false
	}
	// An insert the feeder wrote down as a copy-up's echo is one, whatever the
	// clocks below say, so that is asked first. It used to be asked last, after
	// the ended check, and wall times do not follow the oplog at this scale:
	// measured on 2026-09-30, a copy-up's insert committed as a transaction
	// carried wall time 25.242 and the delete after it 25.220. The application
	// had deleted the document between the copy-up recording its promotion and
	// inserting, the feeder ended the record at 25.224, and the copy-up's own
	// insert was read as the application writing the _id again: an order shown
	// placed after it was deleted, one round of six.
	if known, yes := (meta.ResidencyRecord{Echoed: p.echoed}).EchoedAt(pos); known && yes {
		return true
	}
	// Announced and not yet spent, which means the insert this record was
	// written to precede has not been seen. The way an unclaimed eviction
	// announcement is, and for the same reason: while nobody has accounted for
	// the copy-up, suppressing is the side that does not tell an application
	// its data changed when it did not.
	//
	// Bounded, though, by the same window the feeder spends notices in. See
	// echoOutstanding.
	//
	// ABOVE the ended check, and that ordering is the fix rather than a detail.
	// It used to be below, on the stated grounds that "a record that has ended
	// describes a promotion that is over". A record that has ended describes a
	// promotion whose *hold* is over, which is not the same thing: the insert
	// that promotion caused can still be in flight, and while its echo is
	// outstanding nobody has accounted for it.
	//
	// endedAt has two writers and the check below reads only one of them. The
	// feeder writes it when the application deletes the document, which is the
	// case that check is for. The evictor writes it as it evicts, deliberately,
	// so the row outlives the hold for a change stream to read. Measured on
	// 2026-09-24 with the sieve instrumented: promotedAt 14:15:43.160, the
	// evictor ending the hold at 14:15:43.233, the copy-up's own insert reaching
	// the sieve at 14:15:43.238 with echoPending true and no echo spent, and the
	// shop reporting an order placed that nobody placed.
	//
	// An application deleting a document and writing the _id again is still
	// shown, because by the time that second insert exists the copy-up's insert
	// has already been through here and the feeder has spent its echo, so
	// pending is false and this branch does not fire.
	if p.pending && !p.at.IsZero() && time.Since(p.at) <= echoOutstanding {
		return true
	}
	// The application has deleted the document since, and this insert happened
	// after that, so it is the application writing the _id again and not this
	// promotion.
	//
	// Ending the record rather than deleting it is what makes the other half of
	// this work. Deleting it took away the only evidence that the copy-up's own
	// insert was a promotion, and a consumer that had not caught up was then
	// shown an insert of a document it never inserted: measured as exactly that,
	// insert then delete, for a document only the archive held.
	if !p.ended.IsZero() && !at.IsZero() && at.After(p.ended) {
		return false
	}
	if p.at.After(recent) {
		// The window is a floor for one case, a record the TTL removed and a
		// later promotion wrote again, knowing only the later echo. A record
		// outlives the last write to it by at least meta.NoticeTTL, so an
		// insert from less than that before this promotion was never an echo
		// such a record forgot, and the list is complete for it once the echo
		// is spent and nothing was withdrawn. Half the TTL, for the two clocks
		// promotedAt and an event's time come from.
		//
		// Without it the window hid the application's own insert of a document
		// a copy-up then put on the fast tier a moment later. Measured on
		// 2026-09-30: an order placed, then deleted through another router
		// while a transaction's warm was moving it, and the order's insert was
		// missing from a stream read from before it, 6 of 6. The copy-up's own
		// insert is in the list and stays hidden.
		if known, yes := (meta.ResidencyRecord{Echoed: p.echoed}).EchoedAt(pos); known && !p.pending && !p.uncertain &&
			!at.IsZero() && at.After(p.at.Add(-meta.NoticeTTL/2)) {
			return yes
		}
		return true
	}
	// Exact, and it answers the question the window cannot.
	//
	// promotedAt describes one promotion and a record outlives several. A
	// document copied up, evicted and copied up again keeps one record whose
	// promotedAt is the later promotion's, so the earlier copy-up's insert is
	// older than the record accounting for it and the comparison below reads it
	// as somebody else's write. Measured at seed 90801: four copy-ups delivered
	// to an application as inserts it never made, promotedAt one to two seconds
	// after the event every time.
	//
	// The feeder writes down which insert it spent each echo on, so this is a
	// lookup rather than a judgement.
	//
	// Below both branches above on purpose, and that ordering is the fix rather
	// than a detail. A promotion whose echo is still outstanding has not been
	// written down yet, so a miss there would mean "not yet" and not "not
	// ours". And the window stays a floor: a record cleared by its TTL between
	// two promotions comes back knowing only the later one, and answering
	// exactly from an incomplete list would let the earlier insert through
	// where the floor had been catching it. So this can still only ever
	// suppress more than the old rule did, never less.
	//
	// Not while an echo is still pending, though. Pending means a promotion was
	// announced and the feeder never wrote its insert down, so the list is
	// known to be missing the most recent one, and answering "not ours" from it
	// is exact and wrong. Measured on 2026-09-25, test/echorace: the list held
	// the first promotion's position, the second's echo was never spent, and a
	// consumer reading seventy seconds late was shown the second copy-up as an
	// insert, two rounds of two. The window below then decides, which is what
	// it did before the list existed.
	//
	// And not once an echo has been withdrawn in this promotion, for the same
	// reason: the promoter that withdrew it lost a race to insert, and cannot
	// tell whether the winner's insert is the echo it was withdrawing. Measured
	// the same way with 32 reads across two routers: five rounds of five after
	// one uncontested promotion had put a position in the list.
	if known, yes := (meta.ResidencyRecord{Echoed: p.echoed}).EchoedAt(pos); known && (yes || (!p.pending && !p.uncertain)) {
		return yes
	}
	// The event carries no time to judge it by on a server old enough not to
	// send one, and there is nothing left to say.
	if at.IsZero() {
		return false
	}
	// One-sided, and this is the half that was wrong.
	//
	// Residency is recorded *before* the copy-up, deliberately, so that a
	// consumer can tell a promotion from an insert the moment the event
	// arrives. A promotion's own insert therefore always happens at or after
	// the record was written. An insert that happened *before* it is somebody
	// else's, and comparing the two by absolute difference reached back and
	// hid it: measured, five of the application's own inserts missing from its
	// own change stream, one of them two and a half seconds older than the
	// record that suppressed it.
	//
	// What this gives up is stated rather than hidden. promotedAt is a router's
	// clock and an event's time is the fast tier primary's, so a router running
	// ahead of the primary makes a genuine promotion look earlier than its own
	// record, and this would let it through. The two branches above are what
	// cover that: while the echo is outstanding nothing gets through, and a
	// promotion recorded inside the window is suppressed whatever the event
	// says. What is left is a promotion whose echo has been spent, older than
	// thirty seconds, on a router whose clock leads the primary's by more than
	// the promotion took, and that is a narrower case than the one this fixes.
	d := at.Sub(p.at)
	if d < 0 {
		return false
	}
	return d <= promotionWindow
}

// eventTime is when a change stream event happened, as the cluster that
// produced it saw it. wallTime is what a comparison against a wall clock needs
// and MongoDB has sent it since 6.0; clusterTime is the fallback, to one
// second, and is always there.
func eventTime(ev bson.RawValue) time.Time {
	doc, ok := ev.DocumentOK()
	if !ok {
		return time.Time{}
	}
	if v, err := doc.LookupErr("wallTime"); err == nil {
		if t, tok := v.TimeOK(); tok {
			return t.UTC()
		}
	}
	if v, err := doc.LookupErr("clusterTime"); err == nil {
		if secs, _, tok := v.TimestampOK(); tok {
			return time.Unix(int64(secs), 0).UTC()
		}
	}
	return time.Time{}
}

// promotion is what a residency record says about one document: when it was
// promoted, and whether the insert that promotion causes has been accounted for.
type promotion struct {
	at      time.Time
	pending bool
	// ended is when the document left the fast tier, zero while it is still
	// there. An insert after it is the application writing that _id again rather
	// than this promotion's own. See the feeder's dropResidency.
	ended time.Time
	// echoed is where each of this document's copy-up inserts sits in the fast
	// tier's oplog, as the feeder wrote it down. See meta.ResidencyRecord.Echoed.
	echoed []bson.Timestamp
	// uncertain says an echo of this promotion was withdrawn by a promoter
	// that lost a race to insert, so echoed may be missing an insert that did
	// happen. See meta.WithdrawEchoes.
	uncertain bool
}

// promotionTimes reports, for each of these document ids that has a residency
// record, when it was promoted and whether its echo is still outstanding.
func (r *Router) promotionTimes(ctx context.Context, ids []string) map[string]promotion {
	out := map[string]promotion{}
	if len(ids) == 0 {
		return out
	}
	cur, err := r.cold.Database(meta.DB).Collection(meta.Residency).Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		// Nothing is hidden when the answer cannot be had: an application
		// seeing a promotion is bad, and one missing a real insert because a
		// lookup failed would be worse.
		r.log.Warn("could not read promotion records, letting the events through", "err", err)
		return out
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	for cur.Next(ctx) {
		var rec struct {
			ID      string           `bson:"_id"`
			At      time.Time        `bson:"promotedAt"`
			Pending bool             `bson:"echoPending"`
			Ended   time.Time        `bson:"endedAt"`
			Echoed  []bson.Timestamp `bson:"echoed"`
			Unsure  bool             `bson:"echoUncertain"`
		}
		if cur.Decode(&rec) == nil {
			p := promotion{at: rec.At.UTC(), pending: rec.Pending, echoed: rec.Echoed, uncertain: rec.Unsure}
			if !rec.Ended.IsZero() {
				p.ended = rec.Ended.UTC()
			}
			out[rec.ID] = p
		}
	}
	return out
}

// nextBatch pulls the events out of a getMore reply.
func nextBatch(reply bson.Raw) ([]bson.RawValue, bool) {
	cursor, err := reply.LookupErr("cursor")
	if err != nil {
		return nil, false
	}
	cdoc, ok := cursor.DocumentOK()
	if !ok {
		return nil, false
	}
	batch, err := cdoc.LookupErr("nextBatch")
	if err != nil {
		return nil, false
	}
	arr, ok := batch.ArrayOK()
	if !ok {
		return nil, false
	}
	vals, err := arr.Values()
	if err != nil {
		return nil, false
	}
	return vals, true
}

// replaceBatch rebuilds a cursor reply with a different set of events. The
// field names which batch: "firstBatch" for the aggregate that opened the
// stream, "nextBatch" for a getMore. Both carry events and both are filtered,
// because a consumer resuming from a stored token reads the first one.
//
// Everything else is copied through untouched, and postBatchResumeToken above
// all: it is how a client resumes, and inventing one would be worse than any
// event this filters. Dropping events from a batch is what a $match in the
// pipeline would have done, so the token stays correct for what remains.
func replaceBatch(reply bson.Raw, field string, keep bson.A, requestID int32) (*wire.Message, error) {
	elems, err := reply.Elements()
	if err != nil {
		return nil, err
	}
	out := make(bson.D, 0, len(elems))
	for _, e := range elems {
		if e.Key() != "cursor" {
			out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
			continue
		}
		cdoc, ok := e.Value().DocumentOK()
		if !ok {
			out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
			continue
		}
		celems, cerr := cdoc.Elements()
		if cerr != nil {
			return nil, cerr
		}
		cursor := make(bson.D, 0, len(celems))
		for _, ce := range celems {
			if ce.Key() == field {
				cursor = append(cursor, bson.E{Key: field, Value: keep})
				continue
			}
			cursor = append(cursor, bson.E{Key: ce.Key(), Value: ce.Value()})
		}
		out = append(out, bson.E{Key: "cursor", Value: cursor})
	}
	return wire.NewMsg(requestID, requestID, out)
}

// cursorIDOf reads the cursor id out of a reply.
func cursorIDOf(reply bson.Raw) (int64, bool) {
	cursor, err := reply.LookupErr("cursor")
	if err != nil {
		return 0, false
	}
	cdoc, ok := cursor.DocumentOK()
	if !ok {
		return 0, false
	}
	return intOf64(cdoc.Lookup("id"))
}

// intOf64 reads an int32 or int64 as an int64.
func intOf64(v bson.RawValue) (int64, bool) {
	switch v.Type {
	case bson.TypeInt64:
		return v.Int64OK()
	case bson.TypeInt32:
		n, ok := v.Int32OK()
		return int64(n), ok
	case bson.TypeDouble:
		f, ok := v.DoubleOK()
		return int64(f), ok
	}
	return 0, false
}

// cursorIDsOf reads the ids a killCursors names.
func cursorIDsOf(cmd bson.Raw) []int64 {
	var out []int64
	val, err := cmd.LookupErr("cursors")
	if err != nil {
		return out
	}
	arr, ok := val.ArrayOK()
	if !ok {
		return out
	}
	vals, verr := arr.Values()
	if verr != nil {
		return out
	}
	for _, v := range vals {
		if id, ok := intOf64(v); ok {
			out = append(out, id)
		}
	}
	return out
}

// A change stream that asks for the document gets null when the fast tier no
// longer holds it.
//
// fullDocument: "updateLookup" tells the server to fetch the document as it
// stands when the event is delivered. That lookup happens on the fast tier,
// which is the one place that does not hold every document, so an update to a
// document that has since been evicted comes back with fullDocument: null.
//
// Null does not mean "somewhere else", it means gone. Measured: a change stream
// reported an update with fullDocument: null while a read of that same document
// through the router returned it. An application watching a collection to keep
// a cache or a search index in step would delete the entry, and the document is
// still there.
//
// So a null lookup is asked of the archive, and answered from it when the
// archive holds the document live. A document the application really deleted is
// tombstoned there, so it stays null, which is the answer that was right all
// along.
//
// The copy may be a moment behind the fast tier, because the archive trails it.
// updateLookup never promised the post-image: it fetches whatever the document
// is when the event is read, which may already be a later version or nothing at
// all. A copy a moment old is within that, and "deleted" is not.

// fillLookups replaces null fullDocument fields with the archive's copy.
//
// The second result counts how many were filled, for the log line that says the
// stream was answered across both tiers.
func (r *Router) fillLookups(ctx context.Context, events bson.A) (bson.A, int) {
	type pending struct {
		index int
		id    bson.RawValue
		ns    string
	}
	var want []pending
	for i, ev := range events {
		raw, ok := ev.(bson.RawValue)
		if !ok {
			continue
		}
		doc, dok := raw.DocumentOK()
		if !dok {
			continue
		}
		// Present and null. Absent means the client never asked for the lookup,
		// and inventing the field would be answering a question nobody put.
		fd, ferr := doc.LookupErr("fullDocument")
		if ferr != nil || fd.Type != bson.TypeNull {
			continue
		}
		op, _ := lookupString(doc, "operationType")
		if op != "update" && op != "replace" {
			continue
		}
		key, kerr := doc.LookupErr("documentKey")
		if kerr != nil {
			continue
		}
		kdoc, kok := key.DocumentOK()
		if !kok {
			continue
		}
		idv, ierr := kdoc.LookupErr("_id")
		if ierr != nil {
			continue
		}
		// Per event, because a stream on a whole database carries events from
		// several collections and two of them can hold the same _id.
		evns, nsok := eventNS(doc)
		if !nsok {
			continue
		}
		want = append(want, pending{index: i, id: idv, ns: evns})
	}
	if len(want) == 0 {
		return events, 0
	}

	byNS := map[string]bson.A{}
	for _, w := range want {
		byNS[w.ns] = append(byNS[w.ns], w.id)
	}
	// The archive is asked only after mirroring has caught up, the same
	// barrier a read clears. A lookup comes back null here because the fast
	// tier no longer holds the document, and when the application has just
	// deleted it the archive may not yet have the delete, or even the update
	// the event describes. Measured on 2026-09-25, an update followed at once
	// by a delete and read straight away: 13 of 20 events carried a document
	// older than their own update, where MongoDB answered null all 13 times.
	// Once per batch, and only on this path, which is the one where the fast
	// tier had nothing.
	//
	// Under router.freshness: strict an archive that has not caught up is not
	// asked at all, and the documents stay null. That is an answer MongoDB
	// gives for a document no longer there, where the archive's could be older
	// than the change the event describes.
	// One database for the narrow bar, or none: a stream on a whole
	// deployment fills documents from several.
	fillDB := nsDB(want[0].ns)
	for _, w := range want {
		if nsDB(w.ns) != fillDB {
			fillDB = ""
		}
	}
	if r.archiveIsFreshAcross(ctx, want[0].ns, fillDB, 0, "filling a change stream's documents from the archive before mirroring caught up",
		"an event's fullDocument may be older than the change it describes, or a document already deleted") != nil {
		return events, 0
	}
	found := make(map[string]bson.Raw, len(want))
	for evns, ids := range byNS {
		db, coll := splitNS(evns)
		cur, err := r.cold.Database(db).Collection(coll).Find(ctx, bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
			meta.NotArchivedDeleted(),
		})
		if err != nil {
			r.log.Warn("could not ask the archive for a change stream's documents; they stay null",
				"ns", evns, "err", err)
			continue
		}
		for cur.Next(ctx) {
			doc := bson.Raw(append([]byte(nil), cur.Current...))
			if idv, ierr := doc.LookupErr("_id"); ierr == nil {
				found[evns+"|"+string(idv.Value)+string(rune(idv.Type))] = doc
			}
		}
		_ = cur.Close(context.WithoutCancel(ctx))
	}

	filled := 0
	out := make(bson.A, len(events))
	copy(out, events)
	for _, w := range want {
		doc, ok := found[w.ns+"|"+string(w.id.Value)+string(rune(w.id.Type))]
		if !ok {
			continue
		}
		raw, _ := events[w.index].(bson.RawValue)
		rebuilt, rerr := withFullDocument(raw, doc)
		if rerr != nil {
			continue
		}
		out[w.index] = rebuilt
		filled++
	}
	return out, filled
}

// withFullDocument rebuilds an event with its fullDocument replaced, leaving
// every other field where the driver expects it.
func withFullDocument(ev bson.RawValue, doc bson.Raw) (bson.RawValue, error) {
	src, ok := ev.DocumentOK()
	if !ok {
		return ev, fmt.Errorf("event is not a document")
	}
	elems, err := src.Elements()
	if err != nil {
		return ev, err
	}
	out := make(bson.D, 0, len(elems))
	for _, e := range elems {
		if e.Key() == "fullDocument" {
			var v bson.D
			if uerr := bson.Unmarshal(doc, &v); uerr != nil {
				return ev, uerr
			}
			out = append(out, bson.E{Key: "fullDocument", Value: v})
			continue
		}
		var v any
		if uerr := e.Value().Unmarshal(&v); uerr != nil {
			return ev, uerr
		}
		out = append(out, bson.E{Key: e.Key(), Value: v})
	}
	raw, merr := bson.Marshal(out)
	if merr != nil {
		return ev, merr
	}
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: raw}, nil
}
