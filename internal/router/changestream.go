package router

import (
	"context"
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

// changeStreamRegistry remembers which cursors are change streams that need
// their events filtered, and on what namespace.
//
// Per router rather than per connection, for the same reason the archive cursor
// registry is: a driver may issue getMore from any connection in its pool.
type changeStreamRegistry struct {
	mu sync.RWMutex
	ns map[int64]string
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
}

// noteChangeStream remembers the cursor an opened change stream returned, so
// its later batches can be filtered.
func (c *conn) noteChangeStream(cmd bson.Raw, db string, reply *wire.Message) {
	ns, isCS := changeStreamCollection(cmd, db)
	if !isCS {
		return
	}
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
	watching := ns
	switch {
	case ns != "":
		if !c.r.cfg.Manages(ns) {
			return
		}
	case db == "admin":
		// The whole deployment. Which databases it will carry events for is
		// not knowable here.
		watching = "the whole deployment"
	default:
		if !c.r.managesAnythingIn(db) {
			return
		}
		watching = db + ", the whole database"
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return
	}
	id, ok := cursorIDOf(rdoc)
	if !ok || id == 0 {
		return
	}
	c.r.changeStreams.add(id, watching)
	c.log.Info("filtering tiering events out of a change stream", "watching", watching, "cursor", id)
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

	keep, dropped := c.r.sieveEvents(ctx, events)
	// A lookup the fast tier could not answer is asked of the archive, because
	// null means deleted and this document is not. See fillLookups.
	keep, filled := c.r.fillLookups(ctx, keep)
	if dropped == 0 && filled == 0 {
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

	out, berr := replaceNextBatch(rdoc, keep, msg.Header.RequestID)
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
	// and dropped.
	scratch := make([]bool, len(events))

	for i, ev := range events {
		doc, ok := ev.DocumentOK()
		if !ok {
			continue
		}
		op, _ := lookupString(doc, "operationType")
		kinds[i] = op
		if evns, nsok := eventNS(doc); nsok {
			_, coll := splitNS(evns)
			scratch[i] = meta.IsScratch(coll)
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
		case scratch[i]:
			dropped++
		case kinds[i] == "delete" && isEviction(evicted, docIDs[i], eventTime(ev)):
			dropped++
		case kinds[i] == "insert" && isPromotion(promoted, docIDs[i], eventTime(ev), recent):
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
			keep = append(keep, ev)
		}
	}
	return keep, dropped
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
const evictionWindow = 30 * time.Second

// evictionNote is what the sieve needs to know about one announcement.
type evictionNote struct {
	left int
	at   time.Time
}

// isEviction reports whether this delete is the evictor making room rather than
// the application removing a document.
func isEviction(notes map[string]evictionNote, id string, at time.Time) bool {
	note, ok := notes[id]
	if !ok {
		return false
	}
	// Unclaimed, which is what presence alone used to mean: announced, and the
	// feeder has not applied it yet. Kept exactly so this can only suppress
	// more than it did.
	if note.left > 0 {
		return true
	}
	if at.IsZero() || note.at.IsZero() {
		return false
	}
	d := at.Sub(note.at)
	if d < 0 {
		d = -d
	}
	return d <= evictionWindow
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
			out[rec.ID] = evictionNote{left: rec.Left(), at: rec.At.UTC()}
		}
	}
	return out
}

// isPromotion reports whether this insert is a copy-up rather than the
// application creating a document.
func isPromotion(promoted map[string]promotion, id string, at time.Time, recent time.Time) bool {
	p, ok := promoted[id]
	if !ok {
		return false
	}
	// Announced and not yet spent, which means the insert this record was
	// written to precede has not been seen. Unconditional, the way an
	// unclaimed eviction announcement is, and for the same reason: it can only
	// suppress more than the window would and never less.
	if p.pending {
		return true
	}
	if p.at.After(recent) {
		return true
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
			ID      string    `bson:"_id"`
			At      time.Time `bson:"promotedAt"`
			Pending bool      `bson:"echoPending"`
		}
		if cur.Decode(&rec) == nil {
			out[rec.ID] = promotion{at: rec.At.UTC(), pending: rec.Pending}
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

// replaceNextBatch rebuilds a getMore reply with a different set of events.
//
// Everything else is copied through untouched, and postBatchResumeToken above
// all: it is how a client resumes, and inventing one would be worse than any
// event this filters. Dropping events from a batch is what a $match in the
// pipeline would have done, so the token stays correct for what remains.
func replaceNextBatch(reply bson.Raw, keep bson.A, requestID int32) (*wire.Message, error) {
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
			if ce.Key() == "nextBatch" {
				cursor = append(cursor, bson.E{Key: "nextBatch", Value: keep})
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
