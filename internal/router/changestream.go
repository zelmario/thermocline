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
	if !isCS || ns == "" || !c.r.cfg.Manages(ns) {
		return
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return
	}
	id, ok := cursorIDOf(rdoc)
	if !ok || id == 0 {
		return
	}
	c.r.changeStreams.add(id, ns)
	c.log.Info("filtering tiering events out of a change stream", "ns", ns, "cursor", id)
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
	ns, watched := c.r.changeStreams.get(id)
	if !watched {
		return nil, false
	}
	c.log.Debug("filtering a change stream batch", "ns", ns, "cursor", id)

	rdoc, derr := reply.ReplyDoc()
	if derr != nil || !replyOK(rdoc) {
		return nil, false
	}
	events, eok := nextBatch(rdoc)
	if !eok || len(events) == 0 {
		return nil, false
	}

	keep, dropped := c.r.sieveEvents(ctx, ns, events)
	// A lookup the fast tier could not answer is asked of the archive, because
	// null means deleted and this document is not. See fillLookups.
	keep, filled := c.r.fillLookups(ctx, ns, keep)
	if dropped == 0 && filled == 0 {
		return nil, false
	}
	if dropped > 0 {
		c.r.Stats.EventsHidden.Add(int64(dropped))
		c.log.Info("hid tiering events from a change stream", "ns", ns, "hidden", dropped)
	}
	if filled > 0 {
		c.log.Debug("answered a change stream's document lookups from the archive",
			"ns", ns, "filled", filled)
	}

	out, berr := replaceNextBatch(rdoc, keep, msg.Header.RequestID)
	if berr != nil {
		return nil, false
	}
	return out, true
}

// sieveEvents returns the events an application should see, and how many were
// this system's own work.
func (r *Router) sieveEvents(ctx context.Context, ns string, events []bson.RawValue) (bson.A, int) {
	// Gather the ids to ask about, so one lookup covers the whole batch.
	var deleteIDs, insertIDs []string
	kinds := make([]string, len(events))
	docIDs := make([]string, len(events))

	for i, ev := range events {
		doc, ok := ev.DocumentOK()
		if !ok {
			continue
		}
		op, _ := lookupString(doc, "operationType")
		kinds[i] = op
		if op != "delete" && op != "insert" {
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

	evicted := r.announcedIn(ctx, meta.Evicting, deleteIDs, time.Time{})
	promoted := r.announcedIn(ctx, meta.Residency, insertIDs, time.Now().Add(-promotionWindow))

	keep := make(bson.A, 0, len(events))
	dropped := 0
	for i, ev := range events {
		switch {
		case kinds[i] == "delete" && evicted[docIDs[i]]:
			dropped++
		case kinds[i] == "insert" && promoted[docIDs[i]]:
			dropped++
		default:
			keep = append(keep, ev)
		}
	}
	return keep, dropped
}

// announcedIn reports which of these document ids appear in one of the
// bookkeeping collections.
//
// since, when set, requires the record to be newer than it, which is what
// separates a document promoted a moment ago from one that has merely been
// resident for a month.
func (r *Router) announcedIn(ctx context.Context, coll string, ids []string, since time.Time) map[string]bool {
	out := map[string]bool{}
	if len(ids) == 0 {
		return out
	}
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}
	if !since.IsZero() {
		filter = append(filter, bson.E{Key: "promotedAt", Value: bson.D{{Key: "$gte", Value: since}}})
	}
	cur, err := r.cold.Database(meta.DB).Collection(coll).Find(ctx, filter)
	if err != nil {
		// Nothing is hidden when the answer cannot be had. An application
		// seeing an eviction is bad; an application missing a real delete
		// because a lookup failed would be worse.
		r.log.Warn("could not read tiering announcements, letting the events through",
			"collection", coll, "err", err)
		return out
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	for cur.Next(ctx) {
		var rec struct {
			ID string `bson:"_id"`
		}
		if cur.Decode(&rec) == nil {
			out[rec.ID] = true
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
func (r *Router) fillLookups(ctx context.Context, ns string, events bson.A) (bson.A, int) {
	type pending struct {
		index int
		id    bson.RawValue
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
		want = append(want, pending{index: i, id: idv})
	}
	if len(want) == 0 {
		return events, 0
	}

	ids := make(bson.A, 0, len(want))
	for _, w := range want {
		ids = append(ids, w.id)
	}
	db, coll := splitNS(ns)
	cur, err := r.cold.Database(db).Collection(coll).Find(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
		meta.NotArchivedDeleted(),
	})
	if err != nil {
		r.log.Warn("could not ask the archive for a change stream's documents; they stay null",
			"ns", ns, "err", err)
		return events, 0
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	found := make(map[string]bson.Raw, len(want))
	for cur.Next(ctx) {
		doc := bson.Raw(append([]byte(nil), cur.Current...))
		if idv, ierr := doc.LookupErr("_id"); ierr == nil {
			found[string(idv.Value)+string(rune(idv.Type))] = doc
		}
	}

	filled := 0
	out := make(bson.A, len(events))
	copy(out, events)
	for _, w := range want {
		doc, ok := found[string(w.id.Value)+string(rune(w.id.Type))]
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
