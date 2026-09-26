package router

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// A change stream batch the archive fills can be far larger than anything
// mongod sends.
//
// mongod keeps a batch under 16MB, and an update event whose lookup came back
// null is a few hundred bytes, so it packs as many as a thousand of them into
// one batch. fillLookups then puts a whole document into each. Measured on
// 2026-09-26 with 200 documents of 1MB, updated, evicted, and a stream resumed
// from a token taken before the updates: one first batch of 200 events and
// 209,826,801 bytes. mongosh read it. The Go driver refuses a message past
// maxMessageSizeBytes, 48MB, so it dropped the connection, retried the read,
// was sent the same reply, and gave up after 28 seconds; and a consumer that
// resumes from its stored token asks for that same batch every time, so it
// could never get past it.
//
// So a filled batch is cut at streamBatchBytes, mongod's own size, and what is
// left over is held here and handed out by the getMores that follow, before
// the fast tier is asked for anything new. What is held is the events as the
// fast tier sent them, not yet filled: at most one of mongod's batches, filled
// as it is delivered.
//
// The resume token has to follow the cut. A driver resumes from a batch's
// postBatchResumeToken once it has read the whole batch, and mongod's names
// the end of everything it sent, held events included. A reply that stops
// early says its last event's own _id instead, and only the reply that hands
// out the last held event carries mongod's.
const streamBatchBytes = 16 << 20

// fillChunk is how many events are filled at a time on the way to the cut, so
// that finding where to stop does not mean reading every document first.
const fillChunk = 32

// heldStream is what one change stream cursor has been sent by the fast tier
// and not yet handed to the client.
type heldStream struct {
	mu     sync.Mutex
	events bson.A
	// pbrt is the postBatchResumeToken of the batch these came from, for the
	// reply that hands out the last of them.
	pbrt bson.RawValue
	// top is that reply's other fields, ok, $clusterTime and operationTime.
	top   bson.D
	ns    string
	owner cursorOwner
	// lastUsed is when these were held or last handed out, for reapHeld.
	lastUsed time.Time
}

func (reg *changeStreamRegistry) hold(id int64, h *heldStream) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.held == nil {
		reg.held = map[int64]*heldStream{}
	}
	h.lastUsed = time.Now()
	reg.held[id] = h
}

// reapHeld forgets events held for a cursor nobody has asked about for the
// idle window, the same one the router's own cursors are reaped by. A client
// that crashed sends no killCursors, and what it left is up to 16MB.
func (reg *changeStreamRegistry) reapHeld(idle time.Duration) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	for id, h := range reg.held {
		h.mu.Lock()
		stale := time.Since(h.lastUsed) > idle
		h.mu.Unlock()
		if stale {
			delete(reg.held, id)
		}
	}
}

func (reg *changeStreamRegistry) heldFor(id int64) (*heldStream, bool) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	h, ok := reg.held[id]
	return h, ok
}

func (reg *changeStreamRegistry) release(id int64) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	delete(reg.held, id)
}

// fillBounded fills events in order until the next one would take the batch
// past streamBatchBytes, and returns the filled ones and the rest as they came.
// At least one event is always returned, so a single large one still goes.
func (r *Router) fillBounded(ctx context.Context, events bson.A) (bson.A, bson.A, int) {
	out := make(bson.A, 0, len(events))
	used, filled := 0, 0
	for start := 0; start < len(events); start += fillChunk {
		end := min(start+fillChunk, len(events))
		chunk, _ := r.fillLookups(ctx, events[start:end])
		for i, ev := range chunk {
			size := eventSize(ev)
			if len(out) > 0 && used+size > streamBatchBytes {
				return out, events[start+i:], filled
			}
			out = append(out, ev)
			used += size
			// An event fillLookups changed is one it filled.
			if eventSize(events[start+i]) != size {
				filled++
			}
		}
	}
	return out, nil, filled
}

// eventSize is an event's size as a document in the reply.
func eventSize(ev any) int {
	switch v := ev.(type) {
	case bson.RawValue:
		return len(v.Value)
	case bson.Raw:
		return len(v)
	}
	b, err := bson.Marshal(bson.D{{Key: "e", Value: ev}})
	if err != nil {
		return 0
	}
	return len(b)
}

// cutStreamBatch fills a sieved batch and cuts it at streamBatchBytes, holding
// the rest for the cursor's next getMore. It returns the events to send and
// the token the reply should carry, which is the zero value when the batch
// went whole and the reply's own token stands.
func (c *conn) cutStreamBatch(ctx context.Context, id int64, rdoc bson.Raw, owner cursorOwner, keep bson.A) (bson.A, bson.RawValue, int) {
	if id == 0 {
		// A cursor mongod has closed has no getMore to hand the rest to.
		out, filled := c.r.fillLookups(ctx, keep)
		return out, bson.RawValue{}, filled
	}
	out, rest, filled := c.r.fillBounded(ctx, keep)
	if len(rest) == 0 {
		return out, bson.RawValue{}, filled
	}
	h := &heldStream{
		events: append(bson.A(nil), rest...),
		owner:  owner,
	}
	if cur, err := rdoc.LookupErr("cursor"); err == nil {
		if cdoc, ok := cur.DocumentOK(); ok {
			h.pbrt = cdoc.Lookup("postBatchResumeToken")
			h.ns, _ = stringField(cdoc, "ns")
		}
	}
	if elems, err := rdoc.Elements(); err == nil {
		for _, e := range elems {
			if e.Key() != "cursor" {
				h.top = append(h.top, bson.E{Key: e.Key(), Value: e.Value()})
			}
		}
	}
	c.r.changeStreams.hold(id, h)
	c.log.Info("a change stream batch filled from the archive was past 16MB, handing it out over several getMores",
		"cursor", id, "sent", len(out), "held", len(rest))
	return out, lastEventID(out), filled
}

// lastEventID is the resume token of the last event in a batch.
func lastEventID(events bson.A) bson.RawValue {
	if len(events) == 0 {
		return bson.RawValue{}
	}
	switch v := events[len(events)-1].(type) {
	case bson.RawValue:
		if doc, ok := v.DocumentOK(); ok {
			return doc.Lookup("_id")
		}
	case bson.Raw:
		return v.Lookup("_id")
	}
	return bson.RawValue{}
}

// serveHeldStream answers a getMore for a change stream that has events held
// back, without asking the fast tier, under the checks mongod would have made:
// the collection has to be the cursor's and the caller has to be its owner.
func (c *conn) serveHeldStream(ctx context.Context, msg *wire.Message, name string) (bool, error) {
	if name != "getMore" {
		return false, nil
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return false, nil
	}
	id, ok := intOf64(cmd.Lookup("getMore"))
	if !ok {
		return false, nil
	}
	h, ok := c.r.changeStreams.heldFor(id)
	if !ok {
		return false, nil
	}
	if named, nok := stringField(cmd, "collection"); nok && db(cmd)+"."+named != h.ns {
		out, cerr := c.wrongCursorNamespace(msg, db(cmd)+"."+named, h.ns)
		if cerr != nil {
			return true, cerr
		}
		return true, c.send(out)
	}
	if !h.owner.sameAs(c.owner(cmd)) {
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a getMore for a change stream this client did not open",
			"ns", h.ns, "cursor", id, "differs_by", h.owner.differsFrom(c.owner(cmd)))
		out, cerr := c.notYourCursor(msg, id)
		if cerr != nil {
			return true, cerr
		}
		return true, c.send(out)
	}

	h.mu.Lock()
	if len(h.events) == 0 {
		// Handed out by a getMore that ran alongside this one, so this one
		// goes to the fast tier like any other.
		h.mu.Unlock()
		return false, nil
	}
	take := h.events
	if v, lerr := cmd.LookupErr("batchSize"); lerr == nil {
		if n, nok := intOf(v); nok && n > 0 && n < len(take) {
			take = take[:n]
		}
	}
	out, rest, _ := c.r.fillBounded(ctx, take)
	remaining := append(append(bson.A(nil), rest...), h.events[len(take):]...)
	token := h.pbrt
	if len(remaining) > 0 {
		h.events = remaining
		h.lastUsed = time.Now()
		token = lastEventID(out)
	} else {
		h.events = nil
		c.r.changeStreams.release(id)
	}
	h.mu.Unlock()

	cursor := bson.D{{Key: "nextBatch", Value: out}}
	if token.Type != 0 {
		cursor = append(cursor, bson.E{Key: "postBatchResumeToken", Value: token})
	}
	cursor = append(cursor, bson.E{Key: "id", Value: id}, bson.E{Key: "ns", Value: h.ns})
	reply := append(bson.D{{Key: "cursor", Value: cursor}}, h.top...)
	m, err := wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, reply)
	if err != nil {
		return true, err
	}
	return true, c.send(m)
}

// replaceStreamBatch is replaceBatch with the resume token set as well, when
// the batch was cut.
func replaceStreamBatch(reply bson.Raw, field string, keep bson.A, token bson.RawValue, requestID int32) (*wire.Message, error) {
	if token.Type == 0 {
		return replaceBatch(reply, field, keep, requestID)
	}
	elems, err := reply.Elements()
	if err != nil {
		return nil, err
	}
	out := make(bson.D, 0, len(elems))
	for _, e := range elems {
		cdoc, ok := e.Value().DocumentOK()
		if e.Key() != "cursor" || !ok {
			out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
			continue
		}
		celems, cerr := cdoc.Elements()
		if cerr != nil {
			return nil, cerr
		}
		cursor := make(bson.D, 0, len(celems))
		for _, ce := range celems {
			switch ce.Key() {
			case field:
				cursor = append(cursor, bson.E{Key: field, Value: keep})
			case "postBatchResumeToken":
				cursor = append(cursor, bson.E{Key: "postBatchResumeToken", Value: token})
			default:
				cursor = append(cursor, bson.E{Key: ce.Key(), Value: ce.Value()})
			}
		}
		out = append(out, bson.E{Key: "cursor", Value: cursor})
	}
	return wire.NewMsg(requestID, requestID, out)
}
