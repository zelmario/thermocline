package router

import (
	"context"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// Sending a read to the node the client asked for.
//
// The rules below are narrow on purpose. Everything that is not plainly a read
// of documents, or that has any state tied to the connection it travels on, goes
// to the primary exactly as it always did. Getting this wrong does not produce a
// slow system, it produces a transaction that half happened on one node, so the
// bias is heavily towards leaving things where they are.

// routeToSecondary reports whether this command may be answered by a secondary.
func (c *conn) routeToSecondary(msg *wire.Message, name string) bool {
	// Only possible at all once authentication is terminated. Otherwise a second
	// connection would be authenticated as the router rather than as the caller,
	// which is the privilege bypass this project has already fixed once.
	if !c.r.cfg.TerminateAuth {
		return false
	}
	if msg.MoreToCome() {
		return false
	}
	if !readsDocuments[name] || name == "getMore" {
		return false
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return false
	}
	mode, ok := readPrefMode(cmd)
	if !ok || !wantsSecondary(mode) {
		return false
	}
	// A transaction is pinned to one node and to one connection for its whole
	// life, and its reads must see its own uncommitted writes, which exist
	// nowhere but the primary.
	if inTransaction(cmd) {
		return false
	}
	// An aggregation that ends in $out or $merge is a write wearing a read's
	// name.
	if name == "aggregate" && pipelineWritesOut(cmd) {
		return false
	}
	return true
}

// pipelineWritesOut reports whether a pipeline ends by writing a collection.
func pipelineWritesOut(cmd bson.Raw) bool {
	val, err := cmd.LookupErr("pipeline")
	if err != nil {
		return false
	}
	arr, err := val.Array().Values()
	if err != nil {
		// Unreadable means unknown, and unknown goes to the primary.
		return true
	}
	for _, v := range arr {
		doc, ok := v.DocumentOK()
		if !ok {
			continue
		}
		if _, err := doc.LookupErr("$out"); err == nil {
			return true
		}
		if _, err := doc.LookupErr("$merge"); err == nil {
			return true
		}
	}
	return false
}

// nextSecondary picks the member to send the next read to.
//
// Round robin rather than anything cleverer. The router has no latency
// measurements of its own, and "nearest" in a driver means nearest to the
// driver, which is not a thing this can observe from here. Spreading evenly is
// the honest reading of every non-primary mode, and it is what the client was
// really asking for: not this particular node, but not the primary.
func (r *Router) nextSecondary() (string, bool) {
	r.primaryMu.RLock()
	members := r.members
	primary := r.primary
	r.primaryMu.RUnlock()

	var candidates []string
	for _, m := range members {
		if m != primary {
			candidates = append(candidates, m)
		}
	}
	if len(candidates) == 0 {
		return "", false
	}
	n := atomic.AddUint64(&r.rr, 1)
	return candidates[int(n%uint64(len(candidates)))], true
}

// borrowUpstream returns the connection this command should travel on.
//
// The bool says whether it came from a pool, which decides whether it has to be
// given back. The client's own primary connection is not pooled and must not be
// released.
func (c *conn) borrowUpstream(ctx context.Context, msg *wire.Message, name string) (*pooled, bool) {
	// A cursor lives on the node that created it, so its follow-ups have to go
	// back to the same connection. Anything else gets "cursor not found" from a
	// node that never had it.
	if name == "getMore" || name == "killCursors" {
		if id, ok := cursorIDFromCommand(msg, name); ok {
			if p, pinned := c.pinned[id]; pinned {
				return p, true
			}
		}
		return nil, false
	}

	if !c.routeToSecondary(msg, name) {
		return nil, false
	}
	addr, ok := c.r.nextSecondary()
	if !ok {
		return nil, false
	}
	p, err := c.r.poolFor(addr).get(ctx)
	if err != nil {
		// A secondary that cannot be reached is not a failure to report to the
		// client: the primary can answer this, and answering is better than
		// refusing. Worth a line, because a secondary that is always down turns
		// read spreading off without saying so.
		c.log.Warn("could not use a secondary for a read, falling back to the primary",
			"addr", addr, "err", err)
		return nil, false
	}
	c.r.Stats.SecondaryReads.Add(1)
	return p, true
}

// keepOrRelease decides what happens to a borrowed connection once its reply is
// in hand.
//
// A reply carrying a live cursor id keeps the connection, because the cursor is
// on that node and nowhere else. Anything else gives it straight back.
func (c *conn) keepOrRelease(p *pooled, reply *wire.Message) {
	if p == nil {
		return
	}
	id, ok := cursorIDFromReply(reply)
	if ok && id != 0 {
		if c.pinned == nil {
			c.pinned = make(map[int64]*pooled)
		}
		c.pinned[id] = p
		return
	}
	// A getMore that exhausted its cursor returns id 0, so the pin goes too.
	if ok && id == 0 {
		c.unpinExhausted(p)
	}
	p.release()
}

// unpinExhausted forgets a cursor whose connection is being returned.
func (c *conn) unpinExhausted(p *pooled) {
	for id, q := range c.pinned {
		if q == p {
			delete(c.pinned, id)
		}
	}
}

// releasePinned closes every connection still holding a cursor for this client.
//
// Closed rather than returned to the pool. The cursors on them are still open,
// and handing such a connection to the next caller leaves those cursors alive
// until the server times them out. Closing is what tells the server to drop
// them, which is what the client disconnecting means.
func (c *conn) releasePinned() {
	for id, p := range c.pinned {
		p.discard()
		delete(c.pinned, id)
	}
}

// cursorIDFromCommand reads the cursor a getMore or killCursors names.
func cursorIDFromCommand(msg *wire.Message, name string) (int64, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return 0, false
	}
	if name == "getMore" {
		v, lerr := cmd.LookupErr("getMore")
		if lerr != nil {
			return 0, false
		}
		id, ok := v.Int64OK()
		return id, ok
	}
	// killCursors carries an array, and one connection per cursor means the
	// first one found is the connection to use. A driver batching cursors from
	// different nodes into one killCursors is not something a mongos would
	// produce, since it kills per namespace.
	v, lerr := cmd.LookupErr("cursors")
	if lerr != nil {
		return 0, false
	}
	vals, aerr := v.Array().Values()
	if aerr != nil || len(vals) == 0 {
		return 0, false
	}
	id, ok := vals[0].Int64OK()
	return id, ok
}

// cursorIDFromReply reads the cursor id a reply carries, if it carries one.
func cursorIDFromReply(reply *wire.Message) (int64, bool) {
	doc, err := reply.ReplyDoc()
	if err != nil {
		return 0, false
	}
	cur, err := doc.LookupErr("cursor")
	if err != nil {
		return 0, false
	}
	cdoc, ok := cur.DocumentOK()
	if !ok {
		return 0, false
	}
	v, err := cdoc.LookupErr("id")
	if err != nil {
		return 0, false
	}
	id, ok := v.Int64OK()
	return id, ok
}
