package router

import (
	"context"
	"sync/atomic"
	"time"

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
		if m != primary && !r.avoidingSecondary(m) {
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

// secondaryCooldown is how long a member stays out of the rotation after it
// says it is neither primary nor secondary.
//
// Long enough that a maintenance window or a resync is not retried on every
// read, short enough that a member which finished one is used again without
// anybody restarting a router. A driver's own equivalent is heartbeat driven
// and finer than this; the router has no heartbeats of its own and learns from
// what the member said, which is the same rule dialPrimary follows.
const secondaryCooldown = 30 * time.Second

// avoidingSecondary reports whether a member is in its cooldown.
func (r *Router) avoidingSecondary(addr string) bool {
	r.avoidMu.Lock()
	defer r.avoidMu.Unlock()
	until, ok := r.avoidUntil[addr]
	return ok && time.Now().Before(until)
}

// avoidSecondary takes a member out of the rotation for a while.
func (r *Router) avoidSecondary(addr string) {
	now := time.Now()
	r.avoidMu.Lock()
	if r.avoidUntil == nil {
		r.avoidUntil = make(map[string]time.Time)
	}
	until, ok := r.avoidUntil[addr]
	fresh := !ok || now.After(until)
	r.avoidUntil[addr] = now.Add(secondaryCooldown)
	r.avoidMu.Unlock()

	if fresh {
		// Once per cooldown rather than once per read, and worth saying at all:
		// a member that is up and not readable spreads no load, and nothing
		// else here would mention it.
		r.log.Warn("a member of the fast tier is in the set but not readable, so reads are not being sent to it",
			"addr", addr, "for", secondaryCooldown,
			"meaning", "it answered a read saying it is neither primary nor secondary, which is what a resync, an initial sync or a maintenance window looks like",
			"effect", "reads that asked for a secondary go to the remaining members, and to the primary if there are none")
	}
	// The connections already open to it are no more readable than the next
	// one would be.
	r.closePool(addr)
}

// moveReadToPrimary sends a read again on the primary after the secondary it
// was given refused it, and reports whether it got an answer.
//
// The router picks a secondary out of hello's `hosts`, and `hosts` is every
// configured member rather than every readable one. A member in a maintenance
// window, an initial sync or a rollback stays in that list, stays up, keeps
// accepting connections and answers reads with NotPrimaryOrSecondary. A driver
// talking to the set directly never picks such a member, so the application
// never sees it. Measured with one secondary in maintenance and retries turned
// off: ten reads straight at the set all answered, ten through the router all
// failed.
//
// Re-sending is safe in a way a write would not be: the read did not run, and
// reading it again on the primary is the same fallback this already does when a
// secondary cannot be dialled at all.
func (c *conn) moveReadToPrimary(ctx context.Context, msg *wire.Message, name string, borrowed *pooled) (*wire.Message, bool) {
	// Noted before the command is looked at, because what the member said about
	// itself does not depend on which command asked.
	c.r.avoidSecondary(borrowed.pool.addr)

	// A cursor lives on the node that made it and cannot be moved, so the
	// client is told, which is exactly what it would be told talking to the set
	// directly. The member still leaves the rotation, so the next read does not
	// go the same way.
	if name == "getMore" || name == "killCursors" {
		return nil, false
	}
	borrowed.discard()
	c.unpinExhausted(borrowed)

	if c.upstream == nil {
		return nil, false
	}
	if err := wire.Write(c.upstream, msg); err != nil {
		c.upstreamBroke()
		return nil, false
	}
	out, err := wire.Read(c.upstream)
	if err != nil {
		c.upstreamBroke()
		return nil, false
	}
	c.r.Stats.SecondaryReadsMoved.Add(1)
	return out, true
}
