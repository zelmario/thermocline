// Package router puts one address in front of both tiers.
//
// It speaks the MongoDB wire protocol and presents itself as a mongos, so
// drivers connect to it exactly as they connect to a database today and no
// application changes are needed.
//
// Almost everything is forwarded to the fast tier untouched. The router only
// acts on a miss: a read that found nothing, or a write that matched nothing,
// where the document may still be in the archive. Those are the two cases where
// the fast tier's answer is wrong rather than merely incomplete.
package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/watermark"
	"github.com/zelmario/thermocline/internal/wire"
)

// Router accepts client connections and routes them between the tiers.
type Router struct {
	cfg   config.RouterConfig
	hot   *mongo.Client
	cold  *mongo.Client
	marks *watermark.Store
	log   *slog.Logger

	// primary is the fast tier address raw connections are proxied to. It is
	// re-resolved after a failure, so a failover is survivable.
	primaryMu sync.RWMutex
	primary   string

	// checkpoints reads how far mirroring has got, so a read served from the
	// archive can wait for it rather than answer from behind.
	checkpoints *checkpoint.Store
	positions   *checkpoint.Positions
	ckptCache   checkpointCache

	wmCache   watermarkCache
	idxCache  indexCache
	collCache collationCache
	viewCache viewCache

	// changeStreams are the client cursors whose events need this system's own
	// housekeeping filtered out of them.
	changeStreams changeStreamRegistry
	cursors       cursorRegistry
	addr          atomic.Value
	Stats         Stats

	// Metrics times commands by how they were served, which is what turns the
	// counters into a picture of what the fast tier is actually saving.
	Metrics *Metrics

	// inv is what each tier is holding, refreshed on a timer.
	inv     inventoryCache
	started time.Time
}

// Addr returns the address the router is listening on, once it is. Empty until
// then. Useful for tests that bind port zero and need to find out where they
// landed.
func (r *Router) Addr() string {
	if v, ok := r.addr.Load().(string); ok {
		return v
	}
	return ""
}

// Stats counts what the router did. Every one of these is a number an operator
// needs: copy-up rate says whether the boundary is cut too deep, and incomplete
// says whether queries are being answered from a tier that cannot answer them.
type Stats struct {
	Commands      atomic.Int64
	ReadMisses    atomic.Int64
	WriteMisses   atomic.Int64
	CopiedUp      atomic.Int64
	Incomplete    atomic.Int64
	Refused       atomic.Int64
	Archived      atomic.Int64
	TxnReplays    atomic.Int64
	WarmedForTxn  atomic.Int64
	DuplicateKeys atomic.Int64
	ChangeStreams atomic.Int64
	Unauthorized  atomic.Int64
	Unreadable    atomic.Int64
	EventsHidden  atomic.Int64
	StaleReads    atomic.Int64
	ClientsOpen   atomic.Int64
	ClientsTotal  atomic.Int64
}

// New builds a Router. It does not listen or connect.
func New(hot, cold *mongo.Client, cfg config.RouterConfig, log *slog.Logger) *Router {
	return &Router{
		cfg:         cfg,
		hot:         hot,
		cold:        cold,
		marks:       watermark.New(cold),
		checkpoints: checkpoint.New(cold),
		positions:   checkpoint.NewPositions(cold),
		log:         log,
		Metrics:     NewMetrics(metricsWindow),
		started:     time.Now(),
	}
}

// Listen serves until ctx is cancelled.
func (r *Router) Listen(ctx context.Context) error {
	if err := r.refreshPrimary(ctx); err != nil {
		return err
	}
	r.warnAboutMissingWatermarks(ctx)

	ln, err := net.Listen("tcp", r.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", r.cfg.Listen, err)
	}
	defer func() { _ = ln.Close() }()
	r.addr.Store(ln.Addr().String())

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	go r.reapCursors(ctx)

	r.log.Info("router listening",
		"addr", ln.Addr().String(),
		"fast_tier", r.currentPrimary(),
		"completeness", string(r.cfg.Completeness))

	var wg sync.WaitGroup
	for {
		client, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			r.log.Warn("accept failed", "err", err)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.serve(ctx, client)
		}()
	}
	wg.Wait()
	r.cursors.closeAll(context.WithoutCancel(ctx))
	return nil
}

// cursorIdle matches the server's own default cursor timeout, so an abandoned
// archive cursor is cleaned up on the same schedule mongod would use.
const cursorIdle = 10 * time.Minute

func (r *Router) reapCursors(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.cursors.reapIdle(ctx, cursorIdle)
		}
	}
}

// serve proxies one client connection for its lifetime.
//
// Each client gets its own connection to the fast tier rather than a shared
// pool. Authentication is a stateful conversation bound to a connection, so
// sharing one would mean re-implementing SCRAM here instead of letting the real
// server do it.
func (r *Router) serve(ctx context.Context, client net.Conn) {
	defer func() { _ = client.Close() }()
	r.Stats.ClientsOpen.Add(1)
	r.Stats.ClientsTotal.Add(1)
	defer r.Stats.ClientsOpen.Add(-1)

	upstream, err := r.dialPrimary(ctx)
	if err != nil {
		r.log.Error("cannot reach the fast tier", "err", err)
		return
	}
	defer func() { _ = upstream.Close() }()

	c := &conn{
		r:        r,
		client:   client,
		upstream: upstream,
		log:      r.log,
	}
	if err := c.pump(ctx); err != nil && !isClosed(err) {
		r.log.Debug("connection ended", "err", err)
	}
}

// conn is one client and its dedicated fast tier connection.
type conn struct {
	r        *Router
	client   net.Conn
	upstream net.Conn
	log      *slog.Logger

	// cls is how the command in flight was served. Every path that does
	// something other than hand the command to the fast tier sets it, and pump
	// files the timing under it once the reply has gone out.
	cls Class

	// pendingKeys are unique key claims made for the insert now in flight, kept
	// so they can be released if the fast tier rejects it.
	pendingKeys []string

	// txnKeys is what an update inside a transaction left to be checked once
	// the reply is back and the real post-image can be read.
	txnKeys txnKeyCheck
}

func (c *conn) pump(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		msg, err := wire.Read(c.client)
		if err != nil {
			return err
		}
		c.r.Stats.Commands.Add(1)

		// Nothing should arrive compressed, because the handshake never agrees
		// a compressor. If one does, the command cannot be read and therefore
		// cannot be guarded, and that has to be said rather than discovered.
		if msg.Header.OpCode == wire.OpCompressed {
			c.r.Stats.Unreadable.Add(1)
			c.log.Warn("a compressed message arrived and cannot be inspected",
				"effect", "no guard runs for it: unique keys, completeness and copy-up are all skipped",
				"remedy", "remove compressors from the connection string, or put the router behind "+
					"a client that does not compress")
		}

		// Timed around the whole exchange, so what is recorded is what the
		// client waited for and not what the router thinks it did.
		started := time.Now()
		c.cls = classOf(msg)
		err = c.handle(ctx, msg)
		c.r.Metrics.Record(c.cls, time.Since(started))
		if err != nil {
			return err
		}
	}
}

// handle forwards one message and decides whether the reply needs work.
func (c *conn) handle(ctx context.Context, msg *wire.Message) error {
	name, db, nameErr := msg.CommandName()

	// Cleared per command. An in-transaction key check that was armed and then
	// never reached, because some path answered the command early, would
	// otherwise be consumed by whatever command came next, against ids from a
	// statement that has already finished.
	c.txnKeys = txnKeyCheck{}

	// Cursors this router owns never existed on the fast tier, so their
	// follow-ups have to be served here or the driver gets "cursor not found".
	if nameErr == nil && !msg.MoreToCome() {
		if handled, err := c.serveOwnCursors(ctx, msg, name); handled {
			return err
		}
	}

	// Aggregations are judged before they run, not on the way back. A pipeline
	// can end in $out or $merge, and acting after the fact would leave a
	// truncated collection already written.
	if nameErr == nil && name == "aggregate" && !msg.MoreToCome() {
		if out, done := c.guardAggregate(ctx, msg, db); done {
			return wire.Write(c.client, out)
		}
	}

	// Renames split a tiered collection between the tiers, and mapReduce reads
	// every document on whichever tier it runs. Neither can be made whole
	// across two clusters, so both are refused rather than answered short.
	if nameErr == nil && !msg.MoreToCome() &&
		(name == "renameCollection" || name == "mapReduce" || name == "mapreduce") {
		if out, done := c.guardDDL(ctx, msg, db, name); done {
			return wire.Write(c.client, out)
		}
	}

	// count and distinct read documents the same way find does, so they need
	// the same completeness check.
	if nameErr == nil && (name == "count" || name == "distinct") && !msg.MoreToCome() {
		if out, done := c.guardSimpleRead(ctx, msg, db, name); done {
			return wire.Write(c.client, out)
		}
	}

	// A write naming many documents only reaches the ones the fast tier holds,
	// so the rest have to come up before it runs. Before, not after: repeating
	// the write after a copy-up would apply $inc twice.
	if nameErr == nil && (name == "update" || name == "delete") && !msg.MoreToCome() {
		if out, done := c.guardMultiWrite(ctx, msg, db, name); done {
			return wire.Write(c.client, out)
		}
	}

	// bulkWrite is guarded before the switch below, because it is not shaped
	// like the commands there: it names its collections in nsInfo and its
	// operations point at them by position, so none of the per-command guards
	// can find a collection to act on.
	c.pendingKeys = nil
	if nameErr == nil && name == "bulkWrite" && !msg.MoreToCome() {
		if cmd, cerr := msg.CommandDoc(); cerr == nil {
			if out, done := c.guardBulkWrite(ctx, msg, cmd); done {
				return wire.Write(c.client, out)
			}
		}
	}

	// A unique key has to be claimed before the write lands, or two writes
	// racing both see a free slot.
	if nameErr == nil && !msg.MoreToCome() {
		switch name {
		case "insert":
			if out, done := c.guardInsert(ctx, msg, db); done {
				return wire.Write(c.client, out)
			}
		case "update", "findAndModify", "findandmodify":
			// An update can move a document onto a value an archived document
			// holds, which the fast tier's unique index cannot see any more than
			// it can for an insert. It runs after guardMultiWrite so the matched
			// documents are all present to compute keys from.
			//
			// Once a transaction is open, the documents this update will touch
			// can include ones the transaction has written and not committed,
			// which are visible only from inside its own session. There the
			// guard runs around the write instead of before it, and refuses by
			// aborting. The statement that opens the transaction is not a
			// special case for correctness, because a transaction cannot have
			// written anything before its first command.
			cmd, cmdErr := msg.CommandDoc()
			t, inTxn := txnFields{}, false
			if cmdErr == nil {
				t, inTxn = txnOf(cmd)
			}
			if inTxn && t.open {
				// Only notes what the statement is about to touch. The check
				// itself happens after the reply, below, so that everything
				// else this command needs, above all the copy-up an update
				// that matched nothing depends on, still runs untouched.
				c.prepareTxnKeyCheck(ctx, msg, cmd, db, name, t)
			} else if out, done := c.guardUpdateKeys(ctx, msg, db, name); done {
				return wire.Write(c.client, out)
			}
		}
	}

	// A handshake that agrees a compressor would make every later command
	// unreadable here, and an unreadable command is an unguarded one.
	if nameErr == nil && isHello(name) {
		msg = c.unnegotiateCompression(msg)
	}

	if err := wire.Write(c.upstream, msg); err != nil {
		return fmt.Errorf("forward to fast tier: %w", err)
	}
	// Fire-and-forget writes expect no reply. Sending one would leave the
	// connection a message out of step for everything that follows.
	if msg.MoreToCome() {
		return nil
	}

	reply, err := wire.Read(c.upstream)
	if err != nil {
		return fmt.Errorf("read from fast tier: %w", err)
	}
	if nameErr != nil {
		return wire.Write(c.client, reply)
	}

	// An insert the fast tier rejected never happened, so its key claims have
	// to go back, or that value is blocked until something else collides.
	if len(c.pendingKeys) > 0 {
		if rdoc, derr := reply.ReplyDoc(); derr != nil || !replyOK(rdoc) || writeFailed(rdoc) {
			c.r.releaseKeys(ctx, c.pendingKeys)
		}
		c.pendingKeys = nil
	}

	// An index change makes what this router remembers about the collection
	// wrong, and the window that matters most is right after somebody declares
	// a field unique.
	if cmd, cerr := msg.CommandDoc(); cerr == nil {
		if coll, ok := indexDDL(cmd, name); ok {
			c.r.forgetIndexes(db + "." + coll)
			c.r.forgetView(db + "." + coll)
			// A collection's collation is fixed at creation, so the only way it
			// changes is a drop and a recreate, which comes through here.
			c.r.forgetCollation(db + "." + coll)
		}
	}

	if isHello(name) {
		return c.replyHello(msg, reply)
	}

	// A change stream on a tiered collection reports the evictor's removals as
	// deletes and copy-ups as inserts. Both are this system's own work, and
	// neither happened as far as the application is concerned.
	if name == "aggregate" {
		if cmd, cerr := msg.CommandDoc(); cerr == nil {
			c.noteChangeStream(cmd, db, reply)
		}
	}
	if name == "getMore" {
		if out, filtered := c.filterChangeStream(ctx, msg, reply); filtered {
			return wire.Write(c.client, out)
		}
	}
	if name == "killCursors" {
		if cmd, cerr := msg.CommandDoc(); cerr == nil {
			for _, id := range cursorIDsOf(cmd) {
				c.r.changeStreams.drop(id)
			}
		}
	}

	rewritten, err := c.intercept(ctx, msg, name, db, reply)
	if err != nil {
		c.log.Warn("routing failed, returning the fast tier's answer",
			"command", name, "err", err)
		return wire.Write(c.client, reply)
	}

	// Inside a transaction the unique key guard runs here, on what the write
	// actually produced, rather than on a guess made before it ran. Nothing is
	// visible outside the transaction yet, so a duplicate can still be refused
	// by aborting it.
	if out, done := c.verifyTxnKeys(ctx, msg, rewritten, name); done {
		return wire.Write(c.client, out)
	}
	return wire.Write(c.client, rewritten)
}

// replyHello rewrites the handshake so the driver stays with this proxy.
func (c *conn) replyHello(msg, reply *wire.Message) error {
	doc, err := reply.ReplyDoc()
	if err != nil {
		return wire.Write(c.client, reply)
	}
	out, err := mongosIdentity(doc)
	if err != nil {
		return wire.Write(c.client, reply)
	}

	var rewritten *wire.Message
	if msg.Header.OpCode == wire.OpQuery {
		rewritten, err = wire.NewReply(reply.Header.RequestID, msg.Header.RequestID, out)
	} else {
		rewritten, err = wire.NewMsg(reply.Header.RequestID, msg.Header.RequestID, out)
	}
	if err != nil {
		return wire.Write(c.client, reply)
	}
	return wire.Write(c.client, rewritten)
}

// currentPrimary returns the fast tier address raw connections use.
func (r *Router) currentPrimary() string {
	r.primaryMu.RLock()
	defer r.primaryMu.RUnlock()
	return r.primary
}

// refreshPrimary asks the fast tier which member is currently primary.
//
// Raw byte proxying needs one address, but a replica set moves. Re-resolving
// through the driver, which already tracks topology, keeps a failover from
// being permanent.
func (r *Router) refreshPrimary(ctx context.Context) error {
	var hello struct {
		Primary string `bson:"primary"`
		Me      string `bson:"me"`
	}
	err := r.hot.Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
	if err != nil {
		return fmt.Errorf("resolve fast tier primary: %w", err)
	}

	addr := hello.Primary
	if addr == "" {
		addr = hello.Me
	}
	if addr == "" {
		return errors.New("fast tier reported no primary address")
	}

	r.primaryMu.Lock()
	changed := r.primary != addr
	r.primary = addr
	r.primaryMu.Unlock()

	if changed {
		r.log.Info("fast tier primary resolved", "addr", addr)
	}
	return nil
}

// primaryWait is how long a new connection waits for the fast tier to have a
// primary, mirroring a driver's serverSelectionTimeoutMS.
//
// This matters because of an asymmetry the proxy creates. Talking to a replica
// set directly, a driver that loses the primary blocks in server selection
// until a new one is elected, and the application sees a pause. Talking to the
// router, the client only knows about one server and that server stays up, so
// there is no server selection to do: a failed dial upstream came straight back
// as an error and the write failed outright. Measured during an election, six
// writes a second apart failed rather than pausing. Waiting here restores the
// behaviour the application would have had without the proxy.
const primaryWait = 20 * time.Second

func (r *Router) dialPrimary(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	deadline := time.Now().Add(primaryWait)

	var lastErr error
	for attempt := 0; ; attempt++ {
		addr := r.currentPrimary()
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			if attempt > 0 {
				r.log.Info("fast tier reachable again", "addr", addr, "waited", time.Since(deadline.Add(-primaryWait)).Round(time.Second))
			}
			return conn, nil
		}
		lastErr = err

		// The address is stale after a failover, and during the election there
		// may be no primary at all to re-resolve to.
		_ = r.refreshPrimary(ctx)

		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("dial %s: %w", addr, lastErr)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func isClosed(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}
