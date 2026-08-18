package router

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/zelmario/thermocline/internal/wire"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Reusable authenticated connections to one member of the fast tier.
//
// This could not exist while authentication was proxied. A connection was only
// useful to the one client whose SCRAM exchange had travelled down it, so there
// was nothing to share and nothing to pool: one client, one connection, for the
// life of the client. Terminating authentication changes what a connection is.
// It is now the router's, authenticated as the router's own account, and any
// client's command can be sent down any of them, because who the caller is has
// already been established and checked before the command gets here.
//
// That is what makes reading from a secondary possible at last, and it is the
// same fact that makes pooling possible, since both were blocked by the same
// thing.

// maxIdlePerMember caps how many spare connections are kept to one member.
//
// Idle connections cost a file descriptor here and a connection slot on the
// server, and the point of pooling is to stop paying the dial and the SCRAM
// exchange per command, not to hold every connection ever opened.
const maxIdlePerMember = 16

// upstreamPool hands out authenticated connections to one address.
type upstreamPool struct {
	r    *Router
	addr string

	mu   sync.Mutex
	idle []net.Conn
}

// pooled is a connection borrowed from a pool, and the pool to give it back to.
type pooled struct {
	conn net.Conn
	pool *upstreamPool
}

// poolFor returns the pool for one member, creating it on first use.
func (r *Router) poolFor(addr string) *upstreamPool {
	r.poolMu.Lock()
	defer r.poolMu.Unlock()
	if r.pools == nil {
		r.pools = make(map[string]*upstreamPool)
	}
	p, ok := r.pools[addr]
	if !ok {
		p = &upstreamPool{r: r, addr: addr}
		r.pools[addr] = p
	}
	return p
}

// get returns a connection ready to carry a command.
func (p *upstreamPool) get(ctx context.Context) (*pooled, error) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		conn := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return &pooled{conn: conn, pool: p}, nil
	}
	p.mu.Unlock()

	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", p.addr, err)
	}
	// A pooled connection carries the same raw bytes as any other, so it needs
	// the same wrapping when the fast tier requires TLS. See tls.go.
	if conn, err = p.r.upstreamTLS(conn, p.addr); err != nil {
		return nil, err
	}
	// The handshake belongs to the connection, not to the client, and it happens
	// exactly once here.
	//
	// mongod accepts a client metadata document only in the first hello on a
	// connection, so a pooled connection cannot carry a second client's
	// handshake: the borrower after the first is answered "The client metadata
	// document may only be sent in the first hello" and its whole session fails.
	// So the router introduces itself once, as itself, and strips the metadata
	// from the client handshakes it forwards afterwards. It is also the honest
	// description: the connection is thermocline's, and anyone reading currentOp
	// on the fast tier should see that rather than whichever application
	// happened to borrow it first.
	tmpl, err := p.r.helloUpstream(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("handshake with %s: %w", p.addr, err)
	}
	// Kept so client handshakes can be answered here instead of forwarded. It
	// describes the member, not the connection, so any of them will do.
	p.r.setHelloTemplate(tmpl)
	// Every connection this router owns is authenticated as the router before
	// it carries anything. There is no path here that produces an anonymous
	// connection to either tier.
	if err := p.r.authenticateUpstream(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("authenticate to %s: %w", p.addr, err)
	}
	return &pooled{conn: conn, pool: p}, nil
}

// release returns a connection for reuse.
//
// Only ever call this when the exchange on the connection finished cleanly. A
// connection whose reply was never read, or was read half way, is one message
// out of step forever, and handing it to the next caller turns one failed
// command into every command after it. discard is for that case.
func (c *pooled) release() {
	if c == nil || c.pool == nil {
		return
	}
	p := c.pool
	p.mu.Lock()
	if len(p.idle) >= maxIdlePerMember {
		p.mu.Unlock()
		_ = c.conn.Close()
		return
	}
	p.idle = append(p.idle, c.conn)
	p.mu.Unlock()
}

// discard closes a connection instead of returning it.
func (c *pooled) discard() {
	if c == nil {
		return
	}
	_ = c.conn.Close()
}

// closeIdle shuts the pools down.
func (r *Router) closeIdle() {
	r.poolMu.Lock()
	pools := make([]*upstreamPool, 0, len(r.pools))
	for _, p := range r.pools {
		pools = append(pools, p)
	}
	r.poolMu.Unlock()
	for _, p := range pools {
		p.mu.Lock()
		for _, conn := range p.idle {
			_ = conn.Close()
		}
		p.idle = nil
		p.mu.Unlock()
	}
}

// The client's own connection to the primary, borrowed per command.
//
// One upstream connection per client was correct while authentication was
// proxied, because the connection was the client's identity and could not be
// used for anyone else. It is also the thing that will not scale: ten thousand
// mostly idle clients held ten thousand mostly idle connections to the primary,
// and a replica set member has a finite number of them.
//
// With authentication terminated the connection is the router's, so it can be
// handed to whoever has a command in flight and taken back afterwards. Idle
// clients then cost nothing upstream.
//
// What has to stay put is a transaction. Its statements are keyed by session
// rather than by connection, so a replica set would very likely accept them
// arriving on different ones, and "very likely" is not a basis for a
// transaction. The lease is held from the first statement to the commit or
// abort, which is a short time by construction, and everything else is borrowed
// for exactly one command.
//
// Cursors are deliberately not pinned here, and that is not an inconsistency
// with the secondary path. A cursor lives on a node, and every connection in
// this pool goes to the same node, so any of them can carry its getMore. The
// secondary path pins because there the cursor is on a different node.

// borrowPrimary gives this connection the upstream it should use for one
// command, keeping the one it already holds if a transaction is open.
func (c *conn) borrowPrimary(ctx context.Context) error {
	if !c.pooledPrimary() {
		return nil
	}
	if c.lease != nil {
		c.upstream = c.lease.conn
		return nil
	}
	p, err := c.r.poolFor(c.r.currentPrimary()).get(ctx)
	if err != nil {
		return fmt.Errorf("reach the fast tier: %w", err)
	}
	c.lease = p
	c.upstream = p.conn
	return nil
}

// upstreamBroke records that an exchange on the borrowed connection did not
// finish, so it must be thrown away rather than handed to the next caller.
//
// This is the rule that matters most in a pool. A connection whose reply was
// never read, or was read half way, is one message out of step forever, and
// returning it turns one failed command into every command after it, for a
// different client each time.
func (c *conn) upstreamBroke() { c.dirty = true }

// returnPrimary gives the connection back unless a transaction still needs it.
func (c *conn) returnPrimary(keep bool) {
	if !c.pooledPrimary() || c.lease == nil {
		return
	}
	if c.dirty {
		c.dirty = false
		c.dropPrimary()
		return
	}
	if keep {
		return
	}
	c.lease.release()
	c.lease = nil
	c.upstream = nil
}

// dropPrimary throws the connection away rather than returning it, which is the
// only safe thing to do when an exchange on it did not finish.
func (c *conn) dropPrimary() {
	if c.lease == nil {
		return
	}
	c.lease.discard()
	c.lease = nil
	c.upstream = nil
}

// pooledPrimary reports whether this connection borrows rather than owns its
// upstream. Only possible once authentication is terminated: a proxied
// connection carries the client's identity and belongs to nobody else.
func (c *conn) pooledPrimary() bool { return c.r.cfg.TerminateAuth }

// noteTransaction records whether a transaction is open on this connection, so
// the lease is held across it and released after it.
//
// Read from the command rather than tracked by hand, because the driver is the
// authority: autocommit false with a session and a transaction number is what a
// statement inside a transaction looks like, and commit and abort are the two
// that end one. A transaction abandoned without either leaves the flag set, and
// the connection is then closed when the client goes rather than handed on with
// a transaction still open on it.
func (c *conn) noteTransaction(msg *wire.Message, name string, nameErr error) {
	if !c.pooledPrimary() || nameErr != nil {
		return
	}
	switch name {
	case "commitTransaction", "abortTransaction":
		c.txnOpen = false
		return
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return
	}
	if inTransaction(cmd) {
		c.txnOpen = true
	}
}

// closePool shuts and forgets the pool for one member, which is what a failover
// makes of the old primary's connections.
func (r *Router) closePool(addr string) {
	r.poolMu.Lock()
	p := r.pools[addr]
	delete(r.pools, addr)
	r.poolMu.Unlock()
	if p == nil {
		return
	}
	p.mu.Lock()
	for _, conn := range p.idle {
		_ = conn.Close()
	}
	p.idle = nil
	p.mu.Unlock()
}

// helloTemplate returns the fast tier's own handshake reply, which is what a
// client handshake is answered from.
func (r *Router) helloTemplate() (bson.Raw, bool) {
	v := r.hello.Load()
	if v == nil {
		return nil, false
	}
	raw, ok := v.(bson.Raw)
	return raw, ok && len(raw) > 0
}

func (r *Router) setHelloTemplate(doc bson.Raw) {
	if len(doc) > 0 {
		r.hello.Store(doc)
	}
}
