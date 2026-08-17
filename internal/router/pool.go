package router

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
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
