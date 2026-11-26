package router

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/zelmario/thermocline/internal/wire"
)

// Cursors left on a member that stopped being primary.
//
// While authentication is proxied, a client's upstream connection carries that
// client's identity to one member, and after a failover the router ends it,
// because every command on it would be refused (see endAfterFailover). A cursor
// that member opened before the step-down is still there: mongod keeps a read
// cursor open across a step-down, and a driver talking straight to the set
// sends the getMore back to the member that made it. Through the router there
// was no connection to that member left, so the getMore went to the new
// primary, which had never heard of the cursor.
//
// Measured on the box on 2026-11-26, a find with batchSize 2 on a collection
// the router does not tier, the primary stepped down, one write, then the rest
// of the cursor: straight to the set 3 of 3 read on, through the router 3 of 3
// got CursorNotFound. With two shops writing and a step-down every 6 minutes it
// was 3 failed getMores in 30 minutes.
//
// So the upstream of a connection ended for a failover is parked instead of
// closed, under the user the fast tier said it is, and a getMore the current
// member does not know is tried on that same user's parked connections.
// mongod ties a cursor to its user, not its connection, so the parked one
// continues it, and no client reaches a cursor another user could not. It is
// kept as long as mongod keeps an idle cursor, and the number kept is bounded.

// parkKeep is how long a parked upstream is kept unused: mongod's own default
// cursorTimeoutMillis, after which there is nothing left on it to continue.
const parkKeep = 10 * time.Minute

// parkMax bounds the upstreams kept at once. The oldest goes first.
const parkMax = 64

// parkedUp is the upstream of a client connection ended for a failover.
type parkedUp struct {
	mu     sync.Mutex
	conn   net.Conn
	member string
	user   string
	used   time.Time
	dead   bool
}

// parkedSet is the router's parked upstreams.
type parkedSet struct {
	mu   sync.Mutex
	list []*parkedUp
}

// park keeps up, the upstream of a connection authenticated as user against
// member. It reports false when it did not, and the caller closes it.
func (s *parkedSet) park(up net.Conn, member, user string, now time.Time) bool {
	if up == nil || user == "" || member == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	for len(s.list) >= parkMax {
		old := s.list[0]
		s.list = s.list[1:]
		old.close()
	}
	s.list = append(s.list, &parkedUp{conn: up, member: member, user: user, used: now})
	return true
}

// pruneLocked closes and forgets what has gone unused past parkKeep or died.
func (s *parkedSet) pruneLocked(now time.Time) {
	kept := s.list[:0]
	for _, p := range s.list {
		p.mu.Lock()
		stale := p.dead || now.Sub(p.used) > parkKeep
		p.mu.Unlock()
		if stale {
			p.close()
			continue
		}
		kept = append(kept, p)
	}
	for i := len(kept); i < len(s.list); i++ {
		s.list[i] = nil
	}
	s.list = kept
}

// forUser lists the parked upstreams authenticated as user on a member other
// than except.
func (s *parkedSet) forUser(user, except string, now time.Time) []*parkedUp {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	var out []*parkedUp
	for _, p := range s.list {
		if p.user == user && p.member != except {
			out = append(out, p)
		}
	}
	return out
}

func (p *parkedUp) close() {
	p.mu.Lock()
	p.dead = true
	_ = p.conn.Close()
	p.mu.Unlock()
}

// exchange sends msg on the parked upstream and reads its reply. A reply that
// answers another request means something was left unread on it when it was
// parked, and the upstream is finished.
func (p *parkedUp) exchange(msg *wire.Message, now time.Time) (*wire.Message, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dead {
		return nil, false
	}
	_ = p.conn.SetDeadline(now.Add(30 * time.Second))
	defer func() { _ = p.conn.SetDeadline(time.Time{}) }()
	if err := wire.Write(p.conn, msg); err != nil {
		p.dead = true
		_ = p.conn.Close()
		return nil, false
	}
	reply, err := wire.Read(p.conn)
	if err != nil || reply.Header.ResponseTo != msg.Header.RequestID {
		p.dead = true
		_ = p.conn.Close()
		return nil, false
	}
	p.used = now
	return reply, true
}

// retryLostCursor sends a getMore the current member answered CursorNotFound
// to this user's parked upstreams, and returns the first reply that is not
// CursorNotFound too.
func (c *conn) retryLostCursor(msg *wire.Message, reply *wire.Message) (*wire.Message, bool) {
	if msg.MoreToCome() {
		return nil, false
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil {
		return nil, false
	}
	if code, ok := rdoc.Lookup("code").AsInt64OK(); !ok || code != cursorNotFound {
		return nil, false
	}
	if c.r.cfg.TerminateAuth {
		return c.retryLostCursorPooled(msg)
	}
	if c.observedUser == "" {
		return nil, false
	}
	now := time.Now()
	for _, p := range c.r.parked.forUser(c.observedUser, c.upstreamMember(), now) {
		out, ok := p.exchange(msg, now)
		if !ok {
			continue
		}
		odoc, derr := out.ReplyDoc()
		if derr != nil {
			continue
		}
		if code, ok := odoc.Lookup("code").AsInt64OK(); ok && code == cursorNotFound {
			continue
		}
		c.r.Stats.CursorsFoundParked.Add(1)
		c.log.Info("continued a cursor on the member that made it, which stopped being primary",
			"member", p.member)
		return out, true
	}
	return nil, false
}

// retryLostCursorPooled is retryLostCursor with router.terminate_auth on. The
// upstream connections are this router's own, authenticated as one user, so
// any connection to the member that made a cursor can continue it, and a
// pool's is as good as the one that opened it. The client's own scope is
// judged before this, as for any getMore: the namespace by authorisation, and
// the session by mongod, which refuses a cursor opened in another.
//
// Measured on the box on 2026-11-26 before this: a cursor read on after one
// write across a step-down got CursorNotFound 3 times in 3.
func (c *conn) retryLostCursorPooled(msg *wire.Message) (*wire.Message, bool) {
	c.r.primaryMu.RLock()
	members := append([]string(nil), c.r.members...)
	c.r.primaryMu.RUnlock()
	here := c.upstreamMember()
	for _, m := range members {
		if m == here {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		up, err := c.r.poolFor(m).get(ctx)
		cancel()
		if err != nil {
			continue
		}
		_ = up.conn.SetDeadline(time.Now().Add(30 * time.Second))
		if err := wire.Write(up.conn, msg); err != nil {
			up.discard()
			continue
		}
		out, err := wire.Read(up.conn)
		if err != nil || out.Header.ResponseTo != msg.Header.RequestID {
			up.discard()
			continue
		}
		_ = up.conn.SetDeadline(time.Time{})
		up.release()
		odoc, derr := out.ReplyDoc()
		if derr != nil {
			continue
		}
		if code, ok := odoc.Lookup("code").AsInt64OK(); ok && code == cursorNotFound {
			continue
		}
		c.r.Stats.CursorsFoundParked.Add(1)
		c.log.Info("continued a cursor on the member that made it, which stopped being primary",
			"member", m)
		return out, true
	}
	return nil, false
}
