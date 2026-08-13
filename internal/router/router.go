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

	"github.com/zelmar/thermocline/internal/config"
	"github.com/zelmar/thermocline/internal/watermark"
	"github.com/zelmar/thermocline/internal/wire"
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

	wmCache watermarkCache
	Stats   Stats
}

// Stats counts what the router did. Every one of these is a number an operator
// needs: copy-up rate says whether the boundary is cut too deep, and incomplete
// says whether queries are being answered from a tier that cannot answer them.
type Stats struct {
	Commands     atomic.Int64
	ReadMisses   atomic.Int64
	WriteMisses  atomic.Int64
	CopiedUp     atomic.Int64
	Incomplete   atomic.Int64
	Refused      atomic.Int64
	ClientsOpen  atomic.Int64
	ClientsTotal atomic.Int64
}

// New builds a Router. It does not listen or connect.
func New(hot, cold *mongo.Client, cfg config.RouterConfig, log *slog.Logger) *Router {
	return &Router{
		cfg:   cfg,
		hot:   hot,
		cold:  cold,
		marks: watermark.New(cold),
		log:   log,
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

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	r.log.Info("router listening",
		"addr", r.cfg.Listen,
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
	return nil
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

		if err := c.handle(ctx, msg); err != nil {
			return err
		}
	}
}

// handle forwards one message and decides whether the reply needs work.
func (c *conn) handle(ctx context.Context, msg *wire.Message) error {
	name, db, nameErr := msg.CommandName()

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

	if isHello(name) {
		return c.replyHello(msg, reply)
	}

	rewritten, err := c.intercept(ctx, msg, name, db, reply)
	if err != nil {
		c.log.Warn("routing failed, returning the fast tier's answer",
			"command", name, "err", err)
		return wire.Write(c.client, reply)
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

func (r *Router) dialPrimary(ctx context.Context) (net.Conn, error) {
	addr := r.currentPrimary()
	d := net.Dialer{Timeout: 5 * time.Second}

	conn, err := d.DialContext(ctx, "tcp", addr)
	if err == nil {
		return conn, nil
	}

	// The address may be stale after a failover. Re-resolve once and retry.
	if rerr := r.refreshPrimary(ctx); rerr != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return d.DialContext(ctx, "tcp", r.currentPrimary())
}

func isClosed(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}
