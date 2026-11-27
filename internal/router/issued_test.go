package router

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/wire"
)

// A getMore on a tiered collection is forwarded only for a cursor this process
// handed out, because the guard that keeps the rest of it whole lives in this
// process. One opened before a restart came back 3 of 40 with no error.
func TestAGetMoreOnACursorThisProcessDidNotHandOutIsRefused(t *testing.T) {
	r := &Router{cfg: config.RouterConfig{Managed: []string{"shop.orders"}},
		log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	getMore := func(id int64, coll string) *wire.Message {
		return msgOf(t, bson.D{{Key: "getMore", Value: id}, {Key: "collection", Value: coll},
			{Key: "$db", Value: "shop"}})
	}
	newConn := func() (*conn, func()) {
		mine, theirs := net.Pipe()
		go func() {
			for {
				m, err := wire.Read(theirs)
				if err != nil {
					return
				}
				out, _ := wire.NewMsg(0, m.Header.RequestID, bson.D{{Key: "ok", Value: 1.0}})
				_ = wire.Write(theirs, out)
			}
		}()
		return &conn{r: r, upstream: mine, log: r.log}, func() { mine.Close(); theirs.Close() }
	}
	reply := func(id int64, field string) *wire.Message {
		m, _ := wire.NewMsg(0, 0, bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: id},
			{Key: "ns", Value: "shop.orders"}, {Key: field, Value: bson.A{}}}}, {Key: "ok", Value: 1.0}})
		return m
	}

	c, done := newConn()
	defer done()
	ctx := context.Background()

	out, refused := c.refuseForeignGetMore(ctx, getMore(41, "orders"), "shop")
	if !refused {
		t.Fatal("a getMore on a cursor nobody here handed out was forwarded")
	}
	doc, _ := out.ReplyDoc()
	if code, _ := intOf(doc.Lookup("code")); code != 43 {
		t.Fatalf("code %d, want 43 CursorNotFound", code)
	}

	// Handed out by this process, through the one funnel every reply takes.
	c.noteIssued(reply(42, "firstBatch"))
	if _, refused := c.refuseForeignGetMore(ctx, getMore(42, "orders"), "shop"); refused {
		t.Fatal("a getMore on a cursor this process handed out was refused")
	}
	// From any connection of the same process.
	c2, done2 := newConn()
	defer done2()
	if _, refused := c2.refuseForeignGetMore(ctx, getMore(42, "orders"), "shop"); refused {
		t.Fatal("a getMore on another connection of the same process was refused")
	}

	// Drained: forgotten, but only by the reply to a getMore naming it.
	c.askedCursor = 42
	c.noteIssued(reply(0, "firstBatch"))
	if !r.issued.known(42) {
		t.Fatal("a find's whole answer made this process forget a cursor still open")
	}
	c.noteIssued(reply(0, "nextBatch"))
	if r.issued.known(42) {
		t.Fatal("a drained cursor is still remembered")
	}

	// Commands' own cursors and wider change streams name no collection here.
	if _, refused := c.refuseForeignGetMore(ctx, getMore(43, "$cmd.aggregate"), "shop"); refused {
		t.Fatal("a database change stream's getMore was refused here rather than judged by the sieve")
	}
}
