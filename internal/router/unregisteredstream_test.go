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

// A change stream's cursor lives on mongod and outlives the router process
// that opened it. A restarted router has no record of it, so a getMore on it
// came back unfiltered, copy-ups and evictions included. It has to be told to
// resume instead, so the driver opens a stream this router does know, and the
// old cursor is closed.
func TestAGetMoreOnAStreamThisRouterDidNotOpenIsToldToResume(t *testing.T) {
	batch := func(pbrt bool) bson.D {
		cur := bson.D{{Key: "id", Value: int64(77)}, {Key: "ns", Value: "shop.orders"},
			{Key: "nextBatch", Value: bson.A{bson.D{{Key: "operationType", Value: "insert"}}}}}
		if pbrt {
			cur = append(cur, bson.E{Key: "postBatchResumeToken", Value: bson.D{{Key: "_data", Value: "82"}}})
		}
		return bson.D{{Key: "cursor", Value: cur}, {Key: "ok", Value: 1.0}}
	}
	getMore := func(coll string) *wire.Message {
		return msgOf(t, bson.D{{Key: "getMore", Value: int64(77)}, {Key: "collection", Value: coll},
			{Key: "$db", Value: "shop"}})
	}
	for _, tc := range []struct {
		name   string
		coll   string
		reply  bson.D
		resume bool
	}{
		{"a stream on a tiered collection", "orders", batch(true), true},
		{"a stream on the whole database", "$cmd.aggregate", batch(true), true},
		{"a plain cursor on a tiered collection", "orders", batch(false), false},
		{"a stream on a collection nothing tiers", "invoices", batch(true), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mine, theirs := net.Pipe()
			defer mine.Close()
			defer theirs.Close()
			killed := make(chan string, 1)
			go func() {
				m, err := wire.Read(theirs)
				if err != nil {
					return
				}
				cmd, _ := m.CommandDoc()
				killed <- cmd.Index(0).Key()
				out, _ := wire.NewMsg(0, m.Header.RequestID, bson.D{{Key: "ok", Value: 1.0}})
				_ = wire.Write(theirs, out)
			}()
			r := &Router{cfg: config.RouterConfig{Managed: []string{"shop.orders"}},
				log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			c := &conn{r: r, upstream: mine, log: r.log}
			reply, _ := wire.NewMsg(0, 0, tc.reply)
			out, rewritten := c.filterChangeStream(context.Background(), getMore(tc.coll), reply)
			if rewritten != tc.resume {
				t.Fatalf("rewritten = %v, want %v", rewritten, tc.resume)
			}
			if !tc.resume {
				return
			}
			doc, _ := out.ReplyDoc()
			if replyOK(doc) {
				t.Fatal("the batch was delivered")
			}
			labels, _ := doc.Lookup("errorLabels").Array().Values()
			if len(labels) != 1 || labels[0].StringValue() != "ResumableChangeStreamError" {
				t.Fatalf("labels %v: a driver would end the stream rather than resume it", labels)
			}
			if got := <-killed; got != "killCursors" {
				t.Fatalf("sent %q to the fast tier, want killCursors", got)
			}
		})
	}
}
