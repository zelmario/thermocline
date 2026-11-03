package router

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/wire"
)

// A header claiming a large message, then nothing: the connection held what
// had come for as long as the sender waited. It is closed once
// router.message_timeout passes, and a connection quiet between messages for
// longer than that is not.
func TestUnfinishedMessageIsCutAndIdleIsNot(t *testing.T) {
	r := &Router{log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg: config.RouterConfig{MessageTimeout: config.Duration(300 * time.Millisecond)}}

	// A whole message, idle past the timeout, and another: both answered. The
	// depth refusal leaves the connection open and needs no upstream.
	client, proxy := net.Pipe()
	defer client.Close()
	c := &conn{r: r, client: proxy, log: r.log}
	done := make(chan error, 1)
	go func() { done <- c.pump(context.Background()) }()
	for i := 0; i < 2; i++ {
		if i == 1 {
			time.Sleep(700 * time.Millisecond)
			select {
			case err := <-done:
				t.Fatalf("a connection idle between messages was closed: %v", err)
			default:
			}
		}
		_ = client.SetDeadline(time.Now().Add(5 * time.Second))
		go func() { _, _ = client.Write(deepFind(2000)) }()
		reply, err := wire.Read(client)
		if err != nil {
			t.Fatalf("message %d: no reply: %v", i+1, err)
		}
		if doc, _ := reply.ReplyDoc(); !strings.Contains(doc.String(), "nested deeper") {
			t.Fatalf("message %d: reply %v is not the depth refusal", i+1, doc)
		}
	}

	// A header and part of a body, then silence: cut.
	client2, proxy2 := net.Pipe()
	defer client2.Close()
	c2 := &conn{r: r, client: proxy2, log: r.log}
	done2 := make(chan error, 1)
	go func() { done2 <- c2.pump(context.Background()) }()
	head := make([]byte, wire.HeaderLen+100)
	binary.LittleEndian.PutUint32(head[0:4], 32<<20)
	binary.LittleEndian.PutUint32(head[12:16], uint32(wire.OpMsg))
	go func() { _, _ = client2.Write(head) }()
	select {
	case err := <-done2:
		if err == nil {
			t.Fatal("pump ended without saying why")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a message left unfinished held the connection past message_timeout")
	}
}

// One connection past router.max_connections is closed as it is accepted and
// counted; one under it is left alone.
func TestConnectionPastTheCapIsClosed(t *testing.T) {
	r := &Router{log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg: config.RouterConfig{MaxConnections: 2}}
	var warned time.Time

	under, peer := net.Pipe()
	defer peer.Close()
	if r.overCap(under, 1, &warned) {
		t.Fatal("a connection under the cap was refused")
	}
	_ = under.Close()

	over, peer2 := net.Pipe()
	if !r.overCap(over, 2, &warned) {
		t.Fatal("a connection at the cap was let in")
	}
	_ = peer2.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer2.Read(make([]byte, 1)); err == nil {
		t.Fatal("the refused connection is still open")
	}
	if n := r.Stats.ConnectionsRefused.Load(); n != 1 {
		t.Errorf("connections_refused counted %d, want 1", n)
	}
	r.cfg.MaxConnections = 0
	if r.overCap(over, 1_000_000, &warned) {
		t.Error("max_connections 0 still refused")
	}
}
