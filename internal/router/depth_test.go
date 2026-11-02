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

	"github.com/zelmario/thermocline/internal/wire"
)

// A find whose filter is nested millions of levels deep took the whole router
// down before authentication: the query shape advisor walked it recursively,
// the goroutine's stack passed Go's limit, and that is a fatal error recover
// cannot catch. The message must be refused before any guard reads it, and the
// connection must stay usable, because the frame was read whole.
//
// pump runs with no upstream at all. If the message got past the depth check
// it would reach handle and fail on the missing fast tier, or crash the test
// binary in a walker, rather than produce this refusal.
func TestDeepMessageIsRefusedBeforeAnyGuard(t *testing.T) {
	r := &Router{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	client, proxy := net.Pipe()
	defer client.Close()
	c := &conn{r: r, client: proxy, log: r.log}

	done := make(chan error, 1)
	go func() { done <- c.pump(context.Background()) }()

	for i := 0; i < 2; i++ {
		deep := deepFind(3_000_000)
		_ = client.SetDeadline(time.Now().Add(10 * time.Second))
		go func() { _, _ = client.Write(deep) }()
		reply, err := wire.Read(client)
		if err != nil {
			t.Fatalf("message %d: no reply to a deep find: %v", i+1, err)
		}
		doc, err := reply.ReplyDoc()
		if err != nil {
			t.Fatalf("message %d: unreadable reply: %v", i+1, err)
		}
		if msg, _ := doc.Lookup("errmsg").StringValueOK(); !strings.Contains(msg, "nested deeper") {
			t.Fatalf("message %d: reply %v is not the depth refusal", i+1, doc)
		}
	}
	if n := r.Stats.Refused.Load(); n != 2 {
		t.Errorf("refused counted %d, want 2", n)
	}

	_ = client.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pump did not return after the client closed")
	}
}

// deepFind frames {find: {"": {"": ...}}} as an OP_MSG, depth levels deep, built
// as bytes so the test itself does not recurse.
func deepFind(depth int) []byte {
	inner := 5 + 7*(depth-1)
	// The command document: its own header, one embedded document field
	// named "find", and a terminator.
	cmdLen := 4 + 1 + len("find") + 1 + inner + 1
	body := make([]byte, 0, 4+1+cmdLen)
	body = binary.LittleEndian.AppendUint32(body, 0) // flags
	body = append(body, 0)                           // kind 0
	body = binary.LittleEndian.AppendUint32(body, uint32(cmdLen))
	body = append(body, 0x03)
	body = append(body, "find"...)
	body = append(body, 0)
	for i := 0; i < depth-1; i++ {
		body = binary.LittleEndian.AppendUint32(body, uint32(inner-7*i))
		body = append(body, 0x03, 0)
	}
	body = binary.LittleEndian.AppendUint32(body, 5)
	body = append(body, 0)
	for i := 0; i < depth-1; i++ {
		body = append(body, 0)
	}
	body = append(body, 0) // end of the command document

	raw := make([]byte, wire.HeaderLen, wire.HeaderLen+len(body))
	binary.LittleEndian.PutUint32(raw[0:4], uint32(wire.HeaderLen+len(body)))
	binary.LittleEndian.PutUint32(raw[4:8], 7)
	binary.LittleEndian.PutUint32(raw[12:16], uint32(wire.OpMsg))
	return append(raw, body...)
}

// OP_INSERT has no command, so the gate never ran for it, and mongod before
// 5.1 applies it on whatever connection it arrives: under terminate_auth, the
// router's own. It is refused and the connection closed.
func TestLegacyOpcodeIsRefusedAndClosed(t *testing.T) {
	r := &Router{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	client, proxy := net.Pipe()
	defer client.Close()
	c := &conn{r: r, client: proxy, log: r.log}
	done := make(chan error, 1)
	go func() { done <- c.pump(context.Background()) }()

	body := binary.LittleEndian.AppendUint32(nil, 0)
	body = append(body, "payroll.salaries\x00"...)
	body = append(body, 5, 0, 0, 0, 0)
	raw := make([]byte, wire.HeaderLen)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(wire.HeaderLen+len(body)))
	binary.LittleEndian.PutUint32(raw[12:16], 2002)
	raw = append(raw, body...)

	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	go func() { _, _ = client.Write(raw) }()
	reply, err := wire.Read(client)
	if err != nil {
		t.Fatalf("no refusal for OP_INSERT: %v", err)
	}
	doc, _ := reply.ReplyDoc()
	if msg, _ := doc.Lookup("errmsg").StringValueOK(); !strings.Contains(msg, "legacy wire protocol") {
		t.Fatalf("reply %v is not the legacy refusal", doc)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("pump ended without saying why")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the connection stayed open after a legacy message")
	}
}
