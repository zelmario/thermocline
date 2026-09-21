package router

import (
	"log/slog"
	"net"
	"testing"
	"time"
)

// A stop closes the connections that are parked on a read and waits out the
// grace for the ones with a command in flight.
//
// The counting is the part worth pinning. untrack only runs when the serve
// goroutine unwinds, which lags the close, so a naive loop closes the same
// connection every pass and reports hundreds of idle clients where there was
// one. The first version of this did exactly that.
func TestDrainClientsClosesIdleAndWaitsOutBusy(t *testing.T) {
	r := &Router{log: slog.Default(), conns: map[*conn]struct{}{}}

	mk := func(busy bool) (*conn, net.Conn) {
		mine, theirs := net.Pipe()
		c := &conn{client: mine}
		c.busy.Store(busy)
		r.conns[c] = struct{}{}
		return c, theirs
	}
	_, peerIdle := mk(false)
	_, peerBusy := mk(true)
	defer func() { _ = peerIdle.Close(); _ = peerBusy.Close() }()

	start := time.Now()
	idle, cut := r.drainClients(100 * time.Millisecond)
	took := time.Since(start)

	if idle != 1 {
		t.Fatalf("closed %d idle connections, want 1", idle)
	}
	if cut != 1 {
		t.Fatalf("cut %d busy connections, want 1", cut)
	}
	if took < 100*time.Millisecond {
		t.Fatalf("returned after %v, which is less than the grace a busy connection is owed", took)
	}
}

// With nothing in flight it must not wait out the grace at all, because that
// delay is paid by every ordinary restart.
func TestDrainClientsReturnsAtOnceWhenNothingIsBusy(t *testing.T) {
	r := &Router{log: slog.Default(), conns: map[*conn]struct{}{}}
	var peers []net.Conn
	for i := 0; i < 3; i++ {
		mine, theirs := net.Pipe()
		c := &conn{client: mine}
		r.conns[c] = struct{}{}
		peers = append(peers, theirs)
	}
	defer func() {
		for _, p := range peers {
			_ = p.Close()
		}
	}()

	start := time.Now()
	idle, cut := r.drainClients(5 * time.Second)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("took %v with nothing busy, which every restart would pay", took)
	}
	if idle != 3 || cut != 0 {
		t.Fatalf("closed idle=%d cut=%d, want 3 and 0", idle, cut)
	}
}
