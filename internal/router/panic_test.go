package router

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"
)

// A panic while serving one client must close that client and leave the router
// running. Go kills the whole program for an unrecovered panic in any
// goroutine, so without the recovery in serve a single malformed message would
// disconnect every other application using this router.
//
// The panic is provoked rather than waited for: serve is called directly with a
// connection whose first read panics, which is the same path a defect in
// message handling would take.
func TestServeRecoversAPanicAndKeepsRunning(t *testing.T) {
	var logged strings.Builder
	r := &Router{log: slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelError}))}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// If serve does not recover, this goroutine's panic ends the test
		// binary and the failure is unmissable.
		r.serve(context.Background(), panicOnRead{})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return")
	}

	if n := r.Stats.Panics.Load(); n != 1 {
		t.Errorf("panics counted %d, want 1", n)
	}
	out := logged.String()
	if !strings.Contains(out, "recovered a panic") {
		t.Errorf("the panic was not reported: %s", out)
	}
	if !strings.Contains(out, "stack") {
		t.Error("the report carries no stack, which is what makes it findable")
	}

	// And the router is still usable: another connection is served, and
	// counted, rather than the process being gone.
	go func() { r.serve(context.Background(), panicOnRead{}) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.Stats.Panics.Load() == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("the router did not serve a second client after the first panicked")
}

// panicOnRead is a connection that panics the moment it is read, standing in
// for any defect reached while handling a client's bytes.
type panicOnRead struct{ net.Conn }

func (panicOnRead) Read([]byte) (int, error)  { panic("a defect while reading a client's bytes") }
func (panicOnRead) Write([]byte) (int, error) { return 0, nil }
func (panicOnRead) Close() error              { return nil }
func (panicOnRead) RemoteAddr() net.Addr      { return nil }
func (panicOnRead) LocalAddr() net.Addr       { return nil }
