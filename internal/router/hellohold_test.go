package router

import (
	"testing"
	"time"
)

// TestHoldHelloAfterFailover: no wait before any failover, a wait to the end
// of failoverSettle just after one, and none once it has passed.
func TestHoldHelloAfterFailover(t *testing.T) {
	r := &Router{}
	start := time.Now()
	r.holdHelloAfterFailover()
	if d := time.Since(start); d > 20*time.Millisecond {
		t.Fatalf("held %v with no failover", d)
	}

	r.failoverEndedAt.Store(time.Now().UnixNano())
	start = time.Now()
	r.holdHelloAfterFailover()
	if d := time.Since(start); d < failoverSettle-20*time.Millisecond {
		t.Fatalf("held %v just after a failover, want about %v", d, failoverSettle)
	}

	r.failoverEndedAt.Store(time.Now().Add(-2 * failoverSettle).UnixNano())
	start = time.Now()
	r.holdHelloAfterFailover()
	if d := time.Since(start); d > 20*time.Millisecond {
		t.Fatalf("held %v after the window had passed", d)
	}
}
