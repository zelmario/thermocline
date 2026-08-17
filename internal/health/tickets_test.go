package health

import (
	"testing"
	"time"

	"github.com/zelmario/thermocline/internal/config"
)

// MongoDB 7.0 replaced the fixed pool of 128 write tickets with one sized to
// the host and resized under load. A container reports a total of 8.
//
// This is why the threshold is a fraction. An absolute floor large enough to
// mean anything on the old servers is unreachable on the new ones, so eviction
// pauses forever on a completely idle cluster. Testing against 6.0 hid it; the
// real 8.0 cluster reported "8 write tickets free, limit 20" and stopped.
func TestTicketThresholdIsRelativeToPoolSize(t *testing.T) {
	lim := limits() // MinWriteTicketsFree 0.25

	tests := []struct {
		name        string
		free, total int
		wantOK      bool
		why         string
	}{
		{"idle modern server, small pool", 8, 8, true,
			"every ticket free is healthy whatever the pool size"},
		{"idle old server, fixed pool", 128, 128, true,
			"the same reading on a pre-7.0 server"},
		{"modern server under pressure", 1, 8, false,
			"one of eight free is 12%, below the 25% floor"},
		{"old server under pressure", 16, 128, false,
			"same fraction, same verdict, whatever the absolute numbers"},
		{"exactly at the limit", 2, 8, true,
			"25% free is not below the 25% floor"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := healthy()
			r.WriteTickets, r.WriteTicketsTotal = tt.free, tt.total

			v := Evaluate(r, lim, 30*time.Second)
			if v.OK != tt.wantOK {
				t.Errorf("%d of %d free: OK = %v, want %v (%s). reason: %s",
					tt.free, tt.total, v.OK, tt.wantOK, tt.why, v.Reason)
			}
		})
	}
}

// A server that reports free tickets but no pool size gives no fraction to
// judge, so the check has to stand aside rather than invent a denominator.
func TestTicketCheckSkippedWithoutAPoolSize(t *testing.T) {
	r := healthy()
	r.WriteTickets, r.WriteTicketsTotal = 0, 0

	if v := Evaluate(r, limits(), 30*time.Second); !v.OK {
		t.Errorf("eviction stopped with no pool size to compare against: %s", v.Reason)
	}
}

// The message has to name both numbers. "2 free" alone tells an operator
// nothing when the pool might be 8 or 128.
func TestTicketReasonNamesBothNumbers(t *testing.T) {
	r := healthy()
	r.WriteTickets, r.WriteTicketsTotal = 1, 8

	v := Evaluate(r, limits(), 30*time.Second)
	if v.OK {
		t.Fatal("expected eviction to pause")
	}
	for _, want := range []string{"1", "8", "%"} {
		if !contains(v.Reason, want) {
			t.Errorf("reason %q does not mention %q", v.Reason, want)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

var _ = config.BackoffConfig{}
