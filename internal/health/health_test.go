package health

import (
	"strings"
	"testing"
	"time"

	"github.com/zelmar/thermocline/internal/config"
)

func limits() config.BackoffConfig {
	return config.BackoffConfig{
		MaxReplicationLag: config.Duration(10 * time.Second),
		MaxCacheDirty:     0.05,
		MinWriteTickets:   20,
		MinOplogWindow:    config.Duration(24 * time.Hour),
	}
}

// healthy is a reading with every signal comfortably inside its limit.
func healthy() Reading {
	return Reading{
		FeederLag:      time.Second,
		ReplicationLag: time.Second,
		CacheDirty:     0.01,
		WriteTickets:   120,
		OplogWindow:    48 * time.Hour,
	}
}

func TestEvaluateAllows(t *testing.T) {
	if v := Evaluate(healthy(), limits(), 30*time.Second); !v.OK {
		t.Errorf("healthy reading refused: %s", v.Reason)
	}
}

func TestEvaluateStops(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Reading)
		wantHas string
	}{
		{"mirroring behind", func(r *Reading) { r.FeederLag = time.Minute }, "mirroring"},
		{"replication behind", func(r *Reading) { r.ReplicationLag = 30 * time.Second }, "replication"},
		{"cache dirty", func(r *Reading) { r.CacheDirty = 0.4 }, "dirty"},
		{"write tickets exhausted", func(r *Reading) { r.WriteTickets = 2 }, "tickets"},
		{"oplog window short", func(r *Reading) { r.OplogWindow = time.Hour }, "oplog"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := healthy()
			tt.mutate(&r)
			v := Evaluate(r, limits(), 30*time.Second)
			if v.OK {
				t.Fatalf("eviction allowed despite %s", tt.name)
			}
			if !strings.Contains(v.Reason, tt.wantHas) {
				t.Errorf("reason %q does not mention %q, so an operator cannot tell what paused it",
					v.Reason, tt.wantHas)
			}
		})
	}
}

// Mirroring lag is a correctness guard, so not knowing it must stop eviction.
// Removing a document the archive has not received destroys it.
func TestUnknownMirroringLagStopsEviction(t *testing.T) {
	r := healthy()
	r.missing("feeder lag")
	v := Evaluate(r, limits(), 30*time.Second)
	if v.OK {
		t.Fatal("eviction allowed while mirroring lag was unknown")
	}
	if !strings.Contains(v.Reason, "unknown") {
		t.Errorf("reason %q should say the lag is unknown", v.Reason)
	}
}

// The performance guards fail open. A metric that moved in a new MongoDB
// release should not stop eviction forever; that would be its own outage.
func TestUnknownPerformanceSignalsAllowEviction(t *testing.T) {
	for _, name := range []string{"replication lag", "cache dirty", "write tickets", "oplog window"} {
		t.Run(name, func(t *testing.T) {
			r := healthy()
			// Set the value into the danger zone, then mark it unreadable.
			switch name {
			case "replication lag":
				r.ReplicationLag = time.Hour
			case "cache dirty":
				r.CacheDirty = 0.9
			case "write tickets":
				r.WriteTickets = 0
			case "oplog window":
				r.OplogWindow = 0
			}
			r.missing(name)

			if v := Evaluate(r, limits(), 30*time.Second); !v.OK {
				t.Errorf("unreadable %q stopped eviction: %s", name, v.Reason)
			}
		})
	}
}

func TestEvaluateChecksMirroringFirst(t *testing.T) {
	// Everything is wrong at once. The reported reason should be the one that
	// risks data, not the one that risks latency.
	r := Reading{FeederLag: time.Hour, ReplicationLag: time.Hour, CacheDirty: 0.9, WriteTickets: 0}
	v := Evaluate(r, limits(), 30*time.Second)
	if v.OK {
		t.Fatal("eviction allowed with every signal out of range")
	}
	if !strings.Contains(v.Reason, "mirroring") {
		t.Errorf("reason %q should lead with mirroring, the only correctness guard", v.Reason)
	}
}
