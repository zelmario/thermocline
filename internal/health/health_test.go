package health

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
)

func limits() config.BackoffConfig {
	return config.BackoffConfig{
		MaxReplicationLag:   config.Duration(10 * time.Second),
		MaxCacheDirty:       0.05,
		MinWriteTicketsFree: 0.25,
		MinOplogWindow:      config.Duration(24 * time.Hour),
	}
}

// healthy is a reading with every signal comfortably inside its limit.
func healthy() Reading {
	return Reading{
		FeederLag:         time.Second,
		ReplicationLag:    time.Second,
		CacheDirty:        0.01,
		WriteTickets:      120,
		WriteTicketsTotal: 128,
		OplogWindow:       48 * time.Hour,
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
	r := Reading{FeederLag: time.Hour, ReplicationLag: time.Hour, CacheDirty: 0.9,
		WriteTickets: 0, WriteTicketsTotal: 128}
	v := Evaluate(r, limits(), 30*time.Second)
	if v.OK {
		t.Fatal("eviction allowed with every signal out of range")
	}
	if !strings.Contains(v.Reason, "mirroring") {
		t.Errorf("reason %q should lead with mirroring, the only correctness guard", v.Reason)
	}
}

// Eviction is gated on feeder lag, and feeder lag was one machine's clock minus
// another's. The evictor and the feeder are not required to be on the same
// host.
//
// Which way it errs depends on which way the clocks differ, and both are wrong:
// an evictor running ahead of its feeder sees lag that is not there and stops
// evicting for ever, and one running behind sees less lag than there is. The
// first is what an operator notices, the second is the one that matters,
// because this gate is the coarse net over a campaign of deletes.
func TestFeederLagAcrossTwoClocks(t *testing.T) {
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

	// The archive is the reference: this host is 30s ahead of it, the feeder
	// 5s behind it, and mirroring is genuinely one second behind.
	const mine = 30 * time.Second
	const theirs = -5 * time.Second
	at := now.Add(-mine).Add(-time.Second).Add(theirs)

	if got := LagFrom(now, at, mine, theirs, true); got != time.Second {
		t.Errorf("lag = %v, want 1s", got)
	}
	if got := LagFrom(now, at, mine, theirs, false); got != 36*time.Second {
		t.Errorf("uncorrected lag = %v, want 36s, which is what this exists to stop reporting", got)
	}
	// The other direction, which the clamp used to swallow whole: this host
	// half a minute behind its feeder, mirroring genuinely 10s behind.
	behind := now.Add(30 * time.Second).Add(-10 * time.Second)
	if got := LagFrom(now, behind, -30*time.Second, 0, true); got != 10*time.Second {
		t.Errorf("lag = %v, want 10s", got)
	}
	if got := LagFrom(now, behind, -30*time.Second, 0, false); got != 0 {
		t.Errorf("uncorrected lag = %v, want the zero the clamp used to report", got)
	}
}

// A feeder replaying a backlog publishes all the time, so the wall-clock
// reading, which is the moment it published, said about 0s while it was hours
// behind, and the evictor went on removing documents whose notices would
// expire before the feeder reached their deletes. The published cluster time
// against the fast tier's own last write says how far behind it really is.
func TestFeederLagFromTheFastTiersClock(t *testing.T) {
	uri := os.Getenv("TC_TEST_HOT")
	if uri == "" {
		t.Skip("set TC_TEST_HOT to a replica set")
	}
	ctx := context.Background()
	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Disconnect(ctx) }()
	if _, err := cl.Database("tc_health").Collection("w").InsertOne(ctx, bson.D{{Key: "n", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Database("tc_health").Drop(context.Background()) }()
	c := New(cl, nil, "test")

	threeHoursAgo := bson.Timestamp{T: uint32(time.Now().Add(-3 * time.Hour).Unix())}
	behind, ok := c.behindFastTier(ctx, threeHoursAgo)
	if !ok || behind < 2*time.Hour {
		t.Errorf("a position three hours old read as %s behind (known %v), want about three hours", behind, ok)
	}
	future := bson.Timestamp{T: uint32(time.Now().Add(time.Minute).Unix())}
	if behind, ok := c.behindFastTier(ctx, future); !ok || behind != 0 {
		t.Errorf("a position past the newest write read as %s behind (known %v), want 0", behind, ok)
	}
	if _, ok := c.behindFastTier(ctx, bson.Timestamp{}); ok {
		t.Error("a record with no cluster time was judged, want it left to the wall-clock reading")
	}
}

// The checkpoint is the fallback when no position is published (position_every
// 0) or the one there is left from an earlier run. Its EventAt is moved to now
// each time a batch is used up, exactly as the position's At was, so it read
// about 0s behind during a backlog until it was judged by its cluster time too.
func TestFeederLagFromTheCheckpointsClusterTime(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two replica sets")
	}
	ctx := context.Background()
	hot, err := mongo.Connect(options.Client().ApplyURI(hotURI))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hot.Disconnect(ctx) }()
	cold, err := mongo.Connect(options.Client().ApplyURI(coldURI))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cold.Disconnect(ctx) }()
	if _, err := hot.Database("tc_health").Collection("w").InsertOne(ctx, bson.D{{Key: "n", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hot.Database("tc_health").Drop(context.Background()) }()

	id := fmt.Sprintf("lagtest-%d", time.Now().UnixNano())
	cps := cold.Database(meta.DB).Collection(meta.Checkpoints)
	defer func() { _, _ = cps.DeleteOne(context.Background(), bson.D{{Key: "_id", Value: id}}) }()
	threeHoursAgo := bson.Timestamp{T: uint32(time.Now().Add(-3 * time.Hour).Unix())}
	now := time.Now()
	if _, err := cps.InsertOne(ctx, bson.D{
		{Key: "_id", Value: id}, {Key: "token", Value: bson.D{{Key: "_data", Value: "x"}}},
		{Key: "savedAt", Value: now}, {Key: "eventAt", Value: now}, {Key: "ts", Value: threeHoursAgo},
	}); err != nil {
		t.Fatal(err)
	}
	c := New(hot, cold, id)
	var r Reading
	c.sampleFeederLag(ctx, &r)
	if r.isMissing("feeder lag") || r.FeederLag < 2*time.Hour {
		t.Errorf("a checkpoint saved now at a position three hours old read as %s behind (missing %v), want about three hours",
			r.FeederLag, r.isMissing("feeder lag"))
	}

	// A post-batch token names no event, so the record has no ts; its token
	// still carries the time, and it is the same three hours.
	tok := fmt.Sprintf("82%08X%08X2B0229296E04", threeHoursAgo.T, threeHoursAgo.I)
	if _, err := cps.ReplaceOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{
		{Key: "_id", Value: id}, {Key: "token", Value: bson.D{{Key: "_data", Value: tok}}},
		{Key: "savedAt", Value: now}, {Key: "eventAt", Value: now},
	}); err != nil {
		t.Fatal(err)
	}
	var r2 Reading
	c.sampleFeederLag(ctx, &r2)
	if r2.isMissing("feeder lag") || r2.FeederLag < 2*time.Hour {
		t.Errorf("a checkpoint with no ts and a token three hours old read as %s behind (missing %v), want about three hours",
			r2.FeederLag, r2.isMissing("feeder lag"))
	}
}
