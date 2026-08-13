package watermark

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmar/thermocline/internal/meta"
)

// The watermark rules cannot be checked without a database, because the
// only-move-later guarantee lives in the update filter so that two daemons
// racing cannot both win. Run `make dev` and set TC_TEST_COLD to exercise it.
//
//	TC_TEST_COLD='mongodb://127.0.0.1:28018/?replicaSet=rsCold' go test ./internal/watermark
func testStore(t *testing.T) (*Store, func()) {
	t.Helper()
	uri := os.Getenv("TC_TEST_COLD")
	if uri == "" {
		t.Skip("set TC_TEST_COLD to a cold tier URI to run watermark tests")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	s := New(client)
	ns := "tctest.watermarks"
	_, _ = s.coll.DeleteOne(context.Background(), bson.D{{Key: "_id", Value: ns}})
	return s, func() {
		_, _ = s.coll.DeleteOne(context.Background(), bson.D{{Key: "_id", Value: ns}})
		_ = client.Disconnect(context.Background())
	}
}

const testNS = "tctest.watermarks"

// This is the invariant a live run caught the hard way. The warmer had filled a
// range and recorded completeness back to it; the evictor then removed
// everything inside that range and left the claim standing. A router trusting
// it would have answered range queries from an empty collection and called the
// result complete.
func TestRaiseToNeverOverstatesCompleteness(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	warmedBackTo := time.Date(2025, 10, 17, 0, 0, 0, 0, time.UTC)
	evictionBoundary := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

	if err := s.Set(ctx, testNS, warmedBackTo); err != nil {
		t.Fatalf("Set: %v", err)
	}

	raised, err := s.RaiseTo(ctx, testNS, evictionBoundary)
	if err != nil {
		t.Fatalf("RaiseTo: %v", err)
	}
	if !raised {
		t.Error("RaiseTo reported no change while shrinking the claimed range")
	}

	got, err := s.Load(ctx, testNS)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !got.Equal(evictionBoundary) {
		t.Fatalf("watermark = %v, want %v: eviction must shrink what the fast tier claims to hold",
			got, evictionBoundary)
	}
}

// Raising is one-way. A second evictor with an older boundary, or a stale
// sweep finishing late, must not widen the claim back out.
func TestRaiseToIsOneWay(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	later := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	earlier := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	if err := s.Set(ctx, testNS, later); err != nil {
		t.Fatalf("Set: %v", err)
	}
	raised, err := s.RaiseTo(ctx, testNS, earlier)
	if err != nil {
		t.Fatalf("RaiseTo: %v", err)
	}
	if raised {
		t.Error("RaiseTo reported a change while being handed an earlier boundary")
	}

	got, _ := s.Load(ctx, testNS)
	if !got.Equal(later) {
		t.Errorf("watermark moved earlier to %v, want it held at %v", got, later)
	}
}

// An evictor may well run before any warmer has recorded anything.
func TestRaiseToCreatesWhenMissing(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	boundary := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	raised, err := s.RaiseTo(ctx, testNS, boundary)
	if err != nil {
		t.Fatalf("RaiseTo: %v", err)
	}
	if !raised {
		t.Error("RaiseTo reported no change while creating the first watermark")
	}
	got, _ := s.Load(ctx, testNS)
	if !got.Equal(boundary) {
		t.Errorf("watermark = %v, want %v", got, boundary)
	}
}

// A namespace nobody has warmed claims nothing, rather than claiming everything.
func TestLoadMissingClaimsNothing(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()

	got, err := s.Load(context.Background(), "tctest.neverseen")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !got.IsZero() {
		t.Errorf("Load of an unknown namespace = %v, want the zero time", got)
	}
}

var _ = meta.Watermarks
