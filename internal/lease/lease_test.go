package lease

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// The whole of this is decided inside one update on the server, so there is
// nothing here a test without a database could reach. Run `make dev` and set
// TC_TEST_COLD:
//
//	TC_TEST_COLD='mongodb://127.0.0.1:28018/?replicaSet=rsCold' go test ./internal/lease
func testCold(t *testing.T) (*mongo.Client, func()) {
	t.Helper()
	uri := os.Getenv("TC_TEST_COLD")
	if uri == "" {
		t.Skip("set TC_TEST_COLD to a cold tier URI to run the lease tests")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ctx := context.Background()
	clear := func() {
		// This is a real deployment's bookkeeping database, shared with
		// whatever else runs against the cluster, so the rows go by name.
		_, _ = client.Database(meta.DB).Collection(meta.Markers).DeleteMany(ctx,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^lease:tctest"}}}})
	}
	clear()
	return client, func() { clear(); _ = client.Disconnect(ctx) }
}

func TestOnlyOneHolderAtATime(t *testing.T) {
	cold, done := testCold(t)
	defer done()
	ctx := context.Background()

	first := New(cold, "tctest", "a")
	second := New(cold, "tctest", "a")

	if err := first.Take(ctx); err != nil {
		t.Fatalf("the first take failed: %v", err)
	}

	// The case this exists for: a second daemon of the same kind and id,
	// started while the first is running.
	err := second.Take(ctx)
	var held *ErrHeld
	if !errors.As(err, &held) {
		t.Fatalf("the second take returned %v, want an ErrHeld", err)
	}
	if held.PID != os.Getpid() {
		t.Errorf("the error names pid %d, want this process's %d", held.PID, os.Getpid())
	}
	// The message is the whole of what an operator gets, so it has to say what
	// to do rather than only what happened.
	if msg := err.Error(); msg == "" || !contains(msg, "stop that one") {
		t.Errorf("the message does not say what to do: %q", msg)
	}

	// Taking it again is not a conflict with itself. A daemon that reconnects
	// and retakes its own lease must not lock itself out.
	if err := first.Take(ctx); err != nil {
		t.Errorf("the holder could not retake its own lease: %v", err)
	}

	// And released, the next one gets it at once rather than waiting Stale out.
	if err := first.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := second.Take(ctx); err != nil {
		t.Errorf("the lease was released and the next take still failed: %v", err)
	}
	_ = second.Release(ctx)
}

func TestAHolderThatStoppedIsReplacedAfterTheWindow(t *testing.T) {
	cold, done := testCold(t)
	defer done()
	ctx := context.Background()

	dead := New(cold, "tctest", "b")
	if err := dead.Take(ctx); err != nil {
		t.Fatalf("take: %v", err)
	}
	// A process that was killed never releases, so the row stays as it was. Age
	// it by hand rather than sleeping thirty seconds: what is under test is the
	// comparison, and the instant it compares against is the archive's own.
	coll := cold.Database(meta.DB).Collection(meta.Markers)
	if _, err := coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: dead.docID()}},
		mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{
			{Key: "seen", Value: bson.D{{Key: "$subtract", Value: bson.A{"$$NOW", (Stale + time.Second).Milliseconds()}}}},
		}}}}); err != nil {
		t.Fatalf("age the lease: %v", err)
	}

	next := New(cold, "tctest", "b")
	if err := next.Take(ctx); err != nil {
		t.Fatalf("a lease older than the window was not taken over: %v", err)
	}
	// And the one that lost it finds out at its next renewal, which is what
	// stops two of them running.
	err := dead.Keep(ctx)
	if !errors.Is(err, ErrLost) {
		t.Errorf("the displaced holder's renewal returned %v, want ErrLost", err)
	}
	_ = next.Release(ctx)
}

func TestRenewingKeepsItAndReleasingIsOwnOnly(t *testing.T) {
	cold, done := testCold(t)
	defer done()
	ctx := context.Background()

	holder := New(cold, "tctest", "c")
	if err := holder.Take(ctx); err != nil {
		t.Fatalf("take: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := holder.Keep(ctx); err != nil {
			t.Fatalf("renewal %d: %v", i, err)
		}
	}

	// A release only ever removes this process's own claim. Otherwise a daemon
	// shutting down after it had already lost the lease would take the new
	// holder's row with it, which is the failure this whole thing is for,
	// arranged by the tidying up.
	other := New(cold, "tctest", "c")
	if err := other.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := holder.Keep(ctx); err != nil {
		t.Errorf("somebody else's release took this holder's lease: %v", err)
	}
	_ = holder.Release(ctx)
}

// A row deleted by hand is not another holder, so the daemon takes it back
// rather than stopping. An operator clearing bookkeeping should not stop
// mirroring an hour later.
func TestADeletedRowIsRetaken(t *testing.T) {
	cold, done := testCold(t)
	defer done()
	ctx := context.Background()

	holder := New(cold, "tctest", "d")
	if err := holder.Take(ctx); err != nil {
		t.Fatalf("take: %v", err)
	}
	if _, err := cold.Database(meta.DB).Collection(meta.Markers).
		DeleteOne(ctx, bson.D{{Key: "_id", Value: holder.docID()}}); err != nil {
		t.Fatalf("delete the row: %v", err)
	}
	if err := holder.Keep(ctx); err != nil {
		t.Errorf("a deleted row stopped the holder: %v", err)
	}
	_ = holder.Release(ctx)
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
