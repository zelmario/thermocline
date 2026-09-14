package lease

import (
	"context"
	"errors"
	"os"
	"sync"
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

// A lease left behind by a process that was killed, on this host, must not cost
// the next one the whole window.
//
// That is the ordinary way one is left behind: kill -9, an OOM kill, a crash.
// None of them release, and all of them are followed by somebody starting the
// daemon again straight away. The test suites do exactly this, ending with
// pkill -9 and starting the next feeder a second later, and two of them failed
// on a lease that had been renewed a moment before the kill.
//
// Only on this host. Another machine's process cannot be looked up from here,
// and the staleness window is what covers that.
func TestADeadHolderOnThisHostIsNotAHolder(t *testing.T) {
	cold, done := testCold(t)
	defer done()
	ctx := context.Background()
	coll := cold.Database(meta.DB).Collection(meta.Markers)

	killed := New(cold, "tctest", "e")
	if err := killed.Take(ctx); err != nil {
		t.Fatalf("take: %v", err)
	}
	// It was killed: the row says this host and a pid nothing is running under.
	// 0x7FFFFFFF is above every pid_max this will meet.
	if _, err := coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: killed.docID()}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "pid", Value: 0x7FFFFFFF}}}}); err != nil {
		t.Fatalf("set a dead pid: %v", err)
	}

	next := New(cold, "tctest", "e")
	if err := next.Take(ctx); err != nil {
		t.Fatalf("a lease held by a process that has gone was not taken: %v", err)
	}

	// And the holder that is still running is still respected, which is the
	// half that makes this safe: the pid in the row is this test's own.
	live := New(cold, "tctest", "f")
	if err := live.Take(ctx); err != nil {
		t.Fatalf("take: %v", err)
	}
	var held *ErrHeld
	if err := New(cold, "tctest", "f").Take(ctx); !errors.As(err, &held) {
		t.Errorf("a live holder was displaced: %v", err)
	}

	// A holder on another host is left alone whatever its pid says, because
	// nothing here can look it up.
	elsewhere := New(cold, "tctest", "g")
	if err := elsewhere.Take(ctx); err != nil {
		t.Fatalf("take: %v", err)
	}
	if _, err := coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: elsewhere.docID()}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "host", Value: "some-other-host"},
			{Key: "pid", Value: 0x7FFFFFFF},
		}}}); err != nil {
		t.Fatalf("move the lease to another host: %v", err)
	}
	if err := New(cold, "tctest", "g").Take(ctx); !errors.As(err, &held) {
		t.Errorf("a lease on another host was taken on a guess about its pid: %v", err)
	}

	_ = next.Release(ctx)
	_ = live.Release(ctx)
}

// Two processes starting at the same instant against a lease that does not
// exist yet. Both upsert, one insert wins, and the loser is told the key is
// taken rather than being told who holds it.
//
// It is a real start-up shape: a unit file and an operator, or four suites in a
// parallel sweep. Exactly one of them must end up holding it and the other must
// get an answer it can act on, which "E11000 duplicate key" is not.
func TestTwoTakingAMissingLeaseAtOnce(t *testing.T) {
	cold, done := testCold(t)
	defer done()
	ctx := context.Background()

	const racers = 8
	held := make([]*Lease, racers)
	for i := range held {
		held[i] = New(cold, "tctest", "race")
	}
	errs := make([]error, racers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range held {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = held[i].Take(ctx)
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for i, err := range errs {
		var h *ErrHeld
		switch {
		case err == nil:
			winners++
		case errors.As(err, &h):
			// The answer a loser should get.
		default:
			t.Errorf("racer %d got %v, want either the lease or an ErrHeld", i, err)
		}
	}
	if winners != 1 {
		t.Errorf("%d of %d took the lease, want exactly one", winners, racers)
	}
	for _, l := range held {
		_ = l.Release(ctx)
	}
}
