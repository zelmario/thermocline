package watermark

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
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

	if err := s.Set(ctx, testNS, warmedBackTo, ""); err != nil {
		t.Fatalf("Set: %v", err)
	}

	raised, err := s.RaiseTo(ctx, testNS, evictionBoundary, "")
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
	if !got.From.Equal(evictionBoundary) {
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

	if err := s.Set(ctx, testNS, later, ""); err != nil {
		t.Fatalf("Set: %v", err)
	}
	raised, err := s.RaiseTo(ctx, testNS, earlier, "")
	if err != nil {
		t.Fatalf("RaiseTo: %v", err)
	}
	if raised {
		t.Error("RaiseTo reported a change while being handed an earlier boundary")
	}

	got, _ := s.Load(ctx, testNS)
	if !got.From.Equal(later) {
		t.Errorf("watermark moved earlier to %v, want it held at %v", got, later)
	}
}

// An evictor may well run before any warmer has recorded anything, and what it
// must do then is nothing.
//
// This pinned the opposite, and the opposite is how an adoption answers a range
// query with silence. The fast tier starts empty, one sweep scans zero
// documents and evicts zero, and the claim it left said the fast tier holds
// everything since the eviction boundary. Measured through the router on 4,000
// archived orders: 240 in the last thirty days before the sweep, 0 afterwards,
// reported as success.
func TestRaiseToDoesNotCreateAClaim(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	boundary := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	raised, err := s.RaiseTo(ctx, testNS, boundary, "")
	if err != nil {
		t.Fatalf("RaiseTo: %v", err)
	}
	if raised {
		t.Error("RaiseTo reported a change while there was no claim to narrow")
	}
	got, err := s.Load(ctx, testNS)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Claimed {
		t.Errorf("a namespace nobody warmed claims completeness from %v after one sweep", got.From)
	}
}

// A namespace nobody has warmed claims nothing, rather than claiming everything.
//
// This is the bug that made an empty fast tier answer range queries in full: a
// missing watermark decoded to the zero time, and the router read a zero time
// as "complete since the beginning of history".
func TestLoadMissingClaimsNothing(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()

	got, err := s.Load(context.Background(), "tctest.neverseen")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Claimed {
		t.Error("an unknown namespace reported a completeness claim")
	}
	if got.Complete(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Error("an unclaimed namespace answered Complete, which lets short results through as success")
	}
}

// Clearing is how an operator recovers after the fast tier is rebuilt: the old
// claim describes data that is no longer there.
func TestClearWithdrawsTheClaim(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	if err := s.Set(ctx, testNS, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), ""); err != nil {
		t.Fatalf("Set: %v", err)
	}

	existed, err := s.Clear(ctx, testNS)
	if err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if !existed {
		t.Error("Clear reported nothing to remove while a watermark existed")
	}

	got, err := s.Load(ctx, testNS)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Claimed {
		t.Error("the claim survived Clear")
	}

	// Clearing again is not an error, so recovery scripts stay idempotent.
	existed, err = s.Clear(ctx, testNS)
	if err != nil {
		t.Fatalf("second Clear: %v", err)
	}
	if existed {
		t.Error("second Clear claimed to remove something")
	}
}

// A tier that genuinely holds all of history says so, and that has to remain
// expressible: it is the difference between a declared claim and an absent one.
func TestCompleteSinceTheBeginningIsExpressible(t *testing.T) {
	all := Mark{From: time.Time{}, Claimed: true}
	if !all.Complete(time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Error("a tier claiming all of history refused a query reaching back to 1999")
	}

	if !all.CoversAll() {
		t.Error("a tier claiming all of history did not report CoversAll, so unbounded queries get refused")
	}

	none := Mark{}
	if none.Complete(time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Error("an absent claim behaved like a claim covering all of history")
	}
	if none.CoversAll() {
		t.Error("an absent claim reported CoversAll")
	}

	// A claim part way through history cannot answer a query with no lower
	// bound, because such a query can reach further back than the claim.
	partial := Mark{From: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), Claimed: true}
	if partial.CoversAll() {
		t.Error("a partial claim reported CoversAll, which would let unbounded queries return short answers")
	}
}

// The boundary itself is inside the claim, and one instant earlier is not.
func TestCompleteBoundary(t *testing.T) {
	from := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	m := Mark{From: from, Claimed: true}

	if !m.Complete(from) {
		t.Error("a query bounded exactly at the watermark was refused")
	}
	if m.Complete(from.Add(-time.Nanosecond)) {
		t.Error("a query reaching one instant earlier than the watermark was allowed")
	}
}

var _ = meta.Watermarks

// dropDatabase names no collection, so the claims to withdraw are the ones
// keyed by a namespace in that database. It has to reach all of them and stop
// exactly there: a database whose name is a prefix of another one is the case a
// careless match gets wrong.
func TestClearDatabaseWithdrawsOnlyThatDatabase(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	when := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	for _, ns := range []string{"shop.orders", "shop.events", "shopping.orders", "warehouse.stock"} {
		if err := s.Set(ctx, ns, when, ""); err != nil {
			t.Fatalf("Set %s: %v", ns, err)
		}
	}

	n, err := s.ClearDatabase(ctx, "shop")
	if err != nil {
		t.Fatalf("ClearDatabase: %v", err)
	}
	if n != 2 {
		t.Errorf("withdrew %d claims, want 2", n)
	}

	for ns, wantClaimed := range map[string]bool{
		"shop.orders":     false,
		"shop.events":     false,
		"shopping.orders": true,
		"warehouse.stock": true,
	} {
		got, lerr := s.Load(ctx, ns)
		if lerr != nil {
			t.Fatalf("Load %s: %v", ns, lerr)
		}
		if got.Claimed != wantClaimed {
			t.Errorf("%s claimed=%v, want %v", ns, got.Claimed, wantClaimed)
		}
	}

	// A database with nothing recorded is not an error, so the feeder can call
	// this on every dropDatabase it sees.
	if n, err = s.ClearDatabase(ctx, "nothing"); err != nil || n != 0 {
		t.Errorf("ClearDatabase on an untouched database: %d, %v", n, err)
	}
}
