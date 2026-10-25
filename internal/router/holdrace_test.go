package router

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/promote"
	"github.com/zelmario/thermocline/internal/watermark"
)

// A copy-up asks the fast tier which documents it already holds and skips
// those. An eviction sweep that read the holds before this copy-up wrote its
// own deletes such a document anyway, and the statement the copy-up was for
// then misses it. The fuzzer met it as an updateMany $inc that reported 55
// modified where MongoDB modified 56, both tiers keeping the old value.
//
// promote.HeldForTest plays that sweep's delete in the one place it can land: after
// the fast tier answered and before anything else.
func TestCopyUpDoesNotSkipADocumentBeingEvicted(t *testing.T) {
	cl, coll, r := holdRaceSetup(t, "tc_holdrace")
	ctx := context.Background()
	const ns = "tc_holdrace.docs"

	id := rawID(t, 1)
	if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: 1}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := meta.AnnounceEviction(ctx, cl, []string{meta.DocID(ns, id)}, "test:0"); err != nil {
		t.Fatalf("announce: %v", err)
	}
	promote.HeldForTest = func(string, []bson.RawValue, []bool) {
		if _, err := coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: 1}}); err != nil {
			t.Errorf("the evictor's delete: %v", err)
		}
	}
	t.Cleanup(func() { promote.HeldForTest = nil })

	have, left, err := r.holdWhatIsThere(ctx, ns, coll, []bson.RawValue{id})
	if err != nil {
		t.Fatalf("holdWhatIsThere: %v", err)
	}
	if have[0] {
		t.Fatalf("a document the eviction removed was reported held, so the copy-up skips it " +
			"and the statement it was for runs against a fast tier without it")
	}
	if len(left) != 1 || left[0] != 0 {
		t.Errorf("left = %v, want [0], so the caller reads the archive's copy again", left)
	}
}

// The other way the handshake ends: the evictor sees the hold and takes its
// announcement back. The document stays, so it is held, and the copy-up does
// not wait out promote.SettleWait for it.
func TestCopyUpKeepsADocumentWhoseEvictionWasWithdrawn(t *testing.T) {
	cl, coll, r := holdRaceSetup(t, "tc_holdrace2")
	ctx := context.Background()
	const ns = "tc_holdrace2.docs"

	id := rawID(t, 1)
	if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: 1}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	docID := meta.DocID(ns, id)
	if err := meta.AnnounceEviction(ctx, cl, []string{docID}, "test:0"); err != nil {
		t.Fatalf("announce: %v", err)
	}
	promote.HeldForTest = func(string, []bson.RawValue, []bool) {
		go func() {
			time.Sleep(100 * time.Millisecond)
			_ = meta.WithdrawEviction(context.Background(), cl, []string{docID}, "test:0")
		}()
	}
	t.Cleanup(func() { promote.HeldForTest = nil })

	started := time.Now()
	have, left, err := r.holdWhatIsThere(ctx, ns, coll, []bson.RawValue{id})
	if err != nil {
		t.Fatalf("holdWhatIsThere: %v", err)
	}
	if !have[0] || len(left) != 0 {
		t.Fatalf("have=%v left=%v, want the document held and nothing left", have, left)
	}
	if took := time.Since(started); took >= promote.SettleWait {
		t.Errorf("took %v, the whole settle wait, for an eviction withdrawn after 100ms", took)
	}
}

// An announcement whose delete matched nothing is left standing for up to an
// hour on purpose. It is no eviction in flight, and waiting on it would cost
// every copy-up of that document the whole settle wait.
func TestCopyUpDoesNotWaitOnAnOldAnnouncement(t *testing.T) {
	cl, coll, r := holdRaceSetup(t, "tc_holdrace3")
	ctx := context.Background()
	const ns = "tc_holdrace3.docs"

	id := rawID(t, 1)
	if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: 1}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	docID := meta.DocID(ns, id)
	if err := meta.AnnounceEviction(ctx, cl, []string{docID}, "test:0"); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if _, err := cl.Database(meta.DB).Collection(meta.Evicting).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: docID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "at", Value: time.Now().Add(-5 * time.Minute)}}}}); err != nil {
		t.Fatalf("age the announcement: %v", err)
	}

	started := time.Now()
	have, left, err := r.holdWhatIsThere(ctx, ns, coll, []bson.RawValue{id})
	if err != nil {
		t.Fatalf("holdWhatIsThere: %v", err)
	}
	if !have[0] || len(left) != 0 {
		t.Fatalf("have=%v left=%v, want the document held and nothing left", have, left)
	}
	if took := time.Since(started); took >= promote.SettleWait {
		t.Errorf("took %v waiting on an announcement five minutes old", took)
	}
}

// An upsert, an unacknowledged write and an unsorted findAndModify are let
// through on the fast tier's word that it matches something. When the match is
// old enough to be evicted and a sweep takes it before mongod runs the
// statement, the upsert creates a document instead; with the _id in its filter
// the feeder mirrors that over the archived record.
func TestAMatchOldEnoughToEvictIsHeldBeforeAWriteRelies(t *testing.T) {
	cl, coll, r := holdRaceSetup(t, "tc_holdrace4")
	ctx := context.Background()
	const ns = "tc_holdrace4.docs"
	claimFrom(t, r, cl, ns, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	old := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "when", Value: old}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	id := rawID(t, 1)
	if err := meta.AnnounceEviction(ctx, cl, []string{meta.DocID(ns, id)}, "test:0"); err != nil {
		t.Fatalf("announce: %v", err)
	}
	promote.HeldForTest = func(string, []bson.RawValue, []bool) {
		if _, err := coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: 1}}); err != nil {
			t.Errorf("the evictor's delete: %v", err)
		}
	}
	t.Cleanup(func() { promote.HeldForTest = nil })

	filter, _ := bson.Marshal(bson.D{{Key: "_id", Value: 1}})
	has, err := r.fastTierMatchesHeld(ctx, ns, filter, nil, nil)
	if err != nil {
		t.Fatalf("fastTierMatchesHeld: %v", err)
	}
	if has {
		t.Fatalf("a match an eviction took was reported present, so an upsert of it would " +
			"create a document over the archived record")
	}
}

// A match newer than the claim's bound is not one any sweep can take before
// the statement runs, so no hold is written for it: the hold is an archive
// round trip, and an upsert usually names a recent document.
func TestARecentMatchCostsNoHold(t *testing.T) {
	cl, coll, r := holdRaceSetup(t, "tc_holdrace5")
	ctx := context.Background()
	const ns = "tc_holdrace5.docs"
	claimFrom(t, r, cl, ns, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	recent := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	old := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	if _, err := coll.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: 1}, {Key: "when", Value: recent}},
		bson.D{{Key: "_id", Value: 2}, {Key: "when", Value: old}},
		bson.D{{Key: "_id", Value: 3}},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	residency := cl.Database(meta.DB).Collection(meta.Residency)
	for _, c := range []struct {
		id   int32
		held bool
	}{{1, false}, {2, true}, {3, true}} {
		filter, _ := bson.Marshal(bson.D{{Key: "_id", Value: c.id}})
		has, err := r.fastTierMatchesHeld(ctx, ns, filter, nil, nil)
		if err != nil || !has {
			t.Fatalf("_id %d: has=%v err=%v, want a match", c.id, has, err)
		}
		n, err := residency.CountDocuments(ctx, bson.D{{Key: "_id", Value: meta.DocID(ns, rawID(t, c.id))}})
		if err != nil {
			t.Fatalf("count holds: %v", err)
		}
		if (n == 1) != c.held {
			t.Errorf("_id %d: hold written = %v, want %v", c.id, n == 1, c.held)
		}
	}
}

func claimFrom(t *testing.T, r *Router, cl *mongo.Client, ns string, from time.Time) {
	t.Helper()
	r.marks = watermark.New(cl)
	r.cfg.TieredBy = map[string]string{ns: "when"}
	if err := r.marks.Set(context.Background(), ns, from, "when"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	t.Cleanup(func() { _, _ = r.marks.Clear(context.Background(), ns) })
}

func holdRaceSetup(t *testing.T, db string) (*mongo.Client, *mongo.Collection, *Router) {
	t.Helper()
	uri := os.Getenv("TC_TEST_COLD")
	if uri == "" {
		t.Skip("set TC_TEST_COLD to a tier URI to run the copy-up eviction race test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = cl.Disconnect(context.Background()) })
	coll := cl.Database(db).Collection("docs")
	ns := db + ".docs"
	clean := func() {
		c := context.Background()
		_ = coll.Drop(c)
		_, _ = cl.Database(meta.DB).Collection(meta.Residency).DeleteMany(c, bson.D{{Key: "ns", Value: ns}})
		_, _ = cl.Database(meta.DB).Collection(meta.Evicting).DeleteMany(c,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}}})
	}
	clean()
	t.Cleanup(clean)
	if err := coll.Database().CreateCollection(ctx, "docs"); err != nil {
		t.Fatalf("create: %v", err)
	}
	r := &Router{
		hot:  cl,
		cold: cl,
		cfg:  config.RouterConfig{Residency: config.Duration(time.Minute)},
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return cl, coll, r
}

func rawID(t *testing.T, v int32) bson.RawValue {
	t.Helper()
	typ, b, err := bson.MarshalValue(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return bson.RawValue{Type: typ, Value: b}
}
