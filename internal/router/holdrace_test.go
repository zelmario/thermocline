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
	if err := meta.AnnounceEviction(ctx, cl, []string{meta.DocID(ns, id)}); err != nil {
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
	if err := meta.AnnounceEviction(ctx, cl, []string{docID}); err != nil {
		t.Fatalf("announce: %v", err)
	}
	promote.HeldForTest = func(string, []bson.RawValue, []bool) {
		go func() {
			time.Sleep(100 * time.Millisecond)
			_ = meta.WithdrawEviction(context.Background(), cl, []string{docID})
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
	if err := meta.AnnounceEviction(ctx, cl, []string{docID}); err != nil {
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
