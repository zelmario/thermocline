package feeder

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
)

// Two documents trade a unique value through a free one on the fast tier while
// the feeder is behind. Every event then carries the document's present image
// (updateLookup), so the replay asks the archive for a state the fast tier
// never held. Inside one flush and across two, the archive must end as the
// fast tier is, with nothing left recorded as unmirrored.
func TestUniqueSwapReplayedFromPresentImages(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tiers to run the unique swap test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	connect := func(uri string) *mongo.Client {
		c, err := mongo.Connect(options.Client().ApplyURI(uri))
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(func() { _ = c.Disconnect(context.WithoutCancel(ctx)) })
		return c
	}
	hot, cold := connect(hotURI), connect(coldURI)
	const db, coll = "tc_clash", "swap"
	const ns = db + "." + coll
	hc, cc := hot.Database(db).Collection(coll), cold.Database(db).Collection(coll)
	unmirrored := cold.Database(meta.DB).Collection(meta.Unmirrored)
	reset := func(c context.Context) {
		_ = hc.Drop(c)
		_ = cc.Drop(c)
		_, _ = unmirrored.DeleteMany(c, bson.D{{Key: "ns", Value: ns}})
	}
	t.Cleanup(func() { reset(context.WithoutCancel(ctx)) })

	doc := func(id, k int32) bson.Raw {
		b, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "k", Value: k}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	// The present image of a document, as an upsert the way flush builds one.
	up := func(id, k int32) pendingOp {
		d := doc(id, k)
		key, _ := bson.Marshal(bson.D{{Key: "_id", Value: id}})
		return pendingOp{ns: ns, key: key, id: d.Lookup("_id"), doc: d, kind: actionUpsert}
	}
	write := func(f *Feeder, ops []pendingOp) {
		models := make([]mongo.WriteModel, len(ops))
		for i, op := range ops {
			models[i] = meta.ReplaceModel(op.key, op.doc)
		}
		if _, err := f.writeFlush(ctx, []string{ns}, map[string][]mongo.WriteModel{ns: models},
			map[string][]pendingOp{ns: ops}, time.Now()); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	// Before: a holds 1 and b holds 2 on both tiers, unique on k. After, on
	// the fast tier: a holds 2 and b holds 1, by way of a holding -1.
	setup := func() {
		reset(ctx)
		for _, c := range []*mongo.Collection{hc, cc} {
			if _, err := c.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "k", Value: 1}},
				Options: options.Index().SetUnique(true)}); err != nil {
				t.Fatalf("index: %v", err)
			}
			if _, err := c.InsertMany(ctx, []any{doc(1, 1), doc(2, 2)}); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		if _, err := hc.DeleteMany(ctx, bson.D{}); err != nil {
			t.Fatal(err)
		}
		if _, err := hc.InsertMany(ctx, []any{doc(1, 2), doc(2, 1)}); err != nil {
			t.Fatalf("the fast tier's present: %v", err)
		}
	}
	check := func(why string) {
		for _, w := range []struct{ id, k int32 }{{1, 2}, {2, 1}} {
			var got struct {
				K int32 `bson:"k"`
			}
			if err := cc.FindOne(ctx, bson.D{{Key: "_id", Value: w.id}}).Decode(&got); err != nil || got.K != w.k {
				t.Errorf("%s: archive _id %d holds k=%d (%v), the fast tier holds %d", why, w.id, got.K, err, w.k)
			}
		}
		if n, _ := unmirrored.CountDocuments(ctx, bson.D{{Key: "ns", Value: ns}}); n != 0 {
			t.Errorf("%s: %d documents left recorded as unmirrored", why, n)
		}
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.FeederConfig{ID: "tcclash", Namespaces: []string{ns}}

	// All three events in one flush: a to -1, b to 1, a to 2, each with its
	// present image.
	setup()
	f := &Feeder{hot: hot, cold: cold, cfg: cfg, log: quiet}
	write(f, []pendingOp{up(1, 2), up(2, 1), up(1, 2)})
	check("one flush")

	// Split: a's events in one flush, b's in the next. Each refuses the one
	// document whose clash the other flush holds, and the retry of the
	// refused documents puts the archive right.
	setup()
	f = &Feeder{hot: hot, cold: cold, cfg: cfg, log: quiet}
	write(f, []pendingOp{up(1, 2)})
	write(f, []pendingOp{up(2, 1)})
	if !f.hasUnmirrored(ns) {
		t.Fatal("split across flushes, nothing was refused; the test no longer reaches the retry")
	}
	if err := f.flush(ctx); err != nil {
		t.Fatalf("the flush of retries: %v", err)
	}
	check("two flushes and a retry")

	// A real duplicate: the archive alone has the unique index, and the fast
	// tier holds two documents with value 1, the newcomer first in the flush.
	// The archive keeps the document it had and refuses the newcomer only.
	reset(ctx)
	if _, err := cc.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "k", Value: 1}},
		Options: options.Index().SetUnique(true)}); err != nil {
		t.Fatalf("index: %v", err)
	}
	if _, err := cc.InsertOne(ctx, doc(1, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := hc.InsertMany(ctx, []any{doc(1, 1), doc(2, 1)}); err != nil {
		t.Fatal(err)
	}
	f = &Feeder{hot: hot, cold: cold, cfg: cfg, log: quiet}
	write(f, []pendingOp{up(2, 1), up(1, 1)})
	if n, _ := cc.CountDocuments(ctx, bson.D{{Key: "_id", Value: int32(1)}}); n != 1 {
		t.Error("a real duplicate: the archive lost the document it held")
	}
	if n, _ := cc.CountDocuments(ctx, bson.D{{Key: "_id", Value: int32(2)}}); n != 0 {
		t.Error("a real duplicate: the archive took the newcomer")
	}
	var recs []struct {
		Doc int32 `bson:"doc"`
	}
	cur, err := unmirrored.Find(ctx, bson.D{{Key: "ns", Value: ns}})
	if err == nil {
		_ = cur.All(ctx, &recs)
	}
	if len(recs) != 1 || recs[0].Doc != 2 {
		t.Errorf("a real duplicate: recorded %v as unmirrored, want only the newcomer, 2", recs)
	}
}
