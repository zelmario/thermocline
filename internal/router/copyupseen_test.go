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

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/promote"
)

// A copy-up that takes the document its read already holds must not bring
// back one the application deleted in between. The read took {_id: 1} from the
// archive with the feeder at from; then the application deleted it, and the
// delete reached the archive as a tombstone. The copy-up that the read started
// arrives after both, and the fast tier must not hold the document.
func TestCopyUpFromSeenDoesNotUndoADelete(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tiers to run the copy-up test")
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
	const db, coll = "tc_copyupseen", "docs"
	const ns = db + "." + coll
	hc, cc := hot.Database(db).Collection(coll), cold.Database(db).Collection(coll)
	clean := func(c context.Context) {
		_ = hc.Drop(c)
		_ = cc.Drop(c)
		_, _ = cold.Database(meta.DB).Collection(meta.Residency).DeleteMany(c,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}}})
	}
	clean(ctx)
	t.Cleanup(func() { clean(context.WithoutCancel(ctx)) })

	opTime := func() bson.Timestamp {
		raw, err := hot.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Raw()
		if err != nil {
			t.Fatalf("ping: %v", err)
		}
		tt, i, _ := raw.Lookup("operationTime").TimestampOK()
		return bson.Timestamp{T: tt, I: i}
	}

	doc, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: "archived"}})
	if _, err := cc.InsertOne(ctx, bson.Raw(doc)); err != nil {
		t.Fatalf("archive: %v", err)
	}
	// The read: the archive's document, and where the feeder stood before it.
	from := opTime()
	seen := &archivedRead{doc: doc, from: from}

	// Then the application's delete, which the fast tier's oplog carries
	// (the document was put back on the fast tier and deleted there), and
	// which the feeder has mirrored as a tombstone.
	if _, err := hc.InsertOne(ctx, bson.Raw(doc)); err != nil {
		t.Fatalf("hot insert: %v", err)
	}
	if _, err := hc.DeleteOne(ctx, bson.D{{Key: "_id", Value: int32(1)}}); err != nil {
		t.Fatalf("hot delete: %v", err)
	}
	if _, err := cc.UpdateOne(ctx, bson.D{{Key: "_id", Value: int32(1)}}, meta.Tombstone(time.Now().UTC())); err != nil {
		t.Fatalf("tombstone: %v", err)
	}

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Router{hot: hot, cold: cold, log: quiet, indexer: promote.NewIndexer(hot, cold, quiet)}
	r.seenOpTime.Store(checkpoint.PackTS(opTime()))
	id := bson.Raw(doc).Lookup("_id")
	_, _ = r.copyUpFrom(ctx, ns, id, seen)
	if n, _ := hc.CountDocuments(ctx, bson.D{{Key: "_id", Value: int32(1)}}); n != 0 {
		t.Fatal("the copy-up put back a document the application had deleted after the read")
	}
}

// And with nothing in between, the copy-up takes the document the read already
// holds rather than asking the archive again: the archive's copy is gone by the
// time it runs, and the fast tier still ends up with the document.
func TestCopyUpFromSeenSkipsTheSecondRead(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tiers to run the copy-up test")
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
	const db, coll = "tc_copyupseen2", "docs"
	const ns = db + "." + coll
	hc, cc := hot.Database(db).Collection(coll), cold.Database(db).Collection(coll)
	clean := func(c context.Context) {
		_ = hc.Drop(c)
		_ = cc.Drop(c)
		_, _ = cold.Database(meta.DB).Collection(meta.Residency).DeleteMany(c,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}}})
	}
	clean(ctx)
	t.Cleanup(func() { clean(context.WithoutCancel(ctx)) })
	raw, err := hot.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Raw()
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	tt, i, _ := raw.Lookup("operationTime").TimestampOK()
	now := bson.Timestamp{T: tt, I: i}

	doc, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(2)}, {Key: "v", Value: "read once"}})
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Router{hot: hot, cold: cold, log: quiet, indexer: promote.NewIndexer(hot, cold, quiet)}
	r.seenOpTime.Store(checkpoint.PackTS(now))
	if ok, err := r.copyUpFrom(ctx, ns, bson.Raw(doc).Lookup("_id"), &archivedRead{doc: doc, from: now}); err != nil || !ok {
		t.Fatalf("copy-up: %v, %v", ok, err)
	}
	var got bson.Raw
	if err := hc.FindOne(ctx, bson.D{{Key: "_id", Value: int32(2)}}).Decode(&got); err != nil {
		t.Fatalf("the fast tier does not hold the document the read already had: %v", err)
	}
	if v, _ := got.Lookup("v").StringValueOK(); v != "read once" {
		t.Errorf("the fast tier holds %v, want the document the read had", got)
	}
}
