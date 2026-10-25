package router

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/checkpoint"
)

// TestTailTouched reads a real oplog's tail the way a merged read does while
// the archive is being read: every update and delete of the namespace in the
// window, by _id compared as mongod compares it, nothing for inserts or for
// other namespaces, and everything for a transaction. testfresh cannot reach
// this path on a fast machine, where the tracker's stream has the delete
// before the read asks.
func TestTailTouched(t *testing.T) {
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
	db := cl.Database("tc_tail")
	co, other := db.Collection("orders"), db.Collection("other")
	_ = db.Drop(ctx)
	defer func() { _ = db.Drop(context.Background()) }()
	for _, c := range []*mongo.Collection{co, other} {
		if _, err := c.InsertMany(ctx, []any{bson.D{{Key: "_id", Value: int32(1)}}, bson.D{{Key: "_id", Value: int32(2)}},
			bson.D{{Key: "_id", Value: int32(3)}}, bson.D{{Key: "_id", Value: "s"}}}); err != nil {
			t.Fatal(err)
		}
	}
	r := &Router{hot: cl}
	clock := func() bson.Timestamp {
		ts, ok := r.fastTierClock(ctx)
		if !ok {
			t.Fatal("no clock")
		}
		return ts
	}
	keys := func(tt tailTouch) string {
		var ks []string
		for k := range tt.ids {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		return strings.Join(ks, "|")
	}
	want := func(vs ...any) string {
		var ks []string
		for _, v := range vs {
			typ, b, _ := bson.MarshalValue(v)
			ks = append(ks, idKey(bson.RawValue{Type: typ, Value: b}))
		}
		sort.Strings(ks)
		return strings.Join(ks, "|")
	}

	from := clock()
	_, _ = co.UpdateOne(ctx, bson.D{{Key: "_id", Value: int32(1)}}, bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: 1}}}})
	_, _ = co.DeleteOne(ctx, bson.D{{Key: "_id", Value: "s"}})
	_, _ = co.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(9)}})
	_, _ = other.DeleteOne(ctx, bson.D{{Key: "_id", Value: int32(2)}})
	until := clock()
	got := r.tailTouched(ctx, "tc_tail.orders", from, until)
	if !got.ok || got.all {
		t.Fatalf("plain writes: ok=%v all=%v", got.ok, got.all)
	}
	if keys(got) != want(int64(1), "s") {
		t.Errorf("plain writes: the tail named %q, want the update of 1 and the delete of s only", keys(got))
	}
	// The same _id as a double is the same document to mongod's _id index.
	if !got.ids[idKey(func() bson.RawValue { typ, b, _ := bson.MarshalValue(1.0); return bson.RawValue{Type: typ, Value: b} }())] {
		t.Error("1.0 was not found as the 1 the tail named")
	}

	from = clock()
	sess, err := cl.StartSession()
	if err != nil {
		t.Fatal(err)
	}
	_, err = sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
		_, err := co.UpdateOne(sc, bson.D{{Key: "_id", Value: int32(3)}}, bson.D{{Key: "$set", Value: bson.D{{Key: "y", Value: 1}}}})
		return nil, err
	})
	sess.EndSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got = r.tailTouched(ctx, "tc_tail.orders", from, clock())
	if !got.ok || !got.all {
		t.Errorf("a transaction: ok=%v all=%v, want all, since an applyOps can hide anything", got.ok, got.all)
	}

	from = clock()
	_, _ = other.UpdateOne(ctx, bson.D{{Key: "_id", Value: int32(1)}}, bson.D{{Key: "$set", Value: bson.D{{Key: "z", Value: 1}}}})
	got = r.tailTouched(ctx, "tc_tail.orders", from, clock())
	if !got.ok || got.all || len(got.ids) != 0 {
		t.Errorf("another namespace: ok=%v all=%v ids=%q, want nothing", got.ok, got.all, keys(got))
	}
}

// TestTrackerBatchedRetryableWrite runs the tracker against a real oplog: a
// retryable insertMany the server batches into chained applyOps entries,
// marked multiOpType 1, is a write to its own database and not a transaction
// touching every database. Taken for a transaction, every copy-up made every
// read's freshness bar the newest write anywhere.
func TestTrackerBatchedRetryableWrite(t *testing.T) {
	uri := os.Getenv("TC_TEST_HOT")
	if uri == "" {
		t.Skip("set TC_TEST_HOT to a replica set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Disconnect(context.Background()) }()
	busy, quiet := cl.Database("tc_batch_busy"), cl.Database("tc_batch_quiet")
	for _, d := range []*mongo.Database{busy, quiet} {
		_ = d.Drop(ctx)
		defer func(d *mongo.Database) { _ = d.Drop(context.Background()) }(d)
	}
	hello := func() (uint64, int64) {
		var h struct {
			LastWrite struct {
				OpTime struct {
					TS bson.Timestamp `bson:"ts"`
					T  int64          `bson:"t"`
				} `bson:"opTime"`
			} `bson:"lastWrite"`
		}
		if err := cl.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&h); err != nil {
			t.Fatal(err)
		}
		return checkpoint.PackTS(h.LastWrite.OpTime.TS), h.LastWrite.OpTime.T
	}

	r := &Router{hot: cl, touches: newTouchTracker()}
	go func() { _ = r.followTouchesOnce(ctx) }()

	if _, err := quiet.Collection("c").InsertOne(ctx, bson.D{{Key: "_id", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	quietAt, term := hello()
	docs := make([]any, 5000)
	for i := range docs {
		docs[i] = bson.D{{Key: "_id", Value: i}, {Key: "pad", Value: strings.Repeat("x", 200)}}
	}
	if _, err := busy.Collection("c").InsertMany(ctx, docs); err != nil {
		t.Fatal(err)
	}
	through, term2 := hello()
	if term2 != term {
		t.Skip("an election during the test")
	}
	// The case, asserted: the insertMany is chained entries marked as a batch.
	n, err := cl.Database("local").Collection("oplog.rs").CountDocuments(ctx, bson.D{
		{Key: "ts", Value: bson.D{{Key: "$gt", Value: checkpoint.UnpackTS(quietAt)}}},
		{Key: "multiOpType", Value: 1}, {Key: "prevOpTime.ts", Value: bson.D{{Key: "$gt", Value: bson.Timestamp{}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("the insertMany wrote no chained batch entry, so this tests nothing")
	}

	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	need, ok := r.touches.lastWriteIn(wctx, "tc_batch_quiet", through, term)
	if !ok {
		t.Fatal("the tracker did not reach the insertMany")
	}
	if need > quietAt {
		t.Errorf("the quiet database's newest write is %v, past its own insert at %v: the batch was taken for a transaction", checkpoint.UnpackTS(need), checkpoint.UnpackTS(quietAt))
	}
	if need, _ := r.touches.lastWriteIn(wctx, "tc_batch_busy", through, term); need <= quietAt {
		t.Errorf("the busy database's newest write is %v, before the insertMany", checkpoint.UnpackTS(need))
	}
}
