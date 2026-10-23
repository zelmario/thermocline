package router

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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
