package feeder

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// A deleteMany is never a retryable write, so its events carry no lsid, and
// 8.0 logs it as one applyOps whose events share one time. A batch ending
// after the first of them looked like one event on its own and claimed the
// time with the rest unread. The oplog says which times are such a group.
func TestGroupedAtKnowsABatchedDelete(t *testing.T) {
	uri := os.Getenv("TC_TEST_HOT")
	if uri == "" {
		t.Skip("set TC_TEST_HOT to a replica set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hot, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hot.Disconnect(context.WithoutCancel(ctx)) }()
	c := hot.Database("tc_grouped").Collection("c")
	defer func() { _ = hot.Database("tc_grouped").Drop(context.WithoutCancel(ctx)) }()
	if _, err := c.InsertMany(ctx, []any{bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "_id", Value: 2}}, bson.D{{Key: "_id", Value: 3}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.InsertOne(ctx, bson.D{{Key: "_id", Value: 4}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$lte", Value: 3}}}}); err != nil {
		t.Fatal(err)
	}
	oplog := hot.Database("local").Collection("oplog.rs")
	tsOf := func(filter bson.D) bson.Timestamp {
		var e struct {
			TS bson.Timestamp `bson:"ts"`
		}
		if err := oplog.FindOne(ctx, filter, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})).Decode(&e); err != nil {
			t.Fatalf("find %v in the oplog: %v", filter, err)
		}
		return e.TS
	}
	del := tsOf(bson.D{{Key: "op", Value: "c"}, {Key: "o.applyOps.ns", Value: "tc_grouped.c"}})
	ins := tsOf(bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "tc_grouped.c"}, {Key: "o._id", Value: 4}})

	f := testFeeder()
	f.hot = hot
	if g, err := f.groupedAt(ctx, del); err != nil || !g {
		t.Errorf("the deleteMany's applyOps at %v read as grouped %v (%v), want true", del, g, err)
	}
	if g, err := f.groupedAt(ctx, ins); err != nil || g {
		t.Errorf("a single insert at %v read as grouped %v (%v), want false", ins, g, err)
	}

	// And at a batch end: the first event of the delete read, the time is
	// left unclaimed; a single insert's is claimed.
	for _, tc := range []struct {
		name  string
		at    bson.Timestamp
		claim bool
	}{{"the delete's first event", del, false}, {"a single insert", ins, true}} {
		f := testFeeder()
		f.hot = hot
		f.lastCT, f.lastCTRun = tc.at, 1
		f.markBatchEnd(ctx, tokenRaw(t, fmt.Sprintf("82%08X%08X2B042C01002B026E5A1004", tc.at.T, tc.at.I)))
		if claimed := f.lastEventTS == tc.at; claimed != tc.claim {
			t.Errorf("%s: claimed the time %v, want %v", tc.name, claimed, tc.claim)
		}
	}
}
