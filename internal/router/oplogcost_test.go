package router

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// TestOplogCheckCost measures what one oplog check of the merged read costs
// the fast tier's primary under writes, for a feeder that far behind: its
// latency and the entries it examines. A measurement, not a check; run it with
// TC_TEST_HOT pointing at a replica set and -v.
func TestOplogCheckCost(t *testing.T) {
	uri := os.Getenv("TC_TEST_HOT")
	if uri == "" {
		t.Skip("set TC_TEST_HOT to a replica set")
	}
	ctx := context.Background()
	cl, err := mongo.Connect(options.Client().ApplyURI(uri).SetMaxPoolSize(64))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Disconnect(ctx) }()
	const ns = "tc_oplogcost.orders"
	co := cl.Database("tc_oplogcost").Collection("orders")
	_ = co.Drop(ctx)
	defer func() { _ = co.Drop(ctx) }()

	// Writers at about rate a second, updates and inserts, like the shop's
	// fast tier: carts, orders placed, orders shipped.
	rate := 1500
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var written atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			tick := time.NewTicker(time.Second / time.Duration(rate/8))
			defer tick.Stop()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case <-tick.C:
				}
				id := fmt.Sprintf("w%d-%d", w, i%5000)
				_, _ = co.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 1}}}}, options.UpdateOne().SetUpsert(true))
				written.Add(1)
			}
		}(w)
	}
	time.Sleep(5 * time.Second)

	local := cl.Database("local", options.Database().SetReadPreference(readpref.Primary()))
	oplog := local.Collection("oplog.rs")
	// fromBack is the ts of the entry about lag ago at the rate written,
	// counted back from the end, which reads backwards from the newest entry
	// rather than scanning the oplog for a wall time.
	fromBack := func(lag time.Duration) (bson.Timestamp, bson.Timestamp) {
		n := int64(float64(rate) * lag.Seconds())
		var e struct {
			TS bson.Timestamp `bson:"ts"`
		}
		o := options.Find().SetSort(bson.D{{Key: "$natural", Value: -1}}).SetSkip(n).SetLimit(1).SetProjection(bson.D{{Key: "ts", Value: 1}})
		var last struct {
			TS bson.Timestamp `bson:"ts"`
		}
		_ = oplog.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})).Decode(&last)
		cur, err := oplog.Find(ctx, bson.D{}, o)
		if err != nil || !cur.Next(ctx) || cur.Decode(&e) != nil {
			return last.TS, last.TS
		}
		_ = cur.Close(ctx)
		return e.TS, last.TS
	}
	ids := bson.A{}
	for i := 0; i < 10; i++ {
		ids = append(ids, fmt.Sprintf("archived-%d", i))
	}
	for _, lag := range []time.Duration{20 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second} {
		var lat []time.Duration
		examined := int64(0)
		for k := 0; k < 200; k++ {
			from, until := fromBack(lag)
			direct, inner := touchesIDs(ns, ids, false)
			filter := oplogWindow(ns, from, direct, inner)
			t0 := time.Now()
			var out bson.Raw
			out, err = local.RunCommand(ctx, bson.D{
				{Key: "find", Value: "oplog.rs"}, {Key: "filter", Value: filter},
				{Key: "projection", Value: bson.D{{Key: "ts", Value: 1}}}, {Key: "limit", Value: int32(1)},
				{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}, {Key: "afterClusterTime", Value: until}}},
			}).Raw()
			lat = append(lat, time.Since(t0))
			if err != nil {
				t.Fatal(err)
			}
			_ = out
			if k%20 == 0 {
				ex, err := local.RunCommand(ctx, bson.D{{Key: "explain", Value: bson.D{
					{Key: "find", Value: "oplog.rs"}, {Key: "filter", Value: filter}, {Key: "limit", Value: int32(1)}}},
					{Key: "verbosity", Value: "executionStats"}}).Raw()
				if err == nil {
					n, _ := ex.Lookup("executionStats", "totalDocsExamined").AsInt64OK()
					examined += n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		t.Logf("feeder %v behind: oplog check p50 %v p95 %v p99 %v, about %d entries examined",
			lag, lat[len(lat)/2].Round(time.Microsecond), lat[len(lat)*95/100].Round(time.Microsecond),
			lat[len(lat)*99/100].Round(time.Microsecond), examined/10)
	}
	close(stop)
	wg.Wait()
	t.Logf("%d writes in the background", written.Load())
}
