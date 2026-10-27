package router

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Concurrent holds share batches, every one lands, and a write the archive
// refuses is reported to its own caller and to no other.
func TestResidencyBatcherReportsEachWrite(t *testing.T) {
	coldURI := os.Getenv("TC_TEST_COLD")
	if coldURI == "" {
		t.Skip("set TC_TEST_COLD to run the residency batcher test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cold, err := mongo.Connect(options.Client().ApplyURI(coldURI))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = cold.Disconnect(context.WithoutCancel(ctx)) }()
	coll := cold.Database("tc_resbatch").Collection("holds")
	_ = coll.Drop(ctx)
	defer func() { _ = coll.Drop(context.WithoutCancel(ctx)) }()

	b := newResidencyBatcher(coll)
	const n, bad = 600, 137
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("d%04d", i)
			set := bson.D{{Key: "v", Value: i}}
			if i == bad {
				// An upsert that would change _id is refused by mongod.
				set = bson.D{{Key: "_id", Value: "elsewhere"}}
			}
			errs[i] = b.write(ctx, mongo.NewUpdateOneModel().
				SetFilter(bson.D{{Key: "_id", Value: id}}).
				SetUpdate(bson.D{{Key: "$set", Value: set}}).
				SetUpsert(true))
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if i == bad && err == nil {
			t.Errorf("write %d was refused by the archive and reported no error", i)
		}
		if i != bad && err != nil {
			t.Errorf("write %d landed and reported %v", i, err)
		}
	}
	if got, _ := coll.CountDocuments(ctx, bson.D{}); got != n-1 {
		t.Errorf("the archive holds %d holds, want %d", got, n-1)
	}
}
