package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/lease"
	"github.com/zelmario/thermocline/internal/meta"
)

// A backfill holds the evictor's lease while it marks deletes, and renewed it
// without looking at the answer: paused past the lease, it went on marking
// while the evictor, holding the lease again, deleted the same documents. The
// context it works under now ends when the lease is lost.
func TestEvictorHoldEndsWhenTheLeaseIsLost(t *testing.T) {
	coldURI := os.Getenv("TC_TEST_COLD")
	if coldURI == "" {
		t.Skip("set TC_TEST_COLD to run the lease hold test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cold, err := mongo.Connect(options.Client().ApplyURI(coldURI))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cold.Disconnect(context.WithoutCancel(ctx)) }()
	markers := cold.Database(meta.DB).Collection(meta.Markers)
	row := bson.D{{Key: "_id", Value: "lease:evictor:hold-test"}}
	_, _ = markers.DeleteOne(ctx, row)
	t.Cleanup(func() { _, _ = markers.DeleteOne(context.WithoutCancel(ctx), row) })

	held, release, err := holdEvictorLease(ctx, cold, "hold-test")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := markers.UpdateOne(ctx, row, bson.D{{Key: "$set", Value: bson.D{{Key: "owner", Value: "the evictor"}}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-held.Done():
		if cause := context.Cause(held); !errors.Is(cause, lease.ErrLost) {
			t.Errorf("the hold ended with %v, want ErrLost", cause)
		}
	case <-time.After(2*lease.Renew + 2*time.Second):
		t.Error("the hold went on after the lease was taken; work under it would go on too")
	}
}
