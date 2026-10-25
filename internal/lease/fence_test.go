package lease

import (
	"context"
	"errors"
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// A flush is written only while its feeder holds the lease. Fence is the write
// in the flush's transaction that makes it so.
func TestFenceOnlyWhileHeld(t *testing.T) {
	uri := os.Getenv("TC_TEST_COLD")
	if uri == "" {
		t.Skip("set TC_TEST_COLD to run the fence test")
	}
	ctx := context.Background()
	cold, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cold.Disconnect(ctx) }()
	l := New(cold, "feeder", "fence-test")
	row := bson.D{{Key: "_id", Value: l.docID()}}
	markers := cold.Database(meta.DB).Collection(meta.Markers)
	defer func() { _, _ = markers.DeleteOne(context.Background(), row) }()

	if err := l.Take(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Fence(ctx); err != nil {
		t.Fatalf("fencing a held lease: %v", err)
	}
	if _, err := markers.UpdateOne(ctx, row, bson.D{{Key: "$set", Value: bson.D{{Key: "owner", Value: "standby"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := l.Fence(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("fencing a lease another process took returned %v, want ErrLost", err)
	}
}
