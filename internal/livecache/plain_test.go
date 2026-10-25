package livecache

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// What the cache can answer exactly: a namespace that exists, with no
// collation, that is not a view and not a time series. A namespace not there
// yet was recorded as plain for good, so a collation or view created later
// was never seen, and a time series was taken though its writes never reach
// the stream this cache follows.
func TestPlainNamespaces(t *testing.T) {
	uri := os.Getenv("TC_TEST_HOT")
	if uri == "" {
		t.Skip("set TC_TEST_HOT to a replica set to run the live cache against MongoDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Disconnect(context.Background()) }()
	db := cl.Database("tc_plain")
	_ = db.Drop(ctx)
	defer func() { _ = db.Drop(context.Background()) }()

	c := New(1 << 20)
	ask := func(coll string) bool {
		ok, known := c.plain(ctx, cl, "tc_plain."+coll)
		if !known {
			t.Fatalf("could not tell whether %s is plain", coll)
		}
		return ok
	}

	if ask("later") {
		t.Error("a namespace that does not exist yet was taken as plain")
	}
	if err := db.CreateCollection(ctx, "later", options.CreateCollection().
		SetCollation(&options.Collation{Locale: "en", Strength: 2})); err != nil {
		t.Fatal(err)
	}
	if ask("later") {
		t.Error("a collection created with a collation after it was first asked about was taken as plain")
	}

	if err := db.CreateCollection(ctx, "ts", options.CreateCollection().
		SetTimeSeriesOptions(options.TimeSeries().SetTimeField("at"))); err != nil {
		t.Fatal(err)
	}
	if ask("ts") {
		t.Error("a time series collection was taken as plain; its writes never reach the stream")
	}

	if _, err := db.Collection("p").InsertOne(ctx, bson.D{{Key: "n", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if !ask("p") {
		t.Error("an ordinary collection was not taken as plain")
	}
	// Dropped and created again as a view: what was learned has to go.
	if err := db.Collection("p").Drop(ctx); err != nil {
		t.Fatal(err)
	}
	c.DropNS("tc_plain.p")
	if err := db.CreateView(ctx, "p", "ts", mongo.Pipeline{}); err != nil {
		t.Fatal(err)
	}
	if ask("p") {
		t.Error("a name dropped and created again as a view was still taken as plain")
	}
}
