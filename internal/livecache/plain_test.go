package livecache

import (
	"context"
	"math"
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

// skip and limit arrive as any int64 the client likes. Added together they
// wrapped negative, passed the window check, and became a negative slice bound
// while the cache's mutex was held, which stopped every read on the router.
func TestSkipAndLimitThatOverflowAreNotCached(t *testing.T) {
	big := int64(1) << 62
	for _, c := range []struct {
		name        string
		skip, limit int64
	}{
		{"sum wraps negative", big, big},
		{"limit alone past the window", 0, big},
		{"skip alone past the window", big, 10},
		{"largest int64 limit", 0, math.MaxInt64},
	} {
		q, ok := ParseFind("shop.orders", mustRaw(bson.D{{Key: "find", Value: "orders"},
			{Key: "skip", Value: c.skip}, {Key: "limit", Value: c.limit}}))
		if ok {
			t.Errorf("%s: skip %d limit %d was taken as cacheable (Skip+Limit = %d)",
				c.name, c.skip, c.limit, q.Skip+q.Limit)
		}
	}
	if _, ok := ParseFind("shop.orders", mustRaw(bson.D{{Key: "find", Value: "orders"},
		{Key: "skip", Value: int64(10)}, {Key: "limit", Value: int64(20)}})); !ok {
		t.Error("an ordinary skip and limit inside the window was refused")
	}
}

// Two different queries must not name one entry. The keys joined their parts
// with ",", "|", NUL and 0x01, and a client's field names and string values
// can hold all of them.
func TestDifferentQueriesHaveDifferentKeys(t *testing.T) {
	parse := func(filter bson.D) Query {
		q, ok := ParseFind("shop.orders", mustRaw(bson.D{{Key: "find", Value: "orders"},
			{Key: "filter", Value: filter}, {Key: "limit", Value: int32(10)}}))
		if !ok {
			t.Fatalf("filter %v was not cacheable, so this proves nothing", filter)
		}
		return q
	}
	pairs := [][2]bson.D{
		// One field named "a,b" against two fields a and b.
		{{{Key: "a,b", Value: "x"}}, {{Key: "a", Value: "x"}, {Key: "b", Value: "x"}}},
		// Two values that run together across the old NUL separator.
		{{{Key: "a", Value: "x\x00sy"}, {Key: "b", Value: "z"}},
			{{Key: "a", Value: "x"}, {Key: "b", Value: "y\x00sz"}}},
		// A field name holding the old shape separator.
		{{{Key: "a|eq:b", Value: "x"}}, {{Key: "a", Value: "x"}}},
	}
	for _, p := range pairs {
		a, b := parse(p[0]), parse(p[1])
		if a.entryKey() == b.entryKey() {
			t.Errorf("%v and %v share the entry key %q", p[0], p[1], a.entryKey())
		}
	}
}
