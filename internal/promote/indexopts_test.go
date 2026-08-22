package promote

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
)

// An index carries more than its keys, and what is not copied changes what the
// index does.
//
// The fast tier's copy is built from the archive's definition. Reading only the
// fields this package happens to know about means every other one is dropped,
// and the index that arrives is a different index with the same name: a
// wildcard index that indexes every field instead of two, a text index that
// ranks by a different set of weights. Both answer, so nothing fails; they
// answer differently from the tier they were copied from, which is the one
// thing this project promises they will not do.
func TestIndexOptionsSurviveTheCopy(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tier URIs to run the index option test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	hot := connectOrSkip(t, ctx, hotURI)
	cold := connectOrSkip(t, ctx, coldURI)
	const db, coll = "tc_ixopts", "docs"
	const ns = db + "." + coll
	t.Cleanup(func() {
		c := context.WithoutCancel(ctx)
		_ = hot.Database(db).Drop(c)
		_ = cold.Database(db).Drop(c)
	})
	if err := hot.Database(db).Drop(ctx); err != nil {
		t.Fatalf("drop hot: %v", err)
	}
	if err := cold.Database(db).Drop(ctx); err != nil {
		t.Fatalf("drop cold: %v", err)
	}

	// The archive, as an adopted cluster has it.
	cc := cold.Database(db).Collection(coll)
	if _, err := cc.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: 1}, {Key: "title", Value: "red widget"},
			{Key: "body", Value: "a plain widget"}, {Key: "a", Value: 1}, {Key: "b", Value: 2}},
		bson.D{{Key: "_id", Value: 2}, {Key: "title", Value: "blue gadget"},
			{Key: "body", Value: "red red red"}, {Key: "a", Value: 3}, {Key: "b", Value: 4}},
	}); err != nil {
		t.Fatalf("seed archive: %v", err)
	}
	want := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "$**", Value: 1}},
			Options: options.Index().SetName("wild").
				SetWildcardProjection(bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 1}}),
		},
		{
			Keys: bson.D{{Key: "title", Value: "text"}, {Key: "body", Value: "text"}},
			Options: options.Index().SetName("txt").
				SetWeights(bson.D{{Key: "title", Value: 10}, {Key: "body", Value: 1}}),
		},
	}
	if _, err := cc.Indexes().CreateMany(ctx, want); err != nil {
		t.Fatalf("create archive indexes: %v", err)
	}

	ix := NewIndexer(hot, cold, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := ix.build(ctx, ns); err != nil {
		t.Fatalf("build: %v", err)
	}

	got := indexesByName(t, ctx, hot.Database(db).Collection(coll))
	// The control. If neither index arrived, the checks below pass on nothing.
	for _, name := range []string{"wild", "txt"} {
		if _, ok := got[name]; !ok {
			t.Fatalf("the fast tier has no index named %q, so nothing was copied to compare", name)
		}
	}

	// A wildcard index with no projection indexes every field of every document
	// instead of the two asked for, which is a different index and a much larger
	// one.
	if proj, ok := got["wild"].LookupErr("wildcardProjection"); ok != nil {
		t.Errorf("the wildcard index lost its projection, so it indexes every field: %v", got["wild"])
	} else if len(proj.Document()) == 0 {
		t.Errorf("the wildcard index arrived with an empty projection: %v", proj)
	}

	// Weights decide which field a match counts for more, so a copy without them
	// ranks the same $text search differently from the tier it came from.
	if w, err := got["txt"].LookupErr("weights"); err != nil {
		t.Errorf("the text index lost its weights, so it ranks differently: %v", got["txt"])
	} else {
		if title, terr := w.Document().LookupErr("title"); terr != nil {
			t.Errorf("the text index has no weight for title: %v", w)
		} else if n, ok := title.AsInt64OK(); !ok || n != 10 {
			t.Errorf("title weight is %v, want 10; the two tiers rank the same search differently", title)
		}
	}
}

func connectOrSkip(t *testing.T, ctx context.Context, uri string) *mongo.Client {
	t.Helper()
	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Skipf("connect %s: %v", uri, err)
	}
	t.Cleanup(func() { _ = cl.Disconnect(context.WithoutCancel(ctx)) })
	return cl
}

func indexesByName(t *testing.T, ctx context.Context, c *mongo.Collection) map[string]bson.Raw {
	t.Helper()
	cur, err := c.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	out := map[string]bson.Raw{}
	for cur.Next(ctx) {
		raw := append(bson.Raw(nil), cur.Current...)
		name, err := raw.LookupErr("name")
		if err != nil {
			continue
		}
		out[name.StringValue()] = raw
	}
	if err := cur.Err(); err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	return out
}

// A TTL index is the one the fast tier must not get, and the rewrite that made
// every other option survive is exactly the change that could carry it across.
//
// Creating it there would make the fast tier expire documents, the feeder would
// read those expiries as application deletes, and the archive's copies would be
// tombstoned: an index whose job is to delete data reaching into the tier that
// exists to keep it.
func TestTTLIndexIsStillLeftBehind(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tier URIs to run this")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	hot := connectOrSkip(t, ctx, hotURI)
	cold := connectOrSkip(t, ctx, coldURI)
	const db, coll = "tc_ixttl", "docs"
	t.Cleanup(func() {
		c := context.WithoutCancel(ctx)
		_ = hot.Database(db).Drop(c)
		_ = cold.Database(db).Drop(c)
	})
	if err := hot.Database(db).Drop(ctx); err != nil {
		t.Fatalf("drop hot: %v", err)
	}
	if err := cold.Database(db).Drop(ctx); err != nil {
		t.Fatalf("drop cold: %v", err)
	}

	cc := cold.Database(db).Collection(coll)
	if _, err := cc.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "at", Value: time.Now()}}); err != nil {
		t.Fatalf("seed archive: %v", err)
	}
	if _, err := cc.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "at", Value: 1}}, Options: options.Index().SetName("ttl_at").SetExpireAfterSeconds(3600)},
		{Keys: bson.D{{Key: "other", Value: 1}}, Options: options.Index().SetName("other_1")},
	}); err != nil {
		t.Fatalf("create archive indexes: %v", err)
	}

	ix := NewIndexer(hot, cold, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := ix.build(ctx, db+"."+coll); err != nil {
		t.Fatalf("build: %v", err)
	}

	got := indexesByName(t, ctx, hot.Database(db).Collection(coll))
	// The control: the ordinary index beside it has to arrive, or the check
	// below passes on a build that copied nothing at all.
	if _, ok := got["other_1"]; !ok {
		t.Fatalf("the ordinary index did not arrive, so nothing was copied: %v", names(got))
	}
	if _, ok := got["ttl_at"]; ok {
		t.Errorf("the TTL index was copied to the fast tier, which would expire documents there "+
			"and have the archive tombstone its own copies: %v", names(got))
	}
}
