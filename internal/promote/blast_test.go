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

// One index that cannot be built must not cost the others.
//
// createIndexes is one command for the whole batch and MongoDB refuses the
// batch for one bad member. Measured before this was handled: an adopted
// collection with a text index, a wildcard index and an ordinary index on
// {a: 1} ended up holding _id_ and nothing else, because the text index was
// sent without its weights and the other two were refused with it.
//
// The unrepresentable index here is a unique one the fast tier's own documents
// violate, which is the case that survives a correct copy: the archive holds
// the values that make it unique and the fast tier holds a subset that does
// not, so it can only be discovered by trying.
func TestOneBadIndexDoesNotCostTheRest(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tier URIs to run this")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	hot := connectOrSkip(t, ctx, hotURI)
	cold := connectOrSkip(t, ctx, coldURI)
	const db, coll = "tc_ixbad", "docs"
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

	// The archive, where the values are unique.
	if _, err := cold.Database(db).Collection(coll).InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: 1}, {Key: "code", Value: "a"}, {Key: "n", Value: 1}},
		bson.D{{Key: "_id", Value: 2}, {Key: "code", Value: "b"}, {Key: "n", Value: 2}},
	}); err != nil {
		t.Fatalf("seed archive: %v", err)
	}
	if _, err := cold.Database(db).Collection(coll).Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "code", Value: 1}}, Options: options.Index().SetName("code_u").SetUnique(true)},
		{Keys: bson.D{{Key: "n", Value: 1}}, Options: options.Index().SetName("n_1")},
	}); err != nil {
		t.Fatalf("create archive indexes: %v", err)
	}

	// And the fast tier, where they are not, so the unique one cannot be built.
	if _, err := hot.Database(db).Collection(coll).InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: 3}, {Key: "code", Value: "dup"}, {Key: "n", Value: 3}},
		bson.D{{Key: "_id", Value: 4}, {Key: "code", Value: "dup"}, {Key: "n", Value: 4}},
	}); err != nil {
		t.Fatalf("seed fast tier: %v", err)
	}

	ix := NewIndexer(hot, cold, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ix.build(ctx, db+"."+coll)
	if err == nil {
		t.Fatal("the build reported success while a unique index the fast tier's own documents " +
			"violate was in it, so nothing here is being tested")
	}

	got := indexesByName(t, ctx, hot.Database(db).Collection(coll))
	if _, ok := got["n_1"]; !ok {
		t.Errorf("the index that could be built is missing, so one bad index cost the rest: %v", names(got))
	}
	if _, ok := got["code_u"]; ok {
		t.Errorf("the unique index was built over duplicate values, which cannot be right: %v", names(got))
	}
}

func names(m map[string]bson.Raw) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
