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
)

// Pre-image recording is a collection option, and change streams are served
// from the fast tier.
//
// An application that opens a change stream with fullDocumentBeforeChange needs
// the collection it watches to be recording pre-images. In an adoption that
// setting is on the archive's collection, because that is the cluster the
// application has been running against, and the fast tier's copy is created
// here. Created without it, the stream the application has always opened is
// refused, and nothing in the error mentions a tier.
func TestPreImageOptionIsCarried(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tier URIs to run this")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	hot := connectOrSkip(t, ctx, hotURI)
	cold := connectOrSkip(t, ctx, coldURI)
	const db, coll = "tc_preimg", "docs"
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

	if err := cold.Database(db).RunCommand(ctx, bson.D{
		{Key: "create", Value: coll},
		{Key: "changeStreamPreAndPostImages", Value: bson.D{{Key: "enabled", Value: true}}},
	}).Err(); err != nil {
		t.Skipf("this server does not record pre-images: %v", err)
	}

	ix := NewIndexer(hot, cold, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := ix.build(ctx, db+"."+coll); err != nil {
		t.Fatalf("build: %v", err)
	}

	opts := collectionOptions(t, ctx, hot, db, coll)
	// The control: the collection has to exist, or the check below is asking
	// about nothing.
	if opts == nil {
		t.Fatal("the fast tier has no such collection, so nothing was created to inspect")
	}
	v, err := opts.LookupErr("changeStreamPreAndPostImages", "enabled")
	if err != nil {
		t.Fatalf("the fast tier's copy does not record pre-images, so a change stream asking for "+
			"fullDocumentBeforeChange is refused where the archive would have served it: %s", opts.String())
	}
	if on, ok := v.BooleanOK(); !ok || !on {
		t.Errorf("pre-image recording arrived as %v, want true", v)
	}
}

func collectionOptions(t *testing.T, ctx context.Context, cl *mongo.Client, db, coll string) bson.Raw {
	t.Helper()
	cur, err := cl.Database(db).ListCollections(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		t.Fatalf("list %s.%s: %v", db, coll, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	if !cur.Next(ctx) {
		return nil
	}
	doc, ok := cur.Current.Lookup("options").DocumentOK()
	if !ok {
		return bson.Raw{}
	}
	return doc
}

// Carrying every option except a named few puts the weight on that list, so the
// one whose absence would lose data gets its own check.
//
// A capped fast tier removes its own oldest documents to stay inside its size.
// The feeder reads those removals as the application deleting them and the
// archive tombstones the history it exists to keep, so an adopted capped
// collection would quietly destroy itself through the tier meant to protect it.
func TestCappedIsNotCarried(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tier URIs to run this")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	hot := connectOrSkip(t, ctx, hotURI)
	cold := connectOrSkip(t, ctx, coldURI)
	const db, coll = "tc_capped", "docs"
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

	// Capped, and carrying a validator beside it so the check can tell "nothing
	// was carried" from "the right thing was left behind".
	if err := cold.Database(db).RunCommand(ctx, bson.D{
		{Key: "create", Value: coll},
		{Key: "capped", Value: true},
		{Key: "size", Value: 4096},
		{Key: "validator", Value: bson.D{{Key: "n", Value: bson.D{{Key: "$type", Value: "int"}}}}},
	}).Err(); err != nil {
		t.Fatalf("create capped archive collection: %v", err)
	}

	ix := NewIndexer(hot, cold, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := ix.build(ctx, db+"."+coll); err != nil {
		t.Fatalf("build: %v", err)
	}

	opts := collectionOptions(t, ctx, hot, db, coll)
	if opts == nil {
		t.Fatal("the fast tier has no such collection, so nothing was created to inspect")
	}
	// The control. Without it a copy that carried nothing at all would pass.
	if _, err := opts.LookupErr("validator"); err != nil {
		t.Fatalf("the validator was not carried either, so this proves nothing about capped: %s", opts.String())
	}
	if v, err := opts.LookupErr("capped"); err == nil {
		if on, ok := v.BooleanOK(); ok && on {
			t.Errorf("the fast tier's copy is capped, so it will remove its own oldest documents "+
				"and the archive will tombstone them: %s", opts.String())
		}
	}
	if _, err := opts.LookupErr("size"); err == nil {
		t.Errorf("the fast tier's copy carries a size cap: %s", opts.String())
	}
}
