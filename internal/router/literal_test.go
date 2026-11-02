package router

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

// A partial unique index is judged by passing the document through $documents,
// which reads its argument as an expression. Read that way, {active: "$flag",
// flag: true} has active true and is inside an index on {active: true}; read as
// what it is, active is the string "$flag" and the document is outside it, as
// mongod's own index says. The answer decides whether the router refuses a
// write with E11000 that mongod would take.
func TestPartialIndexIsJudgedOnTheLiteralDocument(t *testing.T) {
	hotURI := os.Getenv("TC_TEST_HOT")
	if hotURI == "" {
		t.Skip("set TC_TEST_HOT to a fast tier to run the partial index test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hot, err := mongo.Connect(options.Client().ApplyURI(hotURI))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hot.Disconnect(context.Background()) }()

	r := &Router{hot: hot, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	doc := func(d bson.D) bson.Raw {
		raw, err := bson.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	idx := uniqueIndex{name: "email_1", fields: []string{"email"},
		partial: doc(bson.D{{Key: "active", Value: true}})}

	forged := doc(bson.D{{Key: "_id", Value: 1}, {Key: "email", Value: "a@x"},
		{Key: "active", Value: "$flag"}, {Key: "flag", Value: true}})
	if r.constrains(ctx, "tc_literal", idx, forged) {
		t.Error("a document whose active is the string \"$flag\" was judged inside an index on {active: true}")
	}
	if got := r.constrainsBatch(ctx, "tc_literal", idx, []bson.Raw{forged}); len(got) != 1 || got[0] {
		t.Errorf("the batch form judged the same document inside the index: %v", got)
	}

	real := doc(bson.D{{Key: "_id", Value: 2}, {Key: "email", Value: "b@x"}, {Key: "active", Value: true}})
	if !r.constrains(ctx, "tc_literal", idx, real) {
		t.Error("a document with active true was judged outside the index")
	}
}
