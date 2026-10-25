// Writes sent as OP_MSG document sequences, which is how a driver sends a bulk
// insert and not how a command document looks.
//
// Every guard in the router reads its statements through wire.AllOf, which
// takes both forms, and there is a unit test for the parsing. What that does
// not say is whether a guard actually fires for the sequenced form end to end:
// a driver that puts the documents in a kind 1 section rather than inline is
// the shape a hand-written test cannot produce by accident and mongosh may
// never send.
//
//	go run ./test/seqwrite "mongodb://dba:secret@127.0.0.1:27100/?authSource=admin" shop tseq [bulk|big]
//
// With bulk it sends the same through client.BulkWrite, the bulkWrite command,
// whose ops and nsInfo a driver sends as two sequences. The router read nsInfo
// from the body only, so a driver's bulkWrite went through every guard
// unchecked while mongosh's, which inlines both, was refused.
//
// It writes an _id the archive holds live. Through the router that must be
// refused as a duplicate key, which is what one cluster would do; accepted is
// the silent clobber of an archived document.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("usage: seqwrite URI DB COLL [bulk]")
		os.Exit(2)
	}
	uri, db, coll := os.Args[1], os.Args[2], os.Args[3]

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer func() { _ = cl.Disconnect(context.Background()) }()
	c := cl.Database(db).Collection(coll)

	// Several documents in one insert, which is what makes the driver use a
	// document sequence rather than an inline array. One of them is the _id the
	// archive holds.
	docs := []any{
		bson.D{{Key: "_id", Value: "seq-new-1"}, {Key: "v", Value: "new"}},
		bson.D{{Key: "_id", Value: "archived"}, {Key: "v", Value: "clobber"}},
		bson.D{{Key: "_id", Value: "seq-new-2"}, {Key: "v", Value: "new"}},
	}
	if len(os.Args) > 4 && os.Args[4] == "big" {
		// About 17MB in one insert, past the 16MB a command body may be and
		// within what a driver puts in one payload sequence. The router
		// rebuilds the batch without the archived _id, and rebuilt as one body
		// mongod refused the whole of it as too large.
		pad := strings.Repeat("x", 1<<20)
		big := []any{bson.D{{Key: "_id", Value: "archived"}, {Key: "v", Value: "clobber"}}}
		for i := 0; i < 17; i++ {
			big = append(big, bson.D{{Key: "_id", Value: fmt.Sprintf("seq-big-%d", i)}, {Key: "pad", Value: pad}})
		}
		_, err := c.InsertMany(ctx, big, options.InsertMany().SetOrdered(false))
		var bwe mongo.BulkWriteException
		if err != nil && asBulk(err, &bwe) && len(bwe.WriteErrors) == 1 && bwe.WriteErrors[0].Code == 11000 {
			fmt.Println("REFUSED the archived one")
			return
		}
		fmt.Println("FAILED:", err)
		return
	}
	if len(os.Args) > 4 && os.Args[4] == "bulk" {
		var models []mongo.ClientBulkWrite
		for _, d := range docs {
			models = append(models, mongo.ClientBulkWrite{Database: db, Collection: coll,
				Model: mongo.NewClientInsertOneModel().SetDocument(d)})
		}
		_, err = cl.BulkWrite(ctx, models, options.ClientBulkWrite().SetOrdered(false))
		if err == nil {
			fmt.Println("ACCEPTED all three")
			return
		}
		var cbe mongo.ClientBulkWriteException
		if errors.As(err, &cbe) {
			for _, we := range cbe.WriteErrors {
				if we.Code == 11000 {
					fmt.Println("REFUSED with a duplicate key, which is what one cluster would do")
					return
				}
			}
		}
		fmt.Println("FAILED for another reason:", err)
		return
	}
	_, err = c.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err == nil {
		fmt.Println("ACCEPTED all three")
		return
	}
	var bwe mongo.BulkWriteException
	dup := false
	if ok := asBulk(err, &bwe); ok {
		for _, we := range bwe.WriteErrors {
			if we.Code == 11000 {
				dup = true
			}
		}
	}
	if dup {
		fmt.Println("REFUSED with a duplicate key, which is what one cluster would do")
		return
	}
	fmt.Println("FAILED for another reason:", err)
}

func asBulk(err error, out *mongo.BulkWriteException) bool {
	if e, ok := err.(mongo.BulkWriteException); ok {
		*out = e
		return true
	}
	return false
}
