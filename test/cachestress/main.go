// A workload aimed at where the live cache can be wrong, for its verify mode.
//
// The shop workload compared 3.5 million cached answers with MongoDB and found
// none different, and a mutant that never put an insert into a cached list was
// caught once in 588,664: the shop has 250,000 customers and reads each one's
// orders rarely, so a list was almost never read right after something was
// inserted into it. A clean run said little about inserts, deletes, or moves
// between lists.
//
// This one makes those the common case. A few groups, read constantly, written
// constantly: inserts and deletes into the lists being read, price changes that
// move documents within a sorted list and onto prices that tie, group changes
// that move them between lists, counts and groupings over the same documents.
// Every shape the cache takes is asked for. Run it through a router whose
// live_cache.verify_log is set: the router compares every answer the cache
// could give with MongoDB read at the cache's own position, and the verify log
// is the result. This program only makes the traffic.
//
//	go run ./test/cachestress -uri "mongodb://dba:secret@127.0.0.1:27800/?authSource=admin" -for 5m
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	uri := flag.String("uri", "", "the router to send everything through")
	db := flag.String("db", "stress", "database")
	coll := flag.String("coll", "items", "collection")
	groups := flag.Int("groups", 8, "how many groups the documents fall into; fewer is hotter")
	initial := flag.Int("load", 2000, "documents loaded before the run")
	workers := flag.Int("workers", 16, "concurrent clients")
	dur := flag.Duration("for", 5*time.Minute, "how long to run")
	writes := flag.Float64("writes", 0.4, "fraction of operations that write")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()
	if *uri == "" {
		fmt.Fprintln(os.Stderr, "need -uri")
		os.Exit(2)
	}

	ctx := context.Background()
	cl, err := mongo.Connect(options.Client().ApplyURI(*uri))
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer func() { _ = cl.Disconnect(ctx) }()
	c := cl.Database(*db).Collection(*coll)
	_ = c.Drop(ctx)

	var next atomic.Int64
	base := time.Now().Add(-24 * time.Hour)
	mk := func(r *rand.Rand, id int64) bson.D {
		return bson.D{
			{Key: "_id", Value: id},
			{Key: "g", Value: int32(r.Intn(*groups))},
			{Key: "cat", Value: fmt.Sprintf("c%d", r.Intn(4))},
			// Few prices, so documents tie on the sort field all the time.
			{Key: "price", Value: int32(r.Intn(21))},
			{Key: "at", Value: base.Add(time.Duration(r.Int63n(int64(24 * time.Hour))))},
			{Key: "status", Value: []string{"a", "b", "c"}[r.Intn(3)]},
			{Key: "qty", Value: int32(r.Intn(100))},
		}
	}
	r0 := rand.New(rand.NewSource(*seed))
	docs := make([]any, 0, *initial)
	for i := 0; i < *initial; i++ {
		docs = append(docs, mk(r0, next.Add(1)))
	}
	if _, err := c.InsertMany(ctx, docs); err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
	if _, err := c.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "g", Value: 1}, {Key: "price", Value: 1}}},
		{Keys: bson.D{{Key: "g", Value: 1}, {Key: "cat", Value: 1}, {Key: "at", Value: -1}}},
	}); err != nil {
		fmt.Fprintln(os.Stderr, "indexes:", err)
		os.Exit(1)
	}

	var reads, wrote, failed atomic.Int64
	var errMu sync.Mutex
	errSeen := map[string]int{}
	stop := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(*seed*1000 + int64(w)))
			for time.Now().Before(stop) {
				g := int32(r.Intn(*groups))
				var err error
				if r.Float64() < *writes {
					wrote.Add(1)
					id := 1 + r.Int63n(next.Load())
					switch r.Intn(7) {
					case 0, 1:
						_, err = c.InsertOne(ctx, mk(r, next.Add(1)))
					case 2:
						_, err = c.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
					case 3:
						_, err = c.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
							bson.D{{Key: "$set", Value: bson.D{{Key: "price", Value: int32(r.Intn(21))}}}})
					case 4:
						_, err = c.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
							bson.D{{Key: "$set", Value: bson.D{{Key: "g", Value: g}}}, {Key: "$inc", Value: bson.D{{Key: "qty", Value: 1}}}})
					case 5:
						_, err = c.UpdateMany(ctx, bson.D{{Key: "g", Value: g}, {Key: "status", Value: "a"}, {Key: "price", Value: int32(r.Intn(21))}},
							bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "b"}}}})
					case 6:
						_, err = c.ReplaceOne(ctx, bson.D{{Key: "_id", Value: id}}, mk(r, id))
					}
				} else {
					reads.Add(1)
					err = read(ctx, c, r, g)
				}
				if err != nil {
					failed.Add(1)
					msg := err.Error()
					if len(msg) > 120 {
						msg = msg[:120]
					}
					errMu.Lock()
					errSeen[msg]++
					errMu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	secs := dur.Seconds()
	fmt.Printf("reads %d (%.0f/s), writes %d (%.0f/s), errors %d, documents inserted %d\n",
		reads.Load(), float64(reads.Load())/secs, wrote.Load(), float64(wrote.Load())/secs, failed.Load(), next.Load())
	for msg, n := range errSeen {
		fmt.Printf("  error x%d: %s\n", n, msg)
	}
}

// read asks one of the shapes the cache takes, about group g.
func read(ctx context.Context, c *mongo.Collection, r *rand.Rand, g int32) error {
	drain := func(cur *mongo.Cursor, err error) error {
		if err != nil {
			return err
		}
		defer func() { _ = cur.Close(ctx) }()
		for cur.Next(ctx) {
		}
		return cur.Err()
	}
	switch r.Intn(10) {
	case 0: // sorted by a field that ties
		return drain(c.Find(ctx, bson.D{{Key: "g", Value: g}}, options.Find().SetSort(bson.D{{Key: "price", Value: 1}}).SetLimit(10)))
	case 1: // sorted to the end
		return drain(c.Find(ctx, bson.D{{Key: "g", Value: g}}, options.Find().SetSort(bson.D{{Key: "price", Value: 1}, {Key: "_id", Value: 1}}).SetLimit(10)))
	case 2: // unsorted
		return drain(c.Find(ctx, bson.D{{Key: "g", Value: g}}, options.Find().SetLimit(5)))
	case 3: // two equalities, newest first, a projection
		return drain(c.Find(ctx, bson.D{{Key: "g", Value: g}, {Key: "cat", Value: fmt.Sprintf("c%d", r.Intn(4))}},
			options.Find().SetSort(bson.D{{Key: "at", Value: -1}}).SetLimit(5).SetProjection(bson.D{{Key: "qty", Value: 0}})))
	case 4: // a range
		lo := int32(r.Intn(15))
		return drain(c.Find(ctx, bson.D{{Key: "g", Value: g}, {Key: "price", Value: bson.D{{Key: "$gte", Value: lo}, {Key: "$lt", Value: lo + 6}}}},
			options.Find().SetSort(bson.D{{Key: "price", Value: -1}, {Key: "_id", Value: 1}}).SetLimit(20)))
	case 5: // count
		_, err := c.Database().RunCommand(ctx, bson.D{{Key: "count", Value: c.Name()}, {Key: "query", Value: bson.D{{Key: "g", Value: g}}}}).Raw()
		return err
	case 6: // countDocuments, which is a pipeline
		_, err := c.CountDocuments(ctx, bson.D{{Key: "g", Value: g}, {Key: "status", Value: "a"}})
		return err
	case 7: // a grouping
		return drain(c.Aggregate(ctx, bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "g", Value: g}}}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$status"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}, {Key: "q", Value: bson.D{{Key: "$sum", Value: "$qty"}}}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
		}))
	default: // a point read, of an _id that may have been deleted, which is an answer
		err := c.FindOne(ctx, bson.D{{Key: "_id", Value: 1 + r.Int63n(3000)}}).Err()
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil
		}
		return err
	}
}
