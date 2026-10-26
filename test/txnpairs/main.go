// Whether the live cache shows a transaction whole or not at all.
//
// Writers commit pairs of documents straight to the fast tier, each pair in one
// transaction or one insertMany (which 8.0 writes as one batched entry), and
// later delete both halves the same way. Every event of a pair shares one
// cluster time. Readers ask the router for a group's documents and for its
// count, and both are always even on mongod: no read of mongod returns half of
// a pair. An odd answer is the cache answering between the two events of one
// commit, a state the database never had.
//
// mongod itself fails this unless its reads cannot yield: a find or count with
// readConcern local yields every 10ms and resumes on a newer snapshot, and so
// saw one half of a deleted pair, 9 odd answers in 427,306 reads made straight
// to the fast tier. Set internalQueryExecYieldIterations and
// internalQueryExecYieldPeriodMS to 100000000 on every fast tier node first,
// check a run with -uri pointing at the fast tier gives 0, and set them back
// to -1 and 10 after. Measured that way on 8.0: a router without the cache 0
// in 588,956; the cache before b02d9cc 240 in 619,055; after it 0 in 591,299.
//
// The writers bypass the router on purpose. A commit through the router holds
// every namespace until the stream applies it (NoteWriteAll), so the window is
// only open for writes the router did not see: another router, or an
// application writing to the fast tier directly.
//
//	go run ./test/txnpairs -uri ROUTER -direct HOT -for 2m
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	uri := flag.String("uri", "", "the router the readers ask")
	direct := flag.String("direct", "", "the fast tier, which the writers write to")
	db := flag.String("db", "stress", "database")
	coll := flag.String("coll", "pairs", "collection")
	groups := flag.Int("groups", 4, "groups the pairs fall into")
	readers := flag.Int("readers", 12, "concurrent readers")
	writers := flag.Int("writers", 4, "concurrent writers")
	dur := flag.Duration("for", 2*time.Minute, "how long to run")
	how := flag.String("how", "both", "how a pair is committed: txn, batch (one insertMany) or both")
	k := flag.Int("k", 2, "documents per commit; every answer must hold a multiple of it")
	pad := flag.Int("pad", 0, "bytes of padding per document; 200 documents of 100KB fill more than one 16MB batch")
	flag.Parse()
	if *uri == "" || *direct == "" {
		fmt.Fprintln(os.Stderr, "need -uri and -direct")
		os.Exit(2)
	}
	ctx := context.Background()
	connect := func(u string) *mongo.Client {
		cl, err := mongo.Connect(options.Client().ApplyURI(u))
		if err != nil {
			fmt.Fprintln(os.Stderr, "connect:", err)
			os.Exit(1)
		}
		return cl
	}
	rt, hot := connect(*uri), connect(*direct)
	defer func() { _ = rt.Disconnect(ctx); _ = hot.Disconnect(ctx) }()
	w := hot.Database(*db).Collection(*coll)
	_ = w.Drop(ctx)
	if _, err := w.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "g", Value: 1}, {Key: "_id", Value: 1}}}); err != nil {
		fmt.Fprintln(os.Stderr, "index:", err)
		os.Exit(1)
	}
	r := rt.Database(*db).Collection(*coll)

	var pairs, txns, batches, reads, odd, failed atomic.Int64
	var mu sync.Mutex
	samples := []string{}
	fail := func(what string, err error) {
		failed.Add(1)
		mu.Lock()
		if len(samples) < 8 {
			samples = append(samples, what+": "+err.Error())
		}
		mu.Unlock()
	}
	stop := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for i := 0; i < *writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(i)))
			live := []int64{}
			next := int64(i) << 40
			for time.Now().Before(stop) {
				useTxn := *how == "txn" || (*how == "both" && rnd.Intn(2) == 0)
				if len(live) > 20 || (len(live) > 0 && rnd.Intn(3) == 0) {
					at := rnd.Intn(len(live))
					a := live[at]
					live = append(live[:at], live[at+1:]...)
					ids := bson.A{}
					for j := 0; j < *k; j++ {
						ids = append(ids, a+int64(j))
					}
					filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}
					var err error
					if useTxn {
						err = inTxn(ctx, hot, func(sc context.Context) error { _, e := w.DeleteMany(sc, filter); return e })
					} else {
						// The same pair, one statement per document.
						err = inTxn(ctx, hot, func(sc context.Context) error {
							for _, id := range ids {
								if _, e := w.DeleteOne(sc, bson.D{{Key: "_id", Value: id}}); e != nil {
									return e
								}
							}
							return nil
						})
					}
					if err != nil {
						fail("delete", err)
					}
					continue
				}
				g := int32(rnd.Intn(*groups))
				a := next
				next += int64(*k)
				docs := make([]any, 0, *k)
				for j := 0; j < *k; j++ {
					d := bson.D{{Key: "_id", Value: a + int64(j)}, {Key: "g", Value: g}}
					if *pad > 0 {
						d = append(d, bson.E{Key: "pad", Value: strings.Repeat("x", *pad)})
					}
					docs = append(docs, d)
				}
				var err error
				if useTxn {
					txns.Add(1)
					err = inTxn(ctx, hot, func(sc context.Context) error { _, e := w.InsertMany(sc, docs); return e })
				} else {
					batches.Add(1)
					_, err = w.InsertMany(ctx, docs)
				}
				if err != nil {
					fail("insert", err)
					continue
				}
				pairs.Add(1)
				live = append(live, a)
			}
		}(i)
	}
	for i := 0; i < *readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(1000 + int64(i)))
			for time.Now().Before(stop) {
				g := int32(rnd.Intn(*groups))
				reads.Add(1)
				var n int64
				var what string
				if rnd.Intn(2) == 0 {
					what = "find"
					// One batch, without the padding: a getMore is not read at
					// the first batch's snapshot, so mongod itself can answer a
					// find split across batches with half a commit.
					cur, err := r.Find(ctx, bson.D{{Key: "g", Value: g}}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).
						SetLimit(1000).SetBatchSize(1000).SetProjection(bson.D{{Key: "pad", Value: 0}}))
					if err != nil {
						fail("find", err)
						continue
					}
					for cur.Next(ctx) {
						n++
					}
					if err := cur.Err(); err != nil {
						fail("find", err)
					}
					_ = cur.Close(ctx)
				} else {
					what = "count"
					c, err := r.CountDocuments(ctx, bson.D{{Key: "g", Value: g}})
					if err != nil {
						fail("count", err)
						continue
					}
					n = c
				}
				if n%int64(*k) != 0 {
					odd.Add(1)
					mu.Lock()
					if len(samples) < 8 {
						samples = append(samples, fmt.Sprintf("%s %s of group %d: %d documents", time.Now().Format("15:04:05.000"), what, g, n))
					}
					mu.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("pairs %d (%d in a transaction, %d in one insertMany), reads %d, odd answers %d, errors %d\n",
		pairs.Load(), txns.Load(), batches.Load(), reads.Load(), odd.Load(), failed.Load())
	for _, s := range samples {
		fmt.Println("  e.g.", s)
	}
	if odd.Load() > 0 || failed.Load() > 0 {
		os.Exit(1)
	}
}

func inTxn(ctx context.Context, cl *mongo.Client, f func(context.Context) error) error {
	s, err := cl.StartSession()
	if err != nil {
		return err
	}
	defer s.EndSession(ctx)
	_, err = s.WithTransaction(ctx, func(sc context.Context) (any, error) { return nil, f(sc) })
	return err
}
