// Hot/cold correctness under load, judged against a twin.
//
// Every write a client makes to a tiered collection through the router is made
// the same way to a twin collection on a plain MongoDB, and every read is asked
// of both. Each client has its own pair and nobody else writes either, so the
// two answers must be equal, byte for byte: a difference is the router answering
// something MongoDB would not. Meanwhile the evictor moves aged documents to the
// archive, so the reads cross tiers, writes reach evicted documents and copy them
// up, and deletes of evicted documents become tombstones.
//
// It is the same idea as test/cachestress, an exact oracle, aimed at tiering:
// the documents are dated over the last two days and the rule ages them out at
// one, so about half of every collection lives in the archive alone at any time,
// and writes keep moving documents across the line in both directions.
//
//	go run ./test/tierstress -phase setup -uri ROUTER -twin TWIN -cold ARCHIVE
//	cline watermark --ns tier.items_N --complete-all   (each N), then start the evictor
//	go run ./test/tierstress -phase run -uri ROUTER -twin TWIN -for 5m
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	uri := flag.String("uri", "", "the router")
	twinURI := flag.String("twin", "", "a plain MongoDB for the twin collections")
	coldURI := flag.String("cold", "", "the archive, to clear it at setup")
	phase := flag.String("phase", "run", "setup or run")
	db := flag.String("db", "tier", "the tiered database")
	twinDB := flag.String("twindb", "tiertwin", "the twin database")
	coll := flag.String("coll", "items", "collection name prefix")
	workers := flag.Int("workers", 8, "clients, each with its own collection")
	load := flag.Int("load", 400, "documents per collection at setup")
	groups := flag.Int("groups", 6, "groups per collection")
	dur := flag.Duration("for", 5*time.Minute, "how long to run")
	writes := flag.Float64("writes", 0.35, "fraction of operations that write")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()
	if *uri == "" || *twinURI == "" {
		fmt.Fprintln(os.Stderr, "need -uri and -twin")
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
	rt, tw := connect(*uri), connect(*twinURI)
	defer func() { _ = rt.Disconnect(ctx); _ = tw.Disconnect(ctx) }()

	now := time.Now()
	mk := func(r *rand.Rand, id int64) bson.D {
		return bson.D{
			{Key: "_id", Value: id},
			{Key: "g", Value: int32(r.Intn(*groups))},
			{Key: "price", Value: int32(r.Intn(21))},
			// Over the last two days; the rule evicts what is older than one.
			{Key: "at", Value: now.Add(-time.Duration(r.Int63n(int64(48 * time.Hour)))).Truncate(time.Millisecond)},
			{Key: "status", Value: []string{"a", "b", "c"}[r.Intn(3)]},
			{Key: "qty", Value: int32(r.Intn(100))},
		}
	}
	name := func(w int) string { return fmt.Sprintf("%s_%d", *coll, w) }

	if *phase == "setup" {
		var cold *mongo.Client
		if *coldURI != "" {
			cold = connect(*coldURI)
		}
		r := rand.New(rand.NewSource(*seed))
		for w := 0; w < *workers; w++ {
			// Both tiers and the twin: dropping through the router leaves the
			// archive's copy, which the next run would read as history.
			_ = rt.Database(*db).Collection(name(w)).Drop(ctx)
			if cold != nil {
				_ = cold.Database(*db).Collection(name(w)).Drop(ctx)
			}
			_ = tw.Database(*twinDB).Collection(name(w)).Drop(ctx)
			docs := make([]any, 0, *load)
			for i := 1; i <= *load; i++ {
				docs = append(docs, mk(r, int64(i)))
			}
			for _, c := range []*mongo.Collection{rt.Database(*db).Collection(name(w)), tw.Database(*twinDB).Collection(name(w))} {
				if _, err := c.InsertMany(ctx, docs); err != nil {
					fmt.Fprintln(os.Stderr, "load:", err)
					os.Exit(1)
				}
				if _, err := c.Indexes().CreateMany(ctx, []mongo.IndexModel{
					{Keys: bson.D{{Key: "g", Value: 1}, {Key: "price", Value: 1}}},
					{Keys: bson.D{{Key: "at", Value: 1}}},
				}); err != nil {
					fmt.Fprintln(os.Stderr, "indexes:", err)
					os.Exit(1)
				}
			}
		}
		fmt.Printf("setup: %d collections of %d documents, and their twins\n", *workers, *load)
		return
	}

	var reads, wrote, differ, failed atomic.Int64
	var mu sync.Mutex
	why := map[string]int{}
	samples := []string{}
	note := func(k, sample string) {
		mu.Lock()
		defer mu.Unlock()
		why[k]++
		if len(samples) < 10 {
			samples = append(samples, sample)
		}
	}
	stop := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(*seed*1000 + int64(w)))
			a := rt.Database(*db).Collection(name(w))
			b := tw.Database(*twinDB).Collection(name(w))
			next := int64(*load)
			last := map[int64]string{}
			for time.Now().Before(stop) {
				g := int32(r.Intn(*groups))
				if r.Float64() < *writes {
					wrote.Add(1)
					id := 1 + r.Int63n(next)
					op := r.Intn(8)
					var newDoc bson.D
					if op <= 1 {
						next++
						newDoc = mk(r, next)
					} else if op == 6 {
						newDoc = mk(r, id)
					}
					var ea, eb error
					var na, nb string
					// What each side says it did, not only whether it failed: a
					// delete that touched nothing is no error.
					both := func(f func(c *mongo.Collection) (string, error)) { na, ea = f(a); nb, eb = f(b) }
					switch op {
					case 0, 1:
						both(func(c *mongo.Collection) (string, error) { _, err := c.InsertOne(ctx, newDoc); return "", err })
					case 2:
						both(func(c *mongo.Collection) (string, error) {
							res, err := c.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
							if err != nil {
								return "", err
							}
							return fmt.Sprintf("deleted %d", res.DeletedCount), nil
						})
					case 3:
						p := int32(r.Intn(21))
						both(func(c *mongo.Collection) (string, error) {
							return counted(c.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$set", Value: bson.D{{Key: "price", Value: p}}}}))
						})
					case 4:
						both(func(c *mongo.Collection) (string, error) {
							return counted(c.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
								bson.D{{Key: "$set", Value: bson.D{{Key: "g", Value: g}}}, {Key: "$inc", Value: bson.D{{Key: "qty", Value: 1}}}}))
						})
					case 5:
						p := int32(r.Intn(21))
						both(func(c *mongo.Collection) (string, error) {
							return counted(c.UpdateMany(ctx, bson.D{{Key: "g", Value: g}, {Key: "status", Value: "a"}, {Key: "price", Value: p}},
								bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "b"}}}}))
						})
					case 6:
						both(func(c *mongo.Collection) (string, error) {
							return counted(c.ReplaceOne(ctx, bson.D{{Key: "_id", Value: id}}, newDoc))
						})
					case 7:
						// Brings an aged document back to now, across the line.
						at := time.Now().Truncate(time.Millisecond)
						both(func(c *mongo.Collection) (string, error) {
							return counted(c.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$set", Value: bson.D{{Key: "at", Value: at}}}}))
						})
					}
					last[id] = fmt.Sprintf("op%d", op)
					if (ea == nil) != (eb == nil) || na != nb {
						differ.Add(1)
						note(fmt.Sprintf("write op%d: router %q %v, mongod %q %v", op, na, ea, nb, eb),
							fmt.Sprintf("%s op%d id %d in %s: router %q %v, mongod %q %v", time.Now().Format("15:04:05.000"), op, id, name(w), na, ea, nb, eb))
					} else if ea != nil {
						failed.Add(1)
					}
					continue
				}
				reads.Add(1)
				kind, qa, qb, err := ask(ctx, a, b, r, g, next)
				if err != nil {
					// Counted as a difference: the workload asks nothing mongod refuses,
					// so a failed read is a finding whichever side failed.
					differ.Add(1)
					note("read error: "+kind+": "+err.Error(), "")
					continue
				}
				if d := diff(qa, qb); d != "" {
					differ.Add(1)
					hist := ""
					for k, v := range last {
						if bytes.Contains([]byte(d), []byte(fmt.Sprintf(`{"$numberLong":"%d"}`, k))) {
							hist += fmt.Sprintf(" last write to %d: %s;", k, v)
						}
					}
					note(kind+": "+trimNums(d), fmt.Sprintf("%s in %s: %s%s", kind, name(w), d, hist))
				}
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("reads %d, writes %d, answers that differed %d, errors on both sides %d\n",
		reads.Load(), wrote.Load(), differ.Load(), failed.Load())
	keys := make([]string, 0, len(why))
	for k := range why {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return why[keys[i]] > why[keys[j]] })
	for _, k := range keys {
		fmt.Printf("  x%d %.160s\n", why[k], k)
	}
	for _, s := range samples {
		if s != "" {
			fmt.Println("  e.g.", s)
		}
	}
	if differ.Load() > 0 {
		os.Exit(1)
	}
}

// ask runs one read on the tiered collection and on its twin. Every list read
// is sorted to the end, by _id last, so the two must be in the same order.
func ask(ctx context.Context, a, b *mongo.Collection, r *rand.Rand, g int32, next int64) (string, []bson.Raw, []bson.Raw, error) {
	type q struct {
		kind   string
		filter bson.D
		opts   func() *options.FindOptionsBuilder
	}
	var pick q
	switch r.Intn(9) {
	case 0:
		pick = q{"point", bson.D{{Key: "_id", Value: 1 + r.Int63n(next)}}, func() *options.FindOptionsBuilder { return options.Find() }}
	case 1:
		pick = q{"by price", bson.D{{Key: "g", Value: g}}, func() *options.FindOptionsBuilder {
			return options.Find().SetSort(bson.D{{Key: "price", Value: 1}, {Key: "_id", Value: 1}}).SetLimit(10)
		}}
	case 2:
		pick = q{"newest", bson.D{{Key: "g", Value: g}}, func() *options.FindOptionsBuilder {
			return options.Find().SetSort(bson.D{{Key: "at", Value: -1}, {Key: "_id", Value: 1}}).SetLimit(5)
		}}
	case 3:
		lo := int32(r.Intn(15))
		pick = q{"price range", bson.D{{Key: "g", Value: g}, {Key: "price", Value: bson.D{{Key: "$gte", Value: lo}, {Key: "$lt", Value: lo + 6}}}},
			func() *options.FindOptionsBuilder {
				return options.Find().SetSort(bson.D{{Key: "price", Value: -1}, {Key: "_id", Value: 1}}).SetLimit(20)
			}}
	case 4:
		pick = q{"whole group", bson.D{{Key: "g", Value: g}}, func() *options.FindOptionsBuilder {
			return options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
		}}
	case 5:
		// Across the line between the tiers, the newest first and the oldest.
		since := time.Now().Add(-time.Duration(12+r.Intn(24)) * time.Hour)
		pick = q{"since", bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: since}}}}, func() *options.FindOptionsBuilder {
			return options.Find().SetSort(bson.D{{Key: "at", Value: 1}, {Key: "_id", Value: 1}}).SetLimit(50)
		}}
	case 6:
		return countBoth(ctx, a, b, bson.D{{Key: "g", Value: g}})
	case 7:
		return countBoth(ctx, a, b, bson.D{{Key: "g", Value: g}, {Key: "status", Value: "a"}})
	default:
		pipe := bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "g", Value: g}}}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$status"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}, {Key: "q", Value: bson.D{{Key: "$sum", Value: "$qty"}}}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
		}
		ra, err := all(ctx, func() (*mongo.Cursor, error) { return a.Aggregate(ctx, pipe) })
		if err != nil {
			return "group", nil, nil, err
		}
		rb, err := all(ctx, func() (*mongo.Cursor, error) { return b.Aggregate(ctx, pipe) })
		return "group", ra, rb, err
	}
	ra, err := all(ctx, func() (*mongo.Cursor, error) { return a.Find(ctx, pick.filter, pick.opts()) })
	if err != nil {
		return pick.kind, nil, nil, err
	}
	rb, err := all(ctx, func() (*mongo.Cursor, error) { return b.Find(ctx, pick.filter, pick.opts()) })
	return pick.kind, ra, rb, err
}

func countBoth(ctx context.Context, a, b *mongo.Collection, f bson.D) (string, []bson.Raw, []bson.Raw, error) {
	na, err := a.CountDocuments(ctx, f)
	if err != nil {
		return "count", nil, nil, err
	}
	nb, err := b.CountDocuments(ctx, f)
	if err != nil {
		return "count", nil, nil, err
	}
	x, _ := bson.Marshal(bson.D{{Key: "n", Value: na}})
	y, _ := bson.Marshal(bson.D{{Key: "n", Value: nb}})
	return "count", []bson.Raw{x}, []bson.Raw{y}, nil
}

func all(ctx context.Context, open func() (*mongo.Cursor, error)) ([]bson.Raw, error) {
	cur, err := open()
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()
	var out []bson.Raw
	for cur.Next(ctx) {
		out = append(out, append(bson.Raw(nil), cur.Current...))
	}
	if err := cur.Err(); err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, err
	}
	return out, nil
}

func diff(a, b []bson.Raw) string {
	if len(a) != len(b) {
		ids := func(x []bson.Raw) map[string]bool {
			m := map[string]bool{}
			for _, d := range x {
				m[d.Lookup("_id").String()] = true
			}
			return m
		}
		ia, ib := ids(a), ids(b)
		var extra, missing []string
		for k := range ia {
			if !ib[k] {
				extra = append(extra, k)
			}
		}
		for k := range ib {
			if !ia[k] {
				missing = append(missing, k)
			}
		}
		return fmt.Sprintf("%d documents, mongod %d; router only %v, mongod only %v", len(a), len(b), extra, missing)
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return fmt.Sprintf("document %d", i)
		}
	}
	return ""
}

// trimNums drops the ids from a difference, so differences of one kind count
// together.
func trimNums(s string) string {
	if i := bytes.Index([]byte(s), []byte("; router only")); i >= 0 {
		return s[:i]
	}
	return s
}

// counted is what an update says it did.
func counted(res *mongo.UpdateResult, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("matched %d modified %d upserted %d", res.MatchedCount, res.ModifiedCount, res.UpsertedCount), nil
}
