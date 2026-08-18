// A workload with the shape that decides whether tiering is worth anything.
//
// Every number this project has published so far comes from suites that prove
// correctness: they read ten documents and check the answer. None of them says
// what happens when an application with a realistic access pattern runs against
// two tiers, and that is the only question a decision to deploy this actually
// turns on.
//
// The shape that matters is age. Real reads are not uniform over a collection's
// history: most of them touch the last few days, a few reach back months, and
// almost none touch the oldest records, which is exactly why keeping those on
// expensive storage is waste. This models that with a power law over document
// age, loads a collection whose documents are spread over a period, and then
// reports what fraction of reads the fast tier could serve for a given hot
// window, and what the rest cost.
//
// It is still synthetic data. What it is not is a synthetic access pattern: the
// distribution is the one parameter that decides the answer, so it is a
// parameter here rather than an assumption. Point -recency at what a real
// workload does and the numbers follow.
//
//	go run ./test/workload -uri mongodb://127.0.0.1:27600 -load 200000 -reads 20000
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	uri := flag.String("uri", "mongodb://127.0.0.1:27017/", "what to run the workload against")
	ns := flag.String("ns", "bench.orders", "db.collection to use")
	load := flag.Int("load", 100000, "documents to insert, spread over the period")
	days := flag.Int("days", 365, "how far back the oldest document is")
	reads := flag.Int("reads", 20000, "reads to perform")
	conc := flag.Int("conc", 8, "concurrent clients")
	newest10 := flag.Float64("newest10", 0.7, "share of reads that touch the newest tenth of the collection, which is a thing an operator can say about their own traffic")
	recency := flag.Float64("recency", 0, "the raw power law exponent, if you would rather set it directly than through -newest10")
	ranges := flag.Float64("ranges", 0.1, "fraction of reads that are range queries rather than point lookups")
	skipLoad := flag.Bool("skip-load", false, "reuse a collection loaded by an earlier run")
	snapshot := flag.String("snapshot", "", "the router's dashboard api, to attribute reads to the tier that answered them, e.g. http://127.0.0.1:27101/api/snapshot")
	seed := flag.Int64("seed", 1, "so a run can be repeated exactly")
	flag.Parse()

	// The exponent is derived from the readable knob unless it was set directly.
	//
	// P(a read lands in the newest p of the collection) = p^(1/e), so an
	// exponent of e = ln(p) / ln(share) puts exactly the share asked for in the
	// newest tenth. Stating it the other way round was a mistake worth not
	// repeating: an exponent of 1.4 sounds like a strong bias and produces
	// almost none, which would have made tiering look far worse than any real
	// workload behaves.
	if *recency <= 0 {
		if *newest10 <= 0 || *newest10 >= 1 {
			die("-newest10 must be between 0 and 1, exclusive")
		}
		*recency = math.Log(0.10) / math.Log(*newest10)
	}

	ctx := context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI(*uri).SetAppName("thermocline-workload"))
	if err != nil {
		die("connect: %v", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()
	if err := client.Ping(ctx, nil); err != nil {
		die("ping: %v", err)
	}

	dbName, collName := split(*ns)
	coll := client.Database(dbName).Collection(collName)
	period := time.Duration(*days) * 24 * time.Hour
	start := time.Now().Add(-period)

	if !*skipLoad {
		fmt.Printf("loading %s documents into %s, spread over %d days\n", commas(*load), *ns, *days)
		if err := loadDocs(ctx, coll, *load, start, period); err != nil {
			die("load: %v", err)
		}
	}

	fmt.Printf("reading: %s reads, %d clients, %.0f%% of them in the newest tenth (exponent %.1f), %.0f%% ranges\n",
		commas(*reads), *conc, *newest10*100, *recency, *ranges*100)
	// Asked before and after, so the reads this workload made are the only ones
	// counted. The router is the one that knows which tier answered: from out
	// here every read looks the same, which is the whole point of the proxy and
	// the reason a workload alone cannot say what tiering achieved.
	before := readClasses(*snapshot)
	res := run(ctx, coll, *reads, *conc, *recency, *ranges, *load, start, period, *seed)
	after := readClasses(*snapshot)
	res.report(*load, *days)
	reportClasses(before, after)
}

// loadDocs writes documents whose ids carry their age, which is how the evictor
// decides what is old.
func loadDocs(ctx context.Context, coll *mongo.Collection, n int, start time.Time, period time.Duration) error {
	if err := coll.Drop(ctx); err != nil {
		return err
	}
	const batch = 1000
	docs := make([]any, 0, batch)
	for i := 0; i < n; i++ {
		// Evenly spread in time, so "the newest 10%" is also "the last 10% of
		// the period" and the two ways of talking about a hot window agree.
		at := start.Add(time.Duration(float64(period) * float64(i) / float64(n)))
		docs = append(docs, bson.D{
			{Key: "_id", Value: idAt(at, i)},
			{Key: "at", Value: at},
			{Key: "seq", Value: i},
			{Key: "customer", Value: fmt.Sprintf("c%06d", i%50000)},
			{Key: "status", Value: []string{"placed", "shipped", "settled"}[i%3]},
			{Key: "total", Value: float64(i%9000) / 100},
			{Key: "note", Value: "an order line with enough text on it to be worth storing"},
		})
		if len(docs) == batch {
			if _, err := coll.InsertMany(ctx, docs); err != nil {
				return err
			}
			docs = docs[:0]
			if (i+1)%50000 == 0 {
				fmt.Printf("  %s\n", commas(i+1))
			}
		}
	}
	if len(docs) > 0 {
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			return err
		}
	}
	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "at", Value: 1}}})
	return err
}

// idAt builds an ObjectId carrying a chosen timestamp, because age comes from
// the id unless a date field says otherwise. The counter bytes are set by hand:
// ObjectId.createFromTime zeroes them, so a bulk load collides on _id.
func idAt(at time.Time, seq int) bson.ObjectID {
	var id bson.ObjectID
	secs := uint32(at.Unix())
	id[0], id[1], id[2], id[3] = byte(secs>>24), byte(secs>>16), byte(secs>>8), byte(secs)
	for i := 4; i < 12; i++ {
		id[i] = byte(seq >> uint(8*(11-i)))
	}
	return id
}

type result struct {
	mu       sync.Mutex
	elapsed  time.Duration
	lat      []time.Duration
	byAge    map[int]int // reads per age decile, 0 = newest
	errs     int
	rangeLat []time.Duration
}

func run(ctx context.Context, coll *mongo.Collection, reads, conc int, recency, rangeFrac float64,
	n int, start time.Time, period time.Duration, seed int64) *result {

	res := &result{byAge: map[int]int{}}
	var wg sync.WaitGroup
	per := reads / conc
	began := time.Now()

	for c := 0; c < conc; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + int64(c)))
			lat := make([]time.Duration, 0, per)
			rlat := make([]time.Duration, 0, per/8+1)
			ages := map[int]int{}
			errs := 0

			for i := 0; i < per; i++ {
				// A power law over age: most reads land in the newest slice of
				// the collection, a few reach all the way back.
				f := math.Pow(rng.Float64(), recency)
				seq := n - 1 - int(f*float64(n-1))
				if seq < 0 {
					seq = 0
				}
				ages[decile(seq, n)]++

				t0 := time.Now()
				var err error
				if rng.Float64() < rangeFrac {
					// A range read over a window ending at that document's age,
					// which is what a report or a listing does.
					at := start.Add(time.Duration(float64(period) * float64(seq) / float64(n)))
					cur, ferr := coll.Find(ctx, bson.D{
						{Key: "at", Value: bson.D{
							{Key: "$gte", Value: at.Add(-24 * time.Hour)},
							{Key: "$lt", Value: at},
						}},
					}, options.Find().SetLimit(50))
					if ferr == nil {
						for cur.Next(ctx) {
						}
						err = cur.Err()
						_ = cur.Close(ctx)
					} else {
						err = ferr
					}
					rlat = append(rlat, time.Since(t0))
				} else {
					at := start.Add(time.Duration(float64(period) * float64(seq) / float64(n)))
					err = coll.FindOne(ctx, bson.D{{Key: "_id", Value: idAt(at, seq)}}).Err()
					lat = append(lat, time.Since(t0))
				}
				if err != nil && err != mongo.ErrNoDocuments {
					errs++
				}
			}

			res.mu.Lock()
			res.lat = append(res.lat, lat...)
			res.rangeLat = append(res.rangeLat, rlat...)
			for k, v := range ages {
				res.byAge[k] += v
			}
			res.errs += errs
			res.mu.Unlock()
		}(c)
	}
	wg.Wait()
	res.elapsed = time.Since(began)
	return res
}

func decile(seq, n int) int {
	d := 9 - (seq * 10 / n)
	if d < 0 {
		d = 0
	}
	if d > 9 {
		d = 9
	}
	return d
}

func (r *result) report(n, days int) {
	total := len(r.lat) + len(r.rangeLat)
	fmt.Printf("\n%s reads in %s, %.0f/s, %d errors\n",
		commas(total), r.elapsed.Round(time.Millisecond),
		float64(total)/r.elapsed.Seconds(), r.errs)

	fmt.Println("\npoint lookups")
	printLatency(r.lat)
	if len(r.rangeLat) > 0 {
		fmt.Println("\nrange reads")
		printLatency(r.rangeLat)
	}

	// The number the decision turns on: keep the newest X% of the collection on
	// fast storage, and this is the share of reads it would answer.
	fmt.Println("\nif the fast tier held only the newest documents")
	fmt.Printf("  %-16s %-14s %s\n", "KEPT HOT", "OF THE PERIOD", "READS IT WOULD ANSWER")
	cum := 0
	for d := 0; d < 10; d++ {
		cum += r.byAge[d]
		pct := float64(cum) / float64(total) * 100
		fmt.Printf("  newest %-9s %-14s %5.1f%%\n",
			fmt.Sprintf("%d%%", (d+1)*10),
			fmt.Sprintf("%d days", days*(d+1)/10),
			pct)
	}
}

func printLatency(d []time.Duration) {
	if len(d) == 0 {
		fmt.Println("  none")
		return
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	fmt.Printf("  n=%-8s p50 %-9s p95 %-9s p99 %-9s max %s\n",
		commas(len(d)),
		d[len(d)/2].Round(time.Microsecond),
		d[len(d)*95/100].Round(time.Microsecond),
		d[len(d)*99/100].Round(time.Microsecond),
		d[len(d)-1].Round(time.Microsecond))
}

// readClasses asks the router how it has served commands so far.
func readClasses(url string) map[string]int64 {
	if url == "" {
		return nil
	}
	resp, err := http.Get(url) //nolint:gosec // an operator's own dashboard
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not read %s: %v\n", url, err)
		return nil
	}
	defer func() { _ = resp.Body.Close() }()

	var snap struct {
		Metrics struct {
			Classes []struct {
				Name  string `json:"name"`
				Count int64  `json:"count"`
			} `json:"classes"`
		} `json:"metrics"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		fmt.Fprintf(os.Stderr, "could not read the snapshot: %v\n", err)
		return nil
	}
	out := map[string]int64{}
	for _, c := range snap.Metrics.Classes {
		out[c.Name] = c.Count
	}
	return out
}

// reportClasses says which tier actually answered, which is the number the whole
// design exists to move.
func reportClasses(before, after map[string]int64) {
	if before == nil || after == nil {
		return
	}
	names := []string{"fast", "cached", "miss", "archive", "txn_warm", "refused"}
	labels := map[string]string{
		"fast":     "the fast tier",
		"cached":   "the cache",
		"miss":     "the archive, then copied up",
		"archive":  "the archive",
		"txn_warm": "warmed for a transaction",
		"refused":  "refused",
	}
	var total int64
	deltas := map[string]int64{}
	for _, n := range names {
		d := after[n] - before[n]
		if d < 0 {
			d = 0
		}
		deltas[n] = d
		total += d
	}
	if total == 0 {
		return
	}
	fmt.Println("\nwhat actually answered, according to the router")
	for _, n := range names {
		if deltas[n] == 0 {
			continue
		}
		fmt.Printf("  %-28s %8s  %5.1f%%\n", labels[n], commas(int(deltas[n])),
			float64(deltas[n])/float64(total)*100)
	}
}

func split(ns string) (string, string) {
	for i := 0; i < len(ns); i++ {
		if ns[i] == '.' {
			return ns[:i], ns[i+1:]
		}
	}
	return ns, "orders"
}

func commas(n int) string {
	s := fmt.Sprintf("%d", n)
	out := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out += ","
		}
		out += string(c)
	}
	return out
}

func die(f string, a ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(1)
}
