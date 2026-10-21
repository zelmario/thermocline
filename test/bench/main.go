// A benchmark of one application against a plain MongoDB cluster and against
// the same data behind Thermocline, so the two can be compared on the same
// workload.
//
// The case for tiering rests on one situation: a collection much larger than
// the cache, read mostly near its newest end. A plain cluster then keeps old
// documents and their index entries competing for memory with the ones being
// used. This loads that situation and runs a mixed workload against it:
//
//	bench -uri URI -load 40000000          load three years of orders
//	bench -uri URI -for 10m -conc 32       run, and report each operation
//
// Run the same -run against the cluster itself, then through the router after
// adopting that cluster as the archive, and compare the two reports.
//
// The mix is a parameter, not a claim. What decides the answer is how much of
// the traffic stays near the newest end, -newest10, and how much of it asks a
// question no date bounds, -history.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
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

const customers = 200000

func main() {
	uri := flag.String("uri", "mongodb://127.0.0.1:27017/", "what to run against")
	ns := flag.String("ns", "bench.orders", "db.collection")
	load := flag.Int("load", 0, "load this many orders, spread over -days, then stop")
	loaders := flag.Int("loaders", 8, "parallel loaders")
	days := flag.Int("days", 1095, "how far back the oldest order is")
	pad := flag.Int("pad", 800, "bytes of incompressible padding on each order")
	runFor := flag.Duration("for", 0, "run the workload this long")
	warm := flag.Duration("warm", 2*time.Minute, "run this long first and discard it")
	conc := flag.Int("conc", 32, "concurrent clients")
	newest10 := flag.Float64("newest10", 0.8, "share of reads that touch the newest tenth of the collection")
	history := flag.Float64("history", 0.02, "share of operations that read a customer's whole history, which no date bounds")
	out := flag.String("json", "", "also write the report here as JSON")
	label := flag.String("label", "", "a name for this run in the report")
	seed := flag.Int64("seed", 1, "so a run can be repeated")
	kind := flag.String("workload", "mix", "mix: the operation mix over orders alone; store: shoppers and a back office over a whole shop")
	mixName := flag.String("store-mix", "store", "store workload: the shopper's mix, store (write heavy, the first one) or real (a shop's shape: about one step in seventy buys)")
	rate := flag.Int("rate", 0, "store workload: hold the shoppers to this many operations a second in all, and ship every 20ms, so every target does the same work (0: as fast as they can)")
	loadStoreFlag := flag.Bool("load-store", false, "stock the shop's products and customers in -ns's database, then stop")
	metaURI := flag.String("meta-uri", "", "where the load was recorded, when -uri is a router: "+
		"the record lives beside the data, in a collection the router does not tier")
	flag.Parse()

	ctx := context.Background()
	cl, err := mongo.Connect(options.Client().ApplyURI(*uri).SetMaxPoolSize(uint64(*conc + *loaders + 4)))
	if err != nil {
		die("connect: %v", err)
	}
	defer func() { _ = cl.Disconnect(ctx) }()
	db, coll := split(*ns)
	c := cl.Database(db).Collection(coll)
	period := time.Duration(*days) * 24 * time.Hour

	if *load > 0 {
		start := time.Now().Add(-period)
		if err := loadOrders(ctx, c, *load, *loaders, start, period, *pad); err != nil {
			die("load: %v", err)
		}
		return
	}
	if *loadStoreFlag {
		if err := loadStore(ctx, cl.Database(db), *pad); err != nil {
			die("load store: %v", err)
		}
		return
	}
	if *runFor <= 0 {
		die("nothing to do: give -load N or -for D")
	}

	var m marker
	mc := cl
	if *metaURI != "" {
		if mc, err = mongo.Connect(options.Client().ApplyURI(*metaURI)); err != nil {
			die("connect to -meta-uri: %v", err)
		}
		defer func() { _ = mc.Disconnect(ctx) }()
	}
	if err := mc.Database(db).Collection("bench_meta").FindOne(ctx, bson.D{{Key: "_id", Value: coll}}).Decode(&m); err != nil {
		die("no load recorded for %s: %v", *ns, err)
	}
	recency := math.Log(0.1) / math.Log(*newest10)
	w := &workload{c: c, n: m.N, start: m.Start, period: m.Period, recency: recency,
		history: *history, pad: *pad}

	runner := func(d time.Duration, seed int64) *report { return w.run(ctx, d, *conc, seed) }
	if *kind == "store" {
		mix, ok := mixes[*mixName]
		if !ok {
			die("-store-mix must be store or real, not %q", *mixName)
		}
		st := &store{w: w, db: cl.Database(db), orders: c, rate: *rate, mix: mix}
		runner = func(d time.Duration, seed int64) *report { return st.run(ctx, d, *conc, seed) }
	}
	if *warm > 0 {
		fmt.Printf("warming for %s\n", *warm)
		runner(*warm, *seed+1000)
	}
	fmt.Printf("running %s for %s at %d clients\n", *kind, *runFor, *conc)
	rep := runner(*runFor, *seed)
	rep.Label, rep.URI = *label, redact(*uri)
	rep.print()
	if *out != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			die("write %s: %v", *out, err)
		}
	}
}

type marker struct {
	N      int           `bson:"n"`
	Start  time.Time     `bson:"start"`
	Period time.Duration `bson:"period"`
}

// loadOrders inserts n orders evenly over the period, oldest first within each
// loader's slice, and records what it did so a run can find them again.
func loadOrders(ctx context.Context, c *mongo.Collection, n, loaders int, start time.Time, period time.Duration, pad int) error {
	if err := c.Drop(ctx); err != nil {
		return err
	}
	var done atomic.Int64
	began := time.Now()
	errs := make(chan error, loaders)
	var wg sync.WaitGroup
	per := (n + loaders - 1) / loaders
	for l := 0; l < loaders; l++ {
		from, to := l*per, min((l+1)*per, n)
		wg.Add(1)
		go func(l, from, to int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(l) + 7))
			docs := make([]any, 0, 1000)
			for i := from; i < to; i++ {
				at := start.Add(time.Duration(float64(period) * float64(i) / float64(n)))
				docs = append(docs, order(idAt(at, i), at, i, rng, pad))
				if len(docs) == cap(docs) || i == to-1 {
					if _, err := c.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
						errs <- err
						return
					}
					docs = docs[:0]
					if d := done.Add(1000); d%1000000 < 1000 {
						fmt.Printf("  %s loaded, %.0f/s\n", commas(int(d)), float64(d)/time.Since(began).Seconds())
					}
				}
			}
		}(l, from, to)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	fmt.Printf("loaded %s in %s; building indexes\n", commas(n), time.Since(began).Round(time.Second))
	if _, err := c.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "customer", Value: 1}, {Key: "at", Value: -1}}},
		{Keys: bson.D{{Key: "at", Value: 1}}},
	}); err != nil {
		return err
	}
	_, err := c.Database().Collection("bench_meta").ReplaceOne(ctx, bson.D{{Key: "_id", Value: c.Name()}},
		marker{N: n, Start: start, Period: period}, options.Replace().SetUpsert(true))
	fmt.Printf("done in %s\n", time.Since(began).Round(time.Second))
	return err
}

func order(id bson.ObjectID, at time.Time, i int, rng *rand.Rand, pad int) bson.D {
	b := make([]byte, pad)
	for k := range b {
		b[k] = byte(33 + rng.Intn(94))
	}
	items := bson.A{}
	for k := 0; k < 1+i%4; k++ {
		items = append(items, bson.D{{Key: "sku", Value: fmt.Sprintf("S%05d", (i*7+k)%20000)},
			{Key: "qty", Value: 1 + k}, {Key: "price", Value: float64((i+k)%9000) / 100}})
	}
	return bson.D{
		{Key: "_id", Value: id},
		{Key: "at", Value: at},
		{Key: "customer", Value: fmt.Sprintf("c%06d", (i*7919)%customers)},
		{Key: "status", Value: "settled"},
		{Key: "total", Value: float64(i%90000) / 100},
		{Key: "items", Value: items},
		{Key: "note", Value: string(b)},
	}
}

// idAt builds an ObjectId carrying a chosen timestamp, with the counter bytes
// set by hand so a bulk load does not collide. See test/workload.
func idAt(at time.Time, seq int) bson.ObjectID {
	var id bson.ObjectID
	secs := uint32(at.Unix())
	id[0], id[1], id[2], id[3] = byte(secs>>24), byte(secs>>16), byte(secs>>8), byte(secs)
	for i := 4; i < 12; i++ {
		id[i] = byte(seq >> uint(8*(11-i)))
	}
	return id
}

type workload struct {
	c       *mongo.Collection
	n       int
	start   time.Time
	period  time.Duration
	recency float64
	history float64
	pad     int
}

// The operations, and their shares of the traffic once -history is taken out.
var mix = []struct {
	name  string
	share float64
}{
	{"read by id", 0.60},
	{"customer, last 30 days", 0.15},
	{"orders in an hour", 0.10},
	{"insert", 0.10},
	{"update status", 0.05},
}

type report struct {
	Label   string             `json:"label"`
	URI     string             `json:"uri"`
	Seconds float64            `json:"seconds"`
	Clients int                `json:"clients"`
	Ops     map[string]*opStat `json:"ops"`
}

type opStat struct {
	N      int     `json:"n"`
	Errors int     `json:"errors"`
	PerSec float64 `json:"per_sec"`
	P50    float64 `json:"p50_ms"`
	P95    float64 `json:"p95_ms"`
	P99    float64 `json:"p99_ms"`
	P999   float64 `json:"p999_ms"`
	lat    []time.Duration
}

// seqAt picks an order by age: a power law, so most land near the newest end.
func (w *workload) seqAt(rng *rand.Rand) int {
	f := math.Pow(rng.Float64(), w.recency)
	return max(0, w.n-1-int(f*float64(w.n-1)))
}

func (w *workload) atOf(seq int) time.Time {
	return w.start.Add(time.Duration(float64(w.period) * float64(seq) / float64(w.n)))
}

func (w *workload) run(ctx context.Context, d time.Duration, conc int, seed int64) *report {
	rep := &report{Clients: conc, Ops: map[string]*opStat{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	deadline := time.Now().Add(d)
	began := time.Now()
	var inserted atomic.Int64
	for k := 0; k < conc; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed*1000 + int64(k)))
			local := map[string]*opStat{}
			stat := func(name string) *opStat {
				if s, ok := local[name]; ok {
					return s
				}
				s := &opStat{}
				local[name] = s
				return s
			}
			for time.Now().Before(deadline) {
				name := "customer, whole history"
				if rng.Float64() >= w.history {
					r, acc := rng.Float64(), 0.0
					for _, op := range mix {
						acc += op.share
						if r < acc {
							name = op.name
							break
						}
					}
				}
				t0 := time.Now()
				err := w.do(ctx, name, rng, &inserted)
				s := stat(name)
				s.lat = append(s.lat, time.Since(t0))
				if err != nil {
					s.Errors++
				}
			}
			mu.Lock()
			for name, s := range local {
				all, ok := rep.Ops[name]
				if !ok {
					all = &opStat{}
					rep.Ops[name] = all
				}
				all.lat = append(all.lat, s.lat...)
				all.Errors += s.Errors
			}
			mu.Unlock()
		}(k)
	}
	wg.Wait()
	rep.Seconds = time.Since(began).Seconds()
	finish(rep)
	return rep
}

// finish turns each operation's latencies into its counts and percentiles.
func finish(rep *report) {
	for _, s := range rep.Ops {
		sort.Slice(s.lat, func(i, j int) bool { return s.lat[i] < s.lat[j] })
		s.N = len(s.lat)
		s.PerSec = float64(s.N) / rep.Seconds
		q := func(p float64) float64 {
			if s.N == 0 {
				return 0
			}
			return float64(s.lat[min(s.N-1, int(p*float64(s.N)))].Microseconds()) / 1000
		}
		s.P50, s.P95, s.P99, s.P999 = q(0.50), q(0.95), q(0.99), q(0.999)
	}
}

func (w *workload) do(ctx context.Context, name string, rng *rand.Rand, inserted *atomic.Int64) error {
	drain := func(cur *mongo.Cursor, err error) error {
		if err != nil {
			return err
		}
		defer func() { _ = cur.Close(ctx) }()
		for cur.Next(ctx) {
		}
		return cur.Err()
	}
	customer := func() string { return fmt.Sprintf("c%06d", (w.seqAt(rng)*7919)%customers) }
	switch name {
	case "read by id":
		seq := w.seqAt(rng)
		err := w.c.FindOne(ctx, bson.D{{Key: "_id", Value: idAt(w.atOf(seq), seq)}}).Err()
		if err == mongo.ErrNoDocuments {
			return nil
		}
		return err
	case "customer, last 30 days":
		return drain(w.c.Find(ctx, bson.D{
			{Key: "customer", Value: customer()},
			{Key: "at", Value: bson.D{{Key: "$gte", Value: time.Now().Add(-30 * 24 * time.Hour)}}},
		}, options.Find().SetSort(bson.D{{Key: "at", Value: -1}}).SetLimit(20)))
	case "customer, whole history":
		return drain(w.c.Find(ctx, bson.D{{Key: "customer", Value: customer()}},
			options.Find().SetSort(bson.D{{Key: "at", Value: -1}}).SetLimit(100)))
	case "orders in an hour":
		at := w.atOf(w.seqAt(rng))
		return drain(w.c.Find(ctx, bson.D{{Key: "at", Value: bson.D{
			{Key: "$gte", Value: at.Add(-time.Hour)}, {Key: "$lt", Value: at},
		}}}, options.Find().SetLimit(50)))
	case "insert":
		seq := w.n + int(inserted.Add(1)) + int(rng.Int31())<<20
		now := time.Now()
		_, err := w.c.InsertOne(ctx, order(idAt(now, seq), now, seq, rng, w.pad))
		return err
	case "update status":
		// A recent order: the newest tenth, where order status still changes.
		seq := w.n - 1 - rng.Intn(max(1, w.n/10))
		_, err := w.c.UpdateOne(ctx, bson.D{{Key: "_id", Value: idAt(w.atOf(seq), seq)}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "returned"}}}})
		return err
	}
	return fmt.Errorf("unknown operation %q", name)
}

func (r *report) print() {
	names := make([]string, 0, len(r.Ops))
	for n := range r.Ops {
		names = append(names, n)
	}
	sort.Strings(names)
	total := 0
	fmt.Printf("\n%s  %.0fs, %d clients\n", r.Label, r.Seconds, r.Clients)
	fmt.Printf("  %-30s %9s %8s %8s %8s %8s %8s %6s\n", "operation", "n", "ops/s", "p50 ms", "p95", "p99", "p99.9", "errors")
	for _, n := range names {
		s := r.Ops[n]
		total += s.N
		fmt.Printf("  %-30s %9d %8.0f %8.2f %8.2f %8.2f %8.2f %6d\n", n, s.N, s.PerSec, s.P50, s.P95, s.P99, s.P999, s.Errors)
	}
	fmt.Printf("  %-30s %9d %8.0f\n", "all", total, float64(total)/r.Seconds)
}

func redact(uri string) string {
	for i := 0; i+1 < len(uri); i++ {
		if uri[i] == '@' {
			for j := i; j >= 0; j-- {
				if uri[j] == '/' {
					return uri[:j+1] + "...@" + uri[i+1:]
				}
			}
		}
	}
	return uri
}

func split(ns string) (string, string) {
	for i := 0; i < len(ns); i++ {
		if ns[i] == '.' {
			return ns[:i], ns[i+1:]
		}
	}
	return "test", ns
}

func commas(n int) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func die(f string, a ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(1)
}
