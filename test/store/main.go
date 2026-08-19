// A shop, running against the proxy, checking what a shop would notice.
//
// The workload tool measures throughput. This measures whether an application
// keeps working, which is a different question and the one this system actually
// promises an answer to: no application changes. A benchmark cannot fail that
// promise, because it does not know what its own answers should be.
//
// So this is a small store with the shape a real one has. A product catalogue
// that never grows and is read constantly. Customers. Orders that accumulate
// forever, are read hard for a week and then almost never, and are the only
// thing worth tiering. An event log per order. Reports that reach back over
// months, which is the read that goes to the archive.
//
// While it runs, it checks the things a shop would notice:
//
//   - An order just placed can be read back, with the total it was placed with.
//   - Placing an order increases that customer's order count by exactly one:
//     not two, which is a write applied twice, and not zero, which is a write
//     reported as done and lost.
//   - An order's total equals the sum of its lines, whichever tier answered.
//   - A customer's order list never shows the same order twice.
//   - A report over a month that has already ended returns the same number
//     every time. Nothing writes to the past, so an answer that moves is an
//     answer that was wrong once.
//
// Every one of those is a way this system could be quietly broken while every
// latency number looks healthy.
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
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type shop struct {
	db        *mongo.Database
	orders    *mongo.Collection
	events    *mongo.Collection
	products  *mongo.Collection
	customers *mongo.Collection

	nProducts  int
	nCustomers int
	start      time.Time
	period     time.Duration

	// Violations are the point of the whole thing. Counted separately from
	// errors, because an error is the system saying no and a violation is it
	// saying something untrue.
	violations atomic.Int64
	errs       atomic.Int64

	// months are the report windows, computed once at startup. They have to be
	// fixed: a window derived from time.Now() on every call slides, and a
	// sliding window over evenly spaced orders returns a count one lower or
	// higher depending on where its edge happens to fall. That reads as an
	// unstable answer from the archive and is nothing of the kind. The first
	// run of this tool reported twenty-one of them.
	months []window

	mu   sync.Mutex
	lat  map[string][]time.Duration
	said map[string]int64 // a report's answer, so a later look can disagree with it
	bad  []string
}

// window is one whole calendar month that ended before this run began, so
// nothing written during the run can land in it.
type window struct {
	key       string
	from, end time.Time
}

func main() {
	uri := flag.String("uri", "mongodb://127.0.0.1:27017/", "what to shop against")
	db := flag.String("db", "shop", "database")
	seedOrders := flag.Int("orders", 50000, "orders to seed, spread over the period")
	days := flag.Int("days", 365, "how far back the oldest order is")
	custs := flag.Int("customers", 5000, "customers")
	prods := flag.Int("products", 1000, "products")
	rounds := flag.Int("rounds", 400, "shopping rounds per worker")
	conc := flag.Int("conc", 8, "concurrent shoppers")
	seedOnly := flag.Bool("seed-only", false, "load the shop and stop")
	skipSeed := flag.Bool("skip-seed", false, "shop against what is already there")
	snapshot := flag.String("snapshot", "", "the router's dashboard api, to attribute reads")
	seed := flag.Int64("seed", 1, "so a run can be repeated")
	flag.Parse()

	ctx := context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI(*uri).SetAppName("thermocline-store"))
	if err != nil {
		die("connect: %v", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()
	if err := client.Ping(ctx, nil); err != nil {
		die("ping: %v", err)
	}

	d := client.Database(*db)
	s := &shop{
		db: d, orders: d.Collection("orders"), events: d.Collection("order_events"),
		products: d.Collection("products"), customers: d.Collection("customers"),
		nProducts: *prods, nCustomers: *custs,
		period: time.Duration(*days) * 24 * time.Hour,
		lat:    map[string][]time.Duration{}, said: map[string]int64{},
	}
	s.start = time.Now().Add(-s.period)

	if !*skipSeed {
		fmt.Printf("stocking the shop: %s products, %s customers, %s orders over %d days\n",
			commas(*prods), commas(*custs), commas(*seedOrders), *days)
		if err := s.seed(ctx, *seedOrders); err != nil {
			die("seed: %v", err)
		}
		if *seedOnly {
			fmt.Println("stocked")
			return
		}
	} else {
		m, err := s.readMarker(ctx)
		if err != nil {
			die("this database was not stocked by this tool: %v", err)
		}
		s.start, s.period = m.Start, time.Duration(m.Days)*24*time.Hour
		s.nProducts, s.nCustomers = m.Products, m.Customers
		fmt.Printf("shopping in the store stocked at %s\n", m.Start.Format(time.RFC3339))
	}

	// Whole calendar months, none of them the current one, all of them ended
	// before the first order of this run is placed.
	now := time.Now().UTC()
	first := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	for back := 2; back <= 11; back++ {
		from := first.AddDate(0, -back, 0)
		if from.Before(s.start) {
			continue
		}
		s.months = append(s.months, window{from.Format("2006-01"), from, from.AddDate(0, 1, 0)})
	}
	if len(s.months) == 0 {
		die("the shop holds no month old enough to report on")
	}

	fmt.Printf("opening: %d shoppers, %s rounds each\n", *conc, commas(*rounds))
	before := readClasses(*snapshot)
	began := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s.shop(ctx, rand.New(rand.NewSource(*seed+int64(w))), *rounds, w, *conc)
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(began)
	after := readClasses(*snapshot)

	s.report(elapsed)
	reportClasses(before, after)

	if s.violations.Load() > 0 {
		os.Exit(1)
	}
}

// shop is one shopper doing what shoppers do, over and over.
//
// Writes are partitioned: shopper w only ever places orders for customers
// w, w+conc, w+2*conc and so on. The "one order placed, one order more"
// check compares a count taken before the insert with a count taken after,
// and two shoppers writing to the same customer at the same time would make
// that check fail with nothing wrong. A false alarm here is worse than no
// alarm, because it would train us to ignore the real ones.
func (s *shop) shop(ctx context.Context, rng *rand.Rand, rounds, w, conc int) {
	mine := func() string {
		k := rng.Intn((s.nCustomers-w+conc-1)/conc)*conc + w
		return fmt.Sprintf("c%06d", k)
	}
	for i := 0; i < rounds; i++ {
		switch n := rng.Float64(); {
		case n < 0.40:
			s.browse(ctx, rng)
		case n < 0.60:
			s.readRecentOrder(ctx, rng)
		case n < 0.75:
			s.orderHistory(ctx, rng)
		case n < 0.90:
			s.placeOrder(ctx, rng, mine())
		case n < 0.97:
			s.updateStatus(ctx, rng, mine())
		default:
			s.monthlyReport(ctx, rng)
		}
	}
}

// browse reads a product, which is the most common thing a shop does and the
// one thing that should never touch the archive: the catalogue is not tiered.
func (s *shop) browse(ctx context.Context, rng *rand.Rand) {
	id := rng.Intn(s.nProducts)
	s.timed("browse a product", func() error {
		var p struct {
			Price float64 `bson:"price"`
		}
		err := s.products.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&p)
		if err == nil && p.Price <= 0 {
			s.violation("a product came back with no price: %d", id)
		}
		return err
	})
}

// placeOrder writes an order and then checks the two things a shop would notice
// if this system were broken: that the order is there, and that it is there
// once.
func (s *shop) placeOrder(ctx context.Context, rng *rand.Rand, cust string) {
	var beforeCount int64
	s.timed("count a customer's orders", func() error {
		n, err := s.orders.CountDocuments(ctx, bson.D{{Key: "customer", Value: cust}})
		beforeCount = n
		return err
	})

	lines := bson.A{}
	total := 0.0
	for i := 0; i < 1+rng.Intn(3); i++ {
		price := float64(100+rng.Intn(9000)) / 100
		qty := 1 + rng.Intn(3)
		total += price * float64(qty)
		lines = append(lines, bson.D{
			{Key: "sku", Value: fmt.Sprintf("s%04d", rng.Intn(s.nProducts))},
			{Key: "qty", Value: qty},
			{Key: "price", Value: price},
		})
	}
	total = math.Round(total*100) / 100
	at := time.Now()
	id := idAt(at, rng.Intn(1<<20))

	s.timed("place an order", func() error {
		_, err := s.orders.InsertOne(ctx, bson.D{
			{Key: "_id", Value: id}, {Key: "customer", Value: cust}, {Key: "at", Value: at},
			{Key: "lines", Value: lines}, {Key: "total", Value: total},
			{Key: "status", Value: "placed"},
		})
		return err
	})
	s.timed("log an order event", func() error {
		_, err := s.events.InsertOne(ctx, bson.D{
			{Key: "order", Value: id}, {Key: "at", Value: at}, {Key: "kind", Value: "placed"},
		})
		return err
	})

	// Read your own write, which is the promise, and check the total survived.
	s.timed("read back the new order", func() error {
		var got struct {
			Total float64 `bson:"total"`
			Lines []struct {
				Qty   int     `bson:"qty"`
				Price float64 `bson:"price"`
			} `bson:"lines"`
		}
		err := s.orders.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&got)
		if err == mongo.ErrNoDocuments {
			s.violation("an order that was just placed could not be read back: %s", id.Hex())
			return nil
		}
		if err != nil {
			return err
		}
		if math.Abs(got.Total-total) > 0.001 {
			s.violation("an order came back with a different total: placed %.2f, read %.2f", total, got.Total)
		}
		sum := 0.0
		for _, l := range got.Lines {
			sum += l.Price * float64(l.Qty)
		}
		if math.Abs(math.Round(sum*100)/100-got.Total) > 0.011 {
			s.violation("an order's lines do not add up to its total: %.2f against %.2f", sum, got.Total)
		}
		return nil
	})

	// Exactly one more, whichever tier answers the count.
	s.timed("count a customer's orders", func() error {
		n, err := s.orders.CountDocuments(ctx, bson.D{{Key: "customer", Value: cust}})
		if err != nil {
			return err
		}
		if n != beforeCount+1 {
			s.violation("placing one order moved %s from %d to %d", cust, beforeCount, n)
		}
		return nil
	})
}

// orderHistory is the listing a customer sees, and the place a duplicate would
// show up.
func (s *shop) orderHistory(ctx context.Context, rng *rand.Rand) {
	cust := fmt.Sprintf("c%06d", rng.Intn(s.nCustomers))
	s.timed("a customer's order history", func() error {
		cur, err := s.orders.Find(ctx,
			bson.D{{Key: "customer", Value: cust}},
			options.Find().SetSort(bson.D{{Key: "at", Value: -1}}).SetLimit(10))
		if err != nil {
			return err
		}
		defer func() { _ = cur.Close(ctx) }()
		seen := map[string]bool{}
		for cur.Next(ctx) {
			var o struct {
				ID bson.ObjectID `bson:"_id"`
			}
			if err := cur.Decode(&o); err != nil {
				return err
			}
			if seen[o.ID.Hex()] {
				s.violation("an order appeared twice in one listing: %s", o.ID.Hex())
			}
			seen[o.ID.Hex()] = true
		}
		return cur.Err()
	})
}

// readRecentOrder reads one order, weighted towards recent ones the way a real
// application does.
func (s *shop) readRecentOrder(ctx context.Context, rng *rand.Rand) {
	// Most reads land in the last few weeks; a few reach back a year.
	age := time.Duration(math.Pow(rng.Float64(), 6) * float64(s.period))
	at := time.Now().Add(-age)
	s.timed("read an order by date", func() error {
		var o struct {
			Total float64 `bson:"total"`
		}
		err := s.orders.FindOne(ctx,
			bson.D{{Key: "at", Value: bson.D{{Key: "$lte", Value: at}}}},
			options.FindOne().SetSort(bson.D{{Key: "at", Value: -1}})).Decode(&o)
		if err == mongo.ErrNoDocuments {
			return nil
		}
		if err == nil && o.Total <= 0 {
			s.violation("an order came back with no total, read at %s", at.Format(time.RFC3339))
		}
		return err
	})
}

// updateStatus is the ordinary write against an old document: the one that has
// to reach into the archive to find what it is updating.
func (s *shop) updateStatus(ctx context.Context, rng *rand.Rand, cust string) {
	s.timed("advance an order's status", func() error {
		res, err := s.orders.UpdateOne(ctx,
			bson.D{{Key: "customer", Value: cust}, {Key: "status", Value: "placed"}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "shipped"}}}})
		if err != nil {
			return err
		}
		if res.MatchedCount > 1 {
			s.violation("updateOne matched %d documents", res.MatchedCount)
		}
		return nil
	})
}

// monthlyReport is the read that reaches into the archive, and the one whose
// answer must not move: nothing writes to a month that has ended.
func (s *shop) monthlyReport(ctx context.Context, rng *rand.Rand) {
	w := s.months[rng.Intn(len(s.months))]
	from, end, key := w.from, w.end, w.key

	s.timed("a monthly revenue report", func() error {
		cur, err := s.orders.Aggregate(ctx, mongo.Pipeline{
			bson.D{{Key: "$match", Value: bson.D{{Key: "at", Value: bson.D{
				{Key: "$gte", Value: from}, {Key: "$lt", Value: end}}}}}},
			bson.D{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: nil},
				{Key: "orders", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
		})
		if err != nil {
			return err
		}
		defer func() { _ = cur.Close(ctx) }()
		var n int64
		if cur.Next(ctx) {
			var row struct {
				Orders int64 `bson:"orders"`
			}
			if err := cur.Decode(&row); err != nil {
				return err
			}
			n = row.Orders
		}
		s.mu.Lock()
		prev, seen := s.said[key]
		if !seen {
			s.said[key] = n
		}
		s.mu.Unlock()
		if seen && prev != n {
			s.violation("a report for %s said %d and later said %d, and nothing writes to the past",
				key, prev, n)
		}
		return cur.Err()
	})
}

func (s *shop) timed(what string, f func() error) {
	t0 := time.Now()
	err := f()
	d := time.Since(t0)
	if err != nil {
		s.errs.Add(1)
		fmt.Fprintf(os.Stderr, "  %s: %v\n", what, err)
	}
	s.mu.Lock()
	s.lat[what] = append(s.lat[what], d)
	s.mu.Unlock()
}

func (s *shop) violation(f string, a ...any) {
	s.violations.Add(1)
	s.mu.Lock()
	if len(s.bad) < 20 {
		s.bad = append(s.bad, fmt.Sprintf(f, a...))
	}
	s.mu.Unlock()
}

func (s *shop) report(elapsed time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ops int
	names := make([]string, 0, len(s.lat))
	for k, v := range s.lat {
		names = append(names, k)
		ops += len(v)
	}
	sort.Strings(names)

	fmt.Printf("\n%s operations in %s, %.0f/s\n", commas(ops), elapsed.Round(time.Millisecond),
		float64(ops)/elapsed.Seconds())
	fmt.Printf("\n  %-28s %7s %10s %10s %10s\n", "WHAT THE SHOP DID", "N", "P50", "P95", "P99")
	for _, n := range names {
		d := s.lat[n]
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		fmt.Printf("  %-28s %7s %10s %10s %10s\n", n, commas(len(d)),
			round(d[len(d)/2]), round(d[len(d)*95/100]), round(d[len(d)*99/100]))
	}

	fmt.Printf("\nerrors: %d\n", s.errs.Load())
	if v := s.violations.Load(); v == 0 {
		fmt.Println("the shop noticed nothing wrong")
	} else {
		fmt.Printf("THE SHOP NOTICED %d THINGS WRONG\n", v)
		for _, b := range s.bad {
			fmt.Printf("  %s\n", b)
		}
	}
}

func round(d time.Duration) string {
	if d >= time.Millisecond {
		return d.Round(10 * time.Microsecond).String()
	}
	return d.Round(time.Microsecond).String()
}

func die(f string, a ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(1)
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

func readClasses(url string) map[string]int64 {
	if url == "" {
		return nil
	}
	resp, err := http.Get(url) //nolint:gosec // an operator's own dashboard
	if err != nil {
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
	if json.NewDecoder(resp.Body).Decode(&snap) != nil {
		return nil
	}
	out := map[string]int64{}
	for _, c := range snap.Metrics.Classes {
		out[c.Name] = c.Count
	}
	return out
}

func reportClasses(before, after map[string]int64) {
	if before == nil || after == nil {
		return
	}
	names := []string{"fast", "cached", "miss", "archive", "txn_warm", "refused"}
	labels := map[string]string{
		"fast": "the fast tier", "cached": "the cache", "miss": "the archive, then copied up",
		"archive": "the archive", "txn_warm": "warmed for a transaction", "refused": "refused",
	}
	var total int64
	d := map[string]int64{}
	for _, n := range names {
		if v := after[n] - before[n]; v > 0 {
			d[n] = v
			total += v
		}
	}
	if total == 0 {
		return
	}
	fmt.Println("\nwhat answered, according to the router")
	for _, n := range names {
		if d[n] == 0 {
			continue
		}
		fmt.Printf("  %-28s %8s  %5.1f%%\n", labels[n], commas(int(d[n])), float64(d[n])/float64(total)*100)
	}
}
