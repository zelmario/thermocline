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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// recentOrder is one this worker placed, kept so a cancellation has something
// of its own to cancel and knows whose count to check afterwards.
type recentOrder struct {
	id   bson.ObjectID
	cust string
}

type shop struct {
	db        *mongo.Database
	orders    *mongo.Collection
	events    *mongo.Collection
	products  *mongo.Collection
	customers *mongo.Collection

	nProducts  int
	nCustomers int
	nOrders    int
	// began is when this run started, and the bound pageHistory scrolls below.
	// Everything older than it is written once by the seeder and never touched
	// again, which is what makes a skip stable.
	began  time.Time
	start  time.Time
	period time.Duration

	// What the application itself wrote, so a change stream consumer's events
	// can be judged against it. A shop's search indexer or analytics pipeline
	// tails this stream, and what it must never see is a document moving
	// between tiers: a copy-up is an insert on the fast tier and an eviction is
	// a delete, and neither is anything the application did.
	wrote      sync.Map // order _id hex -> true, placed by a worker
	removed    sync.Map // order _id hex -> true, cancelled by a worker
	seenInsert sync.Map // order _id hex -> true, seen by the consumer
	seenDelete sync.Map // order _id hex -> true, seen by the consumer

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

	// until is when a timed run stops, and it is zero when the run is counted
	// in rounds instead. A run of hours cannot be expressed as a round count:
	// a round's cost changes as the archive grows and as the evictor moves
	// documents under it, so the same number is twenty minutes at the start of
	// a run and an hour at the end.
	until time.Time

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
	runFor := flag.Duration("for", 0, "shop for this long instead, ignoring -rounds")
	beat := flag.Duration("heartbeat", 0, "print a progress line this often")
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
	// Before any of this run's own writes, so pageHistory's bound separates the
	// seeded history from anything placed or cancelled while it scrolls.
	s.began = time.Now()

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
		s.nProducts, s.nCustomers, s.nOrders = m.Products, m.Customers, m.Orders
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

	// A timed run and a counted run are the same shop; only the stopping
	// condition differs, and -for wins when both are given.
	perWorker := *rounds
	if *runFor > 0 {
		perWorker = 0
		fmt.Printf("opening: %d shoppers, shopping for %s\n", *conc, *runFor)
	} else if perWorker > 0 {
		fmt.Printf("opening: %d shoppers, %s rounds each\n", *conc, commas(*rounds))
	} else {
		die("give -rounds or -for; without either this shop never closes")
	}
	before := readClasses(*snapshot)
	began := time.Now()
	if *runFor > 0 {
		s.until = began.Add(*runFor)
	}

	done := make(chan struct{})
	if *beat > 0 {
		go s.heartbeat(*beat, began, done)
	}

	// Before any worker, so every order placed during the run is inside the
	// stream's window. A consumer started afterwards is legitimately short and
	// the check below would read that as this system losing events.
	tailDone := make(chan struct{})
	tailReady := make(chan struct{})
	tailStopped := make(chan struct{})
	go func() { s.tail(ctx, tailDone, tailReady); close(tailStopped) }()
	<-tailReady

	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s.shop(ctx, rand.New(rand.NewSource(*seed+int64(w))), perWorker, w, *conc)
		}(w)
	}
	wg.Wait()
	close(done)
	// Let the consumer catch up before judging it. Events are delivered after
	// the write returns, so a stream cut off the moment the last worker stops
	// is short for a reason that is not this system's.
	time.Sleep(3 * time.Second)
	close(tailDone)
	<-tailStopped
	s.checkStream()
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
	// What this worker has placed and not yet cancelled. Picking a customer at
	// random and hoping they have a recent order wasted four cancellations in
	// five: 51 attempts found 11 orders over half an hour, because a customer
	// drawn from eight hundred usually has nothing from this run.
	//
	// Per worker, and never shared, because each worker owns a disjoint set of
	// customers and the count checks are per customer. One worker cancelling
	// another's order would break the other's arithmetic, not the system's.
	var placed []recentOrder

	// rounds of zero means "until the deadline", which main only passes with
	// s.until set.
	for i := 0; rounds <= 0 || i < rounds; i++ {
		if !s.until.IsZero() && time.Now().After(s.until) {
			return
		}
		switch n := rng.Float64(); {
		case n < 0.22:
			s.browse(ctx, rng)
		case n < 0.26:
			s.pageHistory(ctx, rng)
		case n < 0.32:
			s.trackOrder(ctx, rng)
		case n < 0.34:
			s.reuseReference(ctx, rng)
		case n < 0.38:
			placed = s.cancelOrder(ctx, placed)
		case n < 0.40:
			s.channelReport(ctx, rng, mine())
		case n < 0.60:
			s.readRecentOrder(ctx, rng)
		case n < 0.75:
			s.orderHistory(ctx, rng)
		case n < 0.84:
			if o, ok := s.placeOrder(ctx, rng, mine()); ok {
				placed = append(placed, o)
			}
		case n < 0.90:
			if o, ok := s.orderInTransaction(ctx, rng, mine()); ok {
				placed = append(placed, o)
			}
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

// trackOrder is a customer typing the reference off their confirmation email
// into the tracking box. They type it in whatever case they like, and the shop
// has always found it.
//
// It is the read this workload was missing. The reference is unique and
// case-insensitive, and the order it names is usually old enough to have been
// evicted, so the answer has to come from the archive under the collation the
// index was built with. Nothing else here asks a question whose answer depends
// on a comparison rather than on a value.
func (s *shop) trackOrder(ctx context.Context, rng *rand.Rand) {
	if s.nOrders <= 0 {
		return
	}
	n := rng.Intn(s.nOrders)
	typed := strings.ToLower(orderRef(n))
	s.timed("track an order by its reference", func() error {
		var got struct {
			Ref string `bson:"ref"`
		}
		err := s.orders.FindOne(ctx, bson.D{{Key: "ref", Value: typed}},
			options.FindOne().SetCollation(refCollation())).Decode(&got)
		if err == mongo.ErrNoDocuments {
			s.violation("an order could not be found by its own reference: typed %s", typed)
			return nil
		}
		if err == nil && !strings.EqualFold(got.Ref, typed) {
			s.violation("tracking %s returned the order referenced %s", typed, got.Ref)
		}
		return err
	})
}

// reuseReference is the shop refusing to print a reference twice.
//
// A support tool re-issuing a confirmation is where this comes from, and the
// constraint has to hold whichever tier the first order is on and whatever case
// it is typed in. Accepting it is how two customers end up tracking one parcel.
func (s *shop) reuseReference(ctx context.Context, rng *rand.Rand) {
	if s.nOrders <= 0 {
		return
	}
	n := rng.Intn(s.nOrders)
	typed := strings.ToLower(orderRef(n))
	at := time.Now()
	id := idAt(at, rng.Intn(1<<20))
	s.timed("refuse a reference already in use", func() error {
		_, err := s.orders.InsertOne(ctx, bson.D{
			{Key: "_id", Value: id}, {Key: "customer", Value: "c000000"},
			{Key: "at", Value: at}, {Key: "ref", Value: typed},
			{Key: "lines", Value: bson.A{}}, {Key: "total", Value: 0.0},
			{Key: "status", Value: "placed"},
		})
		if err == nil {
			s.violation("a second order took the reference %s, which order %d already has", typed, n)
			// Take it back out, or every later tracking of that reference has
			// two answers and this one violation becomes hundreds.
			_, _ = s.orders.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
			return nil
		}
		if mongo.IsDuplicateKeyError(err) {
			return nil // The shop said no, which is the whole check.
		}
		return err
	})
}

// orderInTransaction is the same order placed the way a shop that cares about
// consistency places it: the order and its event together, or neither.
//
// It opens with the read a shop does first, how many orders this customer has
// already placed, which reaches back over a history that is mostly in the
// archive. A transaction cannot read the archive, because reading one tier
// inside the caller's snapshot and the other outside it is not a transaction
// any more, so the router has to bring what the read matches up to the fast
// tier and ask the driver to replay. Nothing else in this workload makes it do
// that under load.
//
// Both collections are tiered here, so the two writes land in one transaction
// across two namespaces the feeder mirrors separately.
func (s *shop) orderInTransaction(ctx context.Context, rng *rand.Rand, cust string) (recentOrder, bool) {
	sess, err := s.db.Client().StartSession()
	if err != nil {
		s.errs.Add(1)
		return recentOrder{}, false
	}
	defer sess.EndSession(ctx)

	at := time.Now()
	id := idAt(at, rng.Intn(1<<20))
	total := math.Round(float64(100+rng.Intn(9000))) / 100

	s.wrote.Store(id.Hex(), true)
	committed := false
	s.timed("place an order and its event in one transaction", func() error {
		_, terr := sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
			if _, err := s.orders.CountDocuments(sc, bson.D{{Key: "customer", Value: cust}}); err != nil {
				return nil, err
			}
			if _, err := s.orders.InsertOne(sc, bson.D{
				{Key: "_id", Value: id}, {Key: "customer", Value: cust}, {Key: "at", Value: at},
				{Key: "ref", Value: "Ref" + strings.ToUpper(id.Hex()[14:])},
				{Key: "channel", Value: "web"}, {Key: "currency", Value: "EUR"},
				{Key: "lines", Value: bson.A{bson.D{
					{Key: "sku", Value: fmt.Sprintf("s%04d", rng.Intn(s.nProducts))},
					{Key: "qty", Value: 1}, {Key: "price", Value: total},
				}}},
				{Key: "total", Value: total}, {Key: "status", Value: "placed"},
			}); err != nil {
				return nil, err
			}
			_, err := s.events.InsertOne(sc, bson.D{
				{Key: "order", Value: id}, {Key: "at", Value: at}, {Key: "kind", Value: "placed"},
			})
			return nil, err
		})
		if terr == nil {
			committed = true
		}
		return terr
	})
	if !committed {
		s.wrote.Delete(id.Hex())
		// Nothing of it must be there. A transaction that failed and left half
		// of itself behind is the one thing a transaction promises not to do.
		s.timed("a transaction that failed left nothing", func() error {
			n, err := s.orders.CountDocuments(ctx, bson.D{{Key: "_id", Value: id}})
			if err != nil {
				return err
			}
			if n != 0 {
				s.violation("a transaction that did not commit left its order behind: %s", id.Hex())
			}
			return nil
		})
		return recentOrder{}, false
	}

	s.timed("read back a transaction's order and its event", func() error {
		o, err := s.orders.CountDocuments(ctx, bson.D{{Key: "_id", Value: id}})
		if err != nil {
			return err
		}
		e, err := s.events.CountDocuments(ctx, bson.D{{Key: "order", Value: id}})
		if err != nil {
			return err
		}
		if o != 1 || e < 1 {
			s.violation("a committed transaction left %d orders and %d events for %s", o, e, id.Hex())
		}
		return nil
	})
	return recentOrder{id: id, cust: cust}, true
}

// cancelOrder is a customer cancelling, which is the delete this workload never
// did. Everything else here only ever added.
//
// Only an order this run placed, which a shop would also insist on: you cancel
// a recent order, not one from last March. It keeps the seeded history intact
// for trackOrder, which expects every seeded reference to still be findable, and
// a cancellation of one of those would make that check report a violation for a
// document this workload removed itself.
//
// What a shop would notice: the order is gone, and exactly one order is gone.
func (s *shop) cancelOrder(ctx context.Context, placed []recentOrder) []recentOrder {
	if len(placed) == 0 {
		return placed
	}
	victim := placed[len(placed)-1]
	placed = placed[:len(placed)-1]

	var before int64
	counted := false
	s.timed("count a customer's orders", func() error {
		n, err := s.orders.CountDocuments(ctx, bson.D{{Key: "customer", Value: victim.cust}})
		if err != nil {
			return err
		}
		before, counted = n, true
		return nil
	})

	s.removed.Store(victim.id.Hex(), true)
	deleted := false
	s.timed("cancel an order", func() error {
		res, err := s.orders.DeleteOne(ctx, bson.D{{Key: "_id", Value: victim.id}})
		if err != nil {
			return err
		}
		if res.DeletedCount != 1 {
			s.violation("cancelling one order deleted %d", res.DeletedCount)
			return nil
		}
		deleted = true
		return nil
	})
	if !deleted {
		s.removed.Delete(victim.id.Hex())
	}
	if !deleted || !counted {
		return placed
	}

	// It must be gone whichever tier held it, and its archived copy is still
	// there wearing a mark, which is the path this checks.
	s.timed("read back a cancelled order", func() error {
		err := s.orders.FindOne(ctx, bson.D{{Key: "_id", Value: victim.id}}).Err()
		if err == nil {
			s.violation("a cancelled order came back: %s", victim.id.Hex())
			return nil
		}
		if err == mongo.ErrNoDocuments {
			return nil
		}
		return err
	})
	s.timed("count a customer's orders", func() error {
		n, err := s.orders.CountDocuments(ctx, bson.D{{Key: "customer", Value: victim.cust}})
		if err != nil {
			return err
		}
		if n != before-1 {
			s.violation("cancelling one order moved %s from %d to %d", victim.cust, before, n)
		}
		return nil
	})
	return placed
}

// channelReport is the report a shop runs after adding a column: how many
// orders came through each channel, with the ones that predate the field
// counted rather than dropped.
//
// The invariant is that the parts add up to the whole. Every order is in
// exactly one bucket whether or not it carries the field, so the sum of the
// groups has to equal the count, and it is one aggregation against one count
// over a customer whose orders are spread across both tiers. A tier that drops
// documents or hands the same one over twice breaks it, and neither shows up in
// a report that only ever looks at its own total.
func (s *shop) channelReport(ctx context.Context, rng *rand.Rand, cust string) {
	var total int64
	counted := false
	s.timed("count a customer's orders", func() error {
		n, err := s.orders.CountDocuments(ctx, bson.D{{Key: "customer", Value: cust}})
		if err != nil {
			return err
		}
		total, counted = n, true
		return nil
	})
	if !counted {
		return
	}
	s.timed("a report by channel, over a field half the history lacks", func() error {
		cur, err := s.orders.Aggregate(ctx, mongo.Pipeline{
			{{Key: "$match", Value: bson.D{{Key: "customer", Value: cust}}}},
			{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$channel", "legacy"}}}},
				{Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}},
			}}},
		})
		if err != nil {
			return err
		}
		defer cur.Close(ctx)
		var sum int64
		for cur.Next(ctx) {
			var row struct {
				N int64 `bson:"n"`
			}
			if err := cur.Decode(&row); err != nil {
				return err
			}
			sum += row.N
		}
		if err := cur.Err(); err != nil {
			return err
		}
		if sum != total {
			s.violation("a channel report for %s counted %d orders where the shop has %d", cust, sum, total)
		}
		return nil
	})
}

// placeOrder writes an order and then checks the two things a shop would notice
// if this system were broken: that the order is there, and that it is there
// once.
func (s *shop) placeOrder(ctx context.Context, rng *rand.Rand, cust string) (recentOrder, bool) {
	// Every check below compares one operation's result against another's, so
	// each one is only asked when the operation it rests on actually happened.
	// An error is the system saying no, and what follows a no proves nothing:
	// a count that failed leaves zero behind, and zero against eighteen reads
	// as a shop that lost seventeen orders. See the README.
	var beforeCount int64
	countedBefore := false
	s.timed("count a customer's orders", func() error {
		n, err := s.orders.CountDocuments(ctx, bson.D{{Key: "customer", Value: cust}})
		if err != nil {
			return err
		}
		beforeCount, countedBefore = n, true
		return nil
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

	// Recorded before the write, not after. The consumer sees the event as soon
	// as the write lands, and this function does three more checks before it
	// returns: recording afterwards meant the stream had already reported an
	// order the shop had not yet admitted placing, and every order read as a
	// phantom. 274 of them in one run, all of them this race.
	//
	// Taken back below if the write is refused, because an id recorded for a
	// write that never happened reads as an event the stream lost.
	s.wrote.Store(id.Hex(), true)
	placed := false
	s.timed("place an order", func() error {
		_, err := s.orders.InsertOne(ctx, bson.D{
			{Key: "_id", Value: id}, {Key: "customer", Value: cust}, {Key: "at", Value: at},
			// Unique because the id is, and in the case the printer would use.
			{Key: "ref", Value: "Ref" + strings.ToUpper(id.Hex()[14:])},
			// Fields the seeded orders predate. A shop adds columns and does
			// not backfill a year of history, so every query written after the
			// change meets documents that have never heard of it, and the old
			// ones are exactly the ones that have been evicted to the archive.
			// It is also what marks an order as cancellable below: a shop lets
			// you cancel a recent order, not one from last March.
			{Key: "channel", Value: []string{"web", "app", "phone"}[rng.Intn(3)]},
			{Key: "currency", Value: "EUR"},
			{Key: "lines", Value: lines}, {Key: "total", Value: total},
			{Key: "status", Value: "placed"},
		})
		placed = err == nil
		return err
	})
	s.timed("log an order event", func() error {
		_, err := s.events.InsertOne(ctx, bson.D{
			{Key: "order", Value: id}, {Key: "at", Value: at}, {Key: "kind", Value: "placed"},
		})
		return err
	})

	// Read your own write, which is the promise, and check the total survived.
	// Only when there was a write: an insert that was refused leaves nothing to
	// read back, and an insert whose acknowledgement was lost may have landed
	// anyway. Neither is this check's answer to give.
	if !placed {
		s.wrote.Delete(id.Hex())
		return recentOrder{}, false
	}
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

	// Exactly one more, whichever tier answers the count, and only when there is
	// a number to compare against.
	if !countedBefore {
		return recentOrder{id: id, cust: cust}, true
	}
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
	return recentOrder{id: id, cust: cust}, true
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

// pageHistory is the order list a customer scrolls, read a page at a time, which
// is how every list view in every shop works and is the one thing this workload
// never did: it read ten and stopped.
//
// Paging is where a tiered system can be wrong in a way a single read cannot.
// Each page is a separate query, decided separately, and between two of them the
// evictor and warmer move documents across the tiers underneath. A document seen
// on two pages is a duplicate the customer can see; one on neither is an order
// that has vanished from their history.
//
// Over a stable slice on purpose: only orders placed before this run started,
// sorted by _id, which nothing writes to. Concurrent inserts would shift the
// pages under a skip and produce duplicates that are the pagination's fault
// rather than the router's, and cancellations only ever remove orders this run
// placed, which are all newer than the bound.
func (s *shop) pageHistory(ctx context.Context, rng *rand.Rand) {
	cust := fmt.Sprintf("c%06d", rng.Intn(s.nCustomers))
	before := bson.D{
		{Key: "customer", Value: cust},
		{Key: "at", Value: bson.D{{Key: "$lt", Value: s.began}}},
	}

	var total int64
	counted := false
	s.timed("count the history a customer can scroll", func() error {
		n, err := s.orders.CountDocuments(ctx, before)
		if err != nil {
			return err
		}
		total, counted = n, true
		return nil
	})
	if !counted || total == 0 {
		return
	}

	const page = 7
	seen := map[string]bool{}
	var read int64
	s.timed("scroll a customer's history a page at a time", func() error {
		for skip := int64(0); skip < total; skip += page {
			cur, err := s.orders.Find(ctx, before, options.Find().
				SetSort(bson.D{{Key: "_id", Value: 1}}).
				SetSkip(skip).SetLimit(page))
			if err != nil {
				return err
			}
			for cur.Next(ctx) {
				var o struct {
					ID bson.ObjectID `bson:"_id"`
				}
				if err := cur.Decode(&o); err != nil {
					_ = cur.Close(ctx)
					return err
				}
				if seen[o.ID.Hex()] {
					s.violation("order %s appeared on two pages of %s's history", o.ID.Hex(), cust)
				}
				seen[o.ID.Hex()] = true
				read++
			}
			if err := cur.Err(); err != nil {
				_ = cur.Close(ctx)
				return err
			}
			_ = cur.Close(ctx)
		}
		return nil
	})
	if read != total {
		s.violation("scrolling %s's history saw %d orders where the count says %d", cust, read, total)
	}
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
		msg := fmt.Sprintf(f, a...)
		s.bad = append(s.bad, msg)
		// Printed now, not only in the report. A run of hours that keeps its
		// first violation until the end says nothing about when the system
		// went wrong, and the daemons' logs beside it are what would say why.
		fmt.Printf("[%s] VIOLATION %s\n", time.Now().Format("15:04:05"), msg)
	}
	s.mu.Unlock()
}

// tail is the consumer a shop runs beside its application: a search indexer, an
// analytics pipeline, a cache that invalidates itself. It watches the orders
// collection and has to see what the application did, and only that.
//
// The invariant it checks is the one this system is most able to break. Three
// things move documents without an application asking: a copy-up, when a read
// reaches past the fast tier; an eviction, when the evictor takes a document
// off it; and a warm. On the fast tier a copy-up is an insert and an eviction
// is a delete, so a consumer that is told about them sees orders appear that
// nobody placed and disappear that nobody cancelled.
//
// Phantoms are the hard failure here. A missing event is the other half and is
// checked at the end, after the stream has been given time to drain, because a
// consumer stopped mid-flight is short for reasons that are not this system's.
func (s *shop) tail(ctx context.Context, done <-chan struct{}, ready chan<- struct{}) {
	cur, err := s.orders.Watch(ctx, mongo.Pipeline{}, options.ChangeStream().
		SetFullDocument(options.UpdateLookup))
	if err != nil {
		s.errs.Add(1)
		close(ready)
		return
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	close(ready)

	for {
		select {
		case <-done:
			return
		default:
		}
		if !cur.TryNext(ctx) {
			if cur.Err() != nil {
				s.errs.Add(1)
				return
			}
			continue
		}
		var ev struct {
			OperationType string `bson:"operationType"`
			DocumentKey   struct {
				ID bson.ObjectID `bson:"_id"`
			} `bson:"documentKey"`
		}
		if err := cur.Decode(&ev); err != nil {
			continue
		}
		id := ev.DocumentKey.ID.Hex()
		switch ev.OperationType {
		case "insert":
			s.seenInsert.Store(id, true)
			if _, ours := s.wrote.Load(id); !ours {
				// Seeded orders are written before the consumer starts, so an
				// insert for one of those is a document arriving on the fast
				// tier rather than an order being placed.
				s.violation("the stream reported an order placed that nobody placed: %s", id)
			}
		case "delete":
			s.seenDelete.Store(id, true)
			if _, ours := s.removed.Load(id); !ours {
				s.violation("the stream reported an order cancelled that nobody cancelled: %s", id)
			}
		}
	}
}

// checkStream is the other half of the consumer's promise, asked once the
// stream has drained: everything the application did reached it.
//
// Phantoms are caught as they arrive, because an event that should not exist is
// wrong the moment it is delivered. A missing one can only be judged at the
// end, and only after a wait, which is why it is here.
func (s *shop) checkStream() {
	missingIns, missingDel := 0, 0
	s.wrote.Range(func(k, _ any) bool {
		if _, ok := s.seenInsert.Load(k); !ok {
			missingIns++
		}
		return true
	})
	s.removed.Range(func(k, _ any) bool {
		if _, ok := s.seenDelete.Load(k); !ok {
			missingDel++
		}
		return true
	})
	if missingIns > 0 {
		s.violation("the stream never reported %d orders the shop placed", missingIns)
	}
	if missingDel > 0 {
		s.violation("the stream never reported %d orders the shop cancelled", missingDel)
	}
}

// heartbeat is what makes a run of hours readable while it runs. The report at
// the end says what happened; it does not say it at the second hour, which is
// where a run long enough to be worth doing spends most of its time.
func (s *shop) heartbeat(every time.Duration, began time.Time, done <-chan struct{}) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			s.mu.Lock()
			ops := 0
			for _, v := range s.lat {
				ops += len(v)
			}
			s.mu.Unlock()
			in := time.Since(began)
			fmt.Printf("[%s] %s in: %s operations, %.0f/s, %d errors, %d violations\n",
				time.Now().Format("15:04:05"), in.Round(time.Second), commas(ops),
				float64(ops)/in.Seconds(), s.errs.Load(), s.violations.Load())
		}
	}
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
