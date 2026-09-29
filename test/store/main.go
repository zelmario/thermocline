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
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
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
	// seeded is which of the stocked orders still exist, read at the start of
	// a run that did not stock the shop itself. Nil means all of them. See
	// pickSeeded.
	seeded []int
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

	// resumeEvery is how often the consumer restarts from the token it stored,
	// and checkpointEvery is how many events it processes between stores. Both
	// describe an ordinary consumer rather than a test: a search indexer
	// records where it got to, is restarted by a deploy or a crash, and comes
	// back to the position it wrote down.
	//
	// They matter here for a reason that is specific to this system. A stream
	// opened on a live collection comes back with an empty first batch, so
	// everything it ever delivers arrives in a getMore, and a router that
	// filters getMore alone looks complete. A consumer resuming from a stored
	// token is the only thing that makes the aggregate's own firstBatch carry
	// events, and that batch used to reach the application unfiltered.
	//
	// checkpointEvery is deliberately not 1. A consumer that stores a token
	// after every event replays nothing, and the replay is where this system
	// has to hide the same tiering event twice: the copy-up whose residency
	// record it read the first time may have been tidied away by the second.
	resumeEvery     time.Duration
	checkpointEvery int
	// resumes and replayed are reported rather than asserted on. A run that
	// never resumed proves nothing about resuming, and that has to be visible
	// rather than inferred from the absence of a violation.
	resumes  atomic.Int64
	replayed atomic.Int64
	// lostHistory counts a resume the oplog could no longer serve. It is not a
	// violation: a token older than the oplog window is a deployment's business
	// and this box's oplog is minutes long under load. It is counted so a run
	// that resumed by starting fresh cannot pass as a run that resumed.
	lostHistory atomic.Int64

	// resyncEvery is how often the consumer falls further behind than the
	// oplog, and what it then does about it, which is what every consumer
	// that stores a position eventually has to do: told ChangeStreamHistoryLost,
	// it notes a cluster time, reads the whole collection, rebuilds what it
	// keeps from that, and tails from the time it noted.
	//
	// The read is the part nothing else here does. It is one cursor over every
	// order, both tiers, many batches, while the evictor and the warmer move
	// documents underneath it, and what it builds is judged at the end against
	// the tiers themselves rather than against another read through the
	// router.
	//
	// Falling behind is arranged rather than waited for. This deployment's
	// fast tier keeps weeks of oplog, so the stream is opened at an operation
	// time the oplog no longer holds, which the server refuses with the same
	// check and the same error as a resume token that has aged out.
	resyncEvery time.Duration
	lastResync  time.Time
	resyncs     atomic.Int64
	told        atomic.Int64 // history-lost errors the router passed on
	scanned     atomic.Int64
	// index is what the consumer keeps: the orders it believes exist. Built
	// by each resync and kept by every insert and delete after it.
	idxMu sync.Mutex
	index map[string]bool
	// covered are orders a resync's scan stood in for, so their events may
	// never arrive: they happened in the stretch the consumer fell behind by.
	coveredIns sync.Map
	coveredDel sync.Map
	// truthHot and truthCold are the tiers themselves, for the judgement at
	// the end. Straight at them, not through the router, or the check would
	// compare the router with itself.
	truthHot, truthCold string

	// part and parts say this process is one of several applications writing
	// the same collections, and which one. Customers are partitioned across
	// all of them the way they are across workers within one, so the count
	// checks stay sound, but everything else is shared: the same orders
	// collection, the same reference index, the same change stream.
	//
	// ledger is what makes the stream checkable then. A consumer sees every
	// application's orders, and without a record of who placed what it would
	// report the other application's every order as a phantom. Each
	// application writes down what it is about to do before doing it, in a
	// collection nothing tiers, so by the time an event arrives the entry that
	// explains it is already there. An event no application wrote down is the
	// phantom being looked for.
	part, parts int
	ledger      *mongo.Collection
	foreignIns  atomic.Int64
	foreignDel  atomic.Int64

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
	part := flag.Int("part", 0, "which of -parts applications this is, from 0")
	parts := flag.Int("parts", 1, "applications writing these same collections at once")
	beganAt := flag.String("began", "",
		"RFC 3339 instant every application agrees this run began, when -parts is above 1")
	resumeEvery := flag.Duration("resume-every", 20*time.Second,
		"restart the stream consumer from its stored token this often; 0 never restarts")
	checkpointEvery := flag.Int("checkpoint-every", 10,
		"events the consumer processes between storing its resume token")
	resyncEvery := flag.Duration("resync-every", 0,
		"fall further behind than the oplog this often and resync; 0 never does")
	truthHot := flag.String("truth-hot", "", "the fast tier itself, to judge the consumer's index against")
	truthCold := flag.String("truth-cold", "", "the archive itself, likewise")
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
		resumeEvery: *resumeEvery, checkpointEvery: *checkpointEvery,
		resyncEvery: *resyncEvery, truthHot: *truthHot, truthCold: *truthCold,
		index:  map[string]bool{},
		period: time.Duration(*days) * 24 * time.Hour,
		lat:    map[string][]time.Duration{}, said: map[string]int64{},
	}
	s.start = time.Now().Add(-s.period)
	// Before any of this run's own writes, so pageHistory's bound separates the
	// seeded history from anything placed or cancelled while it scrolls.
	s.began = time.Now()
	if *parts < 1 || *part < 0 || *part >= *parts {
		die("-part %d of -parts %d names no application", *part, *parts)
	}
	s.part, s.parts = *part, *parts
	if s.parts > 1 {
		s.ledger = d.Collection("store_ledger")
		// One bound for every application, or pageHistory's stable slice is
		// not stable: an application that started earlier places orders
		// before this one's own start and cancels them after it, and the
		// pages move under a skip with nothing wrong.
		if *beganAt == "" {
			die("-parts %d needs -began, the instant all of them agree the run began", s.parts)
		}
		t, err := time.Parse(time.RFC3339Nano, *beganAt)
		if err != nil {
			die("-began: %v", err)
		}
		s.began = t
	}

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
		if err := s.readSeeded(ctx); err != nil {
			die("read which stocked orders are left: %v", err)
		}
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
	// With other applications on the same collection, the consumer keeps
	// reading until they have all stopped writing too, or the index judged
	// below would be short by whatever they wrote after this one finished.
	s.finished(ctx)
	// Let the consumer catch up before judging it. Events are delivered after
	// the write returns, so a stream cut off the moment the last worker stops
	// is short for a reason that is not this system's.
	time.Sleep(3 * time.Second)
	close(tailDone)
	<-tailStopped
	s.checkStream()
	s.checkIndex(ctx)
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
	// Across applications as well as workers: worker w of application p owns
	// the customers congruent to p*conc+w modulo conc*parts.
	stride, base := conc*s.parts, s.part*conc+w
	if base >= s.nCustomers {
		die("%d customers cannot be shared among %d shoppers", s.nCustomers, stride)
	}
	mine := func() string {
		k := rng.Intn((s.nCustomers-base+stride-1)/stride)*stride + base
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
		case n < 0.58:
			s.readRecentOrder(ctx, rng)
		case n < 0.60:
			s.activityReport(ctx, rng)
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
	n, ok := s.pickSeeded(rng)
	if !ok {
		return
	}
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

// readSeeded records which stocked orders are still there, for a run against a
// shop somebody else stocked.
//
// Something other than this shop can delete them. testrollback cancels forty
// through the router, and after the rollback they are gone from the archive
// for good, which is correct. The next run then tracked and re-used references
// of orders that no longer exist: the archive rightly accepted a reference
// nobody held, and the shop reported a second order taking it, twice in host
// sweeps (2026-09-28 and 29), both times a reference between Ref002000 and
// Ref002039, exactly the forty deleted.
func (s *shop) readSeeded(ctx context.Context) error {
	cur, err := s.orders.Find(ctx, bson.D{{Key: "ref", Value: bson.D{{Key: "$regex", Value: "^Ref[0-9]{6}$"}}}},
		options.Find().SetProjection(bson.D{{Key: "ref", Value: 1}, {Key: "_id", Value: 0}}))
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	have := make([]bool, s.nOrders)
	for cur.Next(ctx) {
		var o struct {
			Ref string `bson:"ref"`
		}
		if err := cur.Decode(&o); err != nil {
			return err
		}
		if n, err := strconv.Atoi(strings.TrimPrefix(o.Ref, "Ref")); err == nil && n >= 0 && n < s.nOrders {
			have[n] = true
		}
	}
	if err := cur.Err(); err != nil {
		return err
	}
	s.seeded = make([]int, 0, s.nOrders)
	for n, ok := range have {
		if ok {
			s.seeded = append(s.seeded, n)
		}
	}
	if gone := s.nOrders - len(s.seeded); gone > 0 {
		fmt.Printf("%d of the %d stocked orders are gone, and are not tracked or re-used\n", gone, s.nOrders)
	}
	return nil
}

// pickSeeded picks a stocked order that still exists.
func (s *shop) pickSeeded(rng *rand.Rand) (int, bool) {
	if s.seeded == nil {
		return rng.Intn(s.nOrders), true
	}
	if len(s.seeded) == 0 {
		return 0, false
	}
	return s.seeded[rng.Intn(len(s.seeded))], true
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
	n, ok := s.pickSeeded(rng)
	if !ok {
		return
	}
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
	s.intend(ctx, "insert", id)
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
// document this workload removed itself. The ones something else deleted are
// left out when the run starts; see readSeeded.
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

	// The order's events go first, and the order after, which is the ordering a
	// shop would choose anyway and is what makes the activity report below
	// checkable. A report joining events to their orders is racy the other way
	// round: between deleting the order and deleting its events there is an
	// instant where an event has no order, and a reader in that window would
	// report this workload's own two writes as a violation. See the note on
	// reading between two writes that belong to one act.
	s.timed("delete a cancelled order's events", func() error {
		_, err := s.events.DeleteMany(ctx, bson.D{{Key: "order", Value: victim.id}})
		return err
	})

	s.removed.Store(victim.id.Hex(), true)
	s.intend(ctx, "delete", victim.id)
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
	s.intend(ctx, "insert", id)
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

// activityReport is the read a shop's operations page makes: what happened
// lately, with the order each event belongs to attached.
//
// It exists because every other read here names one collection. A $lookup is
// the shape where a second collection is read *inside* a pipeline, and the
// router has to reach the right tiers for a namespace the query never names in
// its $match. A filter in front of a pipeline does not reach the joined side,
// so the joined collection is read on whatever tier the plan lands on.
//
// The invariant is one a shop would state plainly: every event it can see
// belongs to an order it can see. The shop deletes an order's events before it
// deletes the order, so an event without an order is never a state this
// workload produces, whatever instant the report is taken at. An event that
// comes back with no order is either a tombstoned copy joined out of the
// archive, or an order the join could not see on the tier it looked at.
//
// Bounded to this run's own events. The seeded history has none, and an event
// this process did not write is not one whose order it can reason about.
func (s *shop) activityReport(ctx context.Context, rng *rand.Rand) {
	s.timed("an activity report joining events to orders", func() error {
		cur, err := s.events.Aggregate(ctx, mongo.Pipeline{
			bson.D{{Key: "$match", Value: bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: s.began}}}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "at", Value: -1}}}},
			bson.D{{Key: "$limit", Value: 25}},
			bson.D{{Key: "$lookup", Value: bson.D{
				{Key: "from", Value: "orders"},
				{Key: "localField", Value: "order"},
				{Key: "foreignField", Value: "_id"},
				{Key: "as", Value: "o"},
			}}},
		})
		if err != nil {
			return err
		}
		defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
		for cur.Next(ctx) {
			var row struct {
				Order  bson.ObjectID `bson:"order"`
				Orders []struct {
					ID bson.ObjectID `bson:"_id"`
				} `bson:"o"`
			}
			if cur.Decode(&row) != nil {
				continue
			}
			id := row.Order.Hex()
			// Only this process's own events can be judged. Another shop
			// running against the same database has orders this one knows
			// nothing about.
			if _, ours := s.wrote.Load(id); !ours {
				continue
			}
			// An order this run cancelled has had its events deleted first, so
			// an event of it surviving is the thing being looked for; but a
			// report taken while the cancellation is in flight can legitimately
			// see neither. Judged only for orders still live.
			if _, gone := s.removed.Load(id); gone {
				continue
			}
			switch {
			case len(row.Orders) == 0:
				s.violation("an activity report showed an event whose order it could not find: %s", id)
			case len(row.Orders) > 1:
				s.violation("an activity report joined %d orders to one event: %s", len(row.Orders), id)
			case row.Orders[0].ID != row.Order:
				s.violation("an activity report attached order %s to an event of %s",
					row.Orders[0].ID.Hex(), id)
			}
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
	announced := false
	for {
		select {
		case <-done:
			return
		default:
		}
		// The first attempt always starts live, whatever is stored.
		//
		// A token left behind by an earlier run names a position before this
		// run's own bookkeeping existed, so resuming from it delivers orders
		// this process never placed and every one of them is reported as a
		// phantom. That is the harness accusing the system of the harness's
		// own state, which has cost two wrong diagnoses here already.
		if !announced {
			s.clearToken(ctx)
		}
		// Reloaded from the collection on every attempt, not carried in a
		// variable, because that is what a consumer restarted by a deploy
		// does: the process it had the token in is gone.
		token, have := s.loadToken(ctx)
		opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
		if s.resyncEvery > 0 && (!announced || time.Since(s.lastResync) >= s.resyncEvery) {
			t0, ok := s.resync(ctx)
			if !ok {
				s.errs.Add(1)
				if !announced {
					close(ready)
				}
				return
			}
			s.clearToken(ctx)
			opts.SetStartAtOperationTime(&t0)
		} else if have {
			opts.SetStartAfter(token)
		}
		cur, err := s.orders.Watch(ctx, mongo.Pipeline{}, opts)
		if err != nil {
			if !announced {
				s.errs.Add(1)
				close(ready)
				return
			}
			// A token the oplog can no longer serve. Counted and started
			// fresh, because the alternative is a harness that stops checking
			// anything the moment the box gets busy.
			if isHistoryLost(err) {
				s.lostHistory.Add(1)
				s.clearToken(ctx)
				continue
			}
			s.errs.Add(1)
			return
		}
		if !announced {
			close(ready)
			announced = true
		} else {
			s.resumes.Add(1)
		}
		restart := s.consume(ctx, cur, done)
		_ = cur.Close(context.WithoutCancel(ctx))
		if !restart {
			return
		}
	}
}

// consume reads one cursor until it is time to stop or to restart.
//
// It reports whether the caller should reopen the stream. Split out from tail
// so that the restart is one decision in one place: a loop that both reads
// events and reopens its own cursor had two exits and took the wrong one on a
// cursor error, which reads as a consumer that silently stopped checking.
func (s *shop) consume(ctx context.Context, cur *mongo.ChangeStream, done <-chan struct{}) bool {
	since := time.Now()
	n := 0
	// stored says this cursor has written a token that a restart could come
	// back to. Without one, reopening starts the stream live and everything
	// written between the close and the reopen is never delivered, which this
	// harness would report at the end as the system losing events. A consumer
	// with nothing written down does not restart: it keeps reading.
	stored := false
	for {
		select {
		case <-done:
			return false
		default:
		}
		if s.resumeEvery > 0 && stored && time.Since(since) >= s.resumeEvery {
			return true
		}
		if s.resyncEvery > 0 && time.Since(s.lastResync) >= s.resyncEvery {
			return true
		}
		if !cur.TryNext(ctx) {
			if cur.Err() != nil {
				s.errs.Add(1)
				return false
			}
			continue
		}
		var ev struct {
			ID            bson.Raw `bson:"_id"`
			OperationType string   `bson:"operationType"`
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
			if _, again := s.seenInsert.Load(id); again {
				// A replay, which is what at-least-once delivery means and is
				// not a fault. Counted rather than reported: asserting that an
				// event arrives exactly once asserts more than a change stream
				// promises, and this harness has failed that way before.
				s.replayed.Add(1)
			}
			s.seenInsert.Store(id, true)
			s.keep(id, true)
			if _, ours := s.wrote.Load(id); !ours && !s.foreign(ctx, "insert", id, &s.foreignIns) {
				// Seeded orders are written before the consumer starts, so an
				// insert for one of those is a document arriving on the fast
				// tier rather than an order being placed.
				s.violation("the stream reported an order placed that nobody placed: %s", id)
			}
		case "delete":
			if _, again := s.seenDelete.Load(id); again {
				s.replayed.Add(1)
			}
			s.seenDelete.Store(id, true)
			s.keep(id, false)
			if _, ours := s.removed.Load(id); !ours && !s.foreign(ctx, "delete", id, &s.foreignDel) {
				s.violation("the stream reported an order cancelled that nobody cancelled: %s", id)
			}
		}
		n++
		// The token of an event this consumer has finished with, which is the
		// only position it is safe to come back to. Stored every few events
		// rather than every one, so a restart replays the handful in between:
		// see the note on checkpointEvery.
		if s.checkpointEvery > 0 && n%s.checkpointEvery == 0 && len(ev.ID) > 0 {
			s.saveToken(ctx, ev.ID)
			stored = true
		}
	}
}

// loadToken, saveToken and clearToken are the consumer's own bookkeeping, in a
// collection of its own on the cluster it is reading.
//
// Deliberately a collection nothing tiers. A consumer stores its position
// beside its own state, not in the application's tiered data, and this way the
// token survives on the fast tier alone, which is one more thing an archive
// repointed to has never held.
func (s *shop) loadToken(ctx context.Context) (bson.Raw, bool) {
	var doc struct {
		Token bson.Raw `bson:"token"`
	}
	err := s.db.Collection("stream_state").FindOne(ctx,
		bson.D{{Key: "_id", Value: s.consumerID()}}).Decode(&doc)
	if err != nil || len(doc.Token) == 0 {
		return nil, false
	}
	return doc.Token, true
}

func (s *shop) saveToken(ctx context.Context, token bson.Raw) {
	_, err := s.db.Collection("stream_state").UpdateOne(ctx,
		bson.D{{Key: "_id", Value: s.consumerID()}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "token", Value: token}, {Key: "at", Value: time.Now()}}}},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		s.errs.Add(1)
	}
}

func (s *shop) clearToken(ctx context.Context) {
	_, _ = s.db.Collection("stream_state").DeleteOne(ctx,
		bson.D{{Key: "_id", Value: s.consumerID()}})
}

// consumerID is where this application's consumer keeps its token. Each
// application has a consumer of its own, and two sharing one row would resume
// from each other's positions.
func (s *shop) consumerID() string {
	if s.parts > 1 {
		return fmt.Sprintf("indexer-%d", s.part)
	}
	return "indexer"
}

// intend writes down what this application is about to do to an order, before
// doing it, so another application's consumer can tell it from a phantom.
//
// Before, and never after: an entry written once the write returns would race
// the event the write produces, and the other consumer would read that race as
// a phantom. Left in place if the write then fails, because an entry for
// something that did not happen can only ever excuse an event that did not
// arrive, and one that did happen after reporting an error must be excused.
func (s *shop) intend(ctx context.Context, op string, id bson.ObjectID) {
	if s.ledger == nil {
		return
	}
	_, err := s.ledger.InsertOne(ctx, bson.D{
		{Key: "_id", Value: op + ":" + id.Hex()}, {Key: "part", Value: s.part}})
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		s.errs.Add(1)
	}
}

// foreign says an event this application did not cause was caused by another
// one sharing the collection, and counts it.
func (s *shop) foreign(ctx context.Context, op, id string, n *atomic.Int64) bool {
	if s.ledger == nil {
		return false
	}
	var e struct {
		Part int `bson:"part"`
	}
	err := s.ledger.FindOne(ctx, bson.D{{Key: "_id", Value: op + ":" + id}}).Decode(&e)
	if err != nil || e.Part == s.part {
		return false
	}
	n.Add(1)
	return true
}

// isHistoryLost says the resume token names a position the oplog no longer
// holds, which MongoDB reports as ChangeStreamHistoryLost (286).
func isHistoryLost(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Code == 286 || ce.HasErrorLabel("NonResumableChangeStreamError")
	}
	return false
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
		_, covered := s.coveredIns.Load(k)
		if _, ok := s.seenInsert.Load(k); !ok && !covered {
			missingIns++
		}
		return true
	})
	s.removed.Range(func(k, _ any) bool {
		_, covered := s.coveredDel.Load(k)
		if _, ok := s.seenDelete.Load(k); !ok && !covered {
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

	// Printed whatever it says, because a run that never resumed proves nothing
	// about resuming and the absence of a violation would read as proof.
	fmt.Printf("\nconsumer: %d resume(s) from a stored token, %d replayed event(s)",
		s.resumes.Load(), s.replayed.Load())
	if lost := s.lostHistory.Load(); lost > 0 {
		fmt.Printf(", %d token(s) older than the oplog", lost)
	}
	fmt.Print("\n")
	// Printed for the same reason again: a run that never fell behind proves
	// nothing about recovering from it.
	if s.resyncEvery > 0 {
		fmt.Printf("resync: fell behind the oplog %d time(s), told so %d time(s), scans read %s orders\n",
			s.resyncs.Load(), s.told.Load(), commas(int(s.scanned.Load())))
	}
	// Printed for the same reason. A shared run whose consumer saw nothing of
	// the other application proves nothing about sharing a stream.
	if s.parts > 1 {
		fmt.Printf("shared: application %d of %d, the stream carried %d insert(s) and %d delete(s) of the others'\n",
			s.part, s.parts, s.foreignIns.Load(), s.foreignDel.Load())
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
