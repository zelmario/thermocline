package main

// The store workload: shoppers and a back office against one shop, instead of
// reads by _id.
//
// Reads by _id were never how a shop reads. A shop browses a category in price
// order, opens product pages, fills carts, checks out, and shows a customer
// their newest orders, and behind it a worker ships what was placed and
// somebody looks at today's sales. The catalogue and the customers are small
// and read constantly; the orders only grow. That is the shape tiering is for,
// and the one this measures.
//
//	bench -uri URI -load-store                   stock products and customers
//	bench -uri URI -workload store -for 10m      shop
//
// Products, customers and carts are untiered. In a tiered deployment they live
// on the fast tier, so -load-store goes to the fast tier there and to the plain
// cluster otherwise. Orders are the 40 million already loaded with -load.

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	products   = 20000
	categories = 200
	pageSize   = 24
)

func sku(i int) string      { return fmt.Sprintf("S%05d", i) }
func category(i int) string { return fmt.Sprintf("cat%03d", i) }
func custID(i int) string   { return fmt.Sprintf("c%06d", i) }

// resetShopTo puts a loaded shop back the way the load left it, so a cluster
// that took an hour to fill can be used by test after test.
//
// It can be done without reloading because the runs barely touch history. The
// orders they place are dated after the load ended, and shipping only moves
// those: the loaded ones are settled. Prices, stock and carts change, and those
// are reloaded, which takes seconds. Customers are only read. What Thermocline
// wrote beside the data, its own databases, goes too.
func resetShopTo(ctx context.Context, cl *mongo.Client, db *mongo.Database, orders *mongo.Collection) error {
	var m marker
	if err := db.Collection("bench_meta").FindOne(ctx, bson.D{{Key: "_id", Value: orders.Name()}}).Decode(&m); err != nil {
		return fmt.Errorf("no load recorded for %s: %w", orders.Name(), err)
	}
	end := m.Start.Add(m.Period)
	res, err := orders.DeleteMany(ctx, bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: end}}}})
	if err != nil {
		return err
	}
	fmt.Printf("removed %d orders placed after the load ended at %s\n", res.DeletedCount, end.UTC().Format(time.RFC3339))
	if err := loadProducts(ctx, db, 800); err != nil {
		return err
	}
	if err := db.Collection("carts").Drop(ctx); err != nil {
		return err
	}
	names, err := cl.ListDatabaseNames(ctx, bson.D{{Key: "name", Value: bson.D{{Key: "$regex", Value: "^_thermocline"}}}})
	if err != nil {
		return err
	}
	for _, n := range names {
		if err := cl.Database(n).Drop(ctx); err != nil {
			return err
		}
	}
	n, err := orders.EstimatedDocumentCount(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("restocked %d products, emptied the carts, dropped %v; %d orders\n", products, names, n)
	return nil
}

// loadStore stocks the catalogue and the customer list. The products' skus are
// the ones the loaded orders already name, so top sellers and stock agree.
func loadStore(ctx context.Context, db *mongo.Database, pad int) error {
	if err := loadProducts(ctx, db, pad); err != nil {
		return err
	}
	return loadCustomers(ctx, db)
}

// loadProducts stocks the catalogue, the same every time.
func loadProducts(ctx context.Context, db *mongo.Database, pad int) error {
	rng := rand.New(rand.NewSource(11))
	pc := db.Collection("products")
	_ = pc.Drop(ctx)
	docs := make([]any, 0, 1000)
	for i := 0; i < products; i++ {
		docs = append(docs, bson.D{
			{Key: "_id", Value: sku(i)},
			{Key: "category", Value: category(i % categories)},
			{Key: "name", Value: fmt.Sprintf("Product %d", i)},
			{Key: "price", Value: float64(100+rng.Intn(50000)) / 100},
			{Key: "stock", Value: 1000 + rng.Intn(9000)},
			{Key: "rating", Value: float64(10+rng.Intn(40)) / 10},
			{Key: "description", Value: padding(rng, pad/2)},
		})
		if len(docs) == cap(docs) {
			if _, err := pc.InsertMany(ctx, docs); err != nil {
				return err
			}
			docs = docs[:0]
		}
	}
	if _, err := pc.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "category", Value: 1}, {Key: "price", Value: 1}}}); err != nil {
		return err
	}
	return nil
}

// loadCustomers stocks the customer list.
func loadCustomers(ctx context.Context, db *mongo.Database) error {
	rng := rand.New(rand.NewSource(12))
	docs := make([]any, 0, 1000)
	cc := db.Collection("customers")
	_ = cc.Drop(ctx)
	for i := 0; i < customers; i++ {
		docs = append(docs, bson.D{
			{Key: "_id", Value: custID(i)},
			{Key: "name", Value: fmt.Sprintf("Customer %d", i)},
			{Key: "email", Value: fmt.Sprintf("customer%d@example.com", i)},
			{Key: "address", Value: padding(rng, 120)},
			{Key: "since", Value: time.Now().Add(-time.Duration(rng.Intn(1500)) * 24 * time.Hour)},
		})
		if len(docs) == cap(docs) {
			if _, err := cc.InsertMany(ctx, docs); err != nil {
				return err
			}
			docs = docs[:0]
		}
	}
	_ = db.Collection("carts").Drop(ctx)
	fmt.Printf("stocked %d products and %d customers in %s\n", products, customers, db.Name())
	return nil
}

func padding(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	for k := range b {
		b[k] = byte(33 + rng.Intn(94))
	}
	return string(b)
}

// mixEntry is one step a shopper can take, and its share in thousandths.
type mixEntry struct {
	name  string
	share int
}

// The shopper's mix, in thousandths of what one shopper does next.
//
// "store" is the first mix, and it writes far more than a shop does: one step
// in sixteen is a checkout, which at 32 shoppers flat out was about 245 orders
// a second, so the writes decided every result. "real" is the shape a shop's
// traffic takes: product pages and browsing are two thirds of it, about one
// step in seventy buys something, and a support agent opens an old order now
// and then. The shares are a design choice, not measured traffic.
var mixes = map[string][]mixEntry{
	"store": storeMix,
	"real": {
		{"browse a category", 300},
		{"product page", 385},
		{"filter by price", 80},
		{"add to cart", 60},
		{"checkout", 15},
		{"my orders", 60},
		{"open one of my orders", 40},
		{"customer profile", 40},
		{"old order (support)", 20},
	},
}

var storeMix = []mixEntry{
	{"browse a category", 260},
	{"product page", 240},
	{"filter by price", 60},
	{"add to cart", 120},
	{"checkout", 60},
	{"my orders", 100},
	{"open one of my orders", 80},
	{"customer profile", 40},
	{"old order (support)", 40},
}

type store struct {
	w      *workload
	db     *mongo.Database
	orders *mongo.Collection
	placed atomic.Int64
	// rate, when set, is the shoppers' operations a second in all. See run.
	rate int
	// mix is the shopper's mix, one of mixes, unless sessions is set.
	mix []mixEntry
	// sessions plans each shopper's steps a visit at a time. See planVisit.
	sessions bool
	shipped  atomic.Int64
}

// shopper is one client's session state: who is shopping, and the orders the
// last "my orders" page showed, which is where an order detail is opened from.
type shopper struct {
	rng    *rand.Rand
	custZ  *rand.Zipf
	catZ   *rand.Zipf
	prodZ  *rand.Zipf
	cust   string
	recent []bson.ObjectID
	ops    int
	// plan is what is left of the current visit. See planVisit.
	plan []string
}

// planVisit is one visit to the shop, the shape of a common one.
//
// Three in four visitors never log in: they land on a category or a product,
// look around a few pages and leave. About one in four is a customer, who may
// check their orders first, browses, and in about a third of visits puts things
// in the cart, of which a quarter are bought: two purchases in a hundred
// visits. One visit in a hundred is a support agent opening an old order. The
// shares are a design choice, not measured traffic.
func (p *shopper) planVisit() {
	p.recent = p.recent[:0]
	r := p.rng.Float64()
	switch {
	case r < 0.01:
		p.cust = ""
		p.plan = append(p.plan, "old order (support)")
		return
	case r < 0.76:
		p.cust = ""
		p.browse(2 + p.geom(3))
		return
	}
	// Most visits are from a customer who seldom comes back, so four in five
	// are any customer at all and one in five a regular. A Zipf over all of
	// them alone put half the logged-in visits on the same thousand customers
	// out of 25 million, which makes every cache look better than it is.
	if p.rng.Intn(5) == 0 {
		p.cust = custID(int(p.custZ.Uint64()))
	} else {
		p.cust = custID(p.rng.Intn(customers))
	}
	if p.rng.Float64() < 0.25 {
		p.plan = append(p.plan, "my orders")
		if p.rng.Float64() < 0.6 {
			p.plan = append(p.plan, "open one of my orders")
		}
	}
	if p.rng.Float64() < 0.2 {
		p.plan = append(p.plan, "customer profile")
	}
	p.browse(1 + p.geom(3))
	if p.rng.Float64() < 0.35 {
		for n := 1 + p.rng.Intn(3); n > 0; n-- {
			p.plan = append(p.plan, "product page", "add to cart")
		}
		if p.rng.Float64() < 0.25 {
			p.plan = append(p.plan, "checkout")
		}
	}
}

// browse adds n pages of looking around: categories, products, a price filter.
func (p *shopper) browse(n int) {
	for i := 0; i < n; i++ {
		switch r := p.rng.Intn(100); {
		case r < 40:
			p.plan = append(p.plan, "browse a category")
		case r < 85:
			p.plan = append(p.plan, "product page")
		default:
			p.plan = append(p.plan, "filter by price")
		}
	}
}

func (p *shopper) geom(mean float64) int { return int(p.rng.ExpFloat64() * mean) }

func (s *store) newShopper(seed int64) *shopper {
	rng := rand.New(rand.NewSource(seed))
	return &shopper{
		rng:   rng,
		custZ: rand.NewZipf(rng, 1.1, 8, uint64(customers-1)),
		catZ:  rand.NewZipf(rng, 1.2, 2, categories-1),
		prodZ: rand.NewZipf(rng, 1.3, 1, products/categories-1),
	}
}

// pickProduct takes a popular category, then a popular product in it.
func (p *shopper) pickProduct() int {
	return int(p.catZ.Uint64()) + categories*int(p.prodZ.Uint64())
}

func (s *store) run(ctx context.Context, d time.Duration, conc int, seed int64) *report {
	rep := &report{Clients: conc, Ops: map[string]*opStat{}}
	var mu sync.Mutex
	merge := func(local map[string]*opStat) {
		mu.Lock()
		defer mu.Unlock()
		for name, st := range local {
			all, ok := rep.Ops[name]
			if !ok {
				all = &opStat{}
				rep.Ops[name] = all
			}
			all.lat = append(all.lat, st.lat...)
			all.Errors += st.Errors
		}
	}
	timed := func(local map[string]*opStat, name string, f func() error) {
		st, ok := local[name]
		if !ok {
			st = &opStat{}
			local[name] = st
		}
		t0 := time.Now()
		if err := f(); err != nil {
			st.Errors++
		}
		st.lat = append(st.lat, time.Since(t0))
	}

	deadline := time.Now().Add(d)
	began := time.Now()
	// At a set rate, each step takes the next slot on one shared schedule and
	// waits for it. A shopper running late does not wait, so a target that
	// cannot keep up shows it as a lower rate, and the latency is measured from
	// when the step began.
	var slots atomic.Int64
	pace := func() {
		if s.rate <= 0 {
			return
		}
		at := began.Add(time.Duration(slots.Add(1)) * time.Second / time.Duration(s.rate))
		if wait := time.Until(at); wait > 0 {
			time.Sleep(wait)
		}
	}
	var wg sync.WaitGroup
	for k := 0; k < conc; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			p := s.newShopper(seed*1000 + int64(k))
			local := map[string]*opStat{}
			for time.Now().Before(deadline) {
				var name string
				if s.sessions {
					if len(p.plan) == 0 {
						p.planVisit()
					}
					name, p.plan = p.plan[0], p.plan[1:]
				} else {
					// A new customer every twenty or so steps: a session.
					if p.ops%20 == 0 {
						p.cust = custID(int(p.custZ.Uint64()))
						p.recent = p.recent[:0]
					}
					r, acc := p.rng.Intn(1000), 0
					for _, op := range s.mix {
						acc += op.share
						if r < acc {
							name = op.name
							break
						}
					}
				}
				p.ops++
				pace()
				timed(local, name, func() error { return s.shopperStep(ctx, p, name) })
			}
			merge(local)
		}(k)
	}
	// The back office, at a fixed pace whatever the shopper count.
	// office runs a back office task every so often. ready, when given, says
	// whether there is anything to do; a turn with nothing to do is not timed.
	office := func(name string, every time.Duration, f func(*rand.Rand) error, ready ...func() bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + int64(len(name))))
			local := map[string]*opStat{}
			for time.Now().Before(deadline) {
				if len(ready) == 0 || ready[0]() {
					timed(local, name, func() error { return f(rng) })
				}
				time.Sleep(every)
			}
			merge(local)
		}()
	}
	// At a set rate the warehouse ships as fast as the shop sells, so the
	// orders waiting to ship neither pile up nor run out.
	shipEvery := time.Duration(0)
	if s.rate > 0 {
		shipEvery = 20 * time.Millisecond
		for _, op := range s.mix {
			if op.name == "checkout" && op.share > 0 {
				shipEvery = time.Second * 1000 / time.Duration(s.rate*op.share)
			}
		}
	}
	if s.sessions {
		// Visits decide how much is sold, so the warehouse ships what was
		// placed, as it is placed, rather than at a rate of its own.
		office("ship the oldest placed order", 10*time.Millisecond, func(rng *rand.Rand) error {
			s.shipped.Add(1)
			return s.ship(rng)
		}, func() bool { return s.shipped.Load() < s.placed.Load() })
	} else {
		office("ship the oldest placed order", shipEvery, s.ship)
	}
	office("price change", time.Second, s.reprice)
	office("report: sales by hour, 24h", 5*time.Second, s.salesByHour)
	office("report: top sellers, 7 days", 15*time.Second, s.topSellers)
	wg.Wait()
	rep.Seconds = time.Since(began).Seconds()
	finish(rep)
	return rep
}

func (s *store) shopperStep(ctx context.Context, p *shopper, name string) error {
	pc, cc, carts := s.db.Collection("products"), s.db.Collection("customers"), s.db.Collection("carts")
	drain := func(cur *mongo.Cursor, err error) (int, error) {
		if err != nil {
			return 0, err
		}
		defer func() { _ = cur.Close(ctx) }()
		n := 0
		for cur.Next(ctx) {
			n++
		}
		return n, cur.Err()
	}
	switch name {
	case "browse a category":
		page := 0
		if p.rng.Intn(4) == 0 {
			page = 1 + p.rng.Intn(3)
		}
		_, err := drain(pc.Find(ctx, bson.D{{Key: "category", Value: category(int(p.catZ.Uint64()))}},
			options.Find().SetSort(bson.D{{Key: "price", Value: 1}}).SetSkip(int64(page*pageSize)).SetLimit(pageSize).
				SetProjection(bson.D{{Key: "description", Value: 0}})))
		return err
	case "product page":
		err := pc.FindOne(ctx, bson.D{{Key: "_id", Value: sku(p.pickProduct())}}).Err()
		return ignoreNone(err)
	case "filter by price":
		lo := float64(p.rng.Intn(400))
		_, err := drain(pc.Find(ctx, bson.D{
			{Key: "category", Value: category(int(p.catZ.Uint64()))},
			{Key: "price", Value: bson.D{{Key: "$gte", Value: lo}, {Key: "$lte", Value: lo + 50}}},
		}, options.Find().SetSort(bson.D{{Key: "price", Value: 1}}).SetLimit(pageSize).
			SetProjection(bson.D{{Key: "description", Value: 0}})))
		return err
	case "add to cart":
		_, err := carts.UpdateOne(ctx, bson.D{{Key: "_id", Value: p.cust}}, bson.D{
			{Key: "$push", Value: bson.D{{Key: "items", Value: bson.D{
				{Key: "sku", Value: sku(p.pickProduct())}, {Key: "qty", Value: 1 + p.rng.Intn(2)}}}}},
			{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: time.Now()}}},
		}, options.UpdateOne().SetUpsert(true))
		return err
	case "checkout":
		return s.checkout(ctx, p)
	case "my orders":
		cur, err := s.orders.Find(ctx, bson.D{{Key: "customer", Value: p.cust}},
			options.Find().SetSort(bson.D{{Key: "at", Value: -1}}).SetLimit(10).
				SetProjection(bson.D{{Key: "note", Value: 0}}))
		if err != nil {
			return err
		}
		defer func() { _ = cur.Close(ctx) }()
		p.recent = p.recent[:0]
		for cur.Next(ctx) {
			if id, ok := cur.Current.Lookup("_id").ObjectIDOK(); ok {
				p.recent = append(p.recent, id)
			}
		}
		return cur.Err()
	case "open one of my orders":
		if len(p.recent) == 0 {
			return s.shopperStep(ctx, p, "my orders")
		}
		id := p.recent[p.rng.Intn(len(p.recent))]
		return ignoreNone(s.orders.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Err())
	case "customer profile":
		return ignoreNone(cc.FindOne(ctx, bson.D{{Key: "_id", Value: p.cust}}).Err())
	case "old order (support)":
		// A return or a dispute: any order of the last three years, evenly.
		seq := p.rng.Intn(s.w.n)
		return ignoreNone(s.orders.FindOne(ctx, bson.D{{Key: "_id", Value: idAt(s.w.atOf(seq), seq)}}).Err())
	}
	return fmt.Errorf("unknown step %q", name)
}

// checkout is what pressing the button costs: read the cart, place the order,
// take the stock, empty the cart. Timed as one operation.
func (s *store) checkout(ctx context.Context, p *shopper) error {
	carts, pc := s.db.Collection("carts"), s.db.Collection("products")
	var cart struct {
		Items []struct {
			SKU string `bson:"sku"`
			Qty int    `bson:"qty"`
		} `bson:"items"`
	}
	err := carts.FindOne(ctx, bson.D{{Key: "_id", Value: p.cust}}).Decode(&cart)
	if err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	if len(cart.Items) == 0 {
		cart.Items = append(cart.Items, struct {
			SKU string `bson:"sku"`
			Qty int    `bson:"qty"`
		}{sku(p.pickProduct()), 1})
	}
	items, total := bson.A{}, 0.0
	for _, it := range cart.Items {
		price := float64(100+p.rng.Intn(50000)) / 100
		total += price * float64(it.Qty)
		items = append(items, bson.D{{Key: "sku", Value: it.SKU}, {Key: "qty", Value: it.Qty}, {Key: "price", Value: price}})
	}
	now := time.Now()
	seq := s.w.n + int(s.placed.Add(1)) + int(p.rng.Int31())<<20
	if _, err := s.orders.InsertOne(ctx, bson.D{
		{Key: "_id", Value: idAt(now, seq)}, {Key: "at", Value: now}, {Key: "customer", Value: p.cust},
		{Key: "status", Value: "placed"}, {Key: "total", Value: total}, {Key: "items", Value: items},
		{Key: "note", Value: padding(p.rng, s.w.pad)},
	}); err != nil {
		return err
	}
	for _, it := range cart.Items {
		if _, err := pc.UpdateOne(ctx, bson.D{{Key: "_id", Value: it.SKU}},
			bson.D{{Key: "$inc", Value: bson.D{{Key: "stock", Value: -it.Qty}}}}); err != nil {
			return err
		}
	}
	_, err = carts.UpdateOne(ctx, bson.D{{Key: "_id", Value: p.cust}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "items", Value: bson.A{}}}}})
	return err
}

// ship takes the oldest order still placed and marks it shipped, which is what
// a fulfilment worker does all day.
func (s *store) ship(rng *rand.Rand) error {
	err := s.orders.FindOneAndUpdate(context.Background(),
		bson.D{{Key: "status", Value: "placed"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "shipped"}, {Key: "shippedAt", Value: time.Now()}}}},
		options.FindOneAndUpdate().SetSort(bson.D{{Key: "at", Value: 1}})).Err()
	if err == mongo.ErrNoDocuments {
		time.Sleep(50 * time.Millisecond)
		return nil
	}
	return err
}

func (s *store) reprice(rng *rand.Rand) error {
	_, err := s.db.Collection("products").UpdateOne(context.Background(),
		bson.D{{Key: "_id", Value: sku(rng.Intn(products))}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "price", Value: float64(100+rng.Intn(50000)) / 100}}}})
	return err
}

// loadEnd is the newest instant of the loaded history. The reports read
// windows ending there, not now.
//
// Every checkout writes an order dated now, about 150,000 in a ten minute run
// at 32 shoppers, so a window ending now grows with every run: measured on
// 2026-10-21, after a day of runs the last 24 hours held 1,872,509 orders
// against 36,530 in a loaded day, and the hourly report read all of them every
// five seconds. Each run was measured against more data than the one before,
// so no two runs were comparable. A window of the loaded history is the same
// orders in every run, against every target.
func (s *store) loadEnd() time.Time { return s.w.start.Add(s.w.period) }

func (s *store) salesByHour(rng *rand.Rand) error {
	end := s.loadEnd()
	cur, err := s.orders.Aggregate(context.Background(), mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: end.Add(-24 * time.Hour)}, {Key: "$lt", Value: end}}}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$hour", Value: "$at"}}},
			{Key: "orders", Value: bson.D{{Key: "$sum", Value: 1}}}, {Key: "revenue", Value: bson.D{{Key: "$sum", Value: "$total"}}}}}},
	})
	if err != nil {
		return err
	}
	defer func() { _ = cur.Close(context.Background()) }()
	for cur.Next(context.Background()) {
	}
	return cur.Err()
}

func (s *store) topSellers(rng *rand.Rand) error {
	end := s.loadEnd()
	cur, err := s.orders.Aggregate(context.Background(), mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: end.Add(-7 * 24 * time.Hour)}, {Key: "$lt", Value: end}}}}}},
		{{Key: "$unwind", Value: "$items"}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$items.sku"}, {Key: "sold", Value: bson.D{{Key: "$sum", Value: "$items.qty"}}}}}},
		{{Key: "$sort", Value: bson.D{{Key: "sold", Value: -1}}}},
		{{Key: "$limit", Value: 10}},
	})
	if err != nil {
		return err
	}
	defer func() { _ = cur.Close(context.Background()) }()
	for cur.Next(context.Background()) {
	}
	return cur.Err()
}

func ignoreNone(err error) error {
	if err == mongo.ErrNoDocuments {
		return nil
	}
	return err
}
