package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// refCollation is the one comparison the reference is read and written under.
// en/strength 2 folds case, so Ref000123 and ref000123 are one value: that is
// what the shop promises a customer who types it back, and what its unique
// index has to enforce.
func refCollation() *options.Collation {
	return &options.Collation{Locale: "en", Strength: 2}
}

// orderRef is what is printed on the order. Mixed case on purpose: stored one
// way, typed another, which is the whole point of the collation above.
func orderRef(n int) string { return fmt.Sprintf("Ref%06d", n) }

// marker records how the shop was stocked, so a later run against the same
// database asks for customers and products that exist, and reads dates that
// have orders in them. Without it a second run invents its own ids, every read
// misses, and the numbers describe nothing.
type marker struct {
	Start     time.Time `bson:"start"`
	Days      int       `bson:"days"`
	Products  int       `bson:"products"`
	Customers int       `bson:"customers"`
	Orders    int       `bson:"orders"`
}

func (s *shop) readMarker(ctx context.Context) (marker, error) {
	var m marker
	err := s.db.Collection("store_marker").
		FindOne(ctx, bson.D{{Key: "_id", Value: "stocked"}}).Decode(&m)
	return m, err
}

// idAt builds an ObjectID whose timestamp is the order's date. Real order ids
// carry their time, and the tiering decides what to evict from a date, so an id
// that disagrees with the document's own date would make every result confusing
// for the wrong reason.
func idAt(at time.Time, n int) bson.ObjectID {
	var id bson.ObjectID
	binary.BigEndian.PutUint32(id[0:4], uint32(at.Unix()))
	binary.BigEndian.PutUint32(id[4:8], uint32(n))
	binary.BigEndian.PutUint32(id[8:12], uint32(at.Nanosecond()))
	return id
}

func (s *shop) seed(ctx context.Context, orders int) error {
	for _, c := range []*mongo.Collection{s.orders, s.events, s.products, s.customers,
		s.db.Collection("store_marker")} {
		if err := c.Drop(ctx); err != nil {
			return err
		}
	}

	rng := rand.New(rand.NewSource(7))

	// The catalogue. Small, read constantly, never tiered: no shop archives the
	// products it is still selling.
	docs := make([]any, 0, s.nProducts)
	for i := 0; i < s.nProducts; i++ {
		docs = append(docs, bson.D{
			{Key: "_id", Value: i},
			{Key: "sku", Value: fmt.Sprintf("s%04d", i)},
			{Key: "name", Value: fmt.Sprintf("product %d", i)},
			{Key: "price", Value: float64(100+rng.Intn(9000)) / 100},
			{Key: "stock", Value: 10 + rng.Intn(500)},
		})
	}
	if _, err := s.products.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("products: %w", err)
	}

	docs = docs[:0]
	for i := 0; i < s.nCustomers; i++ {
		docs = append(docs, bson.D{
			{Key: "_id", Value: fmt.Sprintf("c%06d", i)},
			{Key: "name", Value: fmt.Sprintf("customer %d", i)},
			{Key: "joined", Value: s.start.Add(time.Duration(rng.Int63n(int64(s.period))))},
		})
		if len(docs) == 2000 {
			if _, err := s.customers.InsertMany(ctx, docs); err != nil {
				return fmt.Errorf("customers: %w", err)
			}
			docs = docs[:0]
		}
	}
	if len(docs) > 0 {
		if _, err := s.customers.InsertMany(ctx, docs); err != nil {
			return fmt.Errorf("customers: %w", err)
		}
	}

	// Orders, spread over the period. This is the collection worth tiering: it
	// only grows, and almost all the reading happens in the newest slice of it.
	type batch struct{ orders, events []any }
	work := make(chan batch, 8)
	var wg sync.WaitGroup
	var insertErr error
	var mu sync.Mutex
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range work {
				_, err := s.orders.InsertMany(ctx, b.orders, options.InsertMany().SetOrdered(false))
				if err == nil {
					_, err = s.events.InsertMany(ctx, b.events, options.InsertMany().SetOrdered(false))
				}
				if err != nil {
					mu.Lock()
					if insertErr == nil {
						insertErr = err
					}
					mu.Unlock()
				}
			}
		}()
	}

	var b batch
	for i := 0; i < orders; i++ {
		at := s.start.Add(time.Duration(float64(i) / float64(orders) * float64(s.period)))
		cust := fmt.Sprintf("c%06d", rng.Intn(s.nCustomers))
		lines := bson.A{}
		total := 0.0
		for l := 0; l < 1+rng.Intn(3); l++ {
			price := float64(100+rng.Intn(9000)) / 100
			qty := 1 + rng.Intn(3)
			total += price * float64(qty)
			lines = append(lines, bson.D{
				{Key: "sku", Value: fmt.Sprintf("s%04d", rng.Intn(s.nProducts))},
				{Key: "qty", Value: qty},
				{Key: "price", Value: price},
			})
		}
		id := idAt(at, i)
		b.orders = append(b.orders, bson.D{
			{Key: "_id", Value: id}, {Key: "customer", Value: cust}, {Key: "at", Value: at},
			{Key: "ref", Value: orderRef(i)},
			{Key: "lines", Value: lines},
			{Key: "total", Value: math.Round(total*100) / 100},
			{Key: "status", Value: []string{"placed", "shipped", "delivered"}[rng.Intn(3)]},
		})
		b.events = append(b.events, bson.D{
			{Key: "order", Value: id}, {Key: "at", Value: at}, {Key: "kind", Value: "placed"},
		})
		if len(b.orders) == 1000 {
			work <- b
			b = batch{}
			if i%20000 < 1000 {
				fmt.Printf("  %s orders\n", commas(i))
			}
		}
	}
	if len(b.orders) > 0 {
		work <- b
	}
	close(work)
	wg.Wait()
	if insertErr != nil {
		return fmt.Errorf("orders: %w", insertErr)
	}

	// The indexes the shop would have. Without them the reads are honest but
	// the numbers measure collection scans, not tiering.
	if _, err := s.orders.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "customer", Value: 1}, {Key: "at", Value: -1}}},
		{Keys: bson.D{{Key: "at", Value: -1}}},
		{Keys: bson.D{{Key: "customer", Value: 1}, {Key: "status", Value: 1}}},
	}); err != nil {
		return fmt.Errorf("order indexes: %w", err)
	}
	// The reference a customer types to track a parcel, and the index a shop
	// puts on it: unique, because two orders sharing one is a support call, and
	// case-insensitive, because nobody types it back the way it was printed.
	//
	// It goes on orders rather than on customers deliberately. Orders are the
	// collection this system tiers and evicts, so this is a unique constraint
	// whose other holder may be in the archive, which is the only place the key
	// registry does any work. A collated one is also the shape that hid four
	// defects until 2026-09-22, none of which this workload could reach.
	if _, err := s.orders.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "ref", Value: 1}},
		Options: options.Index().SetUnique(true).SetCollation(refCollation()),
	}); err != nil {
		return fmt.Errorf("order reference index: %w", err)
	}
	if _, err := s.events.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "order", Value: 1}},
	}); err != nil {
		return fmt.Errorf("event index: %w", err)
	}

	_, err := s.db.Collection("store_marker").InsertOne(ctx, bson.D{
		{Key: "_id", Value: "stocked"},
		{Key: "start", Value: s.start}, {Key: "days", Value: int(s.period / (24 * time.Hour))},
		{Key: "products", Value: s.nProducts}, {Key: "customers", Value: s.nCustomers},
		{Key: "orders", Value: orders},
	})
	return err
}
