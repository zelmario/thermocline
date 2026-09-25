// A probe for the feeder's write order across namespaces.
//
// An application writes an order and then its event, always in that order, so
// no state of the fast tier ever holds an event without its order. The archive
// should never hold one either, at any instant a reader can land on. This
// writes such pairs through the fast tier while a reader polls the archive
// directly for events whose order it does not have, and counts every sighting.
//
// A sighting here is not mirroring lag. Lag makes the archive hold less, in
// the order it was written; this is the archive holding the second write of a
// pair without the first.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	hotURI := flag.String("hot", "", "fast tier")
	coldURI := flag.String("cold", "", "archive")
	db := flag.String("db", "probecross", "database")
	dur := flag.Duration("for", 60*time.Second, "how long to write")
	writers := flag.Int("writers", 4, "concurrent writers")
	flag.Parse()
	ctx := context.Background()
	hot, err := mongo.Connect(options.Client().ApplyURI(*hotURI))
	if err != nil {
		fail(err)
	}
	cold, err := mongo.Connect(options.Client().ApplyURI(*coldURI))
	if err != nil {
		fail(err)
	}
	orders, events := hot.Database(*db).Collection("orders"), hot.Database(*db).Collection("events")
	cOrders, cEvents := cold.Database(*db).Collection("orders"), cold.Database(*db).Collection("events")

	var pairs, polls, sightings atomic.Int64
	stop := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) {
				id := bson.NewObjectID()
				if _, err := orders.InsertOne(ctx, bson.D{{Key: "_id", Value: id}, {Key: "at", Value: time.Now()}}); err != nil {
					continue
				}
				if _, err := events.InsertOne(ctx, bson.D{{Key: "order", Value: id}, {Key: "at", Value: time.Now()}}); err != nil {
					continue
				}
				pairs.Add(1)
			}
		}()
	}
	seen := map[bson.ObjectID]bool{}
	sighted := map[bson.ObjectID]bool{}
	var first bson.ObjectID
	var since time.Time
	for time.Now().Before(stop.Add(3 * time.Second)) {
		polls.Add(1)
		cur, err := cEvents.Find(ctx, bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: since}}}},
			options.Find().SetProjection(bson.D{{Key: "order", Value: 1}, {Key: "at", Value: 1}}))
		if err != nil {
			continue
		}
		var ids []bson.ObjectID
		latest := since
		for cur.Next(ctx) {
			var e struct {
				Order bson.ObjectID `bson:"order"`
				At    time.Time     `bson:"at"`
			}
			if cur.Decode(&e) == nil && !seen[e.Order] {
				ids = append(ids, e.Order)
				if e.At.After(latest) {
					latest = e.At
				}
			}
		}
		_ = cur.Close(ctx)
		if len(ids) == 0 {
			continue
		}
		oc, err := cOrders.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
			options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
		if err != nil {
			continue
		}
		have := map[bson.ObjectID]bool{}
		for oc.Next(ctx) {
			var d struct {
				ID bson.ObjectID `bson:"_id"`
			}
			if oc.Decode(&d) == nil {
				have[d.ID] = true
			}
		}
		_ = oc.Close(ctx)
		for _, id := range ids {
			if have[id] {
				seen[id] = true
			} else if !sighted[id] {
				sighted[id] = true
				sightings.Add(1)
				if first.IsZero() {
					first = id
				}
			}
		}
		// Look back a second, so an event read without its order is asked
		// about again next poll rather than forgotten.
		since = latest.Add(-time.Second)
	}
	wg.Wait()
	fmt.Printf("%d pairs written, %d archive polls, %d sightings of an event whose order the archive did not hold",
		pairs.Load(), polls.Load(), sightings.Load())
	if !first.IsZero() {
		fmt.Printf(", first %s", first.Hex())
	}
	fmt.Println()
	if sightings.Load() > 0 {
		os.Exit(1)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
