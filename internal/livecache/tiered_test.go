package livecache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// The cache over a tiered collection, against what the router answers from
// both tiers, while the tiering itself goes on underneath.
//
// The test plays the tiering. "archive" holds every live order; "orders" is the
// fast tier, holding every order no older than a claim, and whatever was copied
// up. An evictor raises the claim and then deletes what fell below it, which is
// the order the real one keeps. A warmer copies orders up, one at a time as a
// read would and in bulk as lowering the claim does. The application inserts,
// updates and deletes, and an order it touches that only the archive holds is
// copied up first, which is what the router does with a write.
//
// So the fast tier's change stream carries what it does in production: the
// application's changes, evictions that are deletes of orders still alive, and
// copy-ups that are inserts of orders that already existed. The truth is the
// archive, which always holds the answer the router would give.
func TestTieredAgainstBothTiers(t *testing.T) {
	uri := os.Getenv("TC_TEST_HOT")
	if uri == "" {
		t.Skip("set TC_TEST_HOT to a replica set to run the live cache against MongoDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Disconnect(context.Background()) }()
	const ns = "tc_livet.orders"
	db := cl.Database("tc_livet")
	fast, archive := db.Collection("orders"), db.Collection("archive")
	_ = db.Drop(ctx)
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	// Orders a minute apart, so no two share an instant and the sort has one
	// right answer. The newest is now.
	// Most customers order often, so their pages are all above the claim. A
	// few order rarely, about thirty orders over the whole history, and their
	// pages reach below it, where the archive holds orders the fast tier does
	// not: the page the proof exists for. Old orders of theirs are copied up
	// often, as opening one does, so the fast tier holds a full page of theirs
	// with archived orders missing from the middle of it.
	const orders, customers, rare = 3000, 40, 10
	pick := func(r *rand.Rand) int {
		if r.Intn(10) == 0 {
			return customers + r.Intn(rare)
		}
		return r.Intn(customers)
	}
	base := time.Now().UTC().Truncate(time.Millisecond).Add(-orders * time.Minute)
	rng := rand.New(rand.NewSource(11))
	order := func(i int, at time.Time, cust int) bson.D {
		return bson.D{{Key: "_id", Value: fmt.Sprintf("o%06d", i)}, {Key: "at", Value: at},
			{Key: "customer", Value: fmt.Sprintf("c%02d", cust)}, {Key: "status", Value: "placed"},
			{Key: "stock", Value: int32(rng.Intn(9))}}
	}
	var all []any
	var rareIDs []string
	for i := 0; i < orders; i++ {
		d := order(i, base.Add(time.Duration(i)*time.Minute), pick(rng))
		if d[2].Value.(string) >= fmt.Sprintf("c%02d", customers) {
			rareIDs = append(rareIDs, d[0].Value.(string))
		}
		all = append(all, d)
	}
	if _, err := archive.InsertMany(ctx, all); err != nil {
		t.Fatal(err)
	}
	for _, co := range []*mongo.Collection{fast, archive} {
		if _, err := co.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "customer", Value: 1}, {Key: "at", Value: -1}}}); err != nil {
			t.Fatal(err)
		}
	}

	// The claim: every live order at or after from is in the fast tier. Every
	// tiering act and every application write holds mu, so the two
	// collections agree whenever it is free.
	var mu sync.Mutex
	from := base.Add(orders / 2 * time.Minute)
	if _, err := db.Collection("archive").Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: from}}}}}},
		{{Key: "$merge", Value: bson.D{{Key: "into", Value: "orders"}}}},
	}); err != nil {
		t.Fatal(err)
	}
	claim := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return from
	}

	c := New(64 << 20)
	c.Tiers(func(n string) bool { return n == ns }, func(_ context.Context, _ string, sort []SortKey, docs []bson.Raw) (int, time.Time) {
		if len(sort) == 0 || sort[0].Field != "at" || !sort[0].Desc {
			return 0, time.Time{}
		}
		f := claim()
		for i, d := range docs {
			if d.Lookup("at").Time().Before(f) {
				return i, time.Time{}
			}
		}
		return len(docs), time.Time{}
	}, func(ctx context.Context, _ string, eq bson.D, sort []SortKey, _ []bson.Raw, window int) ([]bson.Raw, bool) {
		// The archive is written in the same step as the fast tier here, so
		// it is always current, and the merge over both tiers is its answer.
		// Held under mu, so it sees no write half done.
		mu.Lock()
		defer mu.Unlock()
		sd := bson.D{}
		for _, k := range sort {
			dir := 1
			if k.Desc {
				dir = -1
			}
			sd = append(sd, bson.E{Key: k.Field, Value: dir})
		}
		o := options.Find().SetLimit(int64(window))
		if len(sd) > 0 {
			o.SetSort(sd)
		}
		cur, err := archive.Find(ctx, eq, o)
		if err != nil {
			return nil, false
		}
		var out []bson.Raw
		for cur.Next(ctx) {
			out = append(out, append(bson.Raw(nil), cur.Current...))
		}
		_ = cur.Close(ctx)
		return out, cur.Err() == nil
	})
	c.GroupArchive(func(ctx context.Context, _ string, filter, proj bson.D) ([]bson.Raw, bool) {
		mu.Lock()
		defer mu.Unlock()
		cur, err := archive.Find(ctx, filter, options.Find().SetProjection(proj))
		if err != nil {
			return nil, false
		}
		var out []bson.Raw
		for cur.Next(ctx) {
			out = append(out, append(bson.Raw(nil), cur.Current...))
		}
		_ = cur.Close(ctx)
		return out, cur.Err() == nil
	})
	// Groupings over both tiers, and their pipelines for asking the archive.
	var pipes sync.Map
	groupQ := func(r *rand.Rand) Query {
		var p bson.A
		switch r.Intn(3) {
		case 0:
			p = bson.A{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$customer"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
				{Key: "stock", Value: bson.D{{Key: "$sum", Value: "$stock"}}}}}}}
		case 1:
			cust := fmt.Sprintf("c%02d", customers+r.Intn(rare))
			p = bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "customer", Value: cust}}}},
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$hour", Value: "$at"}}}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}},
				bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: int32(1)}}}}}
		default:
			since := base.Add(time.Duration(r.Intn(4)*600) * time.Minute)
			p = bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: since}}}}}},
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$status"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
					{Key: "stock", Value: bson.D{{Key: "$sum", Value: "$stock"}}}}}}}
		}
		q, ok := ParseGroupAgg(ns, mustRaw(bson.D{{Key: "aggregate", Value: "orders"}, {Key: "pipeline", Value: p}, {Key: "cursor", Value: bson.D{}}}))
		if !ok {
			panic(fmt.Sprintf("not understood: %v", p))
		}
		pipes.Store(q.entryKey(), p)
		return q
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go c.Run(runCtx, cl, streamClient(t, uri), func(string) bool { return false }, slog.New(slog.NewTextHandler(io.Discard, nil)))

	queries := func(r *rand.Rand) Query {
		cust := fmt.Sprintf("c%02d", r.Intn(customers))
		if r.Intn(2) == 0 {
			cust = fmt.Sprintf("c%02d", customers+r.Intn(rare))
		}
		if r.Intn(4) == 0 {
			q, _ := ParseFind(ns, mustRaw(bson.D{{Key: "find", Value: "orders"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: fmt.Sprintf("o%06d", r.Intn(orders+200))}}}, {Key: "limit", Value: int32(1)}}))
			return q
		}
		cmd := bson.D{{Key: "find", Value: "orders"}, {Key: "filter", Value: bson.D{{Key: "customer", Value: cust}}},
			{Key: "sort", Value: bson.D{{Key: "at", Value: int32(-1)}}}, {Key: "skip", Value: int32(r.Intn(2) * 10)}, {Key: "limit", Value: int32(10)}}
		if r.Intn(2) == 0 {
			cmd = append(cmd, bson.E{Key: "projection", Value: bson.D{{Key: "stock", Value: 0}}})
		}
		q, _ := ParseFind(ns, mustRaw(cmd))
		return q
	}

	// resident are orders copied up, by when. The evictor leaves them for a
	// while, as the real one does, which is what lets the fast tier hold a
	// page of a rare customer's orders with an archived one missing from it.
	resident := map[string]time.Time{}

	// copyUp brings an order up from the archive, if only the archive has it,
	// and says whether it did. Held under mu.
	copyUp := func(id string) (bool, error) {
		var d bson.Raw
		if err := archive.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&d); err != nil {
			if err == mongo.ErrNoDocuments {
				return false, nil
			}
			return false, err
		}
		_, err := fast.InsertOne(ctx, d)
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		if err == nil {
			resident[id] = time.Now()
		}
		return err == nil, err
	}
	// The claim moves within the original history, so customers keep enough
	// orders above it to prove a page and the evictor keeps finding some.
	lowest, highest := base.Add(500*time.Minute), base.Add((orders-300)*time.Minute)

	var wg sync.WaitGroup
	var ryw sync.Map
	var evicted, warmed, written int
	deadline := time.Now().Add(25 * time.Second)
	errs := make(chan error, 16)

	// The evictor: raise the claim, then delete what fell below it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(1))
		for time.Now().Before(deadline) {
			time.Sleep(time.Duration(200+r.Intn(300)) * time.Millisecond)
			mu.Lock()
			if from.Before(highest) {
				from = from.Add(time.Duration(5+r.Intn(20)) * time.Minute)
			}
			keep := bson.A{}
			for id, at := range resident {
				if time.Since(at) < 5*time.Second {
					keep = append(keep, id)
				} else {
					delete(resident, id)
				}
			}
			res, err := fast.DeleteMany(ctx, bson.D{{Key: "at", Value: bson.D{{Key: "$lt", Value: from}}},
				{Key: "_id", Value: bson.D{{Key: "$nin", Value: keep}}}})
			if res != nil {
				evicted += int(res.DeletedCount)
			}
			mu.Unlock()
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	// The warmer: copy-ups of single old orders, as a read brings them, and
	// now and then the claim lowered with everything it newly covers copied up
	// before it is.
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(2))
		for time.Now().Before(deadline) {
			time.Sleep(time.Duration(20+r.Intn(30)) * time.Millisecond)
			mu.Lock()
			var err error
			if r.Intn(10) == 0 && from.After(lowest) {
				lower := from.Add(-time.Duration(5+r.Intn(20)) * time.Minute)
				var cur *mongo.Cursor
				cur, err = archive.Find(ctx, bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: lower}, {Key: "$lt", Value: from}}}})
				for err == nil && cur.Next(ctx) {
					_, ierr := fast.InsertOne(ctx, cur.Current)
					if ierr == nil {
						warmed++
					} else if !mongo.IsDuplicateKeyError(ierr) {
						err = ierr
					}
				}
				if err == nil {
					from = lower
				}
			} else {
				var up bool
				id := fmt.Sprintf("o%06d", r.Intn(orders))
				if r.Intn(2) == 0 {
					id = rareIDs[r.Intn(len(rareIDs))]
				}
				up, err = copyUp(id)
				if up {
					warmed++
				}
			}
			mu.Unlock()
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	// The application. After each write the writer reads its customer's newest
	// orders, and must never be told the state before its own write.
	next := orders
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(10 + w)))
			sess, _ := cl.StartSession()
			defer sess.EndSession(context.Background())
			sctx := mongo.NewSessionContext(ctx, sess)
			for time.Now().Before(deadline) {
				time.Sleep(time.Duration(r.Intn(4)) * time.Millisecond)
				cust := pick(r)
				mu.Lock()
				var err error
				switch r.Intn(4) {
				case 0:
					d := order(next, time.Now().UTC().Truncate(time.Millisecond).Add(time.Duration(next)*time.Millisecond), cust)
					next++
					if _, err = fast.InsertOne(sctx, d); err == nil {
						_, err = archive.InsertOne(ctx, d)
					}
				case 1:
					id := fmt.Sprintf("o%06d", r.Intn(next))
					if _, err = copyUp(id); err == nil {
						if _, err = fast.DeleteOne(sctx, bson.D{{Key: "_id", Value: id}}); err == nil {
							_, err = archive.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
						}
					}
				default:
					// A change of status, or the order moved to another
					// customer, which moves it between lists.
					id := fmt.Sprintf("o%06d", r.Intn(next))
					set := bson.D{{Key: "status", Value: "shipped"}, {Key: "stock", Value: int32(r.Intn(9))}}
					if r.Intn(2) == 0 {
						set = bson.D{{Key: "customer", Value: fmt.Sprintf("c%02d", cust)}}
					}
					upd := bson.D{{Key: "$set", Value: set}}
					if _, err = copyUp(id); err == nil {
						if _, err = fast.UpdateOne(sctx, bson.D{{Key: "_id", Value: id}}, upd); err == nil {
							_, err = archive.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, upd)
						}
					}
				}
				written++
				if ot := sess.OperationTime(); ot != nil {
					c.NoteWrite(ns, *ot)
				}
				q, _ := ParseFind(ns, mustRaw(bson.D{{Key: "find", Value: "orders"}, {Key: "filter", Value: bson.D{{Key: "customer", Value: fmt.Sprintf("c%02d", cust)}}},
					{Key: "sort", Value: bson.D{{Key: "at", Value: int32(-1)}}}, {Key: "limit", Value: int32(10)}}))
				if got, hit := c.Get(q); hit {
					if want := direct(t, ctx, archive, q); !reflect.DeepEqual(batchOf(got), want) {
						ryw.Store(fmt.Sprintf("c%02d after write %d", cust, written), fmt.Sprintf("cache %v, both tiers %v", batchOf(got), want))
					}
				}
				mu.Unlock()
				if err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}

	// Readers, so entries exist while all of that goes on.
	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(100 + k)))
			for time.Now().Before(deadline) {
				c.Get(queries(r))
				if r.Intn(4) == 0 {
					c.Get(groupQ(r))
				}
				time.Sleep(time.Millisecond)
			}
		}(k)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	n := 0
	ryw.Range(func(k, v any) bool {
		n++
		if n <= 5 {
			t.Errorf("a writer read the state before its own write, %v: %v", k, v)
		}
		return true
	})

	// Caught up, and nothing moving: every answer must be the both-tier one.
	time.Sleep(500 * time.Millisecond)
	r := rand.New(rand.NewSource(99))
	checked, lists, wrong := 0, 0, 0
	for i := 0; i < 3000; i++ {
		q := queries(r)
		got, hit := c.Get(q)
		if !hit {
			continue
		}
		checked++
		if !hasEq(q, "_id") {
			lists++
		}
		if want := direct(t, ctx, archive, q); !reflect.DeepEqual(batchOf(got), want) {
			wrong++
			if wrong <= 5 {
				t.Errorf("%q skip=%d: cache %v, both tiers %v", q.entryKey(), q.Skip, batchOf(got), want)
			}
		}
	}
	// Groupings over both tiers, asked twice so a tiered entry, filled on its
	// second miss, exists to be checked.
	grouped := 0
	for i := 0; i < 600; i++ {
		q := groupQ(r)
		got, hit := c.Get(q)
		if !hit {
			time.Sleep(20 * time.Millisecond)
			if got, hit = c.Get(q); !hit {
				continue
			}
		}
		grouped++
		p, _ := pipes.Load(q.entryKey())
		mu.Lock()
		real, err := db.RunCommand(ctx, bson.D{{Key: "aggregate", Value: "archive"}, {Key: "pipeline", Value: p}, {Key: "cursor", Value: bson.D{}}}).Raw()
		mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if why := SameGroupAnswer(q, got, real); why != "" {
			wrong++
			if wrong <= 5 {
				t.Errorf("%s: %s: cache %v, both tiers %v", q.entryKey(), why, got.Lookup("cursor", "firstBatch"), real.Lookup("cursor", "firstBatch"))
			}
		}
	}
	s := c.Stats()
	t.Logf("%d evicted, %d copied up, %d written; checked %d answers (%d lists, then %d groupings) after, %d wrong; %d writer reads disagreed; stats %+v",
		evicted, warmed, written, checked, lists, grouped, wrong, n, s)
	if grouped < 100 {
		t.Errorf("only %d groupings came from the cache, too few to say anything", grouped)
	}
	if evicted < 500 || warmed < 200 {
		t.Errorf("the tiering hardly moved (%d evicted, %d copied up), so this proves little", evicted, warmed)
	}
	if lists < 200 {
		t.Errorf("only %d lists came from the cache, too few to say anything", lists)
	}
}
