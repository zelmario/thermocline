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
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// The cache against MongoDB itself, under writes. Every answer it gives once
// the stream has caught up must be the answer MongoDB gives, and a writer must
// never read the state before its own write.
func TestAgainstMongoUnderWrites(t *testing.T) {
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
	t.Cleanup(func() { _ = cl.Disconnect(context.Background()) })
	const ns = "tc_live.products"
	co := cl.Database("tc_live").Collection("products")
	_ = co.Drop(ctx)
	t.Cleanup(func() { _ = co.Drop(context.Background()) })
	rng := rand.New(rand.NewSource(7))
	price := func() float64 { return float64(rng.Intn(1_000_000)) / 100 }
	var docs []any
	for i := 0; i < 600; i++ {
		tags := bson.A{}
		for k := 0; k < i%4; k++ {
			tags = append(tags, fmt.Sprintf("t%d", (i+k)%7))
		}
		docs = append(docs, bson.D{{Key: "_id", Value: fmt.Sprintf("p%04d", i)}, {Key: "category", Value: fmt.Sprintf("c%02d", i%20)},
			{Key: "price", Value: price()}, {Key: "stock", Value: int32(100)}, {Key: "tags", Value: tags}})
	}
	if _, err := co.InsertMany(ctx, docs); err != nil {
		t.Fatal(err)
	}

	c := New(64 << 20)
	// Every fill held after its read, so changes arrive between its position
	// and its install as they do under load. A fill that lost them would hold
	// a state the stream has moved past; the checks below then see it.
	c.fillDelay = 20 * time.Millisecond
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go c.Run(runCtx, cl, streamClient(t, uri), func(string) bool { return false }, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// Range bounds from a short list, so the same range is asked again.
	bound := func(r *rand.Rand) (any, any) {
		lo := r.Intn(8) * 1000
		if r.Intn(2) == 0 {
			return int32(lo), float64(lo + 2500)
		}
		return float64(lo), int64(lo + 2500)
	}
	// The pipelines of the grouped queries, by entry, for asking mongod.
	var pipes sync.Map
	group := func(p bson.A) Query {
		q, ok := ParseGroupAgg(ns, mustRaw(bson.D{{Key: "aggregate", Value: "products"}, {Key: "pipeline", Value: p}, {Key: "cursor", Value: bson.D{}}}))
		if !ok {
			panic(fmt.Sprintf("not understood: %v", p))
		}
		pipes.Store(q.entryKey(), p)
		return q
	}
	queries := func(r *rand.Rand) Query {
		cat := fmt.Sprintf("c%02d", r.Intn(20))
		switch r.Intn(11) {
		case 7:
			// Stock and value per category, in no order.
			return group(bson.A{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$category"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
				{Key: "value", Value: bson.D{{Key: "$sum", Value: "$price"}}}, {Key: "units", Value: bson.D{{Key: "$sum", Value: "$stock"}}}}}}})
		case 8:
			lo, hi := bound(r)
			return group(bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "price", Value: bson.D{{Key: "$gte", Value: lo}, {Key: "$lte", Value: hi}}}}}},
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$category"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}},
				bson.D{{Key: "$sort", Value: bson.D{{Key: "n", Value: int32(-1)}}}}, bson.D{{Key: "$limit", Value: int32(3)}}})
		case 9:
			return group(bson.A{bson.D{{Key: "$unwind", Value: "$tags"}},
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$tags"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}, {Key: "units", Value: bson.D{{Key: "$sum", Value: "$stock"}}}}}},
				bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: int32(1)}}}}})
		case 10:
			return group(bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "category", Value: cat}}}},
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "value", Value: bson.D{{Key: "$sum", Value: "$price"}}}}}}})
		case 4:
			// The store's "filter by price".
			lo, hi := bound(r)
			q, _ := ParseFind(ns, mustRaw(bson.D{{Key: "find", Value: "products"},
				{Key: "filter", Value: bson.D{{Key: "category", Value: cat}, {Key: "price", Value: bson.D{{Key: "$gte", Value: lo}, {Key: "$lte", Value: hi}}}}},
				{Key: "sort", Value: bson.D{{Key: "price", Value: int32(1)}}}, {Key: "limit", Value: int32(10)},
				{Key: "projection", Value: bson.D{{Key: "stock", Value: 0}}}}))
			return q
		case 5:
			lo, _ := bound(r)
			q, _ := ParseFind(ns, mustRaw(bson.D{{Key: "find", Value: "products"},
				{Key: "filter", Value: bson.D{{Key: "price", Value: bson.D{{Key: "$gt", Value: lo}}}}},
				{Key: "sort", Value: bson.D{{Key: "price", Value: int32(-1)}}}, {Key: "skip", Value: int32(r.Intn(2) * 10)}, {Key: "limit", Value: int32(10)}}))
			return q
		case 6:
			lo, hi := bound(r)
			q, _ := ParseCountAgg(ns, mustRaw(bson.D{{Key: "aggregate", Value: "products"}, {Key: "pipeline", Value: bson.A{
				bson.D{{Key: "$match", Value: bson.D{{Key: "price", Value: bson.D{{Key: "$gte", Value: lo}, {Key: "$lt", Value: hi}}}}}},
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}}}},
				{Key: "cursor", Value: bson.D{}}}))
			return q
		case 0:
			q, _ := ParseCountAgg(ns, mustRaw(bson.D{{Key: "aggregate", Value: "products"}, {Key: "pipeline", Value: bson.A{
				bson.D{{Key: "$match", Value: bson.D{{Key: "category", Value: cat}}}},
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}}}},
				{Key: "cursor", Value: bson.D{}}}))
			return q
		case 1:
			q, _ := ParseFind(ns, mustRaw(bson.D{{Key: "find", Value: "products"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: fmt.Sprintf("p%04d", r.Intn(650))}}}, {Key: "limit", Value: int32(1)}}))
			return q
		}
		dir := int32(1)
		if r.Intn(2) == 0 {
			dir = -1
		}
		q, _ := ParseFind(ns, mustRaw(bson.D{{Key: "find", Value: "products"}, {Key: "filter", Value: bson.D{{Key: "category", Value: cat}}},
			{Key: "sort", Value: bson.D{{Key: "price", Value: dir}}}, {Key: "skip", Value: int32(r.Intn(3) * 10)}, {Key: "limit", Value: int32(10)},
			{Key: "projection", Value: bson.D{{Key: "stock", Value: 0}}}}))
		return q
	}

	// Writers, and after each write the writer reads what it touched. It
	// may be told the cache does not know yet; it must never be told the old
	// state.
	var wg sync.WaitGroup
	var ryw sync.Map
	deadline := time.Now().Add(20 * time.Second)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			sess, _ := cl.StartSession()
			defer sess.EndSession(context.Background())
			for time.Now().Before(deadline) {
				// Its own ids: another writer's later write to the same one
				// would make mongod's answer newer than this writer's, which
				// is not the cache reading the state before it.
				id := fmt.Sprintf("p%04d", r.Intn(650/4)*4+w)
				var err error
				sctx := mongo.NewSessionContext(ctx, sess)
				// Noted as the router notes it, by what the write names:
				// see classifyWrite. A namespace-wide mark held every read
				// behind four writers' latest write, about twenty of them
				// inside the applier's 20ms, so entries were filled only when
				// that queue happened to empty: 0 to 733 answers a run.
				note := WriteNote{NS: ns, IDs: []bson.RawValue{strVal(t, id)}}
				switch r.Intn(5) {
				case 0:
					note.Fields = []string{"category"}
					_, err = co.UpdateOne(sctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$set", Value: bson.D{{Key: "category", Value: fmt.Sprintf("c%02d", r.Intn(20))}}}})
				case 1:
					_, err = co.DeleteOne(sctx, bson.D{{Key: "_id", Value: id}})
				case 2:
					d := bson.D{{Key: "_id", Value: id}, {Key: "category", Value: fmt.Sprintf("c%02d", r.Intn(20))},
						{Key: "price", Value: float64(r.Intn(1_000_000)) / 100}, {Key: "stock", Value: int32(5)}}
					note = WriteNote{NS: ns, Docs: []bson.Raw{mustRaw(d)}}
					_, err = co.InsertOne(sctx, d)
				default:
					note.Fields = []string{"price", "stock"}
					var p any = float64(r.Intn(1_000_000)) / 100
					if r.Intn(4) == 0 {
						// A whole number of another type, which a range
						// compares by value.
						p = int32(r.Intn(10_000))
					}
					_, err = co.UpdateOne(sctx, bson.D{{Key: "_id", Value: id}}, bson.D{
						{Key: "$set", Value: bson.D{{Key: "price", Value: p}}},
						{Key: "$inc", Value: bson.D{{Key: "stock", Value: int32(-1)}}},
					})
				}
				if err != nil && !mongo.IsDuplicateKeyError(err) {
					t.Errorf("write: %v", err)
					return
				}
				if ot := sess.OperationTime(); ot != nil {
					c.NoteWrites([]WriteNote{note}, *ot)
				}
				q, _ := ParseFind(ns, mustRaw(bson.D{{Key: "find", Value: "products"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "limit", Value: int32(1)}}))
				if got, hit := c.Get(q); hit {
					want := direct(t, ctx, co, q)
					if !reflect.DeepEqual(batchOf(got), want) {
						ryw.Store(id, fmt.Sprintf("cache %v, mongod %v", batchOf(got), want))
					}
				}
			}
		}(w)
	}
	// Readers, to make entries exist while the writes go on.
	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(100 + k)))
			for time.Now().Before(deadline) {
				c.Get(queries(r))
				time.Sleep(time.Millisecond)
			}
		}(k)
	}
	wg.Wait()
	n := 0
	ryw.Range(func(k, v any) bool {
		n++
		if n <= 5 {
			t.Errorf("a writer read the state before its own write of %v: %v", k, v)
		}
		return true
	})

	// Caught up: every entry must now be what MongoDB answers.
	time.Sleep(500 * time.Millisecond)
	r := rand.New(rand.NewSource(99))
	checked, wrong, ranged, grouped := 0, 0, 0, 0
	for i := 0; i < 4000; i++ {
		q := queries(r)
		got, hit := c.Get(q)
		if !hit {
			continue
		}
		checked++
		if len(q.Ranges) > 0 {
			ranged++
		}
		if q.Kind == KindGroup {
			grouped++
			p, _ := pipes.Load(q.entryKey())
			real, err := cl.Database("tc_live").RunCommand(ctx, bson.D{{Key: "aggregate", Value: "products"}, {Key: "pipeline", Value: p}, {Key: "cursor", Value: bson.D{}}}).Raw()
			if err != nil {
				t.Fatal(err)
			}
			if why := SameGroupAnswer(q, got, real); why != "" {
				wrong++
				if wrong <= 5 {
					t.Errorf("%s: %s: cache %v, mongod %v", q.entryKey(), why, got.Lookup("cursor", "firstBatch"), real.Lookup("cursor", "firstBatch"))
				}
			}
			continue
		}
		want := direct(t, ctx, co, q)
		if !reflect.DeepEqual(batchOf(got), want) {
			wrong++
			if wrong <= 5 {
				c.mu.Lock()
				e := c.entries[q.entryKey()]
				st := "no entry"
				if e != nil {
					st = fmt.Sprintf("entry complete=%v docs=%d", e.complete, len(e.docs))
				}
				c.mu.Unlock()
				t.Errorf("%s skip=%d limit=%d %s: cache %v, mongod %v", q.entryKey(), q.Skip, q.Limit, st, batchOf(got), want)
			}
		}
	}
	// A change landing while a fill is in flight, and the last change that
	// document gets. Under the writers above a fill that lost one was mostly
	// healed by the next write to the same document, whose event carries the
	// whole document, so the loss showed only by accident of timing. Here the
	// fill is asked for, the write follows once the fill has its position, and
	// nothing writes that document again.
	raced, lost := 0, 0
	for round := 0; round < 40; round++ {
		cat := fmt.Sprintf("c%02d", round%20)
		q, _ := ParseFind(ns, mustRaw(bson.D{{Key: "find", Value: "products"}, {Key: "filter", Value: bson.D{{Key: "category", Value: cat}}},
			{Key: "sort", Value: bson.D{{Key: "price", Value: int32(1)}}}, {Key: "limit", Value: int32(3)},
			{Key: "projection", Value: bson.D{{Key: "price", Value: int32(1)}, {Key: "category", Value: int32(1)}, {Key: fmt.Sprintf("r%d", round), Value: int32(1)}}}}))
		if _, hit := c.Get(q); hit {
			continue
		}
		var last bson.M
		o := options.FindOne().SetSort(bson.D{{Key: "price", Value: -1}})
		if err := co.FindOne(ctx, bson.D{{Key: "category", Value: cat}}, o).Decode(&last); err != nil {
			continue
		}
		// Past the applier's next turn, which hands the fill its position,
		// and inside the fill's 20ms delay: to the top of the list.
		time.Sleep(8 * time.Millisecond)
		if _, err := co.UpdateOne(ctx, bson.D{{Key: "_id", Value: last["_id"]}}, bson.D{{Key: "$set", Value: bson.D{{Key: "price", Value: -1 - float64(round)}}}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
		got, hit := c.Get(q)
		if !hit {
			continue
		}
		raced++
		if want := direct(t, ctx, co, q); !reflect.DeepEqual(batchOf(got), want) {
			lost++
			if lost <= 3 {
				t.Errorf("a fill lost a change made while it was in flight: %s: cache %v, mongod %v", q.entryKey(), batchOf(got), want)
			}
		}
	}
	if raced < 10 {
		t.Errorf("only %d fills raced a write, too few to say anything", raced)
	}

	s := c.Stats()
	t.Logf("checked %d answers after the writes (%d of them ranges, %d groupings), %d wrong; %d writer reads disagreed; %d fills raced a write, %d lost it; stats %+v", checked, ranged, grouped, wrong, n, raced, lost, s)
	if checked < 200 {
		t.Errorf("only %d answers came from the cache, too few to say anything", checked)
	}
	if ranged < 100 {
		t.Errorf("only %d range answers came from the cache, too few to say anything", ranged)
	}
	if grouped < 100 {
		t.Errorf("only %d grouped answers came from the cache, too few to say anything", grouped)
	}
}

// streamClient is the client the applier reads its stream through: the same
// one, or with TC_LIVE_STREAM=secondary one that prefers a secondary, which is
// router.live_cache.stream_from: secondary.
func streamClient(t *testing.T, uri string) *mongo.Client {
	t.Helper()
	if os.Getenv("TC_LIVE_STREAM") != "secondary" {
		cl, err := mongo.Connect(options.Client().ApplyURI(uri))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cl.Disconnect(context.Background()) })
		return cl
	}
	cl, err := mongo.Connect(options.Client().ApplyURI(uri).SetReadPreference(readpref.SecondaryPreferred()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Disconnect(context.Background()) })
	return cl
}

func mustRaw(d bson.D) bson.Raw {
	b, err := bson.Marshal(d)
	if err != nil {
		panic(err)
	}
	return b
}

// batchOf reads a reply's documents, or its count, as plain values.
func batchOf(reply bson.Raw) any {
	if n, ok := reply.Lookup("n").AsInt64OK(); ok {
		return n
	}
	out := []map[string]any{}
	vals, err := reply.Lookup("cursor", "firstBatch").Array().Values()
	if err != nil {
		return "unreadable batch: " + err.Error()
	}
	for _, v := range vals {
		m := map[string]any{}
		if err := bson.Unmarshal(v.Document(), &m); err != nil {
			return "unreadable document: " + err.Error()
		}
		out = append(out, m)
	}
	if len(out) == 1 {
		if n, ok := out[0]["n"]; ok && len(out[0]) == 2 {
			return toInt64(n)
		}
	}
	return out
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	}
	return -1
}

func direct(t *testing.T, ctx context.Context, co *mongo.Collection, q Query) any {
	t.Helper()
	filter := bson.D{}
	for _, p := range q.Eq {
		filter = append(filter, bson.E{Key: p.Field, Value: p.Value})
	}
	// One field's bounds in one document, as the fill writes them.
	for i := 0; i < len(q.Ranges); {
		ops := bson.D{}
		f := q.Ranges[i].Field
		for ; i < len(q.Ranges) && q.Ranges[i].Field == f; i++ {
			ops = append(ops, bson.E{Key: q.Ranges[i].Op, Value: q.Ranges[i].Value})
		}
		filter = append(filter, bson.E{Key: f, Value: ops})
	}
	if q.Kind != KindFind {
		n, err := co.CountDocuments(ctx, filter)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return []map[string]any{}
		}
		return n
	}
	fo := options.Find().SetSkip(int64(q.Skip)).SetLimit(int64(q.Limit))
	if len(q.Sort) > 0 {
		s := bson.D{}
		for _, k := range q.Sort {
			d := 1
			if k.Desc {
				d = -1
			}
			s = append(s, bson.E{Key: k.Field, Value: d})
		}
		fo.SetSort(s)
	}
	// The query's own projection. This used {stock: 0} for any projection,
	// written when that was the only one the test asked; the in-flight rounds
	// below include price, category and r<n>, so mongod's side carried tags
	// and the cache's, rightly, did not. Under load more of those rounds hit
	// the cache, and each read as "a fill lost a change" with the price, the
	// change in question, equal on both sides.
	if q.Proj.Set {
		pd := bson.D{}
		v := 0
		if q.Proj.Include {
			v = 1
		}
		for f := range q.Proj.Fields {
			pd = append(pd, bson.E{Key: f, Value: v})
		}
		if q.Proj.NoID {
			pd = append(pd, bson.E{Key: "_id", Value: 0})
		}
		fo.SetProjection(pd)
	}
	cur, err := co.Find(ctx, filter, fo)
	if err != nil {
		t.Fatal(err)
	}
	out := []map[string]any{}
	for cur.Next(ctx) {
		m := map[string]any{}
		if err := bson.Unmarshal(cur.Current, &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}
