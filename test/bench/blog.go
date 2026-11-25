package main

// The blog workload: a news site whose articles are read mostly in their
// first days and then by a long tail of permalinks, whose readers count a view
// on whatever they open, old articles included, and whose comments arrive in
// the days after an article is published.
//
//	bench -uri URI -ns news.articles -load-blog 2000000      load the archive
//	bench -uri URI -ns news.articles -workload blog -for 5m  run it
//
// What it puts through the proxy that the shop does not: a write to an old
// document on most article reads (the view counter), so the copy-up runs all
// the time, and a second collection read by a key, the article, rather than
// by date.

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
	sections         = 20
	tags             = 500
	commentsPerStory = 4
)

var blogMix = []mixEntry{
	{"front page", 300},
	{"section page", 200},
	{"read an article", 250},
	{"its comments", 120},
	{"count a view", 80},
	{"post a comment", 30},
	{"tag page", 20},
}

func articleID(w *workload, seq int) bson.ObjectID { return idAt(w.atOf(seq), seq) }

// loadBlog loads n articles over the period and commentsPerStory comments on
// each, dated in the three days after it, then the indexes the site reads by.
func loadBlog(ctx context.Context, db *mongo.Database, articles *mongo.Collection, n, loaders int, start time.Time, period time.Duration, pad int) error {
	comments := db.Collection("comments")
	for _, c := range []*mongo.Collection{articles, comments} {
		if err := c.Drop(ctx); err != nil {
			return err
		}
	}
	w := &workload{n: n, start: start, period: period}
	began := time.Now()
	var done atomic.Int64
	errs := make(chan error, loaders)
	var wg sync.WaitGroup
	per := (n + loaders - 1) / loaders
	for l := 0; l < loaders; l++ {
		from, to := l*per, min((l+1)*per, n)
		wg.Add(1)
		go func(l, from, to int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(l) + 11))
			var as, cs []any
			flush := func() error {
				if len(as) > 0 {
					if _, err := articles.InsertMany(ctx, as, options.InsertMany().SetOrdered(false)); err != nil {
						return err
					}
				}
				if len(cs) > 0 {
					if _, err := comments.InsertMany(ctx, cs, options.InsertMany().SetOrdered(false)); err != nil {
						return err
					}
				}
				as, cs = as[:0], cs[:0]
				return nil
			}
			for i := from; i < to; i++ {
				at := w.atOf(i)
				as = append(as, article(articleID(w, i), at, i, rng, pad))
				for k := 0; k < commentsPerStory; k++ {
					j := i*commentsPerStory + k
					cat := at.Add(time.Duration(rng.Int63n(int64(72 * time.Hour))))
					if end := start.Add(period); !cat.Before(end) {
						cat = end.Add(-time.Second)
					}
					cs = append(cs, comment(idAt(cat, j), articleID(w, i), cat, rng))
				}
				if len(as) == 500 || i == to-1 {
					if err := flush(); err != nil {
						errs <- err
						return
					}
					if d := done.Add(500); d%200000 < 500 {
						fmt.Printf("  %s articles loaded, %.0f/s\n", commas(int(d)), float64(d)/time.Since(began).Seconds())
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
	fmt.Printf("loaded %s articles and %s comments in %s; building indexes\n",
		commas(n), commas(n*commentsPerStory), time.Since(began).Round(time.Second))
	if _, err := articles.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "at", Value: -1}, {Key: "_id", Value: -1}}},
		{Keys: bson.D{{Key: "section", Value: 1}, {Key: "at", Value: -1}}},
		{Keys: bson.D{{Key: "tags", Value: 1}, {Key: "at", Value: -1}}},
	}); err != nil {
		return err
	}
	if _, err := comments.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "at", Value: -1}, {Key: "_id", Value: -1}}},
		{Keys: bson.D{{Key: "article", Value: 1}, {Key: "at", Value: 1}}},
	}); err != nil {
		return err
	}
	_, err := db.Collection("bench_meta").ReplaceOne(ctx, bson.D{{Key: "_id", Value: articles.Name()}},
		marker{N: n, Start: start, Period: period}, options.Replace().SetUpsert(true))
	fmt.Printf("done in %s\n", time.Since(began).Round(time.Second))
	return err
}

func article(id bson.ObjectID, at time.Time, i int, rng *rand.Rand, pad int) bson.D {
	b := make([]byte, pad)
	for k := range b {
		b[k] = byte(33 + rng.Intn(94))
	}
	return bson.D{
		{Key: "_id", Value: id},
		{Key: "at", Value: at},
		{Key: "section", Value: i % sections},
		{Key: "author", Value: fmt.Sprintf("a%03d", (i*31)%300)},
		{Key: "title", Value: fmt.Sprintf("Story %d", i)},
		{Key: "tags", Value: bson.A{fmt.Sprintf("t%03d", i%tags), fmt.Sprintf("t%03d", (i*7+3)%tags)}},
		{Key: "views", Value: 0},
		{Key: "body", Value: string(b)},
	}
}

func comment(id, article bson.ObjectID, at time.Time, rng *rand.Rand) bson.D {
	return bson.D{
		{Key: "_id", Value: id},
		{Key: "article", Value: article},
		{Key: "at", Value: at},
		{Key: "user", Value: fmt.Sprintf("u%05d", rng.Intn(50000))},
		{Key: "text", Value: fmt.Sprintf("comment %d", rng.Int31())},
	}
}

// resetBlog puts a loaded site back as it was loaded: what runs published or
// commented is removed and the view counters go back to zero.
func resetBlog(ctx context.Context, cl *mongo.Client, db *mongo.Database, articles *mongo.Collection, metaDB string) error {
	var m marker
	if err := db.Collection("bench_meta").FindOne(ctx, bson.D{{Key: "_id", Value: articles.Name()}}).Decode(&m); err != nil {
		return fmt.Errorf("no load recorded for %s: %w", articles.Name(), err)
	}
	after := bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: m.Start.Add(m.Period)}}}}
	ra, err := articles.DeleteMany(ctx, after)
	if err != nil {
		return err
	}
	rc, err := db.Collection("comments").DeleteMany(ctx, after)
	if err != nil {
		return err
	}
	rv, err := articles.UpdateMany(ctx, bson.D{{Key: "views", Value: bson.D{{Key: "$ne", Value: 0}}}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "views", Value: 0}}}})
	if err != nil {
		return err
	}
	if err := cl.Database(metaDB).Drop(ctx); err != nil {
		return err
	}
	fmt.Printf("removed %d articles and %d comments, zeroed %d view counters, dropped %s\n",
		ra.DeletedCount, rc.DeletedCount, rv.ModifiedCount, metaDB)
	return nil
}

type blog struct {
	w        *workload
	articles *mongo.Collection
	comments *mongo.Collection
	rate     int
	posted   atomic.Int64
}

func (b *blog) run(ctx context.Context, d time.Duration, conc int, seed int64) *report {
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
	var slots atomic.Int64
	pace := func() {
		if b.rate <= 0 {
			return
		}
		at := began.Add(time.Duration(slots.Add(1)) * time.Second / time.Duration(b.rate))
		if wait := time.Until(at); wait > 0 {
			time.Sleep(wait)
		}
	}
	var wg sync.WaitGroup
	for k := 0; k < conc; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed*1000 + int64(k)))
			local := map[string]*opStat{}
			for time.Now().Before(deadline) {
				r, acc, name := rng.Intn(1000), 0, ""
				for _, op := range blogMix {
					acc += op.share
					if r < acc {
						name = op.name
						break
					}
				}
				pace()
				timed(local, name, func() error { return b.step(ctx, rng, name) })
			}
			merge(local)
		}(k)
	}
	office := func(name string, every time.Duration, f func(*rand.Rand) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + int64(len(name))))
			local := map[string]*opStat{}
			for time.Now().Before(deadline) {
				timed(local, name, func() error { return f(rng) })
				time.Sleep(every)
			}
			merge(local)
		}()
	}
	office("publish an article", 2*time.Second, b.publish)
	office("report: most viewed, 7 days", 10*time.Second, b.mostViewed)
	wg.Wait()
	rep.Seconds = time.Since(began).Seconds()
	finish(rep)
	return rep
}

func (b *blog) step(ctx context.Context, rng *rand.Rand, name string) error {
	drain := func(cur *mongo.Cursor, err error) error {
		if err != nil {
			return err
		}
		defer func() { _ = cur.Close(ctx) }()
		for cur.Next(ctx) {
		}
		return cur.Err()
	}
	list := options.Find().SetSort(bson.D{{Key: "at", Value: -1}}).SetLimit(20).
		SetProjection(bson.D{{Key: "body", Value: 0}})
	switch name {
	case "front page":
		return drain(b.articles.Find(ctx, bson.D{}, list))
	case "section page":
		return drain(b.articles.Find(ctx, bson.D{{Key: "section", Value: rng.Intn(sections)}}, list))
	case "tag page":
		return drain(b.articles.Find(ctx, bson.D{{Key: "tags", Value: fmt.Sprintf("t%03d", rng.Intn(tags))}}, list))
	case "read an article":
		err := b.articles.FindOne(ctx, bson.D{{Key: "_id", Value: articleID(b.w, b.w.seqAt(rng))}}).Err()
		if err == mongo.ErrNoDocuments {
			return nil
		}
		return err
	case "its comments":
		return drain(b.comments.Find(ctx, bson.D{{Key: "article", Value: articleID(b.w, b.w.seqAt(rng))}},
			options.Find().SetSort(bson.D{{Key: "at", Value: 1}}).SetLimit(50)))
	case "count a view":
		_, err := b.articles.UpdateOne(ctx, bson.D{{Key: "_id", Value: articleID(b.w, b.w.seqAt(rng))}},
			bson.D{{Key: "$inc", Value: bson.D{{Key: "views", Value: 1}}}})
		return err
	case "post a comment":
		now := time.Now()
		_, err := b.comments.InsertOne(ctx, comment(bson.NewObjectIDFromTimestamp(now), articleID(b.w, b.w.seqAt(rng)), now, rng))
		return err
	}
	return fmt.Errorf("unknown operation %q", name)
}

func (b *blog) publish(rng *rand.Rand) error {
	now := time.Now()
	seq := b.w.n + int(b.posted.Add(1))
	doc := article(bson.NewObjectIDFromTimestamp(now), now, seq, rng, b.w.pad)
	_, err := b.articles.InsertOne(context.Background(), doc)
	return err
}

// mostViewed reads the week before the load ended, so every run reads the
// same articles however many runs came before.
func (b *blog) mostViewed(*rand.Rand) error {
	end := b.w.start.Add(b.w.period)
	cur, err := b.articles.Find(context.Background(),
		bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: end.Add(-7 * 24 * time.Hour)}, {Key: "$lt", Value: end}}}},
		options.Find().SetSort(bson.D{{Key: "views", Value: -1}}).SetLimit(10).SetProjection(bson.D{{Key: "body", Value: 0}}))
	if err != nil {
		return err
	}
	defer func() { _ = cur.Close(context.Background()) }()
	for cur.Next(context.Background()) {
	}
	return cur.Err()
}
