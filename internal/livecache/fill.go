package livecache

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Fills used to run on the applier, between events, so every miss stopped the
// stream for a read of the fast tier, and under load the stream fell behind and
// the staleness bound sent the reads it was meant to answer to mongod instead.
// Measured at 64 shoppers on 2026-10-21: 3.38 million reads waited for the
// stream against 559,024 answered.
//
// They run on workers now, and stay exact by the order ReadySet keeps between
// its replays and its replication: a fill is a read at one position, followed by
// every change after it. When the applier hands a fill out it records the
// position it has applied, P0, and from then on keeps every change to that
// namespace aside. The worker reads at exactly P0, a snapshot at that cluster
// time, which is majority committed because the stream only delivers what is.
// When the result comes back the applier installs it and applies the changes it
// kept, later than P0, to that entry alone. So the entry is the state at P0 plus
// everything since, which is what an entry filled on the applier was, and a
// count no longer counts an insert the read had already seen.
//
// Only later than P0. A transaction's changes share one cluster time, and a
// snapshot at P0 holds all of those at P0, including any the stream has not
// delivered yet.

// fillWorkers is how many fills read at once.
const fillWorkers = 4

// maxFillBuffer bounds the changes kept for a fill in flight. A fill that falls
// further behind than this is dropped, and the next miss asks again.
const maxFillBuffer = 10000

// fillTimeout bounds one fill's read. A snapshot read waits for nothing, so a
// long one is an overloaded fast tier, and the miss is answered by mongod.
const fillTimeout = 10 * time.Second

// inflight is a fill handed to a worker and the changes kept for it.
type inflight struct {
	q   Query
	key string
	p0  bson.Timestamp
	buf []Change
	// dropped is set when the buffer overflowed or the namespace was
	// forgotten while the read was out; its result is thrown away.
	dropped bool
}

// fillResult is what a worker read.
type fillResult struct {
	f        *inflight
	docs     []bson.Raw
	complete bool
	n        int64
	// ok false: nothing to install, the miss stays a miss.
	ok bool
	// unproven: a tiered read the fast tier could not prove; remembered.
	unproven bool
}

// fillState is the applier's side of fills in flight. Only the applier touches
// it.
type fillState struct {
	byKey map[string]*inflight
	byNS  map[string][]*inflight
	work  chan *inflight
	done  chan fillResult
}

func newFillState() *fillState {
	return &fillState{
		byKey: map[string]*inflight{},
		byNS:  map[string][]*inflight{},
		work:  make(chan *inflight, 1024),
		done:  make(chan fillResult, 1024),
	}
}

// startFills runs the workers until ctx is done.
func (c *Cache) startFills(ctx context.Context, hot *mongo.Client, fs *fillState) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < fillWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case f := <-fs.work:
					r := c.read(ctx, hot, f)
					if c.fillDelay > 0 {
						time.Sleep(c.fillDelay)
					}
					select {
					case fs.done <- r:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	return &wg
}

// dispatch hands out the fills asked for since the last turn. Called by the
// applier.
func (c *Cache) dispatch(fs *fillState) {
	for {
		select {
		case q := <-c.fillq:
			c.mu.Lock()
			p0 := c.applied
			c.mu.Unlock()
			key := q.entryKey()
			if p0.IsZero() || fs.byKey[key] != nil {
				// Nothing applied yet means no position to read at.
				c.unpend(q)
				continue
			}
			f := &inflight{q: q, key: key, p0: p0}
			select {
			case fs.work <- f:
				fs.byKey[key] = f
				fs.byNS[q.NS] = append(fs.byNS[q.NS], f)
			default:
				c.unpend(q)
			}
		default:
			return
		}
	}
}

// keep records a change for every fill in flight on its namespace. Called by
// the applier for each change before it is applied.
func (fs *fillState) keep(ch Change) {
	for _, f := range fs.byNS[ch.NS] {
		if f.dropped {
			continue
		}
		if len(f.buf) >= maxFillBuffer {
			f.dropped = true
			f.buf = nil
			continue
		}
		f.buf = append(f.buf, ch)
	}
}

// forget drops the fills in flight on a namespace, or on every namespace when
// ns is empty: what they read may no longer be what the stream builds on.
func (fs *fillState) forget(ns string) {
	for k, list := range fs.byNS {
		if ns != "" && k != ns {
			continue
		}
		for _, f := range list {
			f.dropped = true
			f.buf = nil
		}
	}
}

// collect installs the results the workers have finished. Called by the
// applier.
func (c *Cache) collect(fs *fillState) {
	for {
		select {
		case r := <-fs.done:
			c.finish(fs, r)
		default:
			return
		}
	}
}

func (c *Cache) finish(fs *fillState, r fillResult) {
	f := r.f
	delete(fs.byKey, f.key)
	list := fs.byNS[f.q.NS]
	for i, g := range list {
		if g == f {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(fs.byNS, f.q.NS)
	} else {
		fs.byNS[f.q.NS] = list
	}
	switch {
	case f.dropped || !r.ok:
		c.unpend(f.q)
	case r.unproven:
		c.unproven(f.q)
	default:
		c.installAt(f, r.docs, r.complete, r.n)
	}
}

// read is a worker's fill: the query's answer as it stood at the fill's
// position, and for a tiered namespace only as far as the fast tier proves.
func (c *Cache) read(ctx context.Context, hot *mongo.Client, f *inflight) fillResult {
	q := f.q
	out := fillResult{f: f}
	plain, ok := c.plain(ctx, hot, q.NS)
	if !ok {
		return out
	}
	if !plain {
		c.RefuseNS(q.NS)
		return out
	}
	tiered := c.isTiered(q.NS)
	if tiered && q.Kind != KindFind {
		// A count over a tiered namespace is over both tiers, and the fast
		// tier's stream cannot keep one: a copy-up is an insert of a document
		// that was already counted.
		return out
	}
	filter := bson.D{}
	for _, p := range q.Eq {
		filter = append(filter, bson.E{Key: p.Field, Value: p.Value})
	}
	db, coll := splitNS(q.NS)
	rctx, cancel := context.WithTimeout(ctx, fillTimeout)
	defer cancel()
	snapshot := bson.D{{Key: "level", Value: "snapshot"}, {Key: "atClusterTime", Value: f.p0}}

	if q.Kind != KindFind {
		var res struct {
			Cursor struct {
				FirstBatch []struct {
					N int64 `bson:"n"`
				} `bson:"firstBatch"`
			} `bson:"cursor"`
		}
		err := hot.Database(db).RunCommand(rctx, bson.D{
			{Key: "aggregate", Value: coll},
			{Key: "pipeline", Value: bson.A{
				bson.D{{Key: "$match", Value: filter}},
				bson.D{{Key: "$count", Value: "n"}},
			}},
			{Key: "cursor", Value: bson.D{}},
			{Key: "readConcern", Value: snapshot},
		}).Decode(&res)
		if err != nil {
			return out
		}
		if len(res.Cursor.FirstBatch) > 0 {
			out.n = res.Cursor.FirstBatch[0].N
		}
		out.complete, out.ok = true, true
		return out
	}

	window := min(maxWindow, q.Skip+q.Limit+fillWindow)
	cmd := bson.D{
		{Key: "find", Value: coll},
		{Key: "filter", Value: filter},
	}
	if len(q.Sort) > 0 {
		sortDoc := bson.D{}
		for _, s := range q.Sort {
			dir := 1
			if s.Desc {
				dir = -1
			}
			sortDoc = append(sortDoc, bson.E{Key: s.Field, Value: dir})
		}
		cmd = append(cmd, bson.E{Key: "sort", Value: sortDoc})
	}
	cmd = append(cmd,
		bson.E{Key: "limit", Value: int64(window + 1)},
		bson.E{Key: "batchSize", Value: int64(window + 1)},
		bson.E{Key: "singleBatch", Value: true},
		bson.E{Key: "readConcern", Value: snapshot})
	var res struct {
		Cursor struct {
			FirstBatch []bson.Raw `bson:"firstBatch"`
		} `bson:"cursor"`
	}
	if err := hot.Database(db).RunCommand(rctx, cmd).Decode(&res); err != nil {
		return out
	}
	docs := make([]bson.Raw, 0, len(res.Cursor.FirstBatch))
	for _, d := range res.Cursor.FirstBatch {
		docs = append(docs, append(bson.Raw(nil), d...))
	}
	if tiered {
		return c.proveFill(ctx, out, filter, docs, window)
	}
	out.complete = len(docs) <= window
	if !out.complete {
		docs = docs[:window]
	}
	out.docs, out.ok = docs, true
	return out
}

// proveFill keeps what the fast tier's answer settles for a tiered namespace,
// and nothing it does not. An entry here is never the whole list: what follows
// the proven head may be in the archive.
//
// The proof is asked for after the read, which is the order it needs. The
// evictor raises its claim and lets every cached copy of the old one expire
// before it deletes anything, so a claim read now is at least as narrow as any
// eviction the read already missed.
func (c *Cache) proveFill(ctx context.Context, out fillResult, filter bson.D, docs []bson.Raw, window int) fillResult {
	q := out.f.q
	if hasEq(q, "_id") {
		// One document at most, and the fast tier holding it is the whole
		// answer, which is how the router answers it too. Not holding it says
		// nothing: the archive may, and the merge asks it.
		if len(docs) > 0 {
			out.docs, out.complete, out.ok = docs, true, true
			return out
		}
		return c.mergeFill(ctx, out, filter, docs, window, true)
	}
	n := 0
	if c.prove != nil && len(docs) > 0 {
		n = min(c.prove(ctx, q.NS, q.Sort, docs), len(docs), window)
	}
	if n >= q.Skip+q.Limit {
		out.docs, out.ok = docs[:n], true
		return out
	}
	return c.mergeFill(ctx, out, filter, docs, window, false)
}

// mergeFill fills what the fast tier cannot prove from both tiers: the fast
// tier's snapshot merged with the archive's answer, as the router's merged read
// does. See MergeFunc.
//
// The entry stays exact afterwards by the rules every tiered entry keeps. An
// archived document the application changes is copied up first, so its new
// state arrives on the stream; one deleted or evicted arrives as a delete,
// which drops the entry; a copy-up of a document older than the entry's tail
// falls past its end and is left out. On a cold-start adoption this is nearly
// every customer's order history, which the proof alone never held.
func (c *Cache) mergeFill(ctx context.Context, out fillResult, filter bson.D, fast []bson.Raw, window int, byID bool) fillResult {
	q := out.f.q
	// An unsorted list has no head to merge: which documents a limit keeps
	// is the server's choice, and the router's merged read refuses it too.
	if c.merge == nil || (!byID && len(q.Sort) == 0) {
		out.unproven, out.ok = true, true
		return out
	}
	docs, ok := c.merge(ctx, q.NS, filter, q.Sort, fast, window)
	if !ok {
		out.unproven, out.ok = true, true
		return out
	}
	if byID {
		// The whole answer, found or not: an _id names one document.
		out.docs, out.complete, out.ok = docs, true, true
		return out
	}
	if len(docs) < q.Skip+q.Limit && len(docs) == window {
		out.unproven, out.ok = true, true
		return out
	}
	// Fewer than the window means both tiers ran out, so this is the whole
	// list; otherwise a head, never complete, since what follows it may be
	// in the archive.
	out.docs, out.complete, out.ok = docs, len(docs) < window, true
	return out
}

// plain reports whether a namespace has no default collation and is not a
// view, which is what the cache can answer exactly. The second result is false
// when it could not be told.
func (c *Cache) plain(ctx context.Context, hot *mongo.Client, ns string) (bool, bool) {
	c.plainMu.Lock()
	ok, seen := c.plainNS[ns]
	c.plainMu.Unlock()
	if seen {
		return ok, true
	}
	db, coll := splitNS(ns)
	cur, err := hot.Database(db).ListCollections(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		return false, false
	}
	ok = true
	for cur.Next(ctx) {
		if _, cerr := cur.Current.LookupErr("options", "collation"); cerr == nil {
			ok = false
		}
		if t, _ := cur.Current.Lookup("type").StringValueOK(); t == "view" {
			ok = false
		}
	}
	_ = cur.Close(ctx)
	c.plainMu.Lock()
	c.plainNS[ns] = ok
	c.plainMu.Unlock()
	return ok, true
}

// installAt installs a fill read at f.p0 and applies to it, alone, every change
// kept for it later than f.p0.
func (c *Cache) installAt(f *inflight, docs []bson.Raw, complete bool, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.installLocked(f.q, docs, complete, n)
	if e == nil {
		return
	}
	c.inherit(e, f.p0)
	for _, ch := range f.buf {
		if !after(ch.At, f.p0) {
			continue
		}
		if !c.applyToEntry(e, ch) {
			return
		}
	}
	c.evict()
}

// applyToEntry brings one entry up to date with a change, the way Apply does
// for every entry of a namespace. It reports false when the entry was dropped.
func (c *Cache) applyToEntry(e *entry, ch Change) bool {
	if _, live := c.entries[e.key]; !live {
		return false
	}
	id, ok := idKey(ch.ID)
	if !ok {
		c.drop(e)
		return false
	}
	_, holds := c.rev[id][e]
	if holds && c.isTiered(ch.NS) && ch.Doc == nil {
		// May only have been evicted. See Tiers.
		c.drop(e)
		return false
	}
	if holds {
		c.removeDoc(e, id)
	}
	sh := e.shape
	if sh.kind == KindCount {
		switch ch.Op {
		case "delete", "replace":
			c.drop(e)
			return false
		case "update":
			if touches(ch.Updated, sh.fields) {
				c.drop(e)
				return false
			}
		}
	}
	if ch.Doc == nil {
		return true
	}
	key, ok := c.keyOf(sh, ch.Doc)
	if !ok || key != e.key {
		return true
	}
	if sh.kind == KindCount {
		if ch.Op == "insert" {
			e.n++
		}
		return true
	}
	if c.fits(sh, e, ch.Doc) {
		c.insertDoc(e, doc{id: id, raw: trimDoc(sh, ch.Doc)})
	}
	_, live := c.entries[e.key]
	return live
}
