package livecache

import (
	"context"
	"errors"
	"strings"
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
	// point is the namespace and _id of a fill whose query names one _id,
	// "" for any other. Only changes to that document can change its answer.
	// See fillState.keep.
	point string
	buf   []Change
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
	// why is what failed when ok is false and a read was attempted.
	why string
	// until bounds how long the entry may be kept, zero for tieredTTL alone.
	until time.Time
}

// fillState is the applier's side of fills in flight. Only the applier touches
// it.
type fillState struct {
	byKey map[string]*inflight
	byNS  map[string][]*inflight
	// byID holds the fills of queries that name one _id, by namespace and
	// _id, and byNS every other. See keep.
	byID map[string][]*inflight
	work chan *inflight
	done chan fillResult
}

func newFillState() *fillState {
	return &fillState{
		byKey: map[string]*inflight{},
		byNS:  map[string][]*inflight{},
		byID:  map[string][]*inflight{},
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
			f := &inflight{q: q, key: key, p0: p0, point: pointOf(q)}
			select {
			case fs.work <- f:
				fs.byKey[key] = f
				if f.point != "" {
					fs.byID[f.point] = append(fs.byID[f.point], f)
				} else {
					fs.byNS[q.NS] = append(fs.byNS[q.NS], f)
				}
			default:
				c.unpend(q)
			}
		default:
			return
		}
	}
}

// keep records a change for every fill in flight its document can change.
// Called by the applier for each change before it is applied.
//
// A fill of a query naming one _id can only be changed by that document, so it
// is given that document's changes and no others. Every change used to be
// copied to every fill on the namespace: on staging at 11,000 ops/s with 128
// clients, with hundreds of point fills in flight on one collection, that was
// 60 GB allocated in three minutes and the router's CPU spent collecting it.
// A change whose _id cannot be keyed drops every point fill of its namespace,
// as applyToEntry drops an entry it cannot place.
func (fs *fillState) keep(ch Change) {
	for _, f := range fs.byNS[ch.NS] {
		f.add(ch)
	}
	if len(fs.byID) == 0 {
		return
	}
	id, ok := idKey(ch.ID)
	if !ok {
		fs.forgetPoints(ch.NS)
		return
	}
	for _, f := range fs.byID[ch.NS+"\x00"+id] {
		f.add(ch)
	}
}

func (f *inflight) add(ch Change) {
	if f.dropped {
		return
	}
	if len(f.buf) >= maxFillBuffer {
		f.dropped = true
		f.buf = nil
		return
	}
	f.buf = append(f.buf, ch)
}

// pointOf is the namespace and _id a query names, "" when it names no single
// _id.
func pointOf(q Query) string {
	if q.Kind != KindFind {
		return ""
	}
	for _, p := range q.Eq {
		if p.Field != "_id" {
			continue
		}
		// Not an embedded document: idKey keys one by its bytes, and mongod
		// compares one field by field and by value, so {a: 1} and {a: 1.0}
		// are one _id to the query and two keys here. Such a fill is given
		// every change of its namespace, as before.
		if p.Value.Type == bson.TypeEmbeddedDocument || p.Value.Type == bson.TypeArray {
			return ""
		}
		if id, ok := idKey(p.Value); ok {
			return q.NS + "\x00" + id
		}
	}
	return ""
}

// forgetPoints drops the point fills in flight on a namespace, or on every
// namespace when ns is empty.
func (fs *fillState) forgetPoints(ns string) {
	for k, list := range fs.byID {
		if ns != "" && !strings.HasPrefix(k, ns+"\x00") {
			continue
		}
		for _, f := range list {
			f.dropped = true
			f.buf = nil
		}
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
	fs.forgetPoints(ns)
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
	index, at := fs.byNS, f.q.NS
	if f.point != "" {
		index, at = fs.byID, f.point
	}
	list := index[at]
	for i, g := range list {
		if g == f {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(index, at)
	} else {
		index[at] = list
	}
	switch {
	case f.dropped || !r.ok:
		if !f.dropped && r.why != "" {
			c.failed(f.q, r.why)
		}
		c.unpend(f.q)
	case r.unproven:
		c.unproven(f.q)
	default:
		c.installAt(f, r.docs, r.complete, r.n, r.until)
	}
}

// read is a worker's fill: the query's answer as it stood at the fill's
// position, and for a tiered namespace only as far as the fast tier proves.
func (c *Cache) read(ctx context.Context, hot *mongo.Client, f *inflight) fillResult {
	q := f.q
	out := fillResult{f: f}
	plain, ok := c.plain(ctx, hot, q.NS)
	if !ok {
		out.why = "could not read the collection's options"
		return out
	}
	if !plain {
		c.RefuseNS(q.NS)
		return out
	}
	tiered := c.isTiered(q.NS)
	if tiered && q.Kind != KindFind && q.Kind != KindGroup {
		// A count over a tiered namespace is over both tiers, and the fast
		// tier's stream cannot keep one: a copy-up is an insert of a document
		// that was already counted.
		return out
	}
	filter := bson.D{}
	for _, p := range q.Eq {
		filter = append(filter, bson.E{Key: p.Field, Value: p.Value})
	}
	for i := 0; i < len(q.Ranges); {
		ops := bson.D{}
		f := q.Ranges[i].Field
		for ; i < len(q.Ranges) && q.Ranges[i].Field == f; i++ {
			ops = append(ops, bson.E{Key: q.Ranges[i].Op, Value: q.Ranges[i].Value})
		}
		filter = append(filter, bson.E{Key: f, Value: ops})
	}
	db, coll := splitNS(q.NS)
	rctx, cancel := context.WithTimeout(ctx, fillTimeout)
	defer cancel()

	if q.Kind == KindGroup {
		return c.readGroup(rctx, hot.Database(db), coll, filter, out)
	}
	if q.Kind != KindFind {
		var res struct {
			Cursor struct {
				FirstBatch []struct {
					N int64 `bson:"n"`
				} `bson:"firstBatch"`
			} `bson:"cursor"`
		}
		err := snapshotRun(rctx, hot.Database(db), bson.D{
			{Key: "aggregate", Value: coll},
			{Key: "pipeline", Value: bson.A{
				bson.D{{Key: "$match", Value: filter}},
				bson.D{{Key: "$count", Value: "n"}},
			}},
			{Key: "cursor", Value: bson.D{}},
		}, f.p0, &res)
		if err != nil {
			out.why = "count: " + err.Error()
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
		bson.E{Key: "singleBatch", Value: true})
	var res struct {
		Cursor struct {
			FirstBatch []bson.Raw `bson:"firstBatch"`
		} `bson:"cursor"`
	}
	if err := snapshotRun(rctx, hot.Database(db), cmd, f.p0, &res); err != nil {
		out.why = "find: " + err.Error()
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
		var proved int
		proved, out.until = c.prove(ctx, q.NS, q.Sort, docs)
		n = min(proved, len(docs), window)
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
	docs, ok := c.merge(ctx, q.NS, out.f.p0, filter, q.Sort, fast, window)
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
	gen := c.plainGen
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
	found := false
	for cur.Next(ctx) {
		found = true
		if _, cerr := cur.Current.LookupErr("options", "collation"); cerr == nil {
			ok = false
		}
		// A view's documents change under the base collection's name, and a
		// time series collection's under system.buckets, which the stream
		// this cache follows does not deliver. An entry for either is never
		// brought up to date.
		if t, _ := cur.Current.Lookup("type").StringValueOK(); t == "view" || t == "timeseries" {
			ok = false
		}
	}
	_ = cur.Close(ctx)
	if !found {
		// Not there yet, so what it will be is not known: created with a
		// collation, or as a view, it is not plain, and nothing in the stream
		// says when it is created. An empty entry recorded as plain stayed
		// empty while mongod returned what was written after.
		return false, true
	}
	c.plainMu.Lock()
	// A drop forgotten while this listed may have come after the listing,
	// and then what it saw is the collection before the drop: kept, a
	// collection created again with a collation was answered as plain.
	if c.plainGen == gen {
		c.plainNS[ns] = ok
	}
	c.plainMu.Unlock()
	return ok, true
}

// installAt installs a fill read at f.p0 and applies to it, alone, every change
// kept for it later than f.p0.
func (c *Cache) installAt(f *inflight, docs []bson.Raw, complete bool, n int64, until time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.installLocked(f.q, docs, complete, n)
	if e == nil {
		return
	}
	if !until.IsZero() && (e.expires.IsZero() || until.Before(e.expires)) {
		e.expires = until
	}
	e.from = f.p0
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
	_, holds := c.rev[revKey(ch.NS, id)][e]
	if holds && c.isTiered(ch.NS) && ch.Doc == nil {
		// May only have been evicted. See Tiers.
		c.drop(e)
		return false
	}
	// The same rule Apply keeps: where a member goes in an order mongod's plan
	// decides is not this cache's to say, and replaying a fill's buffered
	// changes re-placed it after its equals. See planOrdered.
	if holds && ch.Doc != nil && planOrdered(e, id, ch.Doc) {
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
			if touches(ch.Updated, filterFields(sh)) {
				c.drop(e)
				return false
			}
		}
	}
	if ch.Doc == nil {
		return true
	}
	key, ok := c.keyOf(sh, ch.Doc)
	if !ok || key != e.group {
		return true
	}
	values, ok := c.rangeValues(sh, ch.Doc)
	if !ok || !inBounds(e.bounds, values) {
		_, live := c.entries[e.key]
		return live
	}
	if sh.kind == KindCount {
		if ch.Op == "insert" {
			e.n++
		}
		return true
	}
	if sh.kind == KindGroup {
		return c.addToGroup(sh, e, id, ch.Doc)
	}
	if c.fits(sh, e, ch.Doc) {
		if planOrdered(e, id, ch.Doc) {
			c.drop(e)
			return false
		}
		c.insertDoc(e, doc{id: id, raw: trimDoc(sh, ch.Doc)})
	}
	_, live := c.entries[e.key]
	return live
}

// readGroup is a grouped entry's fill: the documents its $match takes, read
// at the fill's position with only the fields the grouping reads. The totals
// are computed here, from the same documents the stream will change, rather
// than asked of mongod, so an entry is always the sum of what it remembers.
func (c *Cache) readGroup(ctx context.Context, db *mongo.Database, coll string, filter bson.D, out fillResult) fillResult {
	q := out.f.q
	proj := bson.D{{Key: "_id", Value: 1}}
	keep := append(append([]string(nil), q.Group.fields()...), filterFieldsOf(q)...)
	for _, f := range keep {
		if f == "_id" || hasKey(proj, f) {
			continue
		}
		proj = append(proj, bson.E{Key: f, Value: 1})
	}
	cmd := bson.D{
		{Key: "find", Value: coll},
		{Key: "filter", Value: filter},
		{Key: "projection", Value: proj},
		{Key: "limit", Value: int64(MaxGroupDocs + 1)},
		{Key: "batchSize", Value: int64(10000)},
	}
	cur, err := snapshotCursor(ctx, db, cmd, out.f.p0)
	if err != nil {
		out.why = "group: " + err.Error()
		return out
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	for cur.Next(ctx) {
		if len(out.docs) == MaxGroupDocs {
			out.why = "group: the $match takes more documents than an entry remembers"
			return out
		}
		out.docs = append(out.docs, append(bson.Raw(nil), cur.Current...))
	}
	if err := cur.Err(); err != nil {
		out.why = "group: " + err.Error()
		return out
	}
	if c.isTiered(q.NS) {
		return c.withArchive(ctx, out, filter, proj)
	}
	out.complete, out.ok = true, true
	return out
}

// withArchive completes a grouping on a tiered namespace with the archive's
// documents: the fast tier's copy of a document wins, since it is the newer,
// and every other document the $match takes is the archive's.
//
// The entry then stays exact by the rules every tiered entry keeps. A copy-up
// arrives as an insert of a document it already holds, which replaces that
// document's part; a document the application changes is copied up first, so
// its new state arrives too; and any delete of a document it holds drops it,
// because an eviction is a delete of a document that still exists. What the
// stream cannot see, the archive changing under it, is bounded by tieredTTL.
func (c *Cache) withArchive(ctx context.Context, out fillResult, filter, proj bson.D) fillResult {
	if c.archive == nil {
		out.unproven, out.ok = true, true
		return out
	}
	archived, ok := c.archive(ctx, out.f.q.NS, out.f.p0, filter, proj)
	if !ok {
		out.unproven, out.ok = true, true
		out.docs = nil
		return out
	}
	have := make(map[string]bool, len(out.docs))
	for _, d := range out.docs {
		if id, ok := docKey(d); ok {
			have[id] = true
		}
	}
	for _, d := range archived {
		id, ok := docKey(d)
		if !ok {
			out.why = "group: an archived document has no _id"
			out.docs = nil
			return out
		}
		if have[id] {
			continue
		}
		if len(out.docs) == MaxGroupDocs {
			out.why = "group: the $match takes more documents than an entry remembers"
			out.docs = nil
			return out
		}
		out.docs = append(out.docs, d)
	}
	out.complete, out.ok = true, true
	return out
}

func filterFieldsOf(q Query) []string {
	var out []string
	for _, p := range q.Eq {
		out = append(out, p.Field)
	}
	for _, r := range q.Ranges {
		out = append(out, r.Field)
	}
	return out
}

func hasKey(d bson.D, k string) bool {
	for _, e := range d {
		if e.Key == k {
			return true
		}
	}
	return false
}

// snapshotCursor is snapshotRun for a read of more than one batch: the
// getMores of a snapshot cursor read at the same cluster time.
func snapshotCursor(ctx context.Context, db *mongo.Database, cmd bson.D, p0 bson.Timestamp) (*mongo.Cursor, error) {
	at := func(ts bson.Timestamp) bson.D {
		return append(cmd[:len(cmd):len(cmd)], bson.E{Key: "readConcern", Value: bson.D{
			{Key: "level", Value: "snapshot"}, {Key: "atClusterTime", Value: ts}}})
	}
	cur, err := db.RunCommandCursor(ctx, at(p0))
	var ce mongo.CommandError
	if !errors.As(err, &ce) || ce.Code != 72 || !strings.Contains(ce.Message, "atClusterTime") {
		return cur, err
	}
	raw, perr := db.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Raw()
	if perr != nil {
		return nil, err
	}
	t, i, ok := raw.Lookup("operationTime").TimestampOK()
	if !ok {
		return nil, err
	}
	now := bson.Timestamp{T: t, I: i}
	if !after(p0, now) {
		return nil, err
	}
	return db.RunCommandCursor(ctx, at(now))
}

// snapshotRun runs cmd as a snapshot read at p0, the stream's position.
//
// On a quiet stream that position is the resume token of an empty poll, a
// high-water mark that can sit an increment past the node's own clock, and
// mongod refuses a snapshot there: "readConcern atClusterTime value must not
// be greater than the current clusterTime". Every fill of a collection nothing
// was writing failed that way until some write moved the clock, so on a quiet
// collection nothing was cached. Measured through testquery with the router's
// verify mode: 0 fills installed in 50.
//
// The node's clock is past every entry in its oplog, so between it and p0
// there is nothing, and a snapshot at the clock is the snapshot at p0. That is
// the one retry.
func snapshotRun(ctx context.Context, db *mongo.Database, cmd bson.D, p0 bson.Timestamp, out any) error {
	at := func(ts bson.Timestamp) bson.D {
		return append(cmd[:len(cmd):len(cmd)], bson.E{Key: "readConcern", Value: bson.D{
			{Key: "level", Value: "snapshot"}, {Key: "atClusterTime", Value: ts}}})
	}
	err := db.RunCommand(ctx, at(p0)).Decode(out)
	var ce mongo.CommandError
	if !errors.As(err, &ce) || ce.Code != 72 || !strings.Contains(ce.Message, "atClusterTime") {
		return err
	}
	raw, perr := db.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Raw()
	if perr != nil {
		return err
	}
	t, i, ok := raw.Lookup("operationTime").TimestampOK()
	if !ok {
		return err
	}
	now := bson.Timestamp{T: t, I: i}
	if !after(p0, now) {
		return err
	}
	return db.RunCommand(ctx, at(now)).Decode(out)
}
