package router

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/zelmario/thermocline/internal/checkpoint"
)

// The merged read, the unwaited point read and the live cache's merged fill
// each asked the fast tier's oplog whether a document they were about to take
// from the archive had been updated or deleted since the feeder's position.
// One find on local.oplog.rs per read, scanning every entry since that
// position: measured on the Docker test server under 1,500 writes a second,
// 0.8ms at 32 entries and 3.7ms at 3,000, on the primary every client is
// waiting on. The 200 million order adoption made about 185 of them a second,
// and the fast tier's primary went from 128% to 158% CPU against the run that
// waited instead.
//
// The tracker keeps the same answer in memory. It reads the fast tier's oplog
// as it is written, every update and delete of a tiered namespace by _id and
// every command that could hide one. A question about a window it has read to
// the end of is answered here, with no round trip. One it has not, because
// the read is ahead of it or the window starts before the tracker could see,
// still goes to the oplog.
//
// It followed a change stream, which hands out only what a majority has
// written. On the Docker test server at 1,000 writes a second an insert
// reached a change stream 20.7ms after it was acknowledged at the median,
// and a tailable read of the oplog 0.07ms after (p99 46ms against 0.73ms).
// Every read checks a window ending at the fast tier's reply, so the stream
// was behind almost every one: 1,293 of 5,435 checks answered in memory in a
// two minute store run. Seeing an entry a majority may yet roll back only
// makes a check refuse when it need not.
//
// What it records is what the oplog check counts: updates and deletes by _id,
// compared the way mongod's _id index compares (idKey), and anything else that
// names the namespace or its database, DDL above all, which counts as touching
// every document in it. Inserts are left out, as the oplog check leaves them
// out: see touchesIDs.

// touchRetention is how far back the tracker can answer. A window starting
// earlier goes to the oplog, as does one wider than windowCheckable allows.
const touchRetention = time.Minute

// touchMaxIDs bounds the documents remembered. Past it the oldest half of the
// retention is forgotten early, which only sends more windows to the oplog.
const touchMaxIDs = 2_000_000

type touchMark struct {
	upd, del uint64
}

type touchTracker struct {
	mu   sync.Mutex
	cond *sync.Cond
	// applied is how far the stream has been read, start where it began,
	// horizon how far back what is held reaches. All packed.
	applied, start, horizon uint64
	// inclusive says every event at applied itself has been seen, not only
	// those before it. See advance.
	inclusive bool
	ids       map[string]map[string]touchMark
	ns        map[string]uint64
	db        map[string]uint64
	// dels is the latest delete of any document, per namespace. See
	// deletedSince.
	dels map[string]uint64
	// all is the latest entry that may have touched anything, such as a
	// prepared transaction's commit. See touchCommand.
	all uint64
	n   int
	// wrote is the latest entry of any kind, inserts included, on a tiered
	// namespace of each database. See lastWriteIn.
	wrote map[string]uint64
	// moved is closed and replaced every time applied moves, for a read
	// waiting for the tracker to reach a position. See lastWriteIn.
	moved chan struct{}
	// term is the replica set term of the entry at applied, which says whose
	// history the tracker has read. See lastWriteIn.
	term int64
}

func newTouchTracker() *touchTracker {
	t := &touchTracker{ids: map[string]map[string]touchMark{}, ns: map[string]uint64{}, db: map[string]uint64{},
		dels: map[string]uint64{}, wrote: map[string]uint64{}}
	return t
}

// startAt sets where the stream began, which may be before its first batch.
func (t *touchTracker) startAt(at uint64) {
	t.mu.Lock()
	if t.start == 0 {
		t.start = at
	}
	t.mu.Unlock()
}

// advance records that the stream has been read through at. inclusive says
// every event at at itself has been seen too, which holds after a batch ending
// on an event outside a transaction: that event is the only oplog entry at its
// time. It does not hold inside a transaction, whose events share one time and
// may be split across batches, nor for an empty poll's token, a mark one past
// the last entry where a later write can land.
func (t *touchTracker) advance(at uint64, inclusive bool) {
	t.advanceIn(at, inclusive, 0)
}

// advanceIn is advance for an entry written in term, which lastWriteIn holds
// against the term of the position it is asked to reach.
func (t *touchTracker) advanceIn(at uint64, inclusive bool, term int64) {
	t.mu.Lock()
	if at > t.applied || (at == t.applied && inclusive && !t.inclusive) {
		t.applied, t.inclusive, t.term = at, inclusive, term
		if t.start == 0 {
			t.start = at
		}
		t.prune()
		if t.moved != nil {
			close(t.moved)
			t.moved = nil
		}
	}
	t.mu.Unlock()
}

// touch records one event.
func (t *touchTracker) touch(op, db, coll string, id bson.RawValue, at uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.start == 0 {
		// Nothing is answered before the stream's first position.
		return
	}
	t.wrote[db] = max(t.wrote[db], at)
	switch {
	case coll == "":
		if at > t.db[db] {
			t.db[db] = at
		}
	case op == "update" || op == "replace" || op == "delete":
		if id.Type == 0 {
			t.markNS(db+"."+coll, at)
			return
		}
		m := t.ids[db+"."+coll]
		if m == nil {
			m = map[string]touchMark{}
			t.ids[db+"."+coll] = m
		}
		k := idKey(id)
		mk, had := m[k]
		if op == "delete" {
			mk.del = max(mk.del, at)
			t.dels[db+"."+coll] = max(t.dels[db+"."+coll], at)
		} else {
			mk.upd = max(mk.upd, at)
		}
		m[k] = mk
		if !had {
			t.n++
		}
	case op == "insert":
	default:
		t.markNS(db+"."+coll, at)
	}
}

func (t *touchTracker) markNS(ns string, at uint64) {
	if at > t.ns[ns] {
		t.ns[ns] = at
	}
}

// prune forgets what is older than the retention, or older than half of it
// when too much is held. Called with mu held.
func (t *touchTracker) prune() {
	keep := touchRetention
	if t.n > touchMaxIDs {
		keep /= 2
	}
	at := checkpoint.UnpackTS(t.applied)
	if at.T <= uint32(keep/time.Second) {
		return
	}
	cut := checkpoint.PackTS(bson.Timestamp{T: at.T - uint32(keep/time.Second)})
	if cut <= t.horizon+uint64(1)<<32 && t.n <= touchMaxIDs {
		// At most once a second.
		return
	}
	for ns, m := range t.ids {
		for k, mk := range m {
			if mk.upd <= cut && mk.del <= cut {
				delete(m, k)
				t.n--
			}
		}
		if len(m) == 0 {
			delete(t.ids, ns)
		}
	}
	t.horizon = max(t.horizon, cut)
}

// ask says whether any of ids in ns may have changed after from: updated or
// deleted when deletesOnly is false, deleted when it is true, or anything done
// to the namespace or its database.
//
// It never waits for the stream. touched true is the answer. Otherwise what
// the tracker holds covers the window up to rest, and only (rest, until] is
// left for the oplog, when rest is before until: the entries the stream has not
// delivered yet, a handful. Waiting for them instead cost 7.3ms a read on the
// Docker test server at 2,000 operations a second, and "my orders" went from a
// p95 of 1.6ms on the archive alone to 13.8ms: under ten milliseconds of wait,
// then the query anyway. ok false means the tracker cannot say anything about
// this window, which starts before what it holds, and the oplog is asked from
// from as before.
func (t *touchTracker) ask(ns string, from, until bson.Timestamp, ids []bson.RawValue, deletesOnly bool) (touched bool, rest bson.Timestamp, ok bool) {
	if t == nil {
		return false, bson.Timestamp{}, false
	}
	f := checkpoint.PackTS(from)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.start == 0 || f < t.start || f < t.horizon || t.applied <= f {
		return false, bson.Timestamp{}, false
	}
	db, _ := splitNS(ns)
	if t.ns[ns] > f || t.db[db] > f || t.all > f {
		return true, bson.Timestamp{}, true
	}
	m := t.ids[ns]
	for _, id := range ids {
		mk, had := m[idKey(id)]
		if !had {
			continue
		}
		if mk.del > f || (!deletesOnly && mk.upd > f) {
			return true, bson.Timestamp{}, true
		}
	}
	// Everything through applied has been seen when inclusive, everything
	// before it otherwise: the oplog's ts > rest then picks up at applied.
	rest = checkpoint.UnpackTS(t.applied)
	if !t.inclusive {
		rest = prevTS(rest)
	}
	if checkpoint.PackTS(rest) < f {
		rest = from
	}
	return false, rest, true
}

// lastWriteIn is what a read of database db from the archive has to wait for
// mirroring to reach: the newest write to any tiered namespace of db that the
// tracker has read, once it has read the oplog through through.
//
// A database rather than a namespace, because that is as far as a read can
// reach. A pipeline's $lookup and $unionWith, and a view's definition, name
// collections of the database they run in, and nothing in the read itself
// says which. Taking the database answers for all of them without resolving
// any.
//
// The tracker's own start counts as a write. What came before it was never
// read, so the newest write in db may be anywhere up to there.
//
// It waits for the tracker to reach through, at most until ctx ends. ok false
// says it did not, or that the tracker is not reading, and the caller waits
// the way it always did.
//
// term is the term through was written in. The tracker answers only when the
// entry it has read up to was written in that term too, because only then are
// the two one history. A position is a timestamp, and two nodes across an
// election can both hold one at or past it: the tracker still reading the old
// primary, which carried on as a secondary or holds writes the election will
// roll back, can be past the new primary's lastWrite without having read the
// new primary's writes. A term has one primary, so entries of the same term
// are the same history.
func (t *touchTracker) lastWriteIn(ctx context.Context, db string, through uint64, term int64) (need uint64, ok bool) {
	if t == nil {
		return 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for t.start == 0 || t.applied < through || (t.applied == through && !t.inclusive) {
		if t.moved == nil {
			t.moved = make(chan struct{})
		}
		moved := t.moved
		t.mu.Unlock()
		select {
		case <-ctx.Done():
			t.mu.Lock()
			return 0, false
		case <-moved:
		}
		t.mu.Lock()
	}
	if t.term != term {
		return 0, false
	}
	return max(t.wrote[db], t.all, t.start), true
}

// touchAll records an entry that may have changed any document.
func (t *touchTracker) touchAll(at uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.start != 0 && at > t.all {
		t.all = at
	}
}

// deletedSince says whether any document of ns was deleted after from, for a
// window ask has said it knows. It is never forgotten, so it can only say yes
// too often.
func (t *touchTracker) deletedSince(ns string, from bson.Timestamp) bool {
	if t == nil {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	f := checkpoint.PackTS(from)
	return t.dels[ns] > f || t.all > f
}

// covered is how far what the tracker holds covers a window starting at from,
// with no _id to check yet: rest as ask would give it. ok false means it holds
// nothing that covers from.
func (t *touchTracker) covered(from bson.Timestamp) (rest bson.Timestamp, ok bool) {
	if t == nil {
		return bson.Timestamp{}, false
	}
	f := checkpoint.PackTS(from)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.start == 0 || f < t.start || f < t.horizon || t.applied <= f {
		return bson.Timestamp{}, false
	}
	rest = checkpoint.UnpackTS(t.applied)
	if !t.inclusive {
		rest = prevTS(rest)
	}
	if checkpoint.PackTS(rest) < f {
		rest = from
	}
	return rest, true
}

// prevTS is the timestamp just before ts.
func prevTS(ts bson.Timestamp) bson.Timestamp {
	if ts.I > 0 {
		return bson.Timestamp{T: ts.T, I: ts.I - 1}
	}
	return bson.Timestamp{T: ts.T - 1, I: ^uint32(0)}
}

// followTouches keeps the tracker fed for as long as ctx lives.
func (r *Router) followTouches(ctx context.Context) {
	for ctx.Err() == nil {
		err := r.followTouchesOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		// A read that ended may have missed entries: start again knowing
		// nothing, so nothing is answered from a gap.
		r.touches.mu.Lock()
		r.touches.applied, r.touches.start, r.touches.horizon, r.touches.all = 0, 0, 0, 0
		r.touches.ids, r.touches.ns, r.touches.db, r.touches.n = map[string]map[string]touchMark{}, map[string]uint64{}, map[string]uint64{}, 0
		r.touches.wrote = map[string]uint64{}
		r.touches.mu.Unlock()
		r.log.Warn("the oplog read that answers the oplog check in memory stopped, starting it again",
			"err", err, "meanwhile", "merged reads ask the oplog, as they did before")
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (r *Router) followTouchesOnce(ctx context.Context) error {
	oplog := r.hot.Database("local", options.Database().SetReadPreference(readpref.Primary())).Collection("oplog.rs")
	// From a retention ago rather than from now. The window a read asks about
	// starts at the feeder's position, which on an idle feeder stands still
	// and can be well before this router started: opened at now, the tracker
	// could say nothing about it. An oplog that does not reach that far back
	// is read from its first entry.
	var first, last struct {
		TS bson.Timestamp `bson:"ts"`
	}
	tsOnly := bson.D{{Key: "ts", Value: 1}}
	if err := oplog.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: 1}}).SetProjection(tsOnly)).Decode(&first); err != nil {
		return err
	}
	if err := oplog.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}}).SetProjection(tsOnly)).Decode(&last); err != nil {
		return err
	}
	from := first.TS
	if keep := uint32(touchRetention / time.Second); last.TS.T > keep && last.TS.T-keep > from.T {
		from = bson.Timestamp{T: last.TS.T - keep}
	}
	// Every entry is read, inserts and no-ops too, so the position moves with
	// the oplog. What is kept of one is what the check needs: the _id an
	// update or delete names, the operations of an applyOps by _id alone,
	// and a command whole.
	ifID := func(prefix string) bson.D {
		return bson.D{{Key: "$ifNull", Value: bson.A{prefix + "o2._id", prefix + "o._id"}}}
	}
	proj := bson.D{
		{Key: "_id", Value: 0}, {Key: "ts", Value: 1}, {Key: "t", Value: 1}, {Key: "op", Value: 1}, {Key: "ns", Value: 1},
		{Key: "prev", Value: "$prevOpTime.ts"}, {Key: "multiOpType", Value: 1},
		{Key: "id", Value: ifID("$")},
		{Key: "c", Value: bson.D{{Key: "$cond", Value: bson.A{
			bson.D{{Key: "$eq", Value: bson.A{"$op", "c"}}},
			bson.D{{Key: "$cond", Value: bson.A{
				bson.D{{Key: "$isArray", Value: "$o.applyOps"}},
				bson.D{{Key: "applyOps", Value: bson.D{{Key: "$map", Value: bson.D{
					{Key: "input", Value: "$o.applyOps"},
					{Key: "in", Value: bson.D{{Key: "op", Value: "$$this.op"}, {Key: "ns", Value: "$$this.ns"}, {Key: "id", Value: ifID("$$this.")}}},
				}}}}},
				"$o",
			}}},
			"$$REMOVE",
		}}}},
	}
	cur, err := oplog.Find(ctx, bson.D{{Key: "ts", Value: bson.D{{Key: "$gt", Value: from}}}},
		options.Find().SetCursorType(options.TailableAwait).SetMaxAwaitTime(5*time.Millisecond).SetProjection(proj))
	if err != nil {
		return err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	// Every entry after from is on its way, so a window from it is known once
	// the oplog has been read to the window's end.
	r.touches.startAt(checkpoint.PackTS(from))
	for cur.Next(ctx) {
		var e oplogTouch
		if err := cur.Decode(&e); err != nil {
			return err
		}
		at := checkpoint.PackTS(e.TS)
		switch e.Op {
		case "i":
			r.touchInsert(e.NS, at)
		case "u", "d":
			r.touchOne(e.Op, e.NS, e.ID, at)
		case "c":
			r.touchCommand(e.NS, e.C, at)
			// One entry of a transaction written as several. Its operations
			// take effect at the last entry, which need not name them: an
			// update in the first and a commit that names only other
			// collections. So any entry of such a chain touches everything.
			// The first has no previous entry, and is not told from a
			// transaction of one entry; the chain's second entry covers it.
			//
			// A retryable write batched by the server is chained the same way
			// and marked multiOpType 1, and is not a transaction: each entry
			// applies its own operations at its own time, which the applyOps
			// above has recorded. A copy-up writes this way, and taking each
			// of its entries for a transaction made every database written
			// at that instant, which sent every read back to the full wait.
			// On the Docker test server 18,131 of 129,527 applyOps were these.
			if e.Prev.T != 0 && e.MultiOpType != 1 {
				r.touches.touchAll(at)
			}
		}
		if cur.RemainingBatchLength() == 0 {
			// Each entry has a time of its own, and a forward read of the
			// oplog returns none past a hole, so everything through this
			// one has been seen.
			r.touches.advanceIn(at, true, e.T)
		}
	}
	if err := cur.Err(); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("the oplog cursor ended, the oplog may have rolled past it")
}

// oplogTouch is one oplog entry as followTouchesOnce reads it.
type oplogTouch struct {
	TS bson.Timestamp `bson:"ts"`
	// T is the term the entry was written in.
	T  int64         `bson:"t"`
	Op string        `bson:"op"`
	NS string        `bson:"ns"`
	ID bson.RawValue `bson:"id"`
	C  bson.Raw      `bson:"c"`
	// Prev is the previous entry of the same transaction, zero for the first
	// and for anything else.
	Prev bson.Timestamp `bson:"prev"`
	// MultiOpType 1 marks a retryable write the server batched into several
	// entries, which is not a transaction.
	MultiOpType int32 `bson:"multiOpType"`
}

// touchOne records an update or delete of one document, of a tiered
// namespace only.
func (r *Router) touchOne(op, ns string, id bson.RawValue, at uint64) {
	if !r.cfg.Manages(ns) {
		return
	}
	db, coll := splitNS(ns)
	if id.Type == 0 || id.Type == bson.TypeNull {
		// Not known which document: the namespace.
		r.touches.touch("modify", db, coll, bson.RawValue{}, at)
		return
	}
	id.Value = append([]byte(nil), id.Value...)
	if op == "d" {
		r.touches.touch("delete", db, coll, id, at)
		return
	}
	r.touches.touch("update", db, coll, id, at)
}

// touchInsert records an insert, of a tiered namespace only. The oplog check
// leaves inserts out, so it is kept only as a write to the database.
func (r *Router) touchInsert(ns string, at uint64) {
	if !r.cfg.Manages(ns) {
		return
	}
	db, coll := splitNS(ns)
	r.touches.touch("insert", db, coll, bson.RawValue{}, at)
}

// touchCommand records a command entry, as oplogWindow counts one: an applyOps
// by its operations, DDL on a tiered collection as touching all of it, DDL
// whose collection cannot be told as touching its database, and anything else
// on admin, a prepared transaction's commit above all, as touching everything:
// a prepared transaction's operations carry the time it was prepared, and take
// effect at its commit.
func (r *Router) touchCommand(ns string, c bson.Raw, at uint64) {
	db, _ := splitNS(ns)
	if ops, ok := c.Lookup("applyOps").ArrayOK(); ok {
		vals, err := ops.Values()
		if err != nil {
			r.touches.touchAll(at)
			return
		}
		for _, v := range vals {
			d, ok := v.DocumentOK()
			if !ok {
				r.touches.touchAll(at)
				return
			}
			op, _ := d.Lookup("op").StringValueOK()
			ins, _ := d.Lookup("ns").StringValueOK()
			switch op {
			case "n":
			case "i":
				r.touchInsert(ins, at)
			case "u", "d":
				r.touchOne(op, ins, d.Lookup("id"), at)
			default:
				idb, _ := splitNS(ins)
				r.touches.touch("modify", idb, "", bson.RawValue{}, at)
			}
		}
		return
	}
	if db == "admin" {
		r.touches.touchAll(at)
		return
	}
	elems, err := c.Elements()
	if err != nil || len(elems) == 0 {
		r.touches.touch("modify", db, "", bson.RawValue{}, at)
		return
	}
	name, v := elems[0].Key(), elems[0].Value()
	if name == "renameCollection" {
		src, _ := v.StringValueOK()
		dst, _ := c.Lookup("to").StringValueOK()
		for _, n := range []string{src, dst} {
			if n == "" {
				r.touches.touch("modify", db, "", bson.RawValue{}, at)
				return
			}
			if r.cfg.Manages(n) {
				ndb, ncoll := splitNS(n)
				r.touches.touch("modify", ndb, ncoll, bson.RawValue{}, at)
			}
		}
		return
	}
	coll, ok := v.StringValueOK()
	if !ok || name == "dropDatabase" {
		r.touches.touch("modify", db, "", bson.RawValue{}, at)
		return
	}
	if r.cfg.Manages(db + "." + coll) {
		r.touches.touch("modify", db, coll, bson.RawValue{}, at)
	}
}

// fastTierClock is the fast tier primary's clock, a ping's operationTime.
func (r *Router) fastTierClock(ctx context.Context) (bson.Timestamp, bool) {
	raw, err := r.hot.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}},
		options.RunCmd().SetReadPreference(readpref.Primary())).Raw()
	if err != nil {
		return bson.Timestamp{}, false
	}
	t, i, ok := raw.Lookup("operationTime").TimestampOK()
	return bson.Timestamp{T: t, I: i}, ok
}
