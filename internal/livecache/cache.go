package livecache

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// fillWindow is how much more than a request an entry is filled with, so the
// next page and a few deletions are still answerable.
const fillWindow = 50

// Cache holds the views. One goroutine, the applier, changes entries; readers
// take the lock only to look.
type Cache struct {
	mu sync.Mutex

	maxBytes int
	bytes    int
	lru      *list.List

	entries map[string]*entry
	// groups holds the entries of shapes with a range, by group key, so a
	// document's values find every entry it may belong to. See groupKey.
	groups map[string]map[*entry]struct{}
	// shapes are every shape asked for, per namespace, which is what an
	// incoming change is applied against.
	shapes map[string]map[string]*shape
	// rev names the list entries each document is in, by docKey.
	rev map[string]map[*entry]struct{}
	// refused are shapes a document showed could not be answered exactly: an
	// array or a missing value where the shape needs a single one.
	refused map[string]bool
	// refusedNS are namespaces never to answer, such as one with a collation.
	refusedNS map[string]bool

	// applied is how far the change stream has been applied, and pending are
	// the writes this router acknowledged per namespace that it has not yet
	// applied, oldest first. A namespace is served while none of them is older
	// than maxStale. See fresh.
	applied bson.Timestamp
	// closed is how far every event is known applied, which trails applied
	// by the events that share its cluster time. A transaction, and on 8.0 a
	// batched write, puts many events at one time, and the first of them set
	// applied: a write noted at that time was then taken as applied with the
	// rest still to come, and a count read right after an insertMany was
	// short. A later event, the end of a batch or an empty poll closes it.
	closed  bson.Timestamp
	pending map[string][]pendingWrite
	// pendingDB is the same for writes naming a whole database.
	pendingDB map[string][]pendingWrite
	// Writes classified finer than a namespace, see NoteWrites. pendingEntry
	// holds an entry by its key, pendingDoc every entry holding a document by
	// ns and _id, pendingCount every count of a namespace. classified is
	// every classified write per namespace, which an entry installed after
	// the write inherits. sweeps counts calls to Advance between clean-ups of
	// queues nobody looked at.
	pendingEntry, pendingDoc, pendingCount, classified map[string][]pendingWrite
	sweeps                                             int
	// maxStale is how long an acknowledged write may go unapplied while the
	// cache still answers. Zero is read-your-writes. See SetMaxStaleness.
	maxStale time.Duration
	// now is the clock, replaceable in tests.
	now func() time.Time
	// fillDelay holds every fill this long after its read, so a test can
	// widen the window in which changes arrive between a fill's position and
	// its install. Zero outside tests.
	fillDelay time.Duration

	// readers are the users mongod has let read each namespace lately. See
	// Allowed.
	readers map[string]map[string]time.Time

	fillq   chan Query
	filling map[string]bool
	// unprovenAt are tiered entries a fill could not prove, by when, so the
	// same miss does not cost a second read of the fast tier every time.
	unprovenAt map[string]time.Time
	// failedAt are entries whose last fill failed, with why, for State.
	failedAt map[string]fillFailure
	// missedOnce are tiered entries missed once, by when. See admitTiered.
	missedOnce map[string]time.Time
	// plainNS says, per namespace, that it has no default collation and is not
	// a view. Read and written by the fill workers, under plainMu.
	plainMu sync.Mutex
	plainNS map[string]bool
	// plainGen counts what forgetPlain and Reset forgot, so a listing that a
	// drop overtook does not store its answer. See plain.
	plainGen uint64

	// tiered says a namespace is tiered, and prove how much of the fast tier's
	// answer for one is the whole answer's. See Tiers.
	tiered  func(ns string) bool
	prove   ProveFunc
	merge   MergeFunc
	archive ArchiveFunc

	Hits, Misses, Behind, Fills, Dropped, Refusals, Unproven atomic.Int64
}

// ProveFunc reports how many of docs, the head of the fast tier's answer to a
// query on a tiered namespace sorted by sort, are certainly the head of the
// answer over both tiers. Zero proves nothing. until is when what the proof
// rested on is to be asked again, zero for no such time; an entry it proves is
// kept no longer. See tieredTTL.
type ProveFunc func(ctx context.Context, ns string, sort []SortKey, docs []bson.Raw) (n int, until time.Time)

// MergeFunc completes what the fast tier's answer does not prove: fast is the
// fast tier's head of the answer to a query on a tiered namespace, eq its
// filter and sort its sort, and it returns the head of the answer over both
// tiers, at most window long. ok false means it cannot be told now. at is the
// position fast was read at, so a write the fast tier's snapshot already
// reflects is one the archive's answer is checked against too.
type MergeFunc func(ctx context.Context, ns string, at bson.Timestamp, eq bson.D, sort []SortKey, fast []bson.Raw, window int) (docs []bson.Raw, ok bool)

// Tiers lets the cache hold tiered namespaces. Called before Run.
//
// A tiered namespace differs in two ways, and both come from the fast tier
// holding only part of it. An entry is filled with only what prove says the
// fast tier's answer settles, which for a sorted list is the router's own
// newest-first proof, and for an _id is the document being there. And a
// document leaving the fast tier is not known to have gone: an eviction is a
// delete on its change stream, of a document that still exists. So every entry
// holding a document that leaves is dropped, rather than the document being
// taken out of it.
//
// A document arriving needs nothing: a copy-up inserts a document's real state,
// and state is what is applied.
func (c *Cache) Tiers(tiered func(ns string) bool, prove ProveFunc, merge MergeFunc) {
	c.tiered, c.prove, c.merge = tiered, prove, merge
}

// ArchiveFunc reads, for a grouping on a tiered namespace, the documents of
// the archive that filter takes, live ones only, with only proj's fields. ok
// false means it cannot be told now: the archive's answer could hold a copy
// the fast tier has since changed or deleted. at is the position the fast
// tier's half was read at, as for MergeFunc.
type ArchiveFunc func(ctx context.Context, ns string, at bson.Timestamp, filter, proj bson.D) (docs []bson.Raw, ok bool)

// GroupArchive lets groupings on tiered namespaces be filled. Called before
// Run. See readGroup.
func (c *Cache) GroupArchive(f ArchiveFunc) { c.archive = f }

func (c *Cache) isTiered(ns string) bool {
	return c.tiered != nil && c.tiered(ns)
}

type shape struct {
	id     string
	kind   Kind
	ns     string
	fields []string
	// rng are the shape's range fields and operators, the bounds being each
	// entry's own.
	rng  []RangePred
	sort []SortKey
	// group is the grouping of a KindGroup shape.
	group *GroupSpec
	// proj is the projection every query of the shape asks for, which is
	// what its entries keep of a document. See trimDoc.
	proj Projection
}

// trimDoc is what an entry of a shape keeps of a document: what its projection
// leaves, and always _id and the fields the shape filters and sorts by, which
// applying a change reads. Every entry used to keep the whole document, so a
// customer's order history held each order's 800 byte note that the query
// projected away, and 256 MB held a fraction of the entries it could.
func trimDoc(sh *shape, d bson.Raw) bson.Raw {
	if !sh.proj.Set {
		return d
	}
	elems, err := d.Elements()
	if err != nil {
		return d
	}
	keep := func(k string) bool {
		if k == "_id" {
			return true
		}
		for _, f := range sh.fields {
			if f == k {
				return true
			}
		}
		for _, r := range sh.rng {
			if r.Field == k {
				return true
			}
		}
		for _, s := range sh.sort {
			if s.Field == k {
				return true
			}
		}
		if sh.proj.Include {
			return sh.proj.Fields[k]
		}
		return !sh.proj.Fields[k]
	}
	all := true
	for _, e := range elems {
		if !keep(e.Key()) {
			all = false
			break
		}
	}
	if all {
		return d
	}
	out := make([]byte, 4, len(d))
	for _, e := range elems {
		if keep(e.Key()) {
			out = append(out, e...)
		}
	}
	out = append(out, 0)
	binary.LittleEndian.PutUint32(out[:4], uint32(len(out)))
	return out
}

type entry struct {
	key   string
	shape *shape
	// group is the entry's group key, and bounds its range, for a shape with
	// one. See groupKey.
	group  string
	bounds []RangePred
	// grp is a grouped entry's documents and totals. See group.go.
	grp *groupState
	// docs is the sorted head of the list, complete says it is the whole of
	// it. For a count, n is the count.
	docs     []doc
	complete bool
	n        int64
	bytes    int
	el       *list.Element
	// expires is when an entry on a tiered namespace is asked for again. See
	// tieredTTL.
	expires time.Time
	// from is the position the entry was read at. Everything at or before it
	// is in what was read, so an insert there is not counted again. See Apply.
	from bson.Timestamp
}

// tieredTTL is how long an entry on a tiered namespace is kept before it is
// filled again.
//
// What made it exact when filled can change where the stream does not see:
// the archive. The proof rests on the bracket of ages the archive holds, which
// the router asks again at most a minute later (bracketRecheckSafe), and a
// merge rests on the archive's head. testbson writes a Timestamp age into the
// archive, which sorts above every Date: the router caught up within its
// minute, and the cache, asked once, would have kept the old newest three for
// good. Found by the router's verify mode. Bounded now, at ten seconds past
// the router's own minute at worst, for one fill per entry per ten seconds.
const tieredTTL = 10 * time.Second

type doc struct {
	id  string
	raw bson.Raw
}

// New makes an empty cache bounded to maxBytes of documents.
func New(maxBytes int) *Cache {
	return &Cache{
		maxBytes:     maxBytes,
		lru:          list.New(),
		entries:      map[string]*entry{},
		groups:       map[string]map[*entry]struct{}{},
		shapes:       map[string]map[string]*shape{},
		rev:          map[string]map[*entry]struct{}{},
		refused:      map[string]bool{},
		refusedNS:    map[string]bool{},
		pending:      map[string][]pendingWrite{},
		pendingDB:    map[string][]pendingWrite{},
		pendingEntry: map[string][]pendingWrite{},
		pendingDoc:   map[string][]pendingWrite{},
		pendingCount: map[string][]pendingWrite{},
		classified:   map[string][]pendingWrite{},
		now:          time.Now,
		readers:      map[string]map[string]time.Time{},
		fillq:        make(chan Query, 1024),
		filling:      map[string]bool{},
		unprovenAt:   map[string]time.Time{},
		failedAt:     map[string]fillFailure{},
		missedOnce:   map[string]time.Time{},
		plainNS:      map[string]bool{},
	}
}

// unprovenRetry is how long a tiered entry the fast tier could not prove is not
// asked for again. The fast tier gains what would prove it only as documents
// are written or warmed, which is slow next to how often a page is asked for.
const unprovenRetry = 5 * time.Second

// maxUnproven bounds what is remembered of unproven fills.
const maxUnproven = 100000

// readerTTL is how long a user mongod let read a namespace is trusted to still
// be allowed to. Revoking access takes effect here within it.
const readerTTL = time.Minute

// Allowed reports whether user has recently been let read ns by mongod. With
// authentication proxied, mongod decides who may read what, and an answer from
// memory skips mongod; so it is given only to somebody mongod has said yes to.
func (c *Cache) Allowed(ns, user string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	at, ok := c.readers[ns][user]
	return ok && time.Since(at) < readerTTL
}

// Proved records that mongod answered user's read of ns.
func (c *Cache) Proved(ns, user string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.readers[ns]
	if m == nil {
		m = map[string]time.Time{}
		c.readers[ns] = m
	}
	m[user] = time.Now()
}

// pendingWrite is one acknowledged write not yet applied: its position, and
// when it was acknowledged.
type pendingWrite struct {
	ts bson.Timestamp
	at time.Time
}

// maxPending bounds the writes remembered per namespace. Past it the two
// oldest are merged, which keeps the earlier time and the later position, so
// the merged write is waited for sooner and for longer: stricter, never laxer.
const maxPending = 4096

// SetMaxStaleness sets how long a write this router acknowledged may go
// unapplied while the cache still answers its namespace. Called before Run.
//
// Zero, the default, is read-your-writes: after a write to a namespace, every
// read of it goes to mongod until the change stream has applied the write. On
// a namespace written all the time that is every read, which is what measuring
// a store found: 1.68 million reads held for the stream against 196,000
// answered. A bound is what ReadySet offers, and less than it promises, since
// ReadySet's answers lag its replication stream by however long that takes:
// here an answer may miss a write, but never one acknowledged more than d ago.
func (c *Cache) SetMaxStaleness(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxStale = d
}

// NoteWrite records that this router acknowledged a write to ns at or before
// at. Until the change stream is applied that far, ns is answered by mongod
// once the write is older than the staleness bound, and at once with no bound,
// so a client reads what it has just written. See SetMaxStaleness.
func (c *Cache) NoteWrite(ns string, at bson.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending[ns] = c.noteWrite(c.pending[ns], at)
}

// allKey is the pendingDB key for a write that may have touched anything.
const allKey = "\x00all"

// NoteWriteAll is NoteWrite for a write that may have touched any namespace:
// a transaction's commit. Its statements were noted as they were acknowledged,
// at times before the commit, and the stream carries their changes at the
// commit's time, so an event between the two could pass those marks while the
// transaction was still unapplied. The commit's reply names no namespace, so
// it holds every one.
func (c *Cache) NoteWriteAll(at bson.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingDB[allKey] = c.noteWrite(c.pendingDB[allKey], at)
}

// NoteWriteDB is NoteWrite for a write whose namespace is a whole database.
func (c *Cache) NoteWriteDB(db string, at bson.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingDB[db] = c.noteWrite(c.pendingDB[db], at)
}

// WriteNote is what a write could have changed in one namespace, as far as the
// command says. Whole means anything in it; otherwise IDs are the documents it
// named by _id, Fields the top-level fields an update set or removed (nil for a
// delete), and Docs the documents an insert added.
type WriteNote struct {
	NS     string
	Whole  bool
	IDs    []bson.RawValue
	Fields []string
	Docs   []bson.Raw
}

// NoteWrites records writes this router acknowledged at or before at, held
// back from only what each can have changed.
//
// A write used to hold its whole namespace until the stream had applied it.
// On a shop every checkout changes a product's stock, so every product page
// waited on every checkout. A write that names its documents can be held to
// them: an update or delete by _id holds the entries holding that document,
// unless it changes a field some cached query filters or sorts by, which can
// move the document into entries that do not hold it yet; an insert holds the
// entries its document belongs in. Counts of a namespace are held by any
// delete or update, which they cannot see by document. Anything else holds the
// namespace, as before.
//
// An entry filled after the write was noted holds none of this, so it inherits
// every classified write of its namespace newer than the position it was read
// at. See installAt.
func (c *Cache) NoteWrites(notes []WriteNote, at bson.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range notes {
		if n.Whole {
			c.pending[n.NS] = c.noteWrite(c.pending[n.NS], at)
			continue
		}
		shapes := c.shapes[n.NS]
		whole := false
		for _, sh := range shapes {
			if touches(n.Fields, filterFields(sh)) || touches(n.Fields, sortFields(sh)) {
				whole = true
			}
		}
		if whole {
			c.pending[n.NS] = c.noteWrite(c.pending[n.NS], at)
			continue
		}
		c.classified[n.NS] = c.noteWrite(c.classified[n.NS], at)
		for _, id := range n.IDs {
			k, ok := idKey(id)
			if !ok {
				c.pending[n.NS] = c.noteWrite(c.pending[n.NS], at)
				break
			}
			dk := n.NS + "\x00" + k
			c.pendingDoc[dk] = c.noteWrite(c.pendingDoc[dk], at)
		}
		if len(n.IDs) > 0 {
			c.pendingCount[n.NS] = c.noteWrite(c.pendingCount[n.NS], at)
		}
		for _, d := range n.Docs {
			for _, sh := range shapes {
				key, ok := c.keyOfQuiet(sh, d)
				if ok {
					c.pendingEntry[key] = c.noteWrite(c.pendingEntry[key], at)
					continue
				}
				// A filtered field present with a value this cannot key, an
				// array or a decimal, still matches entries mongod would put
				// the document in: {tag: ["a", "b"]} is in {tag: "a"}. Noted
				// against nothing, a read right after the insert was answered
				// without it until the stream refused the shape. So the whole
				// namespace waits for the stream instead. A field that is
				// missing puts the document in no entry and needs nothing.
				if c.fieldsPresent(sh, d) {
					c.pending[n.NS] = c.noteWrite(c.pending[n.NS], at)
				}
			}
		}
	}
}

// filterFields are the fields a shape filters by, equality and range.
func filterFields(sh *shape) []string {
	if len(sh.rng) == 0 {
		return sh.fields
	}
	out := append([]string(nil), sh.fields...)
	for _, r := range sh.rng {
		out = append(out, r.Field)
	}
	return out
}

func sortFields(sh *shape) []string {
	out := make([]string, len(sh.sort))
	for i, s := range sh.sort {
		out[i] = s.Field
	}
	return out
}

// inherit holds a newly installed entry back by every classified write of its
// namespace later than the position it was read at. Called with mu held.
func (c *Cache) inherit(e *entry, p0 bson.Timestamp) {
	for _, w := range c.classified[e.shape.ns] {
		if after(w.ts, p0) && after(w.ts, c.closed) {
			c.pendingEntry[e.key] = append(c.pendingEntry[e.key], w)
		}
	}
}

// sweep forgets queues the stream has passed that no Get has looked at since,
// and what Allowed and Apply would otherwise keep for ever. Called with mu
// held, every so often.
//
// readers gained an entry per user per namespace and lost none. Without SCRAM
// the user is the connection, so every connection that ever read a namespace
// stayed, and an application that churns connections grew it without bound.
// shapes gained one per distinct shape, a $limit or a batchSize being part of
// one, and every change event walks a namespace's shapes. A shape no entry uses
// is dropped; the next fill that needs it makes it again.
func (c *Cache) sweep() {
	cut := time.Now().Add(-readerTTL)
	for ns, m := range c.readers {
		for u, at := range m {
			if at.Before(cut) {
				delete(m, u)
			}
		}
		if len(m) == 0 {
			delete(c.readers, ns)
		}
	}
	used := make(map[*shape]bool, len(c.entries))
	for _, e := range c.entries {
		used[e.shape] = true
	}
	for ns, m := range c.shapes {
		for id, sh := range m {
			if !used[sh] {
				delete(m, id)
			}
		}
		if len(m) == 0 {
			delete(c.shapes, ns)
		}
	}

	for _, m := range []map[string][]pendingWrite{c.pendingEntry, c.pendingDoc, c.pendingCount, c.classified} {
		for k, q := range m {
			stale := true
			for _, w := range q {
				if after(w.ts, c.closed) {
					stale = false
					break
				}
			}
			if stale {
				delete(m, k)
			}
		}
	}
}

func (c *Cache) noteWrite(q []pendingWrite, ts bson.Timestamp) []pendingWrite {
	if !after(ts, c.closed) {
		return q
	}
	now := c.now()
	// Writes within a millisecond of each other are one: the bound is
	// measured from the earliest, and the position is the newest.
	if n := len(q); n > 0 && now.Sub(q[n-1].at) < time.Millisecond {
		if after(ts, q[n-1].ts) {
			q[n-1].ts = ts
		}
		return q
	}
	q = append(q, pendingWrite{ts: ts, at: now})
	if len(q) > maxPending {
		if after(q[0].ts, q[1].ts) {
			q[1].ts = q[0].ts
		}
		q[1].at = q[0].at
		q = q[1:]
	}
	return q
}

// fresh reports whether a namespace's pending writes allow an answer, and
// forgets those the stream has applied. Called with mu held.
func (c *Cache) fresh(m map[string][]pendingWrite, key string, now time.Time) bool {
	q := m[key]
	i := 0
	// Positions only go up along the queue, but a merge can raise one
	// above its successor's, so this stops at the first unapplied write.
	for i < len(q) && !after(q[i].ts, c.closed) {
		i++
	}
	if i == len(q) {
		delete(m, key)
		return true
	}
	if i > 0 {
		q = q[i:]
		m[key] = q
	}
	return c.maxStale > 0 && now.Sub(q[0].at) < c.maxStale
}

func after(a, b bson.Timestamp) bool {
	return a.T > b.T || (a.T == b.T && a.I > b.I)
}

// entryFresh reports whether the writes classified to an entry let it answer.
// See NoteWrites. Called with mu held.
func (c *Cache) entryFresh(e *entry, q Query, now time.Time) bool {
	if !c.fresh(c.pendingEntry, e.key, now) {
		return false
	}
	// An insert is noted against the group, not knowing which bounds hold it.
	if e.group != e.key && !c.fresh(c.pendingEntry, e.group, now) {
		return false
	}
	if e.shape.kind == KindCount || e.shape.kind == KindGroup {
		return c.fresh(c.pendingCount, e.shape.ns, now)
	}
	end := min(q.Skip+q.Limit, len(e.docs))
	start := min(q.Skip, end)
	for _, d := range e.docs[start:end] {
		if !c.fresh(c.pendingDoc, e.shape.ns+"\x00"+d.id, now) {
			return false
		}
	}
	return true
}

// Get answers q from memory, or reports a miss and asks for a fill.
func (c *Cache) Get(q Query) (bson.Raw, bool) {
	key := q.entryKey()
	c.mu.Lock()
	if c.refusedNS[q.NS] || c.refused[q.shapeID()] {
		c.mu.Unlock()
		return nil, false
	}
	// A grouping on a tiered namespace is filled from both tiers, once the
	// router lets it. See readGroup.
	if c.isTiered(q.NS) && q.Kind == KindGroup && c.archive == nil {
		c.mu.Unlock()
		return nil, false
	}
	db, _ := splitNS(q.NS)
	now := c.now()
	if !c.fresh(c.pending, q.NS, now) || !c.fresh(c.pendingDB, db, now) || !c.fresh(c.pendingDB, allKey, now) {
		c.mu.Unlock()
		c.Behind.Add(1)
		return nil, false
	}
	e, ok := c.entries[key]
	if ok && !e.expires.IsZero() && now.After(e.expires) {
		c.drop(e)
		ok = false
	}
	if ok && q.Kind == KindFind && !e.complete && q.Skip+q.Limit > len(e.docs) {
		ok = false
	}
	if !ok {
		c.mu.Unlock()
		c.Misses.Add(1)
		// A count over a tiered namespace is over both tiers, which the fast
		// tier's stream cannot keep (see read), so no fill is asked for.
		if q.Kind == KindFind || q.Kind == KindGroup || !c.isTiered(q.NS) {
			c.requestFill(q)
		}
		return nil, false
	}
	if !c.entryFresh(e, q, now) {
		c.mu.Unlock()
		c.Behind.Add(1)
		return nil, false
	}
	c.lru.MoveToFront(e.el)
	var reply bson.Raw
	if q.Kind == KindFind {
		end := min(q.Skip+q.Limit, len(e.docs))
		start := min(q.Skip, end)
		out := make([]bson.Raw, 0, end-start)
		for _, d := range e.docs[start:end] {
			out = append(out, d.raw)
		}
		c.mu.Unlock()
		reply = findReply(q, out)
	} else if e.grp != nil {
		out, ok := e.grp.output()
		c.mu.Unlock()
		if !ok {
			// mongod's own answer would depend on an order this does not
			// know. See groupState.output.
			c.Misses.Add(1)
			return nil, false
		}
		reply = findReply(q, out)
	} else {
		n := e.n
		c.mu.Unlock()
		reply = countReply(q, n)
	}
	c.Hits.Add(1)
	return reply, true
}

func (c *Cache) requestFill(q Query) {
	c.mu.Lock()
	key := q.entryKey()
	if c.filling[key] {
		c.mu.Unlock()
		return
	}
	if at, ok := c.unprovenAt[key]; ok {
		if time.Since(at) < unprovenRetry {
			c.mu.Unlock()
			return
		}
		delete(c.unprovenAt, key)
	}
	if !c.admitTiered(q, key) {
		c.mu.Unlock()
		return
	}
	c.filling[key] = true
	c.mu.Unlock()
	// Its values point into the client's message, which is reused once the
	// reply is sent, and the fill runs after that.
	q = q.detached()
	select {
	case c.fillq <- q:
	default:
		c.mu.Lock()
		delete(c.filling, key)
		c.mu.Unlock()
	}
}

// admitWindow is how soon a tiered entry must be missed again to be filled.
const admitWindow = 2 * time.Minute

// admitTiered says whether a miss on q should fill it, which on a tiered
// namespace waits for the second miss within admitWindow. Called with mu held.
//
// A fill there that the fast tier cannot prove reads the archive, on top of the
// archive read the client's own query already made. On the 200 million order
// adoption, every customer's "my orders" was such a fill and almost none was
// read again inside the entry's life: 25 million customers. The fills took the
// archive's network volume to 98% busy, its reads stalled in steps of 200ms, and
// "my orders" went from a p95 of 14ms alone to 213ms. The fill a second visit
// asks for is one somebody is coming back to. This is TinyLFU's doorkeeper,
// applied only where a fill is expensive: an untiered fill reads the fast tier.
func (c *Cache) admitTiered(q Query, key string) bool {
	if !c.isTiered(q.NS) {
		return true
	}
	now := c.now()
	if at, ok := c.missedOnce[key]; ok && now.Sub(at) < admitWindow {
		delete(c.missedOnce, key)
		return true
	}
	if len(c.missedOnce) >= maxUnproven {
		clear(c.missedOnce)
	}
	c.missedOnce[key] = now
	return false
}

// State says why Get would not answer q now, or "hit" when it would. For the
// router's verify mode, which waits out the states that pass on their own:
// "filling", "behind" and "entry behind".
func (c *Cache) State(q Query) string {
	key := q.entryKey()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refusedNS[q.NS] || c.refused[q.shapeID()] {
		return "refused"
	}
	db, _ := splitNS(q.NS)
	now := c.now()
	if !c.fresh(c.pending, q.NS, now) || !c.fresh(c.pendingDB, db, now) || !c.fresh(c.pendingDB, allKey, now) {
		return "behind"
	}
	e, ok := c.entries[key]
	if ok && !e.expires.IsZero() && now.After(e.expires) {
		ok = false
	}
	switch {
	case !ok && q.Kind != KindFind && c.isTiered(q.NS):
		return "tiered count"
	case !ok && c.filling[key]:
		return "filling"
	case !ok:
		if _, un := c.unprovenAt[key]; un {
			return "unproven"
		}
		return "no entry"
	case q.Kind == KindFind && !e.complete && q.Skip+q.Limit > len(e.docs):
		if c.filling[key] {
			return "filling"
		}
		return "short"
	case !c.entryFresh(e, q, now):
		return "entry behind"
	}
	return "hit"
}

// fillFailure is why an entry's last fill failed, and when.
type fillFailure struct {
	why string
	at  time.Time
}

func (c *Cache) failed(q Query, why string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.failedAt) >= maxUnproven {
		clear(c.failedAt)
	}
	c.failedAt[q.entryKey()] = fillFailure{why: why, at: time.Now()}
}

// FailedSince reports why q's entry's last fill failed, when it failed after
// since.
func (c *Cache) FailedSince(q Query, since time.Time) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f, ok := c.failedAt[q.entryKey()]
	if !ok || f.at.Before(since) {
		return "", false
	}
	return f.why, true
}

// Filling reports whether a fill for q's entry is in flight.
func (c *Cache) Filling(q Query) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.filling[q.entryKey()]
}

// install puts a filled entry in place. Called by the applier only.
func (c *Cache) install(q Query, docs []bson.Raw, complete bool, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.installLocked(q, docs, complete, n) != nil {
		c.evict()
	}
}

// installLocked is install with mu held, returning the entry, or nil when it
// was not installed. It does not evict, so a caller can bring the entry up to
// date before anything is chosen to go.
func (c *Cache) installLocked(q Query, docs []bson.Raw, complete bool, n int64) *entry {
	key := q.entryKey()
	delete(c.filling, key)
	delete(c.failedAt, key)
	if c.refusedNS[q.NS] || c.refused[q.shapeID()] {
		return nil
	}
	sh := c.shapeFor(q)
	if old, ok := c.entries[key]; ok {
		c.drop(old)
	}
	e := &entry{key: key, shape: sh, complete: complete, n: n, group: q.groupKey(), bounds: q.Ranges}
	if c.isTiered(q.NS) {
		e.expires = c.now().Add(tieredTTL)
	}
	if sh.kind == KindGroup {
		e.grp = newGroupState(sh.group)
	}
	for _, d := range docs {
		if e.grp != nil {
			id, ok := docKey(d)
			if !ok || !c.plainMatch(sh, q, d) {
				return nil
			}
			if !e.grp.add(id, d) {
				c.refuse(sh)
				return nil
			}
			continue
		}
		d = trimDoc(sh, d)
		id, ok := docKey(d)
		if !ok {
			return nil
		}
		if !c.fits(sh, e, d) || !c.plainMatch(sh, q, d) {
			return nil
		}
		e.docs = append(e.docs, doc{id: id, raw: d})
		e.bytes += len(d)
	}
	if e.grp != nil {
		e.bytes += e.grp.bytes()
	}
	e.bytes += len(key) + 64
	c.entries[key] = e
	if e.grp != nil {
		for id := range e.grp.byDoc {
			c.addRev(id, e)
		}
	}
	if len(sh.rng) > 0 {
		g := c.groups[e.group]
		if g == nil {
			g = map[*entry]struct{}{}
			c.groups[e.group] = g
		}
		g[e] = struct{}{}
	}
	for _, d := range e.docs {
		c.addRev(d.id, e)
	}
	e.el = c.lru.PushFront(e)
	c.bytes += e.bytes
	c.Fills.Add(1)
	return e
}

func (c *Cache) shapeFor(q Query) *shape {
	id := q.shapeID()
	m := c.shapes[q.NS]
	if m == nil {
		m = map[string]*shape{}
		c.shapes[q.NS] = m
	}
	if s, ok := m[id]; ok {
		return s
	}
	s := &shape{id: id, kind: q.Kind, ns: q.NS, sort: q.Sort, proj: q.Proj, group: q.Group}
	if s.kind == KindCountAgg {
		s.kind = KindCount
	}
	for _, p := range q.Eq {
		s.fields = append(s.fields, p.Field)
	}
	for _, r := range q.Ranges {
		s.rng = append(s.rng, RangePred{Field: r.Field, Op: r.Op})
	}
	m[id] = s
	return s
}

// fits checks a document against what a shape needs to answer exactly: a
// sortable value for each sort field, of the same bracket as the entry already
// holds. A document that does not fit refuses the shape, and everything held
// under it goes: an order across types, or for a missing field, is one this
// does not reproduce.
func (c *Cache) fits(sh *shape, e *entry, d bson.Raw) bool {
	for _, s := range sh.sort {
		v, err := d.LookupErr(s.Field)
		if err != nil || bracket(v) == "" {
			c.refuse(sh)
			return false
		}
		if len(e.docs) > 0 && bracket(e.docs[0].raw.Lookup(s.Field)) != bracket(v) {
			c.refuse(sh)
			return false
		}
	}
	return true
}

// plainMatch checks that a document mongod returned for a filter holds the
// filtered value itself. One that matched through an array element, which is
// how {tag: "a"} matches {tag: ["a", "b"]}, refuses the shape.
func (c *Cache) plainMatch(sh *shape, q Query, d bson.Raw) bool {
	for _, p := range q.Eq {
		v, err := d.LookupErr(p.Field)
		if err != nil {
			c.refuse(sh)
			return false
		}
		if k, ok := valueKey(v); !ok || k != p.key {
			c.refuse(sh)
			return false
		}
	}
	for _, r := range q.Ranges {
		v, err := d.LookupErr(r.Field)
		if err != nil {
			c.refuse(sh)
			return false
		}
		if _, ok := valueKey(v); !ok || !r.inRange(v) {
			c.refuse(sh)
			return false
		}
	}
	return true
}

// rangeValues are a document's values for a shape's range fields, in the
// order of its bounds, with Type 0 for a missing field, which mongod's range
// never matches. ok false means a value this cannot judge, an array above all,
// and the shape is refused, as keyOf refuses it for an equality. Read once per
// change and shape, not once per entry: see compareNumbers.
func (c *Cache) rangeValues(sh *shape, d bson.Raw) ([]bson.RawValue, bool) {
	out := make([]bson.RawValue, len(sh.rng))
	for i, r := range sh.rng {
		v, err := d.LookupErr(r.Field)
		if err != nil {
			continue
		}
		if bracket(v) == "" {
			c.refuse(sh)
			return nil, false
		}
		out[i] = v
	}
	return out, true
}

// inBounds says whether values, from rangeValues, satisfy an entry's range.
func inBounds(bounds []RangePred, values []bson.RawValue) bool {
	for i, r := range bounds {
		if values[i].Type == 0 || !r.inRange(values[i]) {
			return false
		}
	}
	return true
}

// candidates are the entries a document whose values give key may belong to
// under a shape. Called with mu held.
func (c *Cache) candidates(sh *shape, key string) []*entry {
	if len(sh.rng) == 0 {
		if e, ok := c.entries[key]; ok {
			return []*entry{e}
		}
		return nil
	}
	g := c.groups[key]
	out := make([]*entry, 0, len(g))
	for e := range g {
		out = append(out, e)
	}
	return out
}

func (c *Cache) refuse(sh *shape) {
	if c.refused[sh.id] {
		return
	}
	c.refused[sh.id] = true
	c.Refusals.Add(1)
	for _, e := range c.entries {
		if e.shape == sh {
			c.drop(e)
		}
	}
}

// RefuseNS stops answering a namespace at all, as for one with a collation.
func (c *Cache) RefuseNS(ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refusedNS[ns] = true
	c.dropNS(ns)
}

func (c *Cache) addRev(id string, e *entry) {
	m := c.rev[id]
	if m == nil {
		m = map[*entry]struct{}{}
		c.rev[id] = m
	}
	m[e] = struct{}{}
}

func (c *Cache) delRev(id string, e *entry) {
	if m := c.rev[id]; m != nil {
		delete(m, e)
		if len(m) == 0 {
			delete(c.rev, id)
		}
	}
}

func (c *Cache) drop(e *entry) {
	if _, ok := c.entries[e.key]; !ok {
		return
	}
	delete(c.entries, e.key)
	if g := c.groups[e.group]; g != nil {
		delete(g, e)
		if len(g) == 0 {
			delete(c.groups, e.group)
		}
	}
	for _, d := range e.docs {
		c.delRev(d.id, e)
	}
	if e.grp != nil {
		for id := range e.grp.byDoc {
			c.delRev(id, e)
		}
	}
	if e.el != nil {
		c.lru.Remove(e.el)
	}
	c.bytes -= e.bytes
	c.Dropped.Add(1)
}

func (c *Cache) dropNS(ns string) {
	for _, e := range c.entries {
		if e.shape.ns == ns {
			c.drop(e)
		}
	}
}

func (c *Cache) dropCounts(ns string) {
	for _, e := range c.entries {
		if e.shape.ns == ns && e.shape.kind == KindCount {
			c.drop(e)
		}
	}
}

func (c *Cache) evict() {
	for c.bytes > c.maxBytes && c.lru.Len() > 0 {
		c.drop(c.lru.Back().Value.(*entry))
	}
}

// Change is one document's new state, from the change stream.
type Change struct {
	NS string
	// Op is insert, update, replace or delete.
	Op string
	// ID is the document's _id, and Doc its full new state, nil for a delete
	// or for an update whose document no longer exists.
	ID  bson.RawValue
	Doc bson.Raw
	// Updated names the fields an update set or removed.
	Updated []string
	At      bson.Timestamp
}

// Apply brings every entry of the change's namespace up to date with it.
//
// It sets the document's state rather than applying a difference, which is
// what lets a fill race the stream: an entry filled from a read that already
// saw a change, and then handed that change again, ends up the same. A
// document is taken out of every list it was in and put back where its new
// values place it, so a price change moves it within a listing and a category
// change moves it to another.
func (c *Cache) Apply(ch Change) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if after(ch.At, c.applied) {
		// A later time: every event at the one before has been applied.
		if after(c.applied, c.closed) {
			c.closed = c.applied
		}
		c.applied = ch.At
	}
	shapes := c.shapes[ch.NS]
	if len(shapes) == 0 {
		return
	}
	id, ok := idKey(ch.ID)
	if !ok {
		c.dropNS(ch.NS)
		return
	}
	// Out of every list it was in. On a tiered namespace a document that left
	// may only have been evicted, so the lists holding it go. See Tiers.
	tiered := c.isTiered(ch.NS)
	// A list with no sort, or one whose sort can tie, is in an order mongod's
	// plan decides: record id order under a collection scan, index order
	// under an index scan. Taken out and put back, an updated document went
	// after its equals, so find({status: "new"}).limit(10) returned documents
	// 2 to 11 after an update to the first, where mongod still returns 1 to
	// 10; kept where it stood, it stayed first where an index on {status,
	// priority} had moved it. Neither guess is mongod's, so such a list goes
	// when a member changes and is read again. A list sorted to the end, by
	// _id last, has the one order mongod gives it, and a member whose sort
	// fields did not change is replaced where it stands.
	inPlace := map[*entry]bool{}
	for e := range c.rev[id] {
		if tiered && ch.Doc == nil {
			c.drop(e)
			continue
		}
		if ch.Doc != nil && planOrdered(e, id, ch.Doc) {
			c.drop(e)
			continue
		}
		if c.replaceInPlace(e, id, ch.Doc) {
			inPlace[e] = true
			continue
		}
		c.removeDoc(e, id)
	}
	// Counts cannot be taken down without knowing what the document held
	// before, which a delete or a replace does not say. An update that did
	// not touch a counted field leaves every count as it was.
	switch ch.Op {
	case "delete", "replace":
		c.dropCounts(ch.NS)
	case "update":
		for _, sh := range shapes {
			if sh.kind == KindCount && touches(ch.Updated, filterFields(sh)) {
				for _, e := range c.entries {
					if e.shape == sh {
						c.drop(e)
					}
				}
			}
		}
	}
	if ch.Doc == nil {
		return
	}
	for _, sh := range shapes {
		key, ok := c.keyOf(sh, ch.Doc)
		if !ok {
			continue
		}
		values, ok := c.rangeValues(sh, ch.Doc)
		if !ok {
			continue
		}
		for _, e := range c.candidates(sh, key) {
			if _, live := c.entries[e.key]; !live {
				// Refused, and so dropped, by an earlier candidate.
				break
			}
			if inPlace[e] {
				continue
			}
			if !inBounds(e.bounds, values) {
				continue
			}
			if sh.kind == KindCount {
				// Not an insert the read already counted. A fill reads at the
				// stream's position, and when that falls among the events of
				// one commit the read holds all of them while the stream has
				// applied only some: each one after that was counted twice,
				// and a count read after an insertMany came back too high.
				if ch.Op == "insert" && after(ch.At, e.from) {
					e.n++
				}
				continue
			}
			if sh.kind == KindGroup {
				if !c.addToGroup(sh, e, id, ch.Doc) {
					break
				}
				continue
			}
			if !c.fits(sh, e, ch.Doc) {
				break
			}
			// Where a new member goes in an order mongod's plan decides is
			// mongod's to say. See planOrdered.
			if planOrdered(e, id, ch.Doc) {
				c.drop(e)
				continue
			}
			c.insertDoc(e, doc{id: id, raw: trimDoc(sh, ch.Doc)})
		}
	}
	c.evict()
}

func touches(updated, fields []string) bool {
	for _, u := range updated {
		top := u
		for i := 0; i < len(u); i++ {
			if u[i] == '.' {
				top = u[:i]
				break
			}
		}
		for _, f := range fields {
			if f == top {
				return true
			}
		}
	}
	return false
}

// keyOf is the entry a document belongs to under a shape, if it has a single
// value this can compare in every filtered field. A field holding an array, a
// document or null refuses the shape: MongoDB's equality would match it in ways
// this does not reproduce. A missing field just means it is in no entry.
func (c *Cache) keyOf(sh *shape, d bson.Raw) (string, bool) {
	keys := make([]string, 0, len(sh.fields))
	for _, f := range sh.fields {
		v, err := d.LookupErr(f)
		if err != nil {
			return "", false
		}
		k, ok := valueKey(v)
		if !ok {
			c.refuse(sh)
			return "", false
		}
		keys = append(keys, k)
	}
	b := []byte(sh.id)
	for _, k := range keys {
		b = append(b, 0)
		b = append(b, k...)
	}
	return string(b), true
}

// keyOfQuiet is keyOf without refusing the shape: for a write being noted,
// whose document need not be one the cache would ever hold.
// fieldsPresent reports whether a document carries every field a shape
// filters by for equality, whatever their values.
func (c *Cache) fieldsPresent(sh *shape, d bson.Raw) bool {
	for _, f := range sh.fields {
		if _, err := d.LookupErr(f); err != nil {
			return false
		}
	}
	return true
}

func (c *Cache) keyOfQuiet(sh *shape, d bson.Raw) (string, bool) {
	keys := make([]string, 0, len(sh.fields))
	for _, f := range sh.fields {
		v, err := d.LookupErr(f)
		if err != nil {
			return "", false
		}
		k, ok := valueKey(v)
		if !ok {
			return "", false
		}
		keys = append(keys, k)
	}
	b := []byte(sh.id)
	for _, k := range keys {
		b = append(b, 0)
		b = append(b, k...)
	}
	return string(b), true
}

// planOrdered reports whether where d goes in a listing is left to mongod's
// plan rather than decided by the sort: the listing has no sort, or d's sort
// fields equal another member's. Ties are ordered by record id under a
// collection scan and by the index's other keys under an index scan, and this
// cannot tell which mongod chose. A sort ending in _id cannot tie.
func planOrdered(e *entry, id string, d bson.Raw) bool {
	sh := e.shape
	if sh.kind != KindFind {
		return false
	}
	if len(sh.sort) == 0 {
		return true
	}
	if sh.sort[len(sh.sort)-1].Field == "_id" {
		return false
	}
	for _, x := range e.docs {
		if x.id == id {
			continue
		}
		same := true
		for _, s := range sh.sort {
			if !sameSortValue(x.raw, d, s.Field) {
				same = false
				break
			}
		}
		if same {
			return true
		}
	}
	return false
}

// sameSortValue reports whether two documents tie on a sort field as mongod
// compares it: by value, so 1 and 1.0 tie although their bytes differ. A value
// this cannot key, an array or a document, is read as a tie, which only costs
// a refill.
func sameSortValue(a, b bson.Raw, field string) bool {
	va, ea := a.LookupErr(field)
	vb, eb := b.LookupErr(field)
	if ea != nil || eb != nil {
		return (ea != nil) == (eb != nil)
	}
	ka, oka := valueKey(va)
	kb, okb := valueKey(vb)
	if !oka || !okb {
		return true
	}
	return ka == kb
}

// replaceInPlace puts a document's new version where its old one stood in a
// list, when it still belongs to that list and nothing it is sorted by has
// changed. It reports whether it did. See Apply.
func (c *Cache) replaceInPlace(e *entry, id string, d bson.Raw) bool {
	sh := e.shape
	if d == nil || sh.kind != KindFind || e.grp != nil {
		return false
	}
	at := -1
	for i, x := range e.docs {
		if x.id == id {
			at = i
			break
		}
	}
	if at < 0 {
		return false
	}
	key, ok := c.keyOf(sh, d)
	if !ok || key != e.group {
		return false
	}
	values, ok := c.rangeValues(sh, d)
	if !ok || !inBounds(e.bounds, values) || !c.fits(sh, e, d) {
		return false
	}
	for _, s := range sh.sort {
		if !bytes.Equal(rawField(e.docs[at].raw, s.Field), rawField(d, s.Field)) {
			return false
		}
	}
	nd := trimDoc(sh, d)
	e.bytes += len(nd) - len(e.docs[at].raw)
	c.bytes += len(nd) - len(e.docs[at].raw)
	e.docs[at] = doc{id: id, raw: nd}
	return true
}

// rawField is a field's type and bytes, for telling whether a value changed.
func rawField(d bson.Raw, f string) []byte {
	v, err := d.LookupErr(f)
	if err != nil {
		return nil
	}
	return append([]byte{byte(v.Type)}, v.Value...)
}

func (c *Cache) removeDoc(e *entry, id string) {
	if e.grp != nil {
		e.grp.remove(id)
		c.regrouped(e)
	}
	for i, d := range e.docs {
		if d.id == id {
			e.docs = append(e.docs[:i], e.docs[i+1:]...)
			e.bytes -= len(d.raw)
			c.bytes -= len(d.raw)
			break
		}
	}
	c.delRev(id, e)
}

// regrouped brings a grouped entry's size up to date after a change.
func (c *Cache) regrouped(e *entry) {
	n := len(e.key) + 64 + e.grp.bytes()
	c.bytes += n - e.bytes
	e.bytes = n
}

// addToGroup puts a document's part into a grouped entry, or refuses the
// shape when it holds a value this does not reproduce mongod's handling of.
// It reports whether the entry is still there.
func (c *Cache) addToGroup(sh *shape, e *entry, id string, d bson.Raw) bool {
	if !e.grp.add(id, d) {
		c.refuse(sh)
		return false
	}
	if _, has := e.grp.byDoc[id]; has {
		c.addRev(id, e)
	}
	c.regrouped(e)
	return true
}

// insertDoc places a document in its sorted position. Past the end of an entry
// that is not the whole list it is left out, because a document further down
// may belong in front of it.
func (c *Cache) insertDoc(e *entry, d doc) {
	at := sort.Search(len(e.docs), func(i int) bool { return c.less(e.shape, d.raw, e.docs[i].raw) })
	if at == len(e.docs) && !e.complete {
		return
	}
	e.docs = append(e.docs, doc{})
	copy(e.docs[at+1:], e.docs[at:])
	e.docs[at] = d
	e.bytes += len(d.raw)
	c.bytes += len(d.raw)
	c.addRev(d.id, e)
	// Bounded: an entry that grew past the window gives up its tail and with
	// it the claim to be the whole list.
	if len(e.docs) > maxWindow+fillWindow {
		for _, t := range e.docs[maxWindow:] {
			e.bytes -= len(t.raw)
			c.bytes -= len(t.raw)
			c.delRev(t.id, e)
		}
		e.docs = e.docs[:maxWindow]
		e.complete = false
	}
}

// less orders two documents by the shape's sort. Ties are equal: MongoDB does
// not promise an order among them either.
func (c *Cache) less(sh *shape, a, b bson.Raw) bool {
	for _, s := range sh.sort {
		x, y := a.Lookup(s.Field), b.Lookup(s.Field)
		if bracket(x) != bracket(y) {
			return false
		}
		n := compareSame(x, y)
		if n == 0 {
			continue
		}
		if s.Desc {
			return n > 0
		}
		return n < 0
	}
	return false
}

// DropNS forgets a namespace, for a drop or rename seen on the stream.
func (c *Cache) DropNS(ns string) {
	c.mu.Lock()
	c.dropNS(ns)
	c.mu.Unlock()
	c.forgetPlain(ns)
}

// forgetPlain drops what plain learned about a namespace, or about every
// namespace in a database when ns is one ending in a dot. A collection dropped
// and created again may come back with a collation, or as a view.
func (c *Cache) forgetPlain(ns string) {
	c.plainMu.Lock()
	defer c.plainMu.Unlock()
	c.plainGen++
	if strings.HasSuffix(ns, ".") {
		for k := range c.plainNS {
			if strings.HasPrefix(k, ns) {
				delete(c.plainNS, k)
			}
		}
		return
	}
	delete(c.plainNS, ns)
}

// Reset forgets everything, for a change stream that had to start again and
// cannot say what it missed.
func (c *Cache) Reset() {
	c.mu.Lock()
	for _, e := range c.entries {
		c.drop(e)
	}
	c.applied, c.closed = bson.Timestamp{}, bson.Timestamp{}
	c.mu.Unlock()
	// What it missed may have been a drop and a create.
	c.plainMu.Lock()
	c.plainNS = map[string]bool{}
	c.plainGen++
	c.plainMu.Unlock()
}

// Advance records how far the stream has read when it has no event to give,
// from its resume token.
func (c *Cache) Advance(at bson.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if after(at, c.applied) {
		c.applied = at
	}
	// The end of a batch, or a poll with nothing in it: the server sent all it
	// had up to here. A batch cut at 16MB inside one commit is the case this
	// cannot see, and it is the size of one batch.
	if after(at, c.closed) {
		c.closed = at
	}
	if c.sweeps++; c.sweeps >= 1024 {
		c.sweeps = 0
		c.sweep()
	}
}

// Stats is what the cache holds.
type Stats struct {
	Entries, Bytes                                           int
	Hits, Misses, Behind, Fills, Dropped, Refusals, Unproven int64
}

func (c *Cache) Stats() Stats {
	c.mu.Lock()
	s := Stats{Entries: len(c.entries), Bytes: c.bytes}
	c.mu.Unlock()
	s.Hits, s.Misses, s.Behind = c.Hits.Load(), c.Misses.Load(), c.Behind.Load()
	s.Fills, s.Dropped, s.Refusals = c.Fills.Load(), c.Dropped.Load(), c.Refusals.Load()
	s.Unproven = c.Unproven.Load()
	return s
}

// docKey is a document's identity across entries.
func docKey(d bson.Raw) (string, bool) {
	v, err := d.LookupErr("_id")
	if err != nil {
		return "", false
	}
	return idKey(v)
}

func idKey(v bson.RawValue) (string, bool) {
	k, ok := valueKey(v)
	if ok {
		return k, true
	}
	if v.Type == bson.TypeEmbeddedDocument || v.Type == bson.TypeBinary {
		return "x" + string(v.Type) + string(v.Value), true
	}
	return "", false
}
