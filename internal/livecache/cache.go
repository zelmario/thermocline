package livecache

import (
	"container/list"
	"context"
	"sort"
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

	// readers are the users mongod has let read each namespace lately. See
	// Allowed.
	readers map[string]map[string]time.Time

	fillq   chan Query
	filling map[string]bool
	// unprovenAt are tiered entries a fill could not prove, by when, so the
	// same miss does not cost a second read of the fast tier every time.
	unprovenAt map[string]time.Time
	// plainNS says, per namespace, that it has no default collation and is not
	// a view. Read and written by the fill workers, under plainMu.
	plainMu sync.Mutex
	plainNS map[string]bool

	// tiered says a namespace is tiered, and prove how much of the fast tier's
	// answer for one is the whole answer's. See Tiers.
	tiered func(ns string) bool
	prove  ProveFunc

	Hits, Misses, Behind, Fills, Dropped, Refusals, Unproven atomic.Int64
}

// ProveFunc reports how many of docs, the head of the fast tier's answer to a
// query on a tiered namespace sorted by sort, are certainly the head of the
// answer over both tiers. Zero proves nothing.
type ProveFunc func(ctx context.Context, ns string, sort []SortKey, docs []bson.Raw) int

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
func (c *Cache) Tiers(tiered func(ns string) bool, prove ProveFunc) {
	c.tiered, c.prove = tiered, prove
}

func (c *Cache) isTiered(ns string) bool {
	return c.tiered != nil && c.tiered(ns)
}

type shape struct {
	id     string
	kind   Kind
	ns     string
	fields []string
	sort   []SortKey
}

type entry struct {
	key   string
	shape *shape
	// docs is the sorted head of the list, complete says it is the whole of
	// it. For a count, n is the count.
	docs     []doc
	complete bool
	n        int64
	bytes    int
	el       *list.Element
}

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
			if touches(n.Fields, sh.fields) || touches(n.Fields, sortFields(sh)) {
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
				if key, ok := c.keyOfQuiet(sh, d); ok {
					c.pendingEntry[key] = c.noteWrite(c.pendingEntry[key], at)
				}
			}
		}
	}
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
		if after(w.ts, p0) && after(w.ts, c.applied) {
			c.pendingEntry[e.key] = append(c.pendingEntry[e.key], w)
		}
	}
}

// sweep forgets queues the stream has passed that no Get has looked at since.
// Called with mu held, every so often.
func (c *Cache) sweep() {
	for _, m := range []map[string][]pendingWrite{c.pendingEntry, c.pendingDoc, c.pendingCount, c.classified} {
		for k, q := range m {
			stale := true
			for _, w := range q {
				if after(w.ts, c.applied) {
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
	if !after(ts, c.applied) {
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
	for i < len(q) && !after(q[i].ts, c.applied) {
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
	if e.shape.kind == KindCount {
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
	db, _ := splitNS(q.NS)
	now := c.now()
	if !c.fresh(c.pending, q.NS, now) || !c.fresh(c.pendingDB, db, now) || !c.fresh(c.pendingDB, allKey, now) {
		c.mu.Unlock()
		c.Behind.Add(1)
		return nil, false
	}
	e, ok := c.entries[key]
	if ok && q.Kind == KindFind && !e.complete && q.Skip+q.Limit > len(e.docs) {
		ok = false
	}
	if !ok {
		c.mu.Unlock()
		c.Misses.Add(1)
		c.requestFill(q)
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
	if c.refusedNS[q.NS] || c.refused[q.shapeID()] {
		return nil
	}
	sh := c.shapeFor(q)
	if old, ok := c.entries[key]; ok {
		c.drop(old)
	}
	e := &entry{key: key, shape: sh, complete: complete, n: n}
	for _, d := range docs {
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
	e.bytes += len(key) + 64
	c.entries[key] = e
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
	s := &shape{id: id, kind: q.Kind, ns: q.NS, sort: q.Sort}
	if s.kind == KindCountAgg {
		s.kind = KindCount
	}
	for _, p := range q.Eq {
		s.fields = append(s.fields, p.Field)
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
	return true
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
	for _, d := range e.docs {
		c.delRev(d.id, e)
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
	for e := range c.rev[id] {
		if tiered && ch.Doc == nil {
			c.drop(e)
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
			if sh.kind == KindCount && touches(ch.Updated, sh.fields) {
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
		e, have := c.entries[key]
		if !have {
			continue
		}
		if sh.kind == KindCount {
			if ch.Op == "insert" {
				e.n++
			}
			continue
		}
		if !c.fits(sh, e, ch.Doc) {
			continue
		}
		c.insertDoc(e, doc{id: id, raw: ch.Doc})
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

func (c *Cache) removeDoc(e *entry, id string) {
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
	defer c.mu.Unlock()
	c.dropNS(ns)
}

// Reset forgets everything, for a change stream that had to start again and
// cannot say what it missed.
func (c *Cache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.entries {
		c.drop(e)
	}
	c.applied = bson.Timestamp{}
}

// Advance records how far the stream has read when it has no event to give,
// from its resume token.
func (c *Cache) Advance(at bson.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if after(at, c.applied) {
		c.applied = at
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
