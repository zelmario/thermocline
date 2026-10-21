package livecache

import (
	"container/list"
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

	// applied is how far the change stream has been applied, and lastWrite is
	// the newest write this router acknowledged per namespace. A namespace is
	// served only while applied has reached its last write. See Fresh.
	applied   bson.Timestamp
	lastWrite map[string]bson.Timestamp
	// lastWriteDB is the same for writes naming a whole database.
	lastWriteDB map[string]bson.Timestamp

	// readers are the users mongod has let read each namespace lately. See
	// Allowed.
	readers map[string]map[string]time.Time

	fillq   chan Query
	pending map[string]bool
	// plainNS says, per namespace, that it has no default collation and is not
	// a view. Read and written by the applier only.
	plainNS map[string]bool

	Hits, Misses, Behind, Fills, Dropped, Refusals atomic.Int64
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
		maxBytes:    maxBytes,
		lru:         list.New(),
		entries:     map[string]*entry{},
		shapes:      map[string]map[string]*shape{},
		rev:         map[string]map[*entry]struct{}{},
		refused:     map[string]bool{},
		refusedNS:   map[string]bool{},
		lastWrite:   map[string]bson.Timestamp{},
		lastWriteDB: map[string]bson.Timestamp{},
		readers:     map[string]map[string]time.Time{},
		fillq:       make(chan Query, 1024),
		pending:     map[string]bool{},
		plainNS:     map[string]bool{},
	}
}

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

// NoteWrite records that this router acknowledged a write to ns at or before
// at. Until the change stream is applied that far, ns is answered by mongod, so
// a client always reads what it has just written.
func (c *Cache) NoteWrite(ns string, at bson.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if after(at, c.lastWrite[ns]) {
		c.lastWrite[ns] = at
	}
}

// NoteWriteDB is NoteWrite for a write whose namespace is a whole database.
func (c *Cache) NoteWriteDB(db string, at bson.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if after(at, c.lastWriteDB[db]) {
		c.lastWriteDB[db] = at
	}
}

func after(a, b bson.Timestamp) bool {
	return a.T > b.T || (a.T == b.T && a.I > b.I)
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
	if after(c.lastWrite[q.NS], c.applied) || after(c.lastWriteDB[db], c.applied) {
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
	if c.pending[key] {
		c.mu.Unlock()
		return
	}
	c.pending[key] = true
	c.mu.Unlock()
	// Its values point into the client's message, which is reused once the
	// reply is sent, and the fill runs after that.
	q = q.detached()
	select {
	case c.fillq <- q:
	default:
		c.mu.Lock()
		delete(c.pending, key)
		c.mu.Unlock()
	}
}

// install puts a filled entry in place. Called by the applier only.
func (c *Cache) install(q Query, docs []bson.Raw, complete bool, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := q.entryKey()
	delete(c.pending, key)
	if c.refusedNS[q.NS] || c.refused[q.shapeID()] {
		return
	}
	sh := c.shapeFor(q)
	if old, ok := c.entries[key]; ok {
		c.drop(old)
	}
	e := &entry{key: key, shape: sh, complete: complete, n: n}
	for _, d := range docs {
		id, ok := docKey(d)
		if !ok {
			return
		}
		if !c.fits(sh, e, d) || !c.plainMatch(sh, q, d) {
			return
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
	c.evict()
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
	// Out of every list it was in.
	for e := range c.rev[id] {
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
}

// Stats is what the cache holds.
type Stats struct {
	Entries, Bytes                                 int
	Hits, Misses, Behind, Fills, Dropped, Refusals int64
}

func (c *Cache) Stats() Stats {
	c.mu.Lock()
	s := Stats{Entries: len(c.entries), Bytes: c.bytes}
	c.mu.Unlock()
	s.Hits, s.Misses, s.Behind = c.Hits.Load(), c.Misses.Load(), c.Behind.Load()
	s.Fills, s.Dropped, s.Refusals = c.Fills.Load(), c.Dropped.Load(), c.Refusals.Load()
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
