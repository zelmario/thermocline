// Package cache holds query results for as long as they have been observed to
// stay the same.
//
// The idea is Zelmar's: nothing is configured by hand. The router already sees
// every query and every answer, so it can watch how long each query's result
// stays stable and cache it for a little less than that. A count that has
// returned the same number for five minutes is cached for four and a half; one
// that changes every second is never cached at all, because its observed
// stability never grows.
//
// Two things make this safe enough to be worth doing here, and both come from
// where the cache sits. The router sees the writes as well as the reads, so an
// entry is dropped the moment a write touches its collection rather than
// waiting out a timer. And the expensive read is the one that falls through to
// the archive, which is exactly the read a stable result is most likely to be.
//
// What it cannot see is a writer that does not go through it. That is the whole
// of the staleness this can produce, and the learned TTL is its bound: an entry
// is only ever served for less time than that same query's result has already
// been observed to hold still. Nothing here is on by default.
package cache

import (
	"hash/fnv"
	"sync"
	"time"
)

// Stability is how many recent stable periods are remembered per query. Short,
// because a query whose behaviour changed a while ago should be judged on what
// it does now.
const Stability = 5

// safety is how much of the observed stability is actually used. Ten per cent
// off, so a result that has held for five minutes is cached for four and a
// half: the cache is deliberately shorter-lived than the evidence for it.
const safety = 0.9

// MinTTL is the shortest life worth storing an answer for.
//
// The TTL is derived from how long the answer has been seen to hold, and the
// second sighting of a query has only the gap between the two reads behind it.
// Two connections of a pool asking the same question at the same moment make
// that gap a few hundred nanoseconds, and the entry was stored anyway: the
// whole reply copied, the byte accounting moved, and evictForBytes then walking
// every key in the cache, for an answer that expires before anything can read
// it. Serving a hit is 365us measured, so a TTL below a millisecond cannot be
// used even by the request immediately behind.
//
// Nothing is lost by the floor. What was learned about the query is kept either
// way; only the copy is skipped. A query whose answer genuinely holds for less
// than a millisecond is not one a cache can help.
const MinTTL = time.Millisecond

// Entry is one cached answer.
type Entry struct {
	Reply    []byte
	StoredAt time.Time
	TTL      time.Duration
}

// history is what has been learned about one query.
type history struct {
	lastHash  uint64
	lastSeen  time.Time
	changedAt time.Time
	stable    []time.Duration
	ttl       time.Duration

	// nss is every namespace this key is indexed under in byNS.
	//
	// Kept here rather than worked out again, because the two places that stop
	// watching a query have to unhook it from all of them and neither one is
	// given the list. Invalidate is handed one namespace and eviction is handed
	// none, so without this the first cleaned up nothing but the namespace it
	// was called with and the second swept every namespace there is.
	nss []string

	entry *Entry
}

// MaxEntryBytes is the largest answer worth holding.
//
// A count is a few bytes and a find can be megabytes, and the second is both
// the less likely to repeat and the more expensive to be wrong about. One
// oversized reply would also blow the whole budget on a single query.
const MaxEntryBytes = 256 * 1024

// DefaultMaxBytes is how much memory the answers may occupy in total. Counting
// entries is not a bound: a thousand replies of a megabyte each is a gigabyte,
// and nothing in a count says which of those it is.
const DefaultMaxBytes = 64 * 1024 * 1024

// Cache is a bounded set of learned queries and their most recent answers.
type Cache struct {
	mu       sync.Mutex
	byKey    map[string]*history
	byNS     map[string]map[string]struct{}
	max      int
	maxBytes int
	bytes    int
	maxTTL   time.Duration
	now      func() time.Time
	Hits     uint64
	Misses   uint64
	Learned  uint64
	// Rejected counts answers too large to hold, which is worth knowing: a
	// cache that looks idle may simply be being handed things it will not keep.
	Rejected uint64
}

// New returns a cache holding at most max queries and maxBytes of answers, with
// no entry ever living longer than maxTTL however stable it has looked.
func New(max int, maxTTL time.Duration) *Cache {
	return NewWithBytes(max, DefaultMaxBytes, maxTTL)
}

// NewWithBytes is New with the memory budget stated.
func NewWithBytes(max, maxBytes int, maxTTL time.Duration) *Cache {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	return &Cache{
		byKey:    make(map[string]*history),
		byNS:     make(map[string]map[string]struct{}),
		max:      max,
		maxBytes: maxBytes,
		maxTTL:   maxTTL,
		now:      time.Now,
	}
}

// Bytes reports how much the held answers occupy.
func (c *Cache) Bytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes
}

// Get returns a cached reply when one is still within its learned TTL.
func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.byKey[key]
	if !ok || h.entry == nil {
		c.Misses++
		return nil, false
	}
	if c.now().Sub(h.entry.StoredAt) >= h.entry.TTL {
		c.Misses++
		return nil, false
	}
	c.Hits++
	out := make([]byte, len(h.entry.Reply))
	copy(out, h.entry.Reply)
	return out, true
}

// Observe records what a query answered, and stores it when its own history
// says the answer holds still for long enough to be worth keeping.
//
// The reply is only kept when there is evidence: a query seen once has no
// observed stability and is not cached, however cheap that would have been.
func (c *Cache) Observe(key, ns string, reply []byte) {
	c.ObserveResult(key, ns, reply, hashOf(reply))
}

// ObserveResult is Observe with the answer's hash supplied, because the caller
// has already worked out which part of the reply is the answer and which part
// is the envelope it arrived in. See ResultHash.
func (c *Cache) ObserveResult(key, ns string, reply []byte, h uint64) {
	c.ObserveResultOn(key, []string{ns}, reply, h)
}

// ObserveResultOn is ObserveResult for an answer that depends on more than one
// collection.
//
// An entry is dropped when any collection it was hung from changes, and until
// this it could only be hung from one: the namespace the command named. A read
// of a view names the view, and every invalidation arrives under the collection
// the view reads, so the two never met and a cached read of a view outlived
// every write to its own data. The same is true of a $lookup, whose joined
// collection the command names nowhere the invalidation can see.
//
// Registering an entry under several namespaces is enough because Invalidate
// deletes the entry itself and not merely the reference: a leftover reference
// under another namespace finds nothing and does nothing.
func (c *Cache) ObserveResultOn(key string, nss []string, reply []byte, h uint64) {
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.byKey[key]
	if !ok {
		c.evictIfFull()
		e = &history{lastHash: h, lastSeen: now, changedAt: now}
		c.byKey[key] = e
		c.indexOn(key, e, nss)
		return
	}
	c.indexOn(key, e, nss)

	if h != e.lastHash {
		// How long that answer was being given: from the moment it first
		// appeared to the moment something else did. Measured the same way the
		// advisor measures it, so the TTL used is the TTL reported.
		//
		// The change happened somewhere between the previous look and this one,
		// so this is an upper bound on the truth. The ten per cent haircut is
		// the margin for it, the ceiling bounds it absolutely, and a write
		// through this router bypasses both by dropping the entry outright.
		held := now.Sub(e.changedAt)
		e.stable = append(e.stable, held)
		if len(e.stable) > Stability {
			e.stable = e.stable[len(e.stable)-Stability:]
		}
		c.Learned++
		e.lastHash = h
		e.changedAt = now
		e.entry = nil
	}
	e.lastSeen = now

	// Two kinds of evidence, and which one applies depends on whether this
	// query has ever been seen to change.
	//
	// Once it has, the completed periods are the estimate and they apply to the
	// answer in hand from the moment it arrives: "this changes about every five
	// minutes" is a statement about the query, not about one of its answers.
	// That is the worked example this was built from, and without it every
	// change would cost the next readers a miss.
	//
	// Before that, all there is to go on is how long the answer in hand has
	// already held. It grows as the query keeps giving the same answer, which
	// is what lets a query that never changes be cached at all: learning only
	// from changes never cached the one thing a cache is for.
	if len(e.stable) > 0 {
		e.ttl = c.ttlFrom(e.stable)
	} else {
		e.ttl = c.ttlFrom([]time.Duration{now.Sub(e.changedAt)})
	}

	if e.ttl < MinTTL {
		// Learned from, not held. See MinTTL.
		return
	}
	if len(reply) > MaxEntryBytes {
		// Learned from, not held. What it answered still counts as evidence of
		// how stable this query is; it is simply too large to keep.
		c.Rejected++
		return
	}
	stored := make([]byte, len(reply))
	copy(stored, reply)
	if e.entry != nil {
		c.bytes -= len(e.entry.Reply)
	}
	e.entry = &Entry{Reply: stored, StoredAt: now, TTL: e.ttl}
	c.bytes += len(stored)
	c.evictForBytes()
}

// indexOn hangs a key from every collection its answer depends on.
//
// Additive, and called on every observation rather than only the first.
//
// The set is not always known the first time a query is seen. The router works
// it out by reading the catalog, and a catalog read that comes back short
// leaves a view looking like an ordinary collection: the answer is then hung
// from the namespace the command named and from nothing else. The next read of
// the same query asks again and gets the fuller set, and until this that made
// no difference, because the index was written once when the key was new. A
// write to the collection discovered second did not drop the entry, and the
// cache went on serving an answer whose data had moved.
//
// Additive in the other direction too: a set that comes back short later does
// not remove anything. An extra reference costs an invalidation that was not
// needed, and a missing one costs a stale answer.
func (c *Cache) indexOn(key string, e *history, nss []string) {
	for _, ns := range nss {
		if ns == "" {
			continue
		}
		if c.byNS[ns] == nil {
			c.byNS[ns] = make(map[string]struct{})
		}
		if _, already := c.byNS[ns][key]; already {
			continue
		}
		c.byNS[ns][key] = struct{}{}
		e.nss = append(e.nss, ns)
	}
}

// unhook removes a key from every namespace it was hung from.
//
// byKey is bounded by max_queries and byNS was bounded by nothing. Deleting the
// entry and leaving the references behind is safe, because a reference that
// finds nothing does nothing, and safe is not free: the key stayed in the set
// of every other namespace it was hung from, for a query that may never be
// asked again, and nothing ever removed it. A router answering many distinct
// queries against a collection that is written to accumulated them for as long
// as it ran.
func (c *Cache) unhook(key string, e *history) {
	if e == nil {
		return
	}
	for _, ns := range e.nss {
		keys := c.byNS[ns]
		if keys == nil {
			continue
		}
		delete(keys, key)
		if len(keys) == 0 {
			delete(c.byNS, ns)
		}
	}
	e.nss = nil
}

// forget stops watching a query and gives back the memory its answer held.
func (c *Cache) forget(key string) bool {
	e, ok := c.byKey[key]
	if !ok {
		return false
	}
	held := e.entry != nil
	if held {
		c.bytes -= len(e.entry.Reply)
	}
	c.unhook(key, e)
	delete(c.byKey, key)
	return held
}

// TTLOf reports what has been learned for a query, for the dashboard and for
// tests.
func (c *Cache) TTLOf(key string) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h, ok := c.byKey[key]; ok {
		return h.ttl
	}
	return 0
}

// Invalidate drops everything learned for a namespace, which is what a write
// through this router means: the answers it was holding are answers to a
// question whose data just moved.
//
// The history goes with the entries. A write is evidence that the result
// changed, so the stability measured before it says nothing about after.
func (c *Cache) Invalidate(ns string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Copied first: forget deletes from byNS, including from this very set.
	keys := make([]string, 0, len(c.byNS[ns]))
	for k := range c.byNS[ns] {
		keys = append(keys, k)
	}
	n := 0
	for _, k := range keys {
		if c.forget(k) {
			n++
		}
	}
	delete(c.byNS, ns)
	return n
}

// Stats is everything worth publishing about a running cache, read in one pass
// under the lock.
//
// One call rather than five, because Learned and Rejected are plain fields and
// reading them beside a locked Counters() from a scrape goroutine is a race on
// two of them. It is also the only way the five numbers describe the same
// moment.
type Stats struct {
	Hits, Misses, Learned, Rejected uint64
	Entries                         int
	Bytes                           int
}

// Stats reports what this cache has done and what it is holding.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Hits:     c.Hits,
		Misses:   c.Misses,
		Learned:  c.Learned,
		Rejected: c.Rejected,
		Entries:  len(c.byKey),
		Bytes:    c.bytes,
	}
}

// Counters reports hits and misses, for the report and the dashboard.
func (c *Cache) Counters() (hits, misses uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Hits, c.Misses
}

// InvalidateDatabase drops everything learned for every collection in a
// database, which is what a dropDatabase means: the answers it was holding are
// answers about data that no longer exists.
func (c *Cache) InvalidateDatabase(db string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := db + "."
	// Gathered first, for the same reason Invalidate gathers: forget deletes
	// from byNS while this walks it.
	var keys []string
	for ns, ks := range c.byNS {
		if len(ns) <= len(prefix) || ns[:len(prefix)] != prefix {
			continue
		}
		for k := range ks {
			keys = append(keys, k)
		}
	}
	n := 0
	for _, k := range keys {
		if c.forget(k) {
			n++
		}
	}
	return n
}

// Len reports how many queries are being watched.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.byKey)
}

// ttlFrom turns observed stability into a TTL.
//
// The shortest period observed, not the average: a query that usually holds for
// an hour and sometimes for a second is a query that changes every second as
// far as anybody reading it is concerned. Then ten per cent off, and never more
// than the configured ceiling.
func (c *Cache) ttlFrom(stable []time.Duration) time.Duration {
	if len(stable) == 0 {
		return 0
	}
	shortest := stable[0]
	for _, d := range stable[1:] {
		if d < shortest {
			shortest = d
		}
	}
	ttl := time.Duration(float64(shortest) * safety)
	if c.maxTTL > 0 && ttl > c.maxTTL {
		ttl = c.maxTTL
	}
	return ttl
}

// evictForBytes drops held answers, oldest first, until the memory budget is
// met. The query stays: what was learned about it costs almost nothing, and
// throwing it away would mean learning it again.
//
// A scan per entry dropped, and it was measured and left that way. Replacing it
// with one scan and a sort, which is what fixed evictShapeIfFull next door, made
// both shapes slower:
//
//	                               scan per entry   scan and sort
//	uniform 4KB answers                86,977 ns       136,155 ns
//	a 128KB answer among 4KB ones   1,027,425 ns     1,405,644 ns
//
// The two cases look alike and are not. The advisor evicts on every insert once
// its table is full, so clearing a tenth of it lets the next hundred inserts
// skip the walk entirely. This returns immediately while under budget, and the
// usual k is one, so there is nothing to amortise and the sort is pure cost.
//
// The numbers above are still worth reading: BenchmarkCacheStoreOverBudget says
// a store at the budget costs 87us, and a large answer displacing small ones a
// millisecond, under this lock. That is a real cost for a deployment that turns
// the cache on with max_bytes set tight, and the fix for it is not this loop.
func (c *Cache) evictForBytes() {
	for c.bytes > c.maxBytes {
		var oldestKey string
		var oldest time.Time
		for k, h := range c.byKey {
			if h.entry == nil {
				continue
			}
			if oldestKey == "" || h.entry.StoredAt.Before(oldest) {
				oldestKey, oldest = k, h.entry.StoredAt
			}
		}
		if oldestKey == "" {
			return
		}
		h := c.byKey[oldestKey]
		c.bytes -= len(h.entry.Reply)
		h.entry = nil
	}
}

// evictIfFull makes room by forgetting the query whose answer was seen longest
// ago, which is the one least likely to be asked again.
func (c *Cache) evictIfFull() {
	if c.max <= 0 || len(c.byKey) < c.max {
		return
	}
	var oldestKey string
	var oldest time.Time
	for k, h := range c.byKey {
		if oldestKey == "" || h.lastSeen.Before(oldest) {
			oldestKey, oldest = k, h.lastSeen
		}
	}
	if oldestKey != "" {
		// Its own list, rather than a sweep of every namespace there is: the
		// key is hung from the handful its answer depends on and from no
		// others, and the sweep was the whole of byNS on every eviction.
		c.forget(oldestKey)
	}
}

// HashOf is the result hash the advisor records. Exported because the router
// hashes a reply once and gives it to both.
func HashOf(b []byte) uint64 { return hashOf(b) }

func hashOf(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

// refs is how many namespace references are held in total, for the test that
// pins them to the entries they belong to.
func (c *Cache) refs() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, keys := range c.byNS {
		n += len(keys)
	}
	return n
}
