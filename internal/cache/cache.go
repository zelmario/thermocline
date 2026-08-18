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

	entry *Entry
}

// Cache is a bounded set of learned queries and their most recent answers.
type Cache struct {
	mu      sync.Mutex
	byKey   map[string]*history
	byNS    map[string]map[string]struct{}
	max     int
	maxTTL  time.Duration
	now     func() time.Time
	Hits    uint64
	Misses  uint64
	Learned uint64
}

// New returns a cache holding at most max queries, with no entry ever living
// longer than maxTTL however stable it has looked.
func New(max int, maxTTL time.Duration) *Cache {
	return &Cache{
		byKey:  make(map[string]*history),
		byNS:   make(map[string]map[string]struct{}),
		max:    max,
		maxTTL: maxTTL,
		now:    time.Now,
	}
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
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.byKey[key]
	if !ok {
		c.evictIfFull()
		e = &history{lastHash: h, lastSeen: now, changedAt: now}
		c.byKey[key] = e
		if c.byNS[ns] == nil {
			c.byNS[ns] = make(map[string]struct{})
		}
		c.byNS[ns][key] = struct{}{}
		return
	}

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

	if e.ttl <= 0 {
		return
	}
	stored := make([]byte, len(reply))
	copy(stored, reply)
	e.entry = &Entry{Reply: stored, StoredAt: now, TTL: e.ttl}
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
	keys := c.byNS[ns]
	n := 0
	for k := range keys {
		if h, ok := c.byKey[k]; ok && h.entry != nil {
			n++
		}
		delete(c.byKey, k)
	}
	delete(c.byNS, ns)
	return n
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
	n := 0
	for ns, keys := range c.byNS {
		if len(ns) <= len(prefix) || ns[:len(prefix)] != prefix {
			continue
		}
		for k := range keys {
			if h, ok := c.byKey[k]; ok && h.entry != nil {
				n++
			}
			delete(c.byKey, k)
		}
		delete(c.byNS, ns)
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
		delete(c.byKey, oldestKey)
		for _, keys := range c.byNS {
			delete(keys, oldestKey)
		}
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
