package cache

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zelmario/thermocline/internal/meta"
)

// The advisor watches queries go past and works out which of them would be
// worth caching, and for how long.
//
// It answers the question in the order it has to be asked:
//
//  1. Which shapes are asked repeatedly at all. A query asked once is not a
//     candidate however stable its answer is.
//  2. Among those, which parameters repeat. A shape asked a thousand times with
//     a thousand different values has no repetition to exploit.
//  3. For those, how long the same result came back for the same parameters.
//  4. Suggest a TTL ten per cent shorter than that, so the suggestion is always
//     inside the evidence for it.
//
// Nothing is cached because of this. It measures and it recommends, and the
// caching is a separate decision made with the numbers in hand. That order
// matters in a system whose promise is that no application changes: a cache is
// the one thing here that can answer without asking, and it should be turned on
// against evidence rather than in hope.
//
// The result is hashed, never kept. What a query returned is the application's
// business.

// Advice is what was learned about one query shape and one set of parameters.
type Advice struct {
	Shape      string
	NS         string
	Executions uint64
	// Stable is the shortest period the same result was observed to hold for
	// these parameters, which is what the suggestion is derived from.
	Stable time.Duration
	// Suggested is the TTL to cache this for: ten per cent inside Stable.
	Suggested time.Duration
	// Changes is how many times the result was seen to change. Zero means the
	// answer has never moved while anybody was watching, and Stable is then
	// how long it has held so far.
	Changes uint64
	// Params is how many distinct parameter sets this shape was asked with,
	// and Repeats how many executions were repeats of one already seen. A
	// shape with no repeats has nothing to cache.
	Params  int
	Repeats uint64
}

// Benefit ranks advice by how much caching would save: how often it is asked,
// weighted by how long each answer would survive. A query asked constantly
// whose answer holds for a minute is worth more than a rare one that holds for
// an hour.
func (a Advice) Benefit() float64 {
	if a.Suggested <= 0 {
		return 0
	}
	return float64(a.Repeats) * a.Suggested.Seconds()
}

type paramStat struct {
	executions uint64
	lastHash   uint64
	changedAt  time.Time
	lastSeen   time.Time
	stable     []time.Duration
	changes    uint64
	seen       bool
}

type shapeStat struct {
	ns         string
	executions uint64
	repeats    uint64
	params     map[uint64]*paramStat
	lastSeen   time.Time
}

// Advisor accumulates what the router has seen.
type Advisor struct {
	mu        sync.Mutex
	shapes    map[string]*shapeStat
	maxShapes int
	maxParams int
	now       func() time.Time
	maxTTL    time.Duration

	// cached is the last ranking worked out, and when. See Report.
	cached   []Advice
	cachedAt time.Time
}

// NewAdvisor returns an advisor bounded to maxShapes query shapes, each
// remembering at most maxParams distinct parameter sets.
func NewAdvisor(maxShapes, maxParams int, maxTTL time.Duration) *Advisor {
	return &Advisor{
		shapes:    make(map[string]*shapeStat),
		maxShapes: maxShapes,
		maxParams: maxParams,
		maxTTL:    maxTTL,
		now:       time.Now,
	}
}

// Housekeeping reports whether a namespace belongs to MongoDB or to Thermocline
// rather than to the application.
//
// A driver asks config.collections constantly, to find out whether a collection
// is sharded, and those answers are as stable and as repetitive as anything an
// application sends. So they win the ranking. Measured on the pilot after a few
// thousand queries over real data: every one of the top twenty rows was that
// one shape, rendered identically twenty times because the report prints ? in
// place of the values, and not a single application query appeared.
//
// Crowding the report is the visible half. The shape table is bounded, and a
// query the operator cannot act on taking a slot in it is a query of theirs
// that is not measured at all.
//
// This is a decision about what to advise on, not about what to cache. Whether
// an answer to config.collections may be held is the cache's own business and
// is unchanged.
func Housekeeping(ns string) bool {
	db, _, found := strings.Cut(ns, ".")
	if !found {
		db = ns
	}
	switch db {
	case "admin", "local", "config":
		return true
	}
	return db == meta.DB
}

// Observe records one execution: which query, with which parameters, and what
// it answered.
func (a *Advisor) Observe(shape, ns string, params, result uint64) {
	if Housekeeping(ns) {
		return
	}
	now := a.now()
	a.mu.Lock()
	defer a.mu.Unlock()

	s, ok := a.shapes[shape]
	if !ok {
		a.evictShapeIfFull()
		s = &shapeStat{ns: ns, params: make(map[uint64]*paramStat)}
		a.shapes[shape] = s
	}
	s.executions++
	s.lastSeen = now
	a.cached = nil

	p, ok := s.params[params]
	if !ok {
		if a.maxParams > 0 && len(s.params) >= a.maxParams {
			// The shape is asked with more distinct parameters than are worth
			// remembering, which is itself the finding: there is little
			// repetition to exploit.
			return
		}
		s.params[params] = &paramStat{
			executions: 1, lastHash: result, changedAt: now, lastSeen: now, seen: true,
		}
		return
	}
	// Every execution after the first for these parameters is a repeat, which
	// is what makes a shape worth caching at all.
	p.executions++
	s.repeats++

	if result != p.lastHash {
		// How long that answer was being given: from the moment it first
		// appeared to the moment something else did. The change happened
		// somewhere between the previous look and this one, and taking this one
		// is what makes "returned 3,512,345 for five minutes" come out as five
		// minutes rather than as the four the sampling saw. The ten per cent
		// haircut on the suggestion is the margin for that gap, and a query
		// looked at rarely is exactly the query whose suggestion matters least.
		p.stable = append(p.stable, now.Sub(p.changedAt))
		if len(p.stable) > Stability {
			p.stable = p.stable[len(p.stable)-Stability:]
		}
		p.changes++
		p.lastHash = result
		p.changedAt = now
	}
	p.lastSeen = now
}

// ObserveHit records that a query was asked and answered from a cache, without
// treating the answer as evidence of anything.
//
// How often a query is asked is half of what makes it worth caching, so a hit
// counts. What the answer was is not evidence: it came from this system's own
// memory, and measuring stability against it would be measuring a belief
// against itself.
func (a *Advisor) ObserveHit(shape, ns string, params uint64) {
	if Housekeeping(ns) {
		return
	}
	now := a.now()
	a.mu.Lock()
	defer a.mu.Unlock()

	s, ok := a.shapes[shape]
	if !ok {
		return
	}
	s.executions++
	s.lastSeen = now
	if p, ok := s.params[params]; ok {
		p.executions++
		s.repeats++
	}
}

// Report returns what is worth caching, most valuable first.
//
// One row per shape and parameter set, because that is the unit a TTL applies
// to: the same query with different values can have wildly different stability,
// and averaging them would recommend a number that is wrong for both.
func (a *Advisor) Report(limit int) []Advice {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Held briefly, because the dashboard asks every two seconds and this walks
	// every shape and every parameter set it has ever seen, then sorts them. At
	// the bound that is a hundred thousand rows sorted twice a second to render
	// twelve of them, which is a strange amount of work for a page that is
	// watching for something to happen. A report a second old is the same
	// report.
	if a.cached != nil && a.now().Sub(a.cachedAt) < reportFor {
		return capReport(a.cached, limit)
	}

	var out []Advice
	for shape, s := range a.shapes {
		for _, p := range s.params {
			if p.executions < 2 {
				// Asked once with these parameters. There is nothing to repeat.
				continue
			}
			stable := a.stabilityOf(p)
			if stable <= 0 {
				continue
			}
			suggested := time.Duration(float64(stable) * safety)
			if a.maxTTL > 0 && suggested > a.maxTTL {
				suggested = a.maxTTL
			}
			if suggested < MinTTL {
				// Not worth recommending, because the cache would not hold it.
				// An entry below MinTTL is learned from and dropped: serving a
				// hit costs 365us, so it would expire before anything could
				// read it. Advising it would put a row in `cline cache` that
				// turning the cache on cannot act on, which is worse than a
				// short list, because the whole purpose of this report is to
				// decide whether the feature is worth enabling.
				continue
			}
			out = append(out, Advice{
				Shape:      shape,
				NS:         s.ns,
				Executions: p.executions,
				Stable:     stable,
				Suggested:  suggested,
				Changes:    p.changes,
				Params:     len(s.params),
				Repeats:    s.repeats,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Benefit() != out[j].Benefit() {
			return out[i].Benefit() > out[j].Benefit()
		}
		return out[i].Shape < out[j].Shape
	})
	// Only the head is kept: nobody reads past the first page of a ranking, and
	// holding the whole thing to satisfy a limit of twelve is the same waste in
	// a different place.
	a.cached = capReport(out, reportKeep)
	a.cachedAt = a.now()
	return capReport(a.cached, limit)
}

// reportFor is how long a report stands before it is worked out again.
const reportFor = time.Second

// reportKeep is how many rows are held between recomputations. More than any
// caller asks for, few enough that holding them costs nothing.
const reportKeep = 100

func capReport(rows []Advice, limit int) []Advice {
	if limit > 0 && len(rows) > limit {
		return rows[:limit]
	}
	return rows
}

// stabilityOf is the shortest period this answer was observed to hold, or how
// long the current one has held when it has never been seen to change.
func (a *Advisor) stabilityOf(p *paramStat) time.Duration {
	if len(p.stable) == 0 {
		return p.lastSeen.Sub(p.changedAt)
	}
	shortest := p.stable[0]
	for _, d := range p.stable[1:] {
		if d < shortest {
			shortest = d
		}
	}
	return shortest
}

// Shapes reports how many query shapes are being watched, for the operator who
// wants to know whether the bound has been reached.
func (a *Advisor) Shapes() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.shapes)
}

// evictShapeIfFull forgets the shape nobody has asked for in the longest time.
// evictShapeIfFull drops the least recently seen shapes once the table is full.
//
// In a batch, and that is the whole point. Picking the single oldest costs a
// scan of every shape, and eviction runs on every insert once the table is at
// max_queries, so a deployment with more distinct shapes than the cap paid that
// scan on every cacheable read. Measured at the default cap of 1000:
//
//	BenchmarkAdvisorObserveWhenFull      48,199 ns/op
//	BenchmarkAdvisorObserveWhenNotFull    2,621 ns/op
//
// An eighteen-fold cliff, on 48us added to a command whose whole proxy overhead
// is 130us, paid by a component that runs by default whether or not anything is
// cached. Nothing in the export would have shown it either.
//
// One scan now clears a tenth of the table, so the next hundred inserts do not
// scan at all. What goes is the same set the one-at-a-time version would have
// dropped over those hundred inserts, the least recently seen, and the advisor
// is a bounded sample of recent traffic by construction.
func (a *Advisor) evictShapeIfFull() {
	if a.maxShapes <= 0 || len(a.shapes) < a.maxShapes {
		return
	}
	drop := a.maxShapes / 10
	if drop < 1 {
		drop = 1
	}
	type aged struct {
		key string
		at  time.Time
	}
	all := make([]aged, 0, len(a.shapes))
	for k, s := range a.shapes {
		all = append(all, aged{k, s.lastSeen})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	if drop > len(all) {
		drop = len(all)
	}
	for _, e := range all[:drop] {
		delete(a.shapes, e.key)
	}
}
