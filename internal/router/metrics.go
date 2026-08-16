package router

import (
	"sync"
	"time"

	"github.com/zelmar/thermocline/internal/wire"
)

// The counters the router already kept answer "what happened". A dashboard has
// to answer "what is it costing", and those are different questions. A read the
// fast tier answered and a read that fell through to the archive are both one
// command; the whole point of the design is that they are not the same event.
//
// So every command is timed and filed under how it was served. That is what
// makes the two numbers comparable on one screen, and it is the only honest way
// to show what the fast tier is worth: not a ratio somebody asserted, but the
// measured distance between a hit and a miss on this deployment's own traffic.

// metricsWindow is how many seconds of per-second history are kept. Five
// minutes is enough to see a warm, an eviction sweep or a burst of misses
// without the snapshot growing large enough to be worth paging.
const metricsWindow = 300

// Class is how a command was served.
type Class int

const (
	// ClassFast is the fast tier answering on its own, which is the case the
	// whole design exists to make common.
	ClassFast Class = iota
	// ClassArchive is a read the fast tier could not answer, served from the
	// archive without promoting anything.
	ClassArchive
	// ClassMiss is a point lookup whose document was in the archive. It is
	// served and then copied up, so the next one is a ClassFast.
	ClassMiss
	// ClassTxnWarm is a transaction that needed documents the fast tier did not
	// hold: they were copied up and the transaction was replayed.
	ClassTxnWarm
	// ClassRefused is a command the router would not answer.
	ClassRefused
	// ClassAdmin is handshake and housekeeping traffic. Counted separately so
	// it cannot flatter the latency of real work.
	ClassAdmin
	numClasses
)

// ClassNames are the labels the dashboard shows, in the order it shows them.
var ClassNames = [numClasses]string{"fast", "archive", "miss", "txn_warm", "refused", "admin"}

// bucketsMS are latency histogram edges in milliseconds. They are logarithmic
// because the two cases this has to separate are three orders of magnitude
// apart: a fast tier hit is tens of microseconds and an archive read that waits
// for mirroring is seconds.
var bucketsMS = [...]float64{0.25, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

// adminCommands are handshake and housekeeping. They are the majority of the
// message count on an idle connection and none of the work, so mixing them into
// the fast tier's latency would make the fast tier look faster than it is.
var adminCommands = map[string]bool{
	"hello": true, "ismaster": true, "isMaster": true, "ping": true,
	"buildInfo": true, "buildinfo": true, "saslStart": true, "saslContinue": true,
	"getnonce": true, "logout": true, "endSessions": true, "killCursors": true,
	"listDatabases": true, "listCollections": true, "listIndexes": true,
	"connectionStatus": true, "whatsmyuri": true, "getLog": true, "hostInfo": true,
	"serverStatus": true, "getParameter": true, "commitTransaction": true,
	"abortTransaction": true, "getFreeMonitoringStatus": true, "atlasVersion": true,
}

// classOf is where a command starts out. Anything the router does about it
// moves it somewhere else.
func classOf(msg *wire.Message) Class {
	name, _, err := msg.CommandName()
	if err != nil {
		return ClassAdmin
	}
	if adminCommands[name] {
		return ClassAdmin
	}
	return ClassFast
}

// classStat is one class's running totals.
type classStat struct {
	count   int64
	totalMS float64
	maxMS   float64
	buckets [len(bucketsMS) + 1]int64
}

// second is one second of history, so the dashboard can draw a line rather than
// a single number that hides everything that happened before it.
type second struct {
	At      time.Time
	Count   [numClasses]int64
	TotalMS [numClasses]float64
}

// Metrics records how commands were served and what they cost.
//
// A mutex rather than atomics: this is one lock per command on a path that
// already did network round trips, and it keeps a second's worth of counters
// consistent with each other, which atomics taken separately would not.
type Metrics struct {
	mu      sync.Mutex
	stats   [numClasses]classStat
	history []second
	cur     second
	window  int
}

// NewMetrics builds a recorder keeping window seconds of history.
func NewMetrics(window int) *Metrics {
	if window <= 0 {
		window = 300
	}
	return &Metrics{window: window, history: make([]second, 0, window)}
}

// Record files one command under how it was served.
func (m *Metrics) Record(cls Class, d time.Duration) {
	if cls < 0 || cls >= numClasses {
		return
	}
	ms := float64(d) / float64(time.Millisecond)

	m.mu.Lock()
	defer m.mu.Unlock()

	s := &m.stats[cls]
	s.count++
	s.totalMS += ms
	if ms > s.maxMS {
		s.maxMS = ms
	}
	s.buckets[bucketOf(ms)]++

	now := time.Now().Truncate(time.Second)
	if m.cur.At.IsZero() {
		m.cur.At = now
	}
	if !m.cur.At.Equal(now) {
		m.rollLocked(now)
	}
	m.cur.Count[cls]++
	m.cur.TotalMS[cls] += ms
}

// rollLocked closes the current second and starts a new one.
func (m *Metrics) rollLocked(now time.Time) {
	m.history = append(m.history, m.cur)
	if len(m.history) > m.window {
		m.history = m.history[len(m.history)-m.window:]
	}
	m.cur = second{At: now}
}

func bucketOf(ms float64) int {
	for i, edge := range bucketsMS {
		if ms <= edge {
			return i
		}
	}
	return len(bucketsMS)
}

// ClassSnapshot is one class as the dashboard sees it.
type ClassSnapshot struct {
	Name    string  `json:"name"`
	Count   int64   `json:"count"`
	MeanMS  float64 `json:"mean_ms"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	MaxMS   float64 `json:"max_ms"`
	Buckets []int64 `json:"buckets"`
}

// SecondSnapshot is one second of history.
type SecondSnapshot struct {
	At     int64     `json:"at"`
	Count  []int64   `json:"count"`
	MeanMS []float64 `json:"mean_ms"`
}

// MetricsSnapshot is the whole recorder, safe to serialise.
type MetricsSnapshot struct {
	Buckets []float64        `json:"bucket_edges"`
	Classes []ClassSnapshot  `json:"classes"`
	History []SecondSnapshot `json:"history"`
}

// Snapshot copies the metrics out for reporting.
func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := MetricsSnapshot{
		Buckets: bucketsMS[:],
		Classes: make([]ClassSnapshot, 0, numClasses),
		History: make([]SecondSnapshot, 0, len(m.history)+1),
	}
	for i := 0; i < int(numClasses); i++ {
		s := m.stats[i]
		cs := ClassSnapshot{
			Name:    ClassNames[i],
			Count:   s.count,
			MaxMS:   round2(s.maxMS),
			Buckets: append([]int64(nil), s.buckets[:]...),
		}
		if s.count > 0 {
			cs.MeanMS = round2(s.totalMS / float64(s.count))
		}
		cs.P50MS = round2(quantile(s.buckets[:], s.count, 0.50, s.maxMS))
		cs.P95MS = round2(quantile(s.buckets[:], s.count, 0.95, s.maxMS))
		cs.P99MS = round2(quantile(s.buckets[:], s.count, 0.99, s.maxMS))
		out.Classes = append(out.Classes, cs)
	}

	hist := append(append([]second(nil), m.history...), m.cur)
	for _, h := range hist {
		if h.At.IsZero() {
			continue
		}
		snap := SecondSnapshot{
			At:     h.At.Unix(),
			Count:  make([]int64, numClasses),
			MeanMS: make([]float64, numClasses),
		}
		for i := 0; i < int(numClasses); i++ {
			snap.Count[i] = h.Count[i]
			if h.Count[i] > 0 {
				snap.MeanMS[i] = round2(h.TotalMS[i] / float64(h.Count[i]))
			}
		}
		out.History = append(out.History, snap)
	}
	return out
}

// quantile reads a percentile off the histogram.
//
// It returns the upper edge of the bucket the percentile falls in, which is an
// over-estimate rather than an under-estimate. A latency number that errs
// upwards is the safe direction to be wrong in.
//
// Except that it is capped at the largest value actually seen. Without that cap
// a class whose slowest command took 1.5 s reported a median of 2.5 s, because
// that is where its bucket ends, and a median above the maximum on the same row
// of the same table is the kind of number that makes a reader stop believing
// the rest of the page.
func quantile(buckets []int64, total int64, q, max float64) float64 {
	if total == 0 {
		return 0
	}
	edge := bucketsMS[len(bucketsMS)-1]
	want := int64(float64(total) * q)
	var seen int64
	for i, n := range buckets {
		seen += n
		if seen >= want {
			if i < len(bucketsMS) {
				edge = bucketsMS[i]
			}
			break
		}
	}
	if max > 0 && edge > max {
		return max
	}
	return edge
}

func round2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}
