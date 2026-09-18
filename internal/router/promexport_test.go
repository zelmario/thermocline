package router

import (
	"context"
	"github.com/zelmario/thermocline/internal/health"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/zelmario/thermocline/internal/cache"
	"github.com/zelmario/thermocline/internal/config"
)

// A malformed histogram is not rejected by Prometheus, it is ingested and then
// queried wrong: buckets that are not cumulative make every quantile nonsense,
// and a missing +Inf makes the count disagree with the bucket it should equal.
// Both are silent, so they are worth a test.
func TestMetricsHistogramIsWellFormed(t *testing.T) {
	// Built directly rather than through New, which dials the tiers to set up
	// its stores. Nothing below needs them: with no feeder id, the lag gauge
	// returns early without touching a client.
	r := &Router{
		cfg:     config.RouterConfig{Completeness: config.Archive},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: NewMetrics(metricsWindow),
		started: time.Now(),
	}

	for _, d := range []time.Duration{
		100 * time.Microsecond, 300 * time.Microsecond, 900 * time.Microsecond,
		3 * time.Millisecond, 40 * time.Millisecond, 900 * time.Millisecond,
		9 * time.Second, // past the last edge, so the overflow bucket is used
	} {
		r.Metrics.Record(ClassFast, d)
	}
	r.Metrics.Record(ClassArchive, 500*time.Millisecond)

	var b strings.Builder
	r.writeMetrics(context.Background(), &b)
	out := b.String()

	bucketRe := regexp.MustCompile(`thermocline_command_duration_seconds_bucket\{class="([^"]+)",le="([^"]+)"\} (\d+)`)
	countRe := regexp.MustCompile(`thermocline_command_duration_seconds_count\{class="([^"]+)"\} (\d+)`)

	buckets := map[string][]int64{}
	last := map[string]string{}
	for _, m := range bucketRe.FindAllStringSubmatch(out, -1) {
		buckets[m[1]] = append(buckets[m[1]], atoi(t, m[3]))
		last[m[1]] = m[2]
	}
	counts := map[string]int64{}
	for _, m := range countRe.FindAllStringSubmatch(out, -1) {
		counts[m[1]] = atoi(t, m[2])
	}

	if len(buckets) != int(numClasses) {
		t.Fatalf("got %d classes of buckets, want %d", len(buckets), numClasses)
	}
	for cls, vals := range buckets {
		for i := 1; i < len(vals); i++ {
			if vals[i] < vals[i-1] {
				t.Errorf("%s: buckets are not cumulative: %v", cls, vals)
				break
			}
		}
		if last[cls] != "+Inf" {
			t.Errorf("%s: last bucket is %q, want +Inf", cls, last[cls])
		}
		if got, want := vals[len(vals)-1], counts[cls]; got != want {
			t.Errorf("%s: +Inf bucket is %d, _count is %d, they must agree", cls, got, want)
		}
	}
	// The 9s sample has to land somewhere, and the only bucket that can hold it
	// is the overflow one.
	if counts["fast"] != 7 {
		t.Errorf("fast count is %d, want 7", counts["fast"])
	}
	if !strings.Contains(out, `thermocline_command_duration_seconds_sum{class="fast"}`) {
		t.Error("no _sum for the fast class")
	}
}

// A label value carrying a quote or a backslash would break the exposition
// format, and namespaces come from configuration rather than from us.
func TestMetricsEscapesLabels(t *testing.T) {
	got := promEscape(`a"b\c`)
	if got != `a\"b\\c` {
		t.Errorf("promEscape = %q, want %q", got, `a\"b\\c`)
	}
}

func atoi(t *testing.T, s string) int64 {
	t.Helper()
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("not a number: %q", s)
		}
		n = n*10 + int64(c-'0')
	}
	return n
}

// Every counter an alert is written against has to be exported, not only shown
// on the dashboard: an alert cannot be written against a page somebody has to
// open. panics was readable only there, and it is the first thing worth paging
// on, because any value above zero is a defect with a stack in the log.
func TestMetricsExportsWhatIsWorthPagingOn(t *testing.T) {
	r := &Router{
		cfg:     config.RouterConfig{Completeness: config.Archive},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: NewMetrics(metricsWindow),
		started: time.Now(),
	}
	r.Stats.Panics.Store(2)
	r.Stats.StaleReads.Store(3)
	r.Stats.Refused.Store(4)

	var b strings.Builder
	r.writeMetrics(context.Background(), &b)
	out := b.String()

	for _, want := range []string{
		"thermocline_panics_total 2",
		"thermocline_stale_reads_total 3",
		"thermocline_refused_total 4",
		"thermocline_superseded_cursors_total",
		"thermocline_unauthorized_total",
		"thermocline_duplicate_keys_total",
		"thermocline_unreadable_total",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the exporter does not carry %q", want)
		}
	}
}

// The cache is the one thing here that can answer without asking, and nothing
// in the export said anything about it.
//
// Its hit rate is what says whether turning it on was worth it, and its size is
// what says whether max_bytes is about to start throwing answers away. Both
// were readable only from the dashboard, which is a page somebody has to open,
// and an alert cannot be written against that. Worse for the memory: an
// operator who set max_bytes too low got a cache that quietly held nothing,
// with the counter that would have shown it visible nowhere.
func TestMetricsExportsTheCache(t *testing.T) {
	r := &Router{
		cfg:     config.RouterConfig{Completeness: config.Archive},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: NewMetrics(metricsWindow),
		started: time.Now(),
		cache:   cache.NewWithBytes(100, 1<<20, time.Minute),
	}
	// Two looks at a query that has held still, so there is a hit, a miss and
	// an entry to report.
	//
	// With a gap between them, and this is not decoration. The TTL is derived
	// from how long the answer has been seen to hold, so two observations in
	// the same instant earn a TTL of nothing: below cache.MinTTL the answer is
	// learned from and not stored, and before that floor existed it was stored
	// with a TTL of a few hundred nanoseconds and expired before the Get below
	// could read it. That is what made this test fail about once in a few
	// hundred runs, always under -count, which is the only way it ever ran fast
	// enough. The gap is a hundred times the floor so that a scheduling delay
	// between storing and reading cannot close it.
	r.cache.Observe("q", "shop.orders", []byte("an answer"))
	time.Sleep(100 * cache.MinTTL)
	r.cache.Observe("q", "shop.orders", []byte("an answer"))
	if _, ok := r.cache.Get("q"); !ok {
		t.Fatal("nothing was cached, so this test proves nothing")
	}
	r.cache.Get("absent")

	var b strings.Builder
	r.writeMetrics(context.Background(), &b)
	out := b.String()

	for _, want := range []string{
		"thermocline_cache_hits_total 1",
		"thermocline_cache_misses_total 1",
		"thermocline_cache_entries 1",
		"thermocline_cache_bytes ",
		"thermocline_cache_rejected_total 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the exporter does not carry %q", want)
		}
	}
}

// And says nothing about a cache that is off, rather than publishing a hit rate
// of zero for one that is not running. A zero there reads as "the cache is not
// working" and the truth is that nobody turned it on.
func TestMetricsIsSilentAboutACacheThatIsOff(t *testing.T) {
	r := &Router{
		cfg:     config.RouterConfig{Completeness: config.Archive},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: NewMetrics(metricsWindow),
		started: time.Now(),
	}
	var b strings.Builder
	r.writeMetrics(context.Background(), &b)
	if strings.Contains(b.String(), "thermocline_cache_") {
		t.Error("the exporter published cache metrics for a cache that is off")
	}
}

// thermocline_mirroring_lag_seconds is the number the README tells operators to
// alert on, and it is one machine's clock minus another's.
//
// The clamp that used to be the whole treatment kept it from going negative,
// which is what skew in one direction produced. Skew in the other direction
// produces a positive number, and a positive number is not obviously wrong: a
// router thirty seconds ahead of its feeder graphs thirty seconds of mirroring
// lag on a deployment where nothing is behind at all, and the alert fires.
func TestLagIsMeasuredInOneFrame(t *testing.T) {
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

	// The archive is the reference. This host is 30s ahead of it, the feeder is
	// 5s behind it, and the feeder published a position for the instant the
	// archive calls 11:59:59, so the true lag is one second.
	const mine = 30 * time.Second
	const theirs = -5 * time.Second
	at := now.Add(-mine).Add(-time.Second).Add(theirs)

	if got := health.LagFrom(now, at, mine, theirs, true); got != time.Second {
		t.Errorf("lag = %v, want 1s", got)
	}
	// What it looked like before: 36 seconds of lag on a healthy deployment.
	if got := health.LagFrom(now, at, mine, theirs, false); got != 36*time.Second {
		t.Errorf("uncorrected lag = %v, want the 36s this exists to stop reporting", got)
	}
	// Still clamped. The correction is measured over a round trip, so a
	// position published a moment ago can subtract to a hair below zero.
	if got := health.LagFrom(now, now.Add(time.Millisecond), 0, 0, true); got != 0 {
		t.Errorf("lag = %v, want it clamped at zero", got)
	}
	// And real lag still reads as real lag with no offsets to hand.
	if got := health.LagFrom(now, now.Add(-90*time.Second), 0, 0, false); got != 90*time.Second {
		t.Errorf("lag = %v, want 90s", got)
	}
}

// One quantity, one metric family, whatever route the reading travelled by. The
// alert rules in docs/monitoring.md select on the gauge name and not on the
// daemon, so a reading published anywhere else is a reading nothing alerts on.
func TestClockSkewFromEveryDaemonLandsInOneFamily(t *testing.T) {
	var b strings.Builder
	writeDaemonGauges(&b,
		// The router measures its own, the feeder's arrives in the position
		// record it writes several times a second, and the evictor's comes from
		// the report it publishes after every pass.
		[]daemonSkew{{"router", 0.002}, {"feeder", -0.5}, {"evictor", -61}},
		[]daemonGaugeRow{{"evictor", "something_else", 1}},
	)
	out := b.String()

	for _, want := range []string{
		`thermocline_daemon_gauge{daemon="evictor",gauge="clock_skew_seconds"} -61`,
		`thermocline_daemon_gauge{daemon="feeder",gauge="clock_skew_seconds"} -0.5`,
		`thermocline_daemon_gauge{daemon="router",gauge="clock_skew_seconds"} 0.002`,
		`thermocline_daemon_gauge{daemon="evictor",gauge="something_else"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing:\n%s\ngot:\n%s", want, out)
		}
	}
	if n := strings.Count(out, "# HELP thermocline_daemon_gauge"); n != 1 {
		t.Errorf("the family is declared %d times, want once: Prometheus rejects a repeated HELP", n)
	}
	// Sorted, because an exporter that shuffles its own output makes every diff
	// of two scrapes unreadable.
	if i, j := strings.Index(out, `daemon="evictor"`), strings.Index(out, `daemon="feeder"`); i > j {
		t.Errorf("the rows are not sorted by daemon:\n%s", out)
	}
	// Nothing to say, nothing said: an empty family with a HELP and no series
	// is noise in every scrape.
	var empty strings.Builder
	writeDaemonGauges(&empty, nil, nil)
	if empty.Len() != 0 {
		t.Errorf("wrote a family with no readings in it:\n%s", empty.String())
	}
}

// Every counter the snapshot carries has to reach the export.
//
// The list above is hand written, and a hand written list is how three of these
// went missing: commands, newest_first and warmed_for_txn were readable from
// the dashboard alone, and events_hidden was counted in two places and read in
// none. An alert cannot be written against a page somebody has to open, and it
// certainly cannot be written against a field nothing reads.
//
// So this asks the question mechanically instead. A counter that genuinely does
// not belong in the export has to be named here, which makes leaving it out a
// decision somebody wrote down rather than an omission.
func TestEveryCounterInTheSnapshotIsExported(t *testing.T) {
	r := &Router{
		cfg:     config.RouterConfig{Completeness: config.Archive},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: NewMetrics(metricsWindow),
		started: time.Now(),
	}

	var b strings.Builder
	r.writeMetrics(context.Background(), &b)
	out := b.String()

	// Deliberately not exported, with the reason.
	skip := map[string]string{}

	for k := range r.SnapshotAll().Counters {
		if why, ok := skip[k]; ok {
			t.Logf("%s is not exported on purpose: %s", k, why)
			continue
		}
		// A counter carries the _total suffix, a gauge does not.
		if strings.Contains(out, "\nthermocline_"+k+"_total ") ||
			strings.Contains(out, "\nthermocline_"+k+" ") {
			continue
		}
		t.Errorf("the snapshot counts %q and the export does not carry it", k)
	}
}
