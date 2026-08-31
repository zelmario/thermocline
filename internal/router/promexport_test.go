package router

import (
	"context"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

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
