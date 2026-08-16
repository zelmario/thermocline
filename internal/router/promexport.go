package router

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Everything an operator needs to alert on is already computed here, and until
// now it left the process as a log line once a minute, which nothing can page
// on. This writes the same numbers in the Prometheus text format, so a scrape
// config of four lines gets you alerts without adding a dependency to the
// project.
//
// Mirroring lag is the one that matters most and it is deliberately exported
// from the router rather than from the feeder. The router already reads the
// feeder's published position in order to decide whether an archive read is
// fresh, so it knows the lag continuously; and a lag metric published by the
// feeder itself goes silent exactly when the feeder dies, which is the moment
// you need the number. Scraped from the router, a dead feeder shows as lag
// climbing, which is what an alert should see.

// promEscape quotes a label value.
func promEscape(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}

// writeMetrics renders the whole export.
func (r *Router) writeMetrics(ctx context.Context, b *strings.Builder) {
	snap := r.SnapshotAll()

	counter := func(name, help string, value int64) {
		fmt.Fprintf(b, "# HELP thermocline_%s %s\n", name, help)
		fmt.Fprintf(b, "# TYPE thermocline_%s counter\n", name)
		fmt.Fprintf(b, "thermocline_%s %d\n", name, value)
	}
	gauge := func(name, help string, value float64) {
		fmt.Fprintf(b, "# HELP thermocline_%s %s\n", name, help)
		fmt.Fprintf(b, "# TYPE thermocline_%s gauge\n", name)
		fmt.Fprintf(b, "thermocline_%s %s\n", name, strconv.FormatFloat(value, 'f', -1, 64))
	}

	counter("read_misses_total", "Point lookups that found the document only in the archive.", snap.Counters["read_misses"])
	counter("write_misses_total", "Writes that matched nothing on the fast tier and found the document in the archive.", snap.Counters["write_misses"])
	counter("copied_up_total", "Documents brought back from the archive into the fast tier.", snap.Counters["copied_up"])
	counter("incomplete_total", "Queries reaching past what the fast tier can prove it holds.", snap.Counters["incomplete"])
	counter("from_archive_total", "Reads answered from the archive because the fast tier could not.", snap.Counters["from_archive"])
	counter("refused_total", "Commands the router would not answer.", snap.Counters["refused"])
	counter("txn_replays_total", "Transactions asked to replay after documents were brought up for them.", snap.Counters["txn_replays"])
	counter("duplicate_keys_total", "Writes refused because they would duplicate a key held in the archive.", snap.Counters["duplicate_keys"])
	counter("stale_reads_total", "Archive reads answered before mirroring caught up. Any of these is worth investigating.", snap.Counters["stale_reads"])
	counter("clients_total", "Client connections accepted since start.", snap.Counters["clients_total"])
	gauge("clients_open", "Client connections open now.", float64(snap.Counters["clients_open"]))
	gauge("uptime_seconds", "How long this router has been running.", float64(snap.UptimeS))

	// Commands and latency, per way of being served.
	fmt.Fprintf(b, "# HELP thermocline_command_duration_seconds Command latency by how the command was served.\n")
	fmt.Fprintf(b, "# TYPE thermocline_command_duration_seconds histogram\n")
	for _, c := range snap.Metrics.Classes {
		var cum int64
		for i, edge := range snap.Metrics.Buckets {
			if i < len(c.Buckets) {
				cum += c.Buckets[i]
			}
			fmt.Fprintf(b, "thermocline_command_duration_seconds_bucket{class=\"%s\",le=\"%s\"} %d\n",
				promEscape(c.Name), strconv.FormatFloat(edge/1000, 'f', -1, 64), cum)
		}
		// The overflow bucket, everything slower than the last edge.
		if len(c.Buckets) > len(snap.Metrics.Buckets) {
			cum += c.Buckets[len(snap.Metrics.Buckets)]
		}
		fmt.Fprintf(b, "thermocline_command_duration_seconds_bucket{class=\"%s\",le=\"+Inf\"} %d\n", promEscape(c.Name), cum)
		fmt.Fprintf(b, "thermocline_command_duration_seconds_sum{class=\"%s\"} %s\n",
			promEscape(c.Name), strconv.FormatFloat(c.TotalMS/1000, 'f', -1, 64))
		fmt.Fprintf(b, "thermocline_command_duration_seconds_count{class=\"%s\"} %d\n", promEscape(c.Name), c.Count)
	}

	// Mirroring lag: the first alert anyone should build.
	if lag, ok := r.mirroringLag(ctx); ok {
		gauge("mirroring_lag_seconds",
			"How far the archive trails the fast tier. Eviction safety, recovery exposure and fallback read latency all hang off this.",
			lag.Seconds())
	}

	// What each tier is holding.
	if len(snap.Inventory.Namespaces) > 0 {
		fmt.Fprintf(b, "# HELP thermocline_tier_documents Documents held per namespace and tier.\n")
		fmt.Fprintf(b, "# TYPE thermocline_tier_documents gauge\n")
		for _, n := range snap.Inventory.Namespaces {
			fmt.Fprintf(b, "thermocline_tier_documents{ns=\"%s\",tier=\"fast\"} %d\n", promEscape(n.NS), n.FastDocs)
			fmt.Fprintf(b, "thermocline_tier_documents{ns=\"%s\",tier=\"archive\"} %d\n", promEscape(n.NS), n.ArchiveDocs)
		}
		fmt.Fprintf(b, "# HELP thermocline_tier_bytes Storage used per namespace and tier.\n")
		fmt.Fprintf(b, "# TYPE thermocline_tier_bytes gauge\n")
		for _, n := range snap.Inventory.Namespaces {
			fmt.Fprintf(b, "thermocline_tier_bytes{ns=\"%s\",tier=\"fast\"} %d\n", promEscape(n.NS), n.FastBytes)
			fmt.Fprintf(b, "thermocline_tier_bytes{ns=\"%s\",tier=\"archive\"} %d\n", promEscape(n.NS), n.ArchiveBytes)
		}
	}
}

// mirroringLag reports how far the archive trails the fast tier.
//
// It prefers the feeder's published position, which is what the freshness
// barrier waits on, and falls back to the durable checkpoint when no position
// is being published. A feeder that has stopped shows as lag that grows, which
// is exactly what it should look like from outside.
func (r *Router) mirroringLag(ctx context.Context) (time.Duration, bool) {
	if r.cfg.FeederID == "" {
		return 0, false
	}
	if rec, ok, err := r.positions.At(ctx, r.cfg.FeederID); err == nil && ok {
		return time.Since(rec.At), true
	}
	at, ok, err := r.checkpoints.EventAt(ctx, r.cfg.FeederID)
	if err != nil || !ok {
		return 0, false
	}
	return time.Since(at), true
}
