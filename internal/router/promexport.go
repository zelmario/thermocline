package router

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zelmario/thermocline/internal/meta"
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
	counter("change_streams_total", "Change streams opened on tiered collections. These report evictions as deletes and copy-ups as inserts.", snap.Counters["change_streams"])
	counter("unauthorized_total", "Archive answers refused because the fast tier would not have served this client. Any value above zero is a client reading a collection it has no permission for.", snap.Counters["unauthorized"])
	counter("secondary_reads_total", "Reads that asked for a secondary and were sent to one. With primary_only_reads_total this says how much read load actually moved.", snap.Counters["secondary_reads"])
	counter("failover_replays_total", "Commands re-sent after the fast tier changed primary. Each one is an election a client did not have to see.", snap.Counters["failover_replays"])
	counter("primary_only_reads_total", "Reads that asked for a secondary and were served by the primary. Answers are correct and fresher than asked for; the read load stays on the primary.", snap.Counters["primary_only_reads"])
	counter("unreadable_total", "Messages the router could not inspect, which are therefore unguarded. Compressed connections cause these; any value above zero needs the client's compressors removed.", snap.Counters["unreadable"])
	counter("stale_reads_total", "Archive reads answered before mirroring caught up. Any of these is worth investigating.", snap.Counters["stale_reads"])
	counter("claim_grace_reads_total", "Reads answered from a completeness claim the router could not re-read, allowed by router.claim_grace. Any value above zero means the archive was unreachable and this router answered anyway.", snap.Counters["claim_grace_reads"])
	// The first thing worth paging on, and it was readable only from the
	// dashboard: an alert cannot be written against a page somebody has to open.
	counter("panics_total", "Client connections closed by a recovered panic. Any value above zero is a defect, with a stack in the log.", snap.Counters["panics"])
	counter("superseded_cursors_total", "Cursors the fast tier was left holding after the archive answered the query, closed by the router. Rising with no reads falling through means the router cannot close them and they sit until mongod's timeout.", snap.Counters["superseded_cursors"])
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
	//
	// Infinity, not a missing series, when nothing has ever been mirrored. A
	// feeder that has stopped shows the lag climbing and pages; a feeder that
	// never started at all left no position and no checkpoint, so this used to
	// publish nothing, and an alert on a series that is absent does not fire.
	// The worst state of the system was the one the first alert anyone builds
	// could not see. Unbounded is also the honest reading: without a checkpoint,
	// how far the archive trails the fast tier is unknown.
	const lagHelp = "How far the archive trails the fast tier. Eviction safety, recovery " +
		"exposure and fallback read latency all hang off this. +Inf means nothing has been " +
		"mirrored at all, which is a feeder that has never run."
	if lag, ok := r.mirroringLag(ctx); ok {
		gauge("mirroring_lag_seconds", lagHelp, lag.Seconds())
	} else if r.cfg.FeederID != "" {
		gauge("mirroring_lag_seconds", lagHelp, math.Inf(1))
	}

	// The background daemons, which have no listening socket of their own.
	//
	// Each publishes what it has done into the archive after every pass, and the
	// router reads it here. The counters are useful; the gauge is the one to
	// alert on. An evictor that has stopped, or that is failing closed on
	// mirroring lag, looks exactly like a healthy one from outside until
	// somebody notices it has not run, and seconds_since_pass is that noticing.
	if reports, rerr := r.daemonReports(ctx); rerr == nil && len(reports) > 0 {
		sort.Slice(reports, func(i, j int) bool { return reports[i].ID < reports[j].ID })
		fmt.Fprintf(b, "# HELP thermocline_daemon_seconds_since_pass How long since this daemon finished a pass. A daemon that has stopped shows this climbing.\n")
		fmt.Fprintf(b, "# TYPE thermocline_daemon_seconds_since_pass gauge\n")
		for _, rep := range reports {
			fmt.Fprintf(b, "thermocline_daemon_seconds_since_pass{daemon=\"%s\"} %s\n",
				promEscape(rep.ID), strconv.FormatFloat(time.Since(rep.At).Seconds(), 'f', -1, 64))
		}
		fmt.Fprintf(b, "# HELP thermocline_daemon_dry_run Whether this daemon is running without making changes.\n")
		fmt.Fprintf(b, "# TYPE thermocline_daemon_dry_run gauge\n")
		for _, rep := range reports {
			dry := 0
			if rep.DryRun {
				dry = 1
			}
			fmt.Fprintf(b, "thermocline_daemon_dry_run{daemon=\"%s\"} %d\n", promEscape(rep.ID), dry)
		}
		// Values that go up and down, and may be negative, so they cannot
		// travel with the counters: the evictor's clock skew against the
		// archive is why this exists. Far enough behind and an eviction
		// announcement expires before the feeder reads the delete it explains,
		// and the document is lost from both tiers.
		gauged := false
		for _, rep := range reports {
			if len(rep.Gauges) == 0 {
				continue
			}
			if !gauged {
				fmt.Fprintf(b, "# HELP thermocline_daemon_gauge What each daemon last measured about itself. clock_skew_seconds is this daemon's clock less the archive's; negative means behind, which is the direction that loses documents.\n")
				fmt.Fprintf(b, "# TYPE thermocline_daemon_gauge gauge\n")
				gauged = true
			}
			names := make([]string, 0, len(rep.Gauges))
			for k := range rep.Gauges {
				names = append(names, k)
			}
			sort.Strings(names)
			for _, k := range names {
				fmt.Fprintf(b, "thermocline_daemon_gauge{daemon=\"%s\",gauge=\"%s\"} %s\n",
					promEscape(rep.ID), promEscape(k),
					strconv.FormatFloat(rep.Gauges[k], 'f', -1, 64))
			}
		}
		fmt.Fprintf(b, "# HELP thermocline_daemon_documents_total Documents each daemon has handled since it started, by outcome.\n")
		fmt.Fprintf(b, "# TYPE thermocline_daemon_documents_total counter\n")
		for _, rep := range reports {
			keys := make([]string, 0, len(rep.Counters))
			for k := range rep.Counters {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Fprintf(b, "thermocline_daemon_documents_total{daemon=\"%s\",outcome=\"%s\"} %d\n",
					promEscape(rep.ID), promEscape(k), rep.Counters[k])
			}
		}
	}

	// An operator hold on eviction, exported because a hold that is forgotten is
	// its own outage: the fast tier fills up with everything that should have
	// been removed, and nothing else says why. A pause is meant to be visible
	// and temporary, and this is what makes it the first.
	if r.cold != nil {
		if rec, paused, perr := meta.EvictionPaused(ctx, r.cold); perr == nil {
			held := 0.0
			if paused {
				held = 1
			}
			gauge("eviction_paused",
				"Whether an operator has stopped the evictor removing anything. A hold left on fills the fast tier.",
				held)
			if paused {
				gauge("eviction_paused_seconds",
					"How long eviction has been held. Alert on this being long rather than on it being set.",
					time.Since(rec.Since).Seconds())
			}
		}
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

// daemonReports reads what the background daemons last published.
//
// Separated so the export can be rendered without a cluster behind it, which is
// what the histogram test does.
func (r *Router) daemonReports(ctx context.Context) ([]meta.DaemonReport, error) {
	if r.cold == nil {
		return nil, nil
	}
	return meta.ReadDaemonReports(ctx, r.cold)
}
