package router

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zelmario/thermocline/internal/health"
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

	// The query cache, and only when it is on.
	//
	// Nothing here for a cache nobody enabled: a hit rate of zero published for
	// a cache that is not running reads as "the cache is not working", and the
	// truth is that nobody turned it on. Absent says that and a zero does not.
	//
	// The hit rate is what says whether enabling it was worth it. The bytes are
	// what say whether max_bytes is about to start throwing answers away, and
	// rejected is the other half of that: a cache handed replies larger than
	// MaxEntryBytes looks idle from the hit rate alone, and the reason is
	// visible only here.
	if r.cache != nil {
		cs := r.cache.Stats()
		counter("cache_hits_total", "Reads answered from the query cache without asking either tier.", int64(cs.Hits))
		counter("cache_misses_total", "Cacheable reads the cache could not answer. With hits, this is the hit rate.", int64(cs.Misses))
		counter("cache_learned_total", "Times a watched query was seen to return something new, which is what the TTL is learned from.", int64(cs.Learned))
		counter("cache_rejected_total", "Answers too large to hold. A cache with a low hit rate and a rising count here is being handed replies over 256KB, not failing to learn.", int64(cs.Rejected))
		gauge("cache_entries", "Queries being watched. At max_queries the least recently seen is forgotten on every new one.", float64(cs.Entries))
		gauge("cache_bytes", "Memory the held answers occupy. Against max_bytes, this says how close the cache is to evicting to stay inside its budget.", float64(cs.Bytes))
	}

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

	// What each daemon says about its own clock, in one place, because one
	// alert rule has to cover all of them.
	//
	// Values that go up and down and may be negative, so they cannot travel
	// with the counters. The evictor's is why this exists: far enough behind
	// and an eviction announcement expires before the feeder reads the delete
	// it explains, and the document is lost from both tiers. The router's and
	// the feeder's are here because the freshness barrier and mirroring lag are
	// both worked out by subtracting one host's instant from another's, and a
	// correction nobody can see is a correction nobody can check.
	//
	// The evictor's arrives in its daemon report, which it writes after every
	// pass. The feeder publishes no report, so its reading travels in the
	// position record it writes several times a second, and the router's is its
	// own. Alert rules select on the gauge name rather than the daemon, so all
	// of them are covered by what is already written down.
	var skews []daemonSkew
	var other []daemonGaugeRow
	if mine, ok := r.archiveSkew(ctx); ok {
		skews = append(skews, daemonSkew{"router", mine.Seconds()})
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
	mirroring := r.mirroringLag(ctx)
	if mirroring.feederSkewKnown {
		skews = append(skews, daemonSkew{"feeder", mirroring.feederSkew.Seconds()})
	}
	if mirroring.known {
		gauge("mirroring_lag_seconds", lagHelp, mirroring.lag.Seconds())
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
		for _, rep := range reports {
			for name, v := range rep.Gauges {
				if name == "clock_skew_seconds" {
					skews = append(skews, daemonSkew{rep.ID, v})
					continue
				}
				// Anything else a daemon measured about itself, kept where it
				// has always been published.
				other = append(other, daemonGaugeRow{rep.ID, name, v})
			}
		}
	}
	writeDaemonGauges(b, skews, other)

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
func (r *Router) mirroringLag(ctx context.Context) mirroringReading {
	if r.cfg.FeederID == "" {
		return mirroringReading{}
	}
	// Clamped at zero, the way internal/health does it for the same value.
	//
	// The instant comes from the feeder, and the feeder stamps it from the tier
	// it is reading rather than from this process's clock. A router whose clock
	// trails that one by a little reports a negative lag, which is not a small
	// number, it is a nonsense one: measured -1.62 seconds on a healthy
	// deployment where the feeder had just published.
	//
	// The evictor's copy of this arithmetic has clamped since it was written,
	// and this one is the twin nobody treated: the same quantity was honest in
	// the health check and wrong in the metric an operator graphs.
	//
	// Clamping treated the symptom, and the cause is that the two instants come
	// from two machines. The feeder publishes how far ahead of the archive its
	// own clock is, and this side measures the same for itself, so both can be
	// put in the archive's frame before being subtracted. Without that, a
	// router thirty seconds ahead of its feeder graphs thirty seconds of
	// mirroring lag on a healthy deployment, and the README tells operators to
	// alert on this value.
	if rec, ok, err := r.positions.At(ctx, r.cfg.FeederID); err == nil && ok {
		theirs, theirsKnown := rec.Skew()
		mine, mineKnown := r.archiveSkew(ctx)
		return mirroringReading{
			lag:        health.LagFrom(time.Now(), rec.At, mine, theirs, mineKnown && theirsKnown),
			known:      true,
			feederSkew: theirs, feederSkewKnown: theirsKnown,
		}
	}
	at, ok, err := r.checkpoints.EventAt(ctx, r.cfg.FeederID)
	if err != nil || !ok {
		return mirroringReading{}
	}
	return mirroringReading{lag: max(time.Since(at), 0), known: true}
}

// daemonSkew is one daemon's clock less the archive's, in seconds.
type daemonSkew struct {
	daemon string
	value  float64
}

// daemonGaugeRow is anything else a daemon measured about itself.
type daemonGaugeRow struct {
	daemon string
	name   string
	value  float64
}

// writeDaemonGauges prints what each daemon last measured about itself, with
// the clock offsets from every daemon in one family whatever route they
// travelled by: a daemon report, the feeder's position record, or this process
// measuring its own.
func writeDaemonGauges(b *strings.Builder, skews []daemonSkew, other []daemonGaugeRow) {
	if len(skews) == 0 && len(other) == 0 {
		return
	}
	fmt.Fprintf(b, "# HELP thermocline_daemon_gauge What each daemon last measured about itself. clock_skew_seconds is this daemon's clock less the archive's; negative means behind, which is the direction that loses documents.\n")
	fmt.Fprintf(b, "# TYPE thermocline_daemon_gauge gauge\n")
	sort.Slice(skews, func(i, j int) bool { return skews[i].daemon < skews[j].daemon })
	for _, s := range skews {
		fmt.Fprintf(b, "thermocline_daemon_gauge{daemon=\"%s\",gauge=\"clock_skew_seconds\"} %s\n",
			promEscape(s.daemon), strconv.FormatFloat(s.value, 'f', -1, 64))
	}
	sort.Slice(other, func(i, j int) bool {
		if other[i].daemon != other[j].daemon {
			return other[i].daemon < other[j].daemon
		}
		return other[i].name < other[j].name
	})
	for _, g := range other {
		fmt.Fprintf(b, "thermocline_daemon_gauge{daemon=\"%s\",gauge=\"%s\"} %s\n",
			promEscape(g.daemon), promEscape(g.name), strconv.FormatFloat(g.value, 'f', -1, 64))
	}
}

// mirroringReading is what one look at the feeder's published position says
// about it: how far behind mirroring is, and where the feeder's own clock
// stands, which is both how that lag was worked out and a number worth
// exporting on its own.
type mirroringReading struct {
	lag             time.Duration
	known           bool
	feederSkew      time.Duration
	feederSkewKnown bool
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
