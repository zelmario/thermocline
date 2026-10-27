package router

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zelmario/thermocline/internal/checkpoint"
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

// metricsBudget bounds what one scrape may spend reaching the archive. See the
// /metrics handler.
const metricsBudget = 3 * time.Second

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

	// On the dashboard alone until now, which means no alert could be written
	// against them. commands_total above all: it is the denominator that turns
	// every other counter here into a rate.
	counter("commands_total", "Client commands the router handled.", snap.Counters["commands"])
	counter("newest_first_total", "Reads for the newest N the fast tier proved it could answer from its own reply. Each one is an archive round trip that did not happen.", snap.Counters["newest_first"])
	counter("live_hits_total", "Reads the live cache answered from memory.", snap.Counters["live_hits"])
	counter("live_misses_total", "Reads the live cache could take but did not hold, each asking for a fill.", snap.Counters["live_misses"])
	counter("live_waited_for_stream_total", "Reads the live cache held but sent to mongod, because a write this router acknowledged was not yet applied within router.live_cache.max_staleness.", snap.Counters["live_waited_for_stream"])
	counter("live_fills_total", "Entries the live cache filled from MongoDB.", snap.Counters["live_fills"])
	counter("live_unproven_total", "Fills of a tiered read the fast tier could not prove complete, so not held.", snap.Counters["live_unproven"])
	counter("live_dropped_total", "Entries the live cache dropped: evicted for space, or invalidated by a change it could not apply.", snap.Counters["live_dropped"])
	counter("live_refused_shapes_total", "Query shapes the live cache stopped holding because a document showed it could not answer them exactly.", snap.Counters["live_refused_shapes"])
	gauge("live_entries", "Entries the live cache holds now.", float64(snap.Counters["live_entries"]))
	gauge("live_bytes", "Bytes of documents the live cache holds now.", float64(snap.Counters["live_bytes"]))
	counter("merged_reads_total", "Reads the fast tier could not prove complete, answered by merging its answer with the archive's instead of waiting for mirroring.", snap.Counters["merged_reads"])
	counter("merge_fallbacks_total", "Merged reads the fast tier's oplog sent back to the freshness wait, because a document the archive returned had changed since the feeder's position.", snap.Counters["merge_fallbacks"])
	counter("point_reads_unwaited_total", "Reads by _id answered from the archive without the freshness wait, because the fast tier's oplog showed no delete of that document waiting to be mirrored.", snap.Counters["point_reads_unwaited"])
	counter("point_wait_no_window_total", "Reads by _id sent to the full freshness wait because the feeder's position or the fast tier's cluster time was not known.", snap.Counters["point_wait_no_window"])
	counter("point_wait_not_clear_total", "Reads by _id sent to the full freshness wait because the fast tier's oplog did not clear the document of a delete waiting to be mirrored.", snap.Counters["point_wait_not_clear"])
	counter("point_wait_archive_err_total", "Reads by _id sent to the full freshness wait because the archive's read failed or left a cursor open.", snap.Counters["point_wait_archive_err"])
	counter("point_reads_speculated_total", "Reads by _id whose archive read was started beside the fast tier's, because most recent point reads of the namespace missed.", snap.Counters["point_reads_speculated"])
	counter("point_spec_used_total", "Speculated point reads answered from the archive read started ahead, one round trip sooner.", snap.Counters["point_spec_used"])
	counter("merges_speculated_total", "Merged reads whose archive half was asked as the command arrived, because most recent sorted reads of the namespace were merged.", snap.Counters["merges_speculated"])
	counter("merges_spec_used_total", "Merged reads answered with the archive half asked ahead, one round trip sooner. The difference from merges_speculated_total is archive reads that did not need to happen.", snap.Counters["merges_spec_used"])
	counter("point_spec_wasted_total", "Speculated point reads whose early archive read was not used: the fast tier held the document, the archive did not, or the oplog check did not clear it. Each is one archive read that did not need to happen.", snap.Counters["point_spec_wasted"])
	counter("oplog_checks_in_memory_total", "Oplog checks of merged and unwaited reads answered from memory, with no round trip to the fast tier.", snap.Counters["oplog_checks_in_memory"])
	counter("oplog_checks_queried_total", "Oplog checks of merged and unwaited reads that read the fast tier's oplog.", snap.Counters["oplog_checks_queried"])
	counter("oplog_checks_tail_total", "Oplog checks the tracker answered up to where its stream had got, leaving only the entries after that to read from the oplog.", snap.Counters["oplog_checks_tail"])
	counter("oplog_checks_waited_total", "Oplog checks answered from the tracker's memory after waiting a few milliseconds for its stream to reach the window's end, rather than reading the rest from the fast tier's oplog.", snap.Counters["oplog_checks_waited"])
	counter("fam_claimed_total", "Sorted findAndModifys whose document the archive named inside the fast tier's claim, so it was neither held nor copied up.", snap.Counters["fam_claimed"])
	counter("warmed_for_txn_total", "Documents brought up so a transaction could run against the fast tier alone.", snap.Counters["warmed_for_txn"])
	counter("read_misses_total", "Point lookups that found the document only in the archive.", snap.Counters["read_misses"])
	counter("write_misses_total", "Writes that matched nothing on the fast tier and found the document in the archive.", snap.Counters["write_misses"])
	counter("copied_up_total", "Documents brought back from the archive into the fast tier.", snap.Counters["copied_up"])
	counter("incomplete_total", "Queries reaching past what the fast tier can prove it holds.", snap.Counters["incomplete"])
	counter("from_archive_total", "Reads answered from the archive because the fast tier could not.", snap.Counters["from_archive"])
	counter("refused_total", "Commands the router would not answer.", snap.Counters["refused"])
	counter("txn_replays_total", "Transactions asked to replay after documents were brought up for them.", snap.Counters["txn_replays"])
	counter("duplicate_keys_total", "Writes refused because they would duplicate a key held in the archive.", snap.Counters["duplicate_keys"])
	counter("change_streams_total", "Change streams opened over tiered data, on a collection, a database or the whole deployment. Tiering events are hidden from these; a consumer further behind than the notice TTL is shown an eviction as a delete and a copy-up as an insert, because the record naming them has expired.", snap.Counters["change_streams"])
	counter("events_hidden_total", "Tiering events removed from change stream batches. This is the sieve working; read it against change_streams_total, and a stream open with this flat while documents move is worth investigating.", snap.Counters["events_hidden"])
	counter("unauthorized_total", "Archive answers refused because the fast tier would not have served this client. Any value above zero is a client reading a collection it has no permission for.", snap.Counters["unauthorized"])
	counter("secondary_reads_total", "Reads that asked for a secondary and were sent to one. With primary_only_reads_total this says how much read load actually moved.", snap.Counters["secondary_reads"])
	counter("secondary_reads_moved_total", "Reads a secondary refused as neither primary nor secondary, which the primary then answered. Above zero means a member is in the set but not readable.", snap.Counters["secondary_reads_moved"])
	counter("failover_replays_total", "Commands re-sent after the fast tier changed primary. Each one is an election a client did not have to see.", snap.Counters["failover_replays"])
	counter("failover_siblings_closed_total", "Idle client connections closed because the member they were authenticated against stopped being primary, so a driver's retry could not be sent on one.", snap.Counters["failover_siblings_closed"])
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
	gauge("key_registry_rows",
		"Unique-key claims held on the fast tier. A claim is permanent on purpose, so this grows "+
			"with history and only `cline keys --prune` removes the ones nothing holds. Watch it "+
			"against the data it constrains rather than against a threshold.",
		float64(snap.KeyRegistryRows))

	// The one state in which the archive is not a superset of the fast tier,
	// published as a state rather than left to the feeder's event counter.
	//
	// The feeder counts each document it gives up, so an alert on that fires
	// when it happens and goes quiet fifteen minutes later. What it leaves out
	// is that the deployment is still in that condition, and since the router
	// began refusing the reads that would be short by those documents, that
	// condition is an application seeing errors. This is what stays above zero
	// until somebody resolves it.
	inv := r.InventorySnapshot()
	gauge("not_superset_namespaces",
		"Namespaces the archive is missing documents of, because it refused them and mirroring "+
			"gave up. Above zero, every read of those namespaces the fast tier cannot answer in "+
			"full is refused, and it stays above zero until the duplicate values are resolved.",
		float64(inv.NotSuperset))
	gauge("not_superset_documents",
		"Documents behind not_superset_namespaces, which is the list an operator works through.",
		float64(inv.NotSupersetDocuments))

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
		counter("cache_invalidated_total", "Held answers dropped because something wrote to a collection they depended on. This is the number that explains a hit rate of nothing on a write-heavy collection: every write empties the cache of everything that read it, which is correct and is also why the feature does nothing there. Read it against cache_hits_total before concluding the cache cannot learn.", int64(cs.Invalidated))
		counter("cache_spared_total", "Held answers a write to their collection left in place because it could not have changed them: an insert none of whose documents passes the answer's filter, or an update to no field the answer read. Against cache_invalidated_total, it says how much of the write traffic the cache now survives.", int64(cs.Spared))
		counter("cache_forgotten_total", "Watched queries a write made the cache forget, whether or not an answer was held for them. A query is held from its second sighting, so this is learning undone: on the staging shop it was 53,842 in five minutes while cache_invalidated_total said 335. A high value against cache_misses_total means writes, not repetition, are what keep the hit rate down.", int64(cs.Forgotten))
		fmt.Fprintf(b, "# HELP thermocline_cache_miss_reasons_total Cache misses by why: unknown is a query not being watched (never asked, forgotten by a write, or evicted at max_queries), empty is one watched with no answer held yet, expired is a held answer past its learned TTL.\n")
		fmt.Fprintf(b, "# TYPE thermocline_cache_miss_reasons_total counter\n")
		for _, m := range []struct {
			reason string
			n      uint64
		}{{"unknown", cs.MissUnknown}, {"empty", cs.MissEmpty}, {"expired", cs.MissExpired}} {
			fmt.Fprintf(b, "thermocline_cache_miss_reasons_total{reason=\"%s\"} %d\n", m.reason, m.n)
		}
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

	// The freshness barrier on its own, which the archive and miss classes
	// above include without saying how much of them it is.
	fw := snap.Metrics.FreshnessWait
	fmt.Fprintf(b, "# HELP thermocline_freshness_wait_seconds Time a read that fell through to the archive spent waiting for mirroring to reach what this router had acknowledged, before the archive was asked.\n")
	fmt.Fprintf(b, "# TYPE thermocline_freshness_wait_seconds histogram\n")
	var wcum int64
	for i, edge := range snap.Metrics.Buckets {
		if i < len(fw.Buckets) {
			wcum += fw.Buckets[i]
		}
		fmt.Fprintf(b, "thermocline_freshness_wait_seconds_bucket{le=\"%s\"} %d\n", strconv.FormatFloat(edge/1000, 'f', -1, 64), wcum)
	}
	if len(fw.Buckets) > len(snap.Metrics.Buckets) {
		wcum += fw.Buckets[len(snap.Metrics.Buckets)]
	}
	fmt.Fprintf(b, "thermocline_freshness_wait_seconds_bucket{le=\"+Inf\"} %d\n", wcum)
	fmt.Fprintf(b, "thermocline_freshness_wait_seconds_sum %s\n", strconv.FormatFloat(fw.TotalMS/1000, 'f', -1, 64))
	fmt.Fprintf(b, "thermocline_freshness_wait_seconds_count %d\n", fw.Count)

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

	// The other bar, and the one that actually decides.
	//
	// A fall-through read clears two: the feeder's published instant against
	// the instant the read began, and the feeder's published cluster time
	// against the newest operationTime this router has handed a client.
	// caughtUp's own comment says the second is the binding one on any system
	// with traffic, because a busy router always has a fresh operationTime to
	// clear while a feeder that has fallen behind does not.
	//
	// Only the first was exported, and it is the one docs/monitoring.md tells
	// operators to alert on. Measured on the 2026-09-18 store run: eleven stale
	// reads inside one second with mirroring_lag_seconds at 24ms three seconds
	// before and 14ms five seconds after. The barrier that fired was invisible
	// to every number on the dashboard, so stale_reads_total rose with nothing
	// to explain it.
	//
	// In seconds of cluster time, so it is comparable with the gauge above
	// rather than being a packed timestamp nobody can read. Zero is the healthy
	// value and the one to alert on, the same as its neighbour.
	// Always, when a feeder is configured, because an alert names it and a rule
	// whose metric is sometimes absent never fires. That is the failure
	// testalerts exists to catch, and it caught this one: rendering it only
	// when a reading happened to be cached meant a router serving no
	// fall-through reads exported nothing for it at all.
	//
	// Three states, each with an honest value rather than a convenient one.
	if r.cfg.FeederID != "" {
		behindHelp := "Seconds of cluster time the feeder's published position has not yet " +
			"covered, against the newest operationTime this router has handed a client. This is " +
			"the bar a fall-through read actually has to clear, and stale_reads_total rises when " +
			"it does not. Zero is healthy. +Inf means nothing has been mirrored at all, the same " +
			"as its neighbour above."
		seen := r.seenOpTime.Load()
		// From the record mirroringLag just read for the gauge above, not from
		// the freshness barrier's cache. That cache is refreshed only while a
		// read is waiting on it, so between fall-through reads it holds the
		// feeder where it was at the last one while seen keeps moving with
		// traffic: on 2026-10-13 a healthy feeder in the staging gate graphed
		// 6, 16, 26, then one more second every second up to 54, and the rule
		// below fires on that after two minutes on any router whose reads
		// rarely fall through. Nor is the cache refreshed here: a scrape that
		// could not reach the archive would leave "nothing is mirroring" in it
		// and make the next fall-through read answer stale.
		got, haveReading := mirroring.ts, mirroring.known
		switch {
		case seen == 0:
			// This router has forwarded no reply, so it holds no cluster time
			// to clear and the bar cannot fail. caughtUp says the same: an
			// untilTS of zero passes. Zero is the fact, not a stand-in for one.
			gauge("mirroring_cluster_time_behind_seconds", behindHelp, 0)
		case !haveReading || got.T == 0:
			gauge("mirroring_cluster_time_behind_seconds", behindHelp, math.Inf(1))
		default:
			// Through UnpackTS rather than shifting here, so this does not have
			// to know how a cluster time is packed.
			behind := float64(0)
			if seenT := checkpoint.UnpackTS(seen).T; seenT > got.T {
				behind = float64(seenT - got.T)
			}
			gauge("mirroring_cluster_time_behind_seconds", behindHelp, behind)
		}
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
		// In the archive's frame, because that is where the report was stamped.
		// A daemon on a machine whose clock ran behind this one looked like a
		// daemon that had stopped, and there is an alert rule on this.
		since := r.archiveNowFallback(ctx)
		for _, rep := range reports {
			fmt.Fprintf(b, "thermocline_daemon_seconds_since_pass{daemon=\"%s\"} %s\n",
				promEscape(rep.ID), strconv.FormatFloat(max(since.Sub(rep.At), 0).Seconds(), 'f', -1, 64))
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
			// Zero rather than absent when nothing is held, because this is the
			// series the alert rule is written against and an alert cannot be
			// written against a series that only exists once the thing has
			// happened: ThermoclineEvictionHeld would have had nothing to
			// evaluate until the moment it was needed. The same reason
			// mirroring lag reports +Inf instead of going missing.
			seconds := 0.0
			if paused {
				seconds = max(r.archiveNowFallback(ctx).Sub(rec.Since), 0).Seconds()
			}
			gauge("eviction_paused_seconds",
				"How long eviction has been held, zero when it is not. Alert on this being long rather than on it being set.",
				seconds)
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
			ts:         rec.TS,
			feederSkew: theirs, feederSkewKnown: theirsKnown,
		}
	}
	// The fallback, for a deployment with position_every set to zero. It
	// carries the feeder's offset too, so the subtraction is the same one in
	// the same frame rather than a raw one hiding under a clamp.
	rec, ok, err := r.checkpoints.Mark(ctx, r.cfg.FeederID)
	if err != nil || !ok {
		return mirroringReading{}
	}
	theirs, theirsKnown := rec.Skew()
	mine, mineKnown := r.archiveSkew(ctx)
	return mirroringReading{
		lag:        health.LagFrom(time.Now(), rec.EventAt, mine, theirs, mineKnown && theirsKnown),
		known:      true,
		feederSkew: theirs, feederSkewKnown: theirsKnown,
	}
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
	ts              bson.Timestamp // the position as a cluster time, when the feeder published one
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
