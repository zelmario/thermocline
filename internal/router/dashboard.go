package router

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// The router already knew everything an operator wants to see and told nobody:
// the counters went to a log line once a minute, and the one number that
// actually justifies the design, the difference between a fast tier hit and an
// archive read, was not recorded at all.
//
// This serves it instead. One page, no dependencies, no metrics stack to stand
// up first. If a Prometheus is available the same JSON is trivial to scrape;
// the point is that nothing has to be.

//go:embed dashboard.html
var dashboardHTML []byte

// Snapshot is everything the dashboard draws.
type Snapshot struct {
	NowUnix int64 `json:"now"`
	UptimeS int64 `json:"uptime_s"`
	// KeyRegistryRows is how many unique-key claims the fast tier holds. See
	// Inventory.KeyRegistryRows for why it is reported and not alerted on.
	KeyRegistryRows int64            `json:"key_registry_rows"`
	Listen          string           `json:"listen"`
	FastTier        string           `json:"fast_tier"`
	Completenes     string           `json:"completeness"`
	FeederID        string           `json:"feeder_id"`
	ArchiveWait     string           `json:"archive_wait"`
	Counters        map[string]int64 `json:"counters"`
	Metrics         MetricsSnapshot  `json:"metrics"`
	Inventory       Inventory        `json:"inventory"`
	Cache           CacheSnapshot    `json:"cache"`
}

// CacheSnapshot is what the dashboard shows about the learned cache: whether it
// is serving, how well, and which queries it thinks are worth caching.
//
// The advice is there whether or not the cache is on, because it is the reason
// to turn it on. An operator looking at this page is deciding, and the numbers
// they need for that decision are the ones they cannot get by turning it on and
// hoping.
type CacheSnapshot struct {
	Enabled bool `json:"enabled"`
	// Watching is whether the change stream this router follows on the fast
	// tier is open. It drops answers another writer made stale, and it is also
	// how this router hears that a collection was dropped out of band, so
	// false means both are weaker: the learned TTL is the cache's only bound,
	// and a collection that has gone is noticed at the next catalog listing.
	// It is reported under the cache because the cache is what it was built
	// for; it is not the cache's any more, and Enabled is what says whether
	// the cache is on.
	Watching bool    `json:"watching"`
	Shapes   int     `json:"shapes"`
	Hits     uint64  `json:"hits"`
	Misses   uint64  `json:"misses"`
	HitRate  float64 `json:"hit_rate"`
	// Entries and Bytes are what the hit rate cannot explain: a cache at its
	// max_bytes is evicting answers it learned, and one whose entries sit at
	// max_queries is forgetting a query on every new one.
	Entries  int    `json:"entries"`
	Bytes    int    `json:"bytes"`
	Rejected uint64 `json:"rejected"`
	// Invalidated is held answers dropped because something wrote to a
	// collection they depended on. It is the number that explains a hit rate of
	// nothing: a write-heavy collection empties its own cache.
	Invalidated uint64 `json:"invalidated"`
	// Spared is held answers a write to their collection left in place,
	// because it could not have changed them.
	Spared uint64 `json:"spared"`
	// Forgotten is watched queries a write made the cache forget, held or
	// not. Against Invalidated it says how much learning writes are undoing.
	Forgotten uint64 `json:"forgotten"`
	// Why the misses missed. See cache.Cache.
	MissUnknown uint64        `json:"miss_unknown"`
	MissEmpty   uint64        `json:"miss_empty"`
	MissExpired uint64        `json:"miss_expired"`
	Advice      []CacheAdvice `json:"advice"`
}

// CacheAdvice is one row: a query, how often it is asked, how long its answer
// holds, and what that justifies caching it for.
type CacheAdvice struct {
	Shape      string `json:"shape"`
	NS         string `json:"ns"`
	Executions uint64 `json:"executions"`
	Params     int    `json:"params"`
	StableMS   int64  `json:"stable_ms"`
	SuggestMS  int64  `json:"suggest_ms"`
}

// cacheSnapshot reads what the cache and the advisor know.
func (r *Router) cacheSnapshot() CacheSnapshot {
	out := CacheSnapshot{Enabled: r.cache != nil, Watching: r.watching.Load()}
	if r.cache != nil {
		cs := r.cache.Stats()
		out.Hits, out.Misses = cs.Hits, cs.Misses
		out.Entries, out.Bytes, out.Rejected = cs.Entries, cs.Bytes, cs.Rejected
		out.Invalidated, out.Spared, out.Forgotten = cs.Invalidated, cs.Spared, cs.Forgotten
		out.MissUnknown, out.MissEmpty, out.MissExpired = cs.MissUnknown, cs.MissEmpty, cs.MissExpired
		if total := out.Hits + out.Misses; total > 0 {
			out.HitRate = float64(out.Hits) / float64(total)
		}
	}
	if r.advisor == nil {
		return out
	}
	out.Shapes = r.advisor.Shapes()
	for _, a := range r.advisor.Report(dashboardAdviceRows) {
		out.Advice = append(out.Advice, CacheAdvice{
			Shape:      a.Shape,
			NS:         a.NS,
			Executions: a.Executions,
			Params:     a.Params,
			StableMS:   a.Stable.Milliseconds(),
			SuggestMS:  a.Suggested.Milliseconds(),
		})
	}
	return out
}

// dashboardAdviceRows is how many suggestions the page shows. Enough to act on,
// few enough to read without scrolling past the rest of the page.
const dashboardAdviceRows = 12

// SnapshotAll gathers everything the dashboard needs in one read.
func (r *Router) SnapshotAll() Snapshot {
	return Snapshot{
		NowUnix: time.Now().Unix(),
		UptimeS: int64(time.Since(r.started).Seconds()),
		// From the inventory rather than counted here: the snapshot is served
		// per scrape and the registry is the largest collection this system
		// writes.
		KeyRegistryRows: r.InventorySnapshot().KeyRegistryRows,
		Listen:          r.Addr(),
		FastTier:        r.currentPrimary(),
		Completenes:     string(r.cfg.Completeness),
		Cache:           r.cacheSnapshot(),
		FeederID:        r.cfg.FeederID,
		ArchiveWait:     r.cfg.ArchiveWait.String(),
		Counters: map[string]int64{
			"commands":                 r.Stats.Commands.Load(),
			"read_misses":              r.Stats.ReadMisses.Load(),
			"write_misses":             r.Stats.WriteMisses.Load(),
			"copied_up":                r.Stats.CopiedUp.Load(),
			"incomplete":               r.Stats.Incomplete.Load(),
			"newest_first":             r.Stats.NewestFirst.Load(),
			"point_reads_unwaited":     r.Stats.PointReadsUnwaited.Load(),
			"superseded_cursors":       r.Stats.SupersededCursors.Load(),
			"panics":                   r.Stats.Panics.Load(),
			"refused":                  r.Stats.Refused.Load(),
			"from_archive":             r.Stats.Archived.Load(),
			"txn_replays":              r.Stats.TxnReplays.Load(),
			"warmed_for_txn":           r.Stats.WarmedForTxn.Load(),
			"duplicate_keys":           r.Stats.DuplicateKeys.Load(),
			"change_streams":           r.Stats.ChangeStreams.Load(),
			"unauthorized":             r.Stats.Unauthorized.Load(),
			"primary_only_reads":       r.Stats.PrimaryOnlyReads.Load(),
			"secondary_reads":          r.Stats.SecondaryReads.Load(),
			"secondary_reads_moved":    r.Stats.SecondaryReadsMoved.Load(),
			"failover_replays":         r.Stats.FailoverReplays.Load(),
			"failover_siblings_closed": r.Stats.FailoverSiblingsClosed.Load(),
			"unreadable":               r.Stats.Unreadable.Load(),
			"events_hidden":            r.Stats.EventsHidden.Load(),
			"stale_reads":              r.Stats.StaleReads.Load(),
			"claim_grace_reads":        r.Stats.ClaimGraceReads.Load(),
			"clients_open":             r.Stats.ClientsOpen.Load(),
			"clients_total":            r.Stats.ClientsTotal.Load(),
		},
		Metrics:   r.Metrics.Snapshot(),
		Inventory: r.InventorySnapshot(),
	}
}

// ServeDashboard runs the dashboard until ctx is done.
//
// It binds where it is told and nowhere else. This page names collections and
// reports traffic, so it belongs on an operator network rather than on the
// address the application already reaches.
func (r *Router) ServeDashboard(ctx context.Context, addr string) error {
	r.goSafely("refreshing the dashboard's inventory", func() { r.refreshInventory(ctx) })

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/" {
			http.NotFound(w, req)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(dashboardHTML)
	})
	// Prometheus scrapes this. It is on the dashboard's address rather than the
	// router's, for the same reason the dashboard is: neither belongs on the
	// address the application dials.
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
		// Bounded, because two of the gauges read the archive and the archive
		// being unreachable is exactly when this endpoint has to answer.
		// Without it the driver waits out server selection, thirty seconds by
		// default, which is longer than Prometheus's own ten second scrape
		// timeout: the scrape fails and *every* metric is lost, including
		// panics_total and stale_reads_total, and including the +Inf that
		// mirroring_lag_seconds exists to report for this very state.
		//
		// Short on purpose. Every reading that cannot be had in this budget has
		// an "unknown" rendering already, so the export degrades to the honest
		// answer rather than to no answer at all.
		ctx, cancel := context.WithTimeout(req.Context(), metricsBudget)
		defer cancel()
		var b strings.Builder
		r.writeMetrics(ctx, &b)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, b.String())
	})
	mux.HandleFunc("/api/snapshot", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		enc := json.NewEncoder(w)
		enc.SetIndent("", " ")
		if err := enc.Encode(r.SnapshotAll()); err != nil {
			r.log.Warn("dashboard snapshot failed", "err", err)
		}
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("dashboard listen on %s: %w", addr, err)
	}
	r.log.Info("dashboard listening", "addr", ln.Addr().String())
	warnDashboardReach(r.log, addr)

	r.goSafely("shutting the dashboard down", func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	})

	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// warnDashboardReach says so when the dashboard is reachable from anywhere.
//
// The page has no authentication of its own, which is written down, and the
// advice that goes with it is to bind it to an operator network rather than to
// the address the application dials. Nothing checked, and the difference between
// following that advice and not is one character in a config file.
//
// What is on the page is not a secret in the cryptographic sense and is plenty
// in the ordinary one: every collection this deployment tiers, by name, how much
// of each is hot, the commands per second by how they were served, and the
// refusals. `/api/snapshot` is the same thing as JSON.
//
// A wildcard bind only. An explicit address is somebody's decision and gets no
// line, because a warning on a deliberate choice is a warning nobody reads.
func warnDashboardReach(log *slog.Logger, addr string) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
	default:
		return
	}
	log.Warn("the dashboard is bound to every interface and has no authentication",
		"addr", addr,
		"meaning", "anyone who can reach this port gets every collection this deployment tiers "+
			"by name, how much of each is hot, and the traffic against them, as a page and as "+
			"JSON at /api/snapshot",
		"action", "bind it to an operator network or to 127.0.0.1, or put it behind something "+
			"that authenticates; router.dashboard is deliberately a separate address from "+
			"router.listen for this reason",
		"why", "the page names collections and reports traffic, and it belongs on an operator "+
			"network rather than wherever the application already connects")
}
