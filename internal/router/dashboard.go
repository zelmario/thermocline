package router

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
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
	NowUnix     int64            `json:"now"`
	UptimeS     int64            `json:"uptime_s"`
	Listen      string           `json:"listen"`
	FastTier    string           `json:"fast_tier"`
	Completenes string           `json:"completeness"`
	FeederID    string           `json:"feeder_id"`
	ArchiveWait string           `json:"archive_wait"`
	Counters    map[string]int64 `json:"counters"`
	Metrics     MetricsSnapshot  `json:"metrics"`
	Inventory   Inventory        `json:"inventory"`
}

// SnapshotAll gathers everything the dashboard needs in one read.
func (r *Router) SnapshotAll() Snapshot {
	return Snapshot{
		NowUnix:     time.Now().Unix(),
		UptimeS:     int64(time.Since(r.started).Seconds()),
		Listen:      r.Addr(),
		FastTier:    r.currentPrimary(),
		Completenes: string(r.cfg.Completeness),
		FeederID:    r.cfg.FeederID,
		ArchiveWait: r.cfg.ArchiveWait.String(),
		Counters: map[string]int64{
			"commands":       r.Stats.Commands.Load(),
			"read_misses":    r.Stats.ReadMisses.Load(),
			"write_misses":   r.Stats.WriteMisses.Load(),
			"copied_up":      r.Stats.CopiedUp.Load(),
			"incomplete":     r.Stats.Incomplete.Load(),
			"refused":        r.Stats.Refused.Load(),
			"from_archive":   r.Stats.Archived.Load(),
			"txn_replays":    r.Stats.TxnReplays.Load(),
			"warmed_for_txn": r.Stats.WarmedForTxn.Load(),
			"duplicate_keys": r.Stats.DuplicateKeys.Load(),
			"stale_reads":    r.Stats.StaleReads.Load(),
			"clients_open":   r.Stats.ClientsOpen.Load(),
			"clients_total":  r.Stats.ClientsTotal.Load(),
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
	go r.refreshInventory(ctx)

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

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
