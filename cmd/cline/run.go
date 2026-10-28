package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/evictor"
	"github.com/zelmario/thermocline/internal/feeder"
	"github.com/zelmario/thermocline/internal/health"
	"github.com/zelmario/thermocline/internal/pairing"
	"github.com/zelmario/thermocline/internal/router"
	"github.com/zelmario/thermocline/internal/tierver"
	"github.com/zelmario/thermocline/internal/warmer"
)

// runAll is cline run: every daemon in one process, started in the order
// docs/adoption.md gives for running them apart.
//
// Four services and a timer, and an order between them that matters, were the
// hardest part of trying this on a cluster: start the feeder and wait for it,
// then the router, then claim the unique keys, then the evictor, and the
// warmer on a timer. That order is what this does, so one unit and one
// command replace the five.
//
// The daemons are the same code the separate subcommands run, and those stay:
// a deployment with routers on several hosts runs cline router on the others.
// A second cline run is safe too. Its feeder and evictor find the first one's
// lease and wait, retrying, and its router serves.
//
// A router that stops ends the process, because a process that is up and not
// serving is worse than one systemd restarts. The feeder, evictor and warmer
// are restarted here after restartAfter, which is what Restart=always with
// RestartSec=10s did for each of them as services.
func runAll(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	logPath := fs.String("log-file", "", "append logs here instead of stderr, overriding log.file")
	verbose := fs.Bool("v", false, "log every batch and routing decision")
	evictEvery := fs.Duration("evict-every", 5*time.Minute, "pause between eviction sweeps")
	warmEvery := fs.Duration("warm-every", time.Hour, "pause between warms; 0 warms once, at start")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log, closeLog, err := setupLogging(*logPath, cfg, *verbose)
	if err != nil {
		return err
	}
	defer closeLog()
	warnAboutWindows(log, cfg)

	sig, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(sig)
	defer cancel()

	hot, err := connect(ctx, cfg.Hot.URI, "hot")
	if err != nil {
		return err
	}
	defer disconnect(hot, log)
	primary := router.NewPrimaryWatch()
	cold, err := connect(ctx, cfg.Cold.URI, "cold", primary.Monitor())
	if err != nil {
		return err
	}
	defer disconnect(cold, log)
	warnColdShape(log)
	warnIfArchiveReadPreferenceIgnored(cfg.Cold.URI, log)
	if err := tierver.Check(ctx, hot, cold, cfg.AllowVersionSkew, log); err != nil {
		return err
	}
	if err := pairing.Check(ctx, hot, cold, log); err != nil {
		return err
	}
	log.Info("preflight passed: both tiers are replica sets on the same release, and paired")

	var wg sync.WaitGroup
	defer wg.Wait()
	daemon := func(name string, run func(context.Context, *slog.Logger) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keepRunning(ctx, log.With("component", name), name, run)
		}()
	}

	daemon("feeder", func(ctx context.Context, log *slog.Logger) error {
		f := feeder.New(hot, cold, cfg.Feeder, log)
		if cfg.Feeder.StreamFrom == "secondary" {
			stream, err := connect(ctx, cfg.Hot.URI, "hot", secondaryPreferred, appSuffix("stream"))
			if err != nil {
				return err
			}
			defer disconnect(stream, log)
			f.WithStream(stream)
		}
		return f.Run(ctx)
	})
	if !waitForMirroring(ctx, hot, cold, cfg.Feeder.ID, log) {
		return nil
	}

	rlog := log.With("component", "router")
	r := router.New(hot, cold, cfg.Router, rlog).WatchArchivePrimary(primary)
	if cfg.Router.LiveCache.Enabled && cfg.Router.LiveCache.StreamFrom == "secondary" {
		stream, err := connect(ctx, cfg.Hot.URI, "hot", secondaryPreferred, appSuffix("live"))
		if err != nil {
			return err
		}
		defer disconnect(stream, log)
		r.WithLiveStream(stream)
	}
	go reportRouterStats(ctx, r, rlog)
	if cfg.Router.Dashboard != "" {
		go func() {
			if derr := r.ServeDashboard(ctx, cfg.Router.Dashboard); derr != nil {
				rlog.Error("dashboard stopped", "err", derr)
			}
		}()
	}
	if cfg.Router.Pprof != "" {
		go servePprof(ctx, cfg.Router.Pprof, rlog)
	}
	listening := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		listening <- r.Listen(ctx)
		cancel()
	}()

	// The key walk asks the router which unique indexes it enforces, and on an
	// adoption the router builds them on the fast tier before it listens. Give
	// it that long, and stop here if it never got there.
	for r.Addr() == "" {
		select {
		case err := <-listening:
			return routerStopped(err, log)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if unclaimed := r.UnclaimedUniqueNamespaces(ctx); len(unclaimed) > 0 {
		log.Info("claiming the unique keys the archive already holds, before anything is evicted",
			"collections", unclaimed)
		claimed, conflicts, err := r.RebuildKeys(ctx, unclaimed)
		if err != nil {
			log.Error("could not claim the unique keys, so the evictor is not started", "err", err,
				"action", "cline keys --rebuild, then restart")
			return routerStopped(<-listening, log)
		}
		log.Info("unique keys claimed", "claimed", claimed, "duplicates", conflicts)
	}

	if len(cfg.Evictor.Rules) > 0 {
		daemon("evictor", func(ctx context.Context, log *slog.Logger) error {
			return evictor.New(hot, cold, cfg.Evictor, false, log).Run(ctx, *evictEvery)
		})
	}
	if len(cfg.Warmer.Rules) > 0 {
		daemon("warmer", func(ctx context.Context, log *slog.Logger) error {
			w := warmer.New(hot, cold, cfg.Warmer, cfg.Feeder.UnannouncedDeletes, cfg.Feeder.ID, false, log)
			stats, err := w.Run(ctx)
			log.Info("warm complete", stats.LogArgs()...)
			if err != nil {
				return err
			}
			if *warmEvery <= 0 {
				<-ctx.Done()
				return nil
			}
			log.Info("next warm", "in", *warmEvery)
			select {
			case <-ctx.Done():
			case <-time.After(*warmEvery):
			}
			return nil
		})
	}
	log.Info("running", "listen", cfg.Router.Listen, "collections", len(cfg.Feeder.Namespaces),
		"evictor", len(cfg.Evictor.Rules) > 0, "warmer", len(cfg.Warmer.Rules) > 0)
	return routerStopped(<-listening, log)
}

// restartAfter is how long a stopped daemon waits before it is started again.
const restartAfter = 10 * time.Second

// keepRunning runs one daemon until ctx ends, starting it again after
// restartAfter whenever it returns.
func keepRunning(ctx context.Context, log *slog.Logger, name string, run func(context.Context, *slog.Logger) error) {
	for {
		err := run(ctx, log)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Error(name+" stopped", "err", err, "restart_in", restartAfter)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(restartAfter):
		}
	}
}

// waitForMirroring holds the router back until the feeder is caught up: under
// two seconds behind, three readings in a row, by the same reading the
// evictor's lag gate takes. A router opened on a feeder working through a
// backlog sends every read of a recent write to wait for it, and on the
// staging adoption that refused 2,304 of them. It reports false when ctx ends
// first.
func waitForMirroring(ctx context.Context, hot, cold *mongo.Client, feederID string, log *slog.Logger) bool {
	check := health.New(hot, cold, feederID)
	start, said, calm := time.Now(), time.Now(), 0
	for {
		r := check.Sample(ctx)
		if !r.IsMissingSignal("feeder lag") && r.FeederLag < 2*time.Second {
			calm++
		} else {
			calm = 0
		}
		if calm >= 3 {
			log.Info("the feeder is caught up", "after", time.Since(start).Round(time.Second))
			return true
		}
		if time.Since(said) >= 30*time.Second {
			said = time.Now()
			lag := "unknown"
			if !r.IsMissingSignal("feeder lag") {
				lag = r.FeederLag.Round(time.Second).String()
			}
			log.Info("waiting for the feeder to catch up before the router opens", "behind", lag,
				"waited", time.Since(start).Round(time.Second))
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Second):
		}
	}
}

// routerStopped is the process's exit: nil when it was asked to stop.
func routerStopped(err error, log *slog.Logger) error {
	if err != nil {
		return fatal(log, "router", err)
	}
	return nil
}
