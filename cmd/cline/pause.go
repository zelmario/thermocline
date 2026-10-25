package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
)

// runPause sets or clears the operator's hold on eviction.
//
// It is deliberately its own command rather than a flag on the evictor. The hold
// has to outlive the process that set it, and be settable while no evictor is
// running at all, which is exactly the situation during a restore.
func runPause(args []string, on bool) error {
	name := "resume"
	if on {
		name = "pause"
	}
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	reason := fs.String("reason", "", "why eviction is being held, recorded with the hold")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log, closeLog, err := setupLogging("", cfg, false)
	if err != nil {
		return err
	}
	defer closeLog()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cold, err := connect(ctx, cfg.Cold.URI, "cold")
	if err != nil {
		return err
	}
	defer disconnect(cold, log)

	if !on {
		if err := meta.ResumeEviction(ctx, cold); err != nil {
			return fatal(log, "resume", err)
		}
		log.Info("eviction resumed")
		return nil
	}
	if err := meta.PauseEviction(ctx, cold, *reason); err != nil {
		return fatal(log, "pause", err)
	}
	// The hold stops sweeps that start from now on. One already running reads
	// it before its next batch, and until it has, documents are still leaving
	// the fast tier. The operator's next step is usually restoring the archive,
	// so "paused" is said only once no sweep is in progress: the evictor holds
	// its lease for the whole of a sweep, and taking it here waits that out.
	_, release, err := holdEvictorLease(ctx, cold, cfg.Evictor.FeederID)
	if err != nil {
		return fatal(log, "pause", fmt.Errorf("the hold is recorded, but a sweep may still be running: %w", err))
	}
	release()
	rec, _, _ := meta.EvictionPaused(ctx, cold)
	log.Warn("eviction paused, the evictor will remove nothing",
		"since", rec.Since, "reason", rec.Reason,
		"note", "a hold that is forgotten fills the fast tier; thermocline_eviction_paused reports it")
	return nil
}
