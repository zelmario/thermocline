package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/watermark"
)

// runWatermark inspects and edits the completeness claims.
//
// Two situations need this and neither had an answer before. Recovering from a
// fast tier rebuild meant deleting from _thermocline.watermarks by hand, which
// was written down nowhere. And a fast tier that genuinely holds all of history,
// because it is the original cluster rather than a new empty one, had no way to
// say so now that an absent claim means nothing is guaranteed.
func runWatermark(args []string) error {
	fs := flag.NewFlagSet("watermark", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	ns := fs.String("ns", "", "namespace to act on, as db.collection")
	clear := fs.Bool("clear", false, "withdraw the claim for --ns, so nothing is guaranteed until it is warmed")
	completeAll := fs.Bool("complete-all", false,
		"declare --ns complete for all of history, for a fast tier that is the original cluster")
	completeFrom := fs.String("complete-from", "",
		"declare --ns complete for everything newer than this RFC3339 instant")
	force := fs.Bool("force", false,
		"record a claim the two tiers' counts contradict, for a warm that is still running")
	countFor := fs.Duration("count-for", claimCheckBudget,
		"how long to spend counting both tiers before recording the claim unchecked")
	logPath := fs.String("log-file", "", "append logs here instead of stderr, overriding log.file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	set := 0
	for _, b := range []bool{*clear, *completeAll, *completeFrom != ""} {
		if b {
			set++
		}
	}
	if set > 1 {
		return errors.New("--clear, --complete-all and --complete-from do different things; give one")
	}
	if set == 1 && *ns == "" {
		return errors.New("--ns is required when changing a watermark")
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log, closeLog, err := setupLogging(*logPath, cfg, false)
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

	marks := watermark.New(cold)

	// declaring is the only path that needs the fast tier: a claim is a
	// statement about what it holds, and the command used not to look.
	declaring := *completeAll || *completeFrom != ""
	var hot *mongo.Client
	if declaring {
		hot, err = connect(ctx, cfg.Hot.URI, "hot")
		if err != nil {
			return err
		}
		defer disconnect(hot, log)
	}

	// verify counts both tiers and refuses a claim they contradict. --force
	// turns that into a warning, because an operator watching a warm fill knows
	// something the counts do not.
	verify := func(from time.Time) error {
		why, err := checkClaim(ctx, hot, cold, cfg, *ns, from, *countFor)
		if err != nil {
			return err
		}
		if why == "" {
			return nil
		}
		if !*force {
			return errors.New(why)
		}
		fmt.Println("warning: recording a claim the counts contradict, because --force was given")
		fmt.Println(why)
		return nil
	}

	switch {
	case *clear:
		existed, err := marks.Clear(ctx, *ns)
		if err != nil {
			return err
		}
		if !existed {
			fmt.Printf("%s had no claim to withdraw\n", *ns)
			return nil
		}
		fmt.Printf("withdrew the claim for %s\n", *ns)
		fmt.Println("range queries now fall through to the archive until the warmer refills the fast tier")
		return nil

	case *completeAll:
		if err := verify(time.Time{}); err != nil {
			return err
		}
		if err := marks.Set(ctx, *ns, time.Time{}, cfg.Router.TieredBy[*ns]); err != nil {
			return err
		}
		fmt.Printf("%s is declared complete for all of history\n", *ns)
		fmt.Println("only correct if the fast tier holds every document, which is true when it is the original cluster")
		return nil

	case *completeFrom != "":
		from, err := time.Parse(time.RFC3339, *completeFrom)
		if err != nil {
			return fmt.Errorf("--complete-from must be RFC3339, like 2026-08-14T15:00:00Z: %w", err)
		}
		if err := verify(from.UTC()); err != nil {
			return err
		}
		if err := marks.Set(ctx, *ns, from.UTC(), cfg.Router.TieredBy[*ns]); err != nil {
			return err
		}
		fmt.Printf("%s is declared complete for everything newer than %s\n",
			*ns, from.UTC().Format(time.RFC3339))
		return nil
	}

	return reportWatermarks(ctx, marks, *ns)
}

// reportWatermarks prints what is claimed, which is the first thing to look at
// when the router starts refusing queries.
func reportWatermarks(ctx context.Context, marks *watermark.Store, only string) error {
	all, err := marks.All(ctx)
	if err != nil {
		return err
	}

	if only != "" {
		mark, ok := all[only]
		if !ok {
			fmt.Printf("%s claims nothing, so range queries reaching into it are refused\n", only)
			return nil
		}
		fmt.Printf("%s %s\n", only, describe(mark))
		return nil
	}

	if len(all) == 0 {
		fmt.Println("no completeness claims recorded")
		fmt.Println("range queries are refused until the warmer runs, or a claim is declared here")
		return nil
	}

	names := make([]string, 0, len(all))
	width := 0
	for ns := range all {
		names = append(names, ns)
		if len(ns) > width {
			width = len(ns)
		}
	}
	sort.Strings(names)
	for _, ns := range names {
		fmt.Printf("%-*s  %s\n", width, ns, describe(all[ns]))
	}
	return nil
}

func describe(m watermark.Mark) string {
	if m.From.IsZero() {
		return "complete for all of history"
	}
	// To the nanosecond the claim was stored with, not to the second. A boundary
	// printed in whole seconds is up to a second older than the claim, and
	// anybody checking coverage from it counts documents the claim does not
	// cover: testwarm read 51 archived documents in a range whose claim covered
	// 50, and flagged the fast tier as short when the claim was exact.
	return "complete for everything newer than " + m.From.UTC().Format(time.RFC3339Nano)
}
