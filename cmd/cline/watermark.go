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
		if err := marks.Set(ctx, *ns, time.Time{}); err != nil {
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
		if err := marks.Set(ctx, *ns, from.UTC()); err != nil {
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
	return "complete for everything newer than " + m.From.Format(time.RFC3339)
}
