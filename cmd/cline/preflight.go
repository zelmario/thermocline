package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/router"
)

// Everything a starting router would say about a pair, without starting one.
//
// The findings a router produces at startup are the ones that decide whether a
// deployment is fit to tier: collections stranded on the archive, a database
// the fast tier has no trace of, a collation or validator that differs between
// the tiers, a unique index the fast tier is missing, accounts and roles the
// archive does not have, unclaimed unique keys on an adopted archive, and a
// version or FCV mismatch that refuses the start outright.
//
// Until this existed the only way to see them was to start a router against the
// cluster, and a starting router is not a reader: it builds the archive's
// indexes on the fast tier, creates the fast tier's views on the archive, and
// drops leftover scratch collections. Asking "is this pair suitable" therefore
// meant building indexes on it to find out, which is not a question anybody
// wants to ask of production that way.
//
// So this runs the same checks in the same order and writes nothing. It is the
// first thing to run against a cluster being considered, and the thing to run
// again after fixing what it named.
const preflightUsage = `Usage: cline preflight [--config FILE]

Report whether a pair is fit to tier, without writing to either tier.

Runs the checks a router runs when it starts -- versions and feature
compatibility, collections and databases the fast tier does not have, collation
and validator drift, unique indexes the fast tier is missing, accounts and roles
the archive lacks, write concerns, and whether anything has ever claimed the
unique keys an adopted archive carries -- and prints what they find.

Nothing is created, built or removed. A real router start additionally builds
the archive's indexes on the fast tier, mirrors the fast tier's views to the
archive, and clears leftover scratch collections; none of that happens here.

Exits non-zero when the pair is one a router would refuse to serve, so a rollout
script can gate on it. Findings that a router only warns about are printed and
do not change the exit status: they are for a person to read.
`

func runPreflight(args []string) error {
	fs := flag.NewFlagSet("preflight", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	logPath := fs.String("log-file", "", "append findings here instead of stderr, overriding log.file")
	verbose := fs.Bool("v", false, "log every decision the checks make")
	fs.Usage = func() { fmt.Print(preflightUsage); fs.PrintDefaults() }
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	hot, err := connect(ctx, cfg.Hot.URI, "hot")
	if err != nil {
		return fmt.Errorf("the fast tier could not be reached, which a router would refuse to start without: %w", err)
	}
	defer disconnect(hot, log)

	cold, err := connect(ctx, cfg.Cold.URI, "cold")
	if err != nil {
		return fmt.Errorf("the archive could not be reached, which a router would refuse to start without: %w", err)
	}
	defer disconnect(cold, log)
	warnColdShape(log)
	warnIfArchiveReadPreferenceIgnored(cfg.Cold.URI, log)

	fmt.Printf("Checking %d tiered namespace(s) across both tiers. Nothing will be written.\n\n",
		len(cfg.Feeder.Namespaces))

	r := router.New(hot, cold, cfg.Router, log)
	if err := r.Survey(ctx); err != nil {
		// The pair itself is one a router refuses. Said as an error rather than
		// a warning because that is what it would be at a real start, and a
		// rollout script has to be able to stop here.
		return err
	}

	fmt.Print(`
Nothing above refuses a start. Read the warnings: most of them name something an
application would notice and no error would report, and the two worth acting on
before any eviction are a unique index the fast tier is missing and unique keys
nothing has claimed. See docs/adoption.md.
`)
	return nil
}
