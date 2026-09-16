package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/router"
	"github.com/zelmario/thermocline/internal/watermark"
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
	countFor := fs.Duration("count-for", claimCheckBudget,
		"how long to spend counting both tiers to check the completeness claims; a claim not reached stands unchecked")
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

	// Not len(cfg.Feeder.Namespaces): empty tiers everything, and the count is
	// the first thing an operator reads. It said "0 tiered namespace(s)" for the
	// deployment that tiers the most.
	tiered := len(cfg.Feeder.Namespaces)
	all := tiered == 0
	if all {
		fmt.Println("feeder.namespaces is empty, which tiers everything except the system")
		fmt.Println("databases and this deployment's own bookkeeping.")
		if found, ferr := archiveNamespaces(ctx, cold); ferr == nil {
			tiered = len(found)
		}
	}
	fmt.Printf("Checking %d tiered namespace(s) across both tiers. Nothing will be written.\n\n", tiered)

	r := router.New(hot, cold, cfg.Router, log)
	if err := r.Survey(ctx); err != nil {
		// The pair itself is one a router refuses. Said as an error rather than
		// a warning because that is what it would be at a real start, and a
		// rollout script has to be able to stop here.
		return err
	}

	verifyClaims(ctx, hot, cold, cfg, *countFor)

	fmt.Print(`
Nothing above refuses a start. Read the warnings: most of them name something an
application would notice and no error would report, and the two worth acting on
before any eviction are a unique index the fast tier is missing and unique keys
nothing has claimed. See docs/adoption.md.
`)
	return nil
}

// verifyClaims counts both tiers for every tiered namespace that carries a
// completeness claim, and names the ones the numbers contradict.
//
// A claim is what makes a bounded read answerable from the fast tier alone, so
// one the fast tier no longer covers is every such read coming back short,
// silently, for as long as it stands. It is checked when it is declared and
// never again, and the ordinary way it stops being true has nothing to do with
// anybody declaring it: restore the fast tier from a backup and the claim,
// which lives on the archive, still describes the tier that backup replaced.
// The same goes for a collection dropped and recreated out of band, or a fast
// tier rebuilt from nothing.
//
// The counting is the same counting `cline watermark` does before recording a
// claim, so there is one definition of what bears a claim out. It is bounded,
// and a namespace the budget does not reach is reported as not reached rather
// than passed over: a claim not checked is a claim that stands.
func verifyClaims(ctx context.Context, hot, cold *mongo.Client, cfg config.Config, budget time.Duration) {
	marks, err := watermark.New(cold).All(ctx)
	if err != nil {
		fmt.Printf("\nthe completeness claims could not be read, so none of them was checked: %v\n", err)
		return
	}

	// Every recorded claim when the configuration names nothing, because empty
	// tiers everything and a claim exists only for a namespace something has
	// tiered. Reading the list instead checked no claim at all on such a
	// deployment, which is the one failure this function exists to catch:
	// a claim not checked is a claim that stands.
	claimed := make([]string, 0, len(marks))
	if len(cfg.Feeder.Namespaces) == 0 {
		for ns, m := range marks {
			if m.Claimed {
				claimed = append(claimed, ns)
			}
		}
	} else {
		for _, ns := range cfg.Feeder.Namespaces {
			if m, ok := marks[ns]; ok && m.Claimed {
				claimed = append(claimed, ns)
			}
		}
	}
	if len(claimed) == 0 {
		return
	}
	sort.Strings(claimed)

	fmt.Printf("\nChecking %d completeness claim(s) against what the two tiers hold.\n", len(claimed))
	deadline := time.Now().Add(budget)
	short, checked := 0, 0
	for _, ns := range claimed {
		left := time.Until(deadline)
		if left <= 0 {
			fmt.Printf("  %s: not reached inside %s, so this claim stands unchecked\n", ns, budget)
			continue
		}
		why, err := checkClaimFor(ctx, hot, cold, cfg, ns, marks[ns].From, left, false)
		if err != nil {
			fmt.Printf("  %s: %v\n", ns, err)
			continue
		}
		checked++
		if why != "" {
			short++
			fmt.Printf("  %s: %s\n", ns, why)
		}
	}
	switch {
	case short > 0:
		fmt.Printf("\n%d of %d claim(s) checked are short. Until that is put right, a read bounded\n"+
			"inside the claimed range is answered from the fast tier alone and returns fewer\n"+
			"documents than exist, with no error. Warm the namespace, or clear the claim with\n"+
			"cline watermark --ns <ns> --clear and let the warmer record it again.\n", short, checked)
	case checked > 0:
		fmt.Printf("Every claim checked is borne out by the counts.\n")
	}
}
