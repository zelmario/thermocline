package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/feeder"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/router"
	"github.com/zelmario/thermocline/internal/watermark"
)

// Everything a start would say about a pair, without starting anything.
//
// The findings a router produces at startup are the ones that decide whether a
// deployment is fit to tier: collections stranded on the archive, a database
// the fast tier has no trace of, a collation or validator that differs between
// the tiers, a unique index the fast tier is missing, accounts and roles the
// archive has and the fast tier does not, unclaimed unique keys on an adopted archive, and a
// version or FCV mismatch that refuses the start outright.
//
// And the feeder's own refusal, which was missing until it was noticed that the
// closing line here said "Nothing above refuses a start" for a deployment whose
// feeder would not start at all: a capped collection named in feeder.namespaces
// stops it dead. Preflight is step 0 of docs/adoption.md, so the refusal it
// missed was met several steps later.
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
the archive has and the fast tier does not, write concerns, and whether anything has ever claimed the
unique keys an adopted archive carries -- and the feeder's check that every
namespace it is told to mirror is a kind that can be mirrored at all. Prints
what they find.

Nothing is created, built or removed. A real router start additionally builds
the archive's indexes on the fast tier, mirrors the fast tier's views to the
archive, and clears leftover scratch collections; none of that happens here.

Exits non-zero when the pair is one a router would refuse to serve, or one whose
feeder would refuse to mirror, so a rollout script can gate on it. Findings that a router only warns about are printed and
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

	r := router.New(hot, cold, cfg.Router, log).Reporting()
	if err := r.Survey(ctx); err != nil {
		// The pair itself is one a router refuses. Said as an error rather than
		// a warning because that is what it would be at a real start, and a
		// rollout script has to be able to stop here.
		return err
	}

	// The feeder's own refusal, which this did not run and the closing line
	// below claimed it had. A capped collection named in feeder.namespaces
	// stops the feeder dead, and an operator following docs/adoption.md step 0
	// read "Nothing above refuses a start" and met it several steps later.
	// Returned rather than warned, for the same reason Survey's errors are: a
	// rollout script has to be able to stop here.
	if err := feeder.CheckCollectionKinds(ctx, hot, cold, cfg.Feeder, log); err != nil {
		return err
	}

	verifyClaims(ctx, hot, cold, cfg, *countFor)
	findReservedFields(ctx, hot, cfg, *countFor)
	listUnmirrored(ctx, cold)

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
// listUnmirrored names the documents the archive refused and the feeder gave up
// on, which is the list an operator has to work through and had nowhere to read.
//
// The router's refusal says how many there are and used to name cline keys
// --rebuild, as did the feeder's two lines, the alert summary, the limitations
// entry and backfill's own paragraph: five places, all wrong the same way. That
// command reads the fast tier's unique indexes, and in the shape this state
// arrives in, an adoption whose archive carries an index the fast tier's copy
// does not, it answers "no unique indexes on any tiered collection, so there is
// nothing to enforce". Measured, in exactly the state they pointed it at.
// So the list is printed here, where somebody asking what is wrong with this
// deployment will see it: the namespace, the document, and what the archive
// said.
func listUnmirrored(ctx context.Context, cold *mongo.Client) {
	rows, err := meta.AllUnmirrored(ctx, cold)
	if err != nil {
		fmt.Printf("\nthe record of documents the archive refused could not be read: %v\n", err)
		return
	}
	if len(rows) == 0 {
		return
	}
	fmt.Printf("\n%d document(s) the archive refused, which mirroring has given up on:\n", len(rows))
	for _, r := range rows {
		fmt.Printf("  %-34s %s\n", r.NS, firstLine(r.Why))
	}
	fmt.Print(`
While these stand, the archive is not a superset of the fast tier for those
namespaces, so every read of them the fast tier cannot answer in full is
refused rather than answered short. Each one is a value another live archive
document already holds: find that document, decide which of the two keeps the
value, and write the loser again. Mirroring carries it then and the refusals
lift by themselves.
`)
}

// findReservedFields names the application's own documents that carry one of
// the field names this system writes into archived copies.
//
// The router refuses writes that would leave one, on every path. That is
// preventive, and an adoption is the shape this product exists for: the
// cluster arrives with years of documents already in it, written by an
// application that never heard of this system, and nothing about the guard
// helps a document that is already there.
//
// Such a document is invisible to every read that falls through to the archive,
// because the predicate that excludes deleted copies is {$exists: false} and it
// cannot tell the two apart. A lookup by _id still returns it, which is what
// makes it worth naming: the collection answers two different ways about one
// document and no error is raised either way.
//
// Only the fast tier is counted, and that is what makes the answer clean. The
// archive's live copies and its tombstones both carry the field by design, so a
// count there says nothing. Nothing writes it to the fast tier: the router
// never marks anything there, and a copy-up brings up live documents only. So a
// fast tier document carrying it is the application's own.
//
// Bounded, because the field is not indexed and this is a collection scan. It
// is here rather than in a router's startup for that reason: an operator ran
// this on purpose and can spend a minute on it, where a router restart during
// an incident cannot.
func findReservedFields(ctx context.Context, hot *mongo.Client, cfg config.Config, budget time.Duration) {
	namespaces := cfg.Feeder.Namespaces
	if len(namespaces) == 0 {
		// Everything is tiered, and the fast tier's own collections are the ones
		// whose documents this is about.
		found, err := hotNamespaces(ctx, hot)
		if err != nil {
			fmt.Printf("\nthe fast tier's collections could not be listed, so none was checked for "+
				"reserved field names: %v\n", err)
			return
		}
		namespaces = found
	}
	if len(namespaces) == 0 {
		return
	}
	sort.Strings(namespaces)

	fmt.Printf("\nChecking %d tiered namespace(s) for documents carrying this system's own field names.\n",
		len(namespaces))
	deadline := time.Now().Add(budget)
	var carrying []string
	checked := 0
	for _, ns := range namespaces {
		if time.Until(deadline) <= 0 {
			fmt.Printf("  %s: not reached inside %s, so it stands unchecked\n", ns, budget)
			continue
		}
		db, coll, ok := strings.Cut(ns, ".")
		if !ok {
			continue
		}
		filter := bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: meta.TombstoneField, Value: bson.D{{Key: "$exists", Value: true}}}},
			bson.D{{Key: meta.BodyDroppedField, Value: bson.D{{Key: "$exists", Value: true}}}},
		}}}
		cctx, cancel := context.WithDeadline(ctx, deadline)
		n, err := hot.Database(db).Collection(coll).CountDocuments(cctx, filter)
		cancel()
		if err != nil {
			fmt.Printf("  %s: %v\n", ns, err)
			continue
		}
		checked++
		if n > 0 {
			carrying = append(carrying, fmt.Sprintf("%s (%d)", ns, n))
		}
	}
	switch {
	case len(carrying) > 0:
		fmt.Printf("\n%s\n", strings.Join(carrying, "\n"))
		fmt.Printf("\nThese documents carry a field this system marks archived documents deleted\n" +
			"with, so every read that falls through to the archive leaves them out and\n" +
			"reports success, while a lookup by _id still returns them. Rename the field in\n" +
			"the application and in these documents before tiering them. New writes carrying\n" +
			"it are refused; these were already here.\n")
	case checked > 0:
		fmt.Printf("None of them carries one.\n")
	}
}

// hotNamespaces lists the fast tier's application collections, for the
// deployment that tiers everything by naming nothing.
func hotNamespaces(ctx context.Context, hot *mongo.Client) ([]string, error) {
	dbs, err := hot.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, db := range dbs {
		switch db {
		case "admin", "local", "config":
			continue
		}
		if isBookkeepingDB(db) {
			continue
		}
		colls, err := hot.Database(db).ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return nil, err
		}
		for _, c := range colls {
			if strings.HasPrefix(c, "system.") || meta.IsScratch(c) {
				continue
			}
			out = append(out, db+"."+c)
		}
	}
	return out, nil
}

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
