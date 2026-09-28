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
	"github.com/zelmario/thermocline/internal/pairing"
	"github.com/zelmario/thermocline/internal/router"
)

// runKeys manages the unique key registry.
//
// The registry is what keeps a unique index meaningful once documents start
// leaving the fast tier, and it only protects keys it knows about. A database
// adopted with documents already in it starts with none of them claimed, so
// --rebuild has to be run before eviction begins. It is also how the registry
// comes back after a fast tier is rebuilt, since it lives there.
func runKeys(args []string) error {
	fs := flag.NewFlagSet("keys", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	rebuild := fs.Bool("rebuild", false, "claim a key for every document that already exists")
	prune := fs.Bool("prune", false,
		"remove claims no live document holds any more, which is how the registry stops growing")
	pruneFor := fs.Duration("prune-for", time.Minute,
		"how long to spend pruning before stopping; a claim not reached is a claim that stays")
	logPath := fs.String("log-file", "", "append logs here instead of stderr, overriding log.file")
	if err := fs.Parse(args); err != nil {
		return err
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

	hot, err := connect(ctx, cfg.Hot.URI, "hot")
	if err != nil {
		return err
	}
	defer disconnect(hot, log)

	cold, err := connect(ctx, cfg.Cold.URI, "cold")
	if err != nil {
		return err
	}
	defer disconnect(cold, log)
	// A command that writes checks the pairing the daemons check, because it
	// writes into whichever archive cold.uri reaches. See internal/pairing.
	if err := pairing.Check(ctx, hot, cold, log); err != nil {
		return err
	}

	// Empty tiers everything, and refusing here left the command the adoption
	// guide tells you to run unusable on that deployment. The archive holds
	// every document, so it also knows every collection: the same answer the
	// configuration would have given, which is what rollback does too.
	namespaces := cfg.Feeder.Namespaces
	if len(namespaces) == 0 {
		fmt.Println("No namespaces are configured, so every collection is tiered.")
		fmt.Println("Asking the archive which ones it holds.")
		found, ferr := archiveNamespaces(ctx, cold)
		if ferr != nil {
			return fmt.Errorf("could not ask the archive which collections it holds: %w", ferr)
		}
		if len(found) == 0 {
			return errors.New("the archive holds no collections to scan")
		}
		namespaces = found
		fmt.Printf("Found %d.\n", len(namespaces))
	}

	r := router.New(hot, cold, cfg.Router, log)
	guarded := r.UniqueNamespaces(ctx, namespaces)
	if len(guarded) == 0 {
		fmt.Println("no unique indexes on any tiered collection, so there is nothing to enforce")
		return nil
	}

	if *prune {
		// The registry only ever grew: a claim is permanent on purpose, because
		// evicting a document takes its index entry off the fast tier and the
		// claim is what remembers the value was taken. Nothing removed one whose
		// document has been deleted from both tiers, and the registry lives on
		// the fast tier. Measured on 500 inserts and 500 deletes: nought
		// documents on either tier, 500 claims still there.
		removed, err := r.PruneKeys(ctx, guarded, *pruneFor)
		if err != nil {
			return err
		}
		names := make([]string, 0, len(removed))
		total := 0
		for ns, n := range removed {
			names = append(names, ns)
			total += n
		}
		sort.Strings(names)
		for _, ns := range names {
			if removed[ns] > 0 {
				fmt.Printf("%s: removed %d claim(s) nothing holds\n", ns, removed[ns])
			}
		}
		if total == 0 {
			fmt.Println("every claim is still held by a live document, so nothing was removed")
		}
		return nil
	}

	if !*rebuild {
		fmt.Println("unique indexes enforced across both tiers:")
		for _, ns := range guarded {
			fmt.Printf("  %s\n", ns)
		}
		fmt.Println("\nrun with --rebuild to claim keys for documents that already exist,")
		fmt.Println("or --prune to remove claims no live document holds any more")
		return nil
	}

	claimed, conflicts, err := r.RebuildKeys(ctx, guarded)
	if err != nil {
		return err
	}

	names := make([]string, 0, len(claimed))
	for ns := range claimed {
		names = append(names, ns)
	}
	sort.Strings(names)
	for _, ns := range names {
		fmt.Printf("%s: claimed %d\n", ns, claimed[ns])
	}
	if len(names) == 0 {
		fmt.Println("nothing new to claim")
	}

	total := 0
	for _, n := range conflicts {
		total += n
	}
	if total > 0 {
		fmt.Printf("\n%d key(s) are held by more than one live document.\n", total)
		fmt.Println("The unique constraint was already broken before this ran, so no winner was picked.")
		fmt.Println("Find and resolve them before evicting, or the duplicates become permanent.")
	}
	return nil
}
