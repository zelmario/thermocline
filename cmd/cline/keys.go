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

	"github.com/zelmar/thermocline/internal/config"
	"github.com/zelmar/thermocline/internal/router"
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

	if len(cfg.Feeder.Namespaces) == 0 {
		return errors.New("feeder.namespaces is empty, so there is no list of collections to scan; name them to use this command")
	}

	r := router.New(hot, cold, cfg.Router, log)
	guarded := r.UniqueNamespaces(ctx, cfg.Feeder.Namespaces)
	if len(guarded) == 0 {
		fmt.Println("no unique indexes on any tiered collection, so there is nothing to enforce")
		return nil
	}

	if !*rebuild {
		fmt.Println("unique indexes enforced across both tiers:")
		for _, ns := range guarded {
			fmt.Printf("  %s\n", ns)
		}
		fmt.Println("\nrun with --rebuild to claim keys for documents that already exist")
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
