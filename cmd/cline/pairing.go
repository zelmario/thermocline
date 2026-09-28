package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/pairing"
)

// runPairing checks, or with --accept records, which archive the fast tier
// works with. See internal/pairing.
func runPairing(args []string) error {
	fs := flag.NewFlagSet("pairing", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	accept := fs.Bool("accept", false, "record the archive cold.uri points at now as the one this fast tier works with")
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

	if *accept {
		id, aerr := pairing.Accept(ctx, hot, cold)
		if aerr != nil {
			return aerr
		}
		fmt.Printf("the fast tier now works with archive %s\n", id)
		return nil
	}
	if cerr := pairing.Check(ctx, hot, cold, log); cerr != nil {
		return cerr
	}
	fmt.Println("the fast tier and the archive are the pair they have been")
	return nil
}
