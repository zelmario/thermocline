// Command cline runs the Thermocline daemons.
//
//	cline feeder --config /etc/cline.yaml
//
// One binary, one subcommand per component. The feeder is the only one
// implemented so far; the others report what they are waiting on rather than
// pretending to work.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/zelmar/thermocline/internal/config"
	"github.com/zelmar/thermocline/internal/evictor"
	"github.com/zelmar/thermocline/internal/feeder"
	"github.com/zelmar/thermocline/internal/router"
	"github.com/zelmar/thermocline/internal/warmer"
)

const usage = `cline - storage tiering for MongoDB

Usage:
  cline feeder   --config FILE            mirror writes from the fast tier into the archive
  cline evictor  --config FILE [--dry-run] [--once]
                                          remove documents the archive already holds
  cline warmer   --config FILE [--dry-run]
                                          fill the fast tier from the archive
  cline router   --config FILE            serve both tiers on one address
  cline watermark --config FILE [--ns NS] [--clear | --complete-all | --complete-from T]
                                          show or edit what the fast tier claims to hold
  cline version

Every subcommand takes --log-file PATH to append logs to a file instead of
stderr, overriding log.file in the config. Rotate it with logrotate and send
SIGHUP; the daemon reopens the file.

The feeder must be running before the evictor will remove anything.
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		return errors.New("no subcommand given")
	}

	switch os.Args[1] {
	case "feeder":
		return runFeeder(os.Args[2:])
	case "evictor":
		return runEvictor(os.Args[2:])
	case "warmer":
		return runWarmer(os.Args[2:])
	case "router":
		return runRouter(os.Args[2:])
	case "watermark":
		return runWatermark(os.Args[2:])
	case "version":
		fmt.Println("cline 0.1.0-dev")
		return nil
	case "-h", "--help", "help":
		fmt.Print(usage)
		return nil
	default:
		fmt.Print(usage)
		return fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
}

func runFeeder(args []string) error {
	fs := flag.NewFlagSet("feeder", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	logPath := fs.String("log-file", "", "append logs here instead of stderr, overriding log.file")
	verbose := fs.Bool("v", false, "log every batch written")
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

	log.Info("connected to both tiers", "feeder", cfg.Feeder.ID)

	f := feeder.New(hot, cold, cfg.Feeder, log)
	if err := f.Run(ctx); err != nil {
		return err
	}
	log.Info("feeder stopped", "lag_at_exit", f.Lag().Round(time.Millisecond))
	return nil
}

func runEvictor(args []string) error {
	fs := flag.NewFlagSet("evictor", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	dryRun := fs.Bool("dry-run", false, "report what would be removed, remove nothing")
	once := fs.Bool("once", false, "run one sweep and exit")
	every := fs.Duration("every", 5*time.Minute, "pause between sweeps when running continuously")
	healthWait := fs.Duration("health-timeout", 2*time.Minute,
		"with --once, give up if the cluster stays unfit to evict for this long")
	logPath := fs.String("log-file", "", "append logs here instead of stderr, overriding log.file")
	verbose := fs.Bool("v", false, "log every batch")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if len(cfg.Evictor.Rules) == 0 {
		return errors.New("evictor.rules is empty, there is nothing to remove")
	}

	log, closeLog, err := setupLogging(*logPath, cfg, *verbose)
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

	e := evictor.New(hot, cold, cfg.Evictor, *dryRun, log)
	if *once {
		// A one-shot sweep has to return. Waiting forever for the cluster to
		// become healthy is right for a daemon and wrong for a command.
		e.SetMaxHealthWait(*healthWait)
		stats, err := e.RunOnce(ctx)
		log.Info("sweep complete", stats.LogArgs()...)
		return err
	}
	return e.Run(ctx, *every)
}

func runWarmer(args []string) error {
	fs := flag.NewFlagSet("warmer", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	dryRun := fs.Bool("dry-run", false, "report what would be copied, copy nothing")
	logPath := fs.String("log-file", "", "append logs here instead of stderr, overriding log.file")
	verbose := fs.Bool("v", false, "log every batch")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if len(cfg.Warmer.Rules) == 0 {
		return errors.New("warmer.rules is empty, there is nothing to fill")
	}

	log, closeLog, err := setupLogging(*logPath, cfg, *verbose)
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

	w := warmer.New(hot, cold, cfg.Warmer, *dryRun, log)
	stats, err := w.Run(ctx)
	log.Info("warm complete", stats.LogArgs()...)
	return err
}

func runRouter(args []string) error {
	fs := flag.NewFlagSet("router", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	listen := fs.String("listen", "", "override router.listen")
	logPath := fs.String("log-file", "", "append logs here instead of stderr, overriding log.file")
	verbose := fs.Bool("v", false, "log every routing decision")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if *listen != "" {
		cfg.Router.Listen = *listen
	}

	log, closeLog, err := setupLogging(*logPath, cfg, *verbose)
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

	r := router.New(hot, cold, cfg.Router, log)
	go reportRouterStats(ctx, r, log)
	return r.Listen(ctx)
}

// reportRouterStats logs the counters an operator watches. Copy-up rate says
// whether the eviction boundary is cut too deep; refused says whether queries
// are reaching past what the fast tier holds.
func reportRouterStats(ctx context.Context, r *router.Router, log *slog.Logger) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if r.Stats.Commands.Load() == 0 {
				continue
			}
			log.Info("router",
				"commands", r.Stats.Commands.Load(),
				"read_misses", r.Stats.ReadMisses.Load(),
				"write_misses", r.Stats.WriteMisses.Load(),
				"copied_up", r.Stats.CopiedUp.Load(),
				"incomplete", r.Stats.Incomplete.Load(),
				"refused", r.Stats.Refused.Load(),
				"clients", r.Stats.ClientsOpen.Load())
		}
	}
}

func connect(ctx context.Context, uri, name string) (*mongo.Client, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetAppName("cline"))
	if err != nil {
		return nil, fmt.Errorf("connect to %s tier: %w", name, err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("ping %s tier: %w", name, err)
	}
	return client, nil
}

func disconnect(client *mongo.Client, log *slog.Logger) {
	if err := client.Disconnect(context.Background()); err != nil {
		log.Warn("disconnect failed", "err", err)
	}
}
