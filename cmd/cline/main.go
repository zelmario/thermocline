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
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/evictor"
	"github.com/zelmario/thermocline/internal/feeder"
	"github.com/zelmario/thermocline/internal/router"
	"github.com/zelmario/thermocline/internal/tierver"
	"github.com/zelmario/thermocline/internal/warmer"
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
  cline indexes  --config FILE [--ns NS]
                                          compare both tiers' indexes, so a
                                          read that falls through is not left
                                          scanning the archive
  cline cache    --config FILE [--top N] [--ns NS]
                                          which queries would be worth caching,
                                          and for how long, from what the router
                                          has watched go past
  cline keys     --config FILE [--rebuild]
                                          unique keys that must survive eviction
  cline pause    --config FILE [--reason WHY]
                                          stop the evictor removing anything, for
                                          as long as the archive cannot be
                                          trusted, such as while it is restored
  cline resume   --config FILE            let the evictor remove again
  cline rollback --config FILE [--ns NS] [--clear] [--force]
                                          count, and then remove, the documents
                                          the archive holds marked deleted, so it
                                          can be read directly again
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
	case "keys":
		return runKeys(os.Args[2:])
	case "pause":
		return runPause(os.Args[2:], true)
	case "resume":
		return runPause(os.Args[2:], false)
	case "indexes":
		return runIndexes(os.Args[2:])
	case "cache":
		return runCache(os.Args[2:])
	case "rollback":
		return runRollback(os.Args[2:])
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
	warnAboutWindows(log, cfg)

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

	if err := tierver.Check(ctx, hot, cold, cfg.AllowVersionSkew, log); err != nil {
		return err
	}

	log.Info("connected to both tiers", "feeder", cfg.Feeder.ID)

	f := feeder.New(hot, cold, cfg.Feeder, log)
	if err := f.Run(ctx); err != nil {
		return fatal(log, "feeder", err)
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
	warnAboutWindows(log, cfg)

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

	if err := tierver.Check(ctx, hot, cold, cfg.AllowVersionSkew, log); err != nil {
		return err
	}

	e := evictor.New(hot, cold, cfg.Evictor, *dryRun, log)
	if *once {
		// A one-shot sweep has to return. Waiting forever for the cluster to
		// become healthy is right for a daemon and wrong for a command.
		e.SetMaxHealthWait(*healthWait)
		stats, err := e.RunOnce(ctx)
		log.Info("sweep complete", stats.LogArgs()...)
		if err != nil {
			return fatal(log, "evictor", err)
		}
		return nil
	}
	if err := e.Run(ctx, *every); err != nil {
		return fatal(log, "evictor", err)
	}
	return nil
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
	warnAboutWindows(log, cfg)

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

	if err := tierver.Check(ctx, hot, cold, cfg.AllowVersionSkew, log); err != nil {
		return err
	}

	w := warmer.New(hot, cold, cfg.Warmer, cfg.Feeder.UnannouncedDeletes, *dryRun, log)
	stats, err := w.Run(ctx)
	log.Info("warm complete", stats.LogArgs()...)
	if err != nil {
		return fatal(log, "warmer", err)
	}
	return nil
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
	warnAboutWindows(log, cfg)

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

	warnIfArchiveReadPreferenceIgnored(cfg.Cold.URI, log)

	r := router.New(hot, cold, cfg.Router, log)
	go reportRouterStats(ctx, r, log)
	if cfg.Router.Dashboard != "" {
		go func() {
			if derr := r.ServeDashboard(ctx, cfg.Router.Dashboard); derr != nil {
				// A dashboard that cannot bind is not a reason to stop routing
				// traffic. It is a reason to say so loudly.
				log.Error("dashboard stopped", "err", derr)
			}
		}()
	}
	if err := r.Listen(ctx); err != nil {
		return fatal(log, "router", err)
	}
	return nil
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
				"from_archive", r.Stats.Archived.Load(),
				"stale_reads", r.Stats.StaleReads.Load(),
				"txn_replays", r.Stats.TxnReplays.Load(),
				"refused", r.Stats.Refused.Load(),
				"clients", r.Stats.ClientsOpen.Load())
		}
	}
}

// fatal records why a daemon stopped, through the logger rather than only on
// stderr.
//
// main prints the returned error to stderr, and stderr is not where the logs
// go once log.file or --log-file is set. A feeder halting on an unannounced
// delete would end its log file mid-sentence and say nothing about why, which
// defeats the point of a policy whose whole job is to stop and alert.
func fatal(log *slog.Logger, what string, err error) error {
	log.Error(what+" stopped", "err", err)
	return err
}

// warnIfArchiveReadPreferenceIgnored says that a read preference on cold.uri
// does not do what it looks like it does.
//
// It is the natural thing to reach for when adopting a large cluster: during
// the adoption most reads fall through to the archive, which is the production
// cluster being relieved, and sending them to its secondaries instead of its
// primary is exactly the kind of thing an operator does. Measured with
// readPreference=secondaryPreferred on cold.uri: the fallback read was served
// by the archive's primary anyway.
//
// It is ignored because archive reads are issued as commands, and the driver
// runs a command against the primary whatever the client's preference says.
// Keeping it that way is deliberate rather than an oversight: the freshness
// barrier waits for the feeder to have written to the archive, which means its
// primary, and a secondary is behind that by a lag nothing here measures. A
// read served there could miss a write that has already been acknowledged.
//
// So the setting stays ignored and stops being silent.
func warnIfArchiveReadPreferenceIgnored(uri string, log *slog.Logger) {
	cs, err := connstring.ParseAndValidate(uri)
	if err != nil || cs.ReadPreference == "" {
		return
	}
	if strings.EqualFold(cs.ReadPreference, "primary") {
		return
	}
	log.Warn("the read preference on cold.uri is ignored",
		"read_preference", cs.ReadPreference,
		"meaning", "reads that fall through are served by the archive's primary whatever this says, "+
			"because they are issued as commands",
		"why", "the freshness barrier waits for mirroring to reach the archive's primary, and a "+
			"secondary is behind that by a lag nothing measures, so a read served there could "+
			"miss a write that has already been acknowledged")
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

	// A sharded fast tier is refused here rather than discovered later.
	//
	// Everything this system does to a fast tier assumes a replica set. The
	// router proxies bytes to one primary address, which a mongos does not
	// report: measured, its hello answers {msg: "isdbgrid", isWritablePrimary:
	// true} and nothing else, so the router failed to start with "fast tier
	// reported no primary address", which is true and says nothing about the
	// cause. The daemons were worse off, because they started.
	//
	// Every reading the evictor holds itself back on comes from a replica set.
	// Measured against a mongos: replSetGetStatus is "not supported through
	// mongos", the local database cannot be reached at all so there is no oplog
	// window, and serverStatus carries no wiredTiger section so neither cache
	// dirty nor write tickets exist. A reading that cannot be taken does not
	// block, so four of the five backoffs quietly did nothing: a dry run with
	// candidates reported would_evict=5 and backoffs=0 on a cluster it could
	// tell nothing about.
	//
	// So this says no, with the reason. Supporting a sharded fast tier is a
	// feature, and the first thing it needs is health readings that mean
	// something there.
	if name == "hot" {
		kind, herr := kindOfTier(pingCtx, client)
		switch {
		case herr != nil:
			// Unreadable rather than wrong: say so and let the caller decide.
			return nil, fmt.Errorf("ask the fast tier what it is: %w", herr)
		case kind == tierMongos:
			return nil, fmt.Errorf("the fast tier is a mongos, and sharded fast tiers are not supported: " +
				"this proxies to one primary address, which a mongos does not have, and every health " +
				"reading the evictor pauses on is a replica set's. Point cline at a replica set")
		case kind == tierStandalone:
			// A standalone has no oplog, so no change stream, so nothing to
			// mirror from: the feeder used to sit there retrying and the router
			// stopped with "fast tier reported no primary address", which is
			// true and explains nothing. One member is enough, and that is what
			// the development pair is.
			return nil, fmt.Errorf("the fast tier is a standalone mongod, and this needs a replica set: " +
				"the feeder mirrors from a change stream and a standalone has no oplog to open one on. " +
				"A single-member replica set is enough. Run rs.initiate() on it, or start it with --replSet")
		}
	}
	return client, nil
}

// tierKind is what a connection landed on, which decides whether this can work
// against it at all.
type tierKind int

const (
	tierReplicaSet tierKind = iota
	tierMongos
	tierStandalone
)

// tierKind asks the server what it is.
//
// mongos says so in hello with msg: "isdbgrid", which is the same field a
// driver reads to decide it is talking to a sharded cluster. A replica set
// member names its set in setName. Anything else is a standalone.
func kindOfTier(ctx context.Context, client *mongo.Client) (tierKind, error) {
	var hello struct {
		Msg     string `bson:"msg"`
		SetName string `bson:"setName"`
	}
	if err := client.Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return tierReplicaSet, err
	}
	switch {
	case hello.Msg == "isdbgrid":
		return tierMongos, nil
	case hello.SetName == "":
		return tierStandalone, nil
	default:
		return tierReplicaSet, nil
	}
}

func disconnect(client *mongo.Client, log *slog.Logger) {
	if err := client.Disconnect(context.Background()); err != nil {
		log.Warn("disconnect failed", "err", err)
	}
}

// warnAboutWindows says so when a warm window reaches past its own eviction
// cutoff.
//
// Every daemon that reads a config says it, not only the two that act on those
// rules, because an operator reading one log should not have to know which
// component owns the contradiction. See config.WindowWarnings for what it costs
// and why it is not refused.
func warnAboutWindows(log *slog.Logger, cfg config.Config) {
	for _, w := range cfg.FeederIDWarnings() {
		log.Warn("the evictor is pointed at a feeder this config does not describe",
			"detail", w,
			"effect", "the mirroring lag gate finds no checkpoint under that id, so eviction "+
				"pauses on every pass and nothing is ever removed",
			"action", "leave evictor.feeder_id out to take it from feeder.id, or set it to the id "+
				"of the feeder that is actually running")
	}
	for _, w := range cfg.AgeFieldWarnings() {
		log.Warn("a namespace is aged two different ways",
			"detail", w,
			"effect", "the evictor and the warmer select different documents, and the router cannot "+
				"tell which of the two a completeness claim is measured in, so every bounded query "+
				"on this namespace is answered by the archive")
	}
	for _, w := range cfg.WindowWarnings() {
		log.Warn("a warm window reaches past its own eviction cutoff",
			"detail", w,
			"effect", "the same documents are copied up and removed again for as long as both run",
			"note", "answers stay correct; the evictor narrows the completeness claim before it "+
				"removes, and the warmer checks the fast tier still holds a window before claiming it")
	}
}
