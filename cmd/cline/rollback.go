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
	"github.com/zelmario/thermocline/internal/meta"
)

// runRollback prepares the archive to be used directly again.
//
// Going back is repointing the application at the archive, because the feeder
// has been mirroring every write into it all along. One thing has to happen
// first, and it is the kind of thing that is obvious afterwards: a delete is
// mirrored as a mark and not as a removal, so every document deleted since the
// cutover is still physically there carrying _tcDeletedAt. The router filters
// those out of every answer and a client reading the cluster directly does not.
//
// The remedy was a deleteMany per tiered namespace, typed by hand, during an
// incident. Three things go wrong with that. A namespace gets missed, and the
// collection nobody thought about is the one that answers wrongly. It is run
// against the fast tier, where it does nothing and looks like it worked. And it
// is run while the feeder is still mirroring, so the deletes that arrive
// afterwards leave fresh marks behind the ones just cleared.
//
// So this counts first and clears only when asked, and refuses to clear while a
// feeder is still publishing, which is the ordering nobody gets told about.
func runRollback(args []string) error {
	fs := flag.NewFlagSet("rollback", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	clear := fs.Bool("clear", false, "remove the marked documents, rather than counting them")
	force := fs.Bool("force", false, "clear even though a feeder still looks alive")
	nsFlag := fs.String("ns", "", "one namespace, rather than every tiered one")
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

	cold, err := connect(ctx, cfg.Cold.URI, "cold")
	if err != nil {
		return err
	}
	defer disconnect(cold, log)

	namespaces := cfg.Feeder.Namespaces
	if *nsFlag != "" {
		namespaces = []string{*nsFlag}
	}
	if len(namespaces) == 0 {
		fmt.Println("No namespaces are configured, so nothing here knows which collections to look at.")
		fmt.Println("Name one with --ns.")
		return nil
	}
	sort.Strings(namespaces)

	alive, since := feederPublishing(ctx, cold, cfg.Feeder.ID)
	if *clear && alive && !*force {
		fmt.Printf("A feeder published %s ago, so it is still mirroring.\n\n", since.Round(time.Second))
		fmt.Print(`Clearing now would leave fresh marks behind the ones it removed: every delete
that reaches the archive after this point writes another. Stop the application
and the feeder first, let mirroring finish, then run this again.

--force clears anyway, which is only right if you know nothing is writing.
`)
		return fmt.Errorf("a feeder is still publishing")
	}

	total := int64(0)
	for _, ns := range namespaces {
		db, coll := splitNS(ns)
		if db == "" || coll == "" {
			continue
		}
		c := cold.Database(db).Collection(coll)
		filter := bson.D{{Key: "_tcDeletedAt", Value: bson.D{{Key: "$exists", Value: true}}}}
		n, cerr := c.CountDocuments(ctx, filter)
		if cerr != nil {
			fmt.Printf("  %-40s could not be read: %v\n", ns, cerr)
			continue
		}
		total += n
		if !*clear {
			fmt.Printf("  %-40s %d marked deleted\n", ns, n)
			continue
		}
		if n == 0 {
			fmt.Printf("  %-40s nothing to clear\n", ns)
			continue
		}
		res, derr := c.DeleteMany(ctx, filter)
		if derr != nil {
			fmt.Printf("  %-40s could not be cleared: %v\n", ns, derr)
			continue
		}
		fmt.Printf("  %-40s cleared %d\n", ns, res.DeletedCount)
	}

	fmt.Print("\n")
	switch {
	case *clear:
		fmt.Println("The archive now answers the way the router did.")
		fmt.Println("Point the application at it, and stop the router before anything else writes.")
	case total == 0:
		fmt.Println("Nothing is marked deleted, so the archive already answers the way the router does.")
	default:
		fmt.Printf("%d document(s) are marked deleted. A client reading this cluster directly sees\n", total)
		fmt.Print(`them; the router does not. Clear them with --clear before pointing anything at
it, with the application and the feeder stopped so nothing writes more.
`)
	}
	return nil
}

// feederPublishing reports whether a feeder is still writing its position, and
// how long ago it last did.
//
// The position record carries the interval it is written at, so "recently" is
// read from the deployment rather than guessed: four missed publications is the
// same threshold the evictor's health check uses to decide a feeder has gone.
func feederPublishing(ctx context.Context, cold *mongo.Client, id string) (bool, time.Duration) {
	var pos struct {
		SavedAt time.Time `bson:"savedAt"`
		EveryMS int64     `bson:"everyMS"`
	}
	err := cold.Database(meta.DB).Collection(meta.PositionsColl).
		FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&pos)
	if err != nil || pos.SavedAt.IsZero() {
		return false, 0
	}
	every := time.Duration(pos.EveryMS) * time.Millisecond
	if every <= 0 {
		every = time.Second
	}
	stale := 4 * every
	if stale < 2*time.Second {
		stale = 2 * time.Second
	}
	age := time.Since(pos.SavedAt)
	return age <= stale, age
}

// splitNS divides db.collection at the first dot, because a collection name may
// contain dots and a database name may not.
func splitNS(ns string) (db, coll string) {
	db, coll, _ = strings.Cut(ns, ".")
	return db, coll
}
