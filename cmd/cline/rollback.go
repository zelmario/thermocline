package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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
	switch {
	case *nsFlag != "":
		namespaces = []string{*nsFlag}
	case len(namespaces) == 0:
		// An empty feeder.namespaces means every collection is tiered, which is
		// a supported configuration and the one where this command was least
		// use: it told the operator to name them one at a time, which is the
		// hand-typed process it exists to replace, during an incident, with the
		// missed collection being the one that answers wrongly afterwards.
		//
		// The archive holds every document, so it also knows every collection.
		// Asking it is the same answer the configuration would have given.
		fmt.Println("No namespaces are configured, so every collection is tiered.")
		fmt.Println("Asking the archive which ones it holds.")
		found, derr := archiveNamespaces(ctx, cold)
		if derr != nil {
			return fmt.Errorf("could not ask the archive which collections it holds, so this "+
				"cannot know what to look at; name one with --ns: %w", derr)
		}
		if len(found) == 0 {
			return fmt.Errorf("the archive holds no collections to look at")
		}
		namespaces = found
		fmt.Printf("Found %d.\n", len(namespaces))
	}
	sort.Strings(namespaces)

	// What the archive is missing, before anything about tombstones, because it
	// is the half an operator cannot fix afterwards: an application repointed at
	// an archive without its accounts cannot log in at all, and MongoDB answers
	// that with an ordinary authentication failure naming nothing.
	gaps := reportArchiveGaps(ctx, cfg, log)

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
	// Both of the reassuring endings are about delete marks alone, and they used
	// to be printed whatever else was missing: "the archive already answers the
	// way the router does" appeared directly under a list of collections it has
	// never held. Marks are one of the things standing between an operator and
	// going back, and on an adoption they are rarely the biggest.
	ready := "The archive now answers the way the router did."
	if gaps {
		ready = "Delete marks are dealt with. What is listed above is not, and until it is " +
			"the archive\nis not what the application had."
	}
	switch {
	case *clear:
		fmt.Println(ready)
		if !gaps {
			fmt.Println("Point the application at it, and stop the router before anything else writes.")
		}
	case total == 0 && !gaps:
		fmt.Println("Nothing is marked deleted, so the archive already answers the way the router does.")
	case total == 0:
		fmt.Println("Nothing is marked deleted, but what is listed above is missing from the archive.")
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

// isBookkeepingDB reports whether a database is Thermocline's own rather than
// the application's, for the three listings that must leave it alone.
//
// Both spellings, and that is the point. The configured one comes from
// meta.db, which config.Load has already applied to meta.DB by the time any of
// this runs, and a deployment sharing one pair of tiers with another sets it to
// something of its own. The default is kept beside it because the other
// deployment on those tiers may be the one that never set it, and because the
// parallel test sweep gives each worker its own _thermocline_wN.
//
// It used to read `HasPrefix(db, meta.DefaultDB) || HasPrefix(db, "_thermocline")`,
// which is the same literal twice: meta.DefaultDB is "_thermocline". The
// comment beside it said "the configured bookkeeping database and the default
// one", so the intention was written down and only the default was implemented.
// A deployment with meta.db set to anything not starting with "_thermocline"
// had its own bookkeeping listed as application data by rollback, which is the
// command that reads the archive back out as the application's cluster.
func isBookkeepingDB(db string) bool {
	return strings.HasPrefix(db, meta.DB) || strings.HasPrefix(db, meta.DefaultDB)
}

// archiveNamespaces lists every collection the archive holds, for the
// configuration that tiers all of them.
//
// System databases and system collections are left out: they are a node's own
// bookkeeping, they carry no tombstones, and clearing anything in them is not
// what rollback means. Views too, which hold no documents.
func archiveNamespaces(ctx context.Context, cold *mongo.Client) ([]string, error) {
	dbs, err := cold.ListDatabaseNames(ctx, bson.D{})
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
		cur, lerr := cold.Database(db).ListCollections(ctx, bson.D{{Key: "type", Value: "collection"}})
		if lerr != nil {
			return nil, fmt.Errorf("list collections in %s: %w", db, lerr)
		}
		for cur.Next(ctx) {
			name, ok := cur.Current.Lookup("name").StringValueOK()
			if !ok || strings.HasPrefix(name, "system.") {
				continue
			}
			out = append(out, db+"."+name)
		}
		cerr := cur.Err()
		_ = cur.Close(ctx)
		if cerr != nil {
			return nil, fmt.Errorf("list collections in %s: %w", db, cerr)
		}
	}
	return out, nil
}

// reportArchiveGaps names what the fast tier has and the archive does not, for
// somebody about to point an application at the archive.
//
// Two kinds, and neither is a document, which is why nothing mirrored them.
//
// Accounts cannot be mirrored at all: MongoDB stores SCRAM credentials derived
// per cluster, so an account created on the fast tier after the cutover can only
// be recreated in the archive with its password, which this does not have. The
// router says the same thing at startup for the other direction, which is the
// one an adoption meets.
//
// Views hold no documents, so the feeder never carries one. A view created
// through the router is mirrored down as it is created now, but one made before
// that, or made out of band, is not, and a read of it after repointing is an
// error rather than a wrong answer.
//
// Best effort throughout. This runs during an incident, the fast tier may be the
// thing that is broken, and a rollback must not depend on it being reachable.
func reportArchiveGaps(ctx context.Context, cfg config.Config, log *slog.Logger) bool {
	hot, err := connect(ctx, cfg.Hot.URI, "hot")
	if err != nil {
		fmt.Printf("Could not reach the fast tier to compare it: %v\n", err)
		fmt.Println("Nothing below is affected; this section is a courtesy.")
		fmt.Println()
		return false
	}
	defer disconnect(hot, log)
	cold, err := connect(ctx, cfg.Cold.URI, "cold")
	if err != nil {
		return false
	}
	defer disconnect(cold, log)

	users := missingAccounts(ctx, hot, cold)
	views := missingViews(ctx, hot, cold)
	colls := missingCollections(ctx, hot, cold)
	if len(users) == 0 && len(views) == 0 && len(colls) == 0 {
		return false
	}

	fmt.Println("The fast tier has things the archive does not:")
	if len(users) > 0 {
		fmt.Printf("  accounts  %s\n", strings.Join(users, ", "))
		fmt.Println("            create them in the archive with the same names, passwords and roles;")
		fmt.Println("            credentials are per cluster, so nothing could have copied them")
	}
	if len(views) > 0 {
		fmt.Printf("  views     %s\n", strings.Join(views, ", "))
		fmt.Println("            recreate them in the archive with createView")
	}
	if len(colls) > 0 {
		fmt.Printf("  collections  %d the archive has never held. The feeder mirrors what\n", len(colls))
		fmt.Println("               feeder.namespaces tiers and nothing else, so these have lived")
		fmt.Println("               on the fast tier alone since the cutover. An application")
		fmt.Println("               repointed at the archive will not find them.")
		printByDatabase(colls)
		fmt.Println("               Copy them across first:")
		fmt.Println("                 mongodump --uri=<hot> --db=DB --collection=COLL -o /tmp/back")
		fmt.Println("                 mongorestore --uri=<cold> /tmp/back")
	}
	fmt.Println()
	return true
}

// printByDatabase lists namespaces grouped by their database, one line each.
//
// A hundred names joined by commas on one line is a list nobody reads, and this
// is read during an incident. Grouped, an operator sees which applications are
// affected before they see how many collections there are.
func printByDatabase(nss []string) {
	const maxDBs = 12
	order := []string{}
	byDB := map[string][]string{}
	for _, ns := range nss {
		db, coll, ok := strings.Cut(ns, ".")
		if !ok {
			db, coll = "", ns
		}
		if _, seen := byDB[db]; !seen {
			order = append(order, db)
		}
		byDB[db] = append(byDB[db], coll)
	}
	sort.Strings(order)
	shown := order
	if len(order) > maxDBs {
		shown = order[:maxDBs]
	}
	for _, db := range shown {
		list := strings.Join(byDB[db], ", ")
		if len(list) > 100 {
			list = list[:97] + "..."
		}
		fmt.Printf("                 %-22s %s\n", db, list)
	}
	if len(order) > len(shown) {
		fmt.Printf("                 and %d more database(s)\n", len(order)-len(shown))
	}
}

// missingCollections names collections the fast tier has that the archive does
// not, which is the largest thing standing between an operator and going back
// and the one this command used to say nothing about.
//
// The archive is the complete copy of what is *tiered*. Everything else has
// lived on the fast tier alone since the cutover: the adoption runbook has an
// operator copy those collections up at the start, and nothing ever copies them
// back down. Rolling back without them repoints an application at a cluster
// holding its orders and not its catalogue.
//
// Measured on the shop harness, which is the first thing to rehearse the whole
// procedure rather than the command: the fast tier held customers, order_events,
// orders, products and store_marker, the archive held order_events and orders,
// and the application would not start. The command reported the accounts and
// said "the archive already answers the way the router does".
//
// Views are excluded because they are reported on their own line above, and
// system collections because they are a node's own bookkeeping.
func missingCollections(ctx context.Context, hot, cold *mongo.Client) []string {
	h, err := collectionsIn(ctx, hot)
	if err != nil {
		return nil
	}
	c, err := collectionsIn(ctx, cold)
	if err != nil {
		return nil
	}
	var out []string
	for n := range h {
		if !c[n] {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

// collectionsIn names every ordinary collection on a cluster, skipping the
// databases that belong to a node rather than to an application.
func collectionsIn(ctx context.Context, client *mongo.Client) (map[string]bool, error) {
	dbs, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, db := range dbs {
		switch db {
		case "admin", "local", "config":
			continue
		}
		if isBookkeepingDB(db) {
			continue
		}
		names, lerr := client.Database(db).ListCollectionNames(ctx,
			bson.D{{Key: "type", Value: "collection"}})
		if lerr != nil {
			return nil, lerr
		}
		for _, n := range names {
			if strings.HasPrefix(n, "system.") || meta.IsScratch(n) {
				continue
			}
			out[db+"."+n] = true
		}
	}
	return out, nil
}

// missingAccounts names accounts the fast tier has and the archive does not.
func missingAccounts(ctx context.Context, hot, cold *mongo.Client) []string {
	h, err := accountsIn(ctx, hot)
	if err != nil {
		return nil
	}
	c, err := accountsIn(ctx, cold)
	if err != nil {
		return nil
	}
	var out []string
	for u := range h {
		if !c[u] {
			out = append(out, u)
		}
	}
	sort.Strings(out)
	return out
}

func accountsIn(ctx context.Context, client *mongo.Client) (map[string]bool, error) {
	cur, err := client.Database("admin").Collection("system.users").Find(ctx, bson.D{},
		options.Find().SetProjection(bson.D{{Key: "user", Value: 1}, {Key: "db", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	out := map[string]bool{}
	for cur.Next(ctx) {
		var u struct {
			User string `bson:"user"`
			DB   string `bson:"db"`
		}
		if cur.Decode(&u) != nil || u.User == "" {
			continue
		}
		out[u.User+"@"+u.DB] = true
	}
	return out, cur.Err()
}

// missingViews names views the fast tier has and the archive does not.
func missingViews(ctx context.Context, hot, cold *mongo.Client) []string {
	h, err := viewsIn(ctx, hot)
	if err != nil {
		return nil
	}
	c, err := viewsIn(ctx, cold)
	if err != nil {
		return nil
	}
	var out []string
	for v := range h {
		if !c[v] {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

func viewsIn(ctx context.Context, client *mongo.Client) (map[string]bool, error) {
	dbs, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, db := range dbs {
		switch db {
		case "admin", "local", "config":
			continue
		}
		if isBookkeepingDB(db) {
			continue
		}
		names, lerr := client.Database(db).ListCollectionNames(ctx,
			bson.D{{Key: "type", Value: "view"}})
		if lerr != nil {
			return nil, lerr
		}
		for _, n := range names {
			if strings.HasPrefix(n, "system.") {
				continue
			}
			out[db+"."+n] = true
		}
	}
	return out, nil
}
