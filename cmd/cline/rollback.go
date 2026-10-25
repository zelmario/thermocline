package main

import (
	"context"
	"errors"
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
	"github.com/zelmario/thermocline/internal/lease"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/pairing"
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
		// One the archive does not hold is a typo, not a clean namespace: it
		// counted nought and printed that the archive already answers the way
		// the router does, about a collection nothing had looked at.
		db, coll := splitNS(*nsFlag)
		if db == "" || coll == "" {
			return fmt.Errorf("--ns %q is not db.collection", *nsFlag)
		}
		names, lerr := cold.Database(db).ListCollectionNames(ctx, bson.D{{Key: "name", Value: coll}})
		if lerr != nil {
			return fmt.Errorf("could not ask the archive whether it holds %s: %w", *nsFlag, lerr)
		}
		if len(names) == 0 {
			return fmt.Errorf("the archive holds no collection %s; check the name", *nsFlag)
		}
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
	gaps := reportArchiveGaps(ctx, cfg, namespaces, log)

	alive, since := feederPublishing(ctx, cold, cfg.Feeder.ID)
	if *clear && alive && !*force {
		fmt.Printf("%s\n\n", feederSays(since))
		fmt.Print(`Clearing now would leave fresh marks behind the ones it removed: every delete
that reaches the archive after this point writes another. Stop the application
and the feeder first, let mirroring finish, then run this again.

--force clears anyway, which is only right if you know nothing is writing.
`)
		return fmt.Errorf("a feeder is still publishing")
	}

	total := int64(0)
	// Every namespace that could not be read or cleared. Either reassuring
	// ending below used to be printed whatever happened here: a count that
	// failed added nothing to the total, so "nothing is marked deleted" was
	// printed over a namespace nobody had looked at, and "point the
	// application at it" over one whose clear had failed, with exit status 0.
	failed := 0
	for _, ns := range namespaces {
		db, coll := splitNS(ns)
		if db == "" || coll == "" {
			fmt.Printf("  %-40s is not db.collection, so nothing was read\n", ns)
			failed++
			continue
		}
		c := cold.Database(db).Collection(coll)
		filter := bson.D{{Key: "_tcDeletedAt", Value: bson.D{{Key: "$exists", Value: true}}}}
		n, cerr := c.CountDocuments(ctx, filter)
		if cerr != nil {
			fmt.Printf("  %-40s could not be read: %v\n", ns, cerr)
			failed++
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
			failed++
			continue
		}
		fmt.Printf("  %-40s cleared %d\n", ns, res.DeletedCount)
	}

	fmt.Print("\n")
	if failed > 0 {
		fmt.Printf("%d namespace(s) above could not be read or cleared, so the archive is not ready.\n", failed)
		fmt.Println("Fix what each line says and run this again; do not point the application at it yet.")
		return fmt.Errorf("%d namespace(s) could not be read or cleared", failed)
	}
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
//
// And the feeder's lease, which it renews every few seconds whatever
// position_every says: a feeder told to publish no position wrote none, and
// this read its absence as no feeder at all, so rollback --clear went ahead
// beside one that was still writing. Both are judged against the archive's own
// clock, which is where both were stamped.
//
// When neither can be read the answer is yes, with since negative: refusing
// to clear beside a feeder that may be running is the side to err on, and
// --force is there for an operator who knows better.
func feederPublishing(ctx context.Context, cold *mongo.Client, id string) (bool, time.Duration) {
	var clock struct {
		LocalTime time.Time `bson:"localTime"`
	}
	if err := cold.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&clock); err != nil ||
		clock.LocalTime.IsZero() {
		return true, -1
	}
	now := clock.LocalTime

	var pos struct {
		SavedAt time.Time `bson:"savedAt"`
		EveryMS int64     `bson:"everyMS"`
	}
	err := cold.Database(meta.DB).Collection(meta.PositionsColl).
		FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&pos)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return true, -1
	}
	if err == nil && !pos.SavedAt.IsZero() {
		every := time.Duration(pos.EveryMS) * time.Millisecond
		if every <= 0 {
			every = time.Second
		}
		stale := 4 * every
		if stale < 2*time.Second {
			stale = 2 * time.Second
		}
		if age := now.Sub(pos.SavedAt); age <= stale {
			return true, age
		}
	}

	var held struct {
		Seen time.Time `bson:"seen"`
	}
	err = cold.Database(meta.DB).Collection(meta.Markers).
		FindOne(ctx, bson.D{{Key: "_id", Value: "lease:feeder:" + id}}).Decode(&held)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return true, -1
	}
	if err == nil && !held.Seen.IsZero() {
		if age := now.Sub(held.Seen); age <= lease.Stale {
			return true, age
		}
	}
	if !pos.SavedAt.IsZero() {
		return false, now.Sub(pos.SavedAt)
	}
	return false, 0
}

// feederSays is how a caller of feederPublishing names what it found.
func feederSays(since time.Duration) string {
	if since < 0 {
		return "Whether a feeder is still running could not be read, so this assumes one is."
	}
	return fmt.Sprintf("A feeder was last seen %s ago, so it is still mirroring.", since.Round(time.Second))
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
func reportArchiveGaps(ctx context.Context, cfg config.Config, namespaces []string, log *slog.Logger) bool {
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
	// Said, not refused: a rollback runs during an incident and must not
	// depend on anything but the archive. But an archive the fast tier has not
	// been working with is one the application is about to be pointed at
	// without its own data, which is the one thing this section exists to say.
	if perr := pairing.Check(ctx, hot, cold, log); perr != nil {
		fmt.Printf("WARNING: %v\n\n", perr)
	}
	defer disconnect(cold, log)

	users := missingAccounts(ctx, hot, cold)
	views := missingViews(ctx, hot, cold)
	colls := missingCollections(ctx, hot, cold)
	// The collections the archive has never held are not asked about again for
	// their indexes: they are named below on their own line, and an index gap
	// under a collection that is wholly missing reads as two problems.
	absent := map[string]bool{}
	for _, ns := range colls {
		absent[ns] = true
	}
	idx := missingIndexes(ctx, hot, cold, namespaces, absent)
	if len(users) == 0 && len(views) == 0 && len(colls) == 0 && len(idx) == 0 {
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
	if len(idx) > 0 {
		printIndexGaps(idx)
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

// indexGap is one tiered collection's indexes that an application repointed at
// the archive would not have.
//
// Two lists rather than one, because "the archive does not have it" and "the
// archive has one that does not enforce the same thing" are different states
// with the same consequence, and only the first is visible to a comparison that
// matches on names and keys. See lostUniqueConstraints in indexes.go, which is
// this question asked in the other direction and was wrong in exactly this way
// before anybody tabulated both sides.
type indexGap struct {
	ns      string
	missing []indexInfo // no counterpart on the archive at all
	weaker  []indexInfo // a counterpart exists and does not enforce the same constraint
}

// missingIndexes compares the two tiers' indexes for the collections that tier.
//
// `cline indexes` already answers this question and answers it for a different
// purpose: while the router is in front, an index the archive lacks is a
// fall-through read that scans, and its unique flag is deliberately not carried
// across because the key registry enforces uniqueness between the tiers and a
// unique index on the archive can refuse a write the feeder has to make.
//
// Rollback is the one case where that reasoning inverts. The router is being
// taken away and the key registry lives on the fast tier, so after the cutover
// a constraint the application declared is enforced by nothing at all, and the
// next duplicate is accepted silently by a cluster that looks healthy.
//
// Measured on the shop, 2026-09-24, and it is the reason this exists: the
// rehearsal ended with the application running against the archive and three
// duplicate order references accepted, on a collection whose fast tier copy
// carries {ref: 1} unique with a case-insensitive collation. The archive held
// _id_ and nothing else. Twelve of the shop's own checks passed and the
// constraint was gone.
//
// Collections the archive has never held are left out: they are reported above
// as collections, and an index on a collection that is not there is noise under
// a line that already says the whole thing is missing.
func missingIndexes(ctx context.Context, hot, cold *mongo.Client, namespaces []string, absent map[string]bool) []indexGap {
	var out []indexGap
	for _, ns := range namespaces {
		if absent[ns] {
			continue
		}
		db, coll := splitNS(ns)
		if db == "" || coll == "" {
			continue
		}
		h, err := indexesOf(ctx, hot, db, coll)
		if err != nil {
			continue
		}
		c, err := indexesOf(ctx, cold, db, coll)
		if err != nil {
			continue
		}
		gap := indexGapFor(ns, h, c)
		if len(gap.missing) > 0 || len(gap.weaker) > 0 {
			out = append(out, gap)
		}
	}
	return out
}

// indexGapFor compares one collection's two listings.
//
// Split out from missingIndexes so that the comparison a test can reach is the
// comparison the command makes. A test that rebuilds this loop over its own
// fixtures asserts that the fixture agrees with itself.
func indexGapFor(ns string, hot, cold map[string]indexInfo) indexGap {
	gap := indexGap{ns: ns}
	gap.missing = append(gap.missing, absentFrom(hot, cold)...)
	// The other half of the same question. An index the archive carries under
	// the same name and keys with unique gone enforces nothing, and absentFrom
	// calls it present.
	for _, f := range hot {
		if f.name == "_id_" || !f.unique {
			continue
		}
		a, ok := counterpartOf(f, cold)
		if !ok {
			continue // Already in missing, above.
		}
		if !a.enforcesSameAs(f) {
			gap.weaker = append(gap.weaker, f)
		}
	}
	sort.Slice(gap.missing, func(i, j int) bool { return gap.missing[i].name < gap.missing[j].name })
	sort.Slice(gap.weaker, func(i, j int) bool { return gap.weaker[i].name < gap.weaker[j].name })
	return gap
}

// printIndexGaps says what the application loses and what to run to get it back.
//
// Constraints first and separately from the rest. An index that is only an
// optimisation costs the application speed and the operator can measure it
// afterwards; a unique index is a guarantee the application was written to rely
// on, and nothing measures its absence until two customers are tracking one
// parcel. They are the same object to MongoDB and they are not the same risk,
// so they are not printed in one list.
//
// Sorted into the three buckets once and printed from them, rather than
// counted by one rule and printed by another. The first version of this counted
// a unique TTL index as expiring and emitted a createIndex for it under
// constraints, so the number at the top and the list below it described
// different sets. See [[count-the-list-in-the-prose]]: a count is a claim about
// the list under it.
func printIndexGaps(gaps []indexGap) {
	type placed struct {
		ns string
		ix indexInfo
	}
	var constraints, plain, expiring []placed
	for _, g := range gaps {
		// A TTL index goes to the expiring list whatever else it is. It is the
		// one kind whose remedy is a decision rather than a command, and a
		// unique one is still an index whose job is to delete the history the
		// archive exists to keep.
		for _, m := range g.weaker {
			if m.ttl {
				expiring = append(expiring, placed{g.ns, m})
				continue
			}
			constraints = append(constraints, placed{g.ns, m})
		}
		for _, m := range g.missing {
			switch {
			case m.ttl:
				expiring = append(expiring, placed{g.ns, m})
			case m.unique:
				constraints = append(constraints, placed{g.ns, m})
			default:
				plain = append(plain, placed{g.ns, m})
			}
		}
	}

	fmt.Printf("  indexes   %d the archive does not have, on collections that do tier.\n",
		len(constraints)+len(plain)+len(expiring))
	fmt.Println("            Nothing copies an index to the archive, which is right while the")
	fmt.Println("            router is in front of it and is not once it is taken away: after")
	fmt.Println("            the cutover these are the application's own indexes, gone.")

	if len(constraints) > 0 {
		fmt.Printf("\n  %d of them are unique, which is a constraint and not a speed matter.\n", len(constraints))
		fmt.Print(`  Uniqueness across the tiers is enforced by the key registry on the FAST TIER,
  which this procedure switches off. Recreate these before any application
  writes, or the first duplicate is accepted by a cluster that looks healthy.

`)
		for _, p := range constraints {
			db, coll := splitNamespace(p.ns)
			fmt.Printf("    db.getSiblingDB(%q).%s.createIndex(%s, %s)\n",
				db, coll, p.ix.keyJSON, createOptions(p.ix))
		}
		fmt.Print(`
  A build that fails names two documents already sharing a value. That is a
  duplicate the constraint was supposed to prevent and did not, because the
  registry protects values it knows about and an adoption's history predates it.
  Settle those documents before the cutover, not afterwards.
`)
	}

	if len(plain) > 0 {
		fmt.Printf("\n  %d are plain indexes. The application's queries ran with them and will\n", len(plain))
		fmt.Println("  scan the archive without them, on the tier that holds the most documents.")
		fmt.Print("\n")
		for _, p := range plain {
			db, coll := splitNamespace(p.ns)
			fmt.Printf("    db.getSiblingDB(%q).%s.createIndex(%s, %s)\n",
				db, coll, p.ix.keyJSON, createOptions(p.ix))
		}
	}

	if len(expiring) > 0 {
		fmt.Printf("\n  %d expire documents, and copying one across is a decision rather than a\n", len(expiring))
		fmt.Print(`  step. The archive holds history the fast tier stopped holding, and a TTL
  index built there deletes it on the schedule the application set for a
  collection that was only ever recent. Decide what that retention should mean
  for the whole history before building it, and nothing reports what it removes.

`)
		for _, p := range expiring {
			db, coll := splitNamespace(p.ns)
			fmt.Printf("    %s.%s  %s  expireAfterSeconds: %g\n", db, coll, p.ix.spec, p.ix.ttlSecs)
		}
	}
}

// createOptions writes one index's options the way createIndex takes them back.
//
// unique is carried here and is deliberately left off by printCreateCommands in
// indexes.go. The same index, the same two tiers, opposite answers, because the
// question is different: that one is arming an archive that a router still
// fronts, where a unique index can refuse a write the feeder must make, and this
// one is handing the archive to the application as its only cluster.
//
// partialFilterExpression, collation and sparse come along for the reason they
// do there: they say which documents the constraint covers and which values it
// treats as the same, and an index built without them enforces something the
// application never asked for.
func createOptions(m indexInfo) string {
	opts := fmt.Sprintf("{name: %q", m.name)
	if m.unique {
		opts += ", unique: true"
	}
	if m.sparse {
		opts += ", sparse: true"
	}
	if m.partial != "" {
		opts += ", partialFilterExpression: " + m.partial
	}
	if m.collation != "" {
		opts += ", collation: " + m.collation
	}
	return opts + "}"
}
