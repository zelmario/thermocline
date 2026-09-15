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

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/watermark"
)

// runBackfill closes the hole a stopped feeder leaves.
//
// The feeder exits when the fast tier's oplog has rolled past its checkpoint,
// because reopening cannot fetch the writes made in that window: those oplog
// entries no longer exist. What the operator is then told to do is copy the
// affected namespaces into the archive and clear the checkpoint, and until this
// existed there was no command for the copying, so it was a script written
// during an incident.
//
// A script written during an incident closes two of the three things that can
// have happened in the window. Measured on ten mirrored documents with the
// feeder stopped, one update, one insert and one delete against the fast tier,
// and the remedy then followed exactly: the update came back right, the insert
// arrived, and the deleted document stayed live in the archive and was served
// to a client. The router counted eleven where the application had left ten.
//
// It is not an oversight in the remedy, it is what the remedy can see. A delete
// leaves nothing on the fast tier to copy, so copying the fast tier cannot know
// it happened.
//
// What can know is the completeness claim. Inside the range the fast tier claims
// to hold in full, every live document is on the fast tier by definition, so one
// the archive holds live and the fast tier does not is one that was deleted.
// That is the second half, and it is why this needs a claim to do it: without
// one there is no range in which absence means anything, and marking on a guess
// would delete data rather than recover it.
func runBackfill(args []string) error {
	fs := flag.NewFlagSet("backfill", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	nsFlag := fs.String("ns", "", "one namespace, rather than every tiered one")
	apply := fs.Bool("apply", false, "make the changes, rather than reporting them")
	force := fs.Bool("force", false, "apply even though a feeder still looks alive")
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

	namespaces, err := backfillNamespaces(ctx, cfg, cold, *nsFlag)
	if err != nil {
		return err
	}
	sort.Strings(namespaces)

	// The same ordering rollback insists on, for the same reason: a feeder
	// still mirroring is writing into the window this is trying to close, so
	// what it copies is a moving target and what it marks may be a document the
	// feeder is about to put back.
	alive, since := feederPublishing(ctx, cold, cfg.Feeder.ID)
	if *apply && alive && !*force {
		fmt.Printf("A feeder published %s ago, so it is still mirroring.\n\n", since.Round(time.Second))
		fmt.Print(`Stop it first. This exists for the window where it was not running, and
running it underneath one copies a moving target and can mark a document the
feeder is about to write back.

--force applies anyway, which is only right if you know nothing is mirroring.
`)
		return fmt.Errorf("a feeder is still publishing")
	}

	if !*apply {
		fmt.Println("Reporting only. Nothing is written without --apply.")
	}
	fmt.Println()

	marks := watermark.New(cold)
	var totalCopied, totalMarked int64
	for _, ns := range namespaces {
		db, coll := splitNS(ns)
		if db == "" || coll == "" {
			continue
		}
		copied, cerr := backfillDocuments(ctx, hot, cold, db, coll, *apply)
		if cerr != nil {
			fmt.Printf("  %-40s could not be copied: %v\n", ns, cerr)
			continue
		}
		totalCopied += copied

		marked, why := backfillDeletes(ctx, cfg, marks, hot, cold, ns, db, coll, *apply)
		totalMarked += marked
		verb := "would copy"
		if *apply {
			verb = "copied"
		}
		fmt.Printf("  %-40s %s %d, %s\n", ns, verb, copied, why)
	}

	fmt.Println()
	if *apply {
		fmt.Printf("Copied %d document(s) into the archive and marked %d as deleted.\n", totalCopied, totalMarked)
		fmt.Println("Now clear the feeder's checkpoint and start it again.")
		return nil
	}
	fmt.Printf("Would copy %d document(s) into the archive and mark %d as deleted.\n", totalCopied, totalMarked)
	fmt.Println("Re-run with --apply to do it.")
	return nil
}

// backfillNamespaces is what to work on, honouring the empty list that means
// everything.
func backfillNamespaces(ctx context.Context, cfg config.Config, cold *mongo.Client, only string) ([]string, error) {
	if only != "" {
		return []string{only}, nil
	}
	if len(cfg.Feeder.Namespaces) > 0 {
		return cfg.Feeder.Namespaces, nil
	}
	fmt.Println("No namespaces are configured, so every collection is tiered.")
	fmt.Println("Asking the archive which ones it holds.")
	found, err := archiveNamespaces(ctx, cold)
	if err != nil {
		return nil, fmt.Errorf("could not ask the archive which collections it holds, so this "+
			"cannot know what to look at; name one with --ns: %w", err)
	}
	if len(found) == 0 {
		return nil, fmt.Errorf("the archive holds no collections to look at")
	}
	fmt.Printf("Found %d.\n", len(found))
	return found, nil
}

// backfillBatch is how many documents travel per bulk write. The same trade the
// feeder makes: large enough that the round trip is not the cost, small enough
// that one batch is not a 16MB message.
const backfillBatch = 500

// backfillDocuments copies every document the fast tier holds into the archive.
//
// Upserted by _id, because the fast tier's copy is the newer one: it is what the
// application wrote during the window, and the archive's is what it looked like
// before. A document the archive does not have at all is created.
func backfillDocuments(ctx context.Context, hot, cold *mongo.Client, db, coll string, apply bool) (int64, error) {
	src := hot.Database(db).Collection(coll)
	dst := cold.Database(db).Collection(coll)

	cur, err := src.Find(ctx, bson.D{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var n int64
	var models []mongo.WriteModel
	flush := func() error {
		if len(models) == 0 {
			return nil
		}
		if apply {
			if _, werr := dst.BulkWrite(ctx, models,
				options.BulkWrite().SetOrdered(false).SetBypassDocumentValidation(true)); werr != nil {
				return werr
			}
		}
		models = models[:0]
		return nil
	}
	for cur.Next(ctx) {
		var doc bson.Raw = cur.Current
		id, lerr := doc.LookupErr("_id")
		if lerr != nil {
			continue
		}
		models = append(models, mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: id}}).
			SetReplacement(doc).
			SetUpsert(true))
		n++
		if len(models) >= backfillBatch {
			if ferr := flush(); ferr != nil {
				return n, ferr
			}
		}
	}
	if cerr := cur.Err(); cerr != nil {
		return n, cerr
	}
	return n, flush()
}

// backfillDeletes marks the archive's copies of documents the application
// deleted while nothing was mirroring, and says why it could not when it could
// not.
//
// Only inside the range the fast tier claims to hold in full. That claim is what
// makes absence mean something: every live document newer than the claim's
// instant is on the fast tier, so one the archive holds live from that range and
// the fast tier does not is one that was deleted. Outside it, the fast tier is
// not expected to hold anything and absence means nothing at all.
//
// The claim is read in the dimension the rules tier by, which is the same field
// every completeness decision is made in. Reading it in any other would compare
// a bound against a different clock.
func backfillDeletes(ctx context.Context, cfg config.Config, marks *watermark.Store,
	hot, cold *mongo.Client, ns, db, coll string, apply bool) (int64, string) {

	mark, err := marks.Load(ctx, ns)
	if err != nil {
		return 0, fmt.Sprintf("could not read the completeness claim: %v", err)
	}
	if !mark.Claimed {
		return 0, "no deletes looked for: nothing claims the fast tier is complete for any range of it"
	}
	field, ok := cfg.Router.AgeField(ns)
	if !ok {
		return 0, "no deletes looked for: no rule says which field this namespace ages by"
	}

	// The claim's own dimension. An empty field name means the timestamp inside
	// a default ObjectId, which is what a rule with no date_field uses.
	var inRange bson.D
	if field == "" {
		inRange = bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: bson.NewObjectIDFromTimestamp(mark.From)}}}}
	} else {
		inRange = bson.D{{Key: field, Value: bson.D{{Key: "$gte", Value: mark.From}}}}
	}

	filter := bson.D{
		{Key: "$and", Value: bson.A{
			inRange,
			bson.D{{Key: meta.TombstoneField, Value: bson.D{{Key: "$exists", Value: false}}}},
		}},
	}
	cur, err := cold.Database(db).Collection(coll).Find(ctx, filter,
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return 0, fmt.Sprintf("could not read the archive for deletes: %v", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	src := hot.Database(db).Collection(coll)
	dst := cold.Database(db).Collection(coll)

	// What the claim asserts, checked before anything is marked on the strength
	// of it. Every live document in this range is supposed to be on the fast
	// tier, so a fast tier holding none of them is not a gap to close, it is a
	// claim that is not true, and acting on it would tombstone the archive
	// rather than recover it. `cline watermark --complete-all` on a fast tier
	// that has not been warmed is one line away from that.
	inFast, ferr := src.CountDocuments(ctx, inRange)
	if ferr != nil {
		return 0, fmt.Sprintf("could not read the fast tier for the claimed range: %v", ferr)
	}
	inCold, cerr := cold.Database(db).Collection(coll).CountDocuments(ctx, filter)
	if cerr != nil {
		return 0, fmt.Sprintf("could not read the archive for the claimed range: %v", cerr)
	}
	if inCold > 0 && inFast == 0 {
		return 0, fmt.Sprintf("no deletes marked: the claim says the fast tier holds this range in full "+
			"and it holds none of it, so the claim is wrong rather than the archive; "+
			"withdraw it with cline watermark --ns %s --clear, or warm the range first", ns)
	}

	// And the softer half of the same doubt. A claim can be wrong without being
	// obviously wrong, and marking most of a namespace deleted is not what
	// closing a feeder's gap looks like: the gap is the writes made while it was
	// down, not the collection. Reported and refused rather than done, because
	// the operator is the one who knows whether their fast tier really lost that
	// much.
	const mostOfIt = 0.5
	var marked int64
	now := time.Now().UTC()
	var candidates []bson.RawValue
	for cur.Next(ctx) {
		id, lerr := cur.Current.LookupErr("_id")
		if lerr != nil {
			continue
		}
		// One lookup per candidate rather than loading the fast tier's whole
		// key set: the candidates are the claimed range of one namespace, and
		// an _id lookup is the cheapest question there is.
		n, cerr := src.CountDocuments(ctx, bson.D{{Key: "_id", Value: id}}, options.Count().SetLimit(1))
		if cerr != nil || n > 0 {
			continue
		}
		candidates = append(candidates, id)
	}
	if cerr := cur.Err(); cerr != nil {
		return 0, fmt.Sprintf("stopped reading the archive: %v", cerr)
	}

	if inCold > 0 && float64(len(candidates)) > mostOfIt*float64(inCold) {
		return 0, fmt.Sprintf("no deletes marked: %d of the %d live documents the archive holds in the "+
			"claimed range are missing from the fast tier, which is most of it and not the shape of a "+
			"feeder's gap; check the claim before trusting it", len(candidates), inCold)
	}

	for _, id := range candidates {
		marked++
		if apply {
			if _, uerr := dst.UpdateOne(ctx,
				bson.D{{Key: "_id", Value: id}}, meta.Tombstone(now)); uerr != nil {
				return marked, fmt.Sprintf("could not mark %s: %v", id.String(), uerr)
			}
		}
	}

	verb := "would mark"
	if apply {
		verb = "marked"
	}
	return marked, fmt.Sprintf("%s %d deleted in the claimed range", verb, marked)
}
