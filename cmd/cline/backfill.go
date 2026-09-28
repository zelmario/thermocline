package main

import (
	"context"
	"errors"
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
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/pairing"
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
	clearCP := fs.Bool("clear-checkpoint", false,
		"also clear the feeder's checkpoint, so mirroring starts again from now")
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
	// A command that writes checks the pairing the daemons check, because it
	// writes into whichever archive cold.uri reaches. See internal/pairing.
	if err := pairing.Check(ctx, hot, cold, log); err != nil {
		return err
	}

	namespaces, err := backfillNamespaces(ctx, cfg, cold, *nsFlag)
	if err != nil {
		return err
	}
	sort.Strings(namespaces)

	// A running feeder holds the marking and not the copying, and the difference
	// is what each half does when it is wrong.
	//
	// Copying under a feeder is harmless: it upserts what the fast tier holds,
	// which is what the feeder would have mirrored anyway. Marking is not. The
	// feeder may be writing a document back at the moment this decides the fast
	// tier does not have it, and then the mark lands on a live document.
	//
	// Holding both was the first version and it made the ordinary use
	// impossible. Tiering a collection that already has data means adding it to
	// feeder.namespaces, starting the feeder so nothing new is missed, and then
	// copying what was already there. Refusing while the feeder runs leaves only
	// the other order, and that one has a window in it.
	alive, since := feederPublishing(ctx, cold, cfg.Feeder.ID)
	markingHeld := alive && !*force
	if markingHeld {
		fmt.Printf("A feeder published %s ago, so it is still mirroring.\n", since.Round(time.Second))
		fmt.Println("Documents are copied anyway. Deletes are not marked, because the feeder may be")
		fmt.Println("writing back the very document this would mark. Stop it, or --force, to mark them.")
		fmt.Println()
	}

	if !*apply {
		fmt.Println("Reporting only. Nothing is written without --apply.")
	}
	fmt.Println()

	marks := watermark.New(cold)
	var totalCopied, totalMarked, totalRefused int64
	for _, ns := range namespaces {
		db, coll := splitNS(ns)
		if db == "" || coll == "" {
			continue
		}
		copied, refused, alreadyMarked, refusalWhy, cerr := backfillDocuments(ctx, hot, cold, db, coll, *apply)
		if cerr != nil {
			fmt.Printf("  %-40s could not be copied: %v\n", ns, cerr)
			continue
		}
		totalCopied += copied
		// Documents the archive would not take, named rather than folded into
		// the count. The namespace is still short by these and the router
		// refuses the reads that would be short by them, so an operator who
		// reads only the total would be told this namespace is done.
		if refused > 0 {
			totalRefused += refused
			fmt.Printf("  %-40s %d document(s) the archive refused: %s\n", ns, refused,
				firstLine(refusalWhy))
		}
		// Copied, and invisible once copied. See backfillDocuments.
		if alreadyMarked > 0 {
			fmt.Printf("  %-40s %d document(s) already carry %s or %s, so the archive's copy of "+
				"each reads as one the application deleted and no read that falls through will "+
				"return it. cline preflight lists them; rename the field on the fast tier and "+
				"run this again\n", ns, alreadyMarked, meta.TombstoneField, meta.BodyDroppedField)
		}

		marked, why := backfillDeletes(ctx, cfg, marks, hot, cold, ns, db, coll, *apply, markingHeld)
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
		return finishBackfill(ctx, cold, cfg, *nsFlag, *clearCP, markingHeld, totalRefused)
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

// backfillBatchBytes bounds a batch in bytes as well. See the copy loop.
const backfillBatchBytes = 16 << 20

// backfillDocuments copies every document the fast tier holds into the archive.
//
// Upserted by _id, because the fast tier's copy is the newer one: it is what the
// application wrote during the window, and the archive's is what it looked like
// before. A document the archive does not have at all is created.
func backfillDocuments(ctx context.Context, hot, cold *mongo.Client, db, coll string, apply bool) (copied, refusedOut, markedOut int64, why string, err error) {
	src := hot.Database(db).Collection(coll)
	dst := cold.Database(db).Collection(coll)

	cur, err := src.Find(ctx, bson.D{})
	if err != nil {
		return 0, 0, 0, "", err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var n, refused, marked int64
	var firstRefusal string
	var models []mongo.WriteModel
	// Unordered, so the archive applies what it can and refuses the rest one at
	// a time. The driver reports that as an error even though most of the batch
	// landed, and this used to return it: one document the archive would not
	// take made the whole namespace read "could not be copied", with the
	// documents that were copied uncounted.
	//
	// Measured on three documents where the archive's unique index refuses one:
	// "could not be copied", "Copied 0 document(s)", and then the closing line
	// telling the operator that what was on the fast tier is in the archive now.
	// That is a recovery command reporting success for work it did not do, in
	// the one state where somebody is running it because something is already
	// wrong.
	flush := func() error {
		if len(models) == 0 {
			return nil
		}
		if apply {
			_, werr := dst.BulkWrite(ctx, models,
				options.BulkWrite().SetOrdered(false).SetBypassDocumentValidation(true))
			if werr != nil {
				var bwe mongo.BulkWriteException
				if !errors.As(werr, &bwe) || len(bwe.WriteErrors) == 0 {
					return werr
				}
				// Each write error is one document the archive will not take,
				// and the rest of the batch is in. A duplicate key here is the
				// same condition the feeder gives up on: another live archive
				// document holds that value, and no retry changes it.
				for _, we := range bwe.WriteErrors {
					refused++
					if firstRefusal == "" {
						firstRefusal = we.Message
					}
				}
				n -= int64(len(bwe.WriteErrors))
			}
		}
		models = models[:0]
		return nil
	}
	held := 0
	for cur.Next(ctx) {
		// A copy: these are kept in models until the batch is written, and the
		// driver says Current is valid only until the next call to Next. The
		// same fault 415e995 fixed in the router's copy-up, here in the tool
		// that writes the archive during a recovery.
		doc := append(bson.Raw(nil), cur.Current...)
		held += len(doc)
		id, lerr := doc.LookupErr("_id")
		if lerr != nil {
			continue
		}
		// A fast tier document already wearing one of this system's own marks.
		// The router refuses writes that would leave one and the feeder warns
		// when it mirrors one, but this tool copies verbatim, and a copy of a
		// marked document is a tombstone in the archive: invisible to every
		// read that falls through, and reported here as copied. It is the state
		// preflight's findReservedFields exists to find, and a recovery tool is
		// exactly where nobody has run preflight first.
		//
		// Counted and said, not refused. Refusing would stop a backfill part
		// way through for a document the archive would have taken, and what the
		// operator needs is to know which documents to look at.
		for _, f := range []string{meta.TombstoneField, meta.BodyDroppedField} {
			if _, err := doc.LookupErr(f); err == nil {
				marked++
				break
			}
		}
		models = append(models, meta.ReplaceModel(bson.D{{Key: "_id", Value: id}}, doc))
		n++
		// In bytes as well as documents, for the reason the feeder's flush and
		// the warmer's batch are: a batch of large documents is otherwise held
		// whole, whatever its size.
		if len(models) >= backfillBatch || held >= backfillBatchBytes {
			if ferr := flush(); ferr != nil {
				return n, refused, marked, firstRefusal, ferr
			}
			held = 0
		}
	}
	if cerr := cur.Err(); cerr != nil {
		return n, refused, marked, firstRefusal, cerr
	}
	if ferr := flush(); ferr != nil {
		return n, refused, marked, firstRefusal, ferr
	}
	return n, refused, marked, firstRefusal, nil
}

// firstLine trims a driver's error to something that fits a report line. The
// whole message carries the index name and the key, which is what an operator
// needs; the stack of wrappers around it is not.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 140 {
		s = s[:140] + "..."
	}
	return s
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
	hot, cold *mongo.Client, ns, db, coll string, apply, held bool) (int64, string) {

	if held {
		return 0, "no deletes looked for: a feeder is still publishing"
	}
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
		// Copied out of Current, which is valid only until the next call to
		// Next. These ids are marked deleted in the archive below, so a buffer
		// the driver reused would mark the wrong documents.
		candidates = append(candidates, bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)})
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

// finishBackfill clears the feeder's checkpoint, or says why it did not.
//
// The step after the copy, and until now the one with no command behind it. The
// feeder's own error says to clear the checkpoint and so did this command, and
// the only way to do it was to delete a row out of the bookkeeping collection by
// hand, during an incident, against the right tier: run against the fast tier it
// does nothing and looks like it worked, which is the same way the hand-typed
// rollback went wrong before `cline rollback` existed.
//
// Not done by default, because it is the irreversible half: clearing restarts
// mirroring from now, and if the copy above did not cover every affected
// namespace then the rest of the hole is sealed in rather than closed.
//
// Refused outright after --ns for that exact reason. One namespace was copied
// and the checkpoint is not per namespace.
func finishBackfill(ctx context.Context, cold *mongo.Client, cfg config.Config,
	only string, clear, feederRunning bool, totalRefused int64) error {

	// Which of the two jobs this was decides what to say next, and saying the
	// wrong one is worse than saying nothing. A feeder that is running means
	// this was not a gap being closed: it was a collection with data in it
	// being tiered, and its checkpoint is fine. Telling an operator to clear it
	// there would restart mirroring from now for every namespace, for no
	// reason.
	// Said only when it is true. With documents the archive refused, what was on
	// the fast tier is precisely not all in the archive now, and this is the
	// line an operator reads as "the gap is closed".
	if totalRefused > 0 {
		fmt.Printf("\n%d document(s) could not be copied, so this namespace is still short by them\n"+
			"and the router refuses the reads that would be short: the archive already holds\n"+
			"another live document with the value each of them carries. Resolve those\n"+
			"duplicates and write the documents again; cline preflight lists them.\n",
			totalRefused)
	}
	if feederRunning {
		if totalRefused == 0 {
			fmt.Println("The feeder is mirroring, so there is no checkpoint to clear: what was already")
			fmt.Println("on the fast tier is in the archive now, and what happens next it will carry.")
		} else {
			fmt.Println("The feeder is mirroring, so there is no checkpoint to clear, and it will carry")
			fmt.Println("what happens next. What it will not carry is the documents above.")
		}
		return nil
	}
	if !clear {
		fmt.Println("Now clear the feeder's checkpoint and start it again, or re-run with")
		fmt.Println("--clear-checkpoint to have this do it.")
		return nil
	}
	if only != "" {
		fmt.Println()
		fmt.Print(`Not clearing the checkpoint: this ran over one namespace and the checkpoint is
not per namespace. Clearing it now would restart mirroring from now for every
other namespace too, sealing in whatever hole they have rather than closing it.

Run this without --ns first.
`)
		return fmt.Errorf("--clear-checkpoint needs a run over every namespace")
	}

	res, err := cold.Database(meta.DB).Collection(meta.Checkpoints).
		DeleteOne(ctx, bson.D{{Key: "_id", Value: cfg.Feeder.ID}})
	if err != nil {
		return fmt.Errorf("could not clear the checkpoint for feeder %q: %w", cfg.Feeder.ID, err)
	}
	if res.DeletedCount == 0 {
		fmt.Printf("No checkpoint was stored for feeder %q, so mirroring would have started from now anyway.\n",
			cfg.Feeder.ID)
	} else {
		fmt.Printf("Cleared the checkpoint for feeder %q.\n", cfg.Feeder.ID)
	}
	fmt.Println("Start the feeder. Writes made between the copy above and it starting are not")
	fmt.Println("covered by either, so do this with the application stopped if you can.")
	return nil
}
