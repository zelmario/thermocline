package promote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/meta"
)

// beginOwnDDL opens a bracket around DDL this system is about to do on the fast
// tier, so a client's change stream can tell it from the application's own. It
// returns the closer, which must be called once the command has finished.
//
// A client's change stream watches the fast tier, which is where all of this
// happens, and with showExpandedEvents it carries create and createIndexes.
// Measured before this existed: one point read of one archived document, and a
// client watching was delivered create then createIndexes for a collection the
// archive had held all along, on both the collection and the database shapes of
// the stream.
//
// Opened before the command rather than recorded after it, because the event
// reaches the client as soon as the write is visible and the record has to be
// there already. A closer that is never called leaves the span open, which
// meta.OwnDDLRecord.Covers bounds rather than trusting.
func (ix *Indexer) beginOwnDDL(ctx context.Context, ns string) func() {
	from, ok := ix.hotPosition(ctx)
	if !ok {
		// Nothing to bracket with. The events go through, which is the
		// direction everything here errs in: see meta.MaxOwnDDLRecorded.
		ix.log.Debug("could not read the fast tier's write position, so a client watching with "+
			"showExpandedEvents will see this system's catalog repair", "ns", ns)
		return func() {}
	}
	// Detached, because the span has to outlive a cancelled request: the DDL
	// below happens either way and a client is watching for it.
	bctx := context.WithoutCancel(ctx)
	tok, err := meta.AnnounceOwnDDL(bctx, ix.cold, ns, from)
	if err != nil {
		ix.log.Warn("could not record that this system is about to change the fast tier's catalog",
			"ns", ns, "err", err,
			"effect", "a client watching with showExpandedEvents is told its collection or its "+
				"indexes were just created")
		return func() {}
	}
	return func() {
		to, ok := ix.hotPosition(bctx)
		if !ok {
			// The span stays open and Covers bounds it. Better than closing it
			// at a position that may be before the write.
			return
		}
		if err := meta.CloseOwnDDL(bctx, ix.cold, ns, tok, to); err != nil {
			ix.log.Warn("could not close the record of this system's own catalog change",
				"ns", ns, "err", err)
		}
	}
}

// hotPosition is the fast tier's own latest write position.
//
// hello reports it as lastWrite.opTime.ts, which is an oplog position and not a
// logical clock reading: the entry a command writes next comes strictly after
// it. The command reply's operationTime looks like the same thing and is not,
// which is the mistake this replaced. See meta.OwnDDLSpan.
func (ix *Indexer) hotPosition(ctx context.Context) (bson.Timestamp, bool) {
	raw, err := ix.hot.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Raw()
	if err != nil {
		return bson.Timestamp{}, false
	}
	v, lerr := raw.LookupErr("lastWrite", "opTime", "ts")
	if lerr != nil {
		return bson.Timestamp{}, false
	}
	t, i, ok := v.TimestampOK()
	if !ok {
		return bson.Timestamp{}, false
	}
	return bson.Timestamp{T: t, I: i}, true
}

// The fast tier is filled by copying documents, and an index is not a document,
// so a tier that has just been built holds every document the application asks
// for and no way to find any of them except by _id.
//
// That is the shape of every adoption. The existing cluster becomes the archive
// as it is, carrying the application's indexes; the fast tier is new and empty
// and the warmer fills it. Measured on a 5,000,000 order shop: after warming
// 184,521 orders into the new fast tier its only index was _id_, while the
// archive had customer_1_at_-1, at_-1 and customer_1_status_1, which are the
// three the application's queries use. The tier that exists to be fast was
// scanning, and the tier it was meant to relieve was seeking.
//
// It is worse than slow. The router reads the *fast tier's* unique indexes to
// know which values it has to protect across the tiers, so a fast tier with no
// unique index means neither mongod nor the registry is enforcing one, and a
// constraint the application has relied on for years quietly stops holding on
// the day it is adopted.
//
// So the warmer builds them before it fills a namespace, taking them from the
// archive, which is where the application's own definitions already are.
// Before rather than after, because an index built on an empty collection costs
// nothing and one built on a full one costs a scan.

// An Indexer builds them, once.
//
// Once is what the caller wants and what the fast tier wants: an index that
// exists is not created again, so the second copy-up into a namespace costs
// nothing. A namespace whose build failed is retried, but no more often than
// retryAfter, because the likeliest failure is a unique index that cannot be
// built and hammering it would fill the log rather than fix it.
type Indexer struct {
	hot, cold *mongo.Client
	log       *slog.Logger

	mu    sync.Mutex
	state map[string]indexState
	// creating is the namespaces a create is running for now, one channel each,
	// closed when it finishes. A second caller waits on it rather than going
	// ahead: see EnsureCollection for what going ahead cost.
	creating map[string]chan struct{}
}

type indexState struct {
	done    bool
	lastTry time.Time

	// building says an attempt is running now. Without it a build that hangs is
	// started again every retryAfter for as long as it hangs, and each attempt
	// holds a goroutine and a connection: on a fast tier missing a voting member
	// that was one new build a minute per namespace, for ever. See
	// createIndexes, where the commit quorum that caused it is set.
	building bool
	// again says somebody asked while that attempt was running, so what it built
	// may be a list from before a drop. The rate limit is dropped when it
	// finishes, so the next ask does the work instead of waiting out a minute it
	// has already spent.
	again bool

	// created records that the fast tier has the collection with the archive's
	// options. It is separate from done, which also means the indexes are
	// built, because the two are wanted at different moments: the options have
	// to be there before the first document is written and the indexes do not.
	created     bool
	lastCreated time.Time
}

// retryAfter bounds how often a namespace whose indexes would not build is
// tried again.
const retryAfter = time.Minute

// NewIndexer returns an Indexer that copies the archive's index definitions
// onto the fast tier.
func NewIndexer(hot, cold *mongo.Client, log *slog.Logger) *Indexer {
	return &Indexer{hot: hot, cold: cold, log: log, state: map[string]indexState{}}
}

// Ensure gives the fast tier the archive's indexes for one namespace, unless it
// already has them or has already been asked recently.
func (ix *Indexer) Ensure(ctx context.Context, ns string) {
	if ix == nil {
		return
	}
	// Never a system collection. Building one means creating it from the
	// archive's options, and capped is an option a build does not carry, so a
	// system.profile made here is not capped and mongod will not use it:
	// setProfilingLevel fails with NamespaceExists on that database from then
	// on. Guarded here as well as at the callers, because there are three of
	// them and this is the one place that does the creating.
	if _, coll := splitNS(ns); strings.HasPrefix(coll, "system.") {
		return
	}
	ix.mu.Lock()
	st := ix.state[ns]
	if st.building {
		st.again = true
		ix.state[ns] = st
		ix.mu.Unlock()
		return
	}
	if st.done || time.Since(st.lastTry) < retryAfter {
		ix.mu.Unlock()
		return
	}
	// The create half is carried through rather than reset. Ensure and
	// EnsureCollection answer different questions about the same namespace, and
	// an Ensure that forgot the collection was already made cost the next insert
	// a create it did not need. Forget is what clears it, because a drop is the
	// only thing that makes it untrue.
	ix.state[ns] = indexState{
		lastTry: time.Now(), building: true,
		created: st.created, lastCreated: st.lastCreated,
	}
	ix.mu.Unlock()

	err := ix.build(ctx, ns)

	ix.mu.Lock()
	st = ix.state[ns]
	st.building = false
	st.done = err == nil
	if st.again {
		// Asked again while this was running. What it just built may be the list
		// from before a drop, so it does not count as done and the next ask is
		// not rate limited.
		st.again = false
		st.done = false
		st.lastTry = time.Time{}
	}
	ix.state[ns] = st
	ix.mu.Unlock()

	if err != nil {
		ix.log.Warn("could not build the archive's indexes on the fast tier",
			"ns", ns, "err", err,
			"effect", "the fast tier scans for these reads until they exist",
			"note", "a unique index that will not build means the values are already duplicated there")
	}
}

// EnsureCollection gives the fast tier the collection the way the archive has
// it, and does not wait for its indexes.
//
// The difference from Ensure is when it can be called. A collection created by
// an insert carries no options at all, and they cannot be added afterwards: a
// validator applied later does not judge what is already there and a collation
// cannot be added to an existing collection at any time. So the paths that
// bring a namespace into existence by writing to it have to ask for this
// *before* they write.
//
// Measured on an adopted collection the archive holds with {locale: "en",
// strength: 2}, whose fast tier copy had been dropped: a point read copied the
// document up, the collection came back with no collation, and the query the
// application has always run then matched differently. The copy-up asked the
// indexer immediately afterwards, and by then there was nothing it could do,
// because the collection existed.
//
// The index build stays where it was, in the background after the write, because
// that is a scan and this is a create. Reporting whether it created anything is
// how the caller knows there is a build to start: an insert into a collection
// the fast tier does not hold used to leave it there with the archive's options
// and none of its indexes until something listed the catalog or the router was
// restarted.
func (ix *Indexer) EnsureCollection(ctx context.Context, ns string) bool {
	if ix == nil {
		return false
	}
	if _, coll := splitNS(ns); strings.HasPrefix(coll, "system.") {
		return false
	}
	ix.mu.Lock()
	st := ix.state[ns]
	if st.done || st.created {
		ix.mu.Unlock()
		return false
	}
	// Somebody else is creating it. Wait for them, rather than returning and
	// letting this caller's write create it bare.
	//
	// Returning at once is what the rate limit below used to do to a second
	// caller, and the two are not the same thing at all. The rate limit is there
	// so a create that fails is not retried on every copy-up; this is two
	// copy-ups into a namespace the fast tier does not hold, which is the
	// ordinary way an adopted collection comes back. The reads are sequential
	// and the promotions are not: a point read that misses answers from the
	// archive and promotes in the background.
	//
	// Measured, on an archive collection with a collation and a validator whose
	// fast tier copy had been dropped: six point reads in a row, and in one run
	// of six the collection came back with neither. The first copy-up was still
	// talking to the two clusters when the second gave up waiting and inserted.
	// A collection's options cannot be added afterwards, so what is left is a
	// fast tier copy that matches different documents from the archive's, for
	// as long as it exists.
	if ch, inflight := ix.creating[ns]; inflight {
		ix.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
		}
		return false
	}
	if time.Since(st.lastCreated) < retryAfter {
		ix.mu.Unlock()
		return false
	}
	st.lastCreated = time.Now()
	ix.state[ns] = st
	ch := make(chan struct{})
	if ix.creating == nil {
		ix.creating = map[string]chan struct{}{}
	}
	ix.creating[ns] = ch
	ix.mu.Unlock()

	// Whatever happens below, the waiters are released and the namespace stops
	// being in flight. A create that failed leaves lastCreated set, so the rate
	// limit still holds and the next copy-up does not repeat it immediately.
	defer func() {
		ix.mu.Lock()
		delete(ix.creating, ns)
		ix.mu.Unlock()
		close(ch)
	}()

	db, coll := splitNS(ns)
	if _, err := ix.createWithOptions(ctx, db, coll); err != nil {
		ix.log.Warn("could not create the collection on the fast tier the way the archive has it",
			"ns", ns, "err", err,
			"effect", "a copy-up will create it with no options, and a default collation decides "+
				"which documents match")
		return false
	}
	ix.mu.Lock()
	st = ix.state[ns]
	st.created = true
	ix.state[ns] = st
	ix.mu.Unlock()
	return true
}

// Forget clears the record that a namespace is done, so the next Ensure builds
// it again, and lets that happen at once.
//
// Done means the fast tier had the collection and its indexes when it was
// asked, which stops being true if the collection is dropped there. Only a
// caller that has seen the collection go should call this.
//
// The retry interval goes with it, and that is the whole point. It used to be
// kept, on the reasoning that a namespace which cannot be built should not be
// hammered. That reasoning does not apply here: this only acts on a state that
// says done, which means the last attempt *succeeded*, and a rate limit for
// failures has nothing to say about it. Keeping it meant a collection dropped
// and created again within a minute of its last build was not rebuilt at all
// until that minute was up, which is exactly when an application is writing to
// the empty collection.
//
// Repeated failures are still limited, because a failed build leaves done
// false and this returns without touching anything.
func (ix *Indexer) Forget(ns string) {
	if ix == nil {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	st := ix.state[ns]

	// The create half is always forgotten, timestamp and all. Its rate limit
	// exists to stop a build being hammered, and a create that has to happen
	// before the next write is the opposite case: skipping it because it ran a
	// minute ago is how a collection comes back without its options. Measured
	// under a three-worker sweep, where the earlier sections of the suite took
	// long enough to leave the timestamp inside the window: the copy-up recreated
	// the collection bare and the case-insensitive count answered 1 where the
	// archive answers 2. Alone, the same suite passed.
	st.created = false
	st.lastCreated = time.Time{}

	// The build half keeps its limit. A namespace whose indexes will not build
	// leaves done false, and repeating that on every drop would fill the log
	// rather than fix anything.
	if st.done {
		st.done = false
		st.lastTry = time.Time{}
	}
	ix.state[ns] = st
}

func (ix *Indexer) build(ctx context.Context, ns string) error {
	db, coll := splitNS(ns)

	// The collection before the indexes, because two of its options decide
	// answers and cannot be added afterwards.
	//
	// A view holds no documents of its own, so there is nothing to index either.
	// Asked anyway, mongod answers listIndexes with CommandNotSupportedOnView,
	// and the warning that produced said the fast tier would scan for these
	// reads until the indexes exist, and that a unique index which will not
	// build means the values are already duplicated there. Neither is true of a
	// view, and both were logged every time one was created through the router.
	isView, err := ix.createWithOptions(ctx, db, coll)
	if err != nil {
		return err
	}
	if isView {
		return nil
	}

	want, expiring, err := archiveIndexes(ctx, ix.cold, db, coll)
	if err != nil {
		return err
	}
	ix.warnArchiveExpires(ns, expiring)
	if len(want) == 0 {
		// A collection the archive has no indexes for is nothing to do, and
		// counts as done: there is no point asking again every minute.
		return nil
	}

	have := map[string]bool{}
	hotColl := ix.hot.Database(db).Collection(coll)
	if cur, lerr := hotColl.Indexes().List(ctx); lerr == nil {
		defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
		for cur.Next(ctx) {
			var spec struct {
				Name string `bson:"name"`
			}
			if cur.Decode(&spec) == nil {
				have[spec.Name] = true
			}
		}
	}

	var build []bson.D
	var names []string
	for _, idx := range want {
		if have[idx.name] {
			continue
		}
		build = append(build, idx.spec)
		names = append(names, idx.name)
	}
	if len(build) == 0 {
		return nil
	}
	if err := ix.createIndexes(ctx, db, coll, build); err == nil {
		ix.log.Info("built the archive's indexes on the fast tier",
			"ns", ns, "indexes", names,
			"why", "the fast tier is filled with documents and an index is not a document")
		return nil
	} else {
		// One at a time, because createIndexes is one command for the whole
		// batch and MongoDB refuses the batch for one bad member. That is how a
		// single text index left an adopted collection holding _id_ and nothing
		// else: the indexes beside it were never the problem and were refused
		// with it.
		ix.log.Warn("could not build the archive's indexes in one batch, trying them separately",
			"ns", ns, "err", err)
	}

	var built []string
	var failed error
	for i, spec := range build {
		if err := ix.createIndexes(ctx, db, coll, []bson.D{spec}); err != nil {
			ix.log.Warn("could not build one of the archive's indexes on the fast tier",
				"ns", ns, "index", names[i], "err", err,
				"effect", "the fast tier scans for reads this index would have served")
			failed = err
			continue
		}
		built = append(built, names[i])
	}
	if len(built) > 0 {
		ix.log.Info("built the archive's indexes on the fast tier",
			"ns", ns, "indexes", built,
			"why", "the fast tier is filled with documents and an index is not a document")
	}
	return failed
}

// warnArchiveExpires reports a TTL index on the archive's side of a tiered
// collection.
//
// The archive is the tier that exists to keep history, and a TTL index on it
// deletes that history on a schedule nothing here set. On an adoption it arrives
// with the cluster: the application put it on what used to be its only database,
// and the day that database became the archive the index went on doing what it
// always did. Nothing said so, and the loss is invisible from the fast tier,
// because a read the fast tier answers whole never asks the archive. A read that
// falls through gets an answer short by whatever expired, with no error on it.
//
// A warning rather than a refusal, and rather than dropping it. Expiry may be
// exactly what the deployment wants, and an index the application created is not
// ours to remove. What was missing was the sentence saying it is happening.
//
// Once per namespace per process: build runs once per namespace unless it fails.
func (ix *Indexer) warnArchiveExpires(ns string, expiring []ttlIndex) {
	for _, t := range expiring {
		ix.log.Warn("the archive expires documents of a tiered collection",
			"ns", ns, "index", t.name, "field", t.field,
			"after", (time.Duration(t.secs) * time.Second).String(),
			"effect", "history leaves the tier that exists to keep it, and a read that falls "+
				"through to the archive is short by whatever has expired, with no error on it",
			"action", "drop it on the archive to keep that history, or leave it if the expiry is "+
				"deliberate; the fast tier never carries a TTL index, see cline indexes")
	}
}

// createIndexes sends the archive's own index specifications to the fast tier.
//
// A command rather than the driver's IndexModel, so the specification arrives
// as the archive wrote it. Building a model means naming every option worth
// carrying, and the ones nobody named are the ones that broke this.
func (ix *Indexer) createIndexes(ctx context.Context, db, coll string, specs []bson.D) error {
	list := make(bson.A, len(specs))
	for i, s := range specs {
		list[i] = s
	}
	cmd := bson.D{
		{Key: "createIndexes", Value: coll},
		{Key: "indexes", Value: list},
		// A majority, not the default.
		//
		// mongod's default is every voting member, and a member that cannot vote
		// then stops the build for ever: no error, no timeout, the build sitting
		// in currentOp with waitingForLock false while everything else on the
		// cluster carries on working. Measured on a three member 8.0.26 set with
		// a thousand documents: all members up, 164ms. One member stopped, the
		// same build did not return in ninety seconds. The same state with this
		// option, 117ms.
		//
		// It costs a router that will not start, because the fast tier's unique
		// indexes are built before the listener opens, and there is nothing an
		// operator can do about it from there. Found the hard way on this
		// project's own cluster, where one member came up on the wrong address:
		// a build sat for nineteen minutes and read exactly like a hung proxy.
		//
		// Worth knowing while reading that measurement: a build on an *empty*
		// collection skips the two-phase protocol and needs no votes at all, so
		// the shape that stalls is a fast tier that already holds documents,
		// which is every fast tier after the first warm.
		//
		// A majority is the promise the rest of this system already makes:
		// mirroring, claims and watermarks are all majority-durable and nothing
		// waits on the last member. A member behind builds the index when it
		// catches up, the same as it applies everything else.
		{Key: "commitQuorum", Value: "majority"},
	}
	// Bracketed on both attempts, and not only the first: a fast tier that
	// refuses the commit quorum builds its indexes down the path below, and an
	// unbracketed build there would be delivered to every application on such a
	// cluster. Same defect, second door.
	done := ix.beginOwnDDL(ctx, db+"."+coll)
	err := ix.hot.Database(db).RunCommand(ctx, cmd).Err()
	done()
	if err == nil {
		return nil
	}
	if !rejectsCommitQuorum(err) {
		return err
	}
	// A server that will not take the option at all. Nothing here should stop
	// building indexes over a preference about when they commit, and an index
	// build that quietly stopped happening is the kind of regression that hides
	// for weeks.
	ix.log.Warn("the fast tier will not take a commit quorum, building without one",
		"ns", db+"."+coll, "err", err,
		"note", "the build then waits for every voting member, so one away for maintenance "+
			"means it does not finish")
	done = ix.beginOwnDDL(ctx, db+"."+coll)
	err = ix.hot.Database(db).RunCommand(ctx, cmd[:len(cmd)-1]).Err()
	done()
	return err
}

// rejectsCommitQuorum reports a server refusing the option rather than the
// index. Matched on the message because the codes differ by version and
// topology, and the fallback is harmless when it is wrong.
func rejectsCommitQuorum(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "commitquorum") || strings.Contains(msg, "commit quorum")
}

// createWithOptions makes the fast tier's copy of a collection the way the
// archive has it, before anything inserts into it.
//
// A collection is more than its documents, and the fast tier is filled with
// documents. Everything else about it arrives only if something puts it there,
// and in an adoption nothing does: the existing cluster becomes the archive
// carrying the application's collections as they are, and the fast tier's copy
// is brought into existence by the first insert, which carries no options at
// all. Two of them decide answers rather than performance:
//
// A default collation decides which documents match. Measured on an adopted
// collection created with {locale: "en", strength: 2}: the application's own
// cluster answers countDocuments({name: "alice"}) with 2, and the same query
// through the router answered 1 as soon as the fast tier held the documents.
// No error, no warning, a different answer to the query it has always run.
//
// A validator decides which writes are refused. Measured on the same adoption:
// a write the archive rejects was accepted through the router, so a rule the
// application had relied on stopped applying on the day it was adopted.
//
// Neither can be added to a collection that already holds documents in a way
// that fixes the past, and a collation cannot be added at all. So this runs
// before the first insert: at router startup for every managed namespace, and
// from the warmer before it fills one.
//
// capped and timeseries are deliberately not carried. A capped fast tier would
// remove its own oldest documents, the feeder would read those removals as the
// application deleting them, and the archive would tombstone the history it
// exists to keep. Those are refused rather than copied; see the feeder's
// collection kind check.
func (ix *Indexer) createWithOptions(ctx context.Context, db, coll string) (isView bool, err error) {
	exists, isView, opts, err := archiveCollectionOptions(ctx, ix.cold, db, coll)
	if err != nil || !exists || isView {
		return isView, err
	}

	names, err := ix.hot.Database(db).ListCollectionNames(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		return false, fmt.Errorf("list %s.%s on the fast tier: %w", db, coll, err)
	}
	if len(names) > 0 {
		// Already there. Adding a collation now would not apply to what it
		// already holds, and MongoDB does not allow it either.
		return false, nil
	}

	cmd := bson.D{{Key: "create", Value: coll}}
	cmd = append(cmd, opts...)
	// Bracketed, so a client watching this collection with showExpandedEvents
	// is not told its collection was just created. See beginOwnDDL.
	done := ix.beginOwnDDL(ctx, db+"."+coll)
	cerr := ix.hot.Database(db).RunCommand(ctx, cmd).Err()
	done()
	if cerr != nil {
		// A racing insert is the expected way to lose here, and it is not
		// worth failing the index build over.
		if isNamespaceExists(cerr) {
			return false, nil
		}
		return false, fmt.Errorf("create %s.%s on the fast tier: %w", db, coll, cerr)
	}
	carried := make([]string, 0, len(opts))
	for _, e := range opts {
		carried = append(carried, e.Key)
	}
	ix.log.Info("created the collection on the fast tier the way the archive has it",
		"ns", db+"."+coll, "options", carried,
		"why", "a collation decides which documents match and a validator which writes are refused, "+
			"and neither arrives with the documents; and a collection that does not exist is not "+
			"listed by listCollections and cannot be asked for its stats, however well its reads work")
	return false, nil
}

// MirrorCollection gives the archive the collection the way the fast tier has
// it, when the archive does not hold it yet.
//
// Options travel upward on their own, because an adoption starts with the
// archive holding everything. They do not travel downward, and nothing carried
// them: a collection created through the router exists on the fast tier with its
// options, and the archive's copy is brought into existence by the feeder's
// first mirrored insert, which carries none.
//
// Measured on a collection created through the router with {locale: "en",
// strength: 2}: the fast tier answered countDocuments({name: "alice"}) with 2
// and the archive with 1, so every read that fell through to the archive, which
// is every read of an evicted document, matched differently from the tier the
// application had been talking to.
//
// The same deny-list decides what may cross, in the same polarity: everything
// the fast tier has except what the other tier must not be given.
func MirrorCollection(ctx context.Context, hot, cold *mongo.Client, db, coll string, log *slog.Logger) {
	if strings.HasPrefix(coll, "system.") {
		return
	}
	names, err := cold.Database(db).ListCollectionNames(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		log.Warn("could not check whether the archive holds this collection",
			"ns", db+"."+coll, "err", err)
		return
	}
	if len(names) > 0 {
		// Already there, and its options cannot be changed now anyway.
		return
	}

	opts, isView, err := carryableOptions(ctx, hot, db, coll)
	if err != nil || len(opts) == 0 {
		// Nothing worth carrying, so the feeder's first insert creating it is
		// the same collection.
		return
	}
	if isView {
		log = log.With("kind", "view")
	}
	cmd := append(bson.D{{Key: "create", Value: coll}}, opts...)
	if err := cold.Database(db).RunCommand(ctx, cmd).Err(); err != nil {
		if isNamespaceExists(err) {
			return
		}
		log.Warn("could not create the collection in the archive the way the fast tier has it",
			"ns", db+"."+coll, "err", err,
			"effect", "the archive's copy will be created by the first mirrored insert, with no "+
				"options, and a collation decides which documents match")
		return
	}
	carried := make([]string, 0, len(opts))
	for _, e := range opts {
		carried = append(carried, e.Key)
	}
	log.Info("created the collection in the archive the way the fast tier has it",
		"ns", db+"."+coll, "options", carried,
		"why", "the archive answers every read the fast tier cannot, and a collation decides which "+
			"documents match")
}

// carryableOptions reads one tier's options for a collection, minus the ones
// that must not be given to the other, and says whether it is a view. The test
// file next door has a helper of its own called collectionOptions, which reads
// them unfiltered.
//
// A view is the exception to the deny-list rather than a case of it. viewOn and
// pipeline are kept from crossing *upward* because a view holds no documents and
// cannot be tiered. Downward they are the entire definition, and stripping them
// would give the archive an empty collection wearing the view's name, which
// reads as an empty result instead of a missing view: worse than not carrying it
// at all.
func carryableOptions(ctx context.Context, from *mongo.Client, db, coll string) (bson.D, bool, error) {
	cur, err := from.Database(db).ListCollections(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	if !cur.Next(ctx) {
		return nil, false, cur.Err()
	}
	var raw bson.Raw
	if err := cur.Decode(&raw); err != nil {
		return nil, false, err
	}
	kind, _ := raw.Lookup("type").StringValueOK()
	isView := kind == "view"

	odoc, ok := raw.Lookup("options").DocumentOK()
	if !ok {
		return nil, isView, nil
	}
	elems, err := odoc.Elements()
	if err != nil {
		return nil, isView, nil
	}
	var out bson.D
	for _, e := range elems {
		if notCarried[e.Key()] && !(isView && (e.Key() == "viewOn" || e.Key() == "pipeline")) {
			continue
		}
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	if isView {
		// Without both halves it is not a view, and creating it as anything
		// else is the failure this avoids.
		var haveOn, havePipeline bool
		for _, e := range out {
			switch e.Key {
			case "viewOn":
				haveOn = true
			case "pipeline":
				havePipeline = true
			}
		}
		if !haveOn || !havePipeline {
			return nil, isView, nil
		}
	}
	return out, isView, nil
}

// archiveCollectionOptions reports whether the archive holds the collection, and
// which of its options are worth carrying upward.
//
// The two answers are separate because most collections have no options at all
// and still have to be created. A namespace the archive holds and the fast tier
// does not is invisible until something creates it: listCollections does not
// name it, so an ORM checking whether its collection exists is told no, and
// db.coll.stats() fails with "Unable to retrieve storage" while every read of
// the same collection is answered correctly from the archive. Measured on an
// adoption of a hundred documents: countDocuments answered 100, show
// collections was empty, and stats() errored.
func archiveCollectionOptions(ctx context.Context, cold *mongo.Client, db, coll string) (exists, isView bool, opts bson.D, err error) {
	cur, err := cold.Database(db).ListCollections(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		return false, false, nil, fmt.Errorf("list %s.%s in the archive: %w", db, coll, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	if !cur.Next(ctx) {
		return false, false, nil, cur.Err()
	}
	var raw bson.Raw
	if derr := cur.Decode(&raw); derr != nil {
		return false, false, nil, derr
	}
	// A view is reported here rather than left to fail further down. It holds no
	// documents of its own, so there is nothing to index and nothing to create,
	// and asking mongod for its indexes is an error rather than an empty list.
	if t, tok := raw.Lookup("type").StringValueOK(); tok && t == "view" {
		return true, true, nil, nil
	}
	odoc, ok := raw.Lookup("options").DocumentOK()
	if !ok {
		return true, false, nil, nil
	}

	elems, eerr := odoc.Elements()
	if eerr != nil {
		return true, false, nil, nil
	}
	var out bson.D
	for _, e := range elems {
		if notCarried[e.Key()] {
			continue
		}
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	return true, false, out, nil
}

// notCarried is the collection options the fast tier's copy must not be given.
//
// Everything else goes across. That polarity is deliberate and was learned the
// expensive way on the indexes beside this: a list of the options worth carrying
// silently drops every option nobody thought of, and the copy is then a
// different collection with the same name. The promise is that a client cannot
// tell the two tiers apart, so an option nobody has considered should follow the
// archive rather than be discarded.
//
// What must not follow, and why each one:
//
// capped, size, max. A capped fast tier removes its own oldest documents, the
// feeder reads those removals as the application deleting them, and the archive
// tombstones the history it exists to keep. The feeder refuses a capped
// namespace outright for the same reason.
//
// timeseries, and the expireAfterSeconds that goes with it. A time series
// collection is a view over a hidden bucket collection, so its documents are not
// where a copy-up would put them, and the feeder refuses one.
//
// viewOn and pipeline. A view holds no documents and cannot be tiered; the
// router leaves views to the fast tier and names the ones it does not have.
//
// clusteredIndex, because a clustered collection cannot be given the _id index
// the rest of this assumes, and the archive's own clustering is a storage
// decision that buys nothing on a tier holding a subset.
//
// idIndex, info, uuid and type are not settable at all: the server reports them
// and decides them.
var notCarried = map[string]bool{
	"capped": true, "size": true, "max": true,
	"timeseries": true, "expireAfterSeconds": true,
	"viewOn": true, "pipeline": true,
	"clusteredIndex": true,
	"idIndex":        true, "info": true, "uuid": true, "type": true,
}

// isNamespaceExists reports MongoDB's "collection already exists", which is the
// benign outcome of racing an insert.
func isNamespaceExists(err error) bool {
	var ce mongo.CommandError
	return errors.As(err, &ce) && ce.Code == 48
}

// splitNS is db.collection, split at the first dot, which is where MongoDB
// splits it too.
func splitNS(ns string) (string, string) {
	db, coll, _ := strings.Cut(ns, ".")
	return db, coll
}

// archiveIndex is one index as the archive defines it, carried whole.
//
// The spec is the archive's own listIndexes document with the two fields the
// server owns taken out, rather than a set of options read back off it. Reading
// a set means every option outside that set is dropped, and the index that
// arrives on the fast tier is a different index with the same name.
//
// Measured, and this is what it costs. An adopted collection with a text index
// on {title, body} and weights {title: 10, body: 1}: the copy went out carrying
// the key the archive reports for a text index and no weights, and MongoDB
// refused it with "text index option 'weights' must specify fields or the
// wildcard". createIndexes is one command for the whole batch, so the wildcard
// index and an ordinary index on {a: 1} in the same collection were refused
// with it, and the fast tier ended up holding _id_ and nothing else. Ensure
// retried every minute and failed the same way every time. One text index in an
// adopted collection meant the tier that exists to be fast scanned everything,
// for ever, and the only sign was a warning in a log.
//
// A wildcard index lost its projection the same way, which is quieter and not
// better: {"$**": 1} with wildcardProjection {a: 1, b: 1} indexes two fields,
// and the same key with no projection indexes every field of every document.
// That one builds, so nothing says anything.
type archiveIndex struct {
	name string
	spec bson.D
	// unique is read out for the caller's benefit, because the router looks to
	// the fast tier's unique indexes to know what to enforce across both tiers.
	unique bool
}

// A ttlIndex is one the archive expires documents with, which is not copied and
// is worth saying out loud. See archiveIndexes.
type ttlIndex struct {
	name  string
	field string
	secs  float64
}

// archiveIndexes lists what the archive has, minus the two kinds that must not
// be copied.
//
// _id_ is already there on any collection that exists.
//
// A TTL index is deliberately left behind, and this is the one exclusion worth
// arguing about. Creating it on the fast tier would make the fast tier expire
// documents, the feeder would read those expiries as application deletes, and
// the archive's copies would be tombstoned: an index whose job is to delete
// data would reach across into the tier that exists to keep it. Whether a
// deployment wants that is a decision about the application's data, not
// something to be inherited silently by warming a namespace, so the archive
// goes on expiring its own copies and the fast tier does not.
//
// "Goes on expiring its own copies" is the part that was silent, and on an
// adoption it is the archive deleting the history it was adopted to keep. The
// ones found are returned separately so the caller can say so.
func archiveIndexes(ctx context.Context, cold *mongo.Client, db, coll string) ([]archiveIndex, []ttlIndex, error) {
	cur, err := cold.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list archive indexes for %s.%s: %w", db, coll, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var out []archiveIndex
	var expiring []ttlIndex
	for cur.Next(ctx) {
		var spec bson.D
		if err := bson.Unmarshal(cur.Current, &spec); err != nil {
			continue
		}
		var (
			name   string
			unique bool
			keyed  bool
			ttl    bool
			secs   float64
			field  string
			kept   = make(bson.D, 0, len(spec))
		)
		for _, e := range spec {
			switch e.Key {
			case "v", "ns":
				// The server owns both. v is the index version it decides for
				// itself, and ns is a legacy field it no longer accepts.
				continue
			case "expireAfterSeconds":
				ttl = true
				secs = numeric(e.Value)
			case "name":
				name, _ = e.Value.(string)
			case "key":
				keyed = true
				field = firstKey(e.Value)
			case "unique":
				unique, _ = e.Value.(bool)
			}
			kept = append(kept, e)
		}
		if name == "_id_" || !keyed {
			continue
		}
		if ttl {
			expiring = append(expiring, ttlIndex{name: name, field: field, secs: secs})
			continue
		}
		out = append(out, archiveIndex{name: name, spec: kept, unique: unique})
	}
	return out, expiring, cur.Err()
}

// numeric reads expireAfterSeconds, which the server may report as any of the
// number types depending on how it was created.
func numeric(v any) float64 {
	switch n := v.(type) {
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	}
	return 0
}

// firstKey names the field a TTL index expires on, for the warning to quote.
func firstKey(v any) string {
	d, ok := v.(bson.D)
	if !ok || len(d) == 0 {
		return ""
	}
	return d[0].Key
}
