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
)

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
}

type indexState struct {
	done    bool
	lastTry time.Time
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
	ix.mu.Lock()
	st := ix.state[ns]
	if st.done || time.Since(st.lastTry) < retryAfter {
		ix.mu.Unlock()
		return
	}
	ix.state[ns] = indexState{lastTry: time.Now()}
	ix.mu.Unlock()

	if err := ix.build(ctx, ns); err != nil {
		ix.log.Warn("could not build the archive's indexes on the fast tier",
			"ns", ns, "err", err,
			"effect", "the fast tier scans for these reads until they exist",
			"note", "a unique index that will not build means the values are already duplicated there")
		return
	}
	ix.mu.Lock()
	ix.state[ns] = indexState{done: true, lastTry: time.Now()}
	ix.mu.Unlock()
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
	if !ix.state[ns].done {
		return
	}
	ix.state[ns] = indexState{}
}

func (ix *Indexer) build(ctx context.Context, ns string) error {
	db, coll := splitNS(ns)

	// The collection before the indexes, because two of its options decide
	// answers and cannot be added afterwards.
	if err := ix.createWithOptions(ctx, db, coll); err != nil {
		return err
	}

	want, err := archiveIndexes(ctx, ix.cold, db, coll)
	if err != nil {
		return err
	}
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
	return ix.hot.Database(db).RunCommand(ctx, bson.D{
		{Key: "createIndexes", Value: coll},
		{Key: "indexes", Value: list},
	}).Err()
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
func (ix *Indexer) createWithOptions(ctx context.Context, db, coll string) error {
	exists, opts, err := archiveCollectionOptions(ctx, ix.cold, db, coll)
	if err != nil || !exists {
		return err
	}

	names, err := ix.hot.Database(db).ListCollectionNames(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		return fmt.Errorf("list %s.%s on the fast tier: %w", db, coll, err)
	}
	if len(names) > 0 {
		// Already there. Adding a collation now would not apply to what it
		// already holds, and MongoDB does not allow it either.
		return nil
	}

	cmd := bson.D{{Key: "create", Value: coll}}
	cmd = append(cmd, opts...)
	if err := ix.hot.Database(db).RunCommand(ctx, cmd).Err(); err != nil {
		// A racing insert is the expected way to lose here, and it is not
		// worth failing the index build over.
		if isNamespaceExists(err) {
			return nil
		}
		return fmt.Errorf("create %s.%s on the fast tier: %w", db, coll, err)
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
	return nil
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
func archiveCollectionOptions(ctx context.Context, cold *mongo.Client, db, coll string) (bool, bson.D, error) {
	cur, err := cold.Database(db).ListCollections(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		return false, nil, fmt.Errorf("list %s.%s in the archive: %w", db, coll, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	if !cur.Next(ctx) {
		return false, nil, cur.Err()
	}
	var raw bson.Raw
	if err := cur.Decode(&raw); err != nil {
		return false, nil, err
	}
	odoc, ok := raw.Lookup("options").DocumentOK()
	if !ok {
		return true, nil, nil
	}

	elems, err := odoc.Elements()
	if err != nil {
		return true, nil, nil
	}
	var out bson.D
	for _, e := range elems {
		if notCarried[e.Key()] {
			continue
		}
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	return true, out, nil
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
func archiveIndexes(ctx context.Context, cold *mongo.Client, db, coll string) ([]archiveIndex, error) {
	cur, err := cold.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list archive indexes for %s.%s: %w", db, coll, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var out []archiveIndex
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
			case "name":
				name, _ = e.Value.(string)
			case "key":
				keyed = true
			case "unique":
				unique, _ = e.Value.(bool)
			}
			kept = append(kept, e)
		}
		if name == "_id_" || !keyed || ttl {
			continue
		}
		out = append(out, archiveIndex{name: name, spec: kept, unique: unique})
	}
	return out, cur.Err()
}
