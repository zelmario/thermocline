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
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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
// it again.
//
// Done means the fast tier had the collection and its indexes when it was
// asked, which stops being true if the collection is dropped there. Only a
// caller that has seen the collection missing should call this, and the retry
// interval is left alone so a namespace that cannot be built is still not
// hammered.
func (ix *Indexer) Forget(ns string) {
	if ix == nil {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	st := ix.state[ns]
	if !st.done {
		return
	}
	ix.state[ns] = indexState{done: false, lastTry: st.lastTry}
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

	var build []mongo.IndexModel
	var names []string
	for _, idx := range want {
		if have[idx.name] {
			continue
		}
		build = append(build, idx.model())
		names = append(names, idx.name)
	}
	if len(build) == 0 {
		return nil
	}
	if _, err := hotColl.Indexes().CreateMany(ctx, build); err != nil {
		return err
	}
	ix.log.Info("built the archive's indexes on the fast tier",
		"ns", ns, "indexes", names,
		"why", "the fast tier is filled with documents and an index is not a document")
	return nil
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

	var out bson.D
	// Order matters only for reading the log line back; MongoDB does not care.
	for _, key := range []string{"collation", "validator", "validationLevel", "validationAction"} {
		if v, lerr := odoc.LookupErr(key); lerr == nil {
			out = append(out, bson.E{Key: key, Value: v})
		}
	}
	return true, out, nil
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

// archiveIndex is one index as the archive defines it.
type archiveIndex struct {
	name    string
	keys    bson.Raw
	unique  bool
	sparse  bool
	partial bson.Raw
	// collation decides which documents an index can answer for, so an index
	// built without the one it was defined with answers different questions.
	collation bson.Raw
}

func (a archiveIndex) model() mongo.IndexModel {
	opts := options.Index().SetName(a.name)
	if a.unique {
		// Safe on the fast tier, which holds a subset of the archive: a subset
		// of values that are unique is unique. And it is the point, because
		// the router looks here to learn what to enforce across both tiers.
		opts.SetUnique(true)
	}
	if a.sparse {
		opts.SetSparse(true)
	}
	if len(a.partial) > 0 {
		opts.SetPartialFilterExpression(a.partial)
	}
	if len(a.collation) > 0 {
		var c options.Collation
		if err := bson.Unmarshal(a.collation, &c); err == nil {
			opts.SetCollation(&c)
		}
	}
	return mongo.IndexModel{Keys: a.keys, Options: opts}
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
		var spec struct {
			Name                    string   `bson:"name"`
			Key                     bson.Raw `bson:"key"`
			Unique                  bool     `bson:"unique"`
			Sparse                  bool     `bson:"sparse"`
			PartialFilterExpression bson.Raw `bson:"partialFilterExpression"`
			Collation               bson.Raw `bson:"collation"`
			ExpireAfterSeconds      *int32   `bson:"expireAfterSeconds"`
		}
		if cur.Decode(&spec) != nil || spec.Name == "_id_" || len(spec.Key) == 0 {
			continue
		}
		if spec.ExpireAfterSeconds != nil {
			continue
		}
		out = append(out, archiveIndex{
			name:      spec.Name,
			keys:      spec.Key,
			unique:    spec.Unique,
			sparse:    spec.Sparse,
			partial:   spec.PartialFilterExpression,
			collation: spec.Collation,
		})
	}
	return out, cur.Err()
}
