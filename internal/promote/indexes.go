package promote

import (
	"context"
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

func (ix *Indexer) build(ctx context.Context, ns string) error {
	db, coll := splitNS(ns)

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
