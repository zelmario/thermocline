package router

import (
	"context"
	"encoding/base64"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// A unique index only constrains the documents it can see. Evicting a document
// takes its index entry with it, so the fast tier stops knowing that key was
// ever used and a later insert of the same value succeeds. Nothing errors, and
// the collection now holds two documents that the unique index was there to
// prevent: one in the archive, one in the fast tier.
//
// The fix is a registry of keys that is never evicted. Every insert claims its
// unique keys there first, and the claim is what fails when a value is already
// taken, whichever tier the original document is sitting in.
//
// The registry lives on the fast tier rather than with the rest of the
// bookkeeping. It is consulted on every insert, and putting it on the archive
// would add a cross-cluster round trip to the hot write path.
//
// Claims are permanent, which means a deleted document leaves one behind. That
// is resolved lazily instead of on the delete path: a claim only matters when
// something collides with it, and at that point the router checks whether the
// owning document still exists anywhere. If it does not, the claim is stale and
// the new document takes it over. Doing it this way keeps deletes untouched and
// costs nothing until there is an actual conflict.

// keysCollection is the registry, on the fast tier.
const keysCollection = "keys"

// indexRefresh is how long a listing of unique indexes is reused. Indexes are
// created rarely, and re-listing them on every insert would be absurd.
//
// It is also a contract, and the direction it is stale in is the unsafe one.
// A router that has not noticed a new unique index does not claim keys for it,
// so it lets through a duplicate of an archived document that mongod's own
// index cannot see. The router the createIndexes went through drops its listing
// at once; every *other* router keeps the old one for up to this long, and no
// change stream carries index DDL to tell them.
//
// So: after creating a unique index on a tiered collection, cross tier
// enforcement is immediate on the router that took the command and arrives
// within this on the rest. See the rollout list in README, which says to run
// cline keys --rebuild at that point anyway.
//
// The same shape as watermark.ClaimCacheTTL, and found by asking the same
// question of it: which way is this cache stale, and who is relying on it?
const indexRefresh = time.Minute

// uniqueIndex is one unique constraint the router has to enforce itself.
type uniqueIndex struct {
	name   string
	fields []string

	// sparse and partial indexes constrain only some documents. A key is
	// claimed for a document only when the index would have indexed it.
	sparse  bool
	partial bson.Raw

	// partialFields are the fields the partial filter reads. Writing one of
	// them can move a document into the index without touching an indexed
	// field at all, and that is the moment its key has to be claimed.
	partialFields []string

	// collation, when the index carries one. Two strings it calls equal are
	// one key, and their bytes are not, so the id derived here cannot see the
	// clash. See collatedkeys.go.
	collation bson.Raw
}

// partialFieldNames lists the fields a partial filter reads, descending through
// the logical operators that hold nested filters.
func partialFieldNames(filter bson.Raw) []string {
	seen := map[string]bool{}
	var walk func(bson.Raw)
	walk = func(f bson.Raw) {
		elems, err := f.Elements()
		if err != nil {
			return
		}
		for _, e := range elems {
			if !strings.HasPrefix(e.Key(), "$") {
				seen[e.Key()] = true
				continue
			}
			// $and, $or and $nor carry arrays of further filters.
			if arr, ok := e.Value().ArrayOK(); ok {
				vals, verr := arr.Values()
				if verr != nil {
					continue
				}
				for _, v := range vals {
					if d, dok := v.DocumentOK(); dok {
						walk(d)
					}
				}
				continue
			}
			if d, dok := e.Value().DocumentOK(); dok {
				walk(d)
			}
		}
	}
	walk(filter)
	out := make([]string, 0, len(seen))
	for f := range seen {
		out = append(out, f)
	}
	return out
}

// constrains reports whether idx would index doc at all, and so whether doc's
// key has to be claimed.
//
// A partial index carries a query predicate, and reimplementing MongoDB's
// matcher to evaluate it would be a source of bugs forever. The server is asked
// instead: a collectionless aggregation feeds the candidate document through the
// index's own partialFilterExpression, so the answer comes from the same code
// that will decide whether to index it.
func (r *Router) constrains(ctx context.Context, dbName string, idx uniqueIndex, doc bson.Raw) bool {
	if idx.sparse {
		// A compound sparse index still indexes a document that has any one of
		// its fields, so only a document missing all of them is exempt.
		any := false
		for _, f := range idx.fields {
			if _, ok := lookupPath(doc, f); ok {
				any = true
				break
			}
		}
		if !any {
			return false
		}
	}
	if len(idx.partial) == 0 {
		return true
	}

	cur, err := r.hot.Database(dbName).Aggregate(ctx, bson.A{
		bson.D{{Key: "$documents", Value: bson.A{doc}}},
		bson.D{{Key: "$match", Value: idx.partial}},
		bson.D{{Key: "$limit", Value: 1}},
	})
	if err != nil {
		// Could not ask. Enforcing anyway would reject writes the database
		// itself allows, which is a visible failure caused by bookkeeping, so
		// this stays out of the way and says so.
		r.log.Warn("could not evaluate a partial unique index, not enforcing it across tiers",
			"index", idx.name, "err", err,
			"effect", "a duplicate of an archived document could be inserted")
		return false
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	return cur.Next(ctx)
}

// indexCache remembers which namespaces have unique indexes.
type indexCache struct {
	mu      sync.RWMutex
	fetched map[string]time.Time
	byNS    map[string][]uniqueIndex
	// Which TTL indexes this router has already said are on the fast tier's
	// copy, per namespace, joined. The listing is re-read every indexRefresh
	// and saying it every minute would bury it, so it is said when the answer
	// changes. See warnFastTierExpires.
	ttlSaid map[string]string
}

// uniqueIndexes lists the unique indexes on ns, excluding _id.
//
// Sparse and partial ones are included. They constrain only a subset of
// documents, so each candidate document is tested against the same condition
// the index itself uses before its key is claimed. See constrains.
func (r *Router) uniqueIndexes(ctx context.Context, ns string) []uniqueIndex {
	// Before reading what this router remembers, give it the chance to hear
	// that another one changed the indexes. The count of changes rides on the
	// completeness claim, and reading that claim is what notices it.
	//
	// Here rather than left to the write path's own claim read, because those
	// two run concurrently: the insert guard checks archived ids in a goroutine
	// while this runs on the main path. Measured that way round, the listing
	// was read before the claim refresh landed, the insert that should have been
	// refused was accepted, and only the *next* one was judged correctly.
	r.noticeIndexChanges(ctx, ns)

	r.idxCache.mu.RLock()
	at, seen := r.idxCache.fetched[ns]
	cached := r.idxCache.byNS[ns]
	r.idxCache.mu.RUnlock()
	if seen && time.Since(at) < indexRefresh {
		return cached
	}

	db, coll := splitNS(ns)
	found := []uniqueIndex{}
	// listed counts every index, _id_ included, and is how this tells an
	// absent collection from one that simply has no unique index. Every
	// collection has _id_, so a listing that names nothing at all is a
	// collection that is not there.
	listed := 0
	expiring := []string{}
	cur, err := r.hot.Database(db).Collection(coll).Indexes().List(ctx)
	if err == nil {
		defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
		for cur.Next(ctx) {
			listed++
			var spec struct {
				Name                    string   `bson:"name"`
				Unique                  bool     `bson:"unique"`
				Sparse                  bool     `bson:"sparse"`
				Key                     bson.Raw `bson:"key"`
				PartialFilterExpression bson.Raw `bson:"partialFilterExpression"`
				Collation               bson.Raw `bson:"collation"`
				// Not a unique index's business, and read here because this
				// listing is the only place that sees an index whatever door
				// it came through. See warnFastTierExpires.
				ExpireAfterSeconds *float64 `bson:"expireAfterSeconds"`
			}
			if derr := cur.Decode(&spec); derr != nil {
				continue
			}
			if spec.ExpireAfterSeconds != nil && spec.Name != "_id_" {
				expiring = append(expiring, spec.Name)
			}
			if !spec.Unique || spec.Name == "_id_" {
				continue
			}
			elems, kerr := spec.Key.Elements()
			if kerr != nil {
				continue
			}
			ui := uniqueIndex{
				name:          spec.Name,
				sparse:        spec.Sparse,
				partial:       spec.PartialFilterExpression,
				partialFields: partialFieldNames(spec.PartialFilterExpression),
				collation:     spec.Collation,
			}
			for _, e := range elems {
				ui.fields = append(ui.fields, e.Key())
			}
			found = append(found, ui)
		}
		err = cur.Err()
	}
	if err == nil && listed > 0 {
		r.warnFastTierExpires(ns, expiring)
	}

	// A listing nobody managed to read must not replace what is remembered.
	//
	// It used to. The error branch stored an empty slice and stamped it as
	// fresh, so one unreachable moment on the fast tier -- an election, a
	// dropped connection -- turned into a full minute of believing this
	// collection has no unique index, and for that minute nothing stopped a
	// write duplicating a key an archived document holds. Silently: an empty
	// listing produces no refusals and no log line, and the router looks like
	// it is working.
	//
	// listed counts _id_ as well, so nothing at all means the collection is not
	// there rather than there with no unique index, and that is not an answer
	// either. fetched is left alone in both cases, so the next write tries again
	// rather than waiting out the refresh interval on an answer nobody got.
	if err != nil || listed == 0 {
		if err != nil {
			r.log.Warn("could not list the unique indexes, keeping the previous listing",
				"ns", ns, "err", err, "remembered", len(cached),
				"effect", "uniqueness is enforced across the tiers from what was last read, "+
					"which is right if the indexes have not changed and stale if they have")
		}
		return cached
	}

	// A unique index this router was enforcing is gone from the fast tier.
	//
	// Said out loud, because it is the end of a guarantee and nothing else
	// would mention it. The usual cause is the collection being dropped: the
	// next write recreates it bare, and the constraint the application declared
	// is not on either tier any more. The router stops enforcing it, which is
	// the honest thing to do -- it would refuse writes mongod accepts otherwise
	// -- and this is the only trace.
	if gone := missingFrom(cached, found); len(gone) > 0 {
		r.log.Warn("a unique index this router was enforcing across the tiers is gone from the "+
			"fast tier, so it is no longer enforced",
			"ns", ns, "indexes", gone,
			"effect", "a write may now duplicate a key an archived document holds, which the "+
				"fast tier's own index cannot see and so cannot refuse",
			"action", "if this was not deliberate, recreate the index; the archive's copies of "+
				"the documents are still there and cline keys --rebuild will find any duplicates")
		r.releaseIndexClaims(ns, gone)
	}

	r.idxCache.mu.Lock()
	if r.idxCache.byNS == nil {
		r.idxCache.byNS = map[string][]uniqueIndex{}
		r.idxCache.fetched = map[string]time.Time{}
	}
	r.idxCache.byNS[ns] = found
	r.idxCache.fetched[ns] = time.Now()
	r.idxCache.mu.Unlock()
	return found
}

// refreshIndexes reads a namespace's unique indexes again, right after
// something changed them.
//
// The reading is the point rather than the answer: uniqueIndexes is where a
// listing is compared with the one before it, and that comparison is what
// notices an index has gone. Left to the next write, a collection nobody writes
// to keeps rows for a constraint that no longer exists, and keeps them exactly
// until somebody creates an index of the same name again -- which is the state
// that produced the wrong answer releaseIndexClaims describes.
//
// **Synchronous, and it has to be.** The first version did this in a goroutine
// and lost a race to BumpIndexGen, which is called a few lines later on the
// same DDL: bumping the generation makes the next completeness read drop the
// cached listing, and a dropped listing is nothing to compare against, so the
// comparison found no change, said nothing and released nothing. It passed on a
// quiet machine and failed in a loaded sweep, which is what a scheduling race
// looks like. One listIndexes on a command that changes indexes is not a cost
// worth a race.
func (r *Router) refreshIndexes(ctx context.Context, ns string) {
	if !r.cfg.Manages(ns) {
		return
	}
	rc, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseClaimsLimit)
	defer cancel()
	r.uniqueIndexes(rc, ns)
}

// releaseIndexClaims deletes the registry rows for indexes that no longer exist.
//
// Claims are permanent on purpose: a deleted document leaves its row behind, and
// the row is resolved lazily when something collides with it. That reasoning is
// about documents. For an index that has been dropped the row is not a claim any
// more, it is a claim about a constraint that no longer exists, and leaving it
// is worse than useless.
//
// The fuzzer showed why, and it took nine rounds. Index names here are derived
// from the key and the options, so dropping fz_sku_n_u_pgrp and creating it
// again produces the same name and the same key ids. The rows from the first
// incarnation were still there, so the rebuild that follows the new index could
// insert none of them -- every one a duplicate -- and the documents that
// actually held those keys never got a row. Their keys then read as free: an
// update moved onto { items.sku: "s9", n: 34 }, the row for it named a document
// that had long since moved off that value, the claim was judged stale and
// taken over, and the archived document that really holds it was never asked
// about. mongod refused the same write on an untiered copy.
//
// Driven by the listing rather than by parsing a dropIndexes, because the same
// thing has to happen for an index dropped out of band, and the listing is the
// one place that learns about both.
//
// In the background: it is a write to the registry, it happens when an index is
// dropped rather than on the write path, and the caller is a read that a client
// is waiting on.
func (r *Router) releaseIndexClaims(ns string, indexes []string) {
	r.goSafely("releasing the claims of a dropped unique index", func() {
		ctx, cancel := context.WithTimeout(context.Background(), releaseClaimsLimit)
		defer cancel()
		res, err := r.keys().DeleteMany(ctx, bson.D{
			{Key: "ns", Value: ns},
			{Key: "idx", Value: bson.D{{Key: "$in", Value: indexes}}},
		})
		if err != nil {
			r.log.Warn("could not release the claims of a unique index that no longer exists",
				"ns", ns, "indexes", indexes, "err", err,
				"effect", "if an index of the same name is created again, its rows are already "+
					"there and name documents that may no longer hold those keys",
				"action", "run cline keys --prune, or cline keys --rebuild after recreating it")
			return
		}
		if res.DeletedCount > 0 {
			r.log.Info("released the claims of a unique index that no longer exists",
				"ns", ns, "indexes", indexes, "rows", res.DeletedCount)
		}
	})
}

// releaseClaimsLimit bounds that delete. A registry this cannot clear in a
// minute is one for cline keys --prune to deal with deliberately.
const releaseClaimsLimit = time.Minute

// missingFrom names the unique indexes in was that are not in now.
func missingFrom(was, now []uniqueIndex) []string {
	if len(was) == 0 {
		return nil
	}
	have := make(map[string]struct{}, len(now))
	for _, i := range now {
		have[i.name] = struct{}{}
	}
	var gone []string
	for _, i := range was {
		if _, ok := have[i.name]; !ok {
			gone = append(gone, i.name)
		}
	}
	return gone
}

// noticeIndexChanges reads the completeness claim so that a unique index
// another router created is seen before this one enforces from its own listing.
//
// Cheap: the claim is cached for watermark.ClaimCacheTTL, so this is a map
// lookup on all but one call in five seconds, and that one call is a read the
// write path was going to make anyway. Skipped for a namespace nothing tiers,
// which has no claim to read and would otherwise cost a round trip per write.
func (r *Router) noticeIndexChanges(ctx context.Context, ns string) {
	if !r.cfg.Manages(ns) {
		return
	}
	// The side effect is the point: completenessClaim drops the cached listing
	// when the number it reads has moved. See tiers.go.
	_, _, _ = r.completenessClaim(ctx, ns)
}

// forgetIndexes drops what is remembered about ns, so the next write re-lists.
//
// Without this a unique index created on a running system is not enforced
// across tiers until the cache expires, and that window is exactly when it
// matters: somebody has just decided this field must be unique.
func (r *Router) forgetIndexes(ns string) {
	r.idxCache.mu.Lock()
	delete(r.idxCache.byNS, ns)
	delete(r.idxCache.fetched, ns)
	r.idxCache.mu.Unlock()
}

// staleIndexes makes the next write re-read ns's unique indexes without
// throwing away what is remembered until it has something truthful to replace
// it with.
//
// For a collection whose indexes have changed underneath this router: dropped
// with the collection, or dropped on their own straight on the fast tier.
// Leaving the listing alone is what used to happen for the first, and it goes
// wrong. A collection dropped and rebuilt out
// of band -- which is what resetting an environment looks like -- does not have
// the old collection's unique index, and the router went on refusing writes on
// its behalf, with a duplicate key error naming an index that exists on neither
// tier. Measured: an insert plain mongod accepts, refused with 11000. The
// fuzzer found it after four rounds, because its own reseed is that shape.
//
// What this gives up is a minute of enforcing the old constraint after a drop
// the collection does not come back from the same way. That was never a
// guarantee: the listing expires on its own after indexRefresh, so the
// constraint was going to be lost a minute later regardless, and losing it
// quietly is what made it look like one. uniqueIndexes says so in a log line
// now, which is the honest version of the same outcome.
//
// It is not forgetIndexes, because there is one case where the old listing is
// still the best answer available: a listing nobody could read. Keeping the
// entry and only marking it old is what lets uniqueIndexes fall back to it.
func (r *Router) staleIndexes(ns string) {
	r.idxCache.mu.Lock()
	if _, known := r.idxCache.byNS[ns]; known {
		r.idxCache.fetched[ns] = time.Time{}
	}
	r.idxCache.mu.Unlock()
}

// indexDDL reports whether a command changes which indexes a collection has,
// and on which collection.
func indexDDL(cmd bson.Raw, name string) (string, bool) {
	switch name {
	// create belongs here too: it makes a new collection under a name this
	// router may still remember something about, and what it remembers is about
	// whatever was called that before.
	case "createIndexes", "dropIndexes", "drop", "collMod", "create":
		coll, ok := stringField(cmd, name)
		return coll, ok
	}
	return "", false
}

// A document's registry ids are docKeys, in multikey.go. There is no
// single-key form here on purpose: a field holding an array puts one entry per
// element into the index, and every path that claims a key has to claim all of
// them. The one that read a single value per document let a duplicate of an
// archived document through, and the archive then refused the mirrored insert.

// keyIDFromValues builds the same id from field values worked out some other
// way, which is what the update path has to do: the document that will hold the
// key does not exist yet. A nil entry means the field is absent, encoded exactly
// as keyID encodes a missing field, or the two would disagree about the same
// document.
func keyIDFromValues(ns string, idx uniqueIndex, vals []any) (string, bool) {
	if len(vals) != len(idx.fields) {
		return "", false
	}
	key := make(bson.D, 0, len(idx.fields))
	for i, f := range idx.fields {
		// Numbers first, because mongod makes one key out of the same whole
		// number written as an int, a long, a double or a decimal, and
		// marshalling gives each of them different bytes. See numkey.go.
		key = append(key, bson.E{Key: f, Value: canonicalKeyValue(vals[i])})
	}
	raw, err := bson.Marshal(key)
	if err != nil {
		return "", false
	}
	return ns + "|" + idx.name + "|" + base64.RawStdEncoding.EncodeToString(raw), true
}

// keyClaim is one row of the registry.
type keyClaim struct {
	ID    string        `bson:"_id"`
	NS    string        `bson:"ns"`
	Index string        `bson:"idx"`
	Doc   bson.RawValue `bson:"doc"`
	At    time.Time     `bson:"at"`
}

func (r *Router) keys() *mongo.Collection {
	return r.hot.Database(meta.DB).Collection(keysCollection)
}

// claimRow is the registry row to write, with the owner left out when there is
// none to record.
//
// An owner is not always known at the moment a key has to be claimed. An upsert
// whose filter carries no _id is about to create a document mongod will name
// itself, and an insert can arrive without an _id for the same reason. Writing
// keyClaim directly in that state does not fail the write, it fails the claim:
// bson cannot marshal a zero RawValue at all, "the RawValue Type specifies an
// invalid BSON type: 0x0", and the guard's answer to a registry it cannot write
// is to let the write through. So the one case the owner is unknown was the one
// case nothing was enforced.
func claimRow(kid, ns, index string, owner bson.RawValue, now time.Time) bson.D {
	row := bson.D{
		{Key: "_id", Value: kid},
		{Key: "ns", Value: ns},
		{Key: "idx", Value: index},
		{Key: "at", Value: now},
	}
	if !owner.IsZero() {
		row = append(row, bson.E{Key: "doc", Value: owner})
	}
	return row
}

// keyClash is one document in a batch whose unique key is already taken.
type keyClash struct {
	doc   int    // position in the client's batch
	index string // the unique index that rejected it
}

// claimCandidate is one key one document in the batch needs.
type claimCandidate struct {
	doc   int // position in the client's batch
	idx   int // position in idxs
	kid   string
	owner bson.RawValue
	vals  []any
}

// claimKeys reserves every unique key the batch needs.
//
// Rejection is per document, not per batch, because that is what mongod does. An
// unordered insertMany with one duplicate still inserts the rest and reports one
// error, and an application relying on that would silently lose documents if the
// whole batch were refused. Ordered stops at the first clash, again like mongod,
// so nothing after it is inserted and nothing after it keeps a claim.
//
// The claims go in one round trip. They used to go one at a time, which is the
// same shape as inserting a batch of documents one by one and costs the same:
// measured on this box, 500 documents into a tiered collection with one unique
// index took 8,052 ms, against 54 ms for the same insert into a collection the
// guard steps aside for. The registry was 150 times the cost of the write it
// protects.
func (r *Router) claimKeys(ctx context.Context, ns string, docs []bson.Raw, idxs []uniqueIndex, ordered bool) ([]string, []keyClash, error) {
	dbName, _ := splitNS(ns)

	// Which documents each index covers, one question per index rather than one
	// per document and index. See constrainsBatch.
	covered := make([][]bool, len(idxs))
	for i := range idxs {
		covered[i] = r.constrainsBatch(ctx, dbName, idxs[i], docs)
	}

	cands := make([]claimCandidate, 0, len(docs))
	for d, doc := range docs {
		id, _ := doc.LookupErr("_id")
		for i := range idxs {
			if !covered[i][d] {
				continue
			}
			// One candidate per key, not per document. A field holding an
			// array puts one entry per element into the index. See multikey.go.
			kids, tuples := docKeys(ns, idxs[i], doc)
			for k, kid := range kids {
				cands = append(cands, claimCandidate{
					doc: d, idx: i, kid: kid, owner: id, vals: tuples[k],
				})
			}
		}
	}
	if len(cands) == 0 {
		return nil, nil, nil
	}

	settled, err := r.bulkClaim(ctx, ns, idxs, cands)
	if err != nil {
		return nil, nil, err
	}

	var claimed []string
	var claimedFor []int
	var clashes []keyClash
	rejected := map[int]bool{}
	for i, cd := range cands {
		// mongod reports one reason per document, and the first index to reject
		// it is the one it names.
		if rejected[cd.doc] {
			continue
		}
		got := settled[i]
		if !got {
			// Somebody holds this key, or it is a collated index the bulk pass
			// left alone. Either way it is decided one at a time, because that
			// is where a stale claim is resolved and where the archive is asked
			// in the index's own comparison.
			var terr error
			got, terr = r.tryClaim(ctx, ns, idxs[cd.idx], cd.kid, cd.owner, cd.vals)
			if terr != nil {
				r.releaseKeys(ctx, claimed)
				return nil, nil, terr
			}
		}
		if got {
			claimed = append(claimed, cd.kid)
			claimedFor = append(claimedFor, cd.doc)
			continue
		}
		clashes = append(clashes, keyClash{doc: cd.doc, index: idxs[cd.idx].name})
		rejected[cd.doc] = true
	}

	if len(clashes) == 0 {
		return claimed, nil, nil
	}

	// The bulk pass claimed keys before anything knew which documents would be
	// rejected, and a document that is not going to be inserted must not keep
	// one: it would block that value until something collided with it.
	drop := map[int]bool{}
	for _, cl := range clashes {
		drop[cl.doc] = true
	}
	if ordered {
		for d := clashes[0].doc; d < len(docs); d++ {
			drop[d] = true
		}
	}
	keep := make([]string, 0, len(claimed))
	var give []string
	for i, kid := range claimed {
		if drop[claimedFor[i]] {
			give = append(give, kid)
			continue
		}
		keep = append(keep, kid)
	}
	r.releaseKeys(ctx, give)
	return keep, clashes, nil
}

// bulkClaim writes every claim the batch needs in one round trip and reports
// which of them are now this batch's.
//
// A duplicate key error here means somebody already holds that key, which is not
// yet an answer: the claim may be stale. Those go back to tryClaim, one at a
// time, which is where staleness is resolved. So the fast path is one insert and
// the slow path is only paid where there is a real collision.
func (r *Router) bulkClaim(ctx context.Context, ns string, idxs []uniqueIndex, cands []claimCandidate) (map[int]bool, error) {
	now := time.Now().UTC()
	rows := make([]any, 0, len(cands))
	at := make([]int, 0, len(cands))
	for i, cd := range cands {
		if len(idxs[cd.idx].collation) > 0 {
			// A collated index calls strings equal that the key id keeps apart,
			// so the archive has to be asked in the index's own comparison
			// before the key can be taken. That is per key work: see
			// collatedArchiveClash.
			continue
		}
		rows = append(rows, claimRow(cd.kid, ns, idxs[cd.idx].name, cd.owner, now))
		at = append(at, i)
	}
	got := make(map[int]bool, len(at))
	if len(rows) == 0 {
		return got, nil
	}
	for _, i := range at {
		got[i] = true
	}

	_, err := r.keys().InsertMany(ctx, rows, options.InsertMany().SetOrdered(false))
	if err == nil {
		return got, nil
	}
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) {
		return nil, err
	}
	if bwe.WriteConcernError != nil {
		// The rows may or may not be there. Treating them as claimed would hand
		// out keys on a maybe.
		return nil, err
	}
	for _, we := range bwe.WriteErrors {
		if we.Code != 11000 {
			return nil, err
		}
		if we.Index >= 0 && we.Index < len(at) {
			delete(got, at[we.Index])
		}
	}
	return got, nil
}

// tryClaim reserves one key for one document, resolving a stale claim if that
// is what stands in the way. It reports whether the key is now this document's.
//
// Both the insert and the update path go through here, so a value is reserved
// the same way whichever command is about to write it.
func (r *Router) tryClaim(ctx context.Context, ns string, idx uniqueIndex, kid string, owner bson.RawValue, vals []any) (bool, error) {
	got, _, err := r.tryClaimFresh(ctx, ns, idx, kid, owner, vals)
	return got, err
}

// tryClaimFresh is tryClaim, also reporting whether the row is new.
//
// Only the rebuild walk needs the second answer, and it needs it because the
// number it prints is "keys claimed": counting a row that was already this
// document's would make a second rebuild of an unchanged collection report work
// it did not do, which is how an operator tells a rebuild that found something
// from one that found nothing.
func (r *Router) tryClaimFresh(ctx context.Context, ns string, idx uniqueIndex, kid string, owner bson.RawValue, vals []any) (got, fresh bool, err error) {
	// An index with a collation calls strings equal that the id derived above
	// keeps apart, so the archive is asked in the comparison the index itself
	// uses. See collatedkeys.go. This costs one indexed lookup, and only on an
	// index that carries a collation.
	if r.collatedArchiveClash(ctx, ns, idx, vals, owner) {
		return false, false, nil
	}

	now := time.Now().UTC()
	_, err = r.keys().InsertOne(ctx, claimRow(kid, ns, idx.name, owner, now))
	if err == nil {
		return true, true, nil
	}
	if !mongo.IsDuplicateKeyError(err) {
		return false, false, err
	}

	// Somebody holds this key. Whether that is a real duplicate depends on
	// whether their document still exists and still carries this value.
	var claim keyClaim
	derr := r.keys().FindOne(ctx, bson.D{{Key: "_id", Value: kid}}).Decode(&claim)
	if errors.Is(derr, mongo.ErrNoDocuments) {
		// Released between the failed insert and this read. Try once more.
		_, ierr := r.keys().InsertOne(ctx, claimRow(kid, ns, idx.name, owner, now))
		if ierr == nil {
			return true, true, nil
		}
		if mongo.IsDuplicateKeyError(ierr) {
			return false, false, nil
		}
		return false, false, ierr
	}
	if derr != nil {
		return false, false, derr
	}
	// Already ours. An update that rewrites a field to the value it already has
	// must not be rejected by the claim the same document made.
	if !owner.IsZero() && claim.Doc.Equal(owner) {
		return true, false, nil
	}

	live, lerr := r.claimIsLive(ctx, ns, idx, kid, claim, vals, owner)
	if lerr != nil {
		return false, false, lerr
	}
	if live {
		return false, false, nil
	}

	// Stale. Take it over, unless somebody else just did.
	filter := bson.D{{Key: "_id", Value: kid}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "doc", Value: owner}, {Key: "at", Value: now}}}}
	if owner.IsZero() {
		// Nothing to write as the owner, so the row keeps none, and "somebody
		// else just did" means the owner it was judged stale for is still there.
		filter = append(filter, bson.E{Key: "doc", Value: bson.D{{Key: "$exists", Value: !claim.Doc.IsZero()}}})
		update = bson.D{
			{Key: "$set", Value: bson.D{{Key: "at", Value: now}}},
			{Key: "$unset", Value: bson.D{{Key: "doc", Value: ""}}},
		}
	} else {
		filter = append(filter, bson.E{Key: "doc", Value: bson.D{{Key: "$ne", Value: owner}}})
	}
	res, uerr := r.keys().UpdateOne(ctx, filter, update)
	if uerr != nil {
		return false, false, uerr
	}
	// Taken over rather than inserted, and it counts as fresh: the row now
	// stands for a different document than it did, which is work the walk did.
	return res.MatchedCount > 0, res.MatchedCount > 0, nil
}

// claimIsLive reports whether a claim still stands: its owning document must
// still exist in one of the tiers, and must still carry the value the claim is
// for.
//
// Checking only that the document exists is not enough. A document that moved
// off a value leaves its old claim behind pointing at itself, and that claim
// would then block a perfectly legitimate insert of a value nobody holds. So
// the owner is read in full and its key recomputed, rather than probed for
// existence.
func (r *Router) claimIsLive(ctx context.Context, ns string, idx uniqueIndex, kid string,
	claim keyClaim, vals []any, newOwner bson.RawValue) (bool, error) {
	if claim.Doc.IsZero() {
		// No owner to re-read, so the question is asked of the tiers instead:
		// does any live document hold this value? Treating it as taken without
		// asking would block the value for ever, since nothing could ever prove
		// it free again.
		return r.keyHeldByLiveDocument(ctx, ns, idx, vals, newOwner), nil
	}

	db, coll := splitNS(ns)

	// The fast tier is authoritative where it holds the document: the archive
	// trails it by the feeder's mirroring lag and may still show the old value.
	var owner bson.Raw
	hotErr := r.hot.Database(db).Collection(coll).
		FindOne(ctx, bson.D{{Key: "_id", Value: claim.Doc}}).Decode(&owner)
	if hotErr == nil {
		return r.stillHoldsKey(ctx, ns, idx, kid, owner), nil
	}
	if !errors.Is(hotErr, mongo.ErrNoDocuments) {
		return true, hotErr
	}

	coldOwner := func() error {
		return r.cold.Database(db).Collection(coll).
			FindOne(ctx, bson.D{
				{Key: "_id", Value: claim.Doc},
				meta.NotArchivedDeleted(),
			}).Decode(&owner)
	}
	coldErr := coldOwner()
	if coldErr == nil {
		// The owner is gone from the fast tier and live in the archive, which
		// is also what a delete looks like before its tombstone lands. Asked
		// again once mirroring has caught up. See clashIsFresh.
		r.clashIsFresh(ctx, ns)
		coldErr = coldOwner()
	}
	if coldErr == nil {
		return r.stillHoldsKey(ctx, ns, idx, kid, owner), nil
	}
	if !errors.Is(coldErr, mongo.ErrNoDocuments) {
		return true, coldErr
	}
	return false, nil
}

// stillHoldsKey reports whether doc's current value for idx is the one kid was
// issued for. A document that cannot be keyed is treated as still holding it,
// because releasing a key on a guess is the one outcome that loses data.
func (r *Router) stillHoldsKey(ctx context.Context, ns string, idx uniqueIndex, kid string, doc bson.Raw) bool {
	dbName, _ := splitNS(ns)
	// A document that has moved outside a sparse or partial index is no longer
	// indexed by it, so it no longer holds the key either.
	if !r.constrains(ctx, dbName, idx, doc) {
		return false
	}
	kids, _ := docKeys(ns, idx, doc)
	if len(kids) == 0 {
		return true
	}
	// Among, not equal to. A document with an array holds one key per element,
	// and it still holds this one while that element is in it.
	for _, got := range kids {
		if got == kid {
			return true
		}
	}
	return false
}

// releaseKeys undoes claims after an insert that did not land.
func (r *Router) releaseKeys(ctx context.Context, ids []string) {
	if len(ids) == 0 {
		return
	}
	_, err := r.keys().DeleteMany(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
	})
	if err != nil {
		r.log.Warn("could not release unique key claims after a failed insert",
			"err", err, "note", "they will be reclaimed automatically on the next collision")
	}
}

// guardInsert enforces unique indexes across both tiers, before the insert runs.
//
// Returns a reply when the router answered the insert itself, either because
// every document was rejected or because it had to run a trimmed batch and
// merge the rejections back into the answer.
func (c *conn) guardInsert(ctx context.Context, msg *wire.Message, db string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	coll, ok := stringField(cmd, "insert")
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}

	docs := msg.AllOf("documents")
	if len(docs) == 0 {
		return nil, false
	}

	// An insert is what creates the collection when the fast tier does not hold
	// it, and a collection created that way carries none of the archive's
	// options: a default collation decides which documents match and cannot be
	// added afterwards. Measured on a collection the archive holds with
	// {locale: "en", strength: 2}: the copy this insert created had none, so the
	// archive answered countDocuments({name: "alice"}) with 3 and the fast tier's
	// copy with 0. Memoised inside the indexer, so this is a map lookup once the
	// fast tier has the collection. See Indexer.EnsureCollection.
	c.r.ensureTieredCollection(ctx, ns)

	ordered := true
	if v, lerr := cmd.LookupErr("ordered"); lerr == nil {
		if b, bok := v.BooleanOK(); bok {
			ordered = b
		}
	}

	// _id first, and separately from the registry, because it needs no
	// bookkeeping. The registry exists because a document can move off a unique
	// value and leave a claim behind pointing at itself, so a claim has to be
	// re-read against its owner. _id never moves, so the archive is the answer
	// and one lookup settles it.
	//
	// It is asked of the archive while the registry is claimed on the fast tier,
	// because the two questions are independent and they are asked of different
	// clusters. Sequentially they are two round trips on the critical path of
	// every insert; concurrently the insert waits for the slower of the two.
	// Measured here, where both tiers are on one machine, insertOne p50 went
	// from 6 ms to 5 ms against 3 ms with no guard at all. The reason to do it
	// is not that millisecond: in the shape this system is built for the archive
	// is the far cluster, and this takes it off the path the fast tier is
	// already on.
	idCh := make(chan idCheck, 1)
	idDocs := docs
	c.r.goSafely("checking inserted ids against the archive", func() {
		// The send is deferred inside the work so a recovered panic still
		// answers: safely would otherwise return and leave this insert waiting
		// for ever. errIDCheckPanicked reads as "could not check", which is the
		// refusal below.
		res := idCheck{err: errIDCheckPanicked}
		defer func() { idCh <- res }()
		res.clashes, res.err = c.r.archivedIDClashes(ctx, ns, idDocs)
	})

	idxs := c.r.uniqueIndexes(ctx, ns)
	var claimed []string
	var clashes []keyClash
	if len(idxs) > 0 {
		var kerr error
		claimed, clashes, kerr = c.r.claimKeys(ctx, ns, docs, idxs, ordered)
		if kerr != nil {
			// Never block a write because the registry is unhappy. Losing the
			// cross-tier half of a unique index is bad; refusing every insert
			// because a bookkeeping collection is unreachable is worse.
			c.r.releaseKeys(ctx, claimed)
			claimed, clashes = nil, nil
			c.log.Warn("could not check unique keys, letting the insert through", "ns", ns, "err", kerr)
		}
	}

	idRes := <-idCh
	idClashes, ierr := idRes.clashes, idRes.err
	if ierr != nil {
		// An insert that cannot be checked is refused, because the failure this
		// check exists to stop destroys data that is already safe.
		//
		// It used to be let through. Measured with the archive stopped: an
		// insert of an _id the archive held was accepted after the lookup timed
		// out, landed on the fast tier, and the feeder then mirrored it over the
		// archived record. {_id: 500, v: "archived, precious", tier: "gold"}
		// came back as {_id: 500, v: "clobber"}. Nothing anywhere said so.
		//
		// The one case that stays safe is a fast tier claiming all of history:
		// there is nothing archived it does not also hold, so its own _id index
		// is the whole answer and the archive has nothing to add.
		if mark, known := c.r.completeness(ctx, ns); known && mark.Claimed && mark.CoversAll() {
			c.log.Warn("could not check inserted ids against the archive; the fast tier claims "+
				"all of history, so its own _id index is the whole answer",
				"ns", ns, "err", ierr)
			idClashes = nil
		} else {
			// The claims belong to an insert that is about to be refused, so
			// they go back before anything returns. Not before the branch
			// above: that one lets the insert through, and an insert that
			// proceeds keeps its claims or it is not protected at all.
			c.r.releaseKeys(ctx, claimed)
			c.r.Stats.Refused.Add(1)
			c.cls = ClassRefused
			c.log.Error("refused an insert because the archive could not be checked",
				"ns", ns, "err", ierr)
			out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
				"thermocline: refusing this insert because the archive could not be reached to check "+
					"whether it already holds these ids. An insert of an id the archive holds is "+
					"mirrored over the archived document, which destroys it. Retry once the archive "+
					"is back, or warm this namespace so the fast tier holds all of history.")
			if rerr != nil {
				return nil, false
			}
			return out, true
		}
	}

	// One document can only be rejected once, and mongod reports the first
	// reason. _id is checked first, so it wins.
	clashes = mergeClashes(idClashes, clashes)

	if len(clashes) == 0 {
		c.pendingKeys = claimed
		return nil, false
	}

	c.r.Stats.DuplicateKeys.Add(int64(len(clashes)))
	c.log.Debug("insert duplicates an archived document", "ns", ns, "rejected", len(clashes), "of", len(docs))

	// Which documents still go through. Ordered stops at the first clash, the
	// way mongod does; unordered keeps everything that did not clash.
	rejected := map[int]string{}
	for _, cl := range clashes {
		rejected[cl.doc] = cl.index
	}
	limit := len(docs)
	if ordered {
		limit = clashes[0].doc
	}
	var keep []bson.Raw
	var keepAt []int
	for i := 0; i < limit; i++ {
		if _, bad := rejected[i]; bad {
			continue
		}
		keep = append(keep, docs[i])
		keepAt = append(keepAt, i)
	}

	if len(keep) == 0 {
		c.r.releaseKeys(ctx, claimed)
		out, rerr := c.r.insertErrorReply(msg, ns, 0, clashes, nil, nil)
		if rerr != nil {
			return nil, false
		}
		return out, true
	}

	// Run the trimmed batch, then put the rejections back so the client sees the
	// answer it would have had from one cluster.
	trimmed, terr := rebuildInsert(cmd, keep)
	if terr != nil {
		c.r.releaseKeys(ctx, claimed)
		return nil, false
	}
	fwd, ferr := wire.NewMsg(msg.Header.RequestID, 0, trimmed)
	if ferr != nil {
		c.r.releaseKeys(ctx, claimed)
		return nil, false
	}
	if werr := wire.Write(c.upstream, fwd); werr != nil {
		c.upstreamBroke()
		c.r.releaseKeys(ctx, claimed)
		return nil, false
	}
	reply, rerr := wire.Read(c.upstream)
	if rerr != nil {
		c.upstreamBroke()
		c.r.releaseKeys(ctx, claimed)
		return nil, false
	}
	rdoc, derr := reply.ReplyDoc()
	if derr != nil {
		return reply, true
	}

	n := 0
	if v, lerr := rdoc.LookupErr("n"); lerr == nil {
		if got, iok := intOf(v); iok {
			n = got
		}
	}
	out, merr := c.r.insertErrorReply(msg, ns, n, clashes, rdoc, keepAt)
	if merr != nil {
		return reply, true
	}
	return out, true
}

// rebuildInsert reproduces an insert command carrying only the documents that
// are still going through. They go inline rather than in a payload sequence,
// which is equally valid and much simpler to assemble.
func rebuildInsert(cmd bson.Raw, docs []bson.Raw) (bson.D, error) {
	elems, err := cmd.Elements()
	if err != nil {
		return nil, err
	}
	arr := make(bson.A, 0, len(docs))
	for _, d := range docs {
		arr = append(arr, d)
	}
	out := make(bson.D, 0, len(elems)+1)
	replaced := false
	for _, e := range elems {
		if e.Key() == "documents" {
			out = append(out, bson.E{Key: "documents", Value: arr})
			replaced = true
			continue
		}
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	if !replaced {
		out = append(out, bson.E{Key: "documents", Value: arr})
	}
	return out, nil
}

// insertErrorReply builds the reply for a batch that was partly rejected.
//
// The client indexed its documents by their position in the batch it sent, so
// every error has to carry that position: ours directly, and the fast tier's
// mapped back through the documents that were kept.
func (r *Router) insertErrorReply(msg *wire.Message, ns string, n int, clashes []keyClash, upstream bson.Raw, keepAt []int) (*wire.Message, error) {
	errs := bson.A{}
	for _, cl := range clashes {
		errs = append(errs, bson.D{
			{Key: "index", Value: int32(cl.doc)},
			{Key: "code", Value: int32(11000)},
			{Key: "errmsg", Value: "E11000 duplicate key error collection: " + ns + " index: " + cl.index +
				" :: thermocline: the existing document with this key is in the archive, " +
				"where the fast tier's unique index cannot see it"},
		})
	}
	if len(upstream) > 0 && keepAt != nil {
		if v, err := upstream.LookupErr("writeErrors"); err == nil {
			if arr, ok := v.ArrayOK(); ok {
				if vals, verr := arr.Values(); verr == nil {
					for _, wv := range vals {
						wd, dok := wv.DocumentOK()
						if !dok {
							continue
						}
						out := bson.D{}
						for _, e := range mustElements(wd) {
							if e.Key() == "index" {
								if got, iok := intOf(e.Value()); iok && got < len(keepAt) {
									out = append(out, bson.E{Key: "index", Value: int32(keepAt[got])})
									continue
								}
							}
							out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
						}
						errs = append(errs, out)
					}
				}
			}
		}
	}
	return wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, bson.D{
		{Key: "n", Value: int32(n)},
		{Key: "writeErrors", Value: errs},
		{Key: "ok", Value: 1.0},
	})
}

func mustElements(d bson.Raw) []bson.RawElement {
	e, err := d.Elements()
	if err != nil {
		return nil
	}
	return e
}

// duplicateKeyReply is the error the server would have produced if the original
// document had still been in the fast tier.
func (r *Router) duplicateKeyReply(requestID, responseTo int32, ns, index string) (*wire.Message, error) {
	msg := "E11000 duplicate key error collection: " + ns + " index: " + index +
		" :: thermocline: the existing document with this key is in the archive, " +
		"where the fast tier's unique index cannot see it"
	return wire.NewMsg(requestID, responseTo, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "n", Value: int32(0)},
		{Key: "writeErrors", Value: bson.A{bson.D{
			{Key: "index", Value: int32(0)},
			{Key: "code", Value: int32(11000)},
			{Key: "errmsg", Value: msg},
		}}},
		{Key: "code", Value: int32(11000)},
		{Key: "codeName", Value: "DuplicateKey"},
		{Key: "errmsg", Value: msg},
	})
}

// RebuildKeys claims a key for every document that already exists.
//
// The registry only protects what it knows about, so a database adopted with
// documents already in it starts with none of its keys claimed and none of its
// unique indexes enforced across tiers. This walks both tiers and fills it in.
// It is safe to re-run, and it is also the recovery path after a fast tier is
// rebuilt, since the registry lives there.
//
// Conflicts found during the walk are reported rather than resolved. Two live
// documents already sharing a unique key means the constraint was broken before
// this ran, and quietly picking a winner would hide that.
func (r *Router) RebuildKeys(ctx context.Context, namespaces []string) (map[string]int, map[string]int, error) {
	claimed := map[string]int{}
	conflicts := map[string]int{}

	for _, ns := range namespaces {
		idxs := r.uniqueIndexes(ctx, ns)
		if len(idxs) == 0 {
			continue
		}
		got, clash, _, err := r.claimExistingKeys(ctx, ns, idxs)
		claimed[ns] = got
		conflicts[ns] = clash
		if err != nil {
			return claimed, conflicts, err
		}
	}
	return claimed, conflicts, nil
}

// claimExistingKeys claims a key for every document ns already holds, under the
// indexes it is given rather than the ones the listing reports.
//
// Given the indexes, because the caller that matters most is asking about one
// that does not exist yet: a createIndexes deciding whether the two tiers
// together already break the constraint it is about to declare. See
// newuniqueindex.go.
//
// firstClash names a key two live documents share, for the error message. The
// walk does not stop there: the count is worth having, and mongod's own build
// reports one key and finds them all the same way.
func (r *Router) claimExistingKeys(ctx context.Context, ns string, idxs []uniqueIndex) (claimed, conflicts int, firstClash string, err error) {
	db, coll := splitNS(ns)

	// The archive holds every document, so it is the authoritative walk. The
	// fast tier is walked too, for anything written since the last mirror.
	for _, src := range []struct {
		client      *mongo.Client
		filter      bson.D
		fromArchive bool
	}{
		{r.cold, bson.D{meta.NotArchivedDeleted()}, true},
		{r.hot, bson.D{}, false},
	} {
		fromArchive := src.fromArchive
		cur, ferr := src.client.Database(db).Collection(coll).Find(ctx, src.filter)
		if ferr != nil {
			return claimed, conflicts, firstClash, ferr
		}
		for cur.Next(ctx) {
			doc := bson.Raw(cur.Current)
			id, _ := doc.LookupErr("_id")
			for i := range idxs {
				if !r.constrains(ctx, db, idxs[i], doc) {
					continue
				}
				// One row per key, because a field holding an array puts
				// one entry per element into the index. A rebuild that
				// claimed one of them left the rest of that document's
				// values unprotected, which is what --rebuild exists to
				// stop. See multikey.go.
				kids, tuples := docKeys(ns, idxs[i], doc)
				for k, kid := range kids {
					// tryClaim rather than a bare insert, so this walk reaches
					// the same verdict the write paths do. A row whose owner
					// has moved off the value is stale and is taken over; only
					// an owner that still holds the key is a conflict. Doing
					// that here by hand was the difference between reporting a
					// duplicate and reporting a claim nobody had cleaned up.
					got, fresh, terr := r.tryClaimFresh(ctx, ns, idxs[i], kid, id, tuples[k])
					if terr != nil {
						_ = cur.Close(context.WithoutCancel(ctx))
						return claimed, conflicts, firstClash, terr
					}
					if got {
						// Only a row that was not already this document's. A
						// second rebuild of an unchanged collection has to
						// report nothing, which is how an operator tells one
						// that found something from one that did not.
						if fresh {
							claimed++
						}
						continue
					}
					// One document seen twice is not two documents.
					//
					// This walk reads both tiers, and during mirroring lag the
					// archive's copy of a document can carry a value the
					// document no longer has. Claiming from it reports a
					// duplicate against whichever live document really holds
					// that value. The fast tier is authoritative where it holds
					// the document, which is the same rule claimIsLive follows,
					// so an archive copy the fast tier has a newer version of
					// is skipped and the fast tier's own pass covers it.
					//
					// On the conflict path only. It is a read per conflict, and
					// a walk with no conflicts pays nothing for it.
					if fromArchive && r.staleArchiveCopy(ctx, ns, idxs[i], kid, id) {
						continue
					}
					conflicts++
					if firstClash == "" {
						firstClash = describeKey(idxs[i], doc)
					}
				}
			}
		}
		cerr := cur.Err()
		_ = cur.Close(context.WithoutCancel(ctx))
		if cerr != nil {
			return claimed, conflicts, firstClash, cerr
		}
	}
	return claimed, conflicts, firstClash, nil
}

// staleArchiveCopy reports whether the archive's copy of a document is behind
// the fast tier's, for this key.
//
// The fast tier is authoritative where it holds the document: the archive trails
// it by the feeder's mirroring lag and may still show the value the document had
// before its last write. A copy like that is not a second document and must not
// be counted as one.
func (r *Router) staleArchiveCopy(ctx context.Context, ns string, idx uniqueIndex, kid string, id bson.RawValue) bool {
	db, coll := splitNS(ns)
	var current bson.Raw
	if err := r.hot.Database(db).Collection(coll).
		FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&current); err != nil {
		// Not on the fast tier, or unreadable. Either way the archive's copy is
		// the only one there is, so it stands and the conflict is real.
		return false
	}
	return !r.stillHoldsKey(ctx, ns, idx, kid, current)
}

// describeKey renders a document's value for an index the way mongod renders a
// duplicate key in its own error, so the message an application sees says the
// same thing whichever side refused.
func describeKey(idx uniqueIndex, doc bson.Raw) string {
	var b strings.Builder
	b.WriteString("{ ")
	for i, f := range idx.fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(f)
		b.WriteString(": ")
		if v, ok := lookupPath(doc, f); ok {
			b.WriteString(v.String())
		} else {
			b.WriteString("null")
		}
	}
	b.WriteString(" }")
	return b.String()
}

// UniqueNamespaces returns the managed namespaces that have unique indexes the
// router has to enforce itself.
func (r *Router) UniqueNamespaces(ctx context.Context, candidates []string) []string {
	var out []string
	for _, ns := range candidates {
		if len(r.uniqueIndexes(ctx, ns)) > 0 {
			out = append(out, ns)
		}
	}
	return out
}

// warnAboutUnclaimedUniqueKeys names tiered collections whose unique indexes
// the registry has never claimed a single key for, while the archive holds
// documents those indexes would index.
//
// That combination means one thing: a cluster adopted with data and indexes
// already in it. A unique index created *through* this router walks both tiers
// and claims what is already there before the build is forwarded, and a fast
// tier built from nothing has nothing to claim, so neither of those is warned
// about. What is left is the archive that arrived with the constraint already
// on it and nothing having walked it.
//
// **Until something claims them the constraint is not enforced across the
// tiers.** An insert duplicating a value only the archive holds is accepted:
// the fast tier's own index cannot see the other half, and the registry has no
// row to collide with -- a key whose row inserts cleanly is settled there and
// then, with no question asked of the archive. So this is not a slow answer, it
// is a uniqueness guarantee an application has relied on for years quietly not
// holding on the day it is adopted.
//
// The remedy is one command. It was written down as the answer to "an index
// created out of band, a fast tier rebuilt from nothing, a cluster adopted with
// data already in it" and never as a consequence of skipping it, and a
// documented step is not a guarantee.
//
// After ensureFastTierIndexes on purpose: on an adoption the fast tier's copy
// of the index may only exist because the Indexer has just built it, and it is
// the fast tier's listing the router enforces from, so before that pass the
// state this looks for is not yet meaningful.
//
// It stays quiet whenever it cannot tell. A read it cannot make is not evidence
// of an unclaimed constraint, and a warning naming a namespace that is fine
// costs as much as silence over one that is not.
func (r *Router) warnAboutUnclaimedUniqueKeys(ctx context.Context) {
	for _, ns := range r.tieredNamespaces(ctx) {
		for _, idx := range r.uniqueIndexes(ctx, ns) {
			if r.registryHasAClaimFor(ctx, ns, idx) {
				continue
			}
			if !r.archiveHoldsSomethingIndexedBy(ctx, ns, idx) {
				continue
			}
			r.log.Warn("a unique index on a tiered collection has no claims in the registry",
				"ns", ns, "index", idx.name,
				"meaning", "this constraint is not being enforced against documents only the archive "+
					"holds, so an insert duplicating one of their values is accepted",
				"why", "nothing has walked the archive for this index; that happens when a unique "+
					"index is created through the router, and an adopted cluster arrives with "+
					"its indexes already built",
				"action", "cline keys --rebuild, which reports any duplicates that got in")
		}
	}
}

// registryHasAClaimFor reports whether the registry holds any row for this
// index, and says yes when it cannot tell.
func (r *Router) registryHasAClaimFor(ctx context.Context, ns string, idx uniqueIndex) bool {
	prefix := ns + "|" + idx.name + "|"
	// A range over _id rather than a query on the ns and idx fields, because
	// the _id index serves it and the registry carries no other: one row per
	// claimed key makes it the largest bookkeeping collection there is. Every
	// id is prefix + base64, and "}" sorts above every base64 character.
	err := r.keys().FindOne(ctx, bson.D{{Key: "_id", Value: bson.D{
		{Key: "$gte", Value: prefix},
		{Key: "$lt", Value: prefix + "}"},
	}}}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false
	}
	// Found one, or could not look. Both mean no warning.
	return true
}

// archiveHoldsSomethingIndexedBy reports whether the archive holds a live
// document this index would index, and says no when it cannot tell.
//
// Scoped by the index rather than merely counting documents, because a partial
// index over a value no archived document carries has nothing to claim and is
// not the state being warned about.
func (r *Router) archiveHoldsSomethingIndexedBy(ctx context.Context, ns string, idx uniqueIndex) bool {
	db, coll := splitNS(ns)
	filter := bson.D{meta.NotArchivedDeleted()}
	if scope := indexScope(idx); len(scope) > 0 {
		filter = append(filter, bson.E{Key: "$and", Value: scope})
	}
	err := r.cold.Database(db).Collection(coll).FindOne(ctx, filter,
		options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).Err()
	return err == nil
}

// indexScope narrows a query to the documents a unique index actually indexes,
// as clauses for an $and.
//
// A sparse or partial unique index constrains only some documents, and one
// outside it does not hold the key however plainly it holds the value. Matching
// on the values alone refused writes mongod accepts: measured on a unique index
// over sku with partialFilterExpression {active: true}, one document
// {sku: "K", active: false} and an ownerless claim row for that key, an insert
// of {sku: "K", active: true} came back E11000 from the router and was accepted
// by mongod on an untiered collection carrying the same index.
//
// In a query rather than per document, unlike pruneClaims, which reads each
// candidate and asks constrains. Both answer the same question and agree on the
// rules; this shape is for the paths where the point is to let the partial
// index serve the lookup instead of fetching what it would have excluded. Every
// operator MongoDB allows in a partialFilterExpression is one a find filter
// takes.
//
// One function rather than the same two rules written out three times, because
// the places that need them must not drift: whether a value is still held,
// whether the archive holds anything an index would index at all, and whether
// an archived document clashes under the index's collation. The third was
// written without it and refused writes mongod accepts, which is the same
// defect this function was extracted to stop.
func indexScope(idx uniqueIndex) bson.A {
	var scope bson.A
	if len(idx.partial) > 0 {
		scope = append(scope, idx.partial)
	}
	if idx.sparse {
		// A compound sparse index still indexes a document that has any one of
		// its fields, so only one missing all of them is exempt. The same rule
		// constrains applies, in the same polarity.
		var present bson.A
		for _, f := range idx.fields {
			present = append(present, bson.D{{Key: f, Value: bson.D{{Key: "$exists", Value: true}}}})
		}
		scope = append(scope, bson.D{{Key: "$or", Value: present}})
	}
	return scope
}

// keyHeldByLiveDocument reports whether either tier holds a live document with
// this key, in the comparison the index itself uses.
//
// It answers for a claim that records no owner. Errors read as held: a value
// that cannot be checked is one this must not hand to a second document.
func (r *Router) keyHeldByLiveDocument(ctx context.Context, ns string, idx uniqueIndex, vals []any, owner bson.RawValue) bool {
	if len(vals) != len(idx.fields) {
		return true
	}
	filter := bson.D{}
	for i, f := range idx.fields {
		// A missing field is indexed as null, exactly as keyID encodes it.
		filter = append(filter, bson.E{Key: f, Value: vals[i]})
	}
	if !owner.IsZero() {
		// The document being written is allowed to hold its own value.
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$ne", Value: owner}}})
	}
	// Narrowed to the documents the index actually indexes, because holding the
	// value and holding the key are not the same thing. See indexScope.
	if scope := indexScope(idx); len(scope) > 0 {
		filter = append(filter, bson.E{Key: "$and", Value: scope})
	}

	opts := options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})
	if len(idx.collation) > 0 {
		opts.SetCollation(collationFromSpec(idx.collation))
	}
	db, coll := splitNS(ns)

	var doc bson.Raw
	err := r.hot.Database(db).Collection(coll).FindOne(ctx, filter, opts).Decode(&doc)
	if err == nil {
		return true
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		r.log.Warn("could not ask the fast tier whether a unique key is still held",
			"ns", ns, "index", idx.name, "err", err)
		return true
	}

	cold := append(bson.D{meta.NotArchivedDeleted()}, filter...)
	err = r.cold.Database(db).Collection(coll).FindOne(ctx, cold, opts).Decode(&doc)
	if err == nil {
		// Held in the archive and not on the fast tier, which a delete looks
		// like until its tombstone lands. Asked again once mirroring has
		// caught up. See clashIsFresh.
		r.clashIsFresh(ctx, ns)
		err = r.cold.Database(db).Collection(coll).FindOne(ctx, cold, opts).Decode(&doc)
	}
	if err == nil {
		return true
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		r.log.Warn("could not ask the archive whether a unique key is still held",
			"ns", ns, "index", idx.name, "err", err)
		return true
	}
	return false
}

// constrainsBatch reports, for each document, whether idx covers it, asking the
// fast tier once for the whole batch instead of once per document.
//
// A partial index's filter is evaluated by the server, because reimplementing
// MongoDB's query language to decide whether a document is indexed is exactly
// the kind of guess that produces a wrong answer. Doing it per document costs a
// round trip per document: measured, 300 inserts into a collection with a
// partial unique index took 330 ms against 50 ms for the same insert with a
// plain one. One pipeline answers for the batch.
func (r *Router) constrainsBatch(ctx context.Context, dbName string, idx uniqueIndex, docs []bson.Raw) []bool {
	out := make([]bool, len(docs))

	// The sparse rule is local: a compound sparse index still indexes a
	// document carrying any one of its fields, so only one missing all of them
	// is exempt.
	candidates := make([]any, 0, len(docs))
	at := make([]int, 0, len(docs))
	for i, doc := range docs {
		if idx.sparse {
			any := false
			for _, f := range idx.fields {
				if _, ok := lookupPath(doc, f); ok {
					any = true
					break
				}
			}
			if !any {
				continue
			}
		}
		if len(idx.partial) == 0 {
			out[i] = true
			continue
		}
		candidates = append(candidates, bson.D{{Key: "i", Value: int32(len(at))}, {Key: "d", Value: doc}})
		at = append(at, i)
	}
	if len(candidates) == 0 {
		return out
	}

	// Each document is carried with its position and put back together inside
	// the pipeline, so nothing here has to rewrite BSON to add a field.
	cur, err := r.hot.Database(dbName).Aggregate(ctx, bson.A{
		bson.D{{Key: "$documents", Value: candidates}},
		bson.D{{Key: "$replaceWith", Value: bson.D{{Key: "$mergeObjects", Value: bson.A{
			"$d", bson.D{{Key: partialMarker, Value: "$i"}},
		}}}}},
		bson.D{{Key: "$match", Value: idx.partial}},
		bson.D{{Key: "$project", Value: bson.D{{Key: partialMarker, Value: 1}, {Key: "_id", Value: 0}}}},
	})
	if err != nil {
		// Could not ask. Enforcing anyway would reject writes the database
		// itself allows, which is a visible failure caused by bookkeeping, so
		// this stays out of the way and says so.
		r.log.Warn("could not evaluate a partial unique index, not enforcing it across tiers",
			"index", idx.name, "err", err,
			"effect", "a duplicate of an archived document could be inserted")
		return make([]bool, len(docs))
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	for cur.Next(ctx) {
		v, lerr := bson.Raw(cur.Current).LookupErr(partialMarker)
		if lerr != nil {
			continue
		}
		pos, ok := v.AsInt64OK()
		if !ok || pos < 0 || int(pos) >= len(at) {
			continue
		}
		out[at[pos]] = true
	}
	if err := cur.Err(); err != nil {
		r.log.Warn("could not read a partial unique index evaluation, not enforcing it across tiers",
			"index", idx.name, "err", err)
		return make([]bool, len(docs))
	}
	return out
}

// partialMarker is the field a document's position is carried in while its
// partial filter is evaluated. Nothing in a partial filter can refer to it.
const partialMarker = "__tcPartialAt"

// idCheck is the archive's answer about the ids in an insert, carried back from
// the goroutine that asked while the registry was being claimed.
type idCheck struct {
	clashes []keyClash
	err     error
}

// errIDCheckPanicked is what the caller sees when the goroutine asking the
// archive about inserted ids panicked. It reads as "could not check", which is
// the one answer that is safe: an insert of an id the archive holds is mirrored
// over the archived document, which destroys it.
var errIDCheckPanicked = errors.New("the check for archived ids panicked")

// pruneBatch is how many claims are checked with one query per tier.
const pruneBatch = 200

// PruneKeys removes claims no live document holds any more.
//
// A claim is permanent on purpose: evicting a document takes its index entry off
// the fast tier, and the registry is what remembers the value was taken. Nothing
// removed a claim whose document has been deleted from both tiers, though, and
// the registry lives on the fast tier, which is the small expensive one. Measured
// on 500 inserts into a collection with one unique index, then deleting all 500:
// the fast tier held nought documents, the archive nought live ones, and the
// registry still held 500 claims. At any real insert rate that grows with history
// for ever, which is the cost tiering exists to avoid, on the wrong tier.
//
// What makes removing one safe is that it is the same test a collision applies.
// tryClaim already asks whether the owner still exists and still carries the
// value, and hands the key over when it does not. This asks the same question
// ahead of the collision, in batches: for a batch of claims, ask each tier which
// of those key values a live document still holds, and delete the rest.
//
// Bounded by budget, and safe to interrupt: a claim it did not reach is a claim
// that stays, and the next run starts again. Nothing is written except deletions.
func (r *Router) PruneKeys(ctx context.Context, namespaces []string, budget time.Duration) (map[string]int, error) {
	removed := map[string]int{}
	deadline := time.Now().Add(budget)
	if budget <= 0 {
		deadline = time.Now().Add(time.Minute)
	}

	for _, ns := range namespaces {
		idxs := r.uniqueIndexes(ctx, ns)
		byName := map[string]uniqueIndex{}
		for _, idx := range idxs {
			byName[idx.name] = idx
		}

		cur, err := r.keys().Find(ctx, bson.D{{Key: "ns", Value: ns}})
		if err != nil {
			return removed, err
		}
		batch := map[string][]keyClaim{}
		flush := func() error {
			for name, claims := range batch {
				idx, known := byName[name]
				if !known {
					// The index itself is gone, so nothing enforces this value
					// any more and the claim describes a constraint that does
					// not exist. Those go wholesale.
					ids := make([]string, 0, len(claims))
					for _, c := range claims {
						ids = append(ids, c.ID)
					}
					if _, derr := r.keys().DeleteMany(ctx,
						bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}); derr != nil {
						return derr
					}
					removed[ns] += len(ids)
					continue
				}
				n, perr := r.pruneClaims(ctx, ns, idx, claims)
				if perr != nil {
					return perr
				}
				removed[ns] += n
			}
			batch = map[string][]keyClaim{}
			return nil
		}

		count := 0
		for cur.Next(ctx) {
			var claim keyClaim
			if cur.Decode(&claim) != nil {
				continue
			}
			batch[claim.Index] = append(batch[claim.Index], claim)
			count++
			if count%pruneBatch == 0 {
				if ferr := flush(); ferr != nil {
					_ = cur.Close(context.WithoutCancel(ctx))
					return removed, ferr
				}
				if time.Now().After(deadline) {
					_ = cur.Close(context.WithoutCancel(ctx))
					r.log.Info("stopped pruning the key registry at the time budget",
						"ns", ns, "removed", removed[ns],
						"note", "a claim not reached is a claim that stays; run it again")
					return removed, nil
				}
			}
		}
		if cerr := cur.Err(); cerr != nil {
			_ = cur.Close(context.WithoutCancel(ctx))
			return removed, cerr
		}
		_ = cur.Close(context.WithoutCancel(ctx))
		if ferr := flush(); ferr != nil {
			return removed, ferr
		}
	}
	return removed, nil
}

// pruneClaims deletes the claims in one batch that no live document holds.
//
// One query per tier for the whole batch, asked in the index's own comparison,
// and the answer is turned back into key ids so a document that has moved off a
// value does not keep it alive.
func (r *Router) pruneClaims(ctx context.Context, ns string, idx uniqueIndex, claims []keyClaim) (int, error) {
	if len(claims) == 0 {
		return 0, nil
	}
	want := map[string]bool{}
	var clauses bson.A
	for _, c := range claims {
		vals, ok := valuesFromKeyID(ns, idx, c.ID)
		if !ok {
			// A key id this cannot read is one it must not act on.
			continue
		}
		want[c.ID] = true
		clause := bson.D{}
		for i, f := range idx.fields {
			clause = append(clause, bson.E{Key: f, Value: vals[i]})
		}
		clauses = append(clauses, clause)
	}
	if len(clauses) == 0 {
		return 0, nil
	}

	db, coll := splitNS(ns)
	held := map[string]bool{}
	for _, src := range []struct {
		client *mongo.Client
		filter bson.D
	}{
		{r.hot, bson.D{{Key: "$or", Value: clauses}}},
		{r.cold, bson.D{meta.NotArchivedDeleted(), {Key: "$or", Value: clauses}}},
	} {
		opts := options.Find()
		if len(idx.collation) > 0 {
			opts.SetCollation(collationFromSpec(idx.collation))
		}
		cur, err := src.client.Database(db).Collection(coll).Find(ctx, src.filter, opts)
		if err != nil {
			return 0, err
		}
		for cur.Next(ctx) {
			doc := bson.Raw(cur.Current)
			if !r.constrains(ctx, db, idx, doc) {
				continue
			}
			kids, _ := docKeys(ns, idx, doc)
			for _, kid := range kids {
				held[kid] = true
			}
		}
		if cerr := cur.Err(); cerr != nil {
			_ = cur.Close(context.WithoutCancel(ctx))
			return 0, cerr
		}
		_ = cur.Close(context.WithoutCancel(ctx))
	}

	var dead []string
	for id := range want {
		if !held[id] {
			dead = append(dead, id)
		}
	}
	if len(dead) == 0 {
		return 0, nil
	}
	res, err := r.keys().DeleteMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: dead}}}})
	if err != nil {
		return 0, err
	}
	return int(res.DeletedCount), nil
}

// valuesFromKeyID reads a key id back into the values it was built from.
//
// The id is ns|index|base64(bson document of the index's fields), which is
// exactly the values, canonicalised. Reading them back is how a prune can ask
// whether anything still holds a key without having the document that made it.
func valuesFromKeyID(ns string, idx uniqueIndex, id string) ([]any, bool) {
	prefix := ns + "|" + idx.name + "|"
	if !strings.HasPrefix(id, prefix) {
		return nil, false
	}
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(id, prefix))
	if err != nil {
		return nil, false
	}
	doc := bson.Raw(raw)
	elems, err := doc.Elements()
	if err != nil || len(elems) != len(idx.fields) {
		return nil, false
	}
	out := make([]any, len(idx.fields))
	for i, f := range idx.fields {
		v, lerr := doc.LookupErr(f)
		if lerr != nil {
			return nil, false
		}
		if v.Type == bson.TypeNull {
			out[i] = nil
			continue
		}
		out[i] = v
	}
	return out, true
}

// warnFastTierExpires says that the fast tier's copy of a tiered collection has
// a TTL index on it, which this router would have refused.
//
// guardTTL turns down a TTL index on a tiered collection and says why: the fast
// tier holds the recent part, so the index expires the documents that were
// recent enough to be cached, the feeder reads those expiries as the
// application deleting them and the archive tombstones its own copies, and the
// old documents the rule was written for sit in the archive where no TTL index
// is watching. That guard is on this router's door. An index created straight
// against the fast tier's mongod -- an operator, a migration, a restored
// fixture -- comes through the other one, and nothing here saw it.
//
// Measured on 2026-09-14, five recent documents on both tiers and five a year
// old in the archive alone, with a thirty second TTL index put on the fast tier
// by hand: the five recent ones were gone from the fast tier and tombstoned in
// the archive, and the five year-old ones were untouched. The only trace
// anywhere was the feeder's ordinary "deletes not announced by the evictor",
// which is what a real application delete looks like too.
//
// It cannot be prevented from here, so it is said, which is what this system
// does everywhere else it cannot prevent something: the archive's own side of
// this has warnArchiveExpires, and losing a unique constraint to a drop has its
// own line. Driven by the listing rather than by watching for a createIndexes,
// because the listing is the one place that learns about an index whichever way
// it arrived, which is the same argument releaseIndexClaims makes.
//
// Said when the answer changes rather than every refresh, because the listing is
// re-read every indexRefresh and a line every minute is a line nobody reads.
func (r *Router) warnFastTierExpires(ns string, expiring []string) {
	sort.Strings(expiring)
	now := strings.Join(expiring, ",")

	r.idxCache.mu.Lock()
	if r.idxCache.ttlSaid == nil {
		r.idxCache.ttlSaid = map[string]string{}
	}
	before, seen := r.idxCache.ttlSaid[ns]
	r.idxCache.ttlSaid[ns] = now
	r.idxCache.mu.Unlock()

	if seen && before == now {
		return
	}
	if now == "" {
		if seen && before != "" {
			r.log.Info("the TTL index on a tiered collection's fast tier copy is gone",
				"ns", ns, "was", before)
		}
		return
	}
	r.log.Warn("a tiered collection has a TTL index on the fast tier, which this router refuses "+
		"and did not put there",
		"ns", ns, "indexes", expiring,
		"reason", "it was created straight against the fast tier rather than through this router",
		"effect", "the fast tier holds the recent part of this collection, so it expires the "+
			"documents recent enough to be cached and the feeder mirrors those expiries as the "+
			"application deleting them; the older documents the rule was written for are in the "+
			"archive where no TTL index is watching, so this deletes the newest and keeps the oldest",
		"action", "drop it on the fast tier, and use an evictor rule for what it was meant to do; "+
			"documents already tombstoned in the archive are named by cline rollback")
}

// warnAboutStrandedKeyClaims names the unique indexes the registry still holds
// claims for and the fast tier no longer has.
//
// The other direction of warnAboutUnclaimedUniqueKeys, and the one nothing
// asked. Uniqueness across the tiers is enforced from the fast tier's own index
// listing: uniqueIndexes reads that listing and a constraint it does not name is
// not enforced, which is right when an application drops an index and wrong when
// the listing is missing for any other reason.
//
// The reason that matters is the rebuild. Nothing copies a plain index to the
// archive on purpose, because an index describes the whole collection and
// carrying every one of them gives back most of what tiering buys. A unique
// index is not a plain index: it is a constraint the application declared, and
// leaving it off the archive means that after the fast tier is restored from the
// archive, the way docs/operations.md says a fast tier is rebuilt, the
// constraint is on neither tier. Measured by test/testrestore.sh: the registry
// was restored with 200 rows for code_1 and an insert duplicating an archived
// document's code was accepted, because no tier declared the index any more.
//
// Nothing could have said so. The comparison that notices an index going away
// is between one listing and the listing before it, so a router that never saw
// it cannot miss it, and a fresh start after a rebuild never did. The registry
// rows are the only thing left that remembers the constraint existed, so they
// are what this reads.
//
// A warning rather than a refusal, for the same reason the vanishing-index
// warning is one: refusing writes mongod accepts is worse than saying that a
// guarantee has ended, and the operator's move is to recreate the index.
func (r *Router) warnAboutStrandedKeyClaims(ctx context.Context) {
	for _, c := range r.claimedIndexes(ctx) {
		var live *uniqueIndex
		for _, idx := range r.uniqueIndexes(ctx, c.ns) {
			if idx.name == c.idx {
				live = &idx
				break
			}
		}
		if live == nil {
			r.log.Warn("the key registry holds claims for a unique index no tier has",
				"ns", c.ns, "index", c.idx, "over", c.fields,
				"meaning", "the application declared this constraint and nothing is enforcing it now, "+
					"so a write duplicating a value an archived document holds is accepted",
				"why", "a unique index lives on the fast tier alone, because nothing copies an index to "+
					"the archive, so rebuilding the fast tier from the archive leaves the constraint "+
					"on neither tier and these rows are all that remembers it",
				"action", "recreate the index through the router, which walks both tiers and reports "+
					"any duplicates that got in; or drop these claims with cline keys --rebuild if "+
					"the index was meant to go")
			continue
		}
		// An index of the same name over different fields is not the same
		// constraint, and matching on the name alone would call it present.
		// Index names are reused: see permanent-is-about-documents, where
		// "claims are permanent" was reasoned about documents and applied to
		// names. The claims here were made over c.fields, nothing now enforces
		// uniqueness over those, and the rows themselves are dead weight,
		// because every key id built from the live index carries the live
		// index's field names and can never match one of these.
		if !sameFields(live.fields, c.fields) {
			r.log.Warn("the key registry holds claims over fields this index no longer covers",
				"ns", c.ns, "index", c.idx, "claimed over", c.fields, "now over", live.fields,
				"meaning", "an index of this name was dropped and recreated over other fields, so "+
					"uniqueness over the old ones is enforced by nothing and the old rows can "+
					"never match a key again",
				"action", "cline keys --rebuild, which claims what the live indexes cover and "+
					"reports any duplicates that got in")
		}
	}
}

// claimedIndex is one unique index the registry holds claims for, and the
// fields those claims were made over.
type claimedIndex struct {
	ns, idx string
	fields  []string
}

// claimedIndexes reports every index the registry has rows for, one lookup per
// index rather than one pass over the rows.
//
// The registry is the largest bookkeeping collection there is, one row per
// claimed key, and it carries no index but _id. A $group over ns and idx is
// therefore a collection scan, and it was one: measured on a registry of
// 200,000 rows, 45ms against 4ms for the walk below, and the gap grows with the
// registry while the walk grows with the number of indexes. This runs at every
// router start and in every cline preflight.
//
// The walk is the skip scan registryHasAClaimFor already uses for one index,
// applied to all of them: read the lowest _id, then ask for the lowest _id
// above everything that index could have written. Every id is prefix + base64
// and "}" sorts above every base64 character.
//
// The prefix is rebuilt from the row's own ns and idx fields rather than parsed
// out of the id, because a collection name may contain the separator: parsing
// "shop.a|b|email_1|..." on "|" gives a namespace of "shop.a" and an index of
// "b", and the walk would then skip to the wrong place and loop. Rebuilding
// cannot be wrong, because it is how the id was written.
func (r *Router) claimedIndexes(ctx context.Context) []claimedIndex {
	var found []claimedIndex
	after := ""
	// One lookup per index, and a bound so a registry being written underneath
	// this cannot turn it into a loop.
	for i := 0; i < maxClaimedIndexes; i++ {
		var row struct {
			ID  string `bson:"_id"`
			NS  string `bson:"ns"`
			Idx string `bson:"idx"`
		}
		err := r.keys().FindOne(ctx,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: after}}}},
			options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}}).
				SetProjection(bson.D{{Key: "ns", Value: 1}, {Key: "idx", Value: 1}}),
		).Decode(&row)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return found
		}
		if err != nil {
			// The registry is on the fast tier and a router that cannot read it
			// has worse problems, all of which say so elsewhere.
			r.log.Debug("could not walk the key registry's claims", "err", err, "after", after)
			return found
		}
		if row.NS == "" || row.Idx == "" {
			// Not a claim row shaped the way claimRow writes them. Stepping
			// over it by id keeps the walk going rather than stopping on it.
			after = row.ID
			continue
		}
		prefix := row.NS + "|" + row.Idx + "|"
		found = append(found, claimedIndex{ns: row.NS, idx: row.Idx, fields: keyFieldsOf(row.ID, prefix)})
		after = prefix + "}"
	}
	r.log.Debug("stopped walking the key registry's claims at the bound", "indexes", len(found))
	return found
}

// maxClaimedIndexes bounds the walk above. A deployment with more distinct
// unique indexes than this has other problems, and the bound is what stops a
// registry written underneath the walk from turning it into a loop.
const maxClaimedIndexes = 1000

// keyFieldsOf reads the field names a claim was made over out of its id.
//
// The id is prefix + base64 of the BSON document keyIDFromValues built, whose
// keys are the index's fields in order. That makes the row carry what it was
// claimed over, which is the only thing that can tell an index of the same name
// over different fields from the same index.
//
// Nil when it cannot be read, which reads as "unknown" at every caller rather
// than as "no fields".
func keyFieldsOf(id, prefix string) []string {
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(id, prefix))
	if err != nil {
		return nil
	}
	elems, err := bson.Raw(raw).Elements()
	if err != nil {
		return nil
	}
	fields := make([]string, 0, len(elems))
	for _, e := range elems {
		fields = append(fields, e.Key())
	}
	return fields
}

// sameFields reports whether two field lists are the same in the same order.
// Order matters: {a:1, b:1} and {b:1, a:1} are different indexes to mongod and
// produce different key documents here.
//
// An unknown list, which is what keyFieldsOf returns when it cannot read an id,
// counts as the same so that an unreadable row does not produce a warning about
// a constraint that is probably fine.
func sameFields(live, claimed []string) bool {
	if len(claimed) == 0 {
		return true
	}
	if len(live) != len(claimed) {
		return false
	}
	for i := range live {
		if live[i] != claimed[i] {
			return false
		}
	}
	return true
}

// warnAboutUniqueIndexesTheArchiveLacks names the unique constraints the fast
// tier declares and the archive cannot help enforce.
//
// The router already says this when the index is created, in newindex.go. That
// line is said once, to whoever was reading the log that minute, and never
// again: a router restarted a month later repeats nothing, and `cline
// preflight` reports the opposite direction only, a unique index the *fast
// tier* is missing. So an operator who built the index and moved on has no way
// to learn what it costs except by running `cline indexes` and knowing to.
//
// What it costs is on the write path, which is the part that was not said until
// it was measured. Uniqueness is enforced across both tiers, so a write has to
// establish that no live document already holds its key, and when the fast tier
// holds no such document the archive is asked. Without the index there, that is
// a collection scan of the tier built to be the big one: 38-40ms per write at
// 200,003 archived documents against 1-10ms with one, growing with the archive.
//
// Reached by the writes that leave a claim with no owner, which are ordinary
// ones: an upsert whose filter carries no _id, and an insert that lets mongod
// name the document.
func (r *Router) warnAboutUniqueIndexesTheArchiveLacks(ctx context.Context) {
	for _, ns := range r.tieredNamespaces(ctx) {
		idxs := r.uniqueIndexes(ctx, ns)
		if len(idxs) == 0 {
			continue
		}
		db, coll := splitNS(ns)
		have, err := archiveIndexFields(ctx, r.cold, db, coll)
		if err != nil {
			// The archive not answering is said elsewhere, loudly.
			r.log.Debug("could not list the archive's indexes to compare them", "ns", ns, "err", err)
			continue
		}
		var missing []string
		for _, idx := range idxs {
			if !servedByAnArchiveIndex(idx.fields, have) {
				missing = append(missing, idx.name)
			}
		}
		if len(missing) == 0 {
			continue
		}
		sort.Strings(missing)
		r.log.Warn("the archive has no index over the fields of a unique index the fast tier enforces",
			"ns", ns, "indexes", missing,
			"meaning", "a write whose key the fast tier does not hold asks the archive whether any "+
				"live document holds it, and with no index there that question is a collection "+
				"scan on the write path",
			"measured", "38-40ms per write at 200,000 archived documents, against 1-10ms with an "+
				"index over the same fields, and it grows with the archive",
			"action", "build the same index on the archive for a collection written by upserts or "+
				"by inserts that do not carry an _id, which are the writes that ask")
	}
}

// archiveIndexFields reads the field names of every index the archive has for a
// namespace, in key order.
//
// By field name rather than by the rendered key document, which is what the
// first version compared. A direction is stored by mongod in a type of its
// choosing and a key built here is whatever Go marshals, so comparing the two
// renderings compares spelling: an index the archive has would read as missing
// and an operator would be told to build what they already have. The question
// this asks is only which fields are indexed, and a direction does not change
// which values an index treats as equal.
func archiveIndexFields(ctx context.Context, cold *mongo.Client, db, coll string) ([][]string, error) {
	cur, err := cold.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		var ce mongo.CommandError
		if errors.As(err, &ce) && ce.Code == namespaceNotFound {
			// A collection the archive has never heard of has no indexes, which
			// is the answer rather than a failure to get one.
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var out [][]string
	for cur.Next(ctx) {
		key, ok := cur.Current.Lookup("key").DocumentOK()
		if !ok {
			continue
		}
		elems, eerr := key.Elements()
		if eerr != nil {
			continue
		}
		fields := make([]string, 0, len(elems))
		for _, e := range elems {
			fields = append(fields, e.Key())
		}
		out = append(out, fields)
	}
	return out, cur.Err()
}

// servedByAnArchiveIndex reports whether any of the archive's indexes begins
// with exactly these fields, in this order.
//
// A prefix is the honest test: the question asked of the archive is an equality
// on every field of the unique index, and an index whose leading fields are
// those can seek straight to it. Anything looser would stay quiet about an
// index that cannot serve the question, which is the failure worth avoiding in
// the other direction: telling an operator to build what they have is worse
// than saying nothing, so this errs towards silence only where silence is true.
func servedByAnArchiveIndex(fields []string, have [][]string) bool {
	if len(fields) == 0 {
		return true
	}
	for _, idx := range have {
		if len(idx) < len(fields) {
			continue
		}
		same := true
		for i, f := range fields {
			if idx[i] != f {
				same = false
				break
			}
		}
		if same {
			return true
		}
	}
	return false
}
