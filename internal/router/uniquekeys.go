package router

import (
	"context"
	"encoding/base64"
	"errors"
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
}

// uniqueIndexes lists the unique indexes on ns, excluding _id.
//
// Sparse and partial ones are included. They constrain only a subset of
// documents, so each candidate document is tested against the same condition
// the index itself uses before its key is claimed. See constrains.
func (r *Router) uniqueIndexes(ctx context.Context, ns string) []uniqueIndex {
	r.idxCache.mu.RLock()
	at, seen := r.idxCache.fetched[ns]
	cached := r.idxCache.byNS[ns]
	r.idxCache.mu.RUnlock()
	if seen && time.Since(at) < indexRefresh {
		return cached
	}

	db, coll := splitNS(ns)
	found := []uniqueIndex{}
	cur, err := r.hot.Database(db).Collection(coll).Indexes().List(ctx)
	if err == nil {
		defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
		for cur.Next(ctx) {
			var spec struct {
				Name                    string   `bson:"name"`
				Unique                  bool     `bson:"unique"`
				Sparse                  bool     `bson:"sparse"`
				Key                     bson.Raw `bson:"key"`
				PartialFilterExpression bson.Raw `bson:"partialFilterExpression"`
				Collation               bson.Raw `bson:"collation"`
			}
			if derr := cur.Decode(&spec); derr != nil {
				continue
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

// keyID builds the registry id for one document's value of one index.
//
// A missing field is indexed as null by MongoDB, so it is encoded as null here
// too, or two documents both missing the field would look like different keys
// and both be allowed.
func keyID(ns string, idx uniqueIndex, doc bson.Raw) (string, bool) {
	return keyIDFromValues(ns, idx, keyValues(idx, doc))
}

// keyValues reads one document's value of each of an index's fields, in order.
// A nil entry means the field is absent, which MongoDB indexes as null.
func keyValues(idx uniqueIndex, doc bson.Raw) []any {
	vals := make([]any, len(idx.fields))
	for i, f := range idx.fields {
		val, err := doc.LookupErr(strings.Split(f, ".")...)
		if err != nil {
			continue // absent, which is a null key
		}
		vals[i] = val
	}
	return vals
}

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
			kid, ok := keyID(ns, idxs[i], doc)
			if !ok {
				continue
			}
			cands = append(cands, claimCandidate{
				doc: d, idx: i, kid: kid, owner: id, vals: keyValues(idxs[i], doc),
			})
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
	// An index with a collation calls strings equal that the id derived above
	// keeps apart, so the archive is asked in the comparison the index itself
	// uses. See collatedkeys.go. This costs one indexed lookup, and only on an
	// index that carries a collation.
	if r.collatedArchiveClash(ctx, ns, idx, vals, owner) {
		return false, nil
	}

	now := time.Now().UTC()
	_, err := r.keys().InsertOne(ctx, claimRow(kid, ns, idx.name, owner, now))
	if err == nil {
		return true, nil
	}
	if !mongo.IsDuplicateKeyError(err) {
		return false, err
	}

	// Somebody holds this key. Whether that is a real duplicate depends on
	// whether their document still exists and still carries this value.
	var claim keyClaim
	derr := r.keys().FindOne(ctx, bson.D{{Key: "_id", Value: kid}}).Decode(&claim)
	if errors.Is(derr, mongo.ErrNoDocuments) {
		// Released between the failed insert and this read. Try once more.
		_, ierr := r.keys().InsertOne(ctx, claimRow(kid, ns, idx.name, owner, now))
		if ierr == nil {
			return true, nil
		}
		if mongo.IsDuplicateKeyError(ierr) {
			return false, nil
		}
		return false, ierr
	}
	if derr != nil {
		return false, derr
	}
	// Already ours. An update that rewrites a field to the value it already has
	// must not be rejected by the claim the same document made.
	if !owner.IsZero() && claim.Doc.Equal(owner) {
		return true, nil
	}

	live, lerr := r.claimIsLive(ctx, ns, idx, kid, claim, vals, owner)
	if lerr != nil {
		return false, lerr
	}
	if live {
		return false, nil
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
		return false, uerr
	}
	return res.MatchedCount > 0, nil
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

	coldErr := r.cold.Database(db).Collection(coll).
		FindOne(ctx, bson.D{
			{Key: "_id", Value: claim.Doc},
			meta.NotArchivedDeleted(),
		}).Decode(&owner)
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
	got, ok := keyID(ns, idx, doc)
	if !ok {
		return true
	}
	return got == kid
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
	c.r.indexer.EnsureCollection(ctx, ns)

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
		db, coll := splitNS(ns)

		// The archive holds every document, so it is the authoritative walk. The
		// fast tier is walked too, for anything written since the last mirror.
		for _, src := range []struct {
			client *mongo.Client
			filter bson.D
		}{
			{r.cold, bson.D{meta.NotArchivedDeleted()}},
			{r.hot, bson.D{}},
		} {
			cur, err := src.client.Database(db).Collection(coll).Find(ctx, src.filter)
			if err != nil {
				return claimed, conflicts, err
			}
			for cur.Next(ctx) {
				doc := bson.Raw(cur.Current)
				id, _ := doc.LookupErr("_id")
				for i := range idxs {
					if !r.constrains(ctx, db, idxs[i], doc) {
						continue
					}
					kid, ok := keyID(ns, idxs[i], doc)
					if !ok {
						continue
					}
					_, ierr := r.keys().InsertOne(ctx, keyClaim{
						ID: kid, NS: ns, Index: idxs[i].name, Doc: id, At: time.Now().UTC(),
					})
					switch {
					case ierr == nil:
						claimed[ns]++
					case mongo.IsDuplicateKeyError(ierr):
						// Already claimed. Only a different owner is a conflict;
						// the same document seen in both tiers is expected.
						var existing keyClaim
						if derr := r.keys().FindOne(ctx, bson.D{{Key: "_id", Value: kid}}).Decode(&existing); derr == nil {
							if !existing.Doc.Equal(id) {
								conflicts[ns]++
							}
						}
					default:
						_ = cur.Close(context.WithoutCancel(ctx))
						return claimed, conflicts, ierr
					}
				}
			}
			cerr := cur.Err()
			_ = cur.Close(context.WithoutCancel(ctx))
			if cerr != nil {
				return claimed, conflicts, cerr
			}
		}
	}
	return claimed, conflicts, nil
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
			if kid, ok := keyID(ns, idx, doc); ok {
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
