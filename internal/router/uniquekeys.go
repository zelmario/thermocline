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
	case "createIndexes", "dropIndexes", "drop", "collMod":
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
	vals := make([]any, len(idx.fields))
	for i, f := range idx.fields {
		val, err := doc.LookupErr(strings.Split(f, ".")...)
		if err != nil {
			continue // absent, which is a null key
		}
		vals[i] = val
	}
	return keyIDFromValues(ns, idx, vals)
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
		key = append(key, bson.E{Key: f, Value: vals[i]})
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

// keyClash is one document in a batch whose unique key is already taken.
type keyClash struct {
	doc   int    // position in the client's batch
	index string // the unique index that rejected it
}

// claimKeys reserves every unique key the batch needs.
//
// Rejection is per document, not per batch, because that is what mongod does. An
// unordered insertMany with one duplicate still inserts the rest and reports one
// error, and an application relying on that would silently lose documents if the
// whole batch were refused. Ordered stops at the first clash, again like mongod,
// so nothing after it is claimed either.
func (r *Router) claimKeys(ctx context.Context, ns string, docs []bson.Raw, idxs []uniqueIndex, ordered bool) (claimed []string, clashes []keyClash, err error) {
	dbName, _ := splitNS(ns)
	for d, doc := range docs {
		id, _ := doc.LookupErr("_id")
		clashed := false
		for i := range idxs {
			if !r.constrains(ctx, dbName, idxs[i], doc) {
				continue
			}
			kid, ok := keyID(ns, idxs[i], doc)
			if !ok {
				continue
			}
			got, terr := r.tryClaim(ctx, ns, idxs[i], kid, id)
			if terr != nil {
				return claimed, nil, terr
			}
			if got {
				claimed = append(claimed, kid)
				continue
			}
			clashes = append(clashes, keyClash{doc: d, index: idxs[i].name})
			clashed = true
			break
		}
		if clashed && ordered {
			// mongod stops an ordered batch at the first failure, so nothing
			// after this point is inserted and nothing after it may be claimed.
			break
		}
	}
	return claimed, clashes, nil
}

// tryClaim reserves one key for one document, resolving a stale claim if that
// is what stands in the way. It reports whether the key is now this document's.
//
// Both the insert and the update path go through here, so a value is reserved
// the same way whichever command is about to write it.
func (r *Router) tryClaim(ctx context.Context, ns string, idx uniqueIndex, kid string, owner bson.RawValue) (bool, error) {
	now := time.Now().UTC()
	_, err := r.keys().InsertOne(ctx, keyClaim{
		ID: kid, NS: ns, Index: idx.name, Doc: owner, At: now,
	})
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
		_, ierr := r.keys().InsertOne(ctx, keyClaim{
			ID: kid, NS: ns, Index: idx.name, Doc: owner, At: now,
		})
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

	live, lerr := r.claimIsLive(ctx, ns, idx, kid, claim)
	if lerr != nil {
		return false, lerr
	}
	if live {
		return false, nil
	}

	// Stale. Take it over, unless somebody else just did.
	res, uerr := r.keys().UpdateOne(ctx,
		bson.D{{Key: "_id", Value: kid}, {Key: "doc", Value: bson.D{{Key: "$ne", Value: owner}}}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "doc", Value: owner}, {Key: "at", Value: now}}}})
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
func (r *Router) claimIsLive(ctx context.Context, ns string, idx uniqueIndex, kid string, claim keyClaim) (bool, error) {
	if claim.Doc.IsZero() {
		// No owner recorded, so nothing can be proven gone. Treat as taken.
		return true, nil
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
	idClashes, ierr := c.r.archivedIDClashes(ctx, ns, docs)
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

	idxs := c.r.uniqueIndexes(ctx, ns)
	var claimed []string
	var clashes []keyClash
	if len(idxs) > 0 {
		var err error
		claimed, clashes, err = c.r.claimKeys(ctx, ns, docs, idxs, ordered)
		if err != nil {
			// Never block a write because the registry is unhappy. Losing the
			// cross-tier half of a unique index is bad; refusing every insert
			// because a bookkeeping collection is unreachable is worse.
			c.r.releaseKeys(ctx, claimed)
			c.log.Warn("could not check unique keys, letting the insert through", "ns", ns, "err", err)
			return nil, false
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
