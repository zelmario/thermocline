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

	"github.com/zelmar/thermocline/internal/meta"
	"github.com/zelmar/thermocline/internal/wire"
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
}

// indexCache remembers which namespaces have unique indexes.
type indexCache struct {
	mu      sync.RWMutex
	fetched map[string]time.Time
	byNS    map[string][]uniqueIndex
}

// uniqueIndexes lists the unique indexes on ns, excluding _id.
//
// Sparse and partial unique indexes are skipped deliberately. They constrain
// only a subset of documents, and a registry that cannot evaluate the same
// condition would reject inserts the database itself would allow. Leaving them
// out means they behave exactly as they do today, which is honest, and the
// router says so once at startup.
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
			if spec.Sparse || len(spec.PartialFilterExpression) > 0 {
				r.log.Warn("unique index not enforced across tiers",
					"ns", ns, "index", spec.Name,
					"reason", "sparse and partial unique indexes constrain only some documents",
					"effect", "a duplicate of an evicted document can still be inserted")
				continue
			}
			elems, kerr := spec.Key.Elements()
			if kerr != nil {
				continue
			}
			ui := uniqueIndex{name: spec.Name}
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

// keyID builds the registry id for one document's value of one index.
//
// A missing field is indexed as null by MongoDB, so it is encoded as null here
// too, or two documents both missing the field would look like different keys
// and both be allowed.
func keyID(ns string, idx uniqueIndex, doc bson.Raw) (string, bool) {
	key := make(bson.D, 0, len(idx.fields))
	for _, f := range idx.fields {
		val, err := doc.LookupErr(strings.Split(f, ".")...)
		if err != nil {
			key = append(key, bson.E{Key: f, Value: nil})
			continue
		}
		key = append(key, bson.E{Key: f, Value: val})
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
	now := time.Now().UTC()
	for d, doc := range docs {
		id, _ := doc.LookupErr("_id")
		clashed := false
		for i := range idxs {
			kid, ok := keyID(ns, idxs[i], doc)
			if !ok {
				continue
			}
			_, ierr := r.keys().InsertOne(ctx, keyClaim{
				ID: kid, NS: ns, Index: idxs[i].name, Doc: id, At: now,
			})
			if ierr == nil {
				claimed = append(claimed, kid)
				continue
			}
			if !mongo.IsDuplicateKeyError(ierr) {
				return claimed, nil, ierr
			}

			// Somebody holds this key. Whether that is a real duplicate depends
			// on whether their document still exists.
			taken, terr := r.claimIsLive(ctx, ns, kid)
			if terr != nil {
				return claimed, nil, terr
			}
			if !taken {
				// Stale. Take it over, unless somebody else just did.
				res, uerr := r.keys().UpdateOne(ctx,
					bson.D{{Key: "_id", Value: kid}, {Key: "doc", Value: bson.D{{Key: "$ne", Value: id}}}},
					bson.D{{Key: "$set", Value: bson.D{{Key: "doc", Value: id}, {Key: "at", Value: now}}}})
				if uerr != nil {
					return claimed, nil, uerr
				}
				if res.MatchedCount > 0 {
					claimed = append(claimed, kid)
					continue
				}
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

// claimIsLive reports whether the document holding a claim still exists in
// either tier. A claim whose owner is gone is a leftover from a delete and must
// not block a new insert.
func (r *Router) claimIsLive(ctx context.Context, ns, kid string) (bool, error) {
	var claim keyClaim
	err := r.keys().FindOne(ctx, bson.D{{Key: "_id", Value: kid}}).Decode(&claim)
	if errors.Is(err, mongo.ErrNoDocuments) {
		// Released between the failed insert and this read.
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if claim.Doc.IsZero() {
		// No owner recorded, so nothing can be proven gone. Treat as taken.
		return true, nil
	}

	db, coll := splitNS(ns)
	hotErr := r.hot.Database(db).Collection(coll).
		FindOne(ctx, bson.D{{Key: "_id", Value: claim.Doc}},
			options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).Err()
	if hotErr == nil {
		return true, nil
	}
	if !errors.Is(hotErr, mongo.ErrNoDocuments) {
		return true, hotErr
	}

	coldErr := r.cold.Database(db).Collection(coll).
		FindOne(ctx, bson.D{
			{Key: "_id", Value: claim.Doc},
			meta.NotArchivedDeleted(),
		}, options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).Err()
	if coldErr == nil {
		return true, nil
	}
	if !errors.Is(coldErr, mongo.ErrNoDocuments) {
		return true, coldErr
	}
	return false, nil
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

	idxs := c.r.uniqueIndexes(ctx, ns)
	if len(idxs) == 0 {
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

	claimed, clashes, err := c.r.claimKeys(ctx, ns, docs, idxs, ordered)
	if err != nil {
		// Never block a write because the registry is unhappy. Losing the
		// cross-tier half of a unique index is bad; refusing every insert
		// because a bookkeeping collection is unreachable is worse.
		c.r.releaseKeys(ctx, claimed)
		c.log.Warn("could not check unique keys, letting the insert through", "ns", ns, "err", err)
		return nil, false
	}
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
		c.r.releaseKeys(ctx, claimed)
		return nil, false
	}
	reply, rerr := wire.Read(c.upstream)
	if rerr != nil {
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
