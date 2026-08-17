package router

import (
	"context"
	"sort"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// An insert carrying an _id that lives only in the archive was not checked at
// all, and it is the plainest way there is to destroy an archived record.
//
// The unique key registry deliberately skips _id_, and correctly: the registry
// exists because a document can move off a unique value and leave a claim behind
// pointing at itself, so every claim has to be re-read against its owner before
// it can be trusted. _id never moves, so none of that machinery is needed. But
// skipping the registry was taken to mean skipping the check, and nothing else
// looked.
//
// So on a tiered collection an insert of an _id the fast tier no longer holds
// succeeds. Two things then go wrong at once. The application is not told about
// the collision, where one cluster would have answered E11000, and idempotent
// writers rely on that error. And the feeder mirrors the new document up as an
// upsert, so the archived record is overwritten by it.
//
// The remedy is the one mongod would have applied: refuse those documents with a
// duplicate key error, per document rather than per batch, and let the rest of
// the batch through exactly as an ordered or unordered insert requires. That
// path already exists for archived unique values; this only gives it _id to work
// with.

// archivedIDClashes reports which documents in an insert batch carry an _id the
// archive already holds.
//
// One query for the whole batch, and none at all for a collection the fast tier
// claims in full: there, nothing is archived that is not also here, so the fast
// tier's own _id index is the whole answer and would have raised the error
// itself.
func (r *Router) archivedIDClashes(ctx context.Context, ns string, docs []bson.Raw) ([]keyClash, error) {
	if mark, known := r.completeness(ctx, ns); known && mark.Claimed && mark.CoversAll() {
		return nil, nil
	}

	ids := make(bson.A, 0, len(docs))
	at := make(map[string]int, len(docs))
	for i, doc := range docs {
		val, err := doc.LookupErr("_id")
		if err != nil {
			// No _id, so the server generates one and it cannot collide.
			continue
		}
		ids = append(ids, val)
		// Keyed by the raw bytes of the value, so two spellings of the same
		// number are not treated as one and an ObjectId is compared as itself.
		k := string(val.Type) + string(val.Value)
		if _, seen := at[k]; !seen {
			at[k] = i
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}

	db, coll := splitNS(ns)
	cur, err := r.cold.Database(db).Collection(coll).Find(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
		meta.NotArchivedDeleted(),
	}, options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var out []keyClash
	for cur.Next(ctx) {
		val, lerr := cur.Current.LookupErr("_id")
		if lerr != nil {
			continue
		}
		i, ok := at[string(val.Type)+string(val.Value)]
		if !ok {
			continue
		}
		out = append(out, keyClash{doc: i, index: "_id_"})
	}
	if cerr := cur.Err(); cerr != nil {
		return nil, cerr
	}
	sort.Slice(out, func(a, b int) bool { return out[a].doc < out[b].doc })
	return out, nil
}

// mergeClashes joins two lists of rejections, keeping one reason per document
// and ordering them by position the way mongod reports them.
//
// first wins where both reject the same document, because mongod reports the
// first index that refused it and _id is checked first.
func mergeClashes(first, second []keyClash) []keyClash {
	if len(first) == 0 {
		return second
	}
	if len(second) == 0 {
		return first
	}
	seen := make(map[int]bool, len(first)+len(second))
	out := make([]keyClash, 0, len(first)+len(second))
	for _, cl := range append(append([]keyClash{}, first...), second...) {
		if seen[cl.doc] {
			continue
		}
		seen[cl.doc] = true
		out = append(out, cl)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].doc < out[b].doc })
	return out
}
