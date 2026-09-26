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
	out, err := r.archivedIDClashesNow(ctx, ns, docs)
	if err != nil || len(out) == 0 {
		return out, err
	}
	// About to refuse. Asked again once mirroring has caught up, because an _id
	// deleted a moment ago is still live in the archive. See clashIsFresh.
	r.clashIsFresh(ctx, ns)
	return r.archivedIDClashesNow(ctx, ns, docs)
}

// archivedIDClashesNow is archivedIDClashes asked of the archive as it stands.
func (r *Router) archivedIDClashesNow(ctx context.Context, ns string, docs []bson.Raw) ([]keyClash, error) {
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

	clashing := make(bson.A, 0, len(ids))
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
		// Copied, because val points into Current and is sent to the fast tier
		// after the loop, when Current is valid no longer.
		clashing = append(clashing, bson.RawValue{Type: val.Type, Value: append([]byte(nil), val.Value...)})
		out = append(out, keyClash{doc: i, index: "_id_"})
	}
	if cerr := cur.Err(); cerr != nil {
		return nil, cerr
	}

	// An _id the fast tier holds too is not this guard's business, and
	// answering for it is how a retried insert was turned into an error.
	//
	// A driver retries a write after a network error by sending it again with
	// the same session and transaction number, and mongod deduplicates it: the
	// second attempt returns the recorded result and applies nothing. That only
	// works if mongod sees the command. This guard answered E11000 itself, from
	// the archive's copy of the document the first attempt had just written, so
	// the retry came back DuplicateKey where a plain MongoDB answers ok. An
	// application that reads E11000 as "somebody else has this key" then takes
	// a branch it should not, over a write of its own that succeeded.
	//
	// Measured against the previous binary, two attempts a second apart with one
	// session and transaction number: n=1 then E11000, where the same pair with
	// this gives n=1 twice and the collection holds one document.
	//
	// What a suite can hold still is the other half of it, because whether the
	// second attempt is deduplicated depends on the client driver reusing its
	// session and mongosh does not do that reliably. The observable difference
	// is who counted the refusal: thermocline_duplicate_keys_total moves when
	// this router refuses and stays put when mongod does. See testsingle.sh.
	//
	// Handing those back to mongod is right whatever the second attempt is. A
	// retry is deduplicated and answered with the recorded result. A genuine
	// second insert of an _id the fast tier holds is refused by the fast tier's
	// own index, with mongod's own error. And an _id only the archive holds is
	// still this guard's to refuse, because that is the one mongod cannot see.
	//
	// What it opens, stated rather than left for somebody to find: between this
	// reading and mongod applying the insert, one round trip, an eviction sweep
	// could remove that exact document from the fast tier. mongod would then
	// accept the insert, and the feeder would mirror it over the archived copy
	// the eviction had just confirmed. That is the loss this guard exists to
	// stop, in a window of a millisecond, needing a concurrent eviction of the
	// one _id being inserted. The alternative is refusing every retry a driver
	// makes of a write that already succeeded, which needs no coincidence at
	// all and happens whenever a connection drops.
	if len(clashing) > 0 {
		held, herr := r.fastTierAlreadyHas(ctx, ns, clashing, at)
		if herr != nil {
			return nil, herr
		}
		if len(held) > 0 {
			kept := out[:0]
			for _, cl := range out {
				if !held[cl.doc] {
					kept = append(kept, cl)
				}
			}
			r.log.Debug("an _id the archive holds is on the fast tier too, so mongod answers for it",
				"ns", ns, "handed_back", len(out)-len(kept), "still_refused", len(kept))
			out = kept
		}
	}

	sort.Slice(out, func(a, b int) bool { return out[a].doc < out[b].doc })
	return out, nil
}

// fastTierAlreadyHas reports which of the batch's positions carry an _id the
// fast tier already has, so mongod can answer for them.
//
// Named apart from fastTierHolds next door, which answers the same question for
// one document and is only ever used to skip work: this one decides whether a
// client is told its insert failed, so an error here has to stop the insert
// rather than be read as "not held".
func (r *Router) fastTierAlreadyHas(ctx context.Context, ns string, ids bson.A, at map[string]int) (map[int]bool, error) {
	db, coll := splitNS(ns)
	cur, err := r.hot.Database(db).Collection(coll).Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	held := map[int]bool{}
	for cur.Next(ctx) {
		val, lerr := cur.Current.LookupErr("_id")
		if lerr != nil {
			continue
		}
		if i, ok := at[string(val.Type)+string(val.Value)]; ok {
			held[i] = true
		}
	}
	return held, cur.Err()
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
