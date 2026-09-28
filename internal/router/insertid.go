package router

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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
	at := make(map[string][]int, len(docs))
	for i, doc := range docs {
		val, err := doc.LookupErr("_id")
		if err != nil {
			// No _id, so the server generates one and it cannot collide.
			continue
		}
		ids = append(ids, val)
		// Keyed by the value as mongod's _id index compares it, which is by
		// number rather than by type: NumberLong(8) and 8.0 are one _id there.
		// This used to be the raw bytes, on purpose, so that two spellings of
		// the same number were not treated as one. They are one to mongod, and
		// the archive's $in below finds them by value, so the answer came back
		// under bytes no entry here had and was dropped: an insert of 8.0,
		// NumberDecimal("9") or NumberDecimal("0.5") was accepted beside an
		// archived NumberLong(8), 9.0 or 0.5, two documents with one _id.
		// Measured on 2026-09-27. An ObjectId or a string is its bytes either way.
		k := idKey(val)
		// Every position, not the first: an unordered insert that names one
		// _id twice has both refused by mongod when the archive holds it, and
		// flagging only the first let the second through to a fast tier that
		// does not hold it. Measured on 2026-09-28 with _id 5 archived:
		// [{_id:5},{_id:5},{_id:6}] unordered inserted two, one a duplicate
		// of the archived document, where plain mongod inserts one. Keying by
		// value made it reach [{_id:7},{_id:7.0}] too.
		at[k] = append(at[k], i)
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
	placed := map[int]bool{}
	unplaced := 0
	for cur.Next(ctx) {
		val, lerr := cur.Current.LookupErr("_id")
		if lerr != nil {
			continue
		}
		positions, ok := at[idKey(val)]
		if !ok {
			unplaced++
			continue
		}
		// Copied, because val points into Current and is sent to the fast tier
		// after the loop, when Current is valid no longer.
		clashing = append(clashing, bson.RawValue{Type: val.Type, Value: append([]byte(nil), val.Value...)})
		for _, i := range positions {
			out = append(out, keyClash{doc: i, index: "_id_"})
			placed[i] = true
		}
	}
	if cerr := cur.Err(); cerr != nil {
		return nil, cerr
	}

	// An archived _id the $in found that is none of the batch's by value is one
	// the collection's collation calls equal: a collection created with
	// {locale: "en", strength: 2} makes "A" and "a" one _id, and the archive's
	// query ran under that collation while the map above compares bytes.
	// Measured on 2026-09-27: "a" beside an archived "A" and {k: "x"} beside
	// {k: "X"} were both accepted, two documents with one _id. There is no
	// collator here to place the answer, so each batch _id not yet placed is
	// asked about on its own, under the collection's collation, which is the
	// comparison mongod will make. Only when the $in found something it could
	// not place, which an ordinary insert never does.
	if unplaced > 0 {
		if extra, err := r.collatedIDClashes(ctx, ns, docs, at, placed); err != nil {
			return nil, err
		} else {
			for _, cl := range extra {
				id, _ := docs[cl.doc].LookupErr("_id")
				clashing = append(clashing, id)
				out = append(out, cl)
			}
		}
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
	// What it used to open: between this reading and mongod applying the
	// insert, an eviction sweep could remove that exact document from the fast
	// tier, mongod would then accept the insert, and the feeder would mirror it
	// over the archived copy the eviction had just confirmed. That was accepted
	// as a millisecond's window beside the cost of refusing every retry a
	// driver makes of a write that succeeded. It is closed now without that
	// cost, by holding the fast tier's copy against eviction before handing
	// the insert back: see holdAgainstEviction.
	if len(clashing) > 0 {
		held, herr := r.fastTierAlreadyHas(ctx, ns, clashing, at)
		if herr != nil {
			return nil, herr
		}
		// Handed back only once the fast tier's copies are held against
		// eviction, and not at all for one an eviction is already under way
		// for. Without the hold an eviction of that exact document between
		// this read and mongod applying the insert let the insert land, and
		// the feeder mirrored it over the archived copy. That window was
		// accepted here as a millisecond; the collated form of it was met in a
		// thirty-minute two-application run on 2026-09-28, so it is closed
		// instead. See holdAgainstEviction.
		if len(held) > 0 {
			evicting, eerr := r.holdAgainstEviction(ctx, ns, held)
			if eerr != nil {
				return nil, eerr
			}
			for i := range evicting {
				delete(held, i)
			}
		}
		if len(held) > 0 {
			kept := out[:0]
			for _, cl := range out {
				if _, h := held[cl.doc]; !h {
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
func (r *Router) fastTierAlreadyHas(ctx context.Context, ns string, ids bson.A, at map[string][]int) (map[int]bson.RawValue, error) {
	db, coll := splitNS(ns)
	cur, err := r.hot.Database(db).Collection(coll).Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	held := map[int]bson.RawValue{}
	for cur.Next(ctx) {
		val, lerr := cur.Current.LookupErr("_id")
		if lerr != nil {
			continue
		}
		// The fast tier's own spelling of the _id, copied out of Current: the
		// holds and announcements below are keyed by the stored bytes.
		stored := bson.RawValue{Type: val.Type, Value: append([]byte(nil), val.Value...)}
		for _, i := range at[idKey(val)] {
			held[i] = stored
		}
	}
	return held, cur.Err()
}

// holdAgainstEviction places a residency hold on the fast tier documents an
// insert is about to be handed to mongod for, and then reports which of them
// an eviction has already been announced for. It is the router's half of the
// handshake in evictor.dropHeldAfterAnnouncing: this writes the hold and reads
// the announcement, the evictor writes the announcement and reads the hold, so
// whichever runs second sees the first.
//
// An error is returned rather than a guess, and the caller refuses: letting
// mongod answer without the hold is the window this closes.
func (r *Router) holdAgainstEviction(ctx context.Context, ns string, held map[int]bson.RawValue) (map[int]bool, error) {
	stored := make([]bson.RawValue, 0, len(held))
	byDoc := map[string][]int{}
	for i, id := range held {
		d := meta.DocID(ns, id)
		if _, seen := byDoc[d]; !seen {
			stored = append(stored, id)
		}
		byDoc[d] = append(byDoc[d], i)
	}
	if err := r.recordResidencyMany(ctx, ns, stored); err != nil {
		return nil, fmt.Errorf("hold the fast tier's copies against eviction: %w", err)
	}
	docIDs := make([]string, 0, len(byDoc))
	for d := range byDoc {
		docIDs = append(docIDs, d)
	}
	cur, err := r.cold.Database(meta.DB).Collection(meta.Evicting).Find(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}},
	})
	if err != nil {
		return nil, fmt.Errorf("read eviction announcements: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	evicting := map[int]bool{}
	for cur.Next(ctx) {
		var n meta.EvictionNotice
		if cur.Decode(&n) != nil || n.Left() <= 0 {
			continue
		}
		for _, i := range byDoc[n.ID] {
			evicting[i] = true
		}
	}
	return evicting, cur.Err()
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

// idKey is an _id as mongod's _id index compares it, as a map key: one form
// per number whatever its type, and the bytes of anything else. See
// canonicalKeyValue.
func idKey(v bson.RawValue) string {
	c := canonicalKeyValue(v)
	if rv, ok := c.(bson.RawValue); ok {
		return string(rv.Type) + string(rv.Value)
	}
	t, b, err := bson.MarshalValue(c)
	if err != nil {
		return string(v.Type) + string(v.Value)
	}
	return string(t) + string(b)
}

// collatedIDClashes asks the archive, one _id at a time and under the
// collection's default collation, which of the batch positions not yet placed
// hold an _id a live archived document already has. See archivedIDClashesNow.
func (r *Router) collatedIDClashes(ctx context.Context, ns string, docs []bson.Raw, at map[string][]int, placed map[int]bool) ([]keyClash, error) {
	collation, ok := r.defaultCollation(ctx, ns)
	if !ok || len(simplifyCollation(collation)) == 0 {
		return nil, nil
	}
	var co options.Collation
	if err := bson.Unmarshal(collation, &co); err != nil {
		return nil, nil
	}
	db, coll := splitNS(ns)
	c := r.cold.Database(db).Collection(coll)
	var out []keyClash
	for _, positions := range at {
		i := positions[0]
		if placed[i] {
			continue
		}
		id, err := docs[i].LookupErr("_id")
		if err != nil {
			continue
		}
		ferr := c.FindOne(ctx, bson.D{{Key: "_id", Value: id}, meta.NotArchivedDeleted()},
			options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}}).SetCollation(&co)).Err()
		if ferr == nil {
			for _, j := range positions {
				out = append(out, keyClash{doc: j, index: "_id_"})
			}
			continue
		}
		if !errors.Is(ferr, mongo.ErrNoDocuments) {
			return nil, ferr
		}
	}
	return out, nil
}
