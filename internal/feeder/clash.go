package feeder

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/zelmario/thermocline/internal/meta"
)

// A duplicate key the fast tier never held.
//
// An update reaches the feeder with the document as it is when the event is
// read, not as it was when the event happened (fullDocument: updateLookup).
// While the feeder is behind, every image it reads is the latest one, and
// replayed in event order they ask the archive for states the fast tier never
// had. Two documents swapping a unique value through a free one, a to -1, b to
// a's old value, a to b's old value: the first event of a arrives with a's last
// image, b's old value, while the archive still gives b that value. The archive
// refused it as a duplicate, the document was recorded as unmirrored, and every
// read of the namespace that fell through to the archive was refused until the
// application happened to write that document again. Measured with
// test/feedmix.sh: 3,580 of 20,000 documents after 2,000 swaps replayed from a
// backlog, on every build before this.
//
// The images together are the fast tier's present, which its own unique index
// holds to, so they fit once the archive's stale copies are out of their way.
// Order alone does not do it: two documents trading values are a cycle no
// order of two replaces gets through, which the fast tier got through with an
// intermediate value the images no longer carry. Moving a clashing write to the
// end of the flush was tried first and measured: 24,302 refusals against
// 25,494, and the same 3,600 documents left over.

// clearFirst rewrites one namespace's part of a flush to delete the archive's
// copy of every document it upserts before anything else, then write the
// operations as they were. The flush is one transaction, so no reader sees a
// document missing; a document's own operations keep their order; a document
// the flush only marks deleted is left alone, because its copy is what the mark
// keeps.
func clearFirst(models []mongo.WriteModel, ops []pendingOp) ([]mongo.WriteModel, []pendingOp) {
	seen := map[string]bool{}
	var cm []mongo.WriteModel
	var co []pendingOp
	for _, op := range ops {
		if op.kind != actionUpsert || op.clearFirst {
			continue
		}
		k := docKey(op)
		if seen[k] {
			continue
		}
		seen[k] = true
		cm = append(cm, mongo.NewDeleteOneModel().SetFilter(op.key))
		co = append(co, pendingOp{ns: op.ns, key: op.key, id: op.id, kind: actionSkip, clearFirst: true})
	}
	return append(cm, models...), append(co, ops...)
}

// dropDoc takes a refused document out of a cleared flush: its delete and its
// upserts, so the archive keeps the copy it had rather than losing it. A
// tombstone of it stays, since it marks that same copy.
func dropDoc(models []mongo.WriteModel, ops []pendingOp, k string) ([]mongo.WriteModel, []pendingOp) {
	var m []mongo.WriteModel
	var o []pendingOp
	for i, op := range ops {
		if docKey(op) == k && (op.clearFirst || op.kind == actionUpsert) {
			continue
		}
		m, o = append(m, models[i]), append(o, op)
	}
	return m, o
}

// docKey names one document of one namespace by its _id's bytes.
func docKey(op pendingOp) string {
	return op.ns + "\x00" + string([]byte{byte(op.id.Type)}) + string(op.id.Value)
}

// unmirroredRetry is how often a namespace's refused documents are offered to
// the archive again. A clash with a value the fast tier gives up in a later
// flush resolves on the next try; one with a document only the archive holds
// never does, and asks once a minute.
const unmirroredRetry = time.Minute

// unmirroredRetryMax bounds one try, so a namespace with many refused
// documents is worked through over several flushes rather than in one.
const unmirroredRetryMax = 500

// dueRetries reads, for each namespace with refused documents and a try due,
// their present images from the fast tier, as upserts for the end of this
// flush. A document already in the flush is left to it. A document the fast
// tier no longer holds has nothing to mirror, and its record goes: the fast
// tier's delete reaches the archive as a tombstone of its own.
func (f *Feeder) dueRetries(ctx context.Context) []pendingOp {
	if len(f.unmirroredNS) == 0 {
		return nil
	}
	now := time.Now()
	inFlush := make(map[string]bool, len(f.pending))
	for _, op := range f.pending {
		inFlush[docKey(op)] = true
	}
	var out []pendingOp
	for ns := range f.unmirroredNS {
		if !f.mirrorsNS(ns) || now.Sub(f.retriedAt[ns]) < unmirroredRetry {
			continue
		}
		if f.retriedAt == nil {
			f.retriedAt = map[string]time.Time{}
		}
		f.retriedAt[ns] = now
		recs, err := meta.UnmirroredIn(ctx, f.cold, ns)
		if err != nil || len(recs) == 0 {
			continue
		}
		ids := bson.A{}
		for _, r := range recs {
			if len(ids) == unmirroredRetryMax {
				break
			}
			if !inFlush[docKey(pendingOp{ns: ns, id: r.Doc})] {
				ids = append(ids, r.Doc)
			}
		}
		if len(ids) == 0 {
			continue
		}
		db, coll := splitNS(ns)
		// From the primary: a lagging secondary's image can be older than events
		// already mirrored, and writing it would take the archive back.
		cur, err := f.hot.Database(db, options.Database().SetReadPreference(readpref.Primary())).
			Collection(coll).Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
		if err != nil {
			continue
		}
		found := map[string]bool{}
		for cur.Next(ctx) {
			doc := append(bson.Raw(nil), cur.Current...)
			id := doc.Lookup("_id")
			key, err := bson.Marshal(bson.D{{Key: "_id", Value: id}})
			if err != nil {
				continue
			}
			found[docKey(pendingOp{ns: ns, id: id})] = true
			out = append(out, pendingOp{ns: ns, key: key, id: id, doc: doc, kind: actionUpsert, retry: true})
		}
		_ = cur.Close(ctx)
		if cur.Err() != nil {
			continue
		}
		var gone []bson.RawValue
		for _, v := range ids {
			id := v.(bson.RawValue)
			if !found[docKey(pendingOp{ns: ns, id: id})] {
				gone = append(gone, id)
			}
		}
		if len(gone) > 0 {
			if left, err := meta.ClearUnmirroredMany(ctx, f.cold, ns, gone); err == nil && left == 0 {
				f.forgetUnmirrored(ns)
			}
		}
	}
	return out
}

// clashVictim says which document a clash left after clearFirst should be
// refused: i, the one the archive refused, unless i held this value before the
// flush. Then the other is the newcomer, a document earlier in the flush that
// took the value once the stale copies were out of the way, and the archive
// keeps the document it had, as it did before clearFirst existed.
//
// Measured, on a unique index the archive alone has, with two documents on the
// fast tier holding one value: the flush wrote the second before the first, the
// first was refused, and its record stood until the next retry although the
// archive had held it all along.
//
// Asked of the archive outside the flush's transaction, so of the state before
// it. Anything it cannot tell, a write error without keyValue, an equality a
// collation reads otherwise, falls back to i.
func (f *Feeder) clashVictim(ctx context.Context, ns string, ops []pendingOp, i int, err error) int {
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) {
		return i
	}
	var kv bson.Raw
	for _, we := range bwe.WriteErrors {
		if we.Index == i {
			kv, _ = we.Raw.Lookup("keyValue").DocumentOK()
		}
	}
	elems, eerr := kv.Elements()
	if eerr != nil || len(elems) == 0 {
		return i
	}
	db, coll := splitNS(ns)
	var holder struct {
		ID bson.RawValue `bson:"_id"`
	}
	if ferr := f.cold.Database(db, options.Database().SetReadPreference(readpref.Primary())).Collection(coll).
		FindOne(ctx, kv, options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).Decode(&holder); ferr != nil {
		return i
	}
	if docKey(pendingOp{ns: ns, id: holder.ID}) != docKey(ops[i]) {
		return i
	}
	for j := i - 1; j >= 0; j-- {
		op := ops[j]
		if op.kind != actionUpsert || op.clearFirst || docKey(op) == docKey(ops[i]) {
			continue
		}
		same := true
		for _, e := range elems {
			if !op.doc.Lookup(strings.Split(e.Key(), ".")...).Equal(e.Value()) {
				same = false
				break
			}
		}
		if same {
			return j
		}
	}
	return i
}
