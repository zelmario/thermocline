package router

import (
	"context"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// A stats call about a tiered collection counted the fast tier's share of it.
//
// $collStats and the collStats command are let through as metadata, on the
// stated grounds that they describe the collection rather than read its
// documents, so there is no set of documents for the answer to be short of.
// That is right about every field but one. The count is a number of documents,
// and it was short exactly as a find would have been.
//
// Measured on MongoDB's published sample datasets adopted as an archive, asking
// the router and the archive the same 199 questions across twenty-three
// collections: every answer agreed except this one, and it disagreed on all
// four tiered collections. sample_training.trips reported 0 where the archive
// holds 10,000, because that collection is dated 2016 and the hot window is ten
// years, so none of it is on the fast tier at all. An administrative tool shows
// an empty collection, and db.coll.stats() typed by hand after an adoption says
// the data is gone. countDocuments on the same collection answered 10,000
// throughout, so the two ways of asking disagreed by the whole collection.
//
// The count is corrected and the storage figures are not. Bytes on disk are a
// per-tier fact and adding two clusters' compressed sizes together would
// produce a number describing nothing; an operator sizing a fast tier wants
// what the fast tier reports. The count is the one field here that is about the
// collection rather than about a cluster.
//
// It comes from the archive, which is where countDocuments already gets it, so
// the two agree by construction rather than by coincidence.

// statsCountFrom is how long the correcting count may take. A stats call is an
// administrative one and the archive is the cluster being relieved, so this is
// bounded rather than left to run: an uncorrected count is worse than the fast
// tier's, but a stats call that hangs is worse than both.
const statsCountBudget = 10 * time.Second

// correctedCount is the collection's document count, or false when the fast
// tier's own answer already describes the whole collection.
//
// The fast tier's count stands when it claims to hold every document, which is
// what CoversAll means and what an adoption reaches once it has warmed
// everything or declared it. Nothing is corrected for an untiered collection
// either: that one lives on the fast tier alone and its count is exact.
func (c *conn) correctedCount(ctx context.Context, ns string) (int64, bool) {
	base, _, tiered := c.r.tieredBase(ctx, ns)
	if !tiered {
		return 0, false
	}
	if mark, known := c.r.completenessForRead(ctx, base); known && mark.CoversAll() {
		return 0, false
	}

	db, coll := splitNS(base)
	cctx, cancel := context.WithTimeout(ctx, statsCountBudget)
	defer cancel()
	n, err := c.r.cold.Database(db).Collection(coll).
		CountDocuments(cctx, bson.D{meta.NotArchivedDeleted()})
	if err != nil {
		c.log.Warn("could not count the archive for a stats call, so the count describes the fast tier only",
			"ns", ns, "err", err)
		return 0, false
	}
	return n, true
}

// interceptCollStats corrects the count in a collStats reply.
func (c *conn) interceptCollStats(ctx context.Context, msg *wire.Message, db string, reply *wire.Message) (*wire.Message, error) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return reply, nil
	}
	coll, ok := stringField(cmd, "collStats")
	if !ok {
		return reply, nil
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return reply, nil
	}
	n, ok := c.correctedCount(ctx, db+"."+coll)
	if !ok {
		return reply, nil
	}

	var doc bson.D
	if err := bson.Unmarshal(rdoc, &doc); err != nil {
		return reply, nil
	}
	out, changed := withCount(doc, n)
	if !changed {
		return reply, nil
	}
	return wire.NewMsg(reply.Header.RequestID, msg.Header.RequestID, out)
}

// interceptAggCollStats corrects the count in the reply to an aggregate whose
// pipeline begins with $collStats, which is what db.coll.stats() sends.
func (c *conn) interceptAggCollStats(ctx context.Context, msg *wire.Message, db string, reply *wire.Message) (*wire.Message, error) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return reply, nil
	}
	coll, ok := stringField(cmd, "aggregate")
	if !ok || !leadsWithCollStats(cmd) {
		return reply, nil
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return reply, nil
	}
	batch, ok := firstBatch(rdoc)
	if !ok || len(batch) == 0 {
		return reply, nil
	}
	n, ok := c.correctedCount(ctx, db+"."+coll)
	if !ok {
		return reply, nil
	}

	// $collStats produces exactly one document per shard, and the fast tier is
	// a replica set, so there is one. A later stage in the pipeline may have
	// reshaped it into something with no count in it at all, and withCount
	// leaves such a document alone rather than inventing a field for it.
	changedAny := false
	out := make(bson.A, 0, len(batch))
	for _, v := range batch {
		bdoc, dok := v.DocumentOK()
		if !dok {
			out = append(out, v)
			continue
		}
		var doc bson.D
		if err := bson.Unmarshal(bdoc, &doc); err != nil {
			out = append(out, v)
			continue
		}
		fixed, changed := withCount(doc, n)
		changedAny = changedAny || changed
		out = append(out, fixed)
	}
	if !changedAny {
		return reply, nil
	}

	id, ns := cursorIdentity(rdoc, db+"."+coll)
	return cursorReply(reply.Header.RequestID, msg.Header.RequestID, id, ns, "firstBatch", out)
}

// leadsWithCollStats reports whether a pipeline's first stage is $collStats.
func leadsWithCollStats(cmd bson.Raw) bool {
	v, err := cmd.LookupErr("pipeline")
	if err != nil {
		return false
	}
	vals, err := v.Array().Values()
	if err != nil || len(vals) == 0 {
		return false
	}
	stage, ok := vals[0].DocumentOK()
	if !ok {
		return false
	}
	elems, err := stage.Elements()
	if err != nil || len(elems) == 0 {
		return false
	}
	return elems[0].Key() == "$collStats"
}

// withCount replaces the document counts in a stats document and reports
// whether it found any.
//
// Two shapes carry one: $collStats{count:{}} puts it at the top level, and
// $collStats{storageStats:{}} and the collStats command put it inside
// storageStats or beside the storage figures. Only fields that are already
// there are replaced, so a pipeline that projected the count away keeps its
// shape and a reply that never had one is not given one.
func withCount(doc bson.D, n int64) (bson.D, bool) {
	changed := false
	out := make(bson.D, 0, len(doc))
	for _, e := range doc {
		switch {
		case e.Key == "count":
			out = append(out, bson.E{Key: "count", Value: sameNumberKind(e.Value, n)})
			changed = true
		case e.Key == "storageStats":
			inner, ok := asDoc(e.Value)
			if !ok {
				out = append(out, e)
				continue
			}
			fixed, innerChanged := withCount(inner, n)
			changed = changed || innerChanged
			out = append(out, bson.E{Key: "storageStats", Value: fixed})
		default:
			out = append(out, e)
		}
	}
	return out, changed
}

// sameNumberKind returns n in the BSON type the field already had.
//
// mongod answers collStats with an int32 count, and replacing it with an int64
// changes the type of a field in a reply the client did not ask to have
// changed. A driver decoding into an int32 rejects it, and mongosh prints
// Long('40') where the server prints 40, which is how this was caught: the
// check compared the printed answer with the archive's and the numbers were
// equal while the strings were not.
//
// A count that does not fit in an int32 is widened rather than truncated. That
// is a collection of more than two billion documents, where a wrong small
// number would be far worse than a wider type.
func sameNumberKind(old any, n int64) any {
	switch old.(type) {
	case int32:
		if n >= math.MinInt32 && n <= math.MaxInt32 {
			return int32(n)
		}
		return n
	case float64:
		return float64(n)
	default:
		return n
	}
}

// asDoc reads a nested document out of a decoded reply, whichever way the
// driver represented it.
func asDoc(v any) (bson.D, bool) {
	switch t := v.(type) {
	case bson.D:
		return t, true
	case bson.Raw:
		var d bson.D
		if err := bson.Unmarshal(t, &d); err != nil {
			return nil, false
		}
		return d, true
	default:
		return nil, false
	}
}

// cursorIdentity reads the cursor id and namespace back out of a reply, so a
// rebuilt one keeps what the fast tier said rather than assuming.
func cursorIdentity(rdoc bson.Raw, fallbackNS string) (int64, string) {
	id, ns := int64(0), fallbackNS
	cv, err := rdoc.LookupErr("cursor")
	if err != nil {
		return id, ns
	}
	cdoc, ok := cv.DocumentOK()
	if !ok {
		return id, ns
	}
	if v, lerr := cdoc.LookupErr("id"); lerr == nil {
		if n, iok := v.Int64OK(); iok {
			id = n
		}
	}
	if v, lerr := cdoc.LookupErr("ns"); lerr == nil {
		if s, sok := v.StringValueOK(); sok && s != "" {
			ns = s
		}
	}
	return id, ns
}
