package router

import (
	"context"
	"math"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// A count with no predicate is a metadata read on mongod, and it has to stay
// one here.
//
// Drivers send estimatedDocumentCount as {count: coll} because mongod answers it
// from the collection's metadata at once, whatever its size, and administrative
// tools and dashboards call it for that reason. The archive path added the
// filter that leaves out deleted copies, which no index serves, so the router
// turned an instant read into a scan of the whole archive collection. Measured
// on a staging archive of 60 million orders on network volumes, cold start: the
// count was still running when it was killed at 661 seconds.
//
// It is answered from meta.LiveCountEstimate instead: the archive's metadata
// count less its deleted copies, counted on the feeder's partial index. Exact
// in the same sense mongod's own answer is, and as current as the freshness
// barrier makes any archive read.

// unpredicatedCount reports whether a count command asks for the whole
// collection in the form mongod answers from metadata: no query or an empty
// one, and no skip, limit or hint to apply on top.
func unpredicatedCount(cmd bson.Raw) bool {
	if v, err := cmd.LookupErr("query"); err == nil {
		doc, ok := v.DocumentOK()
		if !ok {
			return false
		}
		if elems, eerr := doc.Elements(); eerr != nil || len(elems) > 0 {
			return false
		}
	}
	for _, k := range []string{"skip", "limit", "hint"} {
		if v, err := cmd.LookupErr(k); err == nil {
			if n, ok := v.AsInt64OK(); ok && n == 0 && k != "hint" {
				continue
			}
			return false
		}
	}
	return true
}

// approxSaid remembers the namespaces already warned about, so the warning
// that a count includes deleted copies is said once per router per namespace.
var approxSaid sync.Map

// answerEstimatedCount answers an unpredicated count from the archive's
// metadata. An error is returned as answerSimpleFromArchive's are.
func (c *conn) answerEstimatedCount(ctx context.Context, msg *wire.Message, db, coll string, cmd bson.Raw) (*wire.Message, error) {
	ns := db + "." + coll
	if err := c.archiveReadIsFresh(ctx, ns, time.Now(), maxTimeBudget(cmd)); err != nil {
		return nil, err
	}
	n, exact, err := meta.LiveCountEstimate(ctx, c.r.cold, db, coll)
	if err != nil {
		return nil, err
	}
	if !exact {
		if _, said := approxSaid.LoadOrStore(ns, true); !said {
			c.log.Warn("a count with no predicate includes deleted documents until the archive's index of them is built",
				"ns", ns, "index", meta.TombstoneIndex,
				"meaning", "the feeder builds it in the background at start; until it is there the "+
					"count is the archive's metadata, which still holds the deleted copies",
				"note", "said once per namespace")
		}
	}
	var nv any = n
	if n <= math.MaxInt32 {
		nv = int32(n)
	}
	return wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID,
		bson.D{{Key: "n", Value: nv}, {Key: "ok", Value: 1.0}})
}
