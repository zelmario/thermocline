package router

import (
	"context"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// dbStats counted the fast tier's share of every tiered collection in the
// database, and said nothing about it.
//
// collStats has been corrected since the pilot, on the stated principle that a
// count is a logical fact about the collection while bytes on disk are a
// per-tier one. dbStats carries the same field one level up, under the name
// objects, and it was left as the fast tier's. So the two ways of asking the
// same question disagreed: measured on a database whose one tiered collection
// the archive holds seven documents of and the fast tier had just been created
// empty for, collStats answered 7, countDocuments answered 7, and
// db.stats().objects answered **0**. A database holding seven documents
// reported none.
//
// That is the adoption's ordinary state rather than a corner: the fast tier
// starts empty and fills, so for as long as an adoption is warming, every
// dbStats is short by everything not yet copied up. On the published sample
// datasets it is short by whole collections at a time, because a collection
// dated 2016 read through a ten year hot window has nothing on the fast tier at
// all.
//
// Only objects is corrected, for the same reason collStats corrects only count.
// collections and views are already right, because the router creates the fast
// tier's copy of every namespace it manages at startup: measured on a database
// the fast tier had never held, both listCollections and dbStats.collections
// answered 1. dataSize, storageSize, indexSize and totalSize are bytes on one
// cluster's disk, and adding two clusters' compressed sizes would produce a
// number describing nothing; avgObjSize is derived from those bytes and stays
// with them. An operator sizing a fast tier wants what the fast tier reports.

// dbStatsCountBudget bounds the whole correction, not one collection of it.
//
// The correction costs one archive count per tiered collection in the database
// that is not already claimed complete, and the size of the collection is what
// decides the price. Measured on this box: countDocuments with the tombstone
// filter over a one million document archive collection takes 135 to 173ms warm
// or cold, against 6ms for the metadata count the fast tier's side uses. So a
// database with five such collections costs the archive about three quarters of
// a second per call, and dbStats is a command monitoring agents poll every few
// seconds. On the test database, 92 collections of a few documents each, the
// whole call went from 2 to 8ms to 64 to 92ms: round trips rather than counting.
//
// That is the price of the number being right, and it is worth writing down
// rather than discovering. Warming a collection removes its share of it, because
// a claim that covers everything makes the fast tier's own count the right one
// and nothing is counted for that namespace at all.
//
// The budget is generous because the alternative is the wrong number, and it is
// shared because a correction that ran out half way through would report a
// total that is neither tier's and nobody's. Running out abandons the whole
// correction and says so.
const dbStatsCountBudget = 10 * time.Second

// interceptDBStats corrects the object count in a dbStats reply.
func (c *conn) interceptDBStats(ctx context.Context, msg *wire.Message, db string, reply *wire.Message) (*wire.Message, error) {
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return reply, nil
	}
	delta, ok := c.tieredObjectDelta(ctx, db)
	if !ok || delta == 0 {
		return reply, nil
	}

	var doc bson.D
	if err := bson.Unmarshal(rdoc, &doc); err != nil {
		return reply, nil
	}
	out, changed := withObjects(doc, delta)
	if !changed {
		return reply, nil
	}
	return wire.NewMsg(reply.Header.RequestID, msg.Header.RequestID, out)
}

// tieredObjectDelta is what has to be added to the fast tier's object count for
// it to describe the database, and whether every collection could be counted.
//
// A namespace whose fast tier copy is a view needs no special case, which was
// worth finding out rather than assuming. EstimatedDocumentCount sends `count`,
// and mongod answers that on a view, so the count comes back as the number of
// documents the view yields. Measured on a database holding an untiered
// collection of one document, a tiered name turned into a view over it, and a
// second tiered collection of ten the fast tier held none of: the fast tier's
// own dbStats said 2 and the router said 15, which is the truth. Skipping the
// view instead would have said 12.
//
// Per namespace it is the collection's logical count less what the fast tier
// holds of it. The logical count comes from correctedCount, which is the same
// function collStats uses, so the two commands agree by construction rather
// than by coincidence: a namespace it declines to correct is one whose fast
// tier copy is already the whole collection, and it contributes nothing here
// either. What the fast tier holds comes from its metadata count, which is
// exactly the number mongod summed into objects in the first place.
//
// False means leave the reply alone. A correction is all of the database's
// tiered collections or none of them.
func (c *conn) tieredObjectDelta(ctx context.Context, db string) (int64, bool) {
	prefix := db + "."
	var delta int64
	counted := false

	bctx, cancel := context.WithTimeout(ctx, dbStatsCountBudget)
	defer cancel()

	for _, ns := range c.r.cfg.Managed {
		if !strings.HasPrefix(ns, prefix) {
			continue
		}
		logical, corrected := c.correctedCount(bctx, ns)
		if !corrected {
			continue
		}
		_, coll := splitNS(ns)
		held, err := c.r.hot.Database(db).Collection(coll).EstimatedDocumentCount(bctx)
		if err != nil {
			c.log.Warn("could not count the fast tier's share of a tiered collection, so "+
				"dbStats describes the fast tier only",
				"ns", ns, "err", err,
				"effect", "objects is short by whatever the archive holds and the fast tier "+
					"does not, which during an adoption is most of the database")
			return 0, false
		}
		delta += logical - held
		counted = true
	}
	if !counted {
		return 0, false
	}
	if bctx.Err() != nil {
		// correctedCount reports a count it could not take as "no correction",
		// so a budget that ran out reads as a database with nothing to correct
		// rather than as a failure. Saying so here is what keeps a partial
		// total from being served as a whole one.
		c.log.Warn("ran out of time counting the archive for dbStats, so it describes the "+
			"fast tier only",
			"db", db, "budget", dbStatsCountBudget.String(),
			"effect", "objects is short by whatever the archive holds and the fast tier does not",
			"note", "the cost is one archive count per tiered collection that is not claimed "+
				"complete; warming the collections removes it")
		return 0, false
	}
	return delta, true
}

// withObjects adds the correction to a dbStats reply's object count, and
// reports whether it found one.
//
// Only a field that is already there is changed, so a reply shaped by a server
// release that does not carry objects is passed through rather than given one.
// avgObjSize is deliberately left alone: it is dataSize over objects on the
// fast tier, and dataSize is a per-tier number, so recomputing it against a
// logical count would describe neither tier.
func withObjects(doc bson.D, delta int64) (bson.D, bool) {
	changed := false
	out := make(bson.D, 0, len(doc))
	for _, e := range doc {
		if e.Key != "objects" {
			out = append(out, e)
			continue
		}
		n, ok := numberAs64(e.Value)
		if !ok {
			out = append(out, e)
			continue
		}
		if n+delta < 0 {
			// The fast tier holds documents the archive has none of, which the
			// feeder warns about at startup: mirroring reports changes from
			// now, so anything written before it first ran is on the fast tier
			// alone. Correcting past zero would answer a negative number of
			// documents, which is worse than either tier's.
			out = append(out, e)
			continue
		}
		out = append(out, bson.E{Key: "objects", Value: sameNumberKind(e.Value, n+delta)})
		changed = true
	}
	return out, changed
}

// numberAs64 reads a decoded reply field that ought to be a count, whichever
// numeric type the server used for it.
//
// The kinds mongod actually answers dbStats with, and no others: a field that
// is not a number is left exactly as it came, because inventing a count for a
// reply shaped in some way this does not understand is how a diagnostic starts
// lying.
func numberAs64(v any) (int64, bool) {
	switch t := v.(type) {
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		return int64(t), true
	default:
		return 0, false
	}
}
