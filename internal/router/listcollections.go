package router

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/wire"
)

// A tiered collection the fast tier does not hold was missing from the catalog.
//
// listCollections is answered by the fast tier, because that is where the
// application's collections live and where its writes go. For a tiered
// collection that is only a partial view: the fast tier holds the recent part
// and the archive holds the rest, and reads of it are answered from whichever
// has the documents. The catalog does not work that way. If the fast tier has
// no collection of that name, listCollections does not mention it, and the
// application is told it does not exist.
//
// Reads of it still work. Measured on the pilot, with sample_supplies.sales
// dropped from the fast tier and 5,000 documents in the archive:
//
//	countDocuments  5000
//	findOne         {"_id":"5bd761dcae323e45a93ccfe8"}
//	show collections
//
// The last one is not an error, it is empty. So the data is there and reachable
// by name, and anything that finds its work by asking what exists finds
// nothing: mongodump backs up an empty database, an ORM migrating its schema
// sees no table, a BI tool syncing its model drops the collection from its
// list. It reads as data loss and it is not.
//
// This is not a hypothetical drop. It is the ordinary state of an adoption. The
// existing cluster becomes the archive and the fast tier starts empty, and
// until something puts a collection there, every tiered collection is in
// exactly this position. The router creates them at startup from the archive's
// options, which covers the collections that exist then, and nothing covers one
// created later or dropped since.
//
// So the archive's tiered collections are merged into the reply. Only the ones
// the configuration tiers: an untiered collection the archive holds is read
// from the fast tier and comes back empty, and listing it would be the lie that
// this fixes. That case is a misconfiguration and is warned about at startup.

// listCollectionsBudget bounds the archive's share of the answer. The catalog
// is asked for on every connection by some drivers, and the archive is the
// cluster being relieved, so a slow one costs the fast tier's answer rather
// than the whole command.
const listCollectionsBudget = 5 * time.Second

// interceptListCollections adds the tiered collections the archive holds and
// the fast tier does not.
func (c *conn) interceptListCollections(ctx context.Context, msg *wire.Message, db string, reply *wire.Message) (*wire.Message, error) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return reply, nil
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return reply, nil
	}
	batch, ok := firstBatch(rdoc)
	if !ok {
		return reply, nil
	}
	// Before the archive is asked anything. Some drivers request the catalog on
	// every connection, and the archive is the cluster being relieved, so a
	// database with nothing tiered in it must not cost it a round trip.
	if !c.r.managesAnythingIn(db) {
		return reply, nil
	}

	// Names the fast tier already reported. A collection on both tiers is the
	// normal case and the fast tier's entry is the one to keep: it is what the
	// application writes to, and its options are what a write is validated
	// against.
	seen := make(map[string]struct{}, len(batch))
	for _, v := range batch {
		doc, dok := v.DocumentOK()
		if !dok {
			continue
		}
		if name, nok := stringField(doc, "name"); nok {
			seen[name] = struct{}{}
		}
	}

	extra, err := c.archiveOnlyCollections(ctx, cmd, db, seen)
	if err != nil {
		// The fast tier's answer is a true list of what the fast tier holds, so
		// it stands. Saying so matters: the difference between this and a
		// working merge is invisible in the reply.
		c.log.Warn("could not ask the archive what it holds, so the collection list is the fast tier's only",
			"db", db, "err", err,
			"effect", "a tiered collection the fast tier does not hold is missing from this list, "+
				"though reads of it still work")
		return reply, nil
	}
	if len(extra) == 0 {
		return reply, nil
	}

	// Appended to the first batch. The fast tier may have left a cursor open
	// for the rest of its own list, and these go in once, here, rather than
	// into a getMore this router does not answer.
	out := make(bson.A, 0, len(batch)+len(extra))
	for _, v := range batch {
		out = append(out, v)
	}
	for _, d := range extra {
		out = append(out, d)
	}
	id, ns := cursorIdentity(rdoc, db+".$cmd.listCollections")
	return cursorReply(reply.Header.RequestID, msg.Header.RequestID, id, ns, "firstBatch", out)
}

// archiveOnlyCollections is the tiered collections the archive holds that are
// not in seen.
//
// The command's own filter and nameOnly are passed through, so the archive
// applies them the way the fast tier did and the two halves of the answer are
// selected and shaped alike.
func (c *conn) archiveOnlyCollections(ctx context.Context, cmd bson.Raw, db string, seen map[string]struct{}) ([]bson.D, error) {
	filter := bson.D{}
	if v, err := cmd.LookupErr("filter"); err == nil {
		if fdoc, ok := v.DocumentOK(); ok {
			if err := bson.Unmarshal(fdoc, &filter); err != nil {
				filter = bson.D{}
			}
		}
	}
	opts := options.ListCollections()
	if boolField(cmd, "nameOnly") {
		opts.SetNameOnly(true)
	}

	lctx, cancel := context.WithTimeout(ctx, listCollectionsBudget)
	defer cancel()
	cur, err := c.r.cold.Database(db).ListCollections(lctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(lctx)) }()

	var out []bson.D
	for cur.Next(lctx) {
		var spec bson.D
		if err := bson.Unmarshal(cur.Current, &spec); err != nil {
			continue
		}
		name, ok := stringField(cur.Current, "name")
		if !ok {
			continue
		}
		if _, have := seen[name]; have {
			continue
		}
		if !c.r.cfg.Manages(db + "." + name) {
			continue
		}
		// A view is not tiered and cannot be. It holds no documents, the feeder
		// refuses it for that reason, and a read of one goes to the fast tier
		// and finds nothing, so listing it would be the same lie as listing an
		// untiered collection. It matters most with no namespaces configured,
		// which means everything is tiered and would otherwise sweep views in
		// with the rest, and hand one to the indexer to be built as a
		// collection. Views the fast tier lacks are warned about at startup.
		if kind, kok := stringField(cur.Current, "type"); kok && kind != "collection" {
			continue
		}
		// And only what this client would have been allowed to see. The fast
		// tier filtered its half of the answer by the client's privileges; the
		// archive's half is read with the router's own credentials, so it is
		// asked on the client's behalf before anything is added. See canRead.
		if !c.canRead(db, name) {
			continue
		}
		out = append(out, spec)

		// And close the gap, so the next answer does not need this. The router
		// makes a tiered collection on the fast tier at startup, with the
		// archive's options and indexes, precisely so that it can be listed and
		// asked for its stats; nothing covered one dropped or created since.
		// Seeing it missing here is the same signal, arriving later.
		//
		// In the background and rate limited inside the indexer, because some
		// drivers ask for the catalog on every connection and this must not
		// become a create attempt per connection.
		ns := db + "." + name
		c.r.indexer.Forget(ns)
		c.r.goSafely("building the archive's indexes on the fast tier", func() {
			c.r.indexer.Ensure(context.WithoutCancel(ctx), ns)
		})
	}
	return out, cur.Err()
}

// The same gap one level up: a database the fast tier has no trace of.
//
// listDatabases is answered by the fast tier too, and a database whose tiered
// collections are all in the archive is not in that answer. Measured on the
// pilot, with sample_supplies dropped from the fast tier:
//
//	show dbs                          sample_supplies is not in it
//	use sample_supplies; show collections   sales
//	db.sales.countDocuments({})       5000
//
// So the database is absent from the list and complete underneath it, which is
// worse than a database that lists as empty: nothing names it anywhere, and an
// operator checking what survived an adoption reads it as gone.
//
// The entries added carry no size. sizeOnDisk is bytes on a cluster, and adding
// two clusters' compressed figures together produces a number describing
// nothing; the fast tier holds none of this database, so its figure is zero and
// that is what an operator sizing a fast tier should see. empty is about
// documents rather than bytes, and there are documents, so it is false.
func (c *conn) interceptListDatabases(ctx context.Context, msg *wire.Message, reply *wire.Message) (*wire.Message, error) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return reply, nil
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return reply, nil
	}
	v, err := rdoc.LookupErr("databases")
	if err != nil {
		return reply, nil
	}
	listed, err := v.Array().Values()
	if err != nil {
		return reply, nil
	}

	seen := make(map[string]struct{}, len(listed))
	for _, e := range listed {
		doc, ok := e.DocumentOK()
		if !ok {
			continue
		}
		if name, nok := stringField(doc, "name"); nok {
			seen[name] = struct{}{}
		}
	}

	nameOnly := boolField(cmd, "nameOnly")
	filter := bson.D{}
	if fv, ferr := cmd.LookupErr("filter"); ferr == nil {
		if fdoc, ok := fv.DocumentOK(); ok {
			if err := bson.Unmarshal(fdoc, &filter); err != nil {
				filter = bson.D{}
			}
		}
	}

	lctx, cancel := context.WithTimeout(ctx, listCollectionsBudget)
	defer cancel()
	opts := options.ListDatabases()
	if nameOnly {
		opts.SetNameOnly(true)
	}
	res, err := c.r.cold.ListDatabases(lctx, filter, opts)
	if err != nil {
		c.log.Warn("could not ask the archive which databases it holds, so the list is the fast tier's only",
			"err", err,
			"effect", "a database whose tiered collections are all in the archive is missing from this list, "+
				"though reads of it still work")
		return reply, nil
	}

	extra := make(bson.A, 0)
	for _, d := range res.Databases {
		if _, have := seen[d.Name]; have {
			continue
		}
		if !c.r.managesAnythingIn(d.Name) {
			continue
		}
		// Asked on the client's behalf, for the same reason as the collections.
		// A user with no privileges on a database is not told it exists.
		if !c.canReadAnyOf(lctx, d.Name) {
			continue
		}
		if nameOnly {
			extra = append(extra, bson.D{{Key: "name", Value: d.Name}})
			continue
		}
		extra = append(extra, bson.D{
			{Key: "name", Value: d.Name},
			{Key: "sizeOnDisk", Value: int64(0)},
			{Key: "empty", Value: false},
		})
	}
	if len(extra) == 0 {
		return reply, nil
	}

	var out bson.D
	if err := bson.Unmarshal(rdoc, &out); err != nil {
		return reply, nil
	}
	for i, e := range out {
		if e.Key != "databases" {
			continue
		}
		merged := make(bson.A, 0, len(listed)+len(extra))
		for _, l := range listed {
			merged = append(merged, l)
		}
		merged = append(merged, extra...)
		out[i] = bson.E{Key: "databases", Value: merged}
		return wire.NewMsg(reply.Header.RequestID, msg.Header.RequestID, out)
	}
	return reply, nil
}

// managesAnythingIn reports whether any tiered namespace lives in db, without
// asking either tier. With nothing configured every namespace is tiered.
func (r *Router) managesAnythingIn(db string) bool {
	if isSystemDatabase(db) {
		return false
	}
	if len(r.cfg.Managed) == 0 {
		return true
	}
	for _, ns := range r.cfg.Managed {
		if d, _ := splitNS(ns); d == db {
			return true
		}
	}
	return false
}

// canReadAnyOf reports whether this client may read anything tiered in a
// database, which is what naming the database in a listing amounts to.
//
// The probe needs a collection, and the database is not on the fast tier, so
// one is taken from what is tiered there. A find on a collection the fast tier
// does not have succeeds for a client that is allowed the namespace and is
// refused for one that is not, which is exactly the question.
func (c *conn) canReadAnyOf(ctx context.Context, db string) bool {
	for _, ns := range c.r.cfg.Managed {
		d, coll := splitNS(ns)
		if d == db {
			return c.canRead(db, coll)
		}
	}
	// Nothing configured means everything is tiered, and then the archive's own
	// collection names are the only list there is.
	names, err := c.r.cold.Database(db).ListCollectionNames(ctx, bson.D{})
	if err != nil || len(names) == 0 {
		return false
	}
	return c.canRead(db, names[0])
}
