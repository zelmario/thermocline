package router

import (
	"context"
	"errors"
	"sort"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// warnAboutIndexesTheArchiveLacks names the fast tier's indexes that no index on
// the archive can serve, for every tiered collection.
//
// The read the fast tier cannot answer in full goes to the archive as the
// application wrote it, so it needs the same indexes there that it uses here.
// Nothing copies an index to the archive, on purpose: the archive is the
// application's existing cluster on an adoption, and building on it is the
// operator's call. That makes this the step most easily missed, and missing it
// is silent. Every fall-through read scans, and the cost grows with the archive.
//
// Measured on the Hetzner staging pair on 2026-10-12, a shop of 8,000 orders
// with an evictor running: with only _id and one other index on the archive,
// throughput fell from 269 to 172 operations a second in twenty minutes while
// the archive's primary sat at load 5.6 on two cores, 41 seconds of every ten
// spent counting and aggregating shopstore.orders. Four indexes built in a
// second and the next interval ran at 253. Nothing in any log said why.
//
// Unique indexes are left to warnAboutUniqueIndexesTheArchiveLacks, which says
// what a missing one costs on the write path, and _id_ is on every collection.
// A TTL index is left out too: built on the archive it deletes the copies the
// archive exists to keep, `cline indexes` says so beside it, and the router
// warns about a TTL index on a tiered collection on its own. The first version
// named one here and told the operator to build it, which the remedy it named
// then told them not to do.
func (r *Router) warnAboutIndexesTheArchiveLacks(ctx context.Context) {
	for _, ns := range r.tieredNamespaces(ctx) {
		db, coll := splitNS(ns)
		fast, err := listIndexFields(ctx, r.hot, db, coll)
		if err != nil {
			r.log.Debug("could not list the fast tier's indexes to compare them", "ns", ns, "err", err)
			continue
		}
		have, err := archiveIndexFields(ctx, r.cold, db, coll)
		if err != nil {
			// The archive not answering is said elsewhere, loudly.
			r.log.Debug("could not list the archive's indexes to compare them", "ns", ns, "err", err)
			continue
		}
		if missing := indexesNotServed(fast, have); len(missing) > 0 {
			r.log.Warn("the archive has no index for some of the fast tier's indexes on a tiered collection",
				"ns", ns, "indexes", missing,
				"meaning", "a read the fast tier cannot answer in full runs on the archive as the "+
					"application wrote it, and without the same index there it scans, at a cost "+
					"that grows with the archive",
				"measured", "on a shop of 8,000 orders, throughput fell from 269 to 172 operations a "+
					"second in twenty minutes with the archive's primary at load 5.6, and recovered "+
					"to 253 within a minute of building four indexes",
				"action", "run `cline indexes --commands` for the createIndex commands, and build "+
					"those the application's older reads use; leaving one out keeps the archive "+
					"smaller and is a fair choice for a query that never reaches past the fast tier")
		}
	}
}

// namedIndex is one index as the comparison needs it.
type namedIndex struct {
	name   string
	fields []string
	unique bool
	ttl    bool
}

// listIndexFields reads every index on one side for a namespace, with the field
// names of its key in order. By name rather than by rendered key, for the reason
// archiveIndexFields gives.
func listIndexFields(ctx context.Context, c *mongo.Client, db, coll string) ([]namedIndex, error) {
	cur, err := c.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		var ce mongo.CommandError
		if errors.As(err, &ce) && ce.Code == namespaceNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var out []namedIndex
	for cur.Next(ctx) {
		key, ok := cur.Current.Lookup("key").DocumentOK()
		if !ok {
			continue
		}
		elems, eerr := key.Elements()
		if eerr != nil {
			continue
		}
		idx := namedIndex{}
		if v, ok := cur.Current.Lookup("name").StringValueOK(); ok {
			idx.name = v
		}
		if v, ok := cur.Current.Lookup("unique").BooleanOK(); ok {
			idx.unique = v
		}
		if _, err := cur.Current.LookupErr("expireAfterSeconds"); err == nil {
			idx.ttl = true
		}
		for _, e := range elems {
			idx.fields = append(idx.fields, e.Key())
		}
		out = append(out, idx)
	}
	return out, cur.Err()
}

// indexesNotServed is the names of the fast tier's indexes, other than _id_, the
// unique ones and the TTL ones, that no archive index begins with, sorted.
//
// The same prefix test the unique warning uses, and for the same reason: an
// index that leads with the same fields serves every read the shorter one does,
// and telling an operator to build what they have is worse than saying nothing.
func indexesNotServed(fast []namedIndex, have [][]string) []string {
	var missing []string
	for _, idx := range fast {
		if idx.name == "_id_" || idx.unique || idx.ttl || len(idx.fields) == 0 {
			continue
		}
		if !servedByAnArchiveIndex(idx.fields, have) {
			missing = append(missing, idx.name)
		}
	}
	sort.Strings(missing)
	return missing
}
