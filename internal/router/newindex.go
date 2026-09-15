package router

import (
	"context"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/wire"
)

// Saying, once, that an index a client just built is on one tier only.
//
// Nothing copies a plain index between the tiers and nothing should: an index
// tree describes the whole collection, and carrying every one of them on the
// archive gives back most of what tiering buys. Which ones the archive needs is
// a decision for whoever runs it, and `cline indexes` is the report that decision
// is made from.
//
// What was missing is the moment. An application adding an index for a new query
// gets it on the fast tier, where the working set is, and the read that falls
// through runs on the archive without it. On a multi-terabyte archive the
// difference between an index and a collection scan is not slow against fast, it
// is minutes against milliseconds, which is `cline indexes`' own argument for
// existing. But that report has to be remembered and run, and the divergence is
// created by an ordinary application command that nobody is watching.
//
// So it is said where it happens, naming the index and the namespace. Once per
// index per router, because an application that recreates an index in a loop
// should not fill the log, and rare enough besides that once is generous:
// building an index is not something a client does per query.
//
// Only for the indexes the archive genuinely lacks, and lacks by key rather than
// by name. An archive index over the same fields under another name serves the
// read exactly as well, and warning about it would be a line an operator checks
// and finds nothing wrong with, which is how a warning stops being read.

// noteIndexNotOnTheArchive says which of the indexes a client just built exist
// on the fast tier alone.
func (c *conn) noteIndexNotOnTheArchive(ctx context.Context, cmd bson.Raw, db, coll string, reply *wire.Message) {
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return
	}
	if rdoc, derr := reply.ReplyDoc(); derr != nil || !replyOK(rdoc) {
		return
	}
	wanted := requestedIndexKeys(cmd)
	if len(wanted) == 0 {
		return
	}
	have, err := indexKeysOf(ctx, c.r.cold, db, coll)
	if err != nil {
		// The archive not answering is not this command's problem, and the
		// command has already succeeded.
		return
	}

	var missing []string
	for name, key := range wanted {
		if have[key] {
			continue
		}
		if c.r.saidIndexIsFastTierOnly(ns, name) {
			continue
		}
		missing = append(missing, name)
	}
	if len(missing) == 0 {
		return
	}
	sort.Strings(missing)

	// Two kinds of index are not a decision about speed. mongod will not answer
	// a $text query or a $geoNear without one, so a read of this namespace that
	// falls through does not get slow, it fails, and the message it fails with
	// names an index the application can see on the fast tier. Measured: a
	// $text query with the text index on the fast tier alone came back "text
	// index required for $text query", and answered as soon as the archive had
	// one too.
	required := false
	for _, name := range missing {
		if indexTypeNeedsItsIndex(wanted[name]) {
			required = true
		}
	}
	if required {
		c.log.Warn("an index was built on the fast tier that the archive needs to answer at all",
			"ns", ns, "indexes", missing,
			"meaning", "a text or geospatial query is refused rather than slowed when the "+
				"collection has no such index, so reads this router cannot answer from the "+
				"fast tier will fail on the archive",
			"action", "build the same index on the archive, or keep this namespace warm enough "+
				"that these reads never fall through",
			"why", "nothing copies an index between the tiers on purpose, and for these two "+
				"kinds that choice costs the query rather than its speed")
		return
	}

	c.log.Warn("an index was built on the fast tier and the archive does not have one over the same fields",
		"ns", ns, "indexes", missing,
		"meaning", "reads this router cannot answer from the fast tier are run on the archive, "+
			"and there they have no index to seek into",
		"action", "run cline indexes to see both tiers, and build it on the archive if reads of "+
			"this namespace reach back past what the fast tier holds",
		"why", "nothing copies a plain index between the tiers on purpose: an index describes the "+
			"whole collection, and carrying every one of them on the archive gives back most of "+
			"what tiering buys")
}

// indexTypeNeedsItsIndex reports whether a key document describes an index a
// query cannot run without, rather than one it runs slowly without.
func indexTypeNeedsItsIndex(key string) bool {
	for _, kind := range []string{`"text"`, `"2dsphere"`, `"2d"`} {
		if strings.Contains(key, kind) {
			return true
		}
	}
	return false
}

// requestedIndexKeys reads the name and key of each index a createIndexes asks
// for, keyed by name.
func requestedIndexKeys(cmd bson.Raw) map[string]string {
	arr, err := cmd.LookupErr("indexes")
	if err != nil {
		return nil
	}
	vals, err := arr.Array().Values()
	if err != nil {
		return nil
	}
	out := map[string]string{}
	for _, v := range vals {
		spec, ok := v.DocumentOK()
		if !ok {
			continue
		}
		key, ok := spec.Lookup("key").DocumentOK()
		if !ok {
			continue
		}
		name, ok := spec.Lookup("name").StringValueOK()
		if !ok {
			name = key.String()
		}
		out[name] = key.String()
	}
	return out
}

// indexKeysOf returns the key of every index on one collection, as written.
func indexKeysOf(ctx context.Context, client *mongo.Client, db, coll string) (map[string]bool, error) {
	cur, err := client.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	out := map[string]bool{}
	for cur.Next(ctx) {
		if key, ok := cur.Current.Lookup("key").DocumentOK(); ok {
			out[key.String()] = true
		}
	}
	return out, cur.Err()
}

// saidIndexIsFastTierOnly reports whether this router has already said so for
// one index, and records that it has.
func (r *Router) saidIndexIsFastTierOnly(ns, index string) bool {
	r.fastOnlyMu.Lock()
	defer r.fastOnlyMu.Unlock()
	if r.fastOnlyIndexes == nil {
		r.fastOnlyIndexes = map[string]bool{}
	}
	k := ns + "\x00" + index
	said := r.fastOnlyIndexes[k]
	r.fastOnlyIndexes[k] = true
	return said
}
