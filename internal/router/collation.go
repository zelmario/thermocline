package router

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A collection carries a default collation, and it decides which documents a
// query matches rather than how quickly they are found. Two documents spelled
// Alice and alice are one value under a case-insensitive collation and two
// values without one.
//
// The archive's copy of a collection is created implicitly, by the feeder
// upserting the first document into it, and an implicitly created collection
// has no collation at all. So a case-insensitive collection answers one way on
// the fast tier and another way on the archive, and both answers are returned
// as success. That is the failure this project treats as worst: not slower, not
// refused, just quietly different.
//
// The router cannot give the archive's collection a collation after the fact,
// because a collection's collation is fixed when it is created. What it can do
// is say the collation out loud on every read it sends there, which produces
// the same matching the fast tier would have done.

// collationTTL is how long a collection's default collation is remembered. It
// cannot change without the collection being dropped and recreated, so this is
// only about noticing that.
const collationTTL = time.Minute

type collationCache struct {
	mu      sync.RWMutex
	fetched map[string]time.Time
	byNS    map[string]bson.Raw
}

// defaultCollation returns the collation the fast tier's collection was created
// with, and whether it has one.
func (r *Router) defaultCollation(ctx context.Context, ns string) (bson.Raw, bool) {
	r.collCache.mu.RLock()
	at, seen := r.collCache.fetched[ns]
	cached, has := r.collCache.byNS[ns]
	r.collCache.mu.RUnlock()
	if seen && time.Since(at) < collationTTL {
		return cached, has && len(cached) > 0
	}

	db, coll := splitNS(ns)
	found := bson.Raw(nil)

	cur, err := r.hot.Database(db).ListCollections(ctx, bson.D{{Key: "name", Value: coll}})
	if err == nil {
		defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
		for cur.Next(ctx) {
			var raw bson.Raw
			if cur.Decode(&raw) != nil {
				continue
			}
			opts, oerr := raw.LookupErr("options")
			if oerr != nil {
				continue
			}
			odoc, ok := opts.DocumentOK()
			if !ok {
				continue
			}
			if c, cerr := odoc.LookupErr("collation"); cerr == nil {
				if cdoc, cok := c.DocumentOK(); cok {
					found = make(bson.Raw, len(cdoc))
					copy(found, cdoc)
				}
			}
		}
	}

	r.collCache.mu.Lock()
	if r.collCache.fetched == nil {
		r.collCache.fetched = map[string]time.Time{}
		r.collCache.byNS = map[string]bson.Raw{}
	}
	r.collCache.fetched[ns] = time.Now()
	r.collCache.byNS[ns] = found
	r.collCache.mu.Unlock()

	return found, len(found) > 0
}

// forgetCollation drops a remembered collation, for when the collection under
// it has been dropped or recreated.
func (r *Router) forgetCollation(ns string) {
	r.collCache.mu.Lock()
	delete(r.collCache.fetched, ns)
	delete(r.collCache.byNS, ns)
	r.collCache.mu.Unlock()
}

// simplifyCollation strips the fields mongod adds when it reports a collation
// back, leaving what a caller would have written.
//
// ListCollections returns the collation fully expanded, including a version
// string that belongs to the server that produced it. Sending that version to
// another server is at best noise and at worst a mismatch, so it goes.
func simplifyCollation(c bson.Raw) bson.D {
	elems, err := c.Elements()
	if err != nil {
		return nil
	}
	out := make(bson.D, 0, len(elems))
	for _, e := range elems {
		if e.Key() == "version" {
			continue
		}
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	return out
}
