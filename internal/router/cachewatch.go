package router

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// A cache that only knows about its own writes is a cache with a staleness
// window. This closes it.
//
// A write through this router drops what was learned about that collection
// immediately, which makes the cache correct for the application talking to it.
// It says nothing about anybody else: another application, a migration script, a
// person in a shell. For those the only bound was the learned TTL, which is
// bounded staleness rather than none.
//
// The fast tier already publishes every change it makes, and this system is
// built on reading that: it is how the feeder mirrors. So the router follows the
// same stream and drops cached answers as the collections they came from change.
// The window becomes the change stream's own latency, which is milliseconds,
// rather than a TTL measured in minutes.
//
// This is the difference between expiring a cached result and maintaining it. It
// is still the cheap version of that idea, because an answer is discarded and
// fetched again rather than updated in place, but what a client is served is
// never older than the last write anybody made.
//
// It costs one change stream per router, and only while the cache is on. The
// router's own account needs to be allowed to open one across the deployment,
// which is the same privilege the feeder already needs for the namespaces it
// mirrors.

// invalidationBackoff is how long to wait before reopening a change stream that
// ended. The feeder's reopen logs at ERROR during a failover because it is the
// recovery working; this one is quieter because a cache that misses is slow
// rather than wrong.
const invalidationBackoff = 2 * time.Second

// watchForInvalidation keeps the cache honest about writes this router did not
// make.
//
// When it cannot run, the cache is not wrong, it is weaker: back to the learned
// TTL as the only bound on somebody else's write. That has to be visible rather
// than assumed, because an operator who turned this on believes they have the
// stronger promise. The first failure is an ERROR, the dashboard says whether it
// is watching, and it keeps trying.
func (r *Router) watchForInvalidation(ctx context.Context) {
	if r.cache == nil {
		return
	}
	r.log.Info("watching the fast tier for changes the cache has to hear about",
		"why", "a write by anybody else drops the answers it would have made stale")
	first := true
	for ctx.Err() == nil {
		err := r.followChanges(ctx)
		if ctx.Err() != nil {
			return
		}
		r.watching.Store(false)
		if first {
			r.log.Error("the cache cannot watch the fast tier for changes, so it is back to the "+
				"learned TTL alone as the bound on a write this router did not make",
				"err", err, "remedy", "this router's own account needs to open a change stream, "+
					"or set router.cache.enabled to false")
			first = false
		} else {
			r.log.Warn("the cache's change stream ended, reopening", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(invalidationBackoff):
		}
	}
}

// followChanges reads the fast tier's changes until the stream ends.
func (r *Router) followChanges(ctx context.Context) error {
	// Whatever the collection, because the cache holds answers for tiered and
	// untiered collections alike. Only the namespace is needed, so the
	// documents themselves are left behind.
	cs, err := r.hot.Watch(ctx, mongo.Pipeline{
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "ns", Value: 1},
			{Key: "operationType", Value: 1},
		}}},
	})
	if err != nil {
		return err
	}
	defer func() { _ = cs.Close(context.WithoutCancel(ctx)) }()
	r.watching.Store(true)

	for cs.Next(ctx) {
		var ev struct {
			OperationType string `bson:"operationType"`
			NS            struct {
				DB   string `bson:"db"`
				Coll string `bson:"coll"`
			} `bson:"ns"`
		}
		if err := cs.Decode(&ev); err != nil {
			continue
		}
		switch {
		case ev.NS.Coll != "":
			r.invalidateFor(ev.NS.DB+"."+ev.NS.Coll, ev.OperationType)
		case ev.NS.DB != "":
			// dropDatabase and the invalidate that follows it name no
			// collection, so everything in that database goes.
			if n := r.cache.InvalidateDatabase(ev.NS.DB); n > 0 {
				r.log.Debug("a change to a whole database dropped cached answers",
					"db", ev.NS.DB, "entries", n, "event", ev.OperationType)
			}
		}
	}
	return cs.Err()
}

// invalidateFor drops what was cached for one collection.
func (r *Router) invalidateFor(ns, op string) {
	if n := r.cache.Invalidate(ns); n > 0 {
		r.log.Debug("a change on the fast tier dropped cached answers",
			"ns", ns, "entries", n, "event", op)
	}
}
