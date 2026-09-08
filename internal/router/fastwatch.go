package router

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/checkpoint"
)

// This router remembers things about the fast tier that only stay true while
// nobody else changes it.
//
// Two of them. The query cache holds answers, and an answer is stale the moment
// somebody writes the documents it came from. The indexer holds the fact that a
// namespace has its collection and its indexes on the fast tier, and that stops
// being true the moment somebody drops the collection.
//
// A command through this router updates both, which makes them correct for the
// application talking to it. They say nothing about anybody else: another
// application, a migration script, a person in a shell. The cache had a TTL as
// its bound, which is bounded staleness rather than none. The indexer had no
// bound at all, and that is the one that costs.
//
// The fast tier already publishes every change it makes, and this system is
// built on reading that: it is how the feeder mirrors. So the router follows the
// same stream and corrects itself as the collections change underneath it. The
// window becomes the change stream's own latency, which is milliseconds.
//
// It costs one change stream per router. It used to run only while the cache
// was on, which made it a feature of the cache; it is not, it is how this router
// finds out about a fast tier it does not have to itself.

// What the indexer's memory going stale actually costs, because it is not
// obvious and it is not slowness.
//
// The router creates a tiered collection on the fast tier the way the archive
// has it, with its default collation and its indexes, before any write that
// would otherwise create it bare. It remembers having done so, and never asks
// again. Drop that collection straight on the fast tier, out of band, and the
// memory stands: the next write recreates the collection implicitly, with no
// collation, no validator and no index but _id_.
//
// Measured on an adopted collection with a case-insensitive collation and a
// unique index on name, holding alice and bob in the archive. After the
// out-of-band drop the fast tier's copy came back with `collation: NONE` and no
// unique index, and once the router's own index listing expired a minute later
// it stopped claiming keys for the namespace: an insert of a second alice was
// **accepted**, against a constraint the application had relied on since before
// the adoption. Nothing in either tier's log mentions it.
//
// It corrected itself only when something listed the catalog or the router was
// restarted. Neither is a thing an application does.

// invalidationBackoff is how long to wait before reopening a change stream that
// ended. The feeder's reopen logs at ERROR during a failover because it is the
// recovery working; this one is quieter because one missed window is recovered
// by the next catalog listing or router start.
const invalidationBackoff = 2 * time.Second

// watchFastTier keeps this router honest about changes it did not make.
//
// When it cannot run, nothing here is wrong, it is weaker: the cache is back to
// its learned TTL as the only bound on somebody else's write, and a collection
// dropped out of band is noticed at the next catalog listing or router start.
// That has to be visible rather than assumed. The first failure is an ERROR, the
// dashboard says whether it is watching, and it keeps trying.
func (r *Router) watchFastTier(ctx context.Context) {
	r.log.Info("watching the fast tier for changes this router did not make",
		"why", "a collection dropped out of band leaves this router creating it bare on the "+
			"next write, and a write by anybody else leaves the cache holding stale answers")
	first := true
	// Where the stream had got to when it ended, so the next one carries on
	// from there rather than from now.
	//
	// A stream ends on an ordinary fast tier failover, and reopening at "now"
	// silently skips whatever happened in between. For the cache that was a few
	// stale answers; for this it is the whole defect coming back, because a drop
	// missed here is never noticed again. StartAfter rather than ResumeAfter,
	// because a drop is followed by an invalidate and ResumeAfter refuses to
	// carry on past one.
	var after bson.Raw
	for ctx.Err() == nil {
		next, err := r.followChanges(ctx, after)
		if ctx.Err() != nil {
			return
		}
		after = next
		r.watching.Store(false)
		if after != nil && checkpoint.ResumePointGone(err) {
			// The token names a position the fast tier's oplog no longer has,
			// which means this router was away for longer than the oplog is
			// long. Carrying on from it is impossible and refusing to watch at
			// all would be worse, so it starts again from now and says what
			// that leaves unseen.
			r.log.Warn("this router was away from the fast tier for longer than its oplog, so "+
				"it cannot carry on from where it stopped watching",
				"err", err,
				"effect", "a collection dropped out of band during that time will be noticed at "+
					"the next catalog listing rather than now",
				"action", "cline indexes reports what each tier is missing")
			after = nil
		}
		if first {
			r.log.Error("this router cannot watch the fast tier, so it will not hear about a "+
				"collection dropped out of band until something lists the catalog or it is "+
				"restarted, and the cache is back to its learned TTL alone",
				"err", err,
				"remedy", "this router's own account needs to open a change stream on the fast "+
					"tier, which is the privilege the feeder already holds")
			first = false
		} else {
			r.log.Warn("the fast tier's change stream ended, reopening", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(invalidationBackoff):
		}
	}
}

// followChanges reads the fast tier's changes until the stream ends, and
// reports the last position it reached so the next one can carry on from there.
func (r *Router) followChanges(ctx context.Context, after bson.Raw) (bson.Raw, error) {
	pipeline := mongo.Pipeline{}

	// With the cache off, only the DDL is wanted, and saying so server side is
	// the difference between this costing nothing and this costing a decoded
	// event per write on the whole fast tier. The cache is what needs to hear
	// about ordinary writes; the indexer needs to hear about four events that
	// happen a handful of times a day.
	//
	// Left out entirely when the cache is on, because then every event is
	// wanted anyway and a $match that selects everything is work for nothing.
	if r.cache == nil {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{
				"drop", "rename", "dropDatabase",
			}}}},
		}}})
	}

	// Whatever the collection, because the cache holds answers for tiered and
	// untiered collections alike. Only the namespaces are needed, so the
	// documents themselves are left behind; `to` comes with them because a
	// rename is the one event that names two.
	pipeline = append(pipeline, bson.D{{Key: "$project", Value: bson.D{
		{Key: "ns", Value: 1},
		{Key: "to", Value: 1},
		{Key: "operationType", Value: 1},
	}}})

	opts := options.ChangeStream()
	if after != nil {
		opts.SetStartAfter(after)
	}
	cs, err := r.hot.Watch(ctx, pipeline, opts)
	if err != nil {
		return after, err
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
			To struct {
				DB   string `bson:"db"`
				Coll string `bson:"coll"`
			} `bson:"to"`
		}
		// Before the decode, and before anything acts on the event: the token
		// says this event has been seen, and an event this router cannot decode
		// is one it must not be handed again for ever.
		after = cs.ResumeToken()
		if err := cs.Decode(&ev); err != nil {
			continue
		}
		switch {
		case ev.NS.Coll != "":
			ns := ev.NS.DB + "." + ev.NS.Coll
			r.invalidateFor(ns, ev.OperationType)
			r.noteFastTierDDL(ctx, ns, ev.OperationType)
			if ev.To.Coll != "" {
				// A rename leaves a different collection under the new name
				// too, and this router may well remember the old one.
				r.noteFastTierDDL(ctx, ev.To.DB+"."+ev.To.Coll, ev.OperationType)
			}
		case ev.NS.DB != "":
			// dropDatabase and the invalidate that follows it name no
			// collection, so everything in that database goes.
			if r.cache != nil {
				if n := r.cache.InvalidateDatabase(ev.NS.DB); n > 0 {
					r.log.Debug("a change to a whole database dropped cached answers",
						"db", ev.NS.DB, "entries", n, "event", ev.OperationType)
				}
			}
			if ev.OperationType == "dropDatabase" {
				r.log.Info("a database this router serves was dropped on the fast tier by "+
					"somebody else, forgetting what it knew about it",
					"db", ev.NS.DB,
					"note", "the archive keeps its documents, so tiered collections there answer "+
						"from it until something writes to them again")
				r.forgetEverythingIn(ev.NS.DB)
			}
		}
	}
	return after, cs.Err()
}

// noteFastTierDDL corrects what this router remembers about a namespace whose
// collection has just left the fast tier.
//
// drop and rename only. A create is deliberately not here: this router creates
// collections on the fast tier itself, on the first copy-up into a namespace and
// on every index build, so acting on a create event would have each of those
// undo the memory it had just written and the next write would create the
// collection again. The sequence that matters is a drop followed by a write, and
// the drop is the half worth hearing.
func (r *Router) noteFastTierDDL(ctx context.Context, ns, op string) {
	if op != "drop" && op != "rename" {
		return
	}
	if !r.cfg.Manages(ns) {
		return
	}
	// This fires for a drop this router forwarded as well, where the forwarding
	// path has already done the same work and doing it again changes nothing.
	// The line does not claim otherwise, because a change stream event says
	// what happened and never says who asked for it. The drop it exists for is
	// the one this router never saw.
	r.log.Info("a tiered collection was dropped on the fast tier, forgetting what this router "+
		"knew about it",
		"ns", ns, "op", op,
		"effect", "without this the next write recreates it with no collation, no validator and "+
			"no index but _id_, and a unique index the archive still carries stops being "+
			"enforced on either tier",
		"note", "the archive keeps its documents, so reads answer from it until something "+
			"writes to this namespace again")
	r.forgetDropped(ns)
	// A view dropped out of band on the fast tier, which this stream is the only
	// way to hear about. Same rule as the forwarded drop above it.
	r.dropArchiveView(ctx, ns)
}

// forgetDropped drops what this router remembers about a namespace the fast tier
// no longer holds.
//
// One list for both drops: the one this router forwards and the one it hears
// about on the fast tier's change stream. They were written separately and had
// already drifted, which is the argument for a single list rather than two that
// agree today.
//
// Every memory here is one whose staleness is a wrong answer. forgetIndexes is
// the one that is not, and it is deliberately absent: the router reads the fast
// tier's unique indexes to know which keys it has to protect across the tiers,
// and a collection that has just gone lists none, so re-reading now would
// replace a listing that is merely old with one that is empty. Empty is the
// direction that lets a duplicate of an archived document through, and the
// archive still holds every document the constraint covered, because a drop is
// never replayed there. That listing is refreshed by the index build the next
// write starts, which is when there is something true to read.
func (r *Router) forgetDropped(ns string) {
	r.forgetCompleteness(ns)
	// A drop and a recreate can put differently typed ages under the same name.
	// See sortSafeAges.
	r.forgetBrackets(ns)
	// A collection's collation is fixed at creation, so a drop is the only way
	// it changes.
	r.forgetCollation(ns)
	// These two are here because a drop makes them untrue, and not because a
	// reproduction was found. That is stated rather than dressed up.
	//
	// The hint is a sparse or partial index's own filter, which this router
	// reproduces against the archive so a fall-through read is narrowed the way
	// the hint narrows it, and reproducing the filter of an index that is gone
	// asks the archive a narrower question than the client asked. That has been
	// measured for a dropped *index*, which is why forgetHint exists at all. For
	// a dropped *collection* it could not be: the check written for it answered
	// 20 of 20 either way, because putting the index back goes through
	// createIndexes and that path already clears this cache. The check was
	// removed rather than kept green for nothing.
	r.forgetHint(ns)
	// Whether this name is a view, and what it is a view on, decides whose
	// completeness claim judges a read of it. A drop can put a view where a
	// collection was or the other way round, and either way what is remembered
	// is about the collection before. No reproduction here either.
	r.forgetView(ns)
	// The one that does not correct itself on its own. See the top of this file
	// for what it costs.
	r.indexer.Forget(ns)
}

// invalidateFor drops what was cached for one collection.
func (r *Router) invalidateFor(ns, op string) {
	if r.cache == nil {
		return
	}
	if n := r.cache.Invalidate(ns); n > 0 {
		r.log.Debug("a change on the fast tier dropped cached answers",
			"ns", ns, "entries", n, "event", op)
	}
}
