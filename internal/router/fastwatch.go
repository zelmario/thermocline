package router

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/cache"
	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/tierver"
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
	// Whether a stream has ever been open, which is what separates "cannot" from
	// "ended".
	everOpened := false
	for ctx.Err() == nil {
		next, err := r.followChanges(ctx, after)
		if ctx.Err() != nil {
			return
		}
		after = next
		// Whether this attempt ever had a stream, read before it is cleared.
		// An attempt that opened one and then ended is a failover or a stepdown;
		// one that never opened is the shape a privilege problem takes, and the
		// two want opposite things said about them. See below.
		if r.watching.Swap(false) {
			everOpened = true
		}
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
		switch {
		case everOpened:
			// It has watched, so it can watch. A stream ends on every fast tier
			// failover and on every stepdown, and the next attempt carries on
			// from `after`.
			r.log.Warn("the fast tier's change stream ended, reopening", "err", err)
		case first:
			// Never once established, which is what the ERROR was written for:
			// a privilege this router's account does not hold leaves the whole
			// thing silently weaker, so it has to be loud.
			//
			// It used to fire on the *first end* rather than the first failure
			// to open, and those are not the same event. Measured under
			// testchaos, which shuts the fast tier's primary down beneath a
			// running router: the error was "(ShutdownInProgress) The server is
			// in quiesce mode and will shut down" and this line asserted the
			// account was missing a privilege and that nothing would be heard
			// "until something lists the catalog or it is restarted". Both
			// wrong, on an ordinary election, at ERROR. An operator following
			// that remedy would go and audit roles that are fine.
			//
			// So the remedy is a hypothesis about the error rather than a claim
			// about the cause, and the consequence says what is true while it
			// retries.
			r.log.Error("this router has not been able to watch the fast tier once",
				"err", err,
				"effect", "while this is failing the cache is back to its learned TTL alone, and a "+
					"collection dropped out of band is noticed at the next catalog listing rather "+
					"than at once",
				"retrying", invalidationBackoff.String(),
				"remedy", "if the error above is an authorization failure, this router's own "+
					"account needs to open a change stream on the fast tier, which is the "+
					"privilege the feeder already holds")
			first = false
		default:
			r.log.Warn("still cannot watch the fast tier, retrying", "err", err)
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
	modern := r.expandedEventsSupported(ctx)

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
				"drop", "rename", "dropDatabase", "createIndexes", "dropIndexes",
			}}}},
		}}})
	}

	// Whatever the collection, because the cache holds answers for tiered and
	// untiered collections alike. Only the namespaces are needed, so the
	// documents themselves are left behind; `to` comes with them because a
	// rename is the one event that names two.
	project := bson.D{
		{Key: "ns", Value: 1},
		{Key: "to", Value: 1},
		{Key: "operationType", Value: 1},
	}
	// With the cache on, an insert's document and the names of an update's
	// fields as well, so a write that cannot change a held answer leaves it in
	// place. See cache.Reads.
	//
	// Not the update's values, and not a document over judgedDocMax. The
	// projection above is what kept these events small, and this stream does
	// not survive one over 16MB: it fails on the event, resumes at the same
	// event and fails again, and from then on nothing anybody else writes
	// drops a cached answer. An insert too large to send is judged as a write
	// of unknown effect, which drops what read its collection, as before.
	//
	// $bsonSize arrived in 4.4, and an operator this stream cannot parse stops
	// it opening at all, so below 6.0, where the expanded events are refused
	// for the same reason, every write stays a write of unknown effect.
	if r.cache != nil && modern {
		project = append(project,
			bson.E{Key: "fullDocument", Value: bson.D{{Key: "$cond", Value: bson.A{
				bson.D{{Key: "$lte", Value: bson.A{bson.D{{Key: "$bsonSize", Value: "$fullDocument"}}, judgedDocMax}}},
				"$fullDocument", "$$REMOVE"}}}},
			bson.E{Key: "updatedKeys", Value: bson.D{{Key: "$map", Value: bson.D{
				{Key: "input", Value: bson.D{{Key: "$objectToArray", Value: "$updateDescription.updatedFields"}}},
				{Key: "as", Value: "f"},
				{Key: "in", Value: "$$f.k"}}}}},
			bson.E{Key: "updateDescription.removedFields", Value: 1},
			bson.E{Key: "updateDescription.truncatedArrays.field", Value: 1},
		)
	}
	pipeline = append(pipeline, bson.D{{Key: "$project", Value: project}})

	opts := options.ChangeStream()
	if after != nil {
		opts.SetStartAfter(after)
	}
	// Index DDL is the one change to a collection that an ordinary change
	// stream does not carry, and it is the one this router most needs to hear:
	// a unique index dropped straight on the fast tier leaves it enforcing a
	// constraint that no longer exists, refusing writes mongod accepts, until
	// the listing expires a minute later. Measured, with a duplicate key error
	// naming an index neither tier has.
	//
	// Asked for rather than assumed. showExpandedEvents arrived in MongoDB 6.0
	// and this supports 4.2, where passing it makes the whole stream fail to
	// open -- and that stream is also what drops cached answers and notices a
	// collection dropped out of band, so losing it to gain this would be a poor
	// trade. Below 6.0 the minute stands, which is what it has always been.
	if modern {
		opts.SetShowExpandedEvents(true)
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
			FullDocument      bson.Raw         `bson:"fullDocument"`
			UpdatedKeys       []string         `bson:"updatedKeys"`
			UpdateDescription *eventUpdateDesc `bson:"updateDescription"`
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
			r.invalidateFor(ns, ev.OperationType, eventWrite(ev.OperationType, ev.FullDocument, ev.UpdatedKeys, ev.UpdateDescription))
			r.noteFastTierIndexDDL(ns, ev.OperationType)
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

// expandedEventsSupported reports whether the fast tier will accept
// showExpandedEvents, which is MongoDB 6.0 and up.
//
// Read per connection attempt rather than once at startup, because the stream
// reconnects and a cluster can be upgraded under it. It is a hello, and it is
// paid once per stream rather than once per event.
func (r *Router) expandedEventsSupported(ctx context.Context) bool {
	v, ok := tierver.Read(ctx, r.hot)
	if !ok || len(v.Parts) == 0 {
		return false
	}
	return v.Parts[0] >= 6
}

// noteFastTierIndexDDL forgets a collection's unique index listing when its
// indexes change on the fast tier.
//
// Unlike a drop, the collection is still there, so the next listing reads the
// truth. It is still staleIndexes rather than forgetIndexes, for two reasons:
// a listing that cannot be read should leave the old one standing rather than
// nothing, and uniqueIndexes can only say which constraint it has stopped
// enforcing if it still has the previous listing to compare against. The hint
// goes with it, being the same index's own filter reproduced against the
// archive.
//
// This fires for index builds the router starts itself, on a copy-up or a warm.
// Those change the listing too, and re-reading a listing that has not changed
// costs one listIndexes on the next write.
func (r *Router) noteFastTierIndexDDL(ns, op string) {
	if op != "createIndexes" && op != "dropIndexes" {
		return
	}
	r.staleIndexes(ns)
	// In the background here, unlike the forwarded path: this is the event loop
	// and a listIndexes on it holds up every other change behind this one.
	// Nothing bumps the index generation on this path, so there is no race to
	// lose.
	r.goSafely("re-reading the unique indexes after a change on the fast tier", func() {
		r.refreshIndexes(context.Background(), ns)
	})
	r.forgetHint(ns)
	r.log.Debug("the indexes changed on the fast tier, re-reading them on the next write",
		"ns", ns, "op", op)
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
// Every memory here is one whose staleness is a wrong answer. The unique index
// listing is the awkward one, and it is neither forgotten nor left alone.
//
// Forgetting it is wrong: the router reads the fast tier's unique indexes to
// know which keys it has to protect across the tiers, and a collection that has
// just gone lists none, so re-reading now would replace a listing that is merely
// old with one that is empty. Empty lets a duplicate of an archived document
// through, and the archive still holds every document the constraint covered,
// because a drop is never replayed there.
//
// Leaving it alone was wrong too, which is what used to happen here. A drop
// followed by a rebuild out of band -- resetting an environment -- puts a
// collection under the name that does not have the old one's unique index, and
// the router went on refusing writes on its behalf for a minute, with a
// duplicate key error naming an index that exists on neither tier. The fuzzer
// found it after four rounds, because its own reseed is exactly that shape.
//
// staleIndexes is neither: the old listing stands until the fast tier has
// something truthful to replace it with, and the first write after the
// collection is back reads it.
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
	r.staleIndexes(ns)
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

// judgedDocMax is the largest inserted document the router's change stream is
// sent to judge against cached answers. See followChanges.
const judgedDocMax = 1 << 20

// eventUpdateDesc is the part of an update event that names what changed,
// besides the updated fields' names, which come projected as updatedKeys.
type eventUpdateDesc struct {
	RemovedFields   []string `bson:"removedFields"`
	TruncatedArrays []struct {
		Field string `bson:"field"`
	} `bson:"truncatedArrays"`
}

// eventWrite describes a change event for the cache. An insert without its
// document, an update without its fields, and every other event are writes of
// unknown effect.
func eventWrite(op string, doc bson.Raw, updated []string, ud *eventUpdateDesc) cache.Write {
	switch op {
	case "insert":
		if len(doc) == 0 {
			return cache.AnyWrite
		}
		return cache.Write{Kind: cache.WriteInsert, Inserted: []bson.Raw{doc}}
	case "update":
		// Nil, not empty: an update that set nothing still names its removed
		// fields, and a stream without the projection names nothing at all.
		if updated == nil || ud == nil {
			return cache.AnyWrite
		}
		w := cache.Write{Kind: cache.WriteUpdate}
		w.Fields = append(w.Fields, updated...)
		w.Fields = append(w.Fields, ud.RemovedFields...)
		for _, t := range ud.TruncatedArrays {
			w.Fields = append(w.Fields, t.Field)
		}
		return w
	}
	return cache.AnyWrite
}

// invalidateFor drops what was cached for one collection that w could change.
func (r *Router) invalidateFor(ns, op string, w cache.Write) {
	if r.cache == nil {
		return
	}
	if n := r.cache.InvalidateWrite(ns, w); n > 0 {
		r.log.Debug("a change on the fast tier dropped cached answers",
			"ns", ns, "entries", n, "event", op)
	}
}
