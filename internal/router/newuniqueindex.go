package router

import (
	"context"
	"sync"
	"time"

	"github.com/zelmario/thermocline/internal/wire"
)

// A unique index created on a tiered collection did not cover the documents
// already in the archive.
//
// Those are the only documents the constraint actually needs help with. The
// fast tier's own index enforces itself over what the fast tier holds; the
// registry exists for the rest, and it is filled in by whatever writes a
// document. A collection that has been running for a year has most of its
// documents in the archive, and none of them had ever been written through a
// router that knew about this index, so none of their keys were claimed.
//
// Measured on an evicted document and a fresh unique index: the archive holds
// held@x.com, the index is created through the router, and an insert of a
// second held@x.com is **accepted**. The same probe with `cline keys --rebuild`
// in between refuses it with 11000. So the constraint was enforced against
// everything except the half it was needed for.
//
// It was written down -- the rollout list says to run `cline keys --rebuild`
// after creating a unique index -- and a documented step is not a guarantee.
// Nothing refused, nothing warned, and the duplicate that got in is permanent:
// the archive's copy and the new one both exist, and the fast tier's index
// cannot see the pair.
//
// So the router does it itself, from the one place that knows all three facts
// at once: the createIndexes succeeded, it made a unique index, and the
// namespace is tiered.

// keyBackfill remembers which namespaces are being walked, so two index builds
// in a row do not start two walks of the same collection.
type keyBackfill struct {
	mu sync.Mutex
	on map[string]bool
}

func (b *keyBackfill) start(ns string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.on == nil {
		b.on = map[string]bool{}
	}
	if b.on[ns] {
		return false
	}
	b.on[ns] = true
	return true
}

func (b *keyBackfill) done(ns string) {
	b.mu.Lock()
	delete(b.on, ns)
	b.mu.Unlock()
}

// backfillNewUniqueKeys claims the keys of documents that already exist, after a
// createIndexes that made a unique index on a tiered collection.
//
// In the background and on a context of its own, because it is a walk of the
// archive and the client's createIndexes has already been answered. Waiting for
// it would turn a command that returns in a second into one that returns in
// minutes, and the operator did not ask for that.
//
// The window while it runs is the honest cost: the constraint is enforced over
// the documents already reached and not yet over the rest. It is bounded, it is
// announced at both ends, and it is strictly less than the window it replaces,
// which was for ever.
func (c *conn) backfillNewUniqueKeys(ctx context.Context, ns string, msg, reply *wire.Message) {
	if !c.r.cfg.Manages(ns) {
		return
	}
	// The command, before anything else, because the other two questions cost a
	// round trip each and this one is a parse. An ordinary index build on a
	// collection that happens to have a unique index elsewhere is not a reason
	// to walk the archive, and re-declaring indexes on start-up is something
	// drivers do.
	if !asksForUniqueIndex(msg) {
		return
	}
	if !indexWasBuilt(reply) {
		return
	}
	// After staleIndexes and a re-read, so this is the listing including whatever
	// was just built. Nothing to claim if it did not end up unique after all.
	if len(c.r.uniqueIndexes(ctx, ns)) == 0 {
		return
	}
	if !c.r.backfill.start(ns) {
		c.log.Debug("a key backfill for this collection is already running", "ns", ns)
		return
	}

	log := c.log
	r := c.r
	r.goSafely("claiming existing keys for a new unique index", func() {
		defer r.backfill.done(ns)
		// Its own timeout rather than the client's context, which is gone the
		// moment the reply is sent.
		bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), backfillLimit)
		defer cancel()

		log.Info("a unique index was created on a tiered collection, claiming the keys of the "+
			"documents already there",
			"ns", ns,
			"why", "the fast tier's own index covers what the fast tier holds; the documents in "+
				"the archive are the ones it cannot see, and until their keys are claimed a "+
				"write may duplicate one of them",
			"note", "this walks both tiers and may take a while on a large collection; the "+
				"constraint is enforced over what it has reached and not yet over the rest")
		started := time.Now()
		claimed, conflicts, err := r.RebuildKeys(bg, []string{ns})
		if err != nil {
			log.Warn("could not claim the existing keys for a new unique index",
				"ns", ns, "err", err, "claimed", claimed[ns], "after", time.Since(started).String(),
				"effect", "a write may duplicate a key held by a document whose key was not "+
					"reached, which the fast tier's own index cannot see",
				"action", "run cline keys --rebuild")
			return
		}
		if conflicts[ns] > 0 {
			// Reported, never resolved. Two live documents already sharing a
			// key means the constraint was broken before the index existed, and
			// picking a winner here would hide it. mongod would have refused
			// the build outright; it did not, which means the duplicates are
			// split across the tiers where its own index cannot see them.
			log.Warn("a new unique index has keys held by more than one live document",
				"ns", ns, "conflicts", conflicts[ns], "claimed", claimed[ns],
				"why", "the fast tier built this index because the duplicates are split across "+
					"the tiers, so its own index saw only one of each pair",
				"effect", "the constraint is already broken and this router will not pick a "+
					"winner; the duplicates stay",
				"action", "run cline keys --rebuild to list them, and resolve them before the "+
					"evictor makes the split permanent")
			return
		}
		log.Info("claimed the existing keys for a new unique index",
			"ns", ns, "claimed", claimed[ns], "took", time.Since(started).String())
	})
}

// backfillLimit bounds the walk. A collection this does not finish in an hour
// is one whose operator should be running cline keys --rebuild deliberately,
// with the output in front of them, rather than having a router do it quietly.
const backfillLimit = time.Hour

// asksForUniqueIndex reports whether a createIndexes asks for a unique one.
//
// Through AllOf rather than the command document, because a driver may send the
// index specifications in a payload section rather than inline and both are
// ordinary.
func asksForUniqueIndex(msg *wire.Message) bool {
	if msg == nil {
		return false
	}
	for _, spec := range msg.AllOf("indexes") {
		if u, err := spec.LookupErr("unique"); err == nil {
			if b, ok := u.BooleanOK(); ok && b {
				return true
			}
		}
	}
	return false
}

// indexWasBuilt reports whether a createIndexes reply says it made one.
//
// mongod answers an already-existing index with numIndexesBefore equal to
// numIndexesAfter and note: "all indexes already exist". Walking the archive for
// that would be a scan of the whole collection for nothing, and a driver that
// re-declares its indexes on every start would do it on every start.
func indexWasBuilt(reply *wire.Message) bool {
	if reply == nil {
		return false
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return false
	}
	before, bok := intOf64(rdoc.Lookup("numIndexesBefore"))
	after, aok := intOf64(rdoc.Lookup("numIndexesAfter"))
	if !bok || !aok {
		// Said nothing either way. A walk that was not needed costs a scan; one
		// that was needed and skipped costs a duplicate nothing can see.
		return true
	}
	return after > before
}
