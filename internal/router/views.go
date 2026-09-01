package router

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// Every guard decides whether to act by asking whether the namespace the query
// names is one Thermocline manages. A view breaks that, because the name the
// query gives is the view's, and a view is never in feeder.namespaces: it holds
// no documents of its own, so there is nothing about it to mirror.
//
// So a view over a tiered collection was read with no completeness check at
// all. Measured on a collection holding three of ten documents, the view
// returned three and reported success, which is the silent short answer the
// whole completeness apparatus exists to prevent, reached through a name that
// apparatus did not recognise.
//
// Views are not exotic here either. They are how reporting queries are usually
// packaged, and reporting over history is the workload tiering is for.

// viewTTL is how long a view definition is remembered. Creating or dropping one
// is rare, and the router notices those anyway through the DDL it forwards.
const viewTTL = time.Minute

type viewCache struct {
	mu      sync.RWMutex
	fetched map[string]time.Time
	// on maps a view's namespace to the collection it reads. Empty means the
	// namespace is not a view.
	on map[string]string
}

// viewBase resolves a namespace to the collection its documents actually come
// from, following a chain of views to its end.
//
// It returns the namespace unchanged when it is not a view, so callers can use
// the result without asking which case they are in.
func (r *Router) viewBase(ctx context.Context, ns string) (string, bool) {
	seen := map[string]bool{}
	cur := ns
	isView := false

	// A view may be defined on another view. Ten hops is far more than anyone
	// writes and stops a cycle, which MongoDB rejects at creation but which a
	// stale cache could still appear to describe.
	for i := 0; i < 10; i++ {
		if seen[cur] {
			break
		}
		seen[cur] = true
		on, ok := r.viewOn(ctx, cur)
		if !ok || on == "" {
			break
		}
		isView = true
		cur = on
	}
	return cur, isView
}

// viewOn returns the collection one namespace is a view on, if it is one.
func (r *Router) viewOn(ctx context.Context, ns string) (string, bool) {
	r.viewCache.mu.RLock()
	at, seen := r.viewCache.fetched[ns]
	on, has := r.viewCache.on[ns]
	r.viewCache.mu.RUnlock()
	if seen && time.Since(at) < viewTTL {
		return on, has && on != ""
	}

	db, coll := splitNS(ns)
	found := ""

	cur, err := r.hot.Database(db).ListCollections(ctx, bson.D{{Key: "name", Value: coll}})
	if err == nil {
		defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
		for cur.Next(ctx) {
			var raw bson.Raw
			if cur.Decode(&raw) != nil {
				continue
			}
			if t, _ := lookupString(raw, "type"); t != "view" {
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
			if base, bok := lookupString(odoc, "viewOn"); bok && base != "" {
				found = db + "." + base
			}
		}
	}

	r.viewCache.mu.Lock()
	if r.viewCache.fetched == nil {
		r.viewCache.fetched = map[string]time.Time{}
		r.viewCache.on = map[string]string{}
	}
	r.viewCache.fetched[ns] = time.Now()
	r.viewCache.on[ns] = found
	r.viewCache.mu.Unlock()

	return found, found != ""
}

// forgetView drops a remembered view definition.
func (r *Router) forgetView(ns string) {
	r.viewCache.mu.Lock()
	delete(r.viewCache.fetched, ns)
	delete(r.viewCache.on, ns)
	r.viewCache.mu.Unlock()
}

// tieredBase reports the namespace whose completeness decides whether a query
// on ns can be trusted, and whether ns reached it through a view.
//
// A query on a managed collection judges itself. A query on a view judges the
// collection the view reads. A query on anything else is not this system's
// business.
func (r *Router) tieredBase(ctx context.Context, ns string) (base string, viaView, tiered bool) {
	if r.cfg.Manages(ns) {
		return ns, false, true
	}
	base, isView := r.viewBase(ctx, ns)
	if !isView || !r.cfg.Manages(base) {
		return ns, false, false
	}
	return base, true, true
}

// resolveJoinedViews rewrites the collections an aggregation joins to the
// collections their documents actually come from.
//
// $lookup, $graphLookup and $unionWith all accept a view, and a view is never in
// feeder.namespaces, so a joined view passed the completeness check untested for
// the same reason a queried view once did. The primary namespace was fixed;
// these were not, and a pipeline reaches them by a name the guard did not
// recognise just as easily.
//
// It matters twice. The check reads the wrong watermark, so an incomplete join
// returns a short answer as success. And inside a transaction the scope is
// warmed rather than refused, which would try to copy documents into a view.
func (r *Router) resolveJoinedViews(ctx context.Context, s *aggScope) {
	for i, ns := range s.secondary {
		if r.cfg.Manages(ns) {
			continue
		}
		base, isView := r.viewBase(ctx, ns)
		if !isView || base == ns {
			continue
		}
		if s.named == nil {
			s.named = map[string]string{}
		}
		s.secondary[i] = base
		s.named[base] = " (joined as the view " + ns + ")"
	}
}

// refuseView turns down a read through a view that the fast tier cannot answer
// in full.
//
// The archive cannot answer it either, and that is not a limitation of this
// code: the view exists only on the fast tier, because the feeder mirrors
// documents and never collection definitions. Running the view's own pipeline
// against the archive's copy of the underlying collection is possible and is
// worth doing, but guessing at that translation would risk returning a
// different answer rather than a short one, which would be worse than saying no.
// refuseView says why a view over an incomplete collection is not answered.
//
// It used to say the archive could not answer it because a view exists only on
// the fast tier. That stopped being true when a view created through the router
// began to be mirrored down: the archive can hold the view and every document it
// reads. What is still true is that this router does not send a view read there,
// because the two definitions are only identical for a view it mirrored itself,
// and a view whose pipeline differs between the tiers would answer differently
// without anything saying so. The message says what to do rather than what
// cannot be done.
func (r *Router) refuseView(requestID, responseTo int32, view, base, because string) (*wire.Message, error) {
	return r.commandError(requestID, responseTo,
		"thermocline: refusing this query on the view "+view+" because "+because+
			". The view reads "+base+", which the fast tier does not hold completely, and a view "+
			"read is not sent to the archive. Warm "+base+", or query it directly, where the "+
			"archive answers in full.")
}
