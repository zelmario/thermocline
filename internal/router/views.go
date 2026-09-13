package router

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/promote"
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
	// joins is the collections one view's own pipeline reads besides the one it
	// is defined on, and unreadable says its pipeline could not be walked.
	//
	// Cached here rather than read where it is wanted, because every read of a
	// view asks this and viewOn has the definition in its hand already. Read
	// separately it was a listCollections per query on the hot path, against a
	// namespace tieredBase had just looked up.
	joins      map[string][]string
	unreadable map[string]bool
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
	var joins []string
	unreadable := false

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
			// What this view reads besides the collection it is on. See
			// viewJoins for why it has to be judged, and viewCache.joins for
			// why it is read here.
			pv, perr := odoc.LookupErr("pipeline")
			if perr != nil {
				// mongod always stores one, empty or not. A view without it is
				// a catalog this code does not understand, and not
				// understanding it is not the same as knowing it joins nothing.
				unreadable = true
				continue
			}
			arr, aok := pv.ArrayOK()
			if !aok {
				unreadable = true
				continue
			}
			vals, verr := arr.Values()
			if verr != nil {
				unreadable = true
				continue
			}
			names, complete := joinedCollections(vals, db, 0)
			joins = append(joins, names...)
			if !complete {
				unreadable = true
			}
		}
	}

	r.viewCache.mu.Lock()
	if r.viewCache.fetched == nil {
		r.viewCache.fetched = map[string]time.Time{}
		r.viewCache.on = map[string]string{}
		r.viewCache.joins = map[string][]string{}
		r.viewCache.unreadable = map[string]bool{}
	}
	r.viewCache.fetched[ns] = time.Now()
	r.viewCache.on[ns] = found
	r.viewCache.joins[ns] = joins
	r.viewCache.unreadable[ns] = unreadable
	r.viewCache.mu.Unlock()

	return found, found != ""
}

// viewJoinsOf is what one view's own pipeline reads besides the collection it is
// defined on, from the cache viewOn fills.
func (r *Router) viewJoinsOf(ctx context.Context, ns string) (joins []string, readable bool) {
	// Fills the cache when it is cold, and its own answer is not wanted here.
	_, _ = r.viewOn(ctx, ns)
	r.viewCache.mu.RLock()
	defer r.viewCache.mu.RUnlock()
	return r.viewCache.joins[ns], !r.viewCache.unreadable[ns]
}

// forgetView drops a remembered view definition.
func (r *Router) forgetView(ns string) {
	r.viewCache.mu.Lock()
	delete(r.viewCache.fetched, ns)
	delete(r.viewCache.on, ns)
	delete(r.viewCache.joins, ns)
	delete(r.viewCache.unreadable, ns)
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
	if s.named == nil {
		s.named = map[string]string{}
	}
	// A worklist rather than one pass, because resolving a view adds names and
	// those names can be views too. A view joined by a joined view is two hops
	// from the query, and one pass appended it and never looked at it: the
	// collection *it* joins was then judged by nobody, which is the same wrong
	// answer one level further out.
	//
	// seen makes a cycle terminate, which the catalog should not contain and a
	// stale cache could still describe, and maxJoinedViewHops bounds the rest.
	seen := map[string]bool{}
	for hop := 0; hop < maxJoinedViewHops; hop++ {
		grew := false
		for i, ns := range s.secondary {
			if r.cfg.Manages(ns) || seen[ns] {
				continue
			}
			base, isView := r.viewBase(ctx, ns)
			if !isView || base == ns {
				seen[ns] = true
				continue
			}
			seen[ns] = true
			s.secondary[i] = base
			s.named[base] = " (joined as the view " + ns + ")"
			// And whatever that view joins in turn, for the same reason a view
			// queried directly has its own joins folded in: the join is in the
			// definition and the completeness check reads the query. See
			// viewJoins.
			deeper, readable := r.viewJoins(ctx, ns)
			if !readable {
				s.scopeUnknown = true
				continue
			}
			for _, d := range deeper {
				s.secondary = append(s.secondary, d)
				s.named[d] = " (joined by the view " + ns + ", itself joined here)"
				grew = true
			}
		}
		if !grew {
			return
		}
	}
	// Deeper than this is a shape nothing can be said about, and saying nothing
	// is what scopeUnknown means.
	s.scopeUnknown = true
}

// maxJoinedViewHops bounds how far resolveJoinedViews follows views that join
// views. mongod resolves a view chain a handful deep and refuses a cycle, so
// this is a guard against a catalog that says otherwise rather than a real
// depth.
const maxJoinedViewHops = 10

// viewJoins is every collection the chain of views under ns reads besides the
// one it is defined on.
//
// A view's own $lookup is invisible to the completeness check, because that
// check reads the client's pipeline and the join is in the definition. The
// consequence was measured and it is a wrong answer, not a short one: a view on
// a tiered collection the fast tier holds in full, joining a second tiered
// collection it does not, was answered from the fast tier alone and the rows
// whose partner had been evicted came back with an empty join array. Two rows
// where the archive joins both, one joined and one not, with ok: 1.
//
// joins() on viewSpec already knew this. It is what makes a joining view refuse
// to be answered from the archive, and the comment there says exactly why the
// completeness machinery cannot see the join. What nobody had asked is the same
// question of the *fast tier* path, which is reached whenever the collection the
// view is defined on is complete, and that is the ordinary case for a reporting
// view over recent history.
//
// ok is false when a definition or a stage could not be read, which is the same
// answer as a scope walk that did not reach the bottom: the caller refuses
// rather than judging the read against a list known to be short.
func (r *Router) viewJoins(ctx context.Context, ns string) (joined []string, ok bool) {
	at := ns
	seen := map[string]bool{}
	for hop := 0; hop < maxViewHops; hop++ {
		if seen[at] {
			// A cycle mongod would not have allowed, so nothing can be said
			// about what is at the bottom of this chain.
			return joined, false
		}
		seen[at] = true
		on, isView := r.viewOn(ctx, at)
		if !isView {
			// The chain has reached a collection.
			return joined, true
		}
		names, readable := r.viewJoinsOf(ctx, at)
		joined = append(joined, names...)
		if !readable {
			return joined, false
		}
		at = on
	}
	// A chain longer than mongod itself resolves.
	return joined, false
}

// incompleteViewJoin names a tiered collection a view joins that the fast tier
// does not hold in full, or empty when there is no such thing.
//
// A join reads by key rather than by time, so nothing less than the whole
// collection makes it whole: the same rule checkAggregate applies to a $lookup
// the client wrote, applied to one the view was defined with.
func (r *Router) incompleteViewJoin(ctx context.Context, view string) (string, bool) {
	joined, ok := r.viewJoins(ctx, view)
	if !ok {
		return "a collection it joins, which could not be established", true
	}
	for _, ns := range joined {
		if !r.cfg.Manages(ns) {
			// Untiered, so the fast tier is the whole of it.
			continue
		}
		mark, known := r.completenessForRead(ctx, ns)
		if !known || !mark.CoversAll() {
			return ns, true
		}
	}
	return "", false
}

// isViewNS reports whether ns is a view, for the callers that have already asked
// tieredBase and cannot use its answer.
//
// tieredBase reports viaView only when the collection the view reads is itself
// tiered, because that is the only thing the rest of it is about. A view over an
// untiered reference collection is not a view as far as that answer goes, and it
// can still join the tiered history: the same query with the tiers the other way
// up, and the shape reporting most often takes. No extra round trip either way:
// tieredBase has already filled the view cache for anything it said no to.
func (r *Router) isViewNS(ctx context.Context, ns string, viaView, tiered bool) bool {
	if viaView {
		return true
	}
	if tiered {
		// tieredBase returns before asking, because a managed namespace judges
		// itself. Asking here would put a listCollections on the hot path of
		// every read of every tiered collection.
		return false
	}
	_, isView := r.viewBase(ctx, ns)
	return isView
}

// refuseViewJoin turns down a read through a view whose own pipeline joins a
// tiered collection the fast tier does not hold in full.
//
// Its own message rather than refuseView's, which says the collection the view
// reads is the incomplete one. Here that collection may be complete, or untiered
// and complete by definition, and the trouble is somewhere the query cannot see.
func (r *Router) refuseViewJoin(requestID, responseTo int32, view, which string) (*wire.Message, error) {
	return r.commandError(requestID, responseTo,
		"thermocline: refusing this query on the view "+view+" because the view's own pipeline joins "+
			which+", which is tiered and the fast tier does not hold in full. A join reads by key "+
			"rather than by time, so nothing less than the whole collection makes it whole, and this "+
			"join is in the view's definition where nothing in the query can bound it. Warm "+which+
			", or run the pipeline against the archive directly.")
}

// refuseViewJoinIfShort refuses a read through view when the view's own pipeline
// joins a tiered collection the fast tier does not hold in full.
//
// One place for both read paths. They are judged at opposite ends of the command
// and this question is the same at either end.
func (c *conn) refuseViewJoinIfShort(ctx context.Context, view string,
	msg *wire.Message, reply *wire.Message) (*wire.Message, bool) {
	which, bad := c.r.incompleteViewJoin(ctx, view)
	if !bad {
		return nil, false
	}
	c.r.Stats.Incomplete.Add(1)
	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a read through a view that joins an incomplete collection",
		"view", view, "joins", which,
		"why", "the completeness check reads the client's pipeline, and this join is in the "+
			"view's definition, so the rows whose partner is only in the archive would come "+
			"back with an empty join array and no error on them")
	// A reply is in hand on the find path and not on the aggregation's, which is
	// judged before it runs. The response id is the request's either way.
	responseTo := msg.Header.RequestID
	requestID := responseTo
	if reply != nil {
		requestID = reply.Header.RequestID
	}
	out, err := c.r.refuseViewJoin(requestID, responseTo, view, which)
	if err != nil {
		return nil, false
	}
	return out, true
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
// archiveCanAnswerView reports whether the archive holds this view defined
// exactly as the fast tier holds it.
//
// A view read used to be refused whenever the collection underneath it was
// incomplete, on the grounds that the archive could not be asked: it holds the
// documents and not the definition. A view created through the router is
// mirrored down now, so the archive can hold both, and then the read has a
// correct answer instead of an error.
//
// Exactly as, and nothing less. A view whose pipeline differs between the tiers
// answers a different question, and answering the archive's version of it would
// be the silent difference this whole system exists to avoid. Two definitions
// count as the same when the collection read, the pipeline and the collation all
// match; anything else, including a view the archive does not have, is a
// refusal.
func (r *Router) archiveCanAnswerView(ctx context.Context, ns string) bool {
	r.viewMatch.mu.RLock()
	at, seen := r.viewMatch.fetched[ns]
	cached := r.viewMatch.same[ns]
	r.viewMatch.mu.RUnlock()
	if seen && time.Since(at) < viewTTL {
		return cached
	}

	same := r.viewChainMatches(ctx, ns)

	r.viewMatch.mu.Lock()
	if r.viewMatch.same == nil {
		r.viewMatch.same = map[string]bool{}
		r.viewMatch.fetched = map[string]time.Time{}
	}
	r.viewMatch.same[ns] = same
	r.viewMatch.fetched[ns] = time.Now()
	r.viewMatch.mu.Unlock()
	return same
}

// viewChainMatches compares every link of a view chain, not only the view the
// query named.
//
// A view may read another view. Comparing the named one alone would pass a chain
// whose second link differs, and the second link is where the pipeline that
// decides the answer might live. So this follows viewOn down until it reaches a
// collection, comparing each definition as it goes, and stops at a depth mongod
// itself would refuse to create.
func (r *Router) viewChainMatches(ctx context.Context, ns string) bool {
	db, _ := splitNS(ns)
	at := ns
	for hop := 0; hop < maxViewHops; hop++ {
		hot, hotOK := viewDefinition(ctx, r.hot, at)
		if !hotOK {
			// Not a view on the fast tier: the chain has reached a collection,
			// and the archive holds every document of a tiered one.
			return hop > 0
		}
		cold, coldOK := viewDefinition(ctx, r.cold, at)
		if !coldOK || !hot.equals(cold) || hot.joins() {
			return false
		}
		at = db + "." + hot.on
	}
	return false
}

// maxViewHops bounds the chain this will follow. MongoDB refuses to resolve a
// view more than a handful deep, and a loop cannot be created at all, so this is
// a guard against a catalog that says otherwise rather than a real depth.
const maxViewHops = 10

// viewMatchCache remembers whether the archive's copy of a view matches the fast
// tier's. Creating or dropping a view is rare and the router sees the DDL, the
// same reasoning viewTTL rests on.
type viewMatchCache struct {
	mu      sync.RWMutex
	fetched map[string]time.Time
	same    map[string]bool
}

// viewSpec is what makes a view the question it is.
type viewSpec struct {
	on        string
	pipeline  []byte
	collation []byte
}

// joins reports whether the view's own pipeline reads another collection.
//
// Such a view cannot be answered from the archive, whatever its definition says,
// and the reason is what the archive holds rather than what the view is. The
// feeder mirrors the namespaces it is told to tier and nothing else, so an
// untiered collection's documents never reach the archive: its copy there is
// empty, or is whatever the cluster held on the day it was adopted. A $lookup
// against that returns no rows, which is a wrong answer rather than a refusal,
// and the completeness machinery cannot see it because the join is inside the
// definition and not in the query.
//
// So this stays a refusal, and the refusal is the one that was there before.
func (v viewSpec) joins() bool {
	arr, ok := bson.Raw(v.pipeline).Values()
	if ok != nil {
		// A pipeline that cannot be read is one whose stages are unknown, and
		// an unknown stage is assumed to be the dangerous kind.
		return true
	}
	return stagesJoin(arr)
}

// stagesJoin walks a pipeline for a stage that names another collection, and
// into the one stage that carries pipelines of its own.
func stagesJoin(stages []bson.RawValue) bool {
	for _, st := range stages {
		doc, ok := st.DocumentOK()
		if !ok {
			return true
		}
		elems, err := doc.Elements()
		if err != nil {
			return true
		}
		for _, e := range elems {
			switch e.Key() {
			case "$lookup", "$graphLookup", "$unionWith", "$out", "$merge":
				return true
			case "$facet":
				sub, sok := e.Value().DocumentOK()
				if !sok {
					return true
				}
				fields, ferr := sub.Elements()
				if ferr != nil {
					return true
				}
				for _, f := range fields {
					inner, ierr := f.Value().Array().Values()
					if ierr != nil {
						return true
					}
					if stagesJoin(inner) {
						return true
					}
				}
			}
		}
	}
	return false
}

func (v viewSpec) equals(o viewSpec) bool {
	return v.on == o.on &&
		bytes.Equal(v.pipeline, o.pipeline) &&
		bytes.Equal(v.collation, o.collation)
}

// viewDefinition reads one tier's definition of a view, and reports false when
// the namespace is not a view there at all.
func viewDefinition(ctx context.Context, client *mongo.Client, ns string) (viewSpec, bool) {
	db, coll := splitNS(ns)
	cur, err := client.Database(db).ListCollections(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		return viewSpec{}, false
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	if !cur.Next(ctx) {
		return viewSpec{}, false
	}
	var raw bson.Raw
	if err := cur.Decode(&raw); err != nil {
		return viewSpec{}, false
	}
	if kind, _ := raw.Lookup("type").StringValueOK(); kind != "view" {
		return viewSpec{}, false
	}
	opts, ok := raw.Lookup("options").DocumentOK()
	if !ok {
		return viewSpec{}, false
	}
	on, ok := opts.Lookup("viewOn").StringValueOK()
	if !ok || on == "" {
		return viewSpec{}, false
	}
	spec := viewSpec{on: on}
	if v, err := opts.LookupErr("pipeline"); err == nil {
		spec.pipeline = append([]byte(nil), v.Value...)
	}
	if v, err := opts.LookupErr("collation"); err == nil {
		spec.collation = append([]byte(nil), v.Value...)
	}
	return spec, true
}

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

// dropArchiveView removes the archive's copy of a view the fast tier no longer
// has.
//
// Collection DDL other than rename is logged and never replayed, and that rule
// is about documents: replaying a drop into the archive would destroy the
// history it exists to keep. A view has no documents, so none of that applies to
// one, and leaving it costs three things. The startup report names it for ever,
// and that report is what step 1 of the runbook tells an operator to read. A
// rollback repoints the application at the archive and finds a view it dropped.
// And recreating the view with a different pipeline leaves the archive holding
// the old one, so archiveCanAnswerView says no and every read the fast tier
// cannot answer is refused where it could have been answered.
//
// Only when the archive's own copy is a view. That is the whole guard, and it is
// what makes this safe: a collection is never touched by it, whatever the fast
// tier had under that name.
//
// Best effort, and said out loud when it fails. A view left behind is untidy and
// eventually confusing; failing the client's drop over it would be worse, and
// the fast tier has already done the part that matters.
func (r *Router) dropArchiveView(ctx context.Context, ns string) {
	// The same reach as the mirroring half, and no more. mirrorViewsToArchive
	// carries every view in a database that holds something this router
	// manages, so those are the views the archive has because of us and those
	// are the ones to take back. A database this router manages nothing in is
	// the adopted cluster's own, and nothing else here touches one.
	db, coll := splitNS(ns)
	if !r.managesDatabase(db) {
		return
	}
	if _, isView := viewDefinition(ctx, r.cold, ns); !isView {
		return
	}
	if derr := r.cold.Database(db).Collection(coll).Drop(ctx); derr != nil {
		r.log.Warn("could not drop the archive's copy of a view the fast tier no longer has",
			"ns", ns, "err", derr,
			"effect", "the startup report will go on naming it, and a rollback would find a view "+
				"the application dropped",
			"action", "drop "+ns+" on the archive by hand")
		return
	}
	r.log.Info("dropped the archive's copy of a view", "ns", ns,
		"why", "a view holds no documents, so the rule that a drop is never replayed does not "+
			"reach it, and a stale one is named by every startup report and found by a rollback")
}

// managesDatabase reports whether this router manages any namespace in db,
// which is the reach mirrorViewsToArchive has through viewDatabases. An empty
// managed list means every namespace, and so every database.
func (r *Router) managesDatabase(db string) bool {
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

// mirrorViewsToArchive gives the archive every view the fast tier has that it
// does not, once, at startup.
//
// A view created through the router is mirrored down as it is created. Two kinds
// are not: one that existed before that was true, and one an operator created
// straight on the fast tier. Both matter for the same two reasons. Repointing the
// application at the archive is what rollback is, and a view the archive has
// never heard of is an error rather than a wrong answer there. And a read through
// a view the fast tier cannot answer goes to the archive only when the archive
// holds the same view, so a view missing there is refused where it could have
// been answered.
//
// It creates and never changes: a view the archive already has is left exactly as
// it is, because the archive's definition may be the one somebody meant and this
// has no way to tell. A difference is reported by the read path instead, as a
// refusal naming the view.
// dryRun names what it would give the archive and gives it nothing, for
// cline preflight: creating a view on the archive is a write to the tier a
// rollback reads from, and an operator should see the list before a start makes
// it rather than in the log afterwards. One function with a flag, so which
// views are chosen has a single answer.
func (r *Router) mirrorViewsToArchive(ctx context.Context, dryRun bool) []string {
	dbs, err := r.viewDatabases(ctx)
	if err != nil {
		r.log.Debug("could not list the fast tier's databases to compare its views", "err", err)
		return nil
	}
	var carried []string
	for _, db := range dbs {
		if isSystemDatabase(db) {
			continue
		}
		hotViews, herr := r.hot.Database(db).ListCollectionNames(ctx,
			bson.D{{Key: "type", Value: "view"}})
		if herr != nil || len(hotViews) == 0 {
			continue
		}
		coldNames, cerr := r.cold.Database(db).ListCollectionNames(ctx, bson.D{})
		if cerr != nil {
			continue
		}
		have := map[string]bool{}
		for _, n := range coldNames {
			have[n] = true
		}
		for _, name := range hotViews {
			if have[name] || strings.HasPrefix(name, "system.") {
				continue
			}
			if !dryRun {
				promote.MirrorCollection(ctx, r.hot, r.cold, db, name, r.log)
			}
			carried = append(carried, db+"."+name)
		}
	}
	sort.Strings(carried)
	if dryRun {
		return carried
	}
	if len(carried) > 0 {
		r.log.Info("gave the archive views it did not have",
			"views", carried,
			"why", "repointing the application at the archive is what rollback is, and a view read "+
				"the fast tier cannot answer goes there only when the archive holds the same view")
	}
	return carried
}

// viewDatabases is where this router will look for views to give the archive.
//
// The databases this deployment tiers something in, not every database the fast
// tier happens to hold. A router creating collections in the archive for a
// database it knows nothing about is wrong twice over: it is not its business,
// and on a cluster shared by two deployments, or by three test workers, each one
// would be writing into the others' namespaces. Measured: one worker's router
// gave the archive another worker's view, and a suite that asserts a view is
// fast-tier-only then found it answered.
//
// An empty feeder.namespaces means everything is tiered, and then every database
// is this deployment's.
func (r *Router) viewDatabases(ctx context.Context) ([]string, error) {
	if len(r.cfg.Managed) == 0 {
		names, err := r.hot.ListDatabaseNames(ctx, bson.D{})
		return names, err
	}
	seen := map[string]bool{}
	var out []string
	for _, ns := range r.cfg.Managed {
		db, _ := splitNS(ns)
		if db == "" || seen[db] {
			continue
		}
		seen[db] = true
		out = append(out, db)
	}
	return out, nil
}
