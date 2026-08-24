package router

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A hint names an index, and the archive is expected to carry fewer of them:
// the rollout guide tells operators to drop the ones the cheap tier does not
// need. So the hint is dropped on the way there rather than passed on, which
// for an ordinary index is exactly right. A hint chooses how a query is
// answered, not what the answer is.
//
// A sparse or partial index is not ordinary. It holds only some of the
// collection's documents, so hinting one is also a statement about which
// documents are looked at at all. Measured on PSMDB 8.0, twenty documents of
// which six carry a field, with a sparse index on that field:
//
//	find({})                     -> 20 documents
//	find({}).hint(sparseIndex)   ->  6 documents
//	find({}).hint(partialIndex)  -> only those its filter matches
//
// Dropping that hint therefore answers a different question and reports it as
// success, which is the worst shape of failure this system has.
//
// What is dropped is put back as a filter. The documents a sparse index holds
// are the ones carrying at least one of its fields; the documents a partial
// index holds are the ones its own partialFilterExpression matches. Both are
// predicates the archive can evaluate whether or not it carries the index, so
// the answer comes back the same without requiring the tiers to agree about
// indexes, which is the thing the design deliberately does not require.

// selectiveIndex is an index that holds only part of its collection, and
// therefore one whose hint changes the answer rather than the plan.
type selectiveIndex struct {
	name   string
	key    bson.Raw
	fields []string
	sparse bool
	// partial is the index's own partialFilterExpression, used verbatim. It is
	// a query predicate written by whoever created the index, so the archive
	// evaluates it with the same matcher that decided what to index.
	partial bson.Raw
	// plainKeys says every component of the key is a direction, 1 or -1.
	//
	// It decides whether this index's sparseness can be written down as a
	// filter at all. "At least one of the fields is present" is the rule for a
	// sparse index whose keys are all directions, and it is not the rule for
	// one carrying a 2dsphere or text component: measured on PSMDB 8.0 with
	// {loc: "2dsphere", n: 1} sparse over three documents, hinting it returned
	// the two carrying loc, while the document with n and no loc was left out
	// even though it has one of the fields. Translating that with $or would
	// have put it back, which is the same silent over-answer this whole thing
	// exists to stop.
	plainKeys bool
}

// hintCache remembers the selective indexes of a namespace. Indexes are created
// rarely, and re-listing them on every fallback read would be absurd.
type hintCache struct {
	mu      sync.RWMutex
	byNS    map[string][]selectiveIndex
	fetched map[string]time.Time
}

// forgetHint drops what is remembered about ns's selective indexes, so the next
// fallback read re-lists them.
//
// The twin of forgetIndexes, and it was missing. That one is called wherever
// index DDL passes through, and this one was not, so a router that had just
// taken a dropIndexes went on reproducing the dropped index's filter against
// the archive for up to indexRefresh. An index recreated without its
// partialFilterExpression, or with a different one, is the case that bites: the
// archive is then asked a narrower question than the client asked, and answers
// short. See hintPredicate for why the filter is reproduced at all.
func (r *Router) forgetHint(ns string) {
	r.hintCache.mu.Lock()
	delete(r.hintCache.byNS, ns)
	delete(r.hintCache.fetched, ns)
	r.hintCache.mu.Unlock()
}

// selectiveIndexes lists the sparse and partial indexes on the fast tier's copy
// of a namespace.
//
// The fast tier is the right side to ask. It is the collection the client
// believes it is querying, so its indexes are what the client's hint names.
func (r *Router) selectiveIndexes(ctx context.Context, ns string) []selectiveIndex {
	r.hintCache.mu.RLock()
	at, seen := r.hintCache.fetched[ns]
	cached := r.hintCache.byNS[ns]
	r.hintCache.mu.RUnlock()
	if seen && time.Since(at) < indexRefresh {
		return cached
	}

	db, coll := splitNS(ns)
	found := []selectiveIndex{}
	cur, err := r.hot.Database(db).Collection(coll).Indexes().List(ctx)
	if err == nil {
		defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
		for cur.Next(ctx) {
			var spec struct {
				Name                    string   `bson:"name"`
				Sparse                  bool     `bson:"sparse"`
				Key                     bson.Raw `bson:"key"`
				PartialFilterExpression bson.Raw `bson:"partialFilterExpression"`
			}
			if derr := cur.Decode(&spec); derr != nil {
				continue
			}
			if !spec.Sparse && len(spec.PartialFilterExpression) == 0 {
				continue
			}
			elems, kerr := spec.Key.Elements()
			if kerr != nil {
				continue
			}
			si := selectiveIndex{
				name:      spec.Name,
				key:       spec.Key,
				sparse:    spec.Sparse,
				partial:   spec.PartialFilterExpression,
				plainKeys: true,
			}
			for _, e := range elems {
				si.fields = append(si.fields, e.Key())
			}
			si.plainKeys = allDirections(elems)
			found = append(found, si)
		}
	}

	r.hintCache.mu.Lock()
	if r.hintCache.byNS == nil {
		r.hintCache.byNS = map[string][]selectiveIndex{}
		r.hintCache.fetched = map[string]time.Time{}
	}
	r.hintCache.byNS[ns] = found
	r.hintCache.fetched[ns] = time.Now()
	r.hintCache.mu.Unlock()
	return found
}

// hintIsLoadBearing reports whether the hint has to travel to the archive
// rather than be dropped.
//
// min and max are bounds on an index key, so they are meaningless without the
// index that defines the key, and MongoDB has required a hint alongside them
// since 4.2. Measured on PSMDB 8.0, ten documents indexed on n:
//
//	find({}).min({n:3}).max({n:6}).hint({n:1})  -> 3,4,5
//	the same without the hint                   -> NoQueryExecutionPlans
//
// So dropping the hint here does not cost a plan, it turns a query that worked
// into an error. The hint is kept instead. If the archive does not carry that
// index the caller gets MongoDB's own complaint naming it, which is a true
// statement about a real divergence and one `cline indexes` already reports,
// rather than this proxy inventing a failure of its own.
func hintIsLoadBearing(cmd bson.Raw) bool {
	for _, k := range []string{"min", "max"} {
		if _, err := cmd.LookupErr(k); err == nil {
			return true
		}
	}
	return false
}

// hintPredicate returns the filter a hinted index applies by being hinted, or
// nil when the hint does not narrow anything.
//
// Nil is the answer for every ordinary case: no hint, a hint on an index that
// holds the whole collection, `$natural`, and a hint naming something that is
// not there. The last one matters: a hint the fast tier would have refused must
// keep being refused rather than quietly turning into a filter, and dropping it
// leaves the archive to give the same error the fast tier would.
func (r *Router) hintPredicate(ctx context.Context, ns string, cmd bson.Raw) (bson.D, bool) {
	hv, err := cmd.LookupErr("hint")
	if err != nil {
		return nil, false
	}
	idx, ok := r.findHinted(ctx, ns, hv)
	if !ok {
		return nil, false
	}

	// A sparse index whose key is not all directions does not follow the
	// any-field-present rule, so there is nothing honest to write down. Send
	// the hint instead and let MongoDB be the authority: the archive either
	// carries the index and answers exactly, or says which index it is missing,
	// which is a true statement and one `cline indexes` already reports.
	if idx.sparse && !idx.plainKeys {
		return nil, true
	}

	var terms bson.A
	if idx.sparse {
		// A compound sparse index still indexes a document that carries any one
		// of its fields, so only a document missing all of them is left out.
		// This is the same rule the unique key registry applies; see
		// constrains in uniquekeys.go.
		var present bson.A
		for _, f := range idx.fields {
			present = append(present, bson.D{{Key: f, Value: bson.D{{Key: "$exists", Value: true}}}})
		}
		switch len(present) {
		case 0:
		case 1:
			terms = append(terms, present[0])
		default:
			terms = append(terms, bson.D{{Key: "$or", Value: present}})
		}
	}
	if len(idx.partial) > 0 {
		terms = append(terms, idx.partial)
	}

	switch len(terms) {
	case 0:
		return nil, false
	case 1:
		if d, ok := terms[0].(bson.D); ok {
			return d, false
		}
		return bson.D{{Key: "$and", Value: terms}}, false
	default:
		return bson.D{{Key: "$and", Value: terms}}, false
	}
}

// findHinted resolves a hint to one of the namespace's selective indexes.
//
// A hint is either an index name or a key pattern, and both forms are in use:
// drivers send whichever the application wrote.
func (r *Router) findHinted(ctx context.Context, ns string, hv bson.RawValue) (selectiveIndex, bool) {
	if name, ok := hv.StringValueOK(); ok {
		for _, idx := range r.selectiveIndexes(ctx, ns) {
			if idx.name == name {
				return idx, true
			}
		}
		return selectiveIndex{}, false
	}
	pattern, ok := hv.DocumentOK()
	if !ok {
		return selectiveIndex{}, false
	}
	for _, idx := range r.selectiveIndexes(ctx, ns) {
		if sameKeyPattern(idx.key, pattern) {
			return idx, true
		}
	}
	return selectiveIndex{}, false
}

// sameKeyPattern reports whether two index key patterns name the same index.
//
// Compared field by field rather than byte by byte, because the direction
// arrives as whatever numeric type the driver encoded it with: a shell sending
// 1 and an index stored with 1 can differ in BSON type while meaning the same
// index. Order matters, because {a:1,b:1} and {b:1,a:1} are different indexes.
func sameKeyPattern(a, b bson.Raw) bool {
	ae, err1 := a.Elements()
	be, err2 := b.Elements()
	if err1 != nil || err2 != nil || len(ae) != len(be) {
		return false
	}
	for i := range ae {
		if ae[i].Key() != be[i].Key() {
			return false
		}
		if !sameKeyValue(ae[i].Value(), be[i].Value()) {
			return false
		}
	}
	return true
}

// sameKeyValue compares one field of a key pattern: a direction as any numeric
// type, or a string naming an index kind such as "text" or "2dsphere".
func sameKeyValue(a, b bson.RawValue) bool {
	if an, aok := numberOf(a); aok {
		bn, bok := numberOf(b)
		return bok && an == bn
	}
	as, aok := a.StringValueOK()
	bs, bok := b.StringValueOK()
	return aok && bok && as == bs
}

// numberOf reads any BSON number as a float64, which is enough to tell 1 from
// -1 and from 2dsphere's version numbers.
func numberOf(v bson.RawValue) (float64, bool) {
	switch v.Type {
	case bson.TypeInt32:
		i, ok := v.Int32OK()
		return float64(i), ok
	case bson.TypeInt64:
		i, ok := v.Int64OK()
		return float64(i), ok
	case bson.TypeDouble:
		f, ok := v.DoubleOK()
		return f, ok
	default:
		return 0, false
	}
}

// andPredicate joins a filter with the predicate a dropped hint stood for.
//
// A nil predicate means the hint narrowed nothing, which is every ordinary
// query, and the filter is returned untouched so the common path is unchanged.
func andPredicate(filter bson.D, pred bson.D) bson.D {
	if len(pred) == 0 {
		return filter
	}
	return bson.D{{Key: "$and", Value: bson.A{filter, pred}}}
}

// matchFirst puts that same predicate at the head of a pipeline.
//
// First, because it is not an optimisation but part of the question: a later
// stage may group or project the indexed fields away, and a $match placed after
// one of those would be filtering something else.
func matchFirst(pipeline bson.A, pred bson.D) bson.A {
	if len(pred) == 0 {
		return pipeline
	}
	out := make(bson.A, 0, len(pipeline)+1)
	out = append(out, bson.D{{Key: "$match", Value: pred}})
	return append(out, pipeline...)
}

// allDirections reports whether every component of an index key is a plain
// direction rather than an index kind such as "2dsphere", "text" or "hashed".
//
// Only such an index follows the rule that a sparse index holds every document
// carrying at least one of its fields, which is the rule the translation is
// written from.
func allDirections(key []bson.RawElement) bool {
	for _, e := range key {
		if _, ok := numberOf(e.Value()); !ok {
			return false
		}
	}
	return true
}
