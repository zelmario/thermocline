package router

import (
	"context"
	"errors"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/meta"
)

// A completeness claim is a range predicate, and MongoDB brackets range
// predicates by type: a date bound never matches a value that is not a date.
// So a document whose age field carries a Timestamp is honestly outside the
// claim, and outside every bounded query the claim is used to answer. The
// archive would not return it either, and the two agree.
//
// A sort is not bracketed. It orders across types, and Timestamp, Regex and
// MaxKey all sort above Date. So the one document the claim may ignore is the
// document the sort puts first, and the newest-first proof in newestfirst.go
// hands back an answer missing it. Measured on an adopted collection of seven
// documents: MongoDB answers 99,0,1 and the router answered 0,1,2.
//
// Aging by the _id adds Boolean and Date to that list, because both sort above
// ObjectId.
//
// Only documents the fast tier has never held can do this. A document written
// through the router lands on the fast tier, so the fast tier's own sorted
// answer already contains it wherever it belongs. The dangerous set is what an
// adoption left in the archive and no warm has matched, and that set only ever
// shrinks: warming moves documents out of it, and the evictor cannot add to it
// because it removes only what a date range predicate selects.
//
// That is what makes one question per namespace enough.
var (
	// Types that sort above Date, for a collection aged by a date field.
	aboveDate = bson.A{"timestamp", "regex", "maxKey"}
	// Types that sort above ObjectId, for a collection aged by its _id.
	aboveObjectID = bson.A{"bool", "date", "timestamp", "regex", "maxKey"}
)

// bracketCache remembers, per namespace, whether the archive holds a document
// whose age field sorts above everything the claim can speak for.
type bracketCache struct {
	mu    sync.RWMutex
	byNS  map[string]bool
	asked map[string]bool
}

// sortSafeAges reports whether a sort on ns's age field can be trusted to put
// every document the claim ignores below the ones it covers.
//
// It fails closed. An unreachable archive answers nothing, and answering
// nothing means the optimisation does not apply and the query takes the
// ordinary path through the archive, which is where it went before.
func (r *Router) sortSafeAges(ctx context.Context, ns, ageField string) bool {
	r.brackets.mu.RLock()
	safe, asked := r.brackets.byNS[ns], r.brackets.asked[ns]
	r.brackets.mu.RUnlock()
	if asked {
		return safe
	}

	field, types := ageField, aboveDate
	if field == "" {
		field, types = "_id", aboveObjectID
	}
	db, coll := splitNS(ns)
	err := r.cold.Database(db).Collection(coll).FindOne(ctx, bson.D{
		{Key: field, Value: bson.D{{Key: "$type", Value: types}}},
		meta.NotArchivedDeleted(),
	}).Err()

	switch {
	case err == nil:
		// One exists, so a sort can put it above the answer.
		safe = false
	case errors.Is(err, mongo.ErrNoDocuments):
		safe = true
	default:
		// Do not remember a failure to ask as an answer.
		r.log.Warn("cannot tell whether the archive ages every document the same way",
			"ns", ns, "field", field, "err", err,
			"effect", "a newest-first query is sent to the archive rather than proved from the fast tier, "+
				"which is slower and always correct")
		return false
	}

	r.brackets.mu.Lock()
	if r.brackets.byNS == nil {
		r.brackets.byNS = map[string]bool{}
		r.brackets.asked = map[string]bool{}
	}
	r.brackets.byNS[ns] = safe
	r.brackets.asked[ns] = true
	r.brackets.mu.Unlock()

	if !safe {
		r.log.Info("the archive ages some documents by a value that outranks a date",
			"ns", ns, "field", field,
			"meaning", "a sort orders across types where a range predicate does not, so such a document "+
				"belongs at the top of a newest-first answer and no claim covers it",
			"effect", "newest-first queries on this namespace consult the archive, as they did before")
	}
	return safe
}

// forgetBrackets drops what is remembered about ns, for the DDL that can make
// it untrue: a rename brings another collection's contents under this name.
func (r *Router) forgetBrackets(ns string) {
	r.brackets.mu.Lock()
	delete(r.brackets.byNS, ns)
	delete(r.brackets.asked, ns)
	r.brackets.mu.Unlock()
}
