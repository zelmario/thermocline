package router

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

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

// bracketProbeTimeout bounds the one question above. See sortSafeAges.
const bracketProbeTimeout = 2 * time.Second

// bracketCache remembers, per namespace, whether the archive holds a document
// whose age field sorts above everything the claim can speak for.
type bracketCache struct {
	mu    sync.RWMutex
	byNS  map[string]bool
	asked map[string]bool
	// at is when each answer was given, which decides whether a yes still
	// stands. See bracketRecheckSafe.
	at map[string]time.Time

	// collated remembers, per namespace and index, whether the archive can
	// answer a collated key lookup from an index. See collatedkeys.go.
	collated map[string]collatedAnswer
	// collatedWarned keeps the warning about an index that cannot be enforced
	// to once per namespace and index, now that the answer itself is asked
	// again from time to time.
	collatedWarned map[string]bool
}

// collatedAnswer is one remembered answer about the archive's indexes, with the
// moment it was given.
type collatedAnswer struct {
	usable bool
	asked  time.Time
}

// How long that answer is trusted, and it depends on which answer it is.
//
// It used to be remembered for the life of the router, and the answer decides
// whether a collated unique index is enforced across tiers. When it cannot be,
// the router says so and names the remedy: create the same index on the
// archive, with the same collation. An operator who did exactly that changed
// nothing, because the router never asked again, so the under-enforcement the
// warning describes carried on until somebody restarted it. Nothing said so.
//
// So a no is re-asked quickly: it is the answer with a remedy, and the remedy
// has to take effect while the operator is still watching. A yes is re-asked
// slowly, because the only thing that can make it false is somebody dropping
// the index off the archive, and until then re-asking buys nothing. Either way
// it is one listIndexes, amortised over every insert in the interval.
const (
	collatedRecheckNo  = 10 * time.Second
	collatedRecheckYes = 5 * time.Minute
)

// good reports how long this answer stands for.
func (a collatedAnswer) good() bool {
	if a.usable {
		return time.Since(a.asked) < collatedRecheckYes
	}
	return time.Since(a.asked) < collatedRecheckNo
}

// bracketRecheckSafe is how long a yes stands for.
//
// Only a yes. The two answers are not the same kind of thing and they do not go
// stale the same way.
//
// Yes means the archive holds nothing whose age field sorts above a date, so the
// fast tier's own newest-first answer is a proof. The next write can make that
// untrue: one document with a string where a date belongs sorts above every date
// there is, and the proof then misses it. Measured on six dated documents and one
// string: the archive answers with the string document first, the router that had
// already asked answered without it, and a router started a moment later
// answered correctly. Stale in the direction of a wrong answer, and it never
// expired, because the answer was remembered for the life of the process.
//
// No is the conservative direction: the optimisation does not apply and the
// query takes the ordinary path through the archive, which is always right. It
// is kept, and so is the timeout that produces one, because re-asking costs the
// collection scan the timeout exists to stop paying. The remedy for a no is to
// fix the data or index the field, and the action the warning names is to
// restart the router.
//
// A minute, which is what this codebase already pays per namespace for the
// other question of this shape: a unique index created on one router is not
// enforced by the others for up to indexRefresh. The probe is bounded at
// bracketProbeTimeout and only a namespace that answered yes inside it is asked
// again, so the ones this costs anything on are the ones where it is cheap.
//
// The window is real and is written down: for up to a minute after the first
// document of an odd type reaches the archive, a newest-first query on that
// namespace can still be answered from the fast tier alone.
const bracketRecheckSafe = time.Minute

// sortSafeAges reports whether a sort on ns's age field can be trusted to put
// every document the claim ignores below the ones it covers.
//
// It fails closed. An unreachable archive answers nothing, and answering
// nothing means the optimisation does not apply and the query takes the
// ordinary path through the archive, which is where it went before.
func (r *Router) sortSafeAges(ctx context.Context, ns, ageField string) bool {
	r.brackets.mu.RLock()
	safe, asked, at := r.brackets.byNS[ns], r.brackets.asked[ns], r.brackets.at[ns]
	r.brackets.mu.RUnlock()
	if asked && (!safe || time.Since(at) < bracketRecheckSafe) {
		return safe
	}

	field, types := ageField, aboveDate
	byID := field == ""
	if byID {
		field, types = "_id", aboveObjectID
	}
	db, coll := splitNS(ns)
	// Bounded, because an archive the feeder built has only _id_ and this
	// question is asked of another field, which makes it a collection scan.
	// It is asked once per namespace, on the path of a client's query, so a
	// large archive without that index must not turn the first newest-first
	// query of a router's life into a minute of waiting. Running out of time
	// is answered the same way an error is: the optimisation does not apply.
	probeCtx, cancel := context.WithTimeout(ctx, bracketProbeTimeout)
	defer cancel()
	// Two things make a sort untrustworthy here, and only the first was asked
	// about.
	//
	// A type above the claim's own bracket, which is what the rest of this file
	// is about. And an array, which is not above anything: MongoDB sorts an
	// array field by its extreme element, its largest for a descending sort, and
	// the claim reasoned about the document rather than about one of its values.
	// So a document holding [2020, 2030] sorts above every date the fast tier
	// has while being, to every predicate the evictor and the warmer write, an
	// old document.
	//
	// Measured on six dated documents and one holding those two dates, in the
	// archive alone: the archive answers 99,6,5 and the router answered 6,5,4,
	// missing the document the sort puts first. Unlike the type case it did not
	// depend on anything remembered, so a router started afterwards answered the
	// same way.
	//
	// An array matches $type "date" through its elements, which is exactly why
	// the first clause never saw it, and $type "array" is the one spelling that
	// matches the array itself. Not asked when the age is the _id timestamp:
	// mongod does not allow an array _id.
	probe := bson.D{{Key: field, Value: bson.D{{Key: "$type", Value: types}}}}
	if !byID {
		probe = bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: field, Value: bson.D{{Key: "$type", Value: types}}}},
			bson.D{{Key: field, Value: bson.D{{Key: "$type", Value: "array"}}}},
		}}}
	}
	probe = append(probe, meta.NotArchivedDeleted())
	err := r.cold.Database(db).Collection(coll).FindOne(probeCtx, probe).Err()

	found := false
	switch {
	case err == nil:
		// One exists, so a sort can put it above the answer.
		safe, found = false, true
	case errors.Is(err, mongo.ErrNoDocuments):
		safe = true
	case errors.Is(err, context.DeadlineExceeded):
		// Remembered, unlike an error: asking again would cost the same scan
		// on the next query and every one after it. The answer stands until
		// the operator indexes the field and restarts the router, which is
		// what the action below asks for.
		safe = false
		r.log.Warn("gave up asking whether the archive ages every document the same way",
			"ns", ns, "field", field, "waited", bracketProbeTimeout.String(),
			"reason", "the archive has no index on "+field+", so the question is a collection scan",
			"effect", "newest-first queries on this namespace consult the archive, which is slower "+
				"and always correct",
			"action", "create an index on "+field+" in the archive, then restart the router")
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
		r.brackets.at = map[string]time.Time{}
	}
	r.brackets.byNS[ns] = safe
	r.brackets.asked[ns] = true
	r.brackets.at[ns] = time.Now()
	r.brackets.mu.Unlock()

	if found {
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
	delete(r.brackets.at, ns)
	// And what is remembered about its indexes, which lives in the same place
	// and was being left behind. The key is the namespace and the index name,
	// so a collection dropped and recreated kept an answer about an index that
	// no longer exists, or about a different index of the same name.
	prefix := ns + "|"
	for key := range r.brackets.collated {
		if strings.HasPrefix(key, prefix) {
			delete(r.brackets.collated, key)
			delete(r.brackets.collatedWarned, key)
		}
	}
	r.brackets.mu.Unlock()
}
