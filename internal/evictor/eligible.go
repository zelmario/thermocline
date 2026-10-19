package evictor

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
)

// candidate is one document the age rule selected, before the archive has been
// consulted about it.
type candidate struct {
	id    bson.RawValue
	docID string
	// doc is the fast tier's copy, read at eviction time. See evict: it is
	// what the archive's copy has to equal, and what the delete insists on.
	doc bson.Raw
	// announcedAs names the step that announced this candidate's eviction, so
	// that step can take back exactly the announcements it made. See
	// meta.AnnounceEviction.
	announcedAs string
}

// candidateFilter builds the query that selects removable documents.
//
// Two shapes, and the difference matters more than it looks. With no date field
// configured the age comes from the timestamp inside a default ObjectId, so the
// predicate is a plain _id range that the _id index serves in order, for free.
// With a date field the scan needs a compound index on {dateField, _id} or it
// degrades into a collection scan on every pass.
func candidateFilter(rule config.EvictionRule, cutoff time.Time, after bson.RawValue) (bson.D, error) {
	if rule.UsesObjectIDTime() {
		idRange := bson.D{{Key: "$lt", Value: bson.NewObjectIDFromTimestamp(cutoff)}}
		if !after.IsZero() {
			idRange = append(idRange, bson.E{Key: "$gt", Value: after})
		}
		return bson.D{{Key: "_id", Value: idRange}}, nil
	}

	if rule.DateField == "_id" {
		return nil, fmt.Errorf("rule %q: leave date_field empty to use the _id timestamp", rule.NS)
	}

	// Not an array, and this is the one predicate here that is about
	// correctness rather than about which index serves the scan.
	//
	// A completeness claim says the fast tier holds every live document newer
	// than the cutoff, and this filter says which documents are older than it.
	// Both read one date out of a document, and MongoDB does not promise there
	// is only one: a range predicate on an array field matches when *any*
	// element matches. So a document whose age field holds [old, new] is older
	// than the cutoff and newer than it at the same time. It was removed here
	// for the old element, and a query bounded at the claim still matched it for
	// the new one.
	//
	// Measured on four documents, three ordinary and one holding
	// [four hours ago, thirty seconds ago]: the archive answered 1,2,3,9 for a
	// read bounded at the claim and the router answered 1,2,3. A short answer
	// inside the range the claim promises, with no error on it.
	//
	// Type bracketing keeps every other shape out on its own: a cutoff on a date
	// selects only documents whose field holds a date, so a missing, null or
	// string age is invisible to this filter already. An array of dates is the
	// one thing bracketing lets through, because $type matches an element.
	// Leaving them where they are is the same answer this makes for those:
	// warnAboutUnaged counts them and says so.
	filter := bson.D{
		{Key: rule.DateField, Value: bson.D{
			{Key: "$lt", Value: cutoff},
			{Key: "$not", Value: bson.D{{Key: "$type", Value: "array"}}},
		}},
	}
	if !after.IsZero() {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$gt", Value: after}}})
	}
	return filter, nil
}

// outcome counts what happened to one batch of candidates.
type outcome struct {
	evict       []candidate
	held        int
	unconfirmed int
}

// partition applies the two vetoes that stand between a candidate and removal.
//
// A residency hold means the document was read or promoted recently, so age
// alone is not enough to remove it. That veto is what makes the fast tier a
// working set rather than a date window.
//
// A missing archive copy is the harder stop. Removing a document the archive
// never received destroys it, so anything unconfirmed stays put no matter how
// old it is.
func partition(cands []candidate, held, archived map[string]bool) outcome {
	var out outcome
	for _, c := range cands {
		switch {
		case held[c.docID]:
			out.held++
		case !archived[c.docID]:
			out.unconfirmed++
		default:
			out.evict = append(out.evict, c)
		}
	}
	return out
}

// cutoff returns the instant a document must predate to be a candidate.
func cutoff(now time.Time, olderThan time.Duration) time.Time {
	return now.Add(-olderThan).UTC()
}
