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

	filter := bson.D{
		{Key: rule.DateField, Value: bson.D{{Key: "$lt", Value: cutoff}}},
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
