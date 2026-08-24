package router

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/watermark"
)

// The commonest question a dashboard asks is "the newest ten", and it used to
// go to the archive every time.
//
// It has no lower time bound, so nothing short of a claim on all of history
// covered it, and a fast tier holding the last thirty days was sent to the
// archive to be told which ten orders are the newest. The fast tier is the one
// place that certainly holds them.
//
// The proof is in its own answer. A completeness claim says the fast tier holds
// every live document newer than T. So if it returns the N asked for, sorted
// newest first, and the last of those N is itself newer than T, then every
// document it did not return is older than T, and therefore older than all N.
// Nothing in the archive can displace any of them.
//
// The filter does not matter, which is what makes this worth having. Whatever
// it selects, the fast tier holds every matching document newer than T, so a
// matching document it did not return is older than the N it did.
//
// Two things are required and neither is a judgement call: the sort must lead
// with the field the collection ages by, descending, because that is the only
// order in which "the last one returned" bounds the rest; and the whole answer
// must be in the batch, because the proof is the last document of the N and a
// partial batch does not contain it.
//
// A skip is safe and does not need saying in the command, though it is worth
// saying here. It drops documents off the front, so "everything not returned is
// older than all N" stops being true of the skipped ones. The claim is what
// carries the proof either way: a live document the fast tier does not hold is
// not newer than the claim, and the last one returned is, so nothing in the
// archive can enter the page. The boundaries line up exactly for this, because
// the evictor removes strictly older than its cutoff and claims from that same
// instant, and the warmer warms from its floor and claims that floor.
//
// What it cannot survive is a value the sort ranks above the claim's own
// bracket. See agebracket.go.

// provenNewestFirst reports whether the fast tier's own reply proves it
// answered a newest-first query completely.
func provenNewestFirst(cmd, reply bson.Raw, ageField string, known bool, mark watermark.Mark) bool {
	if !known || !mark.Claimed || mark.From.IsZero() {
		// No claim, or a claim on all of history, which is already handled and
		// needs none of this.
		return false
	}

	sortDoc, hasSort := cmd.Lookup("sort").DocumentOK()
	if !hasSort || !sortsNewestFirst(sortDoc, ageField) {
		return false
	}
	limit, ok := positiveInt(cmd.Lookup("limit"))
	if !ok {
		// No limit means every matching document, and the archive holds ones
		// this cannot speak for.
		return false
	}
	return provenBatch(reply, limit, ageField, mark)
}

// provenBatch is the proof itself, once the sort and the limit have been read
// off whichever kind of command carried them. A find carries them as fields; an
// aggregation carries them as stages. See aggnewest.go.
func provenBatch(reply bson.Raw, limit int, ageField string, mark watermark.Mark) bool {
	batch, ok := firstBatch(reply)
	if !ok || len(batch) != limit {
		// Fewer than asked for means the fast tier ran out, which proves
		// nothing about what the archive holds. More is not possible. A batch
		// smaller than the limit because of batchSize is the same case: the
		// document that carries the proof is not here.
		return false
	}

	last, ok := batch[len(batch)-1].DocumentOK()
	if !ok {
		return false
	}
	at, ok := documentAge(last, ageField)
	if !ok {
		return false
	}
	// Newer than the claim, so everything not returned is older than it.
	return !at.Before(mark.From)
}

// sortsNewestFirst reports whether a command's sort leads with the field the
// collection ages by, descending.
//
// Leads with, rather than consists of. A later key only breaks ties, and the
// documents that tie are all newer than the claim too, so they are all here.
func sortsNewestFirst(sortDoc bson.Raw, ageField string) bool {
	elems, err := sortDoc.Elements()
	if err != nil || len(elems) == 0 {
		return false
	}
	want := ageField
	if want == "" {
		// A rule with no date_field ages a document by the timestamp inside its
		// ObjectId _id, and _id order is that order.
		want = "_id"
	}
	if elems[0].Key() != want {
		return false
	}
	n, ok := intOf(elems[0].Value())
	return ok && n < 0
}

// documentAge reads the instant a document is aged by.
func documentAge(doc bson.Raw, ageField string) (time.Time, bool) {
	if ageField == "" {
		v, err := doc.LookupErr("_id")
		if err != nil {
			return time.Time{}, false
		}
		oid, ok := v.ObjectIDOK()
		if !ok {
			// Not an ObjectId, so it carries no timestamp and this rule cannot
			// age it. Both components refuse such a namespace; this is the same
			// judgement made where a wrong answer would otherwise be produced.
			return time.Time{}, false
		}
		return oid.Timestamp().UTC(), true
	}
	v, err := doc.LookupErr(ageField)
	if err != nil {
		return time.Time{}, false
	}
	t, ok := v.TimeOK()
	if !ok {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// positiveInt reads a limit, which a driver may send as any integer type.
func positiveInt(v bson.RawValue) (int, bool) {
	n, ok := intOf(v)
	if !ok || n <= 0 {
		return 0, false
	}
	return n, true
}
