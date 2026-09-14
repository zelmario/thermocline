package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
)

// Declaring a claim by hand is the one operator action that can make every
// unbounded read return short answers, silently, for as long as it stands.
//
// --complete-all says the fast tier holds every document there is. It is right
// when the fast tier is the original cluster, which is the case it was written
// for, and wrong the moment it is typed against the wrong namespace or before a
// warm has finished. Nothing checked it, and the command did not even connect
// to the fast tier, so it could not have.
//
// The archive holds everything, so the check is available and cheap to state:
// count what each tier holds and refuse a claim the numbers contradict. Counting
// is not free on a large collection, which is the price of not finding out from
// an application instead.

// claimCheckBudget bounds the counting by default.
//
// A claim is declared by hand during a rollout, on a collection that may hold
// millions of documents, and counting both tiers is not free. Running out of
// time is not a refusal: it is answered by saying the claim could not be
// checked, which leaves the operator exactly where they were before this
// existed rather than blocking a rollout on a count.
//
// A minute is the default rather than the rule, because "recorded unchecked" is
// the one outcome nobody wants and on a large collection it was the only one
// available. --count-for raises it for an operator willing to wait, which is the
// usual case for the collection big enough to need it.
const claimCheckBudget = time.Minute

// checkClaim reports why a claim must not be recorded, or an empty string when
// the tiers bear it out.
//
// from is the instant being claimed complete from; a zero one means all of
// history. force turns a refusal into a warning, for the case where the
// operator knows something the counts cannot show, such as a warm still running.
func checkClaim(ctx context.Context, hot, cold *mongo.Client, cfg config.Config,
	ns string, from time.Time, budget time.Duration) (string, error) {
	return checkClaimFor(ctx, hot, cold, cfg, ns, from, budget, true)
}

// checkClaimFor is checkClaim with the tense spelled out, because the same
// counting answers two questions. `cline watermark` asks before recording one,
// and the answer is about what recording it would do; `cline preflight` asks
// about a claim that is already standing, and the answer is about what it is
// doing now. The numbers are identical and the sentence is not, and a report
// that says "recording it would" about a claim recorded last week reads as a
// warning about something that has not happened yet.
func checkClaimFor(ctx context.Context, hot, cold *mongo.Client, cfg config.Config,
	ns string, from time.Time, budget time.Duration, declaring bool) (string, error) {
	if budget <= 0 {
		budget = claimCheckBudget
	}

	db, coll := splitNamespace(ns)
	if db == "" || coll == "" {
		return "", fmt.Errorf("--ns must be db.collection, got %q", ns)
	}

	filter := bson.D{}
	window := "everything"
	if !from.IsZero() {
		field, known := cfg.Router.AgeField(ns)
		if !known {
			// No rule says which field this namespace ages by, so there is no
			// window to count. That is a reason to record the claim unchecked,
			// not a reason to refuse it: the claim is still the operator's to
			// make, and refusing here would turn a check into a new way to
			// fail. It is worth saying, because the router cannot read a bound
			// on such a namespace either, so the claim buys less than it looks.
			fmt.Printf("no eviction or warm rule says which field %s ages by, so this claim is "+
				"being recorded unchecked.\nThe router cannot read a lower bound on a namespace "+
				"it cannot age, so a claim about a range in it is not used for much either.\n", ns)
			return "", nil
		}
		if field == "" {
			filter = bson.D{{Key: "_id", Value: bson.D{
				{Key: "$gte", Value: bson.NewObjectIDFromTimestamp(from)}}}}
			window = "documents whose _id timestamp is at or after " + from.Format(time.RFC3339)
		} else {
			filter = bson.D{{Key: field, Value: bson.D{{Key: "$gte", Value: from}}}}
			window = field + " at or after " + from.Format(time.RFC3339)
		}
	}

	counting, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	coldFilter := append(bson.D{meta.NotArchivedDeleted()}, filter...)
	coldN, err := cold.Database(db).Collection(coll).CountDocuments(counting, coldFilter)
	if err != nil {
		return "", unchecked("the archive", ns, err)
	}
	hotN, err := hot.Database(db).Collection(coll).CountDocuments(counting, filter)
	if err != nil {
		return "", unchecked("the fast tier", ns, err)
	}

	// Nothing on either side is not agreement, and it printed as agreement:
	// checkClaim says nothing when the counts bear the claim out, and nothing
	// is also what it said when the window selected no documents at all. The
	// operator reads silence as "checked".
	//
	// It is reachable without anything being wrong. A namespace aged by the _id
	// timestamp whose documents carry integer _ids is the plain case: the window
	// is {_id: {$gte: ObjectId(from)}}, MongoDB brackets it by type, and an
	// integer _id is never in it, so both tiers count nought however much they
	// hold. The same goes for an instant no document reaches.
	//
	// Said rather than refused. The claim may well be right, and the counts
	// simply have nothing to say about it, which is the one thing the operator
	// cannot tell from silence.
	if hotN == 0 && coldN == 0 {
		if from.IsZero() {
			// The whole collection, on both tiers, and nothing in it. The claim
			// is true of an empty collection, so this is a note and not a doubt.
			fmt.Printf("%s is empty on both tiers, so the claim is true of nothing so far.\n", ns)
			return "", nil
		}
		fmt.Printf("neither tier holds a document with %s in %s, so the counts bear out nothing "+
			"and this claim is being recorded on your word.\nA window selects nothing when the "+
			"age field does not hold the type it is compared against: a rule reading the age from "+
			"the _id timestamp, against documents whose _id is not an ObjectId, is the usual "+
			"one.\n", window, ns)
		return "", nil
	}

	if hotN >= coldN {
		return "", nil
	}
	effect := "It has the router answer those reads from the fast tier alone and return " +
		fmt.Sprintf("%d documents where there are %d, with no error.", hotN, coldN)
	if declaring {
		effect = "Recording it would have the router answer those reads from the fast tier alone " +
			fmt.Sprintf("and return %d documents where there are %d, with no error.\n", hotN, coldN) +
			"Warm it first, or pass --force if a warm is running and you mean to claim it anyway"
	}
	return fmt.Sprintf(
		"the fast tier holds %d of %s in %s and the archive holds %d, so it is %d short of the "+
			"claim.\n%s",
		hotN, window, ns, coldN, coldN-hotN, effect), nil
}

// unchecked turns a counting failure into a note rather than a refusal, and
// says which tier could not be counted so the silence is not read as agreement.
func unchecked(tier, ns string, err error) error {
	fmt.Printf("could not count %s's copy of %s, so this claim is being recorded unchecked: %v\n",
		tier, ns, err)
	return nil
}
