package warmer

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/watermark"
)

// sourceFilter selects the next slice of the archive to copy up.
//
// The walk runs newest first, so `before` moves backwards through time and the
// watermark it produces moves with it. Tombstoned documents are excluded here
// rather than filtered later: they are records the application deleted, and
// copying one into the fast tier would bring it back to life.
func sourceFilter(rule config.WarmRule, floor time.Time, before bson.RawValue) (bson.D, error) {
	filter := bson.D{meta.NotArchivedDeleted()}

	if rule.UsesObjectIDTime() {
		idRange := bson.D{{Key: "$gte", Value: bson.NewObjectIDFromTimestamp(floor)}}
		if !before.IsZero() {
			idRange = append(idRange, bson.E{Key: "$lt", Value: before})
		}
		return append(filter, bson.E{Key: "_id", Value: idRange}), nil
	}

	if rule.DateField == "_id" {
		return nil, fmt.Errorf("rule %q: leave date_field empty to use the _id timestamp", rule.NS)
	}

	filter = append(filter, bson.E{
		Key:   rule.DateField,
		Value: bson.D{{Key: "$gte", Value: floor}},
	})
	if !before.IsZero() {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: before}}})
	}
	return filter, nil
}

// windowFilter selects everything at or after floor, without the tombstone
// exclusion sourceFilter adds. It exists to ask a tier one question: do you
// hold anything at all in this range?
//
// The zero time means the whole of history, and it has to be handled before any
// of it is turned into a bound. A claim on all of history records the zero
// time, and bson.NewObjectIDFromTimestamp of that is 886e0900..., which is
// *larger* than an ObjectID minted in 2025, so `_id >= it` matches nothing.
// Both tiers then count zero, the counts agree, and the check that discards a
// claim the fast tier cannot support concludes it is sound. Hole detection
// stops working at the exact moment the fast tier claims the most.
func windowFilter(rule config.WarmRule, floor time.Time) (bson.D, error) {
	if floor.IsZero() {
		if !rule.UsesObjectIDTime() && rule.DateField == "_id" {
			return nil, fmt.Errorf("rule %q: leave date_field empty to use the _id timestamp", rule.NS)
		}
		return bson.D{}, nil
	}
	if rule.UsesObjectIDTime() {
		return bson.D{{Key: "_id", Value: bson.D{
			{Key: "$gte", Value: bson.NewObjectIDFromTimestamp(floor)},
		}}}, nil
	}
	if rule.DateField == "_id" {
		return nil, fmt.Errorf("rule %q: leave date_field empty to use the _id timestamp", rule.NS)
	}
	return bson.D{{Key: rule.DateField, Value: bson.D{{Key: "$gte", Value: floor}}}}, nil
}

// olderThanFilter selects live archived documents older than floor, which is
// the question "is there anything this warm did not reach".
//
// The tombstone exclusion matters here and does not in windowFilter: a document
// the application deleted is not one the fast tier is missing, and counting it
// would keep a fully warmed tier from ever saying so.
func olderThanFilter(rule config.WarmRule, floor time.Time) (bson.D, error) {
	filter := bson.D{meta.NotArchivedDeleted()}
	if rule.UsesObjectIDTime() {
		return append(filter, bson.E{Key: "_id", Value: bson.D{
			{Key: "$lt", Value: bson.NewObjectIDFromTimestamp(floor)},
		}}), nil
	}
	if rule.DateField == "_id" {
		return nil, fmt.Errorf("rule %q: leave date_field empty to use the _id timestamp", rule.NS)
	}
	return append(filter, bson.E{
		Key:   rule.DateField,
		Value: bson.D{{Key: "$lt", Value: floor}},
	}), nil
}

// watermarkAfter returns the completeness watermark to record once every
// document down to oldest has been copied up.
//
// It deliberately claims one second less than was actually warmed. Documents
// share a timestamp to the second, so stopping partway through a second would
// otherwise claim completeness for a second that is only half filled. Claiming
// slightly too little costs one redundant archive lookup; claiming slightly too
// much returns short results and calls them complete.
func watermarkAfter(oldest time.Time) time.Time {
	return oldest.Add(time.Second).UTC()
}

// resumePoint decides where a run starts.
//
// The watermark doubles as the cursor. It records how far back the fast tier is
// complete, which is exactly the point a new run should continue from, so an
// interrupted warm resumes without a separate bookmark to keep in sync.
//
// An unclaimed watermark starts from now, which fills the whole window. That is
// the same answer the old zero-time check gave, but for a stated reason rather
// than because a zero time happened to fall through.
func resumePoint(saved watermark.Mark, now time.Time) time.Time {
	if !saved.Claimed || saved.From.After(now) {
		return now
	}
	return saved.From
}
