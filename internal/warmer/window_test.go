package warmer

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/watermark"
)

// The archive keeps documents the application deleted, marked with a tombstone.
// Copying one of those into the fast tier would bring a deleted record back to
// life, so every read out of the archive has to exclude them.
func TestSourceFilterExcludesTombstoned(t *testing.T) {
	rules := []config.WarmRule{
		{NS: "app.orders", Back: config.Duration(90 * 24 * time.Hour)},
		{NS: "app.orders", Back: config.Duration(90 * 24 * time.Hour), DateField: "createdAt"},
	}

	for _, rule := range rules {
		name := "objectid"
		if !rule.UsesObjectIDTime() {
			name = "datefield"
		}
		t.Run(name, func(t *testing.T) {
			filter, err := sourceFilter(rule, time.Now().Add(-24*time.Hour), scanCursor{})
			if err != nil {
				t.Fatalf("sourceFilter: %v", err)
			}
			raw, err := bson.Marshal(filter)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			clause, err := bson.Raw(raw).LookupErr(meta.TombstoneField)
			if err != nil {
				t.Fatalf("filter does not exclude tombstoned documents: deleted records would be resurrected")
			}
			exists, err := clause.Document().LookupErr("$exists")
			if err != nil {
				t.Fatalf("tombstone clause has no $exists: %v", err)
			}
			if exists.Boolean() {
				t.Error("filter selects tombstoned documents instead of skipping them")
			}
		})
	}
}

func TestSourceFilterBoundsTheWindow(t *testing.T) {
	rule := config.WarmRule{NS: "app.orders", Back: config.Duration(90 * 24 * time.Hour)}
	floor := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)

	filter, err := sourceFilter(rule, floor, scanCursor{})
	if err != nil {
		t.Fatalf("sourceFilter: %v", err)
	}
	raw, _ := bson.Marshal(filter)
	idClause, err := bson.Raw(raw).LookupErr("_id")
	if err != nil {
		t.Fatalf("no _id clause: %v", err)
	}
	gte, err := idClause.Document().LookupErr("$gte")
	if err != nil {
		t.Fatalf("no floor on the window: %v", err)
	}
	if got := gte.ObjectID().Timestamp().UTC(); !got.Equal(floor) {
		t.Errorf("floor ObjectId carries %v, want %v", got, floor)
	}
	// The first batch is the newest, so it has no upper bound yet.
	if _, err := idClause.Document().LookupErr("$lt"); err == nil {
		t.Error("first batch should have no upper bound")
	}
}

func TestSourceFilterWalksBackwards(t *testing.T) {
	rule := config.WarmRule{NS: "app.orders", Back: config.Duration(time.Hour)}
	raw, _ := bson.Marshal(bson.D{{Key: "_id", Value: bson.NewObjectID()}})
	before, _ := bson.Raw(raw).LookupErr("_id")

	filter, err := sourceFilter(rule, time.Now().Add(-time.Hour), scanCursor{id: before})
	if err != nil {
		t.Fatalf("sourceFilter: %v", err)
	}
	out, _ := bson.Marshal(filter)
	idClause, _ := bson.Raw(out).LookupErr("_id")
	if _, err := idClause.Document().LookupErr("$lt"); err != nil {
		t.Error("later batches must carry an upper bound, or the walk never advances")
	}
}

// The watermark claims slightly less than was warmed, because documents share a
// timestamp to the second. Claiming too little costs one redundant archive
// lookup; claiming too much returns short results and calls them complete.
func TestWatermarkIsConservative(t *testing.T) {
	oldest := time.Date(2026, 8, 13, 10, 30, 15, 0, time.UTC)
	got := watermarkAfter(oldest)
	if !got.After(oldest) {
		t.Errorf("watermarkAfter(%v) = %v, must be later than the oldest warmed document", oldest, got)
	}
	if got.Sub(oldest) > 2*time.Second {
		t.Errorf("watermark gives up %v, more than the one second of imprecision it is covering", got.Sub(oldest))
	}
}

func TestResumePoint(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	earlier := now.Add(-48 * time.Hour)

	tests := []struct {
		name  string
		saved watermark.Mark
		want  time.Time
		why   string
	}{
		{"no watermark starts at now", watermark.Mark{}, now,
			"a fresh namespace is complete only for what has been written since"},
		{"saved watermark resumes there", watermark.Mark{From: earlier, Claimed: true}, earlier,
			"the watermark doubles as the cursor, so an interrupted warm continues"},
		{"a future watermark is not trusted", watermark.Mark{From: now.Add(time.Hour), Claimed: true}, now,
			"clock skew must not make the walk skip a range it never filled"},
		{"a claim of all history resumes at the zero time", watermark.Mark{Claimed: true}, time.Time{},
			"a tier holding everything has nothing left to warm, and must not be confused with an absent claim"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resumePoint(tt.saved, now); !got.Equal(tt.want) {
				t.Errorf("resumePoint = %v, want %v: %s", got, tt.want, tt.why)
			}
		})
	}
}

func TestSourceFilterRejectsIDAsDateField(t *testing.T) {
	rule := config.WarmRule{NS: "app.orders", DateField: "_id", Back: config.Duration(time.Hour)}
	if _, err := sourceFilter(rule, time.Now(), scanCursor{}); err == nil {
		t.Error("sourceFilter accepted date_field: _id, want an error pointing at the empty-field form")
	}
}

// A claim on all of history records the zero time, and that must never be
// turned into a bound.
//
// bson.NewObjectIDFromTimestamp(time.Time{}) is 886e0900..., which sorts
// *after* an ObjectID minted in 2025, so a filter of `_id >= it` matches
// nothing at all. Both tiers then count zero, the counts agree, and
// verifyClaim concludes the claim is sound. Hole detection would stop working
// at the exact moment the fast tier claims the most, and nothing would say so.
func TestWindowFilterTreatsTheZeroTimeAsUnbounded(t *testing.T) {
	rules := []config.WarmRule{
		{NS: "app.orders", Back: config.Duration(90 * 24 * time.Hour)},
		{NS: "app.orders", Back: config.Duration(90 * 24 * time.Hour), DateField: "createdAt"},
	}

	for _, rule := range rules {
		name := "objectid"
		if !rule.UsesObjectIDTime() {
			name = "datefield"
		}
		t.Run(name, func(t *testing.T) {
			filter, err := windowFilter(rule, time.Time{})
			if err != nil {
				t.Fatalf("windowFilter: %v", err)
			}
			if len(filter) != 0 {
				t.Fatalf("a claim on all of history must bound nothing, got %v", filter)
			}
		})
	}

	// And the bound it would otherwise have built really does exclude real
	// documents, which is why the case above cannot be left to chance.
	zeroDerived := bson.NewObjectIDFromTimestamp(time.Time{})
	realID := bson.NewObjectIDFromTimestamp(time.Date(2025, 8, 20, 0, 0, 0, 0, time.UTC))
	if realID.Hex() >= zeroDerived.Hex() {
		t.Fatalf("this test guards a wrap that no longer happens: %s >= %s",
			realID.Hex(), zeroDerived.Hex())
	}
}

// A rule that tiers by a date field has to walk in that field's order, not in
// _id order, because the watermark it produces is a date.
//
// Measured on sample_mflix.comments, whose ObjectIds were minted in 2018 and
// whose comments are dated 1970 to 2017. Resuming a widened window turned the
// date watermark into an _id bound, which no document matched, so the scan
// returned nothing and the loop claimed the whole window complete: the archive
// held 5,233 documents newer than the new boundary and the fast tier held 889.
func TestDateRuleResumesByItsOwnField(t *testing.T) {
	rule := config.WarmRule{NS: "app.orders", Back: config.Duration(time.Hour), DateField: "at"}
	at := time.Date(2016, 8, 23, 0, 0, 0, 0, time.UTC)

	sort, _ := bson.Marshal(scanSort(rule))
	if _, err := bson.Raw(sort).LookupErr("at"); err != nil {
		t.Error("a date_field rule must sort by that field, or the watermark describes a range the walk has not covered")
	}

	filter, err := sourceFilter(rule, time.Now().Add(-time.Hour), scanCursor{at: at, atSet: true})
	if err != nil {
		t.Fatalf("sourceFilter: %v", err)
	}
	out, _ := bson.Marshal(filter)
	if _, err := bson.Raw(out).LookupErr("_id"); err == nil {
		t.Error("the resume bound is an _id, which orders differently from the date this rule tiers by")
	}
	clause, err := bson.Raw(out).LookupErr("at")
	if err != nil {
		t.Fatal("no bound on the date field at all")
	}
	// The filter carries the floor as $gte and the resume point as $lt, and BSON
	// keeps both keys, so look for the one that moves the walk backwards.
	found := false
	for _, e := range func() []bson.RawElement { es, _ := bson.Raw(out).Elements(); return es }() {
		if e.Key() != "at" {
			continue
		}
		if _, err := e.Value().Document().LookupErr("$lt"); err == nil {
			found = true
		}
	}
	if !found {
		t.Errorf("later batches must carry a date upper bound, or the walk never advances: %v", clause)
	}
}

// Documents share a date, so a plain $lt on it would step over every other
// document holding the boundary instant.
func TestDateRuleBreaksTiesById(t *testing.T) {
	rule := config.WarmRule{NS: "app.orders", Back: config.Duration(time.Hour), DateField: "at"}
	raw, _ := bson.Marshal(bson.D{{Key: "_id", Value: bson.NewObjectID()}})
	id, _ := bson.Raw(raw).LookupErr("_id")

	filter, err := sourceFilter(rule, time.Now().Add(-time.Hour),
		scanCursor{at: time.Now().Add(-time.Minute), atSet: true, id: id})
	if err != nil {
		t.Fatalf("sourceFilter: %v", err)
	}
	out, _ := bson.Marshal(filter)
	if _, err := bson.Raw(out).LookupErr("$or"); err != nil {
		t.Error("a cursor inside a run must keep the documents sharing its date that it has not passed yet")
	}
}
