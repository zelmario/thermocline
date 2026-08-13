package evictor

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmar/thermocline/internal/config"
)

func cand(t *testing.T, ns string, id any) candidate {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rv, err := bson.Raw(raw).LookupErr("_id")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	return candidate{id: rv, docID: docIDFor(ns, rv)}
}

// docIDFor mirrors meta.DocID without importing it, so a change to the encoding
// shows up as a test failure here rather than silently agreeing with itself.
func docIDFor(ns string, id bson.RawValue) string {
	const hexdigits = "0123456789abcdef"
	buf := append([]byte{byte(id.Type)}, id.Value...)
	out := make([]byte, 0, len(buf)*2)
	for _, b := range buf {
		out = append(out, hexdigits[b>>4], hexdigits[b&0x0f])
	}
	return ns + "|" + string(out)
}

// The two vetoes between a candidate and removal, and what happens when they
// disagree. Confirmation is the one that must never be skipped: removing a
// document the archive does not hold destroys it.
func TestPartition(t *testing.T) {
	a := cand(t, "app.orders", 1)
	b := cand(t, "app.orders", 2)
	c := cand(t, "app.orders", 3)
	cands := []candidate{a, b, c}

	tests := []struct {
		name            string
		held, archived  map[string]bool
		wantEvict       int
		wantHeld        int
		wantUnconfirmed int
		why             string
	}{
		{
			name:      "all archived and unheld are removed",
			archived:  map[string]bool{a.docID: true, b.docID: true, c.docID: true},
			held:      map[string]bool{},
			wantEvict: 3,
			why:       "old, archived and unused is exactly what should leave",
		},
		{
			name:      "a residency hold outranks age",
			archived:  map[string]bool{a.docID: true, b.docID: true, c.docID: true},
			held:      map[string]bool{b.docID: true},
			wantEvict: 2, wantHeld: 1,
			why: "recent use is what makes the fast tier a working set, not a date window",
		},
		{
			name:      "unarchived documents stay no matter how old",
			archived:  map[string]bool{a.docID: true},
			held:      map[string]bool{},
			wantEvict: 1, wantUnconfirmed: 2,
			why: "removing what the archive never received destroys it",
		},
		{
			name:            "nothing archived means nothing removed",
			archived:        map[string]bool{},
			held:            map[string]bool{},
			wantUnconfirmed: 3,
			why:             "an empty or unreachable archive must never look like permission to delete",
		},
		{
			name:     "a hold on an unarchived document still counts as held",
			archived: map[string]bool{},
			held:     map[string]bool{a.docID: true},
			wantHeld: 1, wantUnconfirmed: 2,
			why: "either veto is enough; they do not need to agree",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := partition(cands, tt.held, tt.archived)
			if len(got.evict) != tt.wantEvict {
				t.Errorf("evict = %d, want %d: %s", len(got.evict), tt.wantEvict, tt.why)
			}
			if got.held != tt.wantHeld {
				t.Errorf("held = %d, want %d", got.held, tt.wantHeld)
			}
			if got.unconfirmed != tt.wantUnconfirmed {
				t.Errorf("unconfirmed = %d, want %d", got.unconfirmed, tt.wantUnconfirmed)
			}
		})
	}
}

func TestCandidateFilterByObjectIDTime(t *testing.T) {
	rule := config.EvictionRule{NS: "app.orders", OlderThan: config.Duration(90 * 24 * time.Hour)}
	boundary := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)

	filter, err := candidateFilter(rule, boundary, bson.RawValue{})
	if err != nil {
		t.Fatalf("candidateFilter: %v", err)
	}

	raw, err := bson.Marshal(filter)
	if err != nil {
		t.Fatalf("marshal filter: %v", err)
	}
	idClause, err := bson.Raw(raw).LookupErr("_id")
	if err != nil {
		t.Fatalf("filter has no _id clause: %v", err)
	}
	lt, err := idClause.Document().LookupErr("$lt")
	if err != nil {
		t.Fatalf("filter has no $lt: %v", err)
	}

	// The generated ObjectId must carry the cutoff, since that is the entire
	// mechanism by which age is read without a second index.
	oid := lt.ObjectID()
	if got := oid.Timestamp().UTC(); !got.Equal(boundary) {
		t.Errorf("boundary ObjectId carries %v, want %v", got, boundary)
	}
	if _, err := idClause.Document().LookupErr("$gt"); err == nil {
		t.Error("first batch should have no $gt cursor bound")
	}
}

func TestCandidateFilterPaginates(t *testing.T) {
	rule := config.EvictionRule{NS: "app.orders", OlderThan: config.Duration(time.Hour)}
	after := cand(t, "app.orders", bson.NewObjectID()).id

	filter, err := candidateFilter(rule, time.Now(), after)
	if err != nil {
		t.Fatalf("candidateFilter: %v", err)
	}
	raw, _ := bson.Marshal(filter)
	idClause, _ := bson.Raw(raw).LookupErr("_id")
	if _, err := idClause.Document().LookupErr("$gt"); err != nil {
		t.Error("later batches must carry a $gt cursor bound, or held documents are rescanned forever")
	}
}

func TestCandidateFilterByDateField(t *testing.T) {
	rule := config.EvictionRule{
		NS: "app.orders", DateField: "createdAt",
		OlderThan: config.Duration(24 * time.Hour),
	}
	filter, err := candidateFilter(rule, time.Now(), bson.RawValue{})
	if err != nil {
		t.Fatalf("candidateFilter: %v", err)
	}
	raw, _ := bson.Marshal(filter)
	if _, err := bson.Raw(raw).LookupErr("createdAt"); err != nil {
		t.Errorf("filter does not constrain the configured date field: %v", err)
	}
}

// Reading age from _id is the empty-date_field case, not date_field: "_id".
// Saying so plainly beats generating a query that compares an ObjectId to a date.
func TestCandidateFilterRejectsIDAsDateField(t *testing.T) {
	rule := config.EvictionRule{NS: "app.orders", DateField: "_id", OlderThan: config.Duration(time.Hour)}
	if _, err := candidateFilter(rule, time.Now(), bson.RawValue{}); err == nil {
		t.Error("candidateFilter accepted date_field: _id, want an error pointing at the empty-field form")
	}
}

func TestCutoff(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	got := cutoff(now, 90*24*time.Hour)
	want := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("cutoff = %v, want %v", got, want)
	}
}
