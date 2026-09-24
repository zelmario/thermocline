package main

import (
	"strings"
	"testing"
)

// What an application loses when it is repointed at the archive, asked without
// a cluster.
//
// The live rehearsal reaches one of these two states: an index the archive does
// not have at all. The other is the one that hid a lost constraint in
// indexes.go for as long as it existed, an index the archive carries under the
// same name and keys with unique gone, and no fixture builds it, so it is
// asked here instead.

func TestRollbackSeesAConstraintTheArchiveKeptWithoutUnique(t *testing.T) {
	hot := map[string]indexInfo{
		"_id_":  {name: "_id_", spec: "_id: 1"},
		"ref_1": {name: "ref_1", spec: "ref: 1", unique: true},
	}
	// The archive has an index of that name on those keys, and it enforces
	// nothing. absentFrom calls it present, which is the whole point.
	cold := map[string]indexInfo{
		"_id_":  {name: "_id_", spec: "_id: 1"},
		"ref_1": {name: "ref_1", spec: "ref: 1"},
	}
	if got := absentFrom(hot, cold); len(got) != 0 {
		t.Fatalf("absentFrom should see nothing missing here, got %v", names(got))
	}
	gap := indexGapFor("shop.orders", hot, cold)
	if len(gap.weaker) != 1 || gap.weaker[0].name != "ref_1" {
		t.Fatalf("the lost constraint was not reported: missing=%v weaker=%v",
			names(gap.missing), names(gap.weaker))
	}
}

func TestRollbackSeesACollationThatChangesWhichValuesCollide(t *testing.T) {
	// Both unique, both on the same key, and they disagree about whether two
	// values are the same. The application declared the case-insensitive one.
	hot := map[string]indexInfo{
		"ref_1": {name: "ref_1", spec: "ref: 1", unique: true,
			collation: `{"locale":"en","strength":2}`},
	}
	cold := map[string]indexInfo{
		"ref_1": {name: "ref_1", spec: "ref: 1", unique: true},
	}
	gap := indexGapFor("shop.orders", hot, cold)
	if len(gap.weaker) != 1 {
		t.Fatalf("a differing collation is a differing constraint, got weaker=%v", names(gap.weaker))
	}
}

func TestCreateOptionsCarriesUniqueAndWhatNarrowsIt(t *testing.T) {
	// unique is carried here and deliberately left off by printCreateCommands
	// in indexes.go, because that one arms an archive a router still fronts and
	// this one hands the archive over as the application's only cluster.
	got := createOptions(indexInfo{
		name: "ref_1", unique: true, sparse: true,
		partial:   `{"active":true}`,
		collation: `{"locale":"en","strength":2}`,
	})
	for _, want := range []string{
		`name: "ref_1"`, "unique: true", "sparse: true",
		`partialFilterExpression: {"active":true}`,
		`collation: {"locale":"en","strength":2}`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("createOptions dropped %s: %s", want, got)
		}
	}
}

func TestCreateOptionsWithoutOptionsIsStillValid(t *testing.T) {
	got := createOptions(indexInfo{name: "at_-1"})
	if got != `{name: "at_-1"}` {
		t.Fatalf("a plain index should carry its name and nothing else, got %s", got)
	}
}
