package router

import (
	"reflect"
	"testing"
)

// The staging shop's own case: the fast tier had four indexes the archive did
// not, and the archive had _id_ and ref_1.
func TestIndexesNotServedNamesWhatTheArchiveLacks(t *testing.T) {
	fast := []namedIndex{
		{name: "_id_", fields: []string{"_id"}},
		{name: "ref_1", fields: []string{"ref"}},
		{name: "at_-1", fields: []string{"at"}},
		{name: "customer_1_at_-1", fields: []string{"customer", "at"}},
		{name: "customer_1_status_1", fields: []string{"customer", "status"}},
	}
	have := [][]string{{"_id"}, {"ref"}}
	got := indexesNotServed(fast, have)
	want := []string{"at_-1", "customer_1_at_-1", "customer_1_status_1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("missing = %v, want %v", got, want)
	}
}

// An archive index that leads with the same fields serves the shorter one, so
// it is not reported, and one leading with other fields does not.
func TestIndexesNotServedAcceptsALongerArchiveIndex(t *testing.T) {
	fast := []namedIndex{
		{name: "customer_1", fields: []string{"customer"}},
		{name: "at_-1", fields: []string{"at"}},
	}
	have := [][]string{{"_id"}, {"customer", "at"}, {"status", "at"}}
	got := indexesNotServed(fast, have)
	if !reflect.DeepEqual(got, []string{"at_-1"}) {
		t.Errorf("missing = %v, want [at_-1]: a {status, at} index cannot seek on at", got)
	}
}

// _id_ is everywhere, and a unique index has its own warning that says what it
// costs on the write path. A TTL index must not be built on the archive at all.
func TestIndexesNotServedLeavesOutIDUniqueAndTTL(t *testing.T) {
	fast := []namedIndex{
		{name: "_id_", fields: []string{"_id"}},
		{name: "email_1", fields: []string{"email"}, unique: true},
		{name: "at_1", fields: []string{"at"}, ttl: true},
	}
	if got := indexesNotServed(fast, [][]string{}); len(got) != 0 {
		t.Errorf("missing = %v, want nothing", got)
	}
}
