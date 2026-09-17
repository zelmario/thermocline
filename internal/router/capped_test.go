package router

import "testing"

// mongod declares the capped flag safeBool in its IDL, which takes a number as
// well as a boolean and calls anything non-zero true. That latitude is why a
// capped collection could once be created through the router by spelling the
// flag as 1, and this reads the same option back out of a collection's rendered
// options, so it has to allow for the same spellings.
func TestCappedOption(t *testing.T) {
	cases := []struct {
		rendered string
		want     bool
	}{
		{"", false},
		{"false", false},
		{"0", false},
		{"true", true},
		// The rendered forms a number arrives in, which is what listCollections
		// gives back for a collection created with capped: 1.
		{`{"$numberInt":"1"}`, true},
		{`{"$numberLong":"1"}`, true},
		{`{"$numberDouble":"1.0"}`, true},
		// And the zero forms, which are a collection that is not capped.
		{`{"$numberInt":"0"}`, false},
		{`{"$numberLong":"0"}`, false},
	}
	for _, c := range cases {
		if got := cappedOption(c.rendered); got != c.want {
			t.Errorf("cappedOption(%q) = %v, want %v", c.rendered, got, c.want)
		}
	}
}
