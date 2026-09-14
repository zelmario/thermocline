package router

import "testing"

// Which collection options a router compares across the two tiers.
//
// It compared two, collation and validator, while the create path beside it
// carried every option there is except a deny-list of the ones a tiered
// collection cannot have. The comment on that deny-list says why it is a
// deny-list: "a list of the options worth carrying silently drops every option
// nobody thought of, and the copy is then a different collection with the same
// name". The check had the mistake the create had already fixed.
//
// Two of the options that travelled and were never checked again:
// validationAction, which decides whether a write is refused or merely logged,
// and changeStreamPreAndPostImages, where the tier that does not have it
// answers a client asking for a pre-image with an error rather than an event.
func TestEveryOptionIsCompared(t *testing.T) {
	cases := []struct {
		name string
		cold collOptions
		hot  collOptions
		want []string
	}{
		{
			name: "the two it always compared",
			cold: collOptions{"collation": "en/2", "validator": `{"a":1}`},
			hot:  collOptions{"collation": "simple/3", "validator": `{"b":1}`},
			want: []string{"collation", "validator"},
		},
		{
			name: "a validator that agrees and an action that does not",
			cold: collOptions{"validator": `{"a":1}`, "validationAction": `"error"`},
			hot:  collOptions{"validator": `{"a":1}`, "validationAction": `"warn"`},
			want: []string{"validationAction"},
		},
		{
			name: "pre-images on one tier only",
			cold: collOptions{"changeStreamPreAndPostImages": `{"enabled":true}`},
			hot:  collOptions{},
			want: []string{"changeStreamPreAndPostImages"},
		},
		{
			name: "an option nobody has thought of yet",
			cold: collOptions{"somethingNewIn9.0": "1"},
			hot:  collOptions{},
			want: []string{"somethingNewIn9.0"},
		},
		{
			name: "agreement is not a finding",
			cold: collOptions{"collation": "en/2", "validator": `{"a":1}`},
			hot:  collOptions{"collation": "en/2", "validator": `{"a":1}`},
			want: nil,
		},
		{
			name: "two plain collections",
			cold: collOptions{},
			hot:  collOptions{},
			want: nil,
		},
		{
			// Reported in a stable order, because this runs at every start and
			// a log that reorders itself is a log nobody can diff.
			name: "several at once, in order",
			cold: collOptions{"validator": `{"a":1}`, "collation": "en/2"},
			hot:  collOptions{},
			want: []string{"collation", "validator"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := drifted(c.cold, c.hot)
			if len(got) != len(c.want) {
				t.Fatalf("drifted = %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("drifted[%d] = %q, want %q", i, got[i], c.want[i])
				}
			}
		})
	}
}
