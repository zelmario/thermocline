package router

import "testing"

// A cluster-wide default write or read concern the archive has and the fast
// tier does not.
//
// It matters because of which tier a client is actually talking to. The router
// answers from the fast tier wherever it can, so a client that names no concern
// of its own gets the fast tier's default -- and on the day an application was
// pointed at a router, that became a different tier's default from the one it
// had been getting. The durability of every unqualified write changed, and
// nothing in the application said so.
//
// The warning was covered nowhere: removing it outright left `go test` and
// every suite passing. It is asked here rather than end to end because the only
// way to arrange it is setDefaultRWConcern, which is cluster-wide -- a suite
// setting it would change the durability of every write on the test cluster
// while it ran, and one that failed to put it back would leave every suite
// after it running against a different guarantee.
func TestConcernDrift(t *testing.T) {
	const majority = `{"w":"majority"}`
	const one = `{"w":1}`

	cases := []struct {
		name string
		cold tierConcerns
		hot  tierConcerns
		want []string // the kinds reported, in order
	}{
		{
			// The case it exists for: the archive was given a default by hand,
			// the fast tier is a new replica set that never was.
			name: "the archive has a global write default and the fast tier does not",
			cold: tierConcerns{write: majority, writeSource: "global"},
			hot:  tierConcerns{write: one, writeSource: "implicit"},
			want: []string{"write"},
		},
		{
			name: "the same for a read concern",
			cold: tierConcerns{read: `{"level":"majority"}`, readSource: "global"},
			hot:  tierConcerns{read: `{"level":"local"}`, readSource: "implicit"},
			want: []string{"read"},
		},
		{
			name: "both drift, and both are reported",
			cold: tierConcerns{write: majority, writeSource: "global",
				read: `{"level":"majority"}`, readSource: "global"},
			hot:  tierConcerns{write: one, read: `{"level":"local"}`},
			want: []string{"write", "read"},
		},
		{
			// Agreement is not a finding however it was arrived at.
			name: "the tiers agree",
			cold: tierConcerns{write: majority, writeSource: "global"},
			hot:  tierConcerns{write: majority, writeSource: "global"},
			want: nil,
		},
		{
			// The one that would make this noise on every start. Both tiers
			// report an implicit default they were born with, and two untouched
			// clusters differing is not something an operator did or can fix by
			// copying a setting across.
			name: "an implicit default on the archive is not a difference anybody made",
			cold: tierConcerns{write: one, writeSource: "implicit"},
			hot:  tierConcerns{write: majority, writeSource: "implicit"},
			want: nil,
		},
		{
			// The other direction is left alone deliberately: the fast tier's
			// default is the one a client gets, so a default only it has is
			// already what the application is being given.
			name: "a global default on the fast tier alone is not reported",
			cold: tierConcerns{write: one, writeSource: "implicit"},
			hot:  tierConcerns{write: majority, writeSource: "global"},
			want: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := concernDrift(c.cold, c.hot)
			if len(got) != len(c.want) {
				t.Fatalf("got %d findings, want %d: %+v", len(got), len(c.want), got)
			}
			for i := range got {
				if got[i].kind != c.want[i] {
					t.Errorf("finding %d is %q, want %q", i, got[i].kind, c.want[i])
				}
				if got[i].archive == got[i].fastTier {
					t.Errorf("finding %d reports the same value on both tiers, which is not a drift", i)
				}
			}
		})
	}
}
