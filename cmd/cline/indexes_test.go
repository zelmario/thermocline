package main

import "testing"

// The report's two questions about one index, asked without a cluster.
//
// absentFrom answers "is it there" and lostUniqueConstraints answers "is the
// constraint still enforced", and the second only exists because the first was
// being asked in its place. An index the fast tier still carries under the same
// name with unique gone is present by every test absentFrom makes, so the
// report went quiet about a constraint nothing enforces.

func idx(name, spec string, unique bool) indexInfo {
	return indexInfo{name: name, spec: spec, unique: unique}
}

func names(in []indexInfo) []string {
	out := make([]string, 0, len(in))
	for _, i := range in {
		out = append(out, i.name)
	}
	return out
}

func eq(a []string, b ...string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestLostUniqueConstraints(t *testing.T) {
	cases := []struct {
		name string
		cold map[string]indexInfo
		hot  map[string]indexInfo
		want []string
	}{
		{
			// The case that was silent. Same name, same keys, unique only on
			// the archive: absentFrom calls it present and says nothing.
			name: "same name, unique lost on the fast tier",
			cold: map[string]indexInfo{"email_1": idx("email_1", "{email: 1}", true)},
			hot:  map[string]indexInfo{"email_1": idx("email_1", "{email: 1}", false)},
			want: []string{"email_1"},
		},
		{
			name: "both unique is nothing to report",
			cold: map[string]indexInfo{"email_1": idx("email_1", "{email: 1}", true)},
			hot:  map[string]indexInfo{"email_1": idx("email_1", "{email: 1}", true)},
			want: nil,
		},
		{
			// absentFrom's finding, not this one's. Reporting it here as well
			// would print the same index twice under two headings.
			name: "absent from the fast tier is the other report",
			cold: map[string]indexInfo{"email_1": idx("email_1", "{email: 1}", true)},
			hot:  map[string]indexInfo{},
			want: nil,
		},
		{
			// absentFrom matches on the key pattern when the name differs, so
			// this asks the same question of the same index: the constraint
			// holds, under another name.
			name: "same keys under another name, still unique",
			cold: map[string]indexInfo{"email_1": idx("email_1", "{email: 1}", true)},
			hot:  map[string]indexInfo{"email_uniq": idx("email_uniq", "{email: 1}", true)},
			want: nil,
		},
		{
			name: "same keys under another name, not unique",
			cold: map[string]indexInfo{"email_1": idx("email_1", "{email: 1}", true)},
			hot:  map[string]indexInfo{"email_idx": idx("email_idx", "{email: 1}", false)},
			want: []string{"email_1"},
		},
		{
			// One of the two enforces it, which is all the constraint needs.
			name: "one of several on the same keys is unique",
			cold: map[string]indexInfo{"email_1": idx("email_1", "{email: 1}", true)},
			hot: map[string]indexInfo{
				"email_idx":  idx("email_idx", "{email: 1}", false),
				"email_uniq": idx("email_uniq", "{email: 1}", true),
			},
			want: nil,
		},
		{
			// A non-unique archive index is not a constraint at all, so its
			// fast tier copy not being unique is the expected state and not a
			// finding. Without this the report would fire on every ordinary
			// index a collection has.
			name: "a plain archive index is not a constraint",
			cold: map[string]indexInfo{"at_1": idx("at_1", "{at: 1}", false)},
			hot:  map[string]indexInfo{"at_1": idx("at_1", "{at: 1}", false)},
			want: nil,
		},
		{
			// _id is unique on both tiers by construction and is never the
			// operator's to rebuild.
			name: "_id is never reported",
			cold: map[string]indexInfo{"_id_": idx("_id_", "{_id: 1}", true)},
			hot:  map[string]indexInfo{"_id_": idx("_id_", "{_id: 1}", false)},
			want: nil,
		},
		{
			// Sorted, so the report reads the same way twice running: a map
			// range would order these differently on each call.
			name: "several are reported in name order",
			cold: map[string]indexInfo{
				"z_1": idx("z_1", "{z: 1}", true),
				"a_1": idx("a_1", "{a: 1}", true),
			},
			hot: map[string]indexInfo{
				"z_1": idx("z_1", "{z: 1}", false),
				"a_1": idx("a_1", "{a: 1}", false),
			},
			want: []string{"a_1", "z_1"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := names(lostUniqueConstraints(c.cold, c.hot))
			if !eq(got, c.want...) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

// absentFrom is what the case above slips past, and saying so in a test keeps
// the two functions' jobs apart: this one is about presence and nothing else.
func TestAbsentFromIgnoresOptions(t *testing.T) {
	cold := map[string]indexInfo{"email_1": idx("email_1", "{email: 1}", true)}
	hot := map[string]indexInfo{"email_1": idx("email_1", "{email: 1}", false)}
	if got := absentFrom(cold, hot); len(got) != 0 {
		t.Fatalf("absentFrom reported %v; it matches on name and keys, not options", names(got))
	}
}
