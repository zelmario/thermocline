package main

import (
	"testing"

	"github.com/zelmario/thermocline/internal/meta"
)

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
			// Both unique, and still not the same constraint. The archive
			// protects every active document and the fast tier only the active
			// ones that are also vip, so the router accepts a duplicate the
			// archive then refuses. Measured: both inserts accepted, fast tier
			// held two and the archive one.
			name: "both unique, the fast tier's filter is narrower",
			cold: map[string]indexInfo{"code_u": {name: "code_u", spec: "{code: 1}", unique: true,
				partial: `{"active":true}`}},
			hot: map[string]indexInfo{"code_u": {name: "code_u", spec: "{code: 1}", unique: true,
				partial: `{"active":true,"vip":true}`}},
			want: []string{"code_u"},
		},
		{
			name: "both unique with the same filter is nothing to report",
			cold: map[string]indexInfo{"code_u": {name: "code_u", spec: "{code: 1}", unique: true,
				partial: `{"active":true}`}},
			hot: map[string]indexInfo{"code_u": {name: "code_u", spec: "{code: 1}", unique: true,
				partial: `{"active":true}`}},
			want: nil,
		},
		{
			// A filter on one side and none on the other is the same gap in
			// whichever direction it runs: the two cover different documents.
			name: "a filter on the archive only",
			cold: map[string]indexInfo{"code_u": {name: "code_u", spec: "{code: 1}", unique: true,
				partial: `{"active":true}`}},
			hot:  map[string]indexInfo{"code_u": {name: "code_u", spec: "{code: 1}", unique: true}},
			want: []string{"code_u"},
		},
		{
			// Which values collide is part of the constraint too: under
			// strength 2 'A' and 'a' are the same key and under the default
			// they are not.
			name: "both unique, collating differently",
			cold: map[string]indexInfo{"code_u": {name: "code_u", spec: "{code: 1}", unique: true,
				collation: `{"locale":"en","strength":2}`}},
			hot:  map[string]indexInfo{"code_u": {name: "code_u", spec: "{code: 1}", unique: true}},
			want: []string{"code_u"},
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
		t.Fatalf("absentFrom reported %v; it matches on keys, not options", names(got))
	}
}

// An index does its work by its keys, so that is what "the same index" means
// here. A name is a label, and mongod lets two indexes on different keys share
// one when it is given explicitly.
func TestAbsentFromMatchesKeysNotNames(t *testing.T) {
	t.Run("the same name on different keys is a different index", func(t *testing.T) {
		// Measured: idx_a on {customer: 1} on the fast tier and idx_a on
		// {unrelated: 1} on the archive reported nothing, so a read falling
		// through and filtering on customer scanned.
		have := map[string]indexInfo{"idx_a": idx("idx_a", "{customer: 1}", false)}
		want := map[string]indexInfo{"idx_a": idx("idx_a", "{unrelated: 1}", false)}
		if got := absentFrom(have, want); !eq(names(got), "idx_a") {
			t.Fatalf("got %v, want [idx_a]", names(got))
		}
	})
	t.Run("a different name on the same keys is the same index", func(t *testing.T) {
		have := map[string]indexInfo{"idx_a": idx("idx_a", "{customer: 1}", false)}
		want := map[string]indexInfo{"whatever": idx("whatever", "{customer: 1}", false)}
		if got := absentFrom(have, want); len(got) != 0 {
			t.Fatalf("got %v, want none", names(got))
		}
	})
	t.Run("the same name on the same keys is present", func(t *testing.T) {
		have := map[string]indexInfo{"idx_a": idx("idx_a", "{customer: 1}", false)}
		want := map[string]indexInfo{"idx_a": idx("idx_a", "{customer: 1}", false)}
		if got := absentFrom(have, want); len(got) != 0 {
			t.Fatalf("got %v, want none", names(got))
		}
	})
}

// A constraint whose keys the fast tier does not carry at all is absentFrom's
// finding, not the lost-constraint one, however the names line up. Reporting it
// in both places would print the same index twice under two headings that
// contradict each other.
func TestLostUniqueIgnoresANameCollision(t *testing.T) {
	cold := map[string]indexInfo{"idx_a": idx("idx_a", "{code: 1}", true)}
	hot := map[string]indexInfo{"idx_a": idx("idx_a", "{unrelated: 1}", true)}
	if got := lostUniqueConstraints(cold, hot); len(got) != 0 {
		t.Fatalf("got %v; the fast tier has no index on those keys, which absentFrom reports", names(got))
	}
	if got := absentFrom(cold, hot); !eq(names(got), "idx_a") {
		t.Fatalf("absentFrom got %v, want [idx_a]", names(got))
	}
}

// rollback leaves Thermocline's own bookkeeping out of the three listings it
// makes of the archive. Both spellings have to be left out, and only one of
// them was: the test read meta.DefaultDB and the literal "_thermocline", which
// are the same string, so a deployment that set meta.db to anything else had
// its bookkeeping listed as the application's data.
func TestIsBookkeepingDB(t *testing.T) {
	saved := meta.DB
	defer func() { meta.DB = saved }()

	meta.DB = "bookkeeping"
	for _, db := range []string{"bookkeeping", "_thermocline", "_thermocline_w0", "bookkeeping_two"} {
		if !isBookkeepingDB(db) {
			t.Errorf("with meta.db=bookkeeping: %q should be left alone", db)
		}
	}
	for _, db := range []string{"shop", "warehouse", "books"} {
		if isBookkeepingDB(db) {
			t.Errorf("with meta.db=bookkeeping: %q is the application's", db)
		}
	}

	// The default deployment, which is the case that already worked.
	meta.DB = meta.DefaultDB
	if !isBookkeepingDB("_thermocline") || isBookkeepingDB("shop") {
		t.Errorf("the default spelling stopped working")
	}
}
