package tierver

import (
	"strings"
	"testing"
)

// The fourth element of a versionArray is a build counter and says nothing
// about what the server understands, so an archive that differs only there is
// not older.
func TestCompare(t *testing.T) {
	cases := []struct {
		name string
		a, b []int32
		want int
	}{
		{"the same", []int32{8, 0, 4, 0}, []int32{8, 0, 4, 0}, 0},
		{"differing only in the build counter", []int32{7, 0, 5, 1}, []int32{7, 0, 5, 9}, 0},
		{"an older patch", []int32{7, 0, 2, 0}, []int32{7, 0, 5, 0}, -1},
		{"an older minor", []int32{7, 0, 9, 0}, []int32{7, 1, 0, 0}, -1},
		{"an older major", []int32{6, 3, 0, 0}, []int32{8, 0, 0, 0}, -1},
		{"a newer major", []int32{8, 0, 0, 0}, []int32{7, 0, 0, 0}, 1},
		{"a short array", []int32{8}, []int32{8, 0, 0, 0}, 0},
		{"a short older array", []int32{7}, []int32{8, 0, 0, 0}, -1},
		{"both empty", nil, nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Compare(c.a, c.b); got != c.want {
				t.Errorf("Compare(%v, %v) = %d, want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

// A patch difference is the middle of a rolling upgrade and has to be allowed.
// Anything above the patch is a different dialect and is not.
func TestSameRelease(t *testing.T) {
	cases := []struct {
		name string
		a, b []int32
		want bool
	}{
		{"identical", []int32{8, 0, 26, 11}, []int32{8, 0, 26, 11}, true},
		{"a patch apart", []int32{8, 0, 4, 0}, []int32{8, 0, 26, 0}, true},
		{"a build counter apart", []int32{8, 0, 26, 1}, []int32{8, 0, 26, 9}, true},
		{"a minor apart", []int32{7, 0, 0, 0}, []int32{7, 3, 0, 0}, false},
		{"a major apart", []int32{7, 0, 30, 0}, []int32{8, 0, 4, 0}, false},
		{"the fast tier older", []int32{6, 0, 26, 0}, []int32{8, 0, 4, 0}, false},
		{"a short array of the same release", []int32{8, 0}, []int32{8, 0, 4, 0}, true},
		{"a short array of a different major", []int32{7}, []int32{8, 0, 0, 0}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := SameRelease(c.a, c.b); got != c.want {
				t.Errorf("SameRelease(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
			}
		})
	}
}

// The refusal has to name both versions and the release to upgrade to,
// whichever side is older, because the operator reading it has been told
// nothing else about the pair.
func TestMismatchNamesBothAndTheUpgrade(t *testing.T) {
	cases := []struct {
		name       string
		hot, cold  Version
		wantUpTo   string
		wantQuoted []string
	}{
		{
			name:       "the ordinary adoption, an older archive",
			hot:        Version{Text: "8.0.26-11", Parts: []int32{8, 0, 26, 11}},
			cold:       Version{Text: "7.0.30-1", Parts: []int32{7, 0, 30, 1}},
			wantUpTo:   "8.0",
			wantQuoted: []string{"8.0.26-11", "7.0.30-1"},
		},
		{
			name:       "an archive newer than the fast tier",
			hot:        Version{Text: "6.0.26", Parts: []int32{6, 0, 26, 0}},
			cold:       Version{Text: "8.0.4", Parts: []int32{8, 0, 4, 0}},
			wantUpTo:   "8.0",
			wantQuoted: []string{"6.0.26", "8.0.4"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			msg := (&Mismatch{Hot: c.hot, Cold: c.cold}).Error()
			for _, want := range c.wantQuoted {
				if !strings.Contains(msg, want) {
					t.Errorf("the refusal does not name %s: %s", want, msg)
				}
			}
			if !strings.Contains(msg, "Upgrade the older tier to "+c.wantUpTo) {
				t.Errorf("the refusal does not say to upgrade to %s: %s", c.wantUpTo, msg)
			}
			if !strings.Contains(msg, "allow_version_skew") {
				t.Errorf("the refusal does not name the way out: %s", msg)
			}
		})
	}
}

// Release is what the message and the log say, so a two-element array and a
// version with no numbers at all both have to produce something readable.
func TestRelease(t *testing.T) {
	cases := []struct {
		in   Version
		want string
	}{
		{Version{Text: "8.0.26-11", Parts: []int32{8, 0, 26, 11}}, "8.0"},
		{Version{Text: "7.3.0", Parts: []int32{7, 3}}, "7.3"},
		{Version{Text: "unreadable", Parts: []int32{8}}, "unreadable"},
	}
	for _, c := range cases {
		if got := c.in.Release(); got != c.want {
			t.Errorf("Release(%v) = %q, want %q", c.in.Parts, got, c.want)
		}
	}
}

// Two tiers on the same release can still refuse different commands, so the
// message has to name both feature compatibility versions and the one command
// that fixes it.
func TestFCVMismatchNamesTheFix(t *testing.T) {
	m := &FCVMismatch{
		Hot:     Version{Text: "8.0.26", Parts: []int32{8, 0, 26, 0}},
		Cold:    Version{Text: "8.0.4", Parts: []int32{8, 0, 4, 0}},
		HotFCV:  "8.0",
		ColdFCV: "7.0",
	}
	msg := m.Error()
	for _, want := range []string{"both on 8.0", "fast tier 8.0", "archive 7.0",
		`setFeatureCompatibilityVersion: "8.0"`, "allow_version_skew"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal does not carry %q: %s", want, msg)
		}
	}
}

// The tier that is behind is the one to move, whichever side it is, and a
// transition reads as the version it is leaving.
func TestHigherFCV(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{"8.0", "7.0", "8.0"},
		{"7.0", "8.0", "8.0"},
		{"8.0", "8.0", "8.0"},
		// A tier mid-transition is named by where it is going, because that
		// is what the operator would have to type.
		{"7.0->8.0", "8.0", "8.0"},
		{"6.0", "7.0->8.0", "8.0"},
		{"7.0->8.0", "7.0", "8.0"},
		{"", "7.0", "7.0"},
	}
	for _, c := range cases {
		if got := higherFCV(c.a, c.b); got != c.want {
			t.Errorf("higherFCV(%q, %q) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}
