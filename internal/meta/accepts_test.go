package meta

import "testing"

// A notice every announcement of which named a session is spent only by a
// delete in one of them; one that names none accepts any delete, which keeps
// an upgrade from tombstoning evictions announced before it.
func TestEvictionNoticeAcceptsOnlyItsSessions(t *testing.T) {
	two := 2
	n := EvictionNotice{Sessions: []string{"aa11", "cc33"}, Named: &two, Pending: &two}
	for _, c := range []struct {
		session string
		want    bool
	}{{"aa11", true}, {"cc33", true}, {"bb22", false}, {"", false}} {
		if got := n.Accepts(c.session); got != c.want {
			t.Errorf("Accepts(%q) = %v, want %v", c.session, got, c.want)
		}
	}
	if !(EvictionNotice{}).Accepts("") || !(EvictionNotice{}).Accepts("aa11") {
		t.Error("a notice naming no session refused a delete")
	}
	// An evictor from before sessions announced again: pending went up and
	// named did not, so one pending announcement is not a session's, and its
	// delete, outside any transaction, must be accepted.
	one, three := 1, 2
	old := EvictionNotice{Sessions: []string{"aa11"}, Named: &one, Pending: &three}
	if !old.Accepts("") {
		t.Error("an older evictor's delete was refused by an earlier announcement's session; it would be tombstoned")
	}
}
