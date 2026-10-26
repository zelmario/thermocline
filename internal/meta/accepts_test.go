package meta

import "testing"

// A notice every announcement of which named a session is spent only by a
// delete in one of them; one that names none accepts any delete, which keeps
// an upgrade from tombstoning evictions announced before it.
func TestEvictionNoticeAcceptsOnlyItsSessions(t *testing.T) {
	n := EvictionNotice{Sessions: []string{"aa11", "cc33"}}
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
}
