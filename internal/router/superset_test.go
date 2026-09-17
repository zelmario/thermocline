package router

import "testing"

// The cached reading is what the hot paths ask, above all the query cache,
// which is consulted before any guard on every cacheable read. What it must do
// is answer from whatever was last read without reaching for the archive, and
// answer zero for a namespace nothing is wrong with, which is every namespace
// in a healthy deployment.
func TestNotSupersetCached(t *testing.T) {
	r := &Router{}

	if got := r.notSupersetCached("shop.orders"); got != 0 {
		t.Fatalf("a router that has read nothing yet says %d, want 0", got)
	}

	r.unmirrored.mu.Lock()
	r.unmirrored.byNS = map[string]int{"shop.orders": 2}
	r.unmirrored.known = true
	r.unmirrored.mu.Unlock()

	if got := r.notSupersetCached("shop.orders"); got != 2 {
		t.Errorf("shop.orders says %d, want 2", got)
	}
	// The namespace beside it is not affected, which is the half that decides
	// whether this is worth having: a guard that stopped caching for a whole
	// deployment because one namespace is in trouble would cost more than it
	// saves.
	if got := r.notSupersetCached("shop.customers"); got != 0 {
		t.Errorf("shop.customers says %d, want 0", got)
	}
}
