package authn

import (
	"context"
	"testing"
	"time"
)

// How long a credential this router has already seen goes on working after it
// has changed on the server.
//
// Lookup answers from a small cache so a login storm does not become a read per
// attempt, and the TTL is the whole of the bound on staleness: a password
// changed or a user dropped on the fast tier keeps working here until the entry
// expires or something calls Forget. Nothing checked it. Removing the TTL from
// the condition, so a cached entry is served for ever, left `go test` passing
// and no suite mentions it either.
//
// No cluster needed, and no clock either. A Source with no client panics the
// moment it reads the database, so "did it go to the database" is observable:
// the cache being used is the call returning, and the cache being bypassed is
// the panic. Constructing the entry directly is what lets the age be chosen.
func TestLookupStopsTrustingACachedCredential(t *testing.T) {
	const ttl = time.Minute
	creds := map[string]Stored{SHA256: {Salt: "c2FsdA==", Iters: 15000}}

	newSource := func(age time.Duration) *Source {
		s := New(nil, ttl)
		s.entries["admin.alice"] = cached{
			creds:   creds,
			fetched: time.Now().Add(-age),
			found:   true,
		}
		return s
	}

	// wentToTheDatabase reports whether Lookup reached past the cache, which a
	// Source with no client cannot survive.
	wentToTheDatabase := func(s *Source) (past bool) {
		defer func() {
			if recover() != nil {
				past = true
			}
		}()
		if _, found, err := s.Lookup(context.Background(), "admin", "alice"); err != nil || !found {
			t.Fatalf("Lookup returned err=%v found=%v from a cached entry", err, found)
		}
		return false
	}

	t.Run("inside the ttl it is served from the cache", func(t *testing.T) {
		if wentToTheDatabase(newSource(ttl / 2)) {
			t.Error("a fresh entry was not used, so the cache buys nothing")
		}
	})

	t.Run("past the ttl it is fetched again", func(t *testing.T) {
		if !wentToTheDatabase(newSource(2 * ttl)) {
			t.Error("a stale entry was served: a changed or revoked credential " +
				"would go on working for as long as this router runs")
		}
	})

	t.Run("Forget takes effect before the ttl does", func(t *testing.T) {
		s := newSource(ttl / 2)
		s.Forget("admin", "alice")
		if !wentToTheDatabase(s) {
			t.Error("Forget left the entry in place, so a credential change waits out the ttl")
		}
	})

	t.Run("another user is not served this one's entry", func(t *testing.T) {
		s := newSource(ttl / 2)
		past := func() (p bool) {
			defer func() {
				if recover() != nil {
					p = true
				}
			}()
			_, _, _ = s.Lookup(context.Background(), "admin", "bob")
			return false
		}()
		if !past {
			t.Error("bob was answered from alice's entry")
		}
	})
}
