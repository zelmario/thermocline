package promote

import (
	"context"
	"testing"
)

// The indexer never builds a system collection on the fast tier, and the reason
// is not tidiness.
//
// Building one means creating it from the archive's options, and `capped` is an
// option a build does not carry. A system.profile created that way is not
// capped, mongod will not use it, and setProfilingLevel answers NamespaceExists
// on that database from then on: profiling is off for good on the fast tier and
// nothing says why. It was found four levels down from a refused claim.
//
// The guard sits in three callers and in the two methods that do the creating,
// and nothing checked any of them. Removing it from Ensure and EnsureCollection
// left every test in the repository passing, which is how a rule that costs one
// line gets taken out as dead weight during a tidy-up.
//
// Both methods return before touching either tier, which is what lets this be
// asked without a cluster: an Indexer with no clients would panic the moment it
// used one, and these calls have to return quietly instead.
func TestIndexerLeavesSystemCollectionsAlone(t *testing.T) {
	ctx := context.Background()

	for _, ns := range []string{
		"shop.system.profile",
		"shop.system.views",
		"shop.system.js",
		"admin.system.users",
	} {
		ix := NewIndexer(nil, nil, nil)

		if created := ix.EnsureCollection(ctx, ns); created {
			t.Errorf("EnsureCollection(%q) reported creating it", ns)
		}
		// Nothing recorded either. A namespace the indexer has taken an
		// interest in has a row here, and one it refuses must not: the row is
		// what makes the second ask cheap, and there is no work to remember.
		if _, ok := ix.state[ns]; ok {
			t.Errorf("EnsureCollection(%q) left state behind", ns)
		}

		// Ensure returns nothing, so the state map is the observable. Reaching
		// past the guard would take the lock, record a build and use the nil
		// client, and the panic would fail this too.
		ix.Ensure(ctx, ns)
		if _, ok := ix.state[ns]; ok {
			t.Errorf("Ensure(%q) left state behind, so it went past the guard", ns)
		}
	}
}

// The collection named system.profile is refused; one merely called
// "systematics" is the application's and must not be.
//
// Without this the guard could be widened to any name starting with "system"
// and still pass, which would quietly stop the fast tier ever getting the
// indexes for a collection whose name happens to begin that way.
// Both methods, because the guard is written out separately in each and
// widening one is not widening the other. Checking only EnsureCollection let a
// widened Ensure through.
func TestIndexerDoesNotRefuseOrdinaryNames(t *testing.T) {
	const ns = "shop.systematics"

	for _, c := range []struct {
		name string
		call func(*Indexer)
	}{
		{"EnsureCollection", func(ix *Indexer) { ix.EnsureCollection(context.Background(), ns) }},
		{"Ensure", func(ix *Indexer) { ix.Ensure(context.Background(), ns) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			ix := NewIndexer(nil, nil, nil)
			// A nil client makes the work panic, so recovering is how this
			// tells "went past the guard" from "was refused" without a
			// cluster. Recording state counts as past it too: a refusal
			// returns before taking the lock.
			past := func() (p bool) {
				defer func() {
					if recover() != nil {
						p = true
					}
				}()
				c.call(ix)
				_, recorded := ix.state[ns]
				return recorded
			}()
			if !past {
				t.Errorf("%s refused %q; only system.* collections are MongoDB's own", c.name, ns)
			}
		})
	}
}
