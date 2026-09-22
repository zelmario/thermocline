package cache

import "strings"

// NothingCanInvalidate reports whether a namespace is one this cache has no way
// of being told about.
//
// Two things drop a stored answer, and both have a reach. The router drops
// everything learned for a collection when it forwards a write to it, so a
// write that does not pass through this router is invisible. The fast tier's
// change stream covers that gap for every other writer, and it is a
// client-level stream, which MongoDB does not open on the admin, local or
// config databases and does not deliver system collections through. The
// archive's own bookkeeping is on the other tier entirely, so no stream of the
// fast tier's carries it either.
//
// Where neither reaches, the learned TTL is not a bound on how stale an answer
// can be, it is the whole of the answer's life. The package doc above calls the
// change stream the reason a writer that goes around the router does not matter,
// and for these namespaces there is no such stream, so that reason is simply
// absent rather than weaker.
//
// What was measured is that the answers were being stored, not that a stale one
// came back: eighteen reads of local.oplog.rs, admin.system.users and the
// archive's watermarks through a cache-on router produced 11 lookups and left 2
// entries behind. Whether one of those is then served is a race against the
// learned TTL, which is why the suite checks the storing and not the serving.
//
// The rule is about what can be heard, not about what matters, so it names the
// namespaces rather than the harm. An application asking these questions gets a
// slower answer than it could have. It gets a true one.
func NothingCanInvalidate(ns string) bool {
	if Housekeeping(ns) {
		return true
	}
	_, coll, found := strings.Cut(ns, ".")
	return found && strings.HasPrefix(coll, "system.")
}
