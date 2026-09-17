package router

import (
	"context"
	"sync"
	"time"

	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// The one state in which the archive is not a superset of the fast tier.
//
// Everything about a fall-through read rests on that: a read the fast tier
// cannot claim to answer in full is sent to the archive, and the answer is
// whole because the archive holds every document the fast tier does. The feeder
// keeps it true, with one exception it cannot avoid. A document whose unique
// value another live archive document already holds is refused by the archive
// for ever, so the feeder gives up on it rather than retrying until mirroring
// stops for every namespace behind it. That document then lives on the fast
// tier alone.
//
// Measured before this file existed, on three documents written at the fast
// tier with the archive carrying a unique index the third breaks: a count
// through the router answered 2, a find returned two of three, and findOne by
// _id returned the third, because a point read asks both tiers. One collection
// giving two different answers about the same document depending on how it was
// asked, with ok: 1 on both.
//
// So the read is refused instead. That is the trade this project makes
// everywhere else: a refusal is a limitation and a wrong answer is a bug. The
// refusal names the namespace, what is wrong and what to do, and it lifts by
// itself the moment the document reaches the archive, because the feeder clears
// the row on the write that finally lands.

// supersetTTL is how long the router believes its last reading.
//
// The same length as the completeness claim's, and for the same reason: this is
// read on the fall-through path, where another round trip to the archive on
// every query would be paid by every deployment for a state almost none of them
// are ever in.
const supersetTTL = 5 * time.Second

// unmirroredCache is the router's view of the rows the feeder writes.
//
// One snapshot of the whole set rather than an entry per namespace, because the
// set is empty in a healthy deployment and tiny otherwise: one read answers for
// every namespace, and the common answer is "none anywhere".
type unmirroredCache struct {
	mu       sync.RWMutex
	byNS     map[string]int
	fetched  time.Time
	known    bool
	lastFail time.Time
}

// notSuperset reports how many documents the archive is missing for this
// namespace because it refused them.
//
// A failure to read the rows keeps whatever was last known rather than refusing
// on ignorance. The archive holds these rows, so a failure to read them means
// the archive is unreachable, and then the fall-through read itself is about to
// fail with the archive's own error, which says more than this would.
func (r *Router) notSuperset(ctx context.Context, ns string) int {
	r.unmirrored.mu.RLock()
	fresh := r.unmirrored.known && time.Since(r.unmirrored.fetched) < supersetTTL
	// A read that failed a moment ago is not worth repeating on every query.
	// These rows live in the archive, so a failure to read them means the
	// archive is unreachable, and then this lookup is a server selection
	// timeout charged to a read that is about to fail with the archive's own
	// error anyway. The claim cache holds itself back the same way and for the
	// same reason.
	failedRecently := !r.unmirrored.lastFail.IsZero() && time.Since(r.unmirrored.lastFail) < supersetTTL
	n := r.unmirrored.byNS[ns]
	r.unmirrored.mu.RUnlock()
	if fresh || failedRecently {
		return n
	}

	byNS, err := meta.UnmirroredNamespaces(ctx, r.cold)
	if err != nil {
		r.unmirrored.mu.Lock()
		r.unmirrored.lastFail = time.Now()
		n := r.unmirrored.byNS[ns]
		r.unmirrored.mu.Unlock()
		return n
	}

	r.unmirrored.mu.Lock()
	r.unmirrored.byNS = byNS
	r.unmirrored.fetched = time.Now()
	r.unmirrored.known = true
	r.unmirrored.lastFail = time.Time{}
	r.unmirrored.mu.Unlock()
	return byNS[ns]
}

// notSupersetCached answers from whatever this router last read, without going
// to the archive.
//
// For the hot paths that must not pay a round trip to ask, above all the query
// cache, which is consulted before any guard on every cacheable read. A stale
// zero there costs one cached answer of the kind that was already being served
// before this existed; a round trip would cost every read in every healthy
// deployment.
func (r *Router) notSupersetCached(ns string) int {
	r.unmirrored.mu.RLock()
	defer r.unmirrored.mu.RUnlock()
	return r.unmirrored.byNS[ns]
}

// refuseIfNotSuperset refuses a read that is about to be answered from an
// archive known to be missing documents of this namespace.
//
// It returns a reply to send when the answer is no. Placed at the funnel every
// archive read passes rather than at each caller, because the callers are a
// find, an aggregation, a count, a distinct and a newest-first proof, and the
// question is the same for all of them.
func (c *conn) refuseIfNotSuperset(ctx context.Context, ns string, requestID, responseTo int32) (*wire.Message, bool) {
	n := c.r.notSuperset(ctx, ns)
	if n == 0 {
		return nil, false
	}

	which := "one document"
	if n > 1 {
		which = itoa(n) + " documents"
	}
	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a read that the archive cannot answer in full",
		"ns", ns, "documents", n,
		"because", "the archive refused these documents, so it is not a superset of the fast tier",
		"effect", "answering from the archive would leave them out and report success",
		"action", "resolve the duplicate unique value, then write the document again; "+
			"cline preflight lists them, and the feeder's log named each one as it gave up",
		"or", "warm this namespace so the fast tier answers it in full, which stops reads falling "+
			"through at all; measured, a complete claim answers them while this stands")

	out, err := c.r.commandError(requestID, responseTo,
		"thermocline: refusing this read of "+ns+" because the archive does not hold "+which+
			" the fast tier does. The archive's unique index refused them, so mirroring gave up on "+
			"them, and this read is answered from the archive because the fast tier cannot be shown "+
			"to hold the whole of it. Answering would leave those documents out and say ok. "+
			"Resolve the duplicate value and write the document again, and this lifts by itself; "+
			"cline preflight lists them. Warming this namespace also answers these "+
			"reads, because a fast tier that holds it in full never falls through. The documents are "+
			"still on the fast tier and a lookup by _id still returns them.")
	if err != nil {
		return nil, false
	}
	return out, true
}
