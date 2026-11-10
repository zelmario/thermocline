package router

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
)

// copyUpSessions bounds the registered sessions copy-ups are made in, one per
// copy-up running at once. Past it a copy-up writes as it did before sessions:
// announced on its residency record, with an echo the feeder spends.
//
// Transactions in them commit at w:1, the write concern the insert had
// outside one. See copyUpFrom for why w:1.
const copyUpSessions = 32

// copyUpSession lends a registered session for a copy-up of docs, or nil when
// the copy-up is to be made the way it was before sessions.
//
// Why a session at all: a copy-up's insert used to be told from the
// application's by its residency record, an echo announced before the insert
// and spent by the feeder after it. The sieve and the feeder then depended on
// that record being there and readable when the event was judged, which made
// its write concern part of their correctness. An insert made in a session the
// register names carries its lsid on the event, in the same commit as the
// document, so both know it from the event itself, as they know a warm. See
// meta.Sessions.
//
// Only when the fast tier was asked and did not hold the documents (asked),
// because the record written beside a session's insert is meta.WarmUpdate,
// which opens a promotion without guessing. And only when every document is
// small enough for its event to keep lsid. See meta.SessionDocMax.
func (r *Router) copyUpSession(ctx context.Context, ns string, asked bool, docs ...bson.Raw) *meta.OwnSession {
	if !asked || r.copyUps == nil {
		return nil
	}
	for _, d := range docs {
		if len(d) > meta.SessionDocMax {
			return nil
		}
	}
	sess, err := r.copyUps.Get(ctx)
	if err != nil {
		r.log.Warn("could not register a session for copy-ups, announcing this one on its residency record",
			"ns", ns, "err", err)
		return nil
	}
	return sess
}
