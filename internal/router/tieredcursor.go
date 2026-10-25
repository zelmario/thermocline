package router

import (
	"context"
	"sync"
	"time"

	"github.com/zelmario/thermocline/internal/watermark"
	"github.com/zelmario/thermocline/internal/wire"
)

// A cursor outlives the decision that let the fast tier answer it, and nothing
// checked whether that decision was still true.
//
// The completeness claim is read once, when the query arrives. If it covers what
// the query reaches back to, the fast tier answers and the router steps out of
// the way. Every getMore after that is forwarded to the fast tier, which by then
// may hold none of it: the evictor narrows the claim and removes the documents,
// and a cursor already open goes on being served by the collection it is
// draining.
//
// Measured on forty documents, all on both tiers, with a claim covering all of
// history. A client took the first batch of three, an eviction sweep took the
// fast tier from forty documents to nought, and the cursor delivered **3 of
// 40**. Nothing had been deleted: the archive held all forty throughout, and a
// query issued after the sweep answered forty. Only the cursor was short, and
// it was short in silence, which is the shape this system exists not to have.
//
// It is not corrected, it is refused, and that is the choice this project makes
// on purpose: a refusal is a limitation and a wrong answer is a bug. Correcting
// it would mean the router continuing the read from the archive at the point the
// fast tier's cursor reached, which it cannot know without tracking the sort key
// of every document it has handed over, and which is not even defined for an
// unsorted query. Refusing costs the application one error on a read it can
// re-run, and the re-run is answered from the archive and is whole.

// nsClaim is one namespace's completeness claim as it stood at that moment.
//
// The claim itself, not the query's bound. The rule is the same whichever branch
// let the query through, including the newest-N proof that needs no bound at
// all: the claim must not have narrowed since.
type nsClaim struct {
	ns      string
	from    time.Time
	claimed bool
}

// tieredRead is the set of claims a cursor was authorised by.
//
// A set rather than one claim, because an aggregation rests on more than the
// collection it names. A $lookup, $unionWith or $graphLookup reads a second
// collection, and checkAggregate requires that one to be held in full, because a
// join reads by key rather than by time. Any of them narrowing under an open
// cursor shortens the rest of it exactly as the primary's does.
type tieredRead struct {
	on []nsClaim
	at time.Time
	// used is the last time a getMore asked about this cursor, which is what
	// idles it out. mongod times a cursor out ten minutes after its last use,
	// not after it was opened, and reaping by at forgot the guard on a cursor
	// still being read: a 20 minute walk over a large result lost it at minute
	// eleven, and the getMores after that were forwarded unchecked while the
	// evictor narrowed the claim under them.
	used time.Time
	// pinned is a cursor opened with noCursorTimeout, which mongod does not
	// time out: reaped after ten idle minutes, its later getMores went to the
	// fast tier unchecked. It is forgotten when it is drained or killed, or
	// when the session it was opened in would have expired. See pinnedIdle.
	pinned bool
}

// pinnedIdle is mongod's default logical session timeout,
// localLogicalSessionTimeoutMinutes. A noCursorTimeout cursor is still killed
// when its session expires, so one abandoned without killCursors is forgotten
// then rather than kept for the life of the router.
const pinnedIdle = 30 * time.Minute

// tieredCursors remembers them, per router.
//
// Per router rather than per connection, for the same reason the change stream
// registry is: a driver may issue getMore from any connection in its pool.
type tieredCursors struct {
	mu sync.RWMutex
	by map[int64]tieredRead
}

func (reg *tieredCursors) add(id int64, r tieredRead) {
	if id == 0 {
		return
	}
	reg.mu.Lock()
	if reg.by == nil {
		reg.by = map[int64]tieredRead{}
	}
	reg.by[id] = r
	reg.mu.Unlock()
}

func (reg *tieredCursors) get(id int64) (tieredRead, bool) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	r, ok := reg.by[id]
	return r, ok
}

// touch records a getMore on a registered cursor. See tieredRead.used.
func (reg *tieredCursors) touch(id int64) {
	reg.mu.Lock()
	if r, ok := reg.by[id]; ok {
		r.used = time.Now()
		reg.by[id] = r
	}
	reg.mu.Unlock()
}

func (reg *tieredCursors) drop(id int64) {
	reg.mu.Lock()
	delete(reg.by, id)
	reg.mu.Unlock()
}

// noteTieredCursor remembers the claim that let the fast tier answer a query,
// when that query left a cursor open.
//
// A reply with cursor id nought is the whole answer and has nothing to outlive.
func (c *conn) noteTieredCursor(on []nsClaim, msg, reply *wire.Message) {
	if len(on) == 0 {
		return
	}
	if id, live := liveCursorOf(reply, on[0].ns); live {
		c.r.tiered.add(id, tieredRead{on: on, at: time.Now(), pinned: noCursorTimeout(msg)})
	}
}

// noCursorTimeout reports whether a command asked for a cursor mongod never
// times out.
func noCursorTimeout(msg *wire.Message) bool {
	cmd, err := msg.CommandDoc()
	return err == nil && safeBool(cmd.Lookup("noCursorTimeout"))
}

// noteTieredCursorOn is the one-claim case, which is every find.
//
// Separate so the slice is built only for a reply that left a cursor open.
// Most replies do not, and a find on a tiered collection is the hot path this
// whole system is for: one allocation per read, thrown away unread, is not
// something to add to it.
func (c *conn) noteTieredCursorOn(ns string, mark watermark.Mark, msg, reply *wire.Message) {
	id, live := liveCursorOf(reply, ns)
	if !live {
		return
	}
	c.r.tiered.add(id, tieredRead{
		on:     []nsClaim{{ns: ns, from: mark.From, claimed: mark.Claimed}},
		at:     time.Now(),
		pinned: noCursorTimeout(msg),
	})
}

// liveCursorOf is the cursor a successful reply left open, if it left one.
//
// A reply with cursor id nought is the whole answer and has nothing to outlive.
func liveCursorOf(reply *wire.Message, fallbackNS string) (int64, bool) {
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return 0, false
	}
	id, _ := cursorIdentity(rdoc, fallbackNS)
	return id, id != 0
}

// stillCovers reports whether a claim still covers what it covered when the
// cursor was opened.
//
// Its own function so the decision can be asked directly. The end-to-end check
// in testevict.sh needs a gate, an evictor and forty documents to reach it, and
// the boundary cases are not among them: the claim being withdrawn rather than
// raised, the claim being *widened* under an open cursor, and an archive that
// cannot be reached at all. Each of those is one line here.
//
// ok false means the claim could not be read, which happens when the archive is
// unreachable and router.claim_grace is off. That is not evidence the claim
// still holds, so it does not count as covering.
//
// A claim that has been withdrawn covers nothing, whatever From says: Claimed
// and From are separate for that reason, and reading From alone would treat a
// withdrawal as a claim from the zero time, which is the widest claim there is.
//
// Equal is covered. The evictor raises a claim to the boundary it then evicts
// against, so a claim that has not moved has removed nothing under the cursor.
func stillCovers(prev nsClaim, now watermark.Mark, ok bool) bool {
	return ok && now.Claimed && !now.From.After(prev.from)
}

// guardTieredGetMore refuses a getMore whose claim has narrowed under it.
//
// Only for a cursor this router authorised against a claim; a getMore on
// anything else is forwarded untouched, which is every cursor on an untiered
// collection and every cursor the router owns against the archive.
func (c *conn) guardTieredGetMore(ctx context.Context, msg *wire.Message) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	id, ok := intOf64(cmd.Lookup("getMore"))
	if !ok || id == 0 {
		return nil, false
	}
	was, known := c.r.tiered.get(id)
	if !known {
		return nil, false
	}
	c.r.tiered.touch(id)

	// The claims as they stand. A read, so one held under router.claim_grace
	// answers here too: an archive that cannot be reached has not narrowed
	// anything, because narrowing one is a write to it.
	//
	// Every namespace the cursor rests on, not only the one it names. The first
	// that has moved is the one reported, because one reason is enough to
	// re-run the query and naming them all would say no more.
	var narrowed nsClaim
	var now watermark.Mark
	for _, prev := range was.on {
		now, ok = c.r.completenessForRead(ctx, prev.ns)
		if !stillCovers(prev, now, ok) {
			narrowed = prev
			break
		}
	}
	if narrowed.ns == "" {
		return nil, false
	}

	c.r.tiered.drop(id)
	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	because := "the completeness claim for " + narrowed.ns + " has been narrowed since this cursor was opened"
	if !ok || !now.Claimed {
		because = "the completeness claim for " + narrowed.ns + " has been withdrawn since this cursor was opened"
	}
	c.log.Warn("refused a getMore whose claim narrowed under it",
		"ns", narrowed.ns, "cursor", id,
		"was", narrowed.from.Format(time.RFC3339), "now", now.From.Format(time.RFC3339),
		"opened", was.at.Format(time.RFC3339),
		"why", "the fast tier answered the first batch because it was complete for this range, "+
			"and it no longer is, so the rest of this cursor would be short with no error on it",
		"action", "re-run the query; it will be answered across both tiers and be whole")
	out, cerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: "+because+", so the rest of this cursor would come from a fast tier "+
			"that no longer holds it. Re-run the query: it will be answered across both tiers "+
			"and be whole.")
	if cerr != nil {
		return nil, false
	}
	return out, true
}

// reapIdle forgets registrations older than the idle window.
//
// A client that walks away from a cursor leaves one behind, and mongod reaps its
// own after ten minutes. This keeps the map from being the only thing that
// remembers a cursor nobody will ask about again.
func (reg *tieredCursors) reapIdle(idle time.Duration) {
	cut := time.Now().Add(-idle)
	reg.mu.Lock()
	for id, r := range reg.by {
		last := r.used
		if r.at.After(last) {
			last = r.at
		}
		if r.pinned {
			// mongod keeps it until the session it was opened in expires.
			if last.Before(time.Now().Add(-pinnedIdle)) {
				delete(reg.by, id)
			}
			continue
		}
		if last.Before(cut) {
			delete(reg.by, id)
		}
	}
	reg.mu.Unlock()
}

// forgetExhaustedTieredCursor drops the registration for a cursor the fast tier
// has just drained.
//
// A getMore whose reply carries cursor id nought is the end of that cursor:
// there will be no further getMore to refuse, and nothing else reads the
// registration. reapIdle would get to it eventually, and eventually is a map
// entry per cursor for as long as mongod would have kept the cursor itself.
func (c *conn) forgetExhaustedTieredCursor(msg *wire.Message, reply *wire.Message) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return
	}
	asked, ok := intOf64(cmd.Lookup("getMore"))
	if !ok || asked == 0 {
		return
	}
	rdoc, rerr := reply.ReplyDoc()
	if rerr != nil || !replyOK(rdoc) {
		return
	}
	// Only a reply that succeeded, checked above: an error carries no cursor
	// document at all, and that is not proof the cursor is gone, because a
	// getMore can fail for a reason the next one would not.
	if id, _ := cursorIdentity(rdoc, ""); id != 0 {
		return
	}
	c.r.tiered.drop(asked)
}
