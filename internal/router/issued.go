package router

import (
	"context"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// A cursor lives on mongod and outlives the router process that handed it to a
// client, and everything this router remembers about it does not. The guard in
// tieredcursor.go is a record in memory: killed and restarted, the router came
// back without it while the cursor was still open, and the driver, whose pool
// the kill had cleared, sent its next getMore on a new connection. A router
// that did not know the id forwarded it unguarded.
//
// Measured in testevict on 2026-11-27: forty documents on both tiers, a claim
// over all of history, a first batch of three, then a kill -9 and restart, then
// a sweep that emptied the fast tier. The cursor delivered 3 of 40, with no
// error. Without the restart the same getMore is refused.
//
// The record cannot be rebuilt, because the claim that authorised the cursor
// was read by the process that died. So this does what mongos does when it
// restarts, whose cursors are its own and die with it: a getMore on a cursor
// this process never handed out is answered CursorNotFound, and the client
// re-runs the query. Only on a tiered collection, because that is the only
// place forwarding it can give a wrong answer: anywhere else the fast tier is
// the whole of the data and its cursor is as good as it ever was.
//
// The other way to reach this is a getMore sent to a different router from the
// one that opened the cursor, through a load balancer that does not keep a
// client on one of them. mongos refuses that too, for the same reason.

// issuedIdle is how long a cursor is remembered after its last use. The longer
// of mongod's two clocks: cursorTimeoutMillis, ten minutes by default, and the
// thirty minute session timeout that ends a noCursorTimeout cursor. Forgetting
// one mongod still holds would refuse a getMore that would have been right, so
// this errs long; remembering one mongod has dropped costs a map entry.
const issuedIdle = 30 * time.Minute

// issuedCursors is every cursor id this process has handed to a client.
type issuedCursors struct {
	mu sync.Mutex
	by map[int64]time.Time
}

func (reg *issuedCursors) note(id int64) {
	reg.mu.Lock()
	if reg.by == nil {
		reg.by = make(map[int64]time.Time)
	}
	reg.by[id] = time.Now()
	reg.mu.Unlock()
}

// known reports whether this process handed the cursor out, and counts the
// question as a use.
func (reg *issuedCursors) known(id int64) bool {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, ok := reg.by[id]; !ok {
		return false
	}
	reg.by[id] = time.Now()
	return true
}

func (reg *issuedCursors) drop(id int64) {
	reg.mu.Lock()
	delete(reg.by, id)
	reg.mu.Unlock()
}

func (reg *issuedCursors) reapIdle(idle time.Duration) {
	cut := time.Now().Add(-idle)
	reg.mu.Lock()
	for id, at := range reg.by {
		if at.Before(cut) {
			delete(reg.by, id)
		}
	}
	reg.mu.Unlock()
}

// noteIssued records the cursor a reply hands the client, and forgets one a
// getMore reply says is finished. Called from send, which every reply passes.
func (c *conn) noteIssued(out *wire.Message) {
	rdoc, err := out.ReplyDoc()
	if err != nil {
		return
	}
	cur, ok := rdoc.Lookup("cursor").DocumentOK()
	if !ok {
		return
	}
	id, ok := intOf64(cur.Lookup("id"))
	if !ok {
		return
	}
	if id != 0 {
		c.r.issued.note(id)
		return
	}
	// A drained cursor, named by the getMore this answers. Only a getMore's
	// reply carries nextBatch.
	if _, nerr := cur.LookupErr("nextBatch"); nerr == nil && c.askedCursor != 0 {
		c.r.issued.drop(c.askedCursor)
	}
}

// refuseForeignGetMore answers a getMore on a tiered collection whose cursor
// this process never handed out. See the top of this file.
func (c *conn) refuseForeignGetMore(ctx context.Context, msg *wire.Message, db string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	id, ok := intOf64(cmd.Lookup("getMore"))
	if !ok || id == 0 {
		return nil, false
	}
	if c.r.issued.known(id) {
		return nil, false
	}
	coll, ok := cmd.Lookup("collection").StringValueOK()
	if !ok || coll == "" || coll[0] == '$' {
		// A database or deployment change stream, or a command's own cursor,
		// which name no collection here. See unregisteredStream.
		return nil, false
	}
	if _, _, tiered := c.r.tieredBase(ctx, db+"."+coll); !tiered {
		return nil, false
	}
	if c.upstream != nil {
		if kill, kerr := wire.NewMsg(0, 0, bson.D{
			{Key: "killCursors", Value: coll}, {Key: "cursors", Value: bson.A{id}}, {Key: "$db", Value: db},
		}); kerr == nil {
			_, _ = c.probe(kill)
		}
	}
	c.r.Stats.Refused.Add(1)
	c.r.Stats.ForeignCursors.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a getMore on a cursor this router process did not open",
		"ns", db+"."+coll, "cursor", id,
		"why", "the guard that keeps a tiered cursor from coming back short is held by the process "+
			"that opened it, which has ended, or by another router",
		"action", "re-run the query")
	out, berr := wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, bson.D{
		{Key: "ok", Value: 0.0},
		// The same three causes explainCursorNotFound names, plus the restart,
		// and how to rule out the one an operator can fix.
		{Key: "errmsg", Value: "thermocline: cursor id " + strconv.FormatInt(id, 10) + " was not opened by this " +
			"router process, which cannot tell whether the rest of it is still whole, so re-run the query. " +
			"It is one of three things: a cursor opened before this router restarted, a cursor another " +
			"router opened, or one that expired. A cursor id says nothing about who minted it, so this " +
			"cannot say which. If several routers sit behind one load balanced address, that is the cause: " +
			"name every router in the connection string instead, so the driver pins the cursor to the one " +
			"that made it."},
		{Key: "code", Value: int32(43)},
		{Key: "codeName", Value: "CursorNotFound"},
		// A change stream on one tiered collection names it too, and resumes
		// on this label from its last token, which is what it should do.
		{Key: "errorLabels", Value: bson.A{"ResumableChangeStreamError"}},
	})
	if berr != nil {
		return nil, false
	}
	return out, true
}
