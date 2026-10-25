package router

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/livecache"
)

// The live cache's verify mode, router.live_cache.verify_log.
//
// Every suite passed with the live cache on, and that said almost nothing: a
// suite asks each question once, and the cache answers only the second time a
// query arrives. One suite's router logged 5 hits in 71 reads. So in this mode
// the cache answers nothing. A read it could answer goes on to be answered as
// if it were off, and what it would have said is compared with what the
// client got. A read it misses waits for its fill, so the first read of a
// query is checked too.
//
// A difference is a lead, not a verdict: a write by another connection
// acknowledged between the two answers changes the second, and the cache's
// promise for those is bounded staleness, not equality. Each line carries what
// it takes to tell which.

// liveVerifyWait bounds how long a missed read waits for its fill.
const liveVerifyWait = 500 * time.Millisecond

// liveCheck is a cached answer waiting for the real one.
type liveCheck struct {
	ns string
	// shape is the query's shape, written on its line so a run can count
	// which shapes were compared.
	shape string
	// q is the query, for a comparison that depends on its kind.
	q      livecache.Query
	cmd    bson.Raw
	cached bson.Raw
	at     time.Time
	// pos is where in the stream the cached answer stands. See exactAnswer.
	pos livecache.Position
}

// liveVerifier appends a line per comparison to the verify log.
type liveVerifier struct {
	mu   sync.Mutex
	path string
}

func (v *liveVerifier) write(line string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	f, err := os.OpenFile(v.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	_, _ = f.WriteString(line)
	_ = f.Close()
}

// liveVerifyGet is Get for verify mode: it waits out the states that pass on
// their own, a fill in flight or a write not applied yet, and logs why when
// the cache still would not answer.
func (c *conn) liveVerifyGet(q livecache.Query) (bson.Raw, livecache.Position, bool) {
	start := time.Now()
	deadline := start.Add(liveVerifyWait)
	// A tiered entry is filled on its second miss (see livecache's
	// admitTiered), so one more ask is what a client coming back would make.
	asked := false
	for {
		if reply, pos, hit := c.r.live.GetAt(q); hit {
			return reply, pos, true
		}
		if why, failed := c.r.live.FailedSince(q, start); failed {
			if len(why) > 160 {
				why = why[:160]
			}
			c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\tfill failed: %s\n", q.NS, why))
			return nil, livecache.Position{}, false
		}
		st := c.r.live.State(q)
		if st == "no entry" && !asked {
			asked = true
			continue
		}
		switch st {
		case "filling", "behind", "entry behind":
			if time.Now().Before(deadline) {
				time.Sleep(2 * time.Millisecond)
				continue
			}
			st += " past the wait"
		case "hit":
			// Answerable between the two looks: ask again, within the wait.
			if time.Now().Before(deadline) {
				continue
			}
			st = "hit past the wait"
		}
		c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\t%s\n", q.NS, st))
		return nil, livecache.Position{}, false
	}
}

// liveVerifySkip logs a read the cache cannot take at all.
func (c *conn) liveVerifySkip(ns, why string) {
	c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\t%s\n", ns, why))
}

// liveVerifyReply compares the answer the client got with the one the cache
// would have given, and logs the comparison.
func (c *conn) liveVerifyReply(chk *liveCheck, real bson.Raw) {
	// Exact where it can be: the database read at the cache's own position.
	if chk.pos.Closed.T != 0 {
		if !c.r.cfg.Manages(chk.ns) {
			c.liveVerifyExact(chk)
			return
		}
		if c.liveVerifyTieredExact(chk) {
			return
		}
	}
	why := liveDiffer(chk.cached, real)
	if chk.q.Kind == livecache.KindGroup {
		// The order of groups without a $sort is mongod's choice, so the
		// same answer can come in another order.
		if !replyOK(real) {
			why = "the real reply is an error"
		} else {
			why = livecache.SameGroupAnswer(chk.q, chk.cached, real)
		}
	}
	took := time.Since(chk.at).Round(time.Microsecond)
	if why == "" {
		c.r.liveVerify.write(fmt.Sprintf("OK\t%s\t%s\t%s\n", chk.ns, took, chk.shape))
		return
	}
	c.log.Warn("the live cache would have answered differently", "ns", chk.ns, "why", why)
	c.r.liveVerify.write(fmt.Sprintf("MISMATCH\t%s\t%s\t%s\tcmd=%s\tcache=%s\treal=%s\n",
		chk.ns, took, why, trimJSON(chk.cmd), trimJSON(chk.cached), trimJSON(real)))
}

// liveDiffer says how two replies differ in what a client reads from them,
// or "" when they do not. A real cursor left open means the client got a
// first batch of a longer answer, which the cache's must begin with.
func liveDiffer(cached, real bson.Raw) string {
	if !replyOK(real) {
		return "the real reply is an error"
	}
	if n, ok := real.Lookup("n").AsInt64OK(); ok {
		m, ok := cached.Lookup("n").AsInt64OK()
		if !ok || m != n {
			return "count"
		}
		return ""
	}
	rb, ok := rawBatch(real.Lookup("cursor", "firstBatch"))
	if !ok {
		return "the real reply has no first batch"
	}
	cb, ok := rawBatch(cached.Lookup("cursor", "firstBatch"))
	if !ok {
		return "the cached reply has no first batch"
	}
	open := false
	if id, ok := real.Lookup("cursor", "id").AsInt64OK(); ok && id != 0 {
		open = true
	}
	if open && len(cb) < len(rb) || !open && len(cb) != len(rb) {
		return fmt.Sprintf("length cache=%d real=%d open=%v", len(cb), len(rb), open)
	}
	for i := range rb {
		if !bytes.Equal(cb[i], rb[i]) {
			return fmt.Sprintf("document %d", i)
		}
	}
	return ""
}

func trimJSON(d bson.Raw) string {
	s := d.String()
	if len(s) > 1500 {
		s = s[:1500] + "..."
	}
	return s
}

// liveVerifyExact judges a cached answer against mongod read at the position
// the answer stands for, rather than against the client's own answer, which
// was read a moment later.
//
// Compared with the client's answer, a write by another connection landing
// between the two made a difference that was not the cache's: with the shop's
// 32 clients, 47 of 204,826 compared answers differed in an hour's first
// minutes, and none of them said whether it was a defect. Read at the same
// cluster time there is nothing between the two. The cache's answer is the
// database at pos.Closed, or at pos.Applied when it has applied part of a group
// of events sharing one time; it matches one of them or it is wrong. Wrong
// against both is half a transaction, a state mongod never shows.
//
// A tiered namespace's answer is made from both tiers, which a snapshot of the
// fast tier cannot judge, so it is still compared with the client's.
func (c *conn) liveVerifyExact(chk *liveCheck) {
	took := time.Since(chk.at).Round(time.Microsecond)
	at := []bson.Timestamp{chk.pos.Closed}
	if chk.pos.Applied != chk.pos.Closed && chk.pos.Applied.T != 0 {
		at = append(at, chk.pos.Applied)
	}
	var why string
	var snap bson.Raw
	for _, ts := range at {
		got, err := c.snapshotAnswer(chk, ts)
		if err != nil {
			c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\tsnapshot read failed: %.160s\n", chk.ns, err.Error()))
			return
		}
		snap = got
		if chk.q.Kind == livecache.KindGroup {
			why = livecache.SameGroupAnswer(chk.q, chk.cached, got)
		} else {
			why = liveDiffer(chk.cached, got)
		}
		if why == "" {
			c.r.liveVerify.write(fmt.Sprintf("OK\t%s\t%s\t%s\texact\n", chk.ns, took, chk.shape))
			return
		}
	}
	c.log.Warn("the live cache answered differently from mongod at its own position", "ns", chk.ns, "why", why)
	c.r.liveVerify.write(fmt.Sprintf("MISMATCH\t%s\t%s\t%s\texact at %v\tcmd=%s\tcache=%s\treal=%s\n",
		chk.ns, took, why, at, trimJSON(chk.cmd), trimJSON(chk.cached), trimJSON(snap)))
}

// snapshotAnswer runs the client's read on the fast tier at one cluster time,
// with snapshot read concern, and returns it in the shape the cache answers
// in. count takes no snapshot read concern outside a transaction, so it is
// asked as the aggregate that counts the same documents.
func (c *conn) snapshotAnswer(chk *liveCheck, ts bson.Timestamp) (bson.Raw, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, coll := splitNS(chk.ns)
	elems, err := chk.cmd.Elements()
	if err != nil || len(elems) == 0 {
		return nil, fmt.Errorf("unreadable command")
	}
	rc := bson.D{{Key: "level", Value: "snapshot"}, {Key: "atClusterTime", Value: ts}}
	if elems[0].Key() == "count" {
		for _, e := range elems {
			switch e.Key() {
			case "skip", "limit", "hint", "collation":
				return nil, fmt.Errorf("count with %s", e.Key())
			}
		}
		match := bson.D{}
		if q, ok := chk.cmd.Lookup("query").DocumentOK(); ok {
			match = bson.D{{Key: "$match", Value: q}}
		}
		pipe := bson.A{bson.D{{Key: "$count", Value: "n"}}}
		if len(match) > 0 {
			pipe = append(bson.A{match}, pipe...)
		}
		var out struct {
			Cursor struct {
				FirstBatch []struct {
					N int64 `bson:"n"`
				} `bson:"firstBatch"`
			} `bson:"cursor"`
		}
		if err := c.r.hot.Database(db).RunCommand(ctx, bson.D{
			{Key: "aggregate", Value: coll}, {Key: "pipeline", Value: pipe},
			{Key: "cursor", Value: bson.D{}}, {Key: "readConcern", Value: rc},
		}).Decode(&out); err != nil {
			return nil, err
		}
		n := int64(0)
		if len(out.Cursor.FirstBatch) > 0 {
			n = out.Cursor.FirstBatch[0].N
		}
		return bson.Marshal(bson.D{{Key: "n", Value: n}, {Key: "ok", Value: 1.0}})
	}
	cmd := bson.D{}
	for _, e := range elems {
		switch e.Key() {
		case "lsid", "$clusterTime", "txnNumber", "autocommit", "startTransaction", "readConcern",
			"$readPreference", "$db", "apiVersion", "apiStrict", "apiDeprecationErrors":
			continue
		}
		cmd = append(cmd, bson.E{Key: e.Key(), Value: e.Value()})
	}
	cmd = append(cmd, bson.E{Key: "readConcern", Value: rc})
	out, err := c.r.hot.Database(db).RunCommand(ctx, cmd).Raw()
	if err != nil {
		return nil, err
	}
	// Only the first batch is compared. A cursor left open holds a snapshot
	// on the fast tier until it times out.
	if id, ok := out.Lookup("cursor", "id").AsInt64OK(); ok && id != 0 {
		_ = c.r.hot.Database(db).RunCommand(ctx, bson.D{
			{Key: "killCursors", Value: coll}, {Key: "cursors", Value: bson.A{id}},
		}).Err()
	}
	return out, nil
}

// liveVerifyTieredExact is liveVerifyExact for a tiered namespace, where it can
// be exact: when the answer is made of documents the fast tier holds at the
// cache's position. Those are the database's documents there, because a
// document leaves the fast tier only once it is old and any write to it brings
// it back first, so a snapshot of the fast tier is the truth for them. It
// reports false, and leaves the comparison to the client's answer, when the
// answer or the snapshot's holds a document the other does not: that one may
// live in the archive alone.
func (c *conn) liveVerifyTieredExact(chk *liveCheck) bool {
	if chk.q.Kind != livecache.KindFind {
		return false
	}
	took := time.Since(chk.at).Round(time.Microsecond)
	snap, err := c.snapshotAnswer(chk, chk.pos.Closed)
	if err != nil {
		return false
	}
	cb, ok1 := rawBatch(chk.cached.Lookup("cursor", "firstBatch"))
	sb, ok2 := rawBatch(snap.Lookup("cursor", "firstBatch"))
	if !ok1 || !ok2 || len(cb) != len(sb) {
		return false
	}
	ids := func(b []bson.Raw) map[string]bool {
		out := map[string]bool{}
		for _, d := range b {
			out[string(d.Lookup("_id").Value)] = true
		}
		return out
	}
	ci, si := ids(cb), ids(sb)
	for k := range ci {
		if !si[k] {
			return false
		}
	}
	why := liveDiffer(chk.cached, snap)
	if why != "" && chk.pos.Applied != chk.pos.Closed && chk.pos.Applied.T != 0 {
		// Part of a group applied: the state at its time is the other answer
		// the cache may give. See liveVerifyExact.
		if at, aerr := c.snapshotAnswer(chk, chk.pos.Applied); aerr == nil && liveDiffer(chk.cached, at) == "" {
			why = ""
		}
	}
	if why != "" {
		c.log.Warn("the live cache answered differently from mongod at its own position", "ns", chk.ns, "why", why)
		c.r.liveVerify.write(fmt.Sprintf("MISMATCH\t%s\t%s\t%s\texact on the fast tier at %v\tcmd=%s\tcache=%s\treal=%s\n",
			chk.ns, took, why, chk.pos.Closed, trimJSON(chk.cmd), trimJSON(chk.cached), trimJSON(snap)))
		return true
	}
	c.r.liveVerify.write(fmt.Sprintf("OK\t%s\t%s\t%s\texact\n", chk.ns, took, chk.shape))
	return true
}
