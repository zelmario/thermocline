package router

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
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
	// need is the latest write this router had acknowledged on the namespace
	// when the read arrived, which the answer must reflect.
	need bson.Timestamp
	// allowed says the cache would have answered this user. One it would not
	// is sent to mongod, so mongod refusing it is no difference.
	allowed bool
}

// liveVerifier appends a line per comparison to the verify log.
type liveVerifier struct {
	mu   sync.Mutex
	path string
	// acked is, per namespace, the latest write this router acknowledged
	// there, with "db." for a whole database and "*" for anything. Kept here,
	// apart from the cache's own bookkeeping, so read-your-writes is checked
	// against what the router did rather than what the cache recorded.
	acked map[string]bson.Timestamp
}

// noteAcked records an acknowledged write for read-your-writes. See acked.
func (v *liveVerifier) noteAcked(key string, at bson.Timestamp) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.acked == nil {
		v.acked = map[string]bson.Timestamp{}
	}
	if old := v.acked[key]; at.T > old.T || (at.T == old.T && at.I > old.I) {
		v.acked[key] = at
	}
}

// need is the latest acknowledged write a read of ns must see.
func (v *liveVerifier) need(ns string) bson.Timestamp {
	v.mu.Lock()
	defer v.mu.Unlock()
	db, _ := splitNS(ns)
	out := bson.Timestamp{}
	for _, k := range []string{ns, db + ".", "*"} {
		if t := v.acked[k]; t.T > out.T || (t.T == out.T && t.I > out.I) {
			out = t
		}
	}
	return out
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
		case "filling", "behind", "entry behind", "group open":
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
	// testauth's limited user, refused by mongod: the cache never answers a
	// user mongod has not let read the namespace (see Allowed), so the answer
	// it held was never theirs to get.
	if !chk.allowed && !replyOK(real) {
		c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\tnot allowed, and mongod refused it\n", chk.ns))
		return
	}
	// The client was told the member stepped down, and its driver asks again.
	// mongod gave no answer to compare with, which is not a different one.
	if !replyOK(real) && isFailoverReply(real) {
		c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\t%s\t%s\n", chk.ns, RealReplyFailover, chk.shape))
		return
	}
	// Nor when the proxy refused it itself: with the archive's primary gone,
	// a tiered read waited for mirroring and was refused, 3 times in a 20
	// minute run, and each was logged as a MISMATCH against the cache's answer.
	if !replyOK(real) && proxyRefusal(real) {
		c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\t%s\t%s\n", chk.ns, RealReplyRefused, chk.shape))
		return
	}
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
	if why == livecache.GroupBatchFull {
		// The client's cursor holds the rest, and reading it here would take
		// it from the client. Not judged, rather than judged wrong.
		c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\t%s\t%s\n", chk.ns, why, chk.shape))
		return
	}
	if why == "" {
		c.r.liveVerify.write(fmt.Sprintf("OK\t%s\t%s\t%s\n", chk.ns, took, chk.shape))
		return
	}
	if c.tieOrderOnly(chk, real, why, nil) {
		return
	}
	if verdict := c.judgeDocument(chk, real, why); verdict != "" {
		if verdict == "race" {
			c.r.liveVerify.write(fmt.Sprintf("RACE\t%s\t%s\t%s\t%s\n", chk.ns, took, chk.shape, why))
			return
		}
		why += ", " + verdict
	}
	c.log.Warn("the live cache would have answered differently", "ns", chk.ns, "why", why)
	c.r.liveVerify.write(fmt.Sprintf("MISMATCH\t%s\t%s\t%s\tcmd=%s\tcache=%s\treal=%s%s\n",
		chk.ns, took, why, trimJSON(chk.cmd), trimJSON(chk.cached), trimJSON(real), differingDocs(chk.cached, real, why)))
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

// tieOrderOnly writes a SKIP, and reports true, when a find's two answers
// differ only in the order of documents that tie on the sort. That order is
// not the cache's to match: see livecache.TiesReordered.
//
// at is the cluster time real was read at, when it is a snapshot of the fast
// tier. Only then may the two hold different documents tying at the limit,
// each of the cache's own read alone at that time to prove it is the
// database's: compared with the client's answer, read later, a document could
// also have moved in between.
func (c *conn) tieOrderOnly(chk *liveCheck, real bson.Raw, why string, at *bson.Timestamp) bool {
	if chk.q.Kind != livecache.KindFind || !strings.HasPrefix(why, "document ") {
		return false
	}
	cb, ok1 := rawBatch(chk.cached.Lookup("cursor", "firstBatch"))
	rb, ok2 := rawBatch(real.Lookup("cursor", "firstBatch"))
	if !ok1 || !ok2 {
		return false
	}
	if livecache.TiesReordered(chk.q.Sort, cb, rb) {
		c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\t%s\t%s\n", chk.ns, TiesInAnotherOrder, chk.shape))
		return true
	}
	if at == nil || chk.q.Skip != 0 {
		return false
	}
	outsiders, ok := livecache.TiesAtTheLimit(chk.q.Sort, chk.q.Limit, cb, rb)
	if !ok {
		return false
	}
	for _, i := range outsiders {
		if !c.sameAt(chk, cb[i], *at) {
			return false
		}
	}
	c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\t%s\t%s\n", chk.ns, TiesAtTheLimit, chk.shape))
	return true
}

// sameAt reports whether doc, as the cache answered it, is the fast tier's
// document with its _id at cluster time at, read with the client's projection.
func (c *conn) sameAt(chk *liveCheck, doc bson.Raw, at bson.Timestamp) bool {
	db, coll := splitNS(chk.ns)
	find := bson.D{{Key: "find", Value: coll}, {Key: "filter", Value: bson.D{{Key: "_id", Value: doc.Lookup("_id")}}}, {Key: "limit", Value: 1}}
	if pv, err := chk.cmd.LookupErr("projection"); err == nil {
		find = append(find, bson.E{Key: "projection", Value: pv})
	}
	find = append(find, bson.E{Key: "readConcern", Value: bson.D{{Key: "level", Value: "snapshot"}, {Key: "atClusterTime", Value: at}}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := c.r.hot.Database(db).RunCommand(ctx, find).Raw()
	if err != nil {
		return false
	}
	sb, ok := rawBatch(out.Lookup("cursor", "firstBatch"))
	return ok && len(sb) == 1 && bytes.Equal(sb[0], doc)
}

// TiesAtTheLimit is the SKIP reason for an answer that differs from mongod's
// only in which documents tying at the limit it kept.
const TiesAtTheLimit = "ties at the limit"

// TiesInAnotherOrder is the SKIP reason for an answer whose only difference
// is the order among documents that tie on the sort.
const TiesInAnotherOrder = "ties in another order"

// RealReplyFailover is the SKIP reason for a read mongod refused because the
// member it reached had stepped down.
const RealReplyFailover = "the real reply is a failover error"

// RealReplyRefused is the SKIP reason for a read this proxy refused rather
// than sent to mongod, so there is no answer of mongod's to compare with.
const RealReplyRefused = "the proxy refused the real read"

// proxyRefusal reports whether an error reply is one this proxy wrote: every
// refusal of its own starts its message with "thermocline: ".
func proxyRefusal(reply bson.Raw) bool {
	msg, ok := reply.Lookup("errmsg").StringValueOK()
	return ok && strings.HasPrefix(msg, "thermocline: ")
}

// differingDocs adds the two documents a "document N" difference names. The
// replies are clipped at 1500 characters, so a difference past the first few
// documents of a listing was logged without the documents that differ: a
// mismatch on the ninth of ten articles showed eight equal ones and stopped.
func differingDocs(cached, real bson.Raw, why string) string {
	var i int
	if _, err := fmt.Sscanf(why, "document %d", &i); err != nil {
		return ""
	}
	cb, ok1 := rawBatch(cached.Lookup("cursor", "firstBatch"))
	rb, ok2 := rawBatch(real.Lookup("cursor", "firstBatch"))
	if !ok1 || !ok2 || i >= len(cb) || i >= len(rb) {
		return ""
	}
	out := ""
	for j := i; j < len(cb) && j < len(rb) && j < i+3; j++ {
		out += fmt.Sprintf("\tcache[%d]=%s\treal[%d]=%s", j, trimJSON(cb[j]), j, trimJSON(rb[j]))
	}
	return out
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
	// Read-your-writes. An answer from before a write this router had
	// acknowledged is exactly right for its own position and still wrong for
	// the client: it must equal the database at that write, unless the write
	// does not change it. So the position it is judged at is the later one.
	if later(chk.need, chk.pos.Closed) {
		at = judgeTimes(chk.need, chk.pos)
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
	if c.tieOrderOnly(chk, snap, why, &at[len(at)-1]) {
		return
	}
	c.log.Warn("the live cache answered differently from mongod at its own position", "ns", chk.ns, "why", why)
	c.r.liveVerify.write(fmt.Sprintf("MISMATCH\t%s\t%s\t%s\texact at %v\tcmd=%s\tcache=%s\treal=%s%s\n",
		chk.ns, took, why, at, trimJSON(chk.cmd), trimJSON(chk.cached), trimJSON(snap), differingDocs(chk.cached, snap, why)))
}

// judgeTimes is when a cached answer may stand, for a client that must see a
// write this router acknowledged at need, later than the cache's Closed: at
// need itself, and at Applied when that is later still.
//
// Judged at need alone, an answer at Applied was a MISMATCH. With two routers
// on the blog, router 2's cache stood at Closed {T,133} and Applied {T,135},
// and router 2 had inserted a comment at {T,134}. The answer held the view
// counted at {T,135}, which the database at {T,134} did not have yet: one
// more view than the snapshot, on the most viewed article, twice in 485,000
// answers. The answer was the database at Applied, exactly.
func judgeTimes(need bson.Timestamp, pos livecache.Position) []bson.Timestamp {
	at := []bson.Timestamp{need}
	if later(pos.Applied, need) {
		at = append(at, pos.Applied)
	}
	return at
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
		case "cursor":
			// One more group than the cache answers with at most. mongod
			// leaves a full batch's cursor open even when nothing follows, so
			// with the client's own batchSize a right answer that filled it
			// could not be told from a short one. With one more, the
			// snapshot's first batch is the whole answer whenever the cache's
			// could be, and holds an extra group when the cache missed one.
			if chk.q.Kind == livecache.KindGroup && chk.q.Group != nil {
				cmd = append(cmd, bson.E{Key: "cursor", Value: bson.D{{Key: "batchSize", Value: chk.q.Group.Batch + 1}}})
				continue
			}
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
	judgeAt := chk.pos.Closed
	if later(chk.need, judgeAt) {
		// Read-your-writes, as in liveVerifyExact.
		judgeAt = chk.need
	}
	snap, err := c.snapshotAnswer(chk, judgeAt)
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
			// Ties at the limit, or a document the archive alone holds.
			return c.tieOrderOnly(chk, snap, liveDiffer(chk.cached, snap), &judgeAt)
		}
	}
	why := liveDiffer(chk.cached, snap)
	if why != "" && later(chk.pos.Applied, judgeAt) {
		// The state at Applied is the other answer the cache may give, and
		// the only one when a write this router acknowledged lies between
		// Closed and Applied. See judgeTimes.
		if at, aerr := c.snapshotAnswer(chk, chk.pos.Applied); aerr == nil && liveDiffer(chk.cached, at) == "" {
			why = ""
		}
	}
	if why != "" && c.tieOrderOnly(chk, snap, why, &judgeAt) {
		return true
	}
	if why != "" {
		c.log.Warn("the live cache answered differently from mongod at its own position", "ns", chk.ns, "why", why)
		c.r.liveVerify.write(fmt.Sprintf("MISMATCH\t%s\t%s\t%s\texact on the fast tier at %v\tpos=%v/%v from=%v merged=%v\tcmd=%s\tcache=%s\treal=%s%s\n",
			chk.ns, took, why, judgeAt, chk.pos.Closed, chk.pos.Applied, chk.pos.From, chk.pos.Merged,
			trimJSON(chk.cmd), trimJSON(chk.cached), trimJSON(snap), differingDocs(chk.cached, snap, why)))
		return true
	}
	c.r.liveVerify.write(fmt.Sprintf("OK\t%s\t%s\t%s\texact\n", chk.ns, took, chk.shape))
	return true
}

// judgeDocument decides a difference in one document of a find against the
// client's answer, where the whole answer could not be read at the cache's
// position. The document is read alone, by _id, on the fast tier at that
// position: if it is there, that is the truth for it. "race" is a cache that
// matched it, the client having read a later write by another connection;
// "wrong at its position" is a defect. "" is a difference this cannot judge:
// two different documents at that place, or one the fast tier did not hold.
func (c *conn) judgeDocument(chk *liveCheck, real bson.Raw, why string) string {
	var i int
	if chk.q.Kind != livecache.KindFind || chk.pos.Closed.T == 0 {
		return ""
	}
	if _, err := fmt.Sscanf(why, "document %d", &i); err != nil {
		return ""
	}
	cb, ok1 := rawBatch(chk.cached.Lookup("cursor", "firstBatch"))
	rb, ok2 := rawBatch(real.Lookup("cursor", "firstBatch"))
	if !ok1 || !ok2 || i >= len(cb) || i >= len(rb) {
		return ""
	}
	id := cb[i].Lookup("_id")
	if !bytes.Equal(id.Value, rb[i].Lookup("_id").Value) || id.Type != rb[i].Lookup("_id").Type {
		return ""
	}
	db, coll := splitNS(chk.ns)
	find := bson.D{{Key: "find", Value: coll}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "limit", Value: 1}}
	if pv, err := chk.cmd.LookupErr("projection"); err == nil {
		find = append(find, bson.E{Key: "projection", Value: pv})
	}
	judgeAt := chk.pos.Closed
	if later(chk.need, judgeAt) {
		judgeAt = chk.need
	}
	find = append(find, bson.E{Key: "readConcern", Value: bson.D{{Key: "level", Value: "snapshot"}, {Key: "atClusterTime", Value: judgeAt}}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := c.r.hot.Database(db).RunCommand(ctx, find).Raw()
	if err != nil {
		return ""
	}
	sb, ok := rawBatch(out.Lookup("cursor", "firstBatch"))
	if !ok || len(sb) != 1 {
		return ""
	}
	if bytes.Equal(sb[0], cb[i]) {
		return "race"
	}
	return "wrong at its position"
}

// later reports whether a is after b.
func later(a, b bson.Timestamp) bool {
	return a.T > b.T || (a.T == b.T && a.I > b.I)
}
