package router

import (
	"bytes"
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
	shape  string
	cmd    bson.Raw
	cached bson.Raw
	at     time.Time
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
func (c *conn) liveVerifyGet(q livecache.Query) (bson.Raw, bool) {
	start := time.Now()
	deadline := start.Add(liveVerifyWait)
	// A tiered entry is filled on its second miss (see livecache's
	// admitTiered), so one more ask is what a client coming back would make.
	asked := false
	for {
		if reply, hit := c.r.live.Get(q); hit {
			return reply, true
		}
		if why, failed := c.r.live.FailedSince(q, start); failed {
			if len(why) > 160 {
				why = why[:160]
			}
			c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\tfill failed: %s\n", q.NS, why))
			return nil, false
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
		return nil, false
	}
}

// liveVerifySkip logs a read the cache cannot take at all.
func (c *conn) liveVerifySkip(ns, why string) {
	c.r.liveVerify.write(fmt.Sprintf("SKIP\t%s\t%s\n", ns, why))
}

// liveVerifyReply compares the answer the client got with the one the cache
// would have given, and logs the comparison.
func (c *conn) liveVerifyReply(chk *liveCheck, real bson.Raw) {
	why := liveDiffer(chk.cached, real)
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
