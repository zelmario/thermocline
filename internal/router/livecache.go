package router

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/livecache"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// liveSkip names the namespaces the live cache never holds: the system's own.
//
// Tiered ones are held, on the terms livecache.Tiers sets out: an entry holds
// what the fast tier's answer proves and nothing more, and a document leaving
// the fast tier drops what held it.
func (r *Router) liveSkip(ns string) bool {
	db, coll := splitNS(ns)
	switch db {
	case "admin", "local", "config", meta.DB:
		return true
	}
	return strings.HasPrefix(coll, "system.") || meta.IsScratch(coll)
}

// liveProve is the newest-first proof (newestfirst.go) for the live cache:
// how many of the fast tier's documents, sorted by sort, no document in the
// archive can come before.
//
// The same claim, the same field, the same bracket check, and one difference.
// The router proves a page by its last document; a fill reads further than the
// page, so this counts how far the proof reaches instead, stopping at the
// first document older than the claim. That one and everything after it may
// have an archived document in front of it.
func (r *Router) liveProve(ctx context.Context, ns string, sort []livecache.SortKey, docs []bson.Raw) int {
	mark, known := r.completenessForRead(ctx, ns)
	if !known || !mark.Claimed {
		return 0
	}
	if mark.CoversAll() {
		// The fast tier claims all of history, so its answer is the answer.
		return len(docs)
	}
	af, ok := r.ageFieldOf(ns)
	if !ok {
		return 0
	}
	want := af
	if want == "" {
		want = "_id"
	}
	if len(sort) == 0 || sort[0].Field != want || !sort[0].Desc {
		return 0
	}
	if !r.sortSafeAges(ctx, ns, af) {
		return 0
	}
	for i, d := range docs {
		at, ok := documentAge(d, af)
		if !ok || at.Before(mark.From) {
			return i
		}
	}
	return len(docs)
}

// liveProof and liveWrites are what a command asked of the live cache, settled
// when its reply goes out. See liveAfterReply.
type liveProof struct {
	ns, user string
}

// liveLookup answers a read from the live cache when it can.
func (c *conn) liveLookup(msg *wire.Message, name, db string) (*wire.Message, bool) {
	if c.r.live == nil || msg.Header.OpCode != wire.OpMsg {
		return nil, false
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	coll, ok := stringField(cmd, name)
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if c.r.liveSkip(ns) {
		return nil, false
	}
	if mode, ok := readPrefMode(cmd); ok && mode != "primary" && mode != "primaryPreferred" {
		return nil, false
	}
	var q livecache.Query
	switch name {
	case "find":
		q, ok = livecache.ParseFind(ns, cmd)
	case "count":
		q, ok = livecache.ParseCount(ns, cmd)
	case "aggregate":
		q, ok = livecache.ParseCountAgg(ns, cmd)
	default:
		return nil, false
	}
	if !ok {
		return nil, false
	}
	// Authentication is proxied, so mongod decides who may read what, and an
	// answer from memory would skip it. Only somebody mongod let read this
	// namespace within the last minute is answered here; anybody else is
	// sent to mongod, and its yes is what lets them in next time.
	user := c.identity()
	if !c.r.live.Allowed(ns, user) {
		c.liveAsk = liveProof{ns: ns, user: user}
		return nil, false
	}
	reply, hit := c.r.live.Get(q)
	if !hit {
		return nil, false
	}
	c.cls = ClassCached
	return wire.NewMsgRaw(0, msg.Header.RequestID, reply), true
}

// liveNoteWrites remembers which namespaces a write names, for when its reply
// goes out.
func (c *conn) liveNoteWrites(name, db string, cmd bson.Raw) {
	if c.r.live == nil {
		return
	}
	if name == "commitTransaction" {
		// It names no namespace and may have written to any. See
		// livecache.NoteWriteAll.
		c.liveWrites = append(c.liveWrites[:0], "*")
		return
	}
	c.liveWrites = append(c.liveWrites[:0], writtenNamespaces(name, db, cmd)...)
}

// liveAfterReply settles what the command asked of the live cache once its
// reply is known: mongod let the user read the namespace, or this router has
// acknowledged a write that the cache must have seen before answering again.
func (c *conn) liveAfterReply(reply *wire.Message) {
	if c.r.live == nil {
		return
	}
	if len(c.liveWrites) > 0 {
		at := checkpoint.UnpackTS(c.r.seenOpTime.Load())
		for _, ns := range c.liveWrites {
			if ns == "*" {
				c.r.live.NoteWriteAll(at)
				continue
			}
			if strings.HasSuffix(ns, ".*") {
				// A whole database: every namespace in it waits.
				c.r.live.NoteWriteDB(strings.TrimSuffix(ns, ".*"), at)
				continue
			}
			c.r.live.NoteWrite(ns, at)
		}
		c.liveWrites = c.liveWrites[:0]
	}
	if c.liveAsk.ns != "" {
		if doc, err := reply.ReplyDoc(); err == nil && replyOK(doc) {
			c.r.live.Proved(c.liveAsk.ns, c.liveAsk.user)
		}
		c.liveAsk = liveProof{}
	}
}

// WithLiveStream gives the live cache its own client for the change stream,
// one that prefers a secondary. See router.live_cache.stream_from.
func (r *Router) WithLiveStream(c *mongo.Client) *Router {
	r.liveStream = c
	return r
}

func (r *Router) liveStreamFrom() string {
	if r.liveStream != nil {
		return "secondary"
	}
	return "primary"
}
