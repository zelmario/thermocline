package router

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/livecache"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// liveSkip names the namespaces the live cache never holds.
//
// Tiered ones, for now. Their change stream carries this system's own work,
// evictions as deletes and copy-ups as inserts, and an entry for them has to be
// filled with an answer from both tiers. Both are solved for client change
// streams (see changestream.go) and are the next step here, not this one.
func (r *Router) liveSkip(ns string) bool {
	db, coll := splitNS(ns)
	switch db {
	case "admin", "local", "config", meta.DB:
		return true
	}
	if strings.HasPrefix(coll, "system.") || meta.IsScratch(coll) {
		return true
	}
	return r.cfg.Manages(ns)
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
