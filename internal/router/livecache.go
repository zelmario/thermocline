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

// liveNoteWrites remembers what a write can have changed, for when its reply
// goes out. See livecache.NoteWrites.
func (c *conn) liveNoteWrites(msg *wire.Message, name, db string, cmd bson.Raw) {
	if c.r.live == nil {
		return
	}
	c.liveWrites = c.liveWrites[:0]
	c.liveNotes = c.liveNotes[:0]
	if name == "commitTransaction" {
		// It names no namespace and may have written to any. See
		// livecache.NoteWriteAll.
		c.liveWrites = append(c.liveWrites, "*")
		return
	}
	if inTransaction(cmd) {
		// Its changes reach the stream at the commit's time, which the
		// commit notes; held whole meanwhile, as before.
		c.liveWrites = append(c.liveWrites, writtenNamespaces(name, db, cmd)...)
		return
	}
	if note, ok := classifyWrite(msg, name, db, cmd); ok {
		c.liveNotes = append(c.liveNotes, note)
		return
	}
	c.liveWrites = append(c.liveWrites, writtenNamespaces(name, db, cmd)...)
}

// classifyWrite says what an insert, update, delete or findAndModify can have
// changed, as livecache.WriteNote, from the command alone. ok false means it
// names no single namespace this can read, and it is noted whole by name.
func classifyWrite(msg *wire.Message, name, db string, cmd bson.Raw) (livecache.WriteNote, bool) {
	var note livecache.WriteNote
	if name == "findandmodify" {
		name = "findAndModify"
	}
	coll, ok := stringField(cmd, name)
	if !ok {
		return note, false
	}
	note.NS = db + "." + coll
	whole := func() (livecache.WriteNote, bool) {
		note.Whole = true
		note.IDs, note.Fields, note.Docs = nil, nil, nil
		return note, true
	}
	switch name {
	case "insert":
		for _, d := range msg.AllOf("documents") {
			note.Docs = append(note.Docs, append(bson.Raw(nil), d...))
		}
		if len(note.Docs) == 0 {
			return whole()
		}
		return note, true
	case "update":
		for _, st := range msg.AllOf("updates") {
			q, _ := st.Lookup("q").DocumentOK()
			id, has := idFromFilter(q)
			upsert, _ := st.Lookup("upsert").BooleanOK()
			if !has || upsert {
				return whole()
			}
			fields, ok := updatedFields(st.Lookup("u"))
			if !ok {
				return whole()
			}
			note.IDs = append(note.IDs, copyValue(id))
			note.Fields = append(note.Fields, fields...)
		}
	case "delete":
		for _, st := range msg.AllOf("deletes") {
			q, _ := st.Lookup("q").DocumentOK()
			id, has := idFromFilter(q)
			if !has {
				return whole()
			}
			note.IDs = append(note.IDs, copyValue(id))
		}
	case "findAndModify":
		q, _ := cmd.Lookup("query").DocumentOK()
		id, has := idFromFilter(q)
		upsert, _ := cmd.Lookup("upsert").BooleanOK()
		if !has || upsert {
			return whole()
		}
		note.IDs = append(note.IDs, copyValue(id))
		if remove, _ := cmd.Lookup("remove").BooleanOK(); !remove {
			fields, ok := updatedFields(cmd.Lookup("update"))
			if !ok {
				return whole()
			}
			note.Fields = fields
		}
	default:
		return note, false
	}
	if len(note.IDs) == 0 {
		return whole()
	}
	return note, true
}

// updatedFields reads the top-level fields an update document's operators
// change. A replacement, a pipeline or anything else unreadable says no: a
// replacement can change every field, and the old ones are not in the command.
func updatedFields(u bson.RawValue) ([]string, bool) {
	d, ok := u.DocumentOK()
	if !ok {
		return nil, false
	}
	ops, err := d.Elements()
	if err != nil || len(ops) == 0 {
		return nil, false
	}
	var out []string
	for _, op := range ops {
		name := op.Key()
		if len(name) == 0 || name[0] != '$' {
			return nil, false
		}
		args, ok := op.Value().DocumentOK()
		if !ok {
			return nil, false
		}
		fields, err := args.Elements()
		if err != nil {
			return nil, false
		}
		for _, f := range fields {
			path := f.Key()
			if i := strings.IndexByte(path, '.'); i >= 0 {
				path = path[:i]
			}
			out = append(out, path)
			if name == "$rename" {
				// The new name is a field the document now has.
				if to, ok := f.Value().StringValueOK(); ok {
					if i := strings.IndexByte(to, '.'); i >= 0 {
						to = to[:i]
					}
					out = append(out, to)
				}
			}
		}
	}
	return out, true
}

func copyValue(v bson.RawValue) bson.RawValue {
	return bson.RawValue{Type: v.Type, Value: append([]byte(nil), v.Value...)}
}

// liveAfterReply settles what the command asked of the live cache once its
// reply is known: mongod let the user read the namespace, or this router has
// acknowledged a write that the cache must have seen before answering again.
func (c *conn) liveAfterReply(reply *wire.Message) {
	if c.r.live == nil {
		return
	}
	if len(c.liveNotes) > 0 {
		c.r.live.NoteWrites(c.liveNotes, checkpoint.UnpackTS(c.r.seenOpTime.Load()))
		c.liveNotes = c.liveNotes[:0]
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
