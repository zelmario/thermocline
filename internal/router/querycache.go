package router

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/cache"
	"github.com/zelmario/thermocline/internal/meta"

	"github.com/zelmario/thermocline/internal/wire"
)

// The cache answers a read without asking either tier, for as long as that
// query's own history says the answer holds still.
//
// It is off by default and it is the one thing in this system that can answer
// without looking, so what it refuses to cache matters more than what it caches.
//
// Never cached:
//
//   - Anything inside a transaction. A transaction reads a snapshot, and an
//     answer from before it began is not in that snapshot.
//   - A read concern naming a cluster time, or asking for snapshot or
//     linearizable. Those are promises about a position in an oplog or a
//     confirmation from a primary, and a stored answer is neither.
//   - A reply that leaves a cursor open. Half an answer is not an answer, and
//     the getMore that follows belongs to a cursor this reply no longer has.
//   - Anything that is not a read.
//
// And the key carries who asked. A cached answer served to a different identity
// is the privilege bypass this system has already had once: with terminate_auth
// the verified user is part of the key, and without it the connection is, since
// one connection is one authenticated client. Neither mode can hand one client
// another's documents.
//
// A write through this router drops everything learned for that collection,
// which is what makes the cache worth having rather than merely fast: the
// common case of a write followed by a read of the same collection is correct
// immediately, not a TTL later. What the router cannot see is another writer,
// and the learned TTL is the whole of that exposure.

// cacheableRead reports whether this command may be answered from the cache,
// and the namespace it reads.
func cacheableRead(name string, cmd bson.Raw, db string) (ns string, ok bool) {
	switch name {
	case "find", "count", "distinct", "aggregate":
	default:
		return "", false
	}
	if inTransaction(cmd) {
		return "", false
	}
	if rc, err := cmd.LookupErr("readConcern"); err == nil {
		doc, dok := rc.DocumentOK()
		if !dok {
			return "", false
		}
		// A cluster time is a position in the fast tier's oplog. A stored answer
		// has no position.
		if _, aerr := doc.LookupErr("afterClusterTime"); aerr == nil {
			return "", false
		}
		if _, aerr := doc.LookupErr("atClusterTime"); aerr == nil {
			return "", false
		}
		if lv, lerr := doc.LookupErr("level"); lerr == nil {
			if s, sok := lv.StringValueOK(); sok && (s == "snapshot" || s == "linearizable") {
				return "", false
			}
		}
	}
	coll, cok := stringField(cmd, name)
	if !cok {
		// A collectionless aggregate has no collection to invalidate on, so
		// there is nothing to hang a cached answer from.
		return "", false
	}
	return db + "." + coll, true
}

// cacheKey identifies a query, its parameters, and who is asking.
//
// Everything that changes the answer is in it, including the batch size, which
// changes the shape of the reply. The fields that belong to a conversation
// rather than to the question are left out: two clients asking the same thing
// in different sessions are asking the same thing.
func (c *conn) cacheKey(name, ns string, cmd bson.Raw) string {
	h := sha256.New()
	h.Write([]byte(c.identity()))
	h.Write([]byte{0})
	h.Write([]byte(ns))
	h.Write([]byte{0})
	h.Write([]byte(name))
	h.Write([]byte{0})

	elems, err := cmd.Elements()
	if err != nil {
		return ""
	}
	for _, e := range elems {
		if sessionFields[e.Key()] || e.Key() == "$readPreference" {
			continue
		}
		h.Write([]byte(e.Key()))
		h.Write([]byte{0})
		h.Write(e.Value().Value)
		h.Write([]byte{byte(e.Value().Type)})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// identity is who this connection is, for the purpose of never handing one
// client another's answer.
func (c *conn) identity() string {
	if c.grants != nil && c.grants.User != "" {
		return "user:" + c.grants.User
	}
	// Authentication is proxied, so the fast tier knows who this is and this
	// router does not. One connection is one authenticated client, so the
	// connection is the identity, and the cache helps a client that repeats
	// itself rather than clients that resemble each other.
	return "conn:" + strconv.FormatUint(c.id, 10)
}

// cacheLookup answers from the cache when it can, before any guard runs: a
// stored answer is the whole answer, and re-deciding completeness for it would
// be deciding about a read that is not happening.
func (c *conn) cacheLookup(msg *wire.Message, name, db string) (*wire.Message, bool) {
	if c.r.cache == nil && c.r.advisor == nil {
		return nil, false
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	ns, ok := cacheableRead(name, cmd, db)
	if !ok {
		return nil, false
	}
	key := c.cacheKey(name, ns, cmd)
	if key == "" {
		return nil, false
	}
	// The shape and its parameters are recorded whether or not anything is
	// cached, because the point of the advisor is to say what caching would be
	// worth before anybody turns it on. See package cache.
	shape, params := cache.Shape(name, ns, cmd)
	c.cacheOf = cachePending{key: key, ns: ns, shape: shape, params: params}

	if c.r.cache == nil {
		return nil, false
	}
	stored, hit := c.r.cache.Get(key)
	if !hit {
		return nil, false
	}
	out, rerr := replyAddressedTo(stored, msg.Header.RequestID)
	if rerr != nil {
		return nil, false
	}
	c.cls = ClassCached
	c.log.Debug("answered from the cache", "ns", ns, "command", name)
	return out, true
}

// cacheStore learns from an answer on its way back to the client.
func (c *conn) cacheStore(reply *wire.Message) {
	pending := c.cacheOf
	c.cacheOf = cachePending{}
	if pending.key == "" || reply == nil {
		return
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return
	}
	// A reply that leaves a cursor open is half an answer, and the getMore that
	// follows belongs to a cursor a stored copy does not have.
	if id, ok := cursorIDFromReply(reply); ok && id != 0 {
		return
	}
	rhash := cache.ResultHash(rdoc)
	if c.r.advisor != nil {
		c.r.advisor.Observe(pending.shape, pending.ns, pending.params, rhash)
	}
	if c.r.cache != nil {
		// The stored bytes are the whole reply, because that is what goes back
		// to the client; the hash is of the answer inside it, because that is
		// what stability means.
		c.r.cache.ObserveResult(pending.key, pending.ns, reply.Raw, rhash)
	}
}

// cacheInvalidate drops what was learned about a collection this command just
// wrote to.
func (c *conn) cacheInvalidate(name, db string, cmd bson.Raw) {
	if c.r.cache == nil {
		return
	}
	for _, ns := range writtenNamespaces(name, db, cmd) {
		if n := c.r.cache.Invalidate(ns); n > 0 {
			c.log.Debug("a write dropped cached answers", "ns", ns, "entries", n)
		}
	}
}

// writtenNamespaces lists what a command changes, so the cache can forget
// exactly those collections.
//
// Deliberately generous: a command whose target cannot be read is treated as
// touching the whole database, because forgetting too much costs a lookup and
// forgetting too little costs a wrong answer.
func writtenNamespaces(name, db string, cmd bson.Raw) []string {
	switch name {
	case "insert", "update", "delete", "findAndModify", "findandmodify":
		if coll, ok := stringField(cmd, name); ok {
			return []string{db + "." + coll}
		}
		return []string{db + ".*"}
	case "bulkWrite":
		var out []string
		if v, err := cmd.LookupErr("nsInfo"); err == nil {
			if arr, aok := v.ArrayOK(); aok {
				if vals, verr := arr.Values(); verr == nil {
					for _, nv := range vals {
						if d, dok := nv.DocumentOK(); dok {
							if ns, sok := stringField(d, "ns"); sok {
								out = append(out, ns)
							}
						}
					}
				}
			}
		}
		return out
	case "aggregate":
		// Only when it writes, which is decided by the last stage.
		stages, ok := pipelineStages(cmd)
		if !ok || len(stages) == 0 {
			return nil
		}
		final, fok := stages[len(stages)-1].DocumentOK()
		if !fok {
			return nil
		}
		if outDB, outColl, isOut := outTarget(final, db); isOut {
			return []string{outDB + "." + outColl}
		}
		if mv, merr := final.LookupErr("$merge"); merr == nil {
			if target, _, tok := mergeTarget(mv, db); tok {
				return []string{target}
			}
		}
		return nil
	case "drop", "create", "renameCollection", "dropDatabase", "collMod", "createIndexes", "dropIndexes":
		// DDL changes what a query would answer as surely as a write does.
		if coll, ok := stringField(cmd, name); ok {
			return []string{db + "." + coll}
		}
		return []string{db + ".*"}
	}
	return nil
}

// replyAddressedTo rewrites a stored reply so it answers this request.
//
// The bytes are a reply to a request that has already been answered. Everything
// in them is still true except who they are addressed to, and a driver matches
// replies to requests by exactly that.
func replyAddressedTo(stored []byte, requestID int32) (*wire.Message, error) {
	raw := make([]byte, len(stored))
	copy(raw, stored)
	if len(raw) < wire.HeaderLen {
		return nil, wire.ErrNotCommand
	}
	binary.LittleEndian.PutUint32(raw[8:12], uint32(requestID))
	out := &wire.Message{Raw: raw}
	out.Header.Length = int32(binary.LittleEndian.Uint32(raw[0:4]))
	out.Header.RequestID = int32(binary.LittleEndian.Uint32(raw[4:8]))
	out.Header.ResponseTo = requestID
	out.Header.OpCode = int32(binary.LittleEndian.Uint32(raw[12:16]))
	return out, nil
}

// send writes a reply to the client, and is the one place every answer passes
// through on its way out.
//
// It exists because the learning has to see what the client actually got.
// Hanging it off one return path missed every read the archive answered, which
// is the read a cache is most worth having for: those return early from the
// guards. One funnel, and nothing can be added later that quietly skips it.
func (c *conn) send(out *wire.Message) error {
	c.cacheStore(out)
	return wire.Write(c.client, out)
}

// cachePending is the key an in-flight read will be learned under.
type cachePending struct {
	key    string
	ns     string
	shape  string
	params uint64
}

// publishAdvice records what has been learned, so it can be read long after the
// traffic that taught it.
//
// Every minute, and on the way out, because an operator asking "what would
// caching be worth" is usually asking about yesterday.
func (r *Router) publishAdvice(ctx context.Context) {
	if r.advisor == nil {
		return
	}
	rows := r.advisor.Report(adviceRows)
	rep := meta.CacheAdviceReport{
		ID:      r.cfg.FeederID + "@" + r.addrOrConfigured(),
		Shapes:  r.advisor.Shapes(),
		Enabled: r.cache != nil,
	}
	if r.cache != nil {
		rep.Hits, rep.Misses = r.cache.Counters()
	}
	for _, a := range rows {
		rep.Suggestions = append(rep.Suggestions, meta.CacheSuggestion{
			Shape:      a.Shape,
			NS:         a.NS,
			Executions: a.Executions,
			Repeats:    a.Repeats,
			Params:     a.Params,
			Changes:    a.Changes,
			Stable:     a.Stable,
			Suggested:  a.Suggested,
		})
	}
	if err := meta.PublishCacheAdvice(ctx, r.cold, rep); err != nil {
		r.log.Debug("could not publish cache advice", "err", err)
	}
}

// adviceRows is how many suggestions are published. Enough to act on, few
// enough that the report is read rather than skimmed.
const adviceRows = 50

// adviceEvery is how often the report is refreshed.
const adviceEvery = time.Minute

// reportAdvice keeps the published report current while the router runs.
func (r *Router) reportAdvice(ctx context.Context) {
	t := time.NewTicker(adviceEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// One last report on the way out, so a router stopped after an
			// hour of traffic does not take what it learned with it.
			r.publishAdvice(context.WithoutCancel(ctx))
			return
		case <-t.C:
			r.publishAdvice(ctx)
		}
	}
}

// addrOrConfigured names this router in the report.
func (r *Router) addrOrConfigured() string {
	if a, ok := r.addr.Load().(string); ok && a != "" {
		return a
	}
	return r.cfg.Listen
}
