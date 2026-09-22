package router

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
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
	// A read that asks for a secondary is asking for different consistency, and
	// with terminate_auth on it gets one: the answer may come from a member
	// behind the primary. Holding that and handing it to a reader who asked for
	// the primary would give them something staler than they asked for, which
	// is the one direction a cache must not move a read in.
	//
	// The reverse is harmless, and it is not worth the complication of keying by
	// mode to exploit it: a read asking for a secondary is asking to be cheap,
	// not to be stale, and an answer from the primary satisfies it. So these are
	// left out entirely.
	if mode, ok := readPrefMode(cmd); ok && mode != "primary" {
		return "", false
	}

	coll, cok := stringField(cmd, name)
	if !cok {
		// A collectionless aggregate has no collection to invalidate on, so
		// there is nothing to hang a cached answer from.
		return "", false
	}
	ns = db + "." + coll
	// And a namespace nothing can tell us about is the same problem one step
	// further out: there is a collection to hang the answer from, and no route
	// by which this router ever hears that it changed. See cache.NothingCanInvalidate
	// for which ones and for what a lag probe read before this was here.
	if cache.NothingCanInvalidate(ns) {
		return "", false
	}
	return ns, true
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
	// The same fields the advisor's shape ignores, and in the same order.
	//
	// These two decide one thing between them: whether two commands are the
	// same query. The advisor recommends caching a shape and this key is what
	// has to hit for that recommendation to mean anything, so a field the shape
	// joins and the key separates is a query the report calls worth caching and
	// the cache never serves. Measured as a divergence rather than reasoned
	// about: the key carried comment and maxTimeMS and hashed the fields in
	// wire order, where cache.Shape drops both and sorts. Every APM agent sets
	// a distinct comment per request.
	//
	// cursor is the one field the shape drops that this keeps, because it
	// changes the reply rather than the question. Only a reply whose cursor is
	// already closed is stored, so a stored answer is one that fitted in a
	// single batch, and handing it to a caller who asked for a smaller
	// batchSize would put more documents in their first batch than they asked
	// for.
	keys := make([]string, 0, len(elems))
	byKey := make(map[string]bson.RawValue, len(elems))
	for _, e := range elems {
		k := e.Key()
		if sessionFields[k] || k == "$readPreference" {
			continue
		}
		if cache.SkipInShape(k) && k != "cursor" {
			continue
		}
		keys = append(keys, k)
		byKey[k] = e.Value()
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := byKey[k]
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write(v.Value)
		h.Write([]byte{byte(v.Type)})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// identity is who this connection is, for the purpose of never handing one
// client another's answer.
func (c *conn) identity() string {
	if c.grants != nil && c.grants.User != "" {
		return "user:" + c.grants.User
	}
	// Authentication is proxied, so the fast tier decided who this is. It
	// decided in a conversation that came through here, and it said yes, so the
	// name is as good as the one terminate_auth produces. See observeauth.go.
	if c.observedUser != "" {
		return "user:" + c.observedUser
	}
	// Nobody has authenticated, or by a mechanism whose name this cannot read.
	// One connection is then one client, which is narrower than it needs to be
	// and never wrong.
	return "conn:" + strconv.FormatUint(c.id, 10)
}

// cacheLookup answers from the cache when it can, before any guard runs: a
// stored answer is the whole answer, and re-deciding completeness for it would
// be deciding about a read that is not happening.
func (c *conn) cacheLookup(ctx context.Context, msg *wire.Message, name, db string) (*wire.Message, bool) {
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
	// The shape and its parameters are recorded whether or not anything is
	// cached, because the point of the advisor is to say what caching would be
	// worth before anybody turns it on. See package cache.
	shape, params := cache.Shape(name, ns, cmd)
	c.cacheOf = cachePending{live: true, ns: ns, shape: shape, params: params}

	if c.r.cache == nil {
		return nil, false
	}
	// A namespace the archive is not a superset of is one whose fall-through
	// reads are refused, and an answer stored before that began is exactly the
	// short answer the refusal exists to prevent. The comment above this call
	// site says a cached answer is served before any guard runs, which is right
	// for the guards that decide completeness: those are about a read that is
	// not happening. This one is not about the read, it is about the namespace
	// being in a state where no answer of any age is trustworthy.
	//
	// Free on the hot path: the set is read from the router's own cached copy
	// without a round trip, and it is empty in every healthy deployment. See
	// superset.go.
	if c.r.notSupersetCached(ns) > 0 {
		return nil, false
	}
	// Below the check above, not before it. Nothing looks a key up when there
	// is nothing to look it up in, and this is a SHA-256 over the whole command
	// on every read of the hot path.
	key := c.cacheKey(name, ns, cmd)
	if key == "" {
		return nil, false
	}
	c.cacheOf.key = key
	// Worked out here rather than where the answer is stored, because that is
	// the last place with a context to ask the catalog with. Only with the cache
	// on: with the advisor alone nothing is stored, so nothing has to be dropped
	// and this would be a listCollections per read for a number nobody uses.
	c.cacheOf.deps = c.r.cacheDeps(ctx, name, cmd, db, ns)
	stored, hit := c.r.cache.Get(key)
	if !hit {
		return nil, false
	}
	out, rerr := replyAddressedTo(stored, msg.Header.RequestID)
	if rerr != nil {
		return nil, false
	}
	// Nothing is learned from an answer this cache produced itself.
	//
	// The reply goes back through the same funnel every answer does, and that
	// funnel learns from what it sends. Left alone, every hit was recorded as a
	// fresh observation of the same value: the entry's age was reset, so a
	// query asked steadily was never re-fetched at all. Measured with a two
	// second ceiling and thirteen seconds of reads, the router asked the fast
	// tier four times and served forty one from memory, when it should have
	// gone back roughly every two seconds. The TTL was not bounding anything.
	//
	// Stability has to be measured against the database, not against a copy of
	// what this already believed. The execution still counts, because how often
	// a query is asked is the other half of what makes it worth caching.
	c.cacheOf = cachePending{}
	if c.r.advisor != nil {
		c.r.advisor.ObserveHit(shape, ns, params)
	}
	c.cls = ClassCached
	c.log.Debug("answered from the cache", "ns", ns, "command", name)
	return out, true
}

// cacheStore learns from an answer on its way back to the client.
func (c *conn) cacheStore(reply *wire.Message) {
	pending := c.cacheOf
	c.cacheOf = cachePending{}
	if !pending.live || reply == nil {
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
	if c.r.cache != nil && pending.key != "" {
		// The stored bytes are the whole reply, because that is what goes back
		// to the client; the hash is of the answer inside it, because that is
		// what stability means.
		deps := pending.deps
		if len(deps) == 0 {
			deps = []string{pending.ns}
		}
		c.r.cache.ObserveResultOn(pending.key, deps, reply.Raw, rhash)
	}
}

// cacheInvalidate drops what was learned about a collection this command just
// wrote to.
func (c *conn) cacheInvalidate(name, db string, cmd bson.Raw) {
	if c.r.cache == nil {
		return
	}
	for _, ns := range writtenNamespaces(name, db, cmd) {
		// A whole database, which is what a dropDatabase and an unreadable
		// target both come back as: forgetting too much costs a lookup and
		// forgetting too little costs a wrong answer.
		if strings.HasSuffix(ns, ".*") {
			if n := c.r.cache.InvalidateDatabase(strings.TrimSuffix(ns, ".*")); n > 0 {
				c.log.Debug("a command dropped cached answers for a whole database",
					"db", strings.TrimSuffix(ns, ".*"), "entries", n)
			}
			continue
		}
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
	case "renameCollection":
		// It names both sides in full, as db.collection, because it is run
		// against admin. Prepending the database again produced
		// "admin.shop.orders", which is nobody's namespace, so neither side was
		// dropped. The change stream covers a rename as well, so this was only
		// ever the hole when that is not running.
		var out []string
		if from, ok := stringField(cmd, "renameCollection"); ok {
			out = append(out, from)
		}
		if to, ok := lookupString(cmd, "to"); ok {
			out = append(out, to)
		}
		return out
	case "drop", "create", "collMod", "createIndexes", "dropIndexes":
		// DDL changes what a query would answer as surely as a write does.
		if coll, ok := stringField(cmd, name); ok {
			return []string{db + "." + coll}
		}
		return []string{db + ".*"}
	case "dropDatabase":
		// Everything in it, which Invalidate cannot express: the caller expands
		// this one.
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
	// live says a read is in flight that the reply should be learned from.
	//
	// Its own field rather than a non-empty key, which is what it used to be.
	// The key is only worth computing when there is a cache to look it up in,
	// and the advisor runs whether or not there is one: with a key standing in
	// for this, every deployment that had not turned the cache on was paying a
	// SHA-256 of every read for a string nothing went on to use.
	live   bool
	key    string
	ns     string
	shape  string
	params uint64
	// deps is every collection this answer depends on, which an entry is hung
	// from so that a change to any of them drops it. See cacheDeps.
	deps []string
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

// adviceShutdownWait bounds the last report. Long enough for a write to a
// healthy archive, short enough that an unreachable one does not hold a
// shutdown open.
const adviceShutdownWait = 5 * time.Second

// reportAdvice keeps the published report current while the router runs.
func (r *Router) reportAdvice(ctx context.Context) {
	t := time.NewTicker(adviceEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// One last report on the way out, so a router stopped after an
			// hour of traffic does not take what it learned with it. The
			// context is detached because the cancellation that brought us
			// here would otherwise cancel this write too, and bounded because
			// Run is waiting on it.
			pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adviceShutdownWait)
			r.publishAdvice(pctx)
			cancel()
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

// cacheDeps is every collection an answer depends on, so an entry can be hung
// from all of them and dropped when any one changes.
//
// The namespace a command names is not the whole of what it read. A read of a
// view names the view, and its answer comes from the collection underneath and
// from whatever the view's own pipeline joins. An aggregation names the
// collection it runs against, and a $lookup in it reads another. Every
// invalidation this cache gets arrives under a collection: the router's own
// write path names it, and so does the fast tier's change stream. So an entry
// hung only from the name in the command was never dropped by a write to the
// data it was actually made of, and stood until its learned TTL ran out.
//
// That is the one thing this cache is documented not to do. See the top of this
// file: a write followed by a read of the same collection is correct
// immediately, and what the router cannot see is another writer.
func (r *Router) cacheDeps(ctx context.Context, name string, cmd bson.Raw, db, ns string) []string {
	deps := []string{ns}
	add := func(more ...string) {
		for _, m := range more {
			if m == "" || m == ns {
				continue
			}
			deps = append(deps, m)
		}
	}
	// Through a view: every hop of the chain, not only the collection at the
	// end of it. A view in the middle is dropped and recreated by its own name,
	// and an entry not hung from that name would survive its definition
	// changing. And whatever the views along the way join. An unreadable
	// definition needs nothing here, because a read of one is refused before it
	// can be answered at all.
	at := ns
	for hop := 0; hop < maxViewHops; hop++ {
		on, isView := r.viewOn(ctx, at)
		if !isView {
			break
		}
		add(on)
		at = on
	}
	if joined, ok := r.viewJoins(ctx, ns); ok {
		for _, j := range joined {
			add(j)
			if jb, jIsView := r.viewBase(ctx, j); jIsView {
				add(jb)
			}
		}
	}
	if name == "aggregate" {
		if stages, sok := pipelineStages(cmd); sok {
			// The walk's own "I saw everything" answer is discarded, and that
			// is safe for one reason: a pipeline deep enough to make it
			// incomplete is one mongod refuses outright, so there is no answer
			// to cache. See maxSubPipelineDepth, which is what keeps it true.
			joined, _ := joinedCollections(stages, db, 0)
			for _, j := range joined {
				add(j)
				// A join may name a view too, and then the documents are one
				// more hop away.
				if jb, jIsView := r.viewBase(ctx, j); jIsView {
					add(jb)
				}
			}
		}
	}
	return deps
}
