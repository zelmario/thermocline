package router

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// Answering from the archive is what keeps the promise that no application
// changes. A query the fast tier cannot answer in full used to be refused, and
// a refused countDocuments is a broken application however safe the refusal is.
//
// It works because the archive is not a partial complement of the fast tier, it
// is a complete MongoDB holding every document. So there is no merging of two
// half-answers: the query runs on one tier or the other, whole.
//
// The one difference a caller can observe is freshness. The archive is behind
// the fast tier by the feeder's mirroring lag, normally well under a second, so
// a read that falls through here has the same staleness as a secondary read.
// That is a timing difference, not an API difference, and no application has to
// be rewritten for it.

// firstBatchDefault matches the server's own default, so a driver that did not
// ask for a batch size sees what it would see from mongod.
const firstBatchDefault = 101

// maxBatchBytes bounds one batch of an archive answer.
//
// Eight megabytes, not sixteen, so that the one document allowed to cross the
// budget cannot push the reply near the wire protocol's message limit: the
// largest BSON document is sixteen megabytes, and eight plus sixteen is still
// comfortably under the forty-eight a message may occupy.
const maxBatchBytes = 8 * 1024 * 1024

// sessionFields belong to the client's conversation with the fast tier. Sending
// them to the archive would attach this read to a session and a transaction
// that exist on a different cluster.
var sessionFields = map[string]bool{
	"lsid": true, "txnNumber": true, "autocommit": true, "startTransaction": true,
	"$clusterTime": true, "$db": true, "readConcern": true, "writeConcern": true,
	"$readPreference": true, "apiVersion": true, "apiStrict": true, "apiDeprecationErrors": true,
}

// cursorOwner is who a cursor belongs to.
//
// A cursor this router owns is served from a registry shared by every
// connection, because that is what a real MongoDB cursor is: server-scoped, so
// a driver's pool may send the getMore from whichever connection is free.
// Nothing else scoped it, and the ids were handed out in sequence from a known
// starting point, so any client could ask for 2^62+1 and be given the next
// batch of somebody else's read. Measured: a second connection that had opened
// nothing named that id, named a collection of its own alongside it, and was
// answered with the first client's documents and their real namespace.
//
// mongod's rule, which this follows: "If authentication is enabled, you can
// only run getMore against cursors you created. Additionally, for cursors
// created inside a session, you cannot call getMore outside the session, and
// for cursors created outside a session, you cannot call getMore inside a
// session."
//
// The user is known only when this router terminates authentication. The
// session is known whenever the client sends one, which every driver since 3.6
// does for every command. When there is neither, the cursor is scoped to the
// connection that opened it, which is stricter than mongod: that combination
// means a deployment with no authentication and a driver old enough to have no
// sessions, and the alternative there is handing one client's documents to
// another.
type cursorOwner struct {
	user string
	lsid string
	conn *conn
}

// differsFrom names the scope that does not match, for the log.
//
// The refusal is word for word mongod's, and the commonest cause of it is not a
// client reading somebody else's cursor at all: two runCommand calls each take
// an implicit session from the driver's pool, so a find and its getMore can
// legitimately arrive in different sessions. The pool usually hands back the
// same one, which is why it surfaces as something that passes for months and
// then fails under load. Saying which scope differed is the difference between
// that being a minute's work and an afternoon's.
func (o cursorOwner) differsFrom(other cursorOwner) string {
	switch {
	case o.user != other.user:
		return "a different authenticated user"
	case o.lsid != "" || other.lsid != "":
		return "a different session, which is also what two runCommand calls " +
			"taking their own implicit session from the driver's pool look like"
	case o.user != "":
		return "nothing: same user, no session"
	default:
		return "a different connection, and neither side named a session or a user"
	}
}

// sameAs reports whether a request may read a cursor this owner opened.
func (o cursorOwner) sameAs(other cursorOwner) bool {
	if o.user != other.user {
		return false
	}
	if o.lsid != "" || other.lsid != "" {
		return o.lsid == other.lsid
	}
	if o.user != "" {
		// Authenticated and sessionless: the user is the whole of the scope,
		// as it is in mongod.
		return true
	}
	return o.conn == other.conn
}

// owner is who this connection is, for the purpose of opening and reading
// cursors.
func (c *conn) owner(cmd bson.Raw) cursorOwner {
	o := cursorOwner{conn: c}
	if c.grants != nil {
		o.user = c.grants.User
	}
	if v, err := cmd.LookupErr("lsid"); err == nil {
		if doc, ok := v.DocumentOK(); ok {
			o.lsid = string(doc)
		}
	}
	return o
}

// archiveCursor is a driver cursor we are serving getMore from.
//
// The mutex is not decoration. A driver may issue two getMore calls for the
// same cursor from two connections in its pool, and a mongo.Cursor is not safe
// for concurrent use: two readers would interleave batches and hand the client
// a mangled result, or race the close. One reader at a time per cursor.
type archiveCursor struct {
	mu       sync.Mutex
	cur      *mongo.Cursor
	ns       string
	owner    cursorOwner
	lastUsed time.Time
	closed   bool
}

// read pulls the next batch under the cursor's own lock.
func (ac *archiveCursor) read(ctx context.Context, size int) (bson.A, bool, error) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if ac.closed {
		return bson.A{}, false, nil
	}
	return readBatch(ctx, ac.cur, size)
}

// close is safe to call more than once, and waits for any read in flight.
func (ac *archiveCursor) close(ctx context.Context) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if ac.closed {
		return
	}
	ac.closed = true
	_ = ac.cur.Close(ctx)
}

// cursorRegistry hands out ids for cursors this router owns rather than the
// fast tier.
//
// It belongs to the router, not to one client connection. A real MongoDB cursor
// is server-scoped: any connection in a driver's pool may issue the getMore, and
// drivers do exactly that. Keeping the registry per connection worked in a shell
// and would have failed against a pooled application driver, with the getMore
// falling through to a fast tier that has never heard of the cursor.
//
// Ownership is decided by the registry and by nothing else. Magnitude says
// nothing: measured on PSMDB 8.0, mongod hands out cursor ids across the whole
// positive int64 range, and three of five samples were above 2^62. So an id is
// ours if it is in this map, and a real cursor id landing exactly on a live
// synthetic one would need a random int64 collision. The worst case there is a
// getMore sent to the wrong tier, which errors rather than returning wrong data.
type cursorRegistry struct {
	mu   sync.Mutex
	next int64
	open map[int64]*archiveCursor
}

func (reg *cursorRegistry) add(ac *archiveCursor) int64 {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.open == nil {
		reg.open = make(map[int64]*archiveCursor)
	}
	// Random, so that nobody can name one they were not given. They used to run
	// in sequence from a fixed start, which meant the first cursor was always
	// 2^62+1 and the second always 2^62+2. The top bit is set to keep them clear
	// of zero and of the small ids a driver might treat specially; it does not
	// separate them from mongod's own, which cover the whole range. Ownership is
	// the registry's answer, and this is the half that costs nothing.
	var id int64
	for {
		id = 1<<62 | (randomInt63() >> 1)
		if _, taken := reg.open[id]; !taken && id > 0 {
			break
		}
	}
	ac.lastUsed = time.Now()
	reg.open[id] = ac
	return id
}

// randomInt63 returns a non-negative random int64.
func randomInt63() int64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail in practice, and a predictable id is worth
		// refusing to produce, so fall back to something that still cannot be
		// counted off: the clock, which an attacker would have to guess to the
		// nanosecond.
		return time.Now().UnixNano()
	}
	return int64(binary.BigEndian.Uint64(b[:]) &^ (1 << 63))
}

func (reg *cursorRegistry) get(id int64) (*archiveCursor, bool) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	ac, ok := reg.open[id]
	if ok {
		ac.lastUsed = time.Now()
	}
	return ac, ok
}

// reapIdle closes cursors nobody has come back for.
//
// A client that abandons a cursor without killing it would otherwise pin an
// archive cursor forever. Ten minutes matches the server's own default timeout,
// so an application sees the same behaviour it would from mongod.
func (reg *cursorRegistry) reapIdle(ctx context.Context, idle time.Duration) {
	var dead []*archiveCursor
	reg.mu.Lock()
	for id, ac := range reg.open {
		if time.Since(ac.lastUsed) > idle {
			dead = append(dead, ac)
			delete(reg.open, id)
		}
	}
	reg.mu.Unlock()
	for _, ac := range dead {
		ac.close(ctx)
	}
}

func (reg *cursorRegistry) drop(ctx context.Context, id int64) {
	reg.mu.Lock()
	ac, ok := reg.open[id]
	delete(reg.open, id)
	reg.mu.Unlock()
	if ok {
		ac.close(ctx)
	}
}

func (reg *cursorRegistry) closeAll(ctx context.Context) {
	reg.mu.Lock()
	open := reg.open
	reg.open = nil
	reg.mu.Unlock()
	for _, ac := range open {
		ac.close(ctx)
	}
}

// answerFromArchive runs a find or aggregate against the archive and returns the
// reply the client would have got from a single complete cluster.
//
// count says whether to record this as an archive read. A point lookup passes
// false and records its own outcome instead: whether it was a miss that will be
// promoted or an ordinary archive read is not known until the answer is in
// hand, and the two are different numbers an operator watches. See Class.
func (c *conn) answerFromArchive(ctx context.Context, msg *wire.Message, db, coll string, count bool) (*wire.Message, error) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, err
	}
	// Before the archive is asked anything, because what is wrong here is the
	// archive's answer rather than the question. See superset.go.
	if out, refused := c.refuseIfNotSuperset(ctx, db+"."+coll,
		msg.Header.RequestID, msg.Header.RequestID); refused {
		return out, nil
	}
	collation, _ := c.r.defaultCollation(ctx, db+"."+coll)
	// Resolved here rather than inside the rewrite, because it needs the fast
	// tier's index list and the rewrite is a pure function of the command.
	hintPred, keepHint := c.r.hintPredicate(ctx, db+"."+coll, cmd)
	rewritten, batchSize, err := rewriteForArchive(cmd, collation, hintPred, keepHint)
	if err != nil {
		return nil, err
	}

	// Every path that answers from the archive waits here, rather than each
	// caller remembering to. A find waited and an aggregation did not, and
	// countDocuments is an aggregation, so a document inserted a moment earlier
	// was found by find and not counted by countDocuments.
	budget := maxTimeBudget(cmd)
	waitedFrom := time.Now()
	c.archiveReadIsFresh(ctx, db+"."+coll, waitedFrom, budget)

	// The archive gets what is left of the caller's deadline, not the whole of
	// it a second time.
	//
	// The wait above is bounded by maxTimeMS, and the command carried maxTimeMS
	// onward unchanged, so a caller could be charged its deadline twice.
	// Measured: a client asking for 800ms with mirroring behind waited 300ms
	// here, handed the archive 800ms of its own, and got a successful answer
	// after 1117ms. Being told a deadline does not apply here is not this
	// proxy's decision to make.
	rewritten = withRemainingTime(rewritten, budget, time.Since(waitedFrom))

	cur, err := c.r.cold.Database(db).RunCommandCursor(ctx, rewritten)
	if err != nil {
		return nil, err
	}

	ns := db + "." + coll
	batch, more, err := readBatch(ctx, cur, batchSize)
	if err != nil {
		_ = cur.Close(ctx)
		return nil, err
	}

	var id int64
	if more {
		id = c.r.cursors.add(&archiveCursor{cur: cur, ns: ns, owner: c.owner(cmd)})
	} else {
		_ = cur.Close(ctx)
	}

	if count {
		c.r.Stats.Archived.Add(1)
		c.cls = ClassArchive
	}
	return cursorReply(msg.Header.RequestID, msg.Header.RequestID, id, ns, "firstBatch", batch)
}

// serveOwnCursors answers getMore and killCursors for cursors this router
// created, which the fast tier has never heard of.
func (c *conn) serveOwnCursors(ctx context.Context, msg *wire.Message, name string) (bool, error) {
	if name != "getMore" && name != "killCursors" {
		return false, nil
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return false, nil
	}

	if name == "killCursors" {
		if !c.archiveKillCursors(ctx, cmd) {
			// Some or all belong to the fast tier, so let it do the work. Ours
			// are already closed, and killing a closed cursor is not an error.
			return false, nil
		}
		out, err := wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, bson.D{{Key: "ok", Value: 1.0}})
		if err != nil {
			return false, nil
		}
		return true, c.send(out)
	}

	reply, mine, err := c.archiveGetMore(ctx, msg, cmd)
	if !mine {
		return false, nil
	}
	if err != nil {
		c.log.Warn("archive getMore failed", "err", err)
		fail, ferr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: reading the next batch from the archive failed: "+err.Error())
		if ferr != nil {
			return true, ferr
		}
		return true, c.send(fail)
	}
	return true, c.send(reply)
}

// archiveGetMore serves the next batch of a cursor this router owns. The second
// result reports whether the cursor was ours at all.
func (c *conn) archiveGetMore(ctx context.Context, msg *wire.Message, cmd bson.Raw) (*wire.Message, bool, error) {
	idVal, err := cmd.LookupErr("getMore")
	if err != nil {
		return nil, false, nil
	}
	id, ok := idVal.Int64OK()
	if !ok {
		return nil, false, nil
	}
	ac, ok := c.r.cursors.get(id)
	if !ok {
		return nil, false, nil
	}
	// The name has to be the cursor's name. mongod has refused a mismatch since
	// at least 6.0, measured: "Requested getMore on namespace 'probe.wrongname',
	// but cursor belongs to a different namespace probe.plain", code 13. This
	// answered with the cursor's documents under the cursor's namespace, which
	// is not a wrong answer but is a different one, and it matters beyond
	// conformance: the authorization gate judges the collection the client
	// named, so accepting a name the cursor does not have means the check and
	// the documents are about two different collections.
	if named, ok := stringField(cmd, "collection"); ok {
		_, wantColl := splitNS(ac.ns)
		if named != wantColl {
			out, cerr := c.wrongCursorNamespace(msg, db(cmd)+"."+named, ac.ns)
			return out, true, cerr
		}
	}

	// A cursor belongs to whoever opened it. Naming somebody else's id is how a
	// client read another client's documents, and the command's own collection
	// name is no protection: the batch comes from the cursor, not from the name.
	if !ac.owner.sameAs(c.owner(cmd)) {
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a getMore for a cursor this client did not open",
			"ns", ac.ns, "cursor", id,
			"differs_by", ac.owner.differsFrom(c.owner(cmd)),
			"note", "mongod refuses the same getMore for the same reason; a client that means to "+
				"continue its own cursor should run the find and the getMore in one explicit session")
		out, cerr := c.notYourCursor(msg, id)
		return out, true, cerr
	}

	size := firstBatchDefault
	if v, err := cmd.LookupErr("batchSize"); err == nil {
		if n, ok := intOf(v); ok && n > 0 {
			size = n
		}
	}

	batch, more, err := ac.read(ctx, size)
	if err != nil {
		c.r.cursors.drop(ctx, id)
		return nil, true, err
	}
	out := id
	if !more {
		c.r.cursors.drop(ctx, id)
		out = 0
	}
	reply, err := cursorReply(msg.Header.RequestID, msg.Header.RequestID, out, ac.ns, "nextBatch", batch)
	return reply, true, err
}

// wrongCursorNamespace answers a getMore whose collection is not the cursor's,
// word for word as mongod does.
func (c *conn) wrongCursorNamespace(msg *wire.Message, asked, actual string) (*wire.Message, error) {
	return wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: fmt.Sprintf(
			"Requested getMore on namespace '%s', but cursor belongs to a different namespace %s",
			asked, actual)},
		{Key: "code", Value: int32(13)},
		{Key: "codeName", Value: "Unauthorized"},
	})
}

// db reads the database a command was sent to.
func db(cmd bson.Raw) string {
	if v, err := cmd.LookupErr("$db"); err == nil {
		if s, ok := v.StringValueOK(); ok {
			return s
		}
	}
	return ""
}

// notYourCursor answers the way mongod does when a getMore names a cursor the
// caller did not open, so a driver sees the error it already handles.
func (c *conn) notYourCursor(msg *wire.Message, id int64) (*wire.Message, error) {
	return wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: fmt.Sprintf(
			"cursor id %d was not created by the authenticated user or was created in another session", id)},
		{Key: "code", Value: int32(13)},
		{Key: "codeName", Value: "Unauthorized"},
	})
}

// archiveKillCursors closes any of the named cursors that are ours, and reports
// whether all of them were.
func (c *conn) archiveKillCursors(ctx context.Context, cmd bson.Raw) bool {
	val, err := cmd.LookupErr("cursors")
	if err != nil {
		return false
	}
	arr, ok := val.ArrayOK()
	if !ok {
		return false
	}
	vals, err := arr.Values()
	if err != nil || len(vals) == 0 {
		return false
	}
	all := true
	for _, v := range vals {
		id, ok := v.Int64OK()
		if !ok {
			all = false
			continue
		}
		if ac, mine := c.r.cursors.get(id); mine {
			// Killing is scoped like reading. Otherwise one client could close
			// another's cursors by counting through ids, and the owner would
			// see its read stop halfway with no explanation.
			if !ac.owner.sameAs(c.owner(cmd)) {
				c.log.Warn("ignored a killCursors for a cursor this client did not open",
					"ns", ac.ns, "cursor", id)
				// mongod reports an id it will not act on under cursorsNotFound
				// rather than failing the command, and this is ours to answer,
				// so say it is not here.
				continue
			}
			c.r.cursors.drop(ctx, id)
			continue
		}
		all = false
	}
	return all
}

// readBatch pulls up to size documents, reporting whether more remain.
func readBatch(ctx context.Context, cur *mongo.Cursor, size int) (bson.A, bool, error) {
	if size < 0 {
		size = firstBatchDefault
	}
	if size == 0 {
		// The caller asked for a cursor and no documents. Report more so it is
		// given one to come back to.
		return bson.A{}, true, nil
	}
	batch := bson.A{}
	used := 0
	for len(batch) < size {
		if !cur.Next(ctx) {
			return batch, false, cur.Err()
		}
		var d bson.D
		if err := bson.Unmarshal(cur.Current, &d); err != nil {
			return batch, false, err
		}
		batch = append(batch, d)

		// A batch is bounded by bytes as well as by documents, which is what
		// mongod itself does and what the count alone does not. A client asking
		// for ten thousand documents of a megabyte each would otherwise have
		// this assemble one reply of ten gigabytes: past the wire protocol's
		// limit, and large enough to take the router down and every connection
		// on it with it.
		//
		// The document that crosses the budget is kept rather than dropped,
		// because the cursor has already yielded it and there is nowhere to put
		// it back. The budget is set low enough that doing so cannot approach
		// the message limit even when that document is the largest BSON allows.
		used += len(cur.Current)
		if used >= maxBatchBytes {
			return batch, true, nil
		}
	}
	// RemainingBatchLength is cheap and does not block; TryNext would fetch.
	return batch, cur.RemainingBatchLength() > 0 || cur.ID() != 0, nil
}

// cursorReply builds the reply shape a driver expects from find, aggregate and
// getMore.
func cursorReply(requestID, responseTo int32, id int64, ns, field string, batch bson.A) (*wire.Message, error) {
	return wire.NewMsg(requestID, responseTo, bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: id},
			{Key: "ns", Value: ns},
			{Key: field, Value: batch},
		}},
		{Key: "ok", Value: 1.0},
	})
}

// rewriteForArchive prepares a client's read for the archive.
//
// Two changes. Tombstoned documents are excluded, because the archive keeps
// records the application deleted and returning one would resurrect it. And the
// client's session and transaction fields are dropped, because they name a
// conversation happening on the other cluster.
func rewriteForArchive(cmd bson.Raw, collation bson.Raw, hintPred bson.D, keepHint bool) (bson.D, int, error) {
	elems, err := cmd.Elements()
	if err != nil {
		return nil, 0, err
	}

	out := make(bson.D, 0, len(elems)+4)
	batchSize := firstBatchDefault
	sawFilter, sawPipeline, sawDiskUse := false, false, false
	sawCollation := false

	// Linearizable survives nothing: it promises a read confirmed on the
	// primary that owns the document, which another cluster cannot say. Every
	// path that answers from the archive comes through here, so refusing here
	// covers all of them.
	level, honourable := archiveLevel(cmd)
	if !honourable {
		return nil, 0, errors.New("readConcern linearizable cannot be answered from the archive: " +
			"it promises a read confirmed on the primary that owns the document; " +
			"warm this range, or read it with majority")
	}

	for _, e := range elems {
		key := e.Key()
		if sessionFields[key] {
			continue
		}
		val := e.Value()

		switch key {
		case "hint":
			// A hint names an index, and the archive is expected to carry
			// fewer of them: the rollout guide tells operators to drop the
			// ones the cheap tier does not need. Passing it on turns a
			// perfectly good query into "hint provided does not correspond to
			// an existing index" the first time it falls through.
			//
			// For an ordinary index that is the whole story, because such a
			// hint chooses how the query is answered and not what the answer
			// is. Two cases are not ordinary and both are handled in hint.go:
			// a sparse or partial index holds only part of the collection, so
			// hinting one narrows the documents and the narrowing is put back
			// as a filter; and min or max are bounds on an index key, so the
			// hint that names the key has to travel with them.
			if keepHint || hintIsLoadBearing(cmd) {
				break
			}
			continue
		case "filter":
			sawFilter = true
			out = append(out, bson.E{Key: "filter", Value: andPredicate(liveOnly(val), hintPred)})
			continue
		case "pipeline":
			sawPipeline = true
			out = append(out, bson.E{Key: "pipeline", Value: matchFirst(livePipeline(val), hintPred)})
			continue
		case "batchSize":
			// Zero is a request, not an absence: a driver asks for it to get a
			// cursor back without any documents yet, so answering with the
			// default hundred and one is not what it asked for.
			if n, ok := intOf(val); ok && n >= 0 {
				batchSize = n
			}
		case "cursor":
			if doc, ok := val.DocumentOK(); ok {
				if n, ok := intOf(doc.Lookup("batchSize")); ok && n >= 0 {
					batchSize = n
				}
			}
		case "allowDiskUse":
			// The caller said so themselves; theirs wins, and appending ours
			// as well would put the same field in the document twice.
			sawDiskUse = true
		case "collation":
			// An explicit collation travels with the command already.
			sawCollation = true
		}
		out = append(out, bson.E{Key: key, Value: val})
	}

	// A find with no filter at all still has to exclude tombstones.
	if _, isFind := lookupString(cmd, "find"); isFind && !sawFilter {
		out = append(out, bson.E{Key: "filter",
			Value: andPredicate(bson.D{meta.NotArchivedDeleted()}, hintPred)})
	}
	if _, isAgg := lookupString(cmd, "aggregate"); isAgg && !sawPipeline {
		out = append(out, bson.E{Key: "pipeline", Value: matchFirst(bson.A{
			bson.D{{Key: "$match", Value: bson.D{meta.NotArchivedDeleted()}}},
		}, hintPred)})
	}

	// Let the archive spill a sort to disk.
	//
	// The fast tier answers a sorted query from an index. The archive is
	// expected not to have that index, because the rollout guide tells
	// operators to drop the ones it does not need, so the same query becomes a
	// blocking sort over the whole collection. Past the server's sort memory
	// limit that does not get slower, it fails, and the caller sees a query
	// that worked before adoption and does not after.
	//
	// Spilling is the right trade on the tier that exists to be cheap and slow.
	// An operator who would rather refuse expensive fallback reads than run
	// them already has router.completeness: strict, which refuses them all.
	if !sawDiskUse {
		if _, isFind := lookupString(cmd, "find"); isFind {
			out = append(out, bson.E{Key: "allowDiskUse", Value: true})
		} else if _, isAgg := lookupString(cmd, "aggregate"); isAgg {
			out = append(out, bson.E{Key: "allowDiskUse", Value: true})
		}
	}

	// The collection's own collation decides which documents match, and the
	// archive's copy of the collection has none, because the feeder created it
	// by upserting into it. Saying it explicitly makes the archive match the
	// way the fast tier would have.
	if !sawCollation && len(collation) > 0 {
		if c := simplifyCollation(collation); len(c) > 0 {
			out = append(out, bson.E{Key: "collation", Value: c})
		}
	}

	// The level carries over; afterClusterTime and atClusterTime do not, because
	// they name instants on the fast tier's timeline.
	//
	// Appended last, never first. The opening element of a command document is
	// the command name, so putting readConcern at the front produced a command
	// called "readConcern", which the archive rejected, which made every
	// fallback read look like a query the router had refused.
	if rc, ok := archiveReadConcern(level); ok {
		out = append(out, bson.E{Key: "readConcern", Value: rc})
	}
	return out, batchSize, nil
}

// liveOnly wraps a filter so it cannot match a tombstone.
func liveOnly(filter bson.RawValue) bson.D {
	doc, ok := filter.DocumentOK()
	if !ok || len(doc) == 0 {
		return bson.D{meta.NotArchivedDeleted()}
	}
	// $and rather than adding a field, because the original may already
	// constrain the tombstone field or use $or at the top level.
	return bson.D{{Key: "$and", Value: bson.A{doc, bson.D{meta.NotArchivedDeleted()}}}}
}

// livePipeline puts a tombstone filter in front of everything else, so no stage
// ever sees a deleted record.
//
// Everything includes the stages that read a collection of their own. A
// $lookup, a $graphLookup and a $unionWith do not read the pipeline's input,
// they read another collection, and a $match in front of the pipeline never
// reaches them. The archive keeps a deleted document as a tombstone, so such a
// stage joins documents the client deleted.
//
// Found by the differential harness and then measured directly: sixty
// documents in three groups, five deleted through the router, then a self join
// on the group. The untiered reference answered 19, 18 and 18 siblings and the
// tiered collection answered 20, 20 and 20, and it stayed wrong after the
// feeder had caught up, because the tombstones are what it was reading. Moving
// two documents between groups then had the tiered side count each of them in
// both groups. The outer side of the same pipeline was right throughout, which
// is what made this look like a working feature.
func livePipeline(pipeline bson.RawValue) bson.A {
	matchLive := bson.D{{Key: "$match", Value: bson.D{meta.NotArchivedDeleted()}}}
	arr, ok := pipeline.ArrayOK()
	if !ok {
		return bson.A{matchLive}
	}
	vals, err := arr.Values()
	if err != nil {
		return bson.A{matchLive}
	}
	out := bson.A{}
	if !startsWithGeoNear(vals) {
		out = append(out, matchLive)
	}
	for _, v := range vals {
		if doc, ok := v.DocumentOK(); ok {
			out = append(out, liveStage(doc, tombstoneRewriteDepth))
		}
	}
	return out
}

// startsWithGeoNear reports whether a stage that has to come first does.
//
// One stage in the whole language cannot have anything put in front of it and
// also reads the input documents, and this is the test for it. $collStats and
// $indexStats have the same rule and are not here because they read no
// documents, so no tombstone can reach them; $changeStream has it too and never
// travels this path.
func startsWithGeoNear(vals []bson.RawValue) bool {
	if len(vals) == 0 {
		return false
	}
	doc, ok := vals[0].DocumentOK()
	if !ok {
		return false
	}
	elems, err := doc.Elements()
	if err != nil || len(elems) != 1 {
		return false
	}
	return elems[0].Key() == "$geoNear"
}

// liveGeoNear puts the tombstone filter into a $geoNear's own query.
func liveGeoNear(spec bson.Raw) (bson.D, bool) {
	elems, err := spec.Elements()
	if err != nil {
		return nil, false
	}
	out := make(bson.D, 0, len(elems)+1)
	sawQuery := false
	for _, e := range elems {
		if e.Key() == "query" {
			out = append(out, bson.E{Key: "query", Value: liveOnly(e.Value())})
			sawQuery = true
			continue
		}
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	if !sawQuery {
		out = append(out, bson.E{Key: "query", Value: bson.D{meta.NotArchivedDeleted()}})
	}
	return out, true
}

// tombstoneRewriteDepth bounds the recursion below. A stage nested this deep
// cannot reach here: one pipeline level costs several levels of BSON nesting
// and mongod refuses a document nested past a hundred, so a command carrying
// one was never accepted in the first place.
const tombstoneRewriteDepth = 100

// liveStage rewrites one stage so that a collection it reads of its own is read
// without tombstones. A stage that reads no collection is returned untouched.
func liveStage(stage bson.Raw, depth int) any {
	if depth <= 0 {
		return stage
	}
	elems, err := stage.Elements()
	if err != nil || len(elems) != 1 {
		return stage
	}
	name, val := elems[0].Key(), elems[0].Value()
	switch name {
	case "$geoNear":
		// $geoNear must be the first stage in the pipeline, so the tombstone
		// filter cannot go in front of it. It takes a `query` of its own, which
		// is what that filter is, and which is why this is a rewrite rather
		// than a refusal.
		//
		// Measured before this, with a 2dsphere index on both tiers and nothing
		// claiming the namespace: the same pipeline answered 3 straight at the
		// archive and failed through the router with "$geoNear was not the
		// first stage in the pipeline after optimization", which names the
		// router's own $match and reads as the client's mistake.
		spec, ok := liveGeoNear(subDoc(val))
		if !ok {
			return stage
		}
		return bson.D{{Key: name, Value: spec}}
	case "$lookup":
		spec, ok := liveJoin(subDoc(val), "from", depth)
		if !ok {
			return stage
		}
		return bson.D{{Key: name, Value: spec}}
	case "$unionWith":
		// Two spellings. {$unionWith: "other"} names a collection and takes no
		// pipeline, so one has to be given to it.
		if coll, ok := val.StringValueOK(); ok {
			return bson.D{{Key: name, Value: bson.D{
				{Key: "coll", Value: coll},
				{Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{meta.NotArchivedDeleted()}}}}},
			}}}
		}
		spec, ok := liveJoin(subDoc(val), "coll", depth)
		if !ok {
			return stage
		}
		return bson.D{{Key: name, Value: spec}}
	case "$graphLookup":
		spec, ok := liveGraphLookup(subDoc(val))
		if !ok {
			return stage
		}
		return bson.D{{Key: name, Value: spec}}
	case "$facet":
		// Each field is a pipeline over the same input documents, which the
		// $match in front of the outer pipeline has already filtered. Only the
		// stages inside them that read a collection of their own need anything.
		fields, ferr := subDoc(val).Elements()
		if ferr != nil {
			return stage
		}
		out := make(bson.D, 0, len(fields))
		for _, f := range fields {
			stages, ok := liveSubPipeline(f.Value(), depth-1, false)
			if !ok {
				return stage
			}
			out = append(out, bson.E{Key: f.Key(), Value: stages})
		}
		return bson.D{{Key: name, Value: out}}
	}
	return stage
}

// liveJoin rewrites the specification of a stage that joins another collection,
// named by the field collKey. The tombstone filter goes at the head of the
// stage's own pipeline, which both $lookup and $unionWith accept alongside a
// named collection.
//
// A stage naming no collection reads the documents its pipeline produces, with
// $documents at the head of it. There is nothing to filter, and a $match put in
// front of $documents would make the pipeline invalid.
//
// Returns false for anything it cannot read, so the stage travels unchanged and
// the server rejects it. Rewriting a malformed stage into a valid one would
// answer a command MongoDB refuses, which is its own kind of disagreement.
func liveJoin(spec bson.Raw, collKey string, depth int) (bson.D, bool) {
	elems, err := spec.Elements()
	if err != nil {
		return nil, false
	}
	_, named := stringField(spec, collKey)
	out := make(bson.D, 0, len(elems)+1)
	var sub bson.RawValue
	hasSub := false
	for _, e := range elems {
		if e.Key() == "pipeline" {
			sub, hasSub = e.Value(), true
			continue
		}
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	switch {
	case hasSub:
		stages, ok := liveSubPipeline(sub, depth-1, named)
		if !ok {
			return nil, false
		}
		out = append(out, bson.E{Key: "pipeline", Value: stages})
	case named:
		out = append(out, bson.E{Key: "pipeline",
			Value: bson.A{bson.D{{Key: "$match", Value: bson.D{meta.NotArchivedDeleted()}}}}})
	}
	return out, true
}

// liveGraphLookup filters what a $graphLookup traverses. It takes no pipeline,
// so the only place to say this is restrictSearchWithMatch, which is applied to
// every document the search considers.
func liveGraphLookup(spec bson.Raw) (bson.D, bool) {
	elems, err := spec.Elements()
	if err != nil {
		return nil, false
	}
	out := make(bson.D, 0, len(elems)+1)
	// A $graphLookup always names a collection. Without one there is nothing to
	// filter, and the stage is returned as it came.
	if _, named := stringField(spec, "from"); !named {
		for _, e := range elems {
			out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
		}
		return out, true
	}
	restricted := false
	for _, e := range elems {
		if e.Key() == "restrictSearchWithMatch" {
			// Not a document is not something to repair: the server rejects it,
			// and so should the rewritten stage.
			if _, ok := e.Value().DocumentOK(); !ok {
				return nil, false
			}
			out = append(out, bson.E{Key: e.Key(), Value: liveOnly(e.Value())})
			restricted = true
			continue
		}
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	if !restricted {
		out = append(out, bson.E{Key: "restrictSearchWithMatch", Value: bson.D{meta.NotArchivedDeleted()}})
	}
	return out, true
}

// liveSubPipeline rewrites a stage's own pipeline, optionally putting the
// tombstone filter at the head of it. Anything that is not a pipeline is
// reported rather than replaced, for the reason in liveJoin.
func liveSubPipeline(pipeline bson.RawValue, depth int, prepend bool) (bson.A, bool) {
	arr, ok := pipeline.ArrayOK()
	if !ok {
		return nil, false
	}
	vals, err := arr.Values()
	if err != nil {
		return nil, false
	}
	out := bson.A{}
	if prepend && !startsWithGeoNear(vals) {
		out = append(out, bson.D{{Key: "$match", Value: bson.D{meta.NotArchivedDeleted()}}})
	}
	for _, v := range vals {
		doc, ok := v.DocumentOK()
		if !ok {
			return nil, false
		}
		out = append(out, liveStage(doc, depth))
	}
	return out, true
}

func lookupString(doc bson.Raw, key string) (string, bool) {
	val, err := doc.LookupErr(key)
	if err != nil {
		return "", false
	}
	return val.StringValueOK()
}

// intOf reads a batch size, which drivers send as int32 or int64.
func intOf(val bson.RawValue) (int, bool) {
	switch val.Type {
	case bson.TypeInt32:
		return int(val.Int32()), true
	case bson.TypeInt64:
		return int(val.Int64()), true
	case bson.TypeDouble:
		return int(val.Double()), true
	}
	return 0, false
}

// count and distinct read user documents just like find does, and were going
// straight through. Against a partly filled fast tier, count returned 9 where
// the answer was 41 and reported success, which is the same silent short answer
// the completeness guard exists to prevent. They are guarded here rather than
// with find because they reply with a plain document instead of a cursor, so
// there is nothing to page and the whole answer comes back in one command.
//
// mapReduce is deliberately not covered. It is deprecated, rare, and its output
// can land in a collection, which needs the same care $out gets.

// simpleReadCollection returns the collection a count or distinct names.
func simpleReadCollection(cmd bson.Raw, name string) (string, bool) {
	return lookupString(cmd, name)
}

// guardSimpleRead completeness-checks count and distinct, and answers them from
// the archive when the fast tier cannot.
// unreadableQuery reports whether a count or distinct carries a query this
// proxy cannot read.
//
// Absent is not unreadable: a count with no query counts everything, which is a
// question this path answers. Present and not a document is a command mongod
// refuses, and reading it as no query at all had the router answer it from the
// archive unbounded.
func unreadableQuery(cmd bson.Raw) bool {
	v, err := cmd.LookupErr("query")
	if err != nil {
		return false
	}
	_, isDoc := v.DocumentOK()
	return !isDoc
}

func (c *conn) guardSimpleRead(ctx context.Context, msg *wire.Message, db, name string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	coll, ok := simpleReadCollection(cmd, name)
	if !ok {
		return nil, false
	}
	// A query this proxy cannot read. mongod refuses it ("BSON field
	// 'count.query' is the wrong type 'int'") and the router answered it: a
	// count with query: 5 came back with a number, and a distinct with the same
	// came back with values. Neither had a query at all as far as this path was
	// concerned, so both were answered from the archive unbounded. Forwarded,
	// mongod says what is wrong with it. See unreadableWriteStage for the same
	// rule on the writing side.
	if unreadableQuery(cmd) {
		return nil, false
	}
	ns := db + "." + coll
	base, viaView, tiered := c.r.tieredBase(ctx, ns)

	// The third read path, and the same question as the other two: a view's own
	// pipeline can join a tiered collection, and nothing in this command names
	// it. distinct is the one that shows it, because a $lookup adds fields
	// rather than rows and a count of rows is the same either way.
	//
	// Before the untiered return, for the reason in isViewNS.
	if c.r.isViewNS(ctx, ns, viaView, tiered) {
		if out, done := c.refuseViewJoinIfShort(ctx, ns, msg, nil); done {
			return out, true
		}
	}

	if !tiered {
		return nil, false
	}
	judged := ns
	if viaView {
		judged = base
	}

	// A read, so router.claim_grace applies. Nothing here writes.
	mark, known := c.r.completenessForRead(ctx, judged)
	if known && mark.Claimed {
		af, aok := c.r.ageFieldOf(judged)
		bound, bounded := boundOfQueryField(cmd, af, aok)
		if (bounded && mark.Complete(bound)) || (!bounded && mark.CoversAll()) {
			return nil, false
		}
	}

	because := "the fast tier does not hold all of " + ns
	switch {
	case !known:
		because = "the completeness watermark could not be read"
	case !mark.Claimed:
		because = "nothing has recorded that the fast tier is complete for " + ns
	}

	c.r.Stats.Incomplete.Add(1)

	// The archive holds the documents, and it holds the view too when the router
	// mirrored it down. When it does not, or its definition differs, the
	// question cannot be put to it at all. See archiveCanAnswerView.
	if viaView && !c.r.archiveCanAnswerView(ctx, ns) {
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a read through a view over an incomplete collection",
			"view", ns, "reads", base, "because", because)
		out, verr := c.r.refuseView(msg.Header.RequestID, msg.Header.RequestID, ns, base, because)
		if verr != nil {
			return nil, false
		}
		return out, true
	}

	if inTransaction(cmd) {
		if filter, fok := txnFilter(cmd, name); fok {
			switch out, res := c.warmForTxn(ctx, ns, filter, copyUpOptions{let: letOf(cmd), collation: docField(cmd, "collation"), budget: maxTimeBudget(cmd)}, msg.Header.RequestID, msg.Header.RequestID,
				"the documents this transaction counts"); res {
			case txnWarmReplay:
				c.r.Stats.TxnReplays.Add(1)
				c.cls = ClassTxnWarm
				return out, true
			case txnWarmComplete:
				// Nothing is missing, so the fast tier can answer this itself.
				return nil, false
			}
		}
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		out, terr := c.r.txnCannotSpanTiers(msg.Header.RequestID, msg.Header.RequestID, ns, because)
		if terr != nil {
			return nil, false
		}
		return out, true
	}

	if c.r.cfg.Completeness == config.Archive {
		// The archive is reached with the router's credentials, so the fast
		// tier has to be asked whether this client was allowed to ask.
		if denied, ok := c.mayRead(msg, db, coll); !ok {
			return denied, true
		}
		out, aerr := c.answerSimpleFromArchive(ctx, msg, db, cmd)
		if aerr == nil {
			c.r.Stats.Archived.Add(1)
			c.cls = ClassArchive
			c.log.Debug("read answered from the archive", "ns", ns, "command", name, "because", because)
			return out, true
		}
		if out, passed := c.r.serverErrorReply(msg.Header.RequestID, msg.Header.RequestID, aerr); passed {
			c.log.Debug("the archive rejected this read; returning its own error", "ns", ns, "err", aerr)
			return out, true
		}
		c.log.Warn("could not answer this read from the archive, refusing instead", "ns", ns, "err", aerr)
	}

	if c.r.cfg.Completeness == config.Permissive {
		c.log.Warn("answer may be short", "ns", ns, "because", because)
		return nil, false
	}

	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a query that would return a short answer", "ns", ns, "because", because)
	out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this query because "+because+
			". Run the warmer, or set router.completeness to archive to answer it from the archive instead.")
	if rerr != nil {
		return nil, false
	}
	return out, true
}

// boundOfQueryField reads a lower time bound from the `query` field that count
// and distinct use in place of find's `filter`.
func boundOfQueryField(cmd bson.Raw, ageField string, known bool) (time.Time, bool) {
	val, err := cmd.LookupErr("query")
	if err != nil {
		return time.Time{}, false
	}
	doc, ok := val.DocumentOK()
	if !ok {
		return time.Time{}, false
	}
	return lowerTimeBound(doc, ageField, known)
}

// answerSimpleFromArchive runs a count or distinct against the archive and
// returns its answer verbatim.
func (c *conn) answerSimpleFromArchive(ctx context.Context, msg *wire.Message, db string, cmd bson.Raw) (*wire.Message, error) {
	elems, err := cmd.Elements()
	if err != nil {
		return nil, err
	}
	// count and distinct cross the same way find does, and for the same
	// reasons: the level of the read concern is portable and its cluster times
	// are not, an explicit collation travels but the collection's own does not,
	// and linearizable cannot be answered by another cluster at all.
	level, honourable := archiveLevel(cmd)
	if !honourable {
		return nil, errors.New("readConcern linearizable cannot be answered from the archive: " +
			"it promises a read confirmed on the primary that owns the document; " +
			"warm this range, or read it with majority")
	}

	out := make(bson.D, 0, len(elems)+3)
	sawQuery, sawCollation := false, false
	for _, e := range elems {
		if sessionFields[e.Key()] {
			continue
		}
		if e.Key() == "query" {
			out = append(out, bson.E{Key: "query", Value: liveOnly(e.Value())})
			sawQuery = true
			continue
		}
		if e.Key() == "collation" {
			sawCollation = true
		}
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	if !sawQuery {
		out = append(out, bson.E{Key: "query", Value: bson.D{meta.NotArchivedDeleted()}})
	}

	coll, _ := stringField(cmd, "count")
	if coll == "" {
		coll, _ = stringField(cmd, "distinct")
	}
	if !sawCollation {
		if collation, ok := c.r.defaultCollation(ctx, db+"."+coll); ok {
			if sc := simplifyCollation(collation); len(sc) > 0 {
				out = append(out, bson.E{Key: "collation", Value: sc})
			}
		}
	}
	if rc, ok := archiveReadConcern(level); ok {
		out = append(out, bson.E{Key: "readConcern", Value: rc})
	}
	// The same question as on the find path, asked here too: a count or a
	// distinct answered from an archive missing documents is short in exactly
	// the same way, and this path builds its own command rather than going
	// through answerFromArchive.
	if refusal, refused := c.refuseIfNotSuperset(ctx, db+"."+coll,
		msg.Header.RequestID, msg.Header.RequestID); refused {
		return refusal, nil
	}
	c.archiveReadIsFresh(ctx, db+"."+coll, time.Now(), maxTimeBudget(cmd))

	res := c.r.cold.Database(db).RunCommand(ctx, out)
	if res.Err() != nil {
		return nil, res.Err()
	}
	raw, err := res.Raw()
	if err != nil {
		return nil, err
	}
	var doc bson.D
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	return wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, withoutForeignTime(doc))
}

// withoutForeignTime removes the archive's own sense of time from a reply that
// is about to be handed to a client talking to the fast tier.
//
// $clusterTime and operationTime describe the cluster that produced them, and
// the two tiers are different clusters with different keys. A driver gossips
// the $clusterTime it is given back on its next command, so an archive reply
// passed through whole leaves the session speaking the archive's time to the
// fast tier, which cannot validate the signature and answers "No keys found for
// HMAC that is valid for time".
//
// The failure lands on the *next* command, which is what made it hard to see:
// the count itself is correct, and then an unrelated find fails. Measured on an
// adoption, in a fresh session each time: after estimatedDocumentCount or
// distinct fell through, the following command failed; after a find or an
// aggregate fell through, it did not, because those build their own replies
// rather than forwarding the archive's.
//
// Dropping them rather than substituting the fast tier's is the conservative
// choice: a driver that is given no $clusterTime keeps the last one it had,
// which is the fast tier's and still true.
func withoutForeignTime(doc bson.D) bson.D {
	out := doc[:0:0]
	for _, e := range doc {
		if e.Key == "$clusterTime" || e.Key == "operationTime" {
			continue
		}
		out = append(out, e)
	}
	return out
}

// serverErrorReply rebuilds an error the archive returned, so the client sees
// what MongoDB said rather than what this proxy made of it.
//
// The archive is a MongoDB cluster holding the same collection, so a command it
// rejects is a command MongoDB rejects. An unknown operator, a malformed
// projection, a duplicate key: the answer to all of those is the server's own,
// and an application branches on the code, not on the prose. Replacing them with
// a generic CommandFailed and a sentence about completeness turned the
// application's own bug into an infrastructure mystery, and turned an error a
// driver knows how to act on into one it does not.
//
// Only errors carrying a server code are passed through. A timeout or a dropped
// connection to the archive is this system failing to answer, not MongoDB
// refusing to, and those still take the caller's refusal path.
func (r *Router) serverErrorReply(requestID, responseTo int32, err error) (*wire.Message, bool) {
	var ce mongo.CommandError
	if !errors.As(err, &ce) {
		return nil, false
	}
	if ce.Code == 0 {
		return nil, false
	}
	// A network error the driver wrapped in a CommandError is still a network
	// error: it belongs to the connection to the archive and not to the command.
	if ce.HasErrorLabel("NetworkError") {
		return nil, false
	}
	// The archive is reached with the router's own credentials, so an
	// authorization failure there is a statement about this deployment's
	// configuration and not about the caller. Handing it to the client would
	// tell an application it lacks a privilege it does in fact hold, and send
	// whoever investigates to the wrong cluster. The caller's refusal, which
	// names what an operator can do, is the honest answer to those.
	switch ce.Code {
	case 13, 18: // Unauthorized, AuthenticationFailed
		return nil, false
	}
	// The archive's own error, plus a sentence when it is the shape a release
	// gap makes. See archiveDialectNote: the message is the only place an
	// application's developer will look, and "$convert found an unknown
	// argument" says nothing about two clusters on different releases.
	message := ce.Message + r.archiveDialectNote(ce.Code, ce.Message)
	out := bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: message},
		{Key: "code", Value: ce.Code},
	}
	if ce.Name != "" {
		out = append(out, bson.E{Key: "codeName", Value: ce.Name})
	}
	if len(ce.Labels) > 0 {
		labels := make(bson.A, 0, len(ce.Labels))
		for _, l := range ce.Labels {
			labels = append(labels, l)
		}
		out = append(out, bson.E{Key: "errorLabels", Value: labels})
	}
	msg, merr := wire.NewMsg(requestID, responseTo, out)
	if merr != nil {
		return nil, false
	}
	return msg, true
}

// withRemainingTime rewrites maxTimeMS to what is left of it.
//
// Nothing happens when the caller set no deadline, which is the common case.
// An exhausted one is not rounded up to something comfortable: it becomes the
// smallest positive value, so the archive answers if it can and MongoDB returns
// its own MaxTimeMSExpired if it cannot. Zero would be worse than wrong, as
// MongoDB reads zero as "no limit", which is the opposite of what an expired
// deadline means.
func withRemainingTime(cmd bson.D, budget, spent time.Duration) bson.D {
	if budget <= 0 {
		return cmd
	}
	left := budget - spent
	if left < time.Millisecond {
		left = time.Millisecond
	}
	for i := range cmd {
		if cmd[i].Key == "maxTimeMS" {
			cmd[i].Value = left.Milliseconds()
			return cmd
		}
	}
	return cmd
}
