package router

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmar/thermocline/internal/config"
	"github.com/zelmar/thermocline/internal/meta"
	"github.com/zelmar/thermocline/internal/wire"
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

// sessionFields belong to the client's conversation with the fast tier. Sending
// them to the archive would attach this read to a session and a transaction
// that exist on a different cluster.
var sessionFields = map[string]bool{
	"lsid": true, "txnNumber": true, "autocommit": true, "startTransaction": true,
	"$clusterTime": true, "$db": true, "readConcern": true, "writeConcern": true,
	"$readPreference": true, "apiVersion": true, "apiStrict": true, "apiDeprecationErrors": true,
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
// Ids start high and climb, and every getMore is checked against the registry
// before it is treated as ours. A real cursor id landing exactly on a live
// synthetic one would need a random int64 collision, and the worst case is a
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
		reg.next = 1 << 62
	}
	reg.next++
	id := reg.next
	ac.lastUsed = time.Now()
	reg.open[id] = ac
	return id
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
func (c *conn) answerFromArchive(ctx context.Context, msg *wire.Message, db, coll string) (*wire.Message, error) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, err
	}
	rewritten, batchSize, err := rewriteForArchive(cmd)
	if err != nil {
		return nil, err
	}

	// Every path that answers from the archive waits here, rather than each
	// caller remembering to. A find waited and an aggregation did not, and
	// countDocuments is an aggregation, so a document inserted a moment earlier
	// was found by find and not counted by countDocuments.
	c.archiveReadIsFresh(ctx, db+"."+coll, time.Now())

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
		id = c.r.cursors.add(&archiveCursor{cur: cur, ns: ns})
	} else {
		_ = cur.Close(ctx)
	}

	c.r.Stats.Archived.Add(1)
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
		return true, wire.Write(c.client, out)
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
		return true, wire.Write(c.client, fail)
	}
	return true, wire.Write(c.client, reply)
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
		if _, mine := c.r.cursors.get(id); mine {
			c.r.cursors.drop(ctx, id)
			continue
		}
		all = false
	}
	return all
}

// readBatch pulls up to size documents, reporting whether more remain.
func readBatch(ctx context.Context, cur *mongo.Cursor, size int) (bson.A, bool, error) {
	if size <= 0 {
		size = firstBatchDefault
	}
	batch := bson.A{}
	for len(batch) < size {
		if !cur.Next(ctx) {
			return batch, false, cur.Err()
		}
		var d bson.D
		if err := bson.Unmarshal(cur.Current, &d); err != nil {
			return batch, false, err
		}
		batch = append(batch, d)
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
func rewriteForArchive(cmd bson.Raw) (bson.D, int, error) {
	elems, err := cmd.Elements()
	if err != nil {
		return nil, 0, err
	}

	out := make(bson.D, 0, len(elems)+1)
	batchSize := firstBatchDefault
	sawFilter, sawPipeline := false, false

	for _, e := range elems {
		key := e.Key()
		if sessionFields[key] {
			continue
		}
		val := e.Value()

		switch key {
		case "filter":
			sawFilter = true
			out = append(out, bson.E{Key: "filter", Value: liveOnly(val)})
			continue
		case "pipeline":
			sawPipeline = true
			out = append(out, bson.E{Key: "pipeline", Value: livePipeline(val)})
			continue
		case "batchSize":
			if n, ok := intOf(val); ok && n > 0 {
				batchSize = n
			}
		case "cursor":
			if doc, ok := val.DocumentOK(); ok {
				if n, ok := intOf(doc.Lookup("batchSize")); ok && n > 0 {
					batchSize = n
				}
			}
		}
		out = append(out, bson.E{Key: key, Value: val})
	}

	// A find with no filter at all still has to exclude tombstones.
	if _, isFind := lookupString(cmd, "find"); isFind && !sawFilter {
		out = append(out, bson.E{Key: "filter", Value: bson.D{meta.NotArchivedDeleted()}})
	}
	if _, isAgg := lookupString(cmd, "aggregate"); isAgg && !sawPipeline {
		out = append(out, bson.E{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{meta.NotArchivedDeleted()}}},
		}})
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
func livePipeline(pipeline bson.RawValue) bson.A {
	out := bson.A{bson.D{{Key: "$match", Value: bson.D{meta.NotArchivedDeleted()}}}}
	arr, ok := pipeline.ArrayOK()
	if !ok {
		return out
	}
	vals, err := arr.Values()
	if err != nil {
		return out
	}
	for _, v := range vals {
		if doc, ok := v.DocumentOK(); ok {
			out = append(out, doc)
		}
	}
	return out
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
func (c *conn) guardSimpleRead(ctx context.Context, msg *wire.Message, db, name string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	coll, ok := simpleReadCollection(cmd, name)
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}

	mark, known := c.r.completeness(ctx, ns)
	if known && mark.Claimed {
		bound, bounded := boundOfQueryField(cmd)
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

	if inTransaction(cmd) {
		c.r.Stats.Refused.Add(1)
		out, terr := c.r.txnCannotSpanTiers(msg.Header.RequestID, msg.Header.RequestID, ns, because)
		if terr != nil {
			return nil, false
		}
		return out, true
	}

	if c.r.cfg.Completeness == config.Archive {
		out, aerr := c.answerSimpleFromArchive(ctx, msg, db, cmd)
		if aerr == nil {
			c.r.Stats.Archived.Add(1)
			c.log.Debug("read answered from the archive", "ns", ns, "command", name, "because", because)
			return out, true
		}
		c.log.Warn("could not answer this read from the archive, refusing instead", "ns", ns, "err", aerr)
	}

	if c.r.cfg.Completeness == config.Permissive {
		c.log.Warn("answer may be short", "ns", ns, "because", because)
		return nil, false
	}

	c.r.Stats.Refused.Add(1)
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
func boundOfQueryField(cmd bson.Raw) (time.Time, bool) {
	val, err := cmd.LookupErr("query")
	if err != nil {
		return time.Time{}, false
	}
	doc, ok := val.DocumentOK()
	if !ok {
		return time.Time{}, false
	}
	return lowerTimeBound(doc)
}

// answerSimpleFromArchive runs a count or distinct against the archive and
// returns its answer verbatim.
func (c *conn) answerSimpleFromArchive(ctx context.Context, msg *wire.Message, db string, cmd bson.Raw) (*wire.Message, error) {
	elems, err := cmd.Elements()
	if err != nil {
		return nil, err
	}
	out := make(bson.D, 0, len(elems)+1)
	sawQuery := false
	for _, e := range elems {
		if sessionFields[e.Key()] {
			continue
		}
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

	coll, _ := stringField(cmd, "count")
	if coll == "" {
		coll, _ = stringField(cmd, "distinct")
	}
	c.archiveReadIsFresh(ctx, db+"."+coll, time.Now())

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
	return wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, doc)
}
