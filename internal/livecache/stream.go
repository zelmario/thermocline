package livecache

import (
	"context"
	"encoding/hex"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// maxEventDoc is the largest document the stream carries whole. A larger one
// arrives without it, and its namespace is forgotten instead of updated.
const maxEventDoc = 4 << 20

// Run is the applier: it follows the fast tier's change stream, hands fills to
// workers and installs what they read, all in one goroutine, so a fill and the
// changes around it are applied in one order. See fill.go. skip names
// namespaces never to cache, the system's own.
//
// The stream is read through stream and fills through hot. They may be
// different nodes: a fill reads a snapshot at the position the stream has
// reached, which every member that delivered it has.
func (c *Cache) Run(ctx context.Context, hot, stream *mongo.Client, skip func(ns string) bool, log *slog.Logger) {
	for ctx.Err() == nil {
		err := c.follow(ctx, hot, stream, skip, log)
		// Whatever was applied before the stream broke is still right, but
		// what happened while it was down is unknown, so everything goes.
		c.Reset()
		if ctx.Err() != nil {
			return
		}
		log.Warn("the live cache's change stream stopped, starting it again with nothing cached",
			"err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (c *Cache) follow(ctx context.Context, hot, stream *mongo.Client, skip func(ns string) bool, log *slog.Logger) error {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{
				"insert", "update", "replace", "delete", "drop", "rename", "dropDatabase", "invalidate",
			}}}},
			// Never cached, so never worth a lookup on the fast tier's primary:
			// the server's own databases and this system's.
			{Key: "ns.db", Value: bson.D{
				{Key: "$nin", Value: bson.A{"admin", "config", "local"}},
				{Key: "$not", Value: bson.Regex{Pattern: "^_thermocline"}},
			}},
		}}},
		{{Key: "$project", Value: bson.D{
			{Key: "operationType", Value: 1}, {Key: "ns", Value: 1}, {Key: "to", Value: 1},
			{Key: "documentKey", Value: 1}, {Key: "clusterTime", Value: 1},
			{Key: "fullDocument", Value: bson.D{{Key: "$cond", Value: bson.A{
				bson.D{{Key: "$lte", Value: bson.A{bson.D{{Key: "$bsonSize", Value: "$fullDocument"}}, maxEventDoc}}},
				"$fullDocument", "$$REMOVE"}}}},
			{Key: "updated", Value: bson.D{{Key: "$concatArrays", Value: bson.A{
				bson.D{{Key: "$map", Value: bson.D{
					{Key: "input", Value: bson.D{{Key: "$objectToArray", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$updateDescription.updatedFields", bson.D{}}}}}}},
					{Key: "as", Value: "f"}, {Key: "in", Value: "$$f.k"}}}},
				bson.D{{Key: "$ifNull", Value: bson.A{"$updateDescription.removedFields", bson.A{}}}},
			}}}},
		}}},
	}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetMaxAwaitTime(5 * time.Millisecond)
	cs, err := stream.Watch(ctx, pipeline, opts)
	if err != nil {
		return err
	}
	defer func() { _ = cs.Close(context.WithoutCancel(ctx)) }()
	fctx, stopFills := context.WithCancel(ctx)
	fs := newFillState()
	workers := c.startFills(fctx, hot, fs)
	defer func() {
		stopFills()
		workers.Wait()
	}()
	said := time.Now()
	// What the batch being read has held so far, whether it is a cursor's
	// first, and the largest event the stream has carried, to tell a batch
	// mongod may have cut short. See cutShort.
	batchN, batchBytes, biggest := 0, 0, 0
	var batchCursor int64
	for {
		if time.Since(said) >= 30*time.Second {
			said = time.Now()
			s := c.Stats()
			log.Info("live cache", "entries", s.Entries, "bytes", s.Bytes, "hits", s.Hits, "misses", s.Misses,
				"waited_for_stream", s.Behind, "fills", s.Fills, "dropped", s.Dropped, "refused_shapes", s.Refusals, "unproven", s.Unproven)
		}
		// Fills first, so a miss waits at most one poll for its entry. Both
		// only hand out and take back: the reads run on the workers.
		c.collect(fs)
		c.dispatch(fs)
		if cs.TryNext(ctx) {
			batchN++
			batchBytes += len(cs.Current)
			biggest = max(biggest, len(cs.Current))
			c.handle(cs.Current, skip, fs)
			// The end of a batch is a position the stream has passed even
			// while it is never empty, which is when the pending marks wait
			// longest for one. See Advance.
			//
			// Under steady writes the batch's token is most often the last
			// event's own, not a high-water mark. Closing a time only on a
			// high-water mark took test/txnpairs from 12,025 hits to 30 in
			// three minutes and made no read more correct: mongod returns a
			// transaction's events in one batch, short of the 16MB cut that
			// Advance already names.
			//
			// Short of that cut is not never. Each update carries its looked-up
			// document, up to maxEventDoc, so a transaction updating 200
			// documents of 100KB fills a batch past 16MB; and the driver resumes
			// on its own after a failover with a fresh aggregate, whose first
			// batch holds 101 events. A batch that may have been cut that way
			// and ends at an event's own token closes nothing: the next event,
			// or the next poll, does.
			//
			// Only a cursor's first batch stops at 101. A getMore stops at
			// 16MB alone, and under steady writes holds more than 101 events
			// as a matter of course: counted against every batch, the rule
			// kept the busiest namespace refused through every round trip.
			if cs.RemainingBatchLength() == 0 {
				tok := cs.ResumeToken()
				first := cs.ID() != batchCursor
				batchCursor = cs.ID()
				if at, ok := clusterTimeFromToken(tok); ok && !(cutShort(batchN, batchBytes, first, biggest) && eventToken(tok)) {
					c.Advance(at)
				}
				batchN, batchBytes = 0, 0
			}
			continue
		}
		if err := cs.Err(); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// An empty batch's token is a high-water mark, except where the stream
		// has not yet read past where it resumed: then it is the resume point,
		// an event's own token, and that event may be the middle of a commit
		// the batch before was cut in. Closing on it answered the commit half
		// applied, so only a high-water mark closes from here.
		if tok := cs.ResumeToken(); !eventToken(tok) {
			if at, ok := clusterTimeFromToken(tok); ok {
				c.Advance(at)
			}
		}
	}
}

func (c *Cache) handle(ev bson.Raw, skip func(ns string) bool, fs *fillState) {
	var e struct {
		Op string `bson:"operationType"`
		NS struct {
			DB   string `bson:"db"`
			Coll string `bson:"coll"`
		} `bson:"ns"`
		To struct {
			DB   string `bson:"db"`
			Coll string `bson:"coll"`
		} `bson:"to"`
		Key     bson.Raw       `bson:"documentKey"`
		At      bson.Timestamp `bson:"clusterTime"`
		Full    bson.RawValue  `bson:"fullDocument"`
		Updated []string       `bson:"updated"`
	}
	// A copy first: the event is the cursor's buffer, which it reuses for the
	// next batch, and what is decoded from it below points into it. Kept, those
	// documents would turn into whatever the next events held.
	ev = append(bson.Raw(nil), ev...)
	if err := bson.Unmarshal(ev, &e); err != nil {
		c.Reset()
		return
	}
	// Only a document is a document. An update whose document was deleted
	// before the lookup carries null, and stored as one it reads back as a
	// batch that decodes to nothing.
	//
	// Null and missing are two different answers. Null is a document that is
	// gone. Missing is the pipeline above dropping one over maxEventDoc, which
	// still exists: read as gone, it was taken out of complete entries and a
	// list came back short.
	missing := e.Full.Type == 0
	var full bson.Raw
	if d, ok := e.Full.DocumentOK(); ok {
		full = d
	}
	ns := e.NS.DB + "." + e.NS.Coll
	switch e.Op {
	case "drop", "rename", "invalidate":
		c.DropNS(ns)
		fs.forget(ns)
		if e.To.Coll != "" {
			c.DropNS(e.To.DB + "." + e.To.Coll)
			fs.forget(e.To.DB + "." + e.To.Coll)
		}
		c.saw(e.At)
		return
	case "dropDatabase":
		fs.forget("")
		c.forgetPlain(e.NS.DB + ".")
		c.mu.Lock()
		for _, en := range c.entries {
			if len(en.shape.ns) > len(e.NS.DB) && en.shape.ns[:len(e.NS.DB)+1] == e.NS.DB+"." {
				c.drop(en)
			}
		}
		c.mu.Unlock()
		c.saw(e.At)
		return
	}
	if e.NS.Coll == "" || skip(ns) {
		c.saw(e.At)
		return
	}
	id, err := e.Key.LookupErr("_id")
	if err != nil {
		c.DropNS(ns)
		fs.forget(ns)
		c.saw(e.At)
		return
	}
	ch := Change{NS: ns, Op: e.Op, ID: id, Doc: full, Updated: e.Updated, At: e.At}
	if e.Op != "delete" && missing {
		// Too large to carry: what changed is unknown.
		c.DropNS(ns)
		fs.forget(ns)
		c.saw(e.At)
		return
	}
	fs.keep(ch)
	c.Apply(ch)
}

func (c *Cache) unproven(q Query) {
	c.Unproven.Add(1)
	c.mu.Lock()
	key := q.entryKey()
	delete(c.filling, key)
	if len(c.unprovenAt) >= maxUnproven {
		clear(c.unprovenAt)
	}
	c.unprovenAt[key] = time.Now()
	c.mu.Unlock()
}

func hasEq(q Query, field string) bool {
	for _, p := range q.Eq {
		if p.Field == field {
			return true
		}
	}
	return false
}

func (c *Cache) unpend(q Query) {
	c.mu.Lock()
	delete(c.filling, q.entryKey())
	c.mu.Unlock()
}

func splitNS(ns string) (string, string) {
	for i := 0; i < len(ns); i++ {
		if ns[i] == '.' {
			return ns[:i], ns[i+1:]
		}
	}
	return ns, ""
}

// clusterTimeFromToken reads the cluster time a resume token marks. The same
// decoding as the feeder's; see feeder/resumetoken.go.
func clusterTimeFromToken(tok bson.Raw) (bson.Timestamp, bool) {
	if len(tok) == 0 {
		return bson.Timestamp{}, false
	}
	data, ok := tok.Lookup("_data").StringValueOK()
	if !ok || len(data) < 18 {
		return bson.Timestamp{}, false
	}
	raw, err := hex.DecodeString(data[:18])
	if err != nil || raw[0] != 0x82 {
		return bson.Timestamp{}, false
	}
	return bson.Timestamp{
		T: uint32(raw[1])<<24 | uint32(raw[2])<<16 | uint32(raw[3])<<8 | uint32(raw[4]),
		I: uint32(raw[5])<<24 | uint32(raw[6])<<16 | uint32(raw[7])<<8 | uint32(raw[8]),
	}, true
}

// cutShort reports whether a batch of n events and size bytes may have been
// ended by mongod's limits rather than by running out of events: the 101 of a
// cursor's first batch, or 16MB less room for one more event. An event holds
// its document up to maxEventDoc, but its _id and documentKey both carry the
// document's _id, which nothing bounds, so the room is the larger of that and
// the biggest event the stream has carried.
func cutShort(n, size int, first bool, biggest int) bool {
	room := max(maxEventDoc+(1<<20), biggest)
	return (first && n >= 101) || size+room >= 16<<20
}

// eventToken reports whether a resume token is an event's own rather than a
// high-water mark. Measured on 8.0: the byte after the token's version is
// 0x29 (type 0) for a high-water mark and 0x2C (type 128) for an event. A
// layout this does not know reads as an event, which only means a batch end
// closes nothing and the next event does.
func eventToken(tok bson.Raw) bool {
	data, ok := tok.Lookup("_data").StringValueOK()
	if !ok || len(data) < 24 {
		return true
	}
	b, err := hex.DecodeString(data[18:24])
	if err != nil || b[0] != 0x2B {
		return true
	}
	return b[2] != 0x29
}

// TokenTime is clusterTimeFromToken for the router's own streams.
func TokenTime(tok bson.Raw) (bson.Timestamp, bool) { return clusterTimeFromToken(tok) }
