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

// Run is the applier: it follows the fast tier's change stream and fills
// entries, both in one goroutine, so a fill and the changes around it are
// applied in one order. skip names namespaces never to cache, which are the
// tiered ones and the system's own.
func (c *Cache) Run(ctx context.Context, hot *mongo.Client, skip func(ns string) bool, log *slog.Logger) {
	for ctx.Err() == nil {
		err := c.follow(ctx, hot, skip, log)
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

func (c *Cache) follow(ctx context.Context, hot *mongo.Client, skip func(ns string) bool, log *slog.Logger) error {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{
			"insert", "update", "replace", "delete", "drop", "rename", "dropDatabase", "invalidate",
		}}}}}}},
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
	cs, err := hot.Watch(ctx, pipeline, opts)
	if err != nil {
		return err
	}
	defer func() { _ = cs.Close(context.WithoutCancel(ctx)) }()
	said := time.Now()
	for {
		if time.Since(said) >= 30*time.Second {
			said = time.Now()
			s := c.Stats()
			log.Info("live cache", "entries", s.Entries, "bytes", s.Bytes, "hits", s.Hits, "misses", s.Misses,
				"waited_for_stream", s.Behind, "fills", s.Fills, "dropped", s.Dropped, "refused_shapes", s.Refusals)
		}
		// Fills first, so a miss waits at most one poll for its entry.
		c.fillPending(ctx, hot)
		if cs.TryNext(ctx) {
			c.handle(cs.Current, skip)
			continue
		}
		if err := cs.Err(); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if at, ok := clusterTimeFromToken(cs.ResumeToken()); ok {
			c.Advance(at)
		}
	}
}

func (c *Cache) handle(ev bson.Raw, skip func(ns string) bool) {
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
	var full bson.Raw
	if d, ok := e.Full.DocumentOK(); ok {
		full = d
	}
	ns := e.NS.DB + "." + e.NS.Coll
	switch e.Op {
	case "drop", "rename", "invalidate":
		c.DropNS(ns)
		if e.To.Coll != "" {
			c.DropNS(e.To.DB + "." + e.To.Coll)
		}
		c.Advance(e.At)
		return
	case "dropDatabase":
		c.mu.Lock()
		for _, en := range c.entries {
			if len(en.shape.ns) > len(e.NS.DB) && en.shape.ns[:len(e.NS.DB)+1] == e.NS.DB+"." {
				c.drop(en)
			}
		}
		c.mu.Unlock()
		c.Advance(e.At)
		return
	}
	if e.NS.Coll == "" || skip(ns) {
		c.Advance(e.At)
		return
	}
	id, err := e.Key.LookupErr("_id")
	if err != nil {
		c.DropNS(ns)
		c.Advance(e.At)
		return
	}
	ch := Change{NS: ns, Op: e.Op, ID: id, Doc: full, Updated: e.Updated, At: e.At}
	if e.Op != "delete" && full == nil && e.Op != "update" {
		// Too large to carry: what changed is unknown.
		c.DropNS(ns)
		c.Advance(e.At)
		return
	}
	c.Apply(ch)
}

// fillPending runs the fills asked for since the last poll.
func (c *Cache) fillPending(ctx context.Context, hot *mongo.Client) {
	for {
		select {
		case q := <-c.fillq:
			c.fill(ctx, hot, q)
		default:
			return
		}
	}
}

func (c *Cache) fill(ctx context.Context, hot *mongo.Client, q Query) {
	db, coll := splitNS(q.NS)
	if ok, seen := c.plainNS[q.NS]; !seen {
		ok = true
		cur, err := hot.Database(db).ListCollections(ctx, bson.D{{Key: "name", Value: coll}})
		if err == nil {
			for cur.Next(ctx) {
				if _, cerr := cur.Current.LookupErr("options", "collation"); cerr == nil {
					ok = false
				}
				if t, _ := cur.Current.Lookup("type").StringValueOK(); t == "view" {
					ok = false
				}
			}
			_ = cur.Close(ctx)
		} else {
			c.unpend(q)
			return
		}
		c.plainNS[q.NS] = ok
		if !ok {
			c.RefuseNS(q.NS)
		}
	} else if !ok {
		c.unpend(q)
		return
	}
	filter := bson.D{}
	for _, p := range q.Eq {
		filter = append(filter, bson.E{Key: p.Field, Value: p.Value})
	}
	fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	co := hot.Database(db).Collection(coll)
	if q.Kind != KindFind {
		n, err := co.CountDocuments(fctx, filter)
		if err != nil {
			c.unpend(q)
			return
		}
		c.install(q, nil, true, n)
		return
	}
	window := min(maxWindow, q.Skip+q.Limit+fillWindow)
	sortDoc := bson.D{}
	for _, s := range q.Sort {
		dir := 1
		if s.Desc {
			dir = -1
		}
		sortDoc = append(sortDoc, bson.E{Key: s.Field, Value: dir})
	}
	fo := options.Find().SetLimit(int64(window + 1))
	if len(sortDoc) > 0 {
		fo.SetSort(sortDoc)
	}
	cur, err := co.Find(fctx, filter, fo)
	if err != nil {
		c.unpend(q)
		return
	}
	var docs []bson.Raw
	for cur.Next(fctx) {
		docs = append(docs, append(bson.Raw(nil), cur.Current...))
	}
	if cur.Err() != nil {
		_ = cur.Close(ctx)
		c.unpend(q)
		return
	}
	_ = cur.Close(ctx)
	complete := len(docs) <= window
	if !complete {
		docs = docs[:window]
	}
	c.install(q, docs, complete, 0)
}

func (c *Cache) unpend(q Query) {
	c.mu.Lock()
	delete(c.pending, q.entryKey())
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
