package livecache

import (
	"math"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// findReply builds what mongod answers a find with: one batch, the cursor
// closed.
//
// Written with bsoncore's appenders rather than by marshalling a bson.D. Every
// hit builds one, and a hit is most reads: on the Docker rig on 2026-10-23 the
// marshal path was 11% of the router's CPU, with 168 allocations for a batch of
// twenty whole documents and 491 when projected. The bytes are the same, which
// reply_test.go checks against the marshal-based builder.
func findReply(q Query, docs []bson.Raw) bson.Raw {
	size := 64 + len(q.NS)
	for _, d := range docs {
		size += len(d) + 8
	}
	idx, b := bsoncore.AppendDocumentStart(make([]byte, 0, size))
	cidx, b := bsoncore.AppendDocumentElementStart(b, "cursor")
	aidx, b := bsoncore.AppendArrayElementStart(b, "firstBatch")
	var key []byte
	for i, d := range docs {
		key = strconv.AppendInt(key[:0], int64(i), 10)
		b = appendProjected(b, string(key), q.Proj, d)
	}
	b, _ = bsoncore.AppendArrayEnd(b, aidx)
	b = bsoncore.AppendInt64Element(b, "id", 0)
	b = bsoncore.AppendStringElement(b, "ns", q.NS)
	b, _ = bsoncore.AppendDocumentEnd(b, cidx)
	b = bsoncore.AppendDoubleElement(b, "ok", 1.0)
	b, _ = bsoncore.AppendDocumentEnd(b, idx)
	return b
}

// appendProjected appends d under key with the projection applied: the kept
// elements are copied as they are, in the document's own order. Read element
// by element from the bytes, because d.Elements() and e.Key() allocate per
// field and this runs for every document of every hit.
func appendProjected(b []byte, key string, p Projection, d bson.Raw) []byte {
	if !p.Set || len(d) < 5 {
		return bsoncore.AppendDocumentElement(b, key, d)
	}
	idx, b := bsoncore.AppendDocumentElementStart(b, key)
	rem := []byte(d[4 : len(d)-1])
	for len(rem) > 0 {
		e, next, ok := bsoncore.ReadElement(rem)
		if !ok {
			// Not a document this can walk: the whole of it, as before.
			return bsoncore.AppendDocumentElement(b[:int(idx)-len(key)-2], key, d)
		}
		if keepField(p, e.KeyBytes()) {
			b = append(b, e...)
		}
		rem = next
	}
	b, _ = bsoncore.AppendDocumentEnd(b, idx)
	return b
}

func keepField(p Projection, k []byte) bool {
	switch {
	case string(k) == "_id":
		return !p.NoID
	case p.Include:
		return p.Fields[string(k)]
	default:
		return !p.Fields[string(k)]
	}
}

// countReply builds the reply to a count, or to the counting pipeline: one
// document carrying the total, or none for zero, which is what $group gives
// for an empty input.
func countReply(q Query, n int64) bson.Raw {
	var num any = n
	if n <= math.MaxInt32 {
		num = int32(n)
	}
	if q.Kind == KindCount {
		return marshal(bson.D{{Key: "n", Value: num}, {Key: "ok", Value: 1.0}})
	}
	batch := bson.A{}
	if n > 0 {
		batch = append(batch, bson.D{{Key: "_id", Value: q.GroupID}, {Key: q.CountField, Value: num}})
	}
	return marshal(bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "firstBatch", Value: batch},
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: q.NS},
		}},
		{Key: "ok", Value: 1.0},
	})
}

func marshal(d bson.D) bson.Raw {
	b, err := bson.Marshal(d)
	if err != nil {
		return nil
	}
	return b
}

// Doc returns the reply as a document, for a caller building a wire message.
func Doc(reply bson.Raw) (bson.D, error) {
	var d bson.D
	err := bson.Unmarshal(reply, &d)
	return d, err
}
