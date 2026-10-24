package livecache

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The marshal-based builders the appenders replaced, kept as the reference
// the new ones must match byte for byte: a cached reply that differs from
// what was built before is a different answer, whatever it decodes to.
func refFindReply(q Query, docs []bson.Raw) bson.Raw {
	batch := make(bson.A, 0, len(docs))
	for _, d := range docs {
		batch = append(batch, refProject(q.Proj, d))
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

func refProject(p Projection, d bson.Raw) bson.Raw {
	if !p.Set {
		return d
	}
	elems, err := d.Elements()
	if err != nil {
		return d
	}
	out := bson.D{}
	for _, e := range elems {
		k := e.Key()
		keep := false
		switch {
		case k == "_id":
			keep = !p.NoID
		case p.Include:
			keep = p.Fields[k]
		default:
			keep = !p.Fields[k]
		}
		if keep {
			out = append(out, bson.E{Key: k, Value: e.Value()})
		}
	}
	return marshal(out)
}

func replyDocs(n int) []bson.Raw {
	docs := make([]bson.Raw, 0, n)
	for i := 0; i < n; i++ {
		d, _ := bson.Marshal(bson.D{
			{Key: "_id", Value: bson.NewObjectID()},
			{Key: "sku", Value: fmt.Sprintf("sku-%d", i)},
			{Key: "price", Value: float64(i) * 1.5},
			{Key: "qty", Value: int32(i)},
			{Key: "big", Value: int64(math.MaxInt64 - int64(i))},
			{Key: "tags", Value: bson.A{"a", "b", int32(i)}},
			{Key: "dims", Value: bson.D{{Key: "w", Value: 1.0}, {Key: "h", Value: nil}}},
			{Key: "", Value: "empty key"},
		})
		docs = append(docs, d)
	}
	return docs
}

func TestFindReplyMatchesMarshal(t *testing.T) {
	projections := []Projection{
		{},
		{Set: true, Include: true, Fields: map[string]bool{"sku": true, "price": true}},
		{Set: true, Include: true, Fields: map[string]bool{"sku": true}, NoID: true},
		{Set: true, Fields: map[string]bool{"tags": true, "dims": true}},
		{Set: true, Fields: map[string]bool{"": true}, NoID: true},
		{Set: true, Include: true, Fields: map[string]bool{"nosuch": true}, NoID: true},
	}
	// An empty document. A malformed one is not compared: the cache holds
	// what mongod returned, and the reference answered one with a nil reply
	// where this copies the bytes, neither of which is an answer.
	edge := []bson.Raw{{5, 0, 0, 0, 0}}
	for _, n := range []int{0, 1, 2, 11, 150, -1} {
		docs := replyDocs(max(n, 0))
		if n < 0 {
			docs = append(replyDocs(2), edge...)
		}
		for i, p := range projections {
			q := Query{NS: "shop.products", Proj: p}
			want, got := refFindReply(q, docs), findReply(q, docs)
			if !bytes.Equal(want, got) {
				t.Fatalf("%d documents, projection %d: the reply differs\nwant %v\ngot  %v", n, i, want, got)
			}
		}
	}
}

func BenchmarkFindReply(b *testing.B) {
	docs := replyDocs(20)
	for _, c := range []struct {
		name string
		p    Projection
	}{
		{"whole", Projection{}},
		{"projected", Projection{Set: true, Include: true, Fields: map[string]bool{"sku": true, "price": true}}},
	} {
		q := Query{NS: "shop.products", Proj: c.p}
		b.Run("marshal/"+c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				refFindReply(q, docs)
			}
		})
		b.Run("append/"+c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				findReply(q, docs)
			}
		})
	}
}
