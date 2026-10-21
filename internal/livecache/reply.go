package livecache

import (
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// findReply builds what mongod answers a find with: one batch, the cursor
// closed.
func findReply(q Query, docs []bson.Raw) bson.Raw {
	batch := make(bson.A, 0, len(docs))
	for _, d := range docs {
		batch = append(batch, project(q.Proj, d))
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

// project applies a projection of top-level fields, keeping the document's own
// field order as mongod does.
func project(p Projection, d bson.Raw) bson.Raw {
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
