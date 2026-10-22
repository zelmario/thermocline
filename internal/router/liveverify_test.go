package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestLiveDiffer(t *testing.T) {
	m := func(d bson.D) bson.Raw {
		b, err := bson.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	batch := func(id int64, docs ...bson.D) bson.Raw {
		a := bson.A{}
		for _, d := range docs {
			a = append(a, d)
		}
		return m(bson.D{{Key: "cursor", Value: bson.D{{Key: "firstBatch", Value: a}, {Key: "id", Value: id}, {Key: "ns", Value: "a.b"}}}, {Key: "ok", Value: 1.0}})
	}
	d1, d2 := bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "_id", Value: 2}}
	for _, c := range []struct {
		name         string
		cached, real bson.Raw
		same         bool
	}{
		{"equal", batch(0, d1, d2), batch(0, d1, d2), true},
		{"a document differs", batch(0, d1, d1), batch(0, d1, d2), false},
		{"shorter", batch(0, d1), batch(0, d1, d2), false},
		{"longer than a closed cursor", batch(0, d1, d2), batch(0, d1), false},
		{"longer than an open cursor's first batch", batch(0, d1, d2), batch(7, d1), true},
		{"empty both", batch(0), batch(0), true},
		{"same count", m(bson.D{{Key: "n", Value: int32(3)}, {Key: "ok", Value: 1.0}}), m(bson.D{{Key: "n", Value: int32(3)}, {Key: "ok", Value: 1.0}}), true},
		{"count differs", m(bson.D{{Key: "n", Value: int32(3)}, {Key: "ok", Value: 1.0}}), m(bson.D{{Key: "n", Value: int32(4)}, {Key: "ok", Value: 1.0}}), false},
		{"real is an error", batch(0, d1), m(bson.D{{Key: "ok", Value: 0.0}, {Key: "errmsg", Value: "x"}}), false},
	} {
		if got := liveDiffer(c.cached, c.real) == ""; got != c.same {
			t.Errorf("%s: same=%v, want %v (%q)", c.name, got, c.same, liveDiffer(c.cached, c.real))
		}
	}
}
