package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestUnpredicatedCount(t *testing.T) {
	cases := []struct {
		cmd  bson.D
		want bool
	}{
		{bson.D{{Key: "count", Value: "c"}}, true},
		{bson.D{{Key: "count", Value: "c"}, {Key: "query", Value: bson.D{}}}, true},
		{bson.D{{Key: "count", Value: "c"}, {Key: "query", Value: bson.D{}}, {Key: "skip", Value: int32(0)}, {Key: "limit", Value: int64(0)}}, true},
		{bson.D{{Key: "count", Value: "c"}, {Key: "query", Value: bson.D{{Key: "a", Value: 1}}}}, false},
		{bson.D{{Key: "count", Value: "c"}, {Key: "skip", Value: int32(5)}}, false},
		{bson.D{{Key: "count", Value: "c"}, {Key: "limit", Value: int32(5)}}, false},
		{bson.D{{Key: "count", Value: "c"}, {Key: "hint", Value: "_id_"}}, false},
		{bson.D{{Key: "count", Value: "c"}, {Key: "query", Value: int32(5)}}, false},
	}
	for _, c := range cases {
		raw, _ := bson.Marshal(c.cmd)
		if got := unpredicatedCount(raw); got != c.want {
			t.Errorf("%v: got %v, want %v", c.cmd, got, c.want)
		}
	}
}

func TestCountDocumentsShape(t *testing.T) {
	m := func(v bson.D) bson.D { return bson.D{{Key: "$match", Value: v}} }
	g := func(id any, name string, sum any) bson.D {
		return bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: id}, {Key: name, Value: bson.D{{Key: "$sum", Value: sum}}}}}}
	}
	cases := []struct {
		name string
		pipe bson.A
		want bool
	}{
		{"driver shape", bson.A{m(bson.D{{Key: "a", Value: 1}}), g(int32(1), "n", int32(1))}, true},
		{"no match", bson.A{g(nil, "n", int32(1))}, true},
		{"group by a field", bson.A{m(bson.D{}), g("$a", "n", int32(1))}, false},
		{"sum of a field", bson.A{m(bson.D{}), g(int32(1), "n", "$x")}, false},
		{"sum of two", bson.A{m(bson.D{}), g(int32(1), "n", int32(2))}, false},
		{"a skip between", bson.A{m(bson.D{}), bson.D{{Key: "$skip", Value: 5}}, g(int32(1), "n", int32(1))}, false},
		{"a match only", bson.A{m(bson.D{})}, false},
	}
	for _, c := range cases {
		raw, _ := bson.Marshal(bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: c.pipe}})
		_, _, _, got := countDocumentsShape(raw)
		if got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
	raw, _ := bson.Marshal(bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: bson.A{}}, {Key: "hint", Value: "_id_"}})
	if subtractable(raw) {
		t.Errorf("a hint is not subtractable")
	}
}
