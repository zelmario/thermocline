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
