package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/livecache"
)

func TestTieredPart(t *testing.T) {
	doc := func(id, at int, status string) bson.Raw {
		b, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(id)}, {Key: "at", Value: int32(at)}, {Key: "status", Value: status}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	chk := &liveCheck{q: livecache.Query{Kind: livecache.KindFind, Sort: []livecache.SortKey{{Field: "at", Desc: true}}, Limit: 3}}
	c := &conn{}
	cache := []bson.Raw{doc(1, 90, "placed"), doc(2, 80, "placed"), doc(3, 70, "placed")}
	cases := []struct {
		name string
		fast []bson.Raw
		want bool
	}{
		{"the fast tier holds them all", []bson.Raw{doc(1, 90, "placed"), doc(2, 80, "placed"), doc(3, 70, "placed")}, true},
		{"a fast tier document after the last is left out", []bson.Raw{doc(1, 90, "placed"), doc(2, 80, "placed"), doc(3, 70, "placed"), doc(4, 60, "placed")}, true},
		{"a fast tier document before the last is left out", []bson.Raw{doc(1, 90, "placed"), doc(5, 85, "placed"), doc(2, 80, "placed"), doc(3, 70, "placed")}, false},
		{"a fast tier document tying with the last is left out", []bson.Raw{doc(1, 90, "placed"), doc(2, 80, "placed"), doc(3, 70, "placed"), doc(6, 70, "placed")}, true},
		{"a document held differently", []bson.Raw{doc(1, 90, "placed"), doc(2, 80, "shipped"), doc(3, 70, "placed")}, false},
	}
	for _, tc := range cases {
		if got := c.tieredPart(chk, cache, tc.fast, bson.Timestamp{T: 100, I: 1}); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
	short := &liveCheck{q: livecache.Query{Kind: livecache.KindFind, Sort: []livecache.SortKey{{Field: "at", Desc: true}}, Limit: 10}}
	if c.tieredPart(short, cache, []bson.Raw{doc(1, 90, "placed"), doc(2, 80, "placed"), doc(3, 70, "placed"), doc(4, 10, "placed")}, bson.Timestamp{T: 100, I: 1}) {
		t.Error("an answer shorter than its limit left out a fast tier document and was judged whole")
	}
}
