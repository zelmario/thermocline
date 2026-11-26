package livecache

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestTiesReordered(t *testing.T) {
	doc := func(id int, views any) bson.Raw {
		b, _ := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "views", Value: views}})
		return b
	}
	byViews := []SortKey{{Field: "views", Desc: true}}
	a, b, c := doc(1, 20), doc(2, 19), doc(3, 19)
	cases := []struct {
		name string
		sort []SortKey
		x, y []bson.Raw
		want bool
	}{
		{"two ties swapped", byViews, []bson.Raw{a, b, c}, []bson.Raw{a, c, b}, true},
		{"same order is no reorder", byViews, []bson.Raw{a, b, c}, []bson.Raw{a, b, c}, false},
		{"swapped across values", byViews, []bson.Raw{a, b, c}, []bson.Raw{b, a, c}, false},
		{"another document", byViews, []bson.Raw{a, b, c}, []bson.Raw{a, b, doc(4, 19)}, false},
		{"same _id, other contents", byViews, []bson.Raw{a, b, c}, []bson.Raw{a, c, doc(2, 18)}, false},
		{"int and double tie", byViews, []bson.Raw{doc(5, 7), doc(6, 7.0)}, []bson.Raw{doc(6, 7.0), doc(5, 7)}, true},
		{"a sort ending in _id cannot tie", []SortKey{{Field: "views"}, {Field: "_id"}}, []bson.Raw{a, b, c}, []bson.Raw{a, c, b}, false},
		{"no sort", nil, []bson.Raw{a, b, c}, []bson.Raw{a, c, b}, false},
		{"dotted path", []SortKey{{Field: "s.views"}}, []bson.Raw{a, b, c}, []bson.Raw{a, c, b}, false},
		{"lengths differ", byViews, []bson.Raw{a, b, c}, []bson.Raw{a, c}, false},
	}
	for _, tc := range cases {
		if got := TiesReordered(tc.sort, tc.x, tc.y); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
