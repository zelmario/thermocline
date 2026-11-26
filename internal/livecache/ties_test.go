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

func TestTiesAtTheLimit(t *testing.T) {
	doc := func(id int, views any) bson.Raw {
		b, _ := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "views", Value: views}})
		return b
	}
	noViews := func(id int) bson.Raw {
		b, _ := bson.Marshal(bson.D{{Key: "_id", Value: id}})
		return b
	}
	byViews := []SortKey{{Field: "views", Desc: true}}
	a, b := doc(1, 50), doc(2, 48)
	x, y, z := doc(3, 48), doc(4, 48), doc(5, 47)
	cases := []struct {
		name  string
		sort  []SortKey
		limit int
		p, q  []bson.Raw
		want  []int
	}{
		{"another tie kept at the cut", byViews, 3, []bson.Raw{a, b, x}, []bson.Raw{a, b, y}, []int{2}},
		{"two kept, in another order", byViews, 3, []bson.Raw{a, x, b}, []bson.Raw{a, b, y}, []int{1}},
		{"the outsider does not tie the cut", byViews, 3, []bson.Raw{a, b, z}, []bson.Raw{a, b, y}, nil},
		{"a tie above the cut is not at the cut", byViews, 3, []bson.Raw{a, x, z}, []bson.Raw{a, y, z}, nil},
		{"short of the limit holds every match", byViews, 4, []bson.Raw{a, b, x}, []bson.Raw{a, b, y}, nil},
		{"same documents is not this", byViews, 3, []bson.Raw{a, b, x}, []bson.Raw{a, x, b}, nil},
		{"same _id, other contents", byViews, 3, []bson.Raw{a, b, x}, []bson.Raw{doc(1, 49), b, y}, nil},
		{"sort ending in _id", []SortKey{{Field: "views"}, {Field: "_id"}}, 3, []bson.Raw{a, b, x}, []bson.Raw{a, b, y}, nil},
		{"sort field missing", byViews, 2, []bson.Raw{a, noViews(6)}, []bson.Raw{a, noViews(7)}, nil},
		{"int and double tie", byViews, 2, []bson.Raw{a, doc(8, 48)}, []bson.Raw{a, doc(9, 48.0)}, []int{1}},
	}
	for _, tc := range cases {
		got, ok := TiesAtTheLimit(tc.sort, tc.limit, tc.p, tc.q)
		if ok != (tc.want != nil) || len(got) != len(tc.want) {
			t.Errorf("%s: got %v %v, want %v", tc.name, got, ok, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		}
	}
}
