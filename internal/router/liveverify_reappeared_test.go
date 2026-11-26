package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestFirstOpIsInsert(t *testing.T) {
	id := bson.NewObjectID()
	other := bson.NewObjectID()
	idv := bson.RawValue{Type: bson.TypeObjectID, Value: id[:]}
	raw := func(d bson.D) bson.Raw {
		b, err := bson.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	cases := []struct {
		name string
		e    bson.D
		want bool
	}{
		{"plain insert", bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "s.o"}, {Key: "o", Value: bson.D{{Key: "_id", Value: id}}}}, true},
		{"plain update", bson.D{{Key: "op", Value: "u"}, {Key: "ns", Value: "s.o"}, {Key: "o2", Value: bson.D{{Key: "_id", Value: id}}}}, false},
		{"copy-up in a transaction", bson.D{{Key: "op", Value: "c"}, {Key: "o", Value: bson.D{{Key: "applyOps", Value: bson.A{
			bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "s.o"}, {Key: "o", Value: bson.D{{Key: "_id", Value: other}}}},
			bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "s.o"}, {Key: "o", Value: bson.D{{Key: "_id", Value: id}}}},
		}}}}}, true},
		{"eviction in a transaction", bson.D{{Key: "op", Value: "c"}, {Key: "o", Value: bson.D{{Key: "applyOps", Value: bson.A{
			bson.D{{Key: "op", Value: "d"}, {Key: "ns", Value: "s.o"}, {Key: "o", Value: bson.D{{Key: "_id", Value: id}}}},
			bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "s.o"}, {Key: "o", Value: bson.D{{Key: "_id", Value: id}}}},
		}}}}}, false},
		{"same _id in another collection", bson.D{{Key: "op", Value: "c"}, {Key: "o", Value: bson.D{{Key: "applyOps", Value: bson.A{
			bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "s.events"}, {Key: "o", Value: bson.D{{Key: "_id", Value: id}}}},
			bson.D{{Key: "op", Value: "u"}, {Key: "ns", Value: "s.o"}, {Key: "o2", Value: bson.D{{Key: "_id", Value: id}}}},
		}}}}}, false},
	}
	for _, tc := range cases {
		if got := firstOpIsInsert(raw(tc.e), "s.o", idv); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
