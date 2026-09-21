package warmer

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Which archive index answers the warm's order without a sorting stage.
//
// The sort is {field: -1, _id: -1}. mongod can walk an index backwards, so an
// index with both directions flipped answers it too; it cannot mix directions,
// so one of each does not. And a direction is stored in whatever numeric type
// mongod chose, which is why this reads it rather than comparing renderings.
func TestIndexServesNewestFirst(t *testing.T) {
	key := func(pairs ...any) bson.Raw {
		d := bson.D{}
		for i := 0; i < len(pairs); i += 2 {
			d = append(d, bson.E{Key: pairs[i].(string), Value: pairs[i+1]})
		}
		raw, err := bson.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return bson.Raw(raw)
	}
	cases := []struct {
		name string
		k    bson.Raw
		want bool
	}{
		{"the order it reads in", key("at", int32(-1), "_id", int32(-1)), true},
		{"both flipped, walked backwards", key("at", int32(1), "_id", int32(1)), true},
		{"stored as a double", key("at", -1.0, "_id", -1.0), true},
		{"directions mixed, which cannot be walked", key("at", int32(1), "_id", int32(-1)), false},
		{"the field on its own, which still sorts", key("at", int32(-1)), false},
		{"the wrong field", key("other", int32(-1), "_id", int32(-1)), false},
		{"the right fields the wrong way round", key("_id", int32(-1), "at", int32(-1)), false},
		{"a longer index it leads", key("at", int32(-1), "_id", int32(-1), "n", int32(1)), true},
		{"a text key, which is not an order", key("at", "text", "_id", int32(-1)), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := indexServesNewestFirst(c.k, "at"); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}
