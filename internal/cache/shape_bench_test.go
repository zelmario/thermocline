package cache

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Shape runs on every read of every collection whenever the advisor is on,
// which is the default and which is the hot path this whole system is for. It
// is worth knowing what it costs.
func BenchmarkShape(b *testing.B) {
	cmd, err := bson.Marshal(bson.D{
		{Key: "find", Value: "orders"},
		{Key: "filter", Value: bson.D{{Key: "grp", Value: "g7"}, {Key: "n", Value: bson.D{{Key: "$gte", Value: 100}}}}},
		{Key: "limit", Value: 20},
		{Key: "$db", Value: "shop"},
		{Key: "lsid", Value: bson.D{{Key: "id", Value: "x"}}},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "primary"}}},
	})
	if err != nil {
		b.Fatal(err)
	}
	raw := bson.Raw(cmd)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if s, _ := Shape("find", "shop.orders", raw); s == "" {
			b.Fatal("empty shape")
		}
	}
}
