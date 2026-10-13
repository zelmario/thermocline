package feeder

import (
	"bytes"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestNamespaceFilterOneBranchPerDatabase pins the shape mongod is given: a
// branch per database, its collections in one $in, whatever order the list was
// written in. One branch per namespace cost a stream 2.5s over a 40,000
// document insertMany with 187 namespaces listed; see namespaceFilter.
func TestNamespaceFilterOneBranchPerDatabase(t *testing.T) {
	got := namespaceFilter([]string{"wh.b", "shop.orders", "shop.fs.chunks", "wh.a", "shop.events"})
	want := bson.A{
		bson.D{{Key: "ns.db", Value: "shop"}, {Key: "ns.coll", Value: bson.D{{Key: "$in", Value: bson.A{"events", "fs.chunks", "orders"}}}}},
		bson.D{{Key: "ns.db", Value: "wh"}, {Key: "ns.coll", Value: bson.D{{Key: "$in", Value: bson.A{"a", "b"}}}}},
	}
	gb, _ := bson.Marshal(bson.D{{Key: "$or", Value: got}})
	wb, _ := bson.Marshal(bson.D{{Key: "$or", Value: want}})
	if !bytes.Equal(gb, wb) {
		t.Fatalf("got  %s\nwant %s", bson.Raw(gb), bson.Raw(wb))
	}
}
