package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestLiveShapeSignatureHasNoValues(t *testing.T) {
	find, _ := bson.Marshal(bson.D{
		{Key: "find", Value: "orders"},
		{Key: "filter", Value: bson.D{
			{Key: "status", Value: "secret-value"},
			{Key: "total", Value: bson.D{{Key: "$gt", Value: 99}, {Key: "$lt", Value: 1000}}},
			{Key: "$or", Value: bson.A{bson.D{{Key: "a.b", Value: bson.D{{Key: "$in", Value: bson.A{1, 2}}}}}}},
		}},
		{Key: "sort", Value: bson.D{{Key: "total", Value: -1}}},
		{Key: "projection", Value: bson.D{{Key: "x", Value: 1}, {Key: "y", Value: bson.D{{Key: "$slice", Value: 2}}}}},
		{Key: "hint", Value: "total_1"},
		{Key: "$db", Value: "shop"},
	})
	got := liveShapeSignature("find", find)
	want := "find filter{status:eq(string),total:$gt+$lt,$or[{a.b:$in}]} sort{total} proj{x,y:expr} opts{hint}"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}

	agg, _ := bson.Marshal(bson.D{
		{Key: "aggregate", Value: "orders"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "cust", Value: 7}}}},
			bson.D{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$cust"},
				{Key: "n", Value: bson.D{{Key: "$avg", Value: "$total"}}},
			}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "n", Value: -1}}}},
		}},
		{Key: "cursor", Value: bson.D{}},
	})
	got = liveShapeSignature("aggregate", agg)
	want = "aggregate [$match{cust:eq(32-bit integer)} $group{_id:field,$avg(field)} $sort{n}]"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
