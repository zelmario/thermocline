package meta

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestAReplaceOfAwkwardNamesIsAPipeline(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  bson.D
		want bool
	}{
		{"plain", bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: bson.D{{Key: "w", Value: 1}}}}, false},
		{"top-level $", bson.D{{Key: "_id", Value: 1}, {Key: "$price", Value: 5}}, true},
		{"top-level dot", bson.D{{Key: "_id", Value: 1}, {Key: "a.b", Value: 5}}, true},
		{"nested $", bson.D{{Key: "_id", Value: 1}, {Key: "s", Value: bson.D{{Key: "$x", Value: 1}}}}, true},
		{"inside an array", bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: bson.A{bson.D{{Key: "b.c", Value: 1}}}}}, true},
	} {
		raw, err := bson.Marshal(tc.doc)
		if err != nil {
			t.Fatal(err)
		}
		_, pipeline := ReplaceModel(bson.D{{Key: "_id", Value: 1}}, raw).(*mongo.UpdateOneModel)
		if pipeline != tc.want {
			t.Errorf("%s: pipeline=%v, want %v", tc.name, pipeline, tc.want)
		}
	}
}
