package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The archive's copy goes into a null fullDocument after mongod has run the
// stream's own stages, so a stream whose stages read fullDocument is left as
// mongod answered it, and one whose stages do not is filled as before.
func TestShapesFullDocument(t *testing.T) {
	cs := bson.D{{Key: "$changeStream", Value: bson.D{{Key: "fullDocument", Value: "updateLookup"}}}}
	for _, c := range []struct {
		name   string
		stages bson.A
		want   bool
	}{
		{"no stages of its own", bson.A{cs}, false},
		{"a $match on the operation", bson.A{cs, bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "update"}}}}}, false},
		{"a $project removing a field of it", bson.A{cs, bson.D{{Key: "$project", Value: bson.D{{Key: "fullDocument.ssn", Value: 0}}}}}, true},
		{"a $match on a field of it", bson.A{cs, bson.D{{Key: "$match", Value: bson.D{{Key: "fullDocument.status", Value: "shipped"}}}}}, true},
		{"a $replaceRoot onto it", bson.A{cs, bson.D{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: "$fullDocument"}}}}}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			cmd := rawDoc(t, bson.D{{Key: "aggregate", Value: "orders"}, {Key: "pipeline", Value: c.stages}, {Key: "cursor", Value: bson.D{}}})
			if got := shapesFullDocument(cmd); got != c.want {
				t.Errorf("shapesFullDocument = %v, want %v", got, c.want)
			}
		})
	}
}
