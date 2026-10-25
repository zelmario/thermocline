package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A view passes documents through when no stage changes _id or the age field.
// The age field may be dotted, and the check looked the dotted name up as one
// literal key, so a view writing meta, or meta.ts.x, was judged as leaving
// meta.ts alone and its reads were judged by the base collection's claim.
func TestViewPassesThroughReadsTheAgeFieldAsAPath(t *testing.T) {
	spec := func(stages ...bson.D) viewSpec {
		arr := bson.A{}
		for _, s := range stages {
			arr = append(arr, s)
		}
		_, raw, err := bson.MarshalValue(arr)
		if err != nil {
			t.Fatal(err)
		}
		return viewSpec{pipeline: raw}
	}
	cases := []struct {
		name  string
		view  viewSpec
		field string
		want  bool
	}{
		{"a field beside it", spec(bson.D{{Key: "$set", Value: bson.D{{Key: "other", Value: 1}}}}), "meta.ts", true},
		{"its parent replaced", spec(bson.D{{Key: "$set", Value: bson.D{{Key: "meta", Value: bson.D{{Key: "ts", Value: "$$NOW"}}}}}}), "meta.ts", false},
		{"itself, dotted", spec(bson.D{{Key: "$set", Value: bson.D{{Key: "meta.ts", Value: "$$NOW"}}}}), "meta.ts", false},
		{"below it", spec(bson.D{{Key: "$addFields", Value: bson.D{{Key: "meta.ts.x", Value: 1}}}}), "meta.ts", false},
		{"its parent unset", spec(bson.D{{Key: "$unset", Value: "meta"}}), "meta.ts", false},
		{"a sibling path", spec(bson.D{{Key: "$set", Value: bson.D{{Key: "meta.other", Value: 1}}}}), "meta.ts", true},
		{"_id below", spec(bson.D{{Key: "$set", Value: bson.D{{Key: "_id.k", Value: 1}}}}), "at", false},
	}
	for _, c := range cases {
		if got := c.view.passesThrough(c.field); got != c.want {
			t.Errorf("%s: passesThrough(%q) = %v, want %v", c.name, c.field, got, c.want)
		}
	}
}
