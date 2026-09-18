package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// What mongod actually carries from an upsert's filter into the document it
// creates, measured against 8.0 rather than reasoned about. Each row here was
// run as `updateOne(FILTER, {$set:{n:1}}, {upsert:true})` against a collection
// nothing tiers, and the document that came back is what decides the answer.
//
// This matters twice over, because equalityInFilter is shared: the reserved
// field guard asks it whether an upsert would write _tcDeletedAt, and the
// unique key registry asks it which keys the upsert is about to take. A
// spelling missed here is a tombstone mark written unnoticed and a cross-tier
// duplicate admitted unclaimed.
func TestEqualityInFilterMatchesWhatMongodCarries(t *testing.T) {
	raw := func(d bson.D) bson.Raw {
		b, err := bson.Marshal(d)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}
	cases := []struct {
		name   string
		filter bson.D
		want   bool
	}{
		// Carried. The document came back holding email.
		{"plain equality", bson.D{{Key: "email", Value: "a@x"}}, true},
		{"$eq", bson.D{{Key: "email", Value: bson.D{{Key: "$eq", Value: "b@x"}}}}, true},
		{"$in with one value", bson.D{{Key: "email", Value: bson.D{{Key: "$in", Value: bson.A{"c@x"}}}}}, true},
		{"$all with one value", bson.D{{Key: "email", Value: bson.D{{Key: "$all", Value: bson.A{"f@x"}}}}}, true},
		{"$and wrapping an equality", bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "email", Value: "g@x"}}, bson.D{{Key: "k", Value: 1}}}}}, true},
		{"$or with one clause", bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: "email", Value: "h@x"}}}}}, true},
		{"nested $and", bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "$and", Value: bson.A{bson.D{{Key: "email", Value: "i@x"}}}}}}}}, true},
		{"$and containing an $in of one", bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "email", Value: bson.D{{Key: "$in", Value: bson.A{"j@x"}}}}}}}}, true},

		// Not carried. The document came back without email, or mongod refused.
		{"$in with two values", bson.D{{Key: "email", Value: bson.D{{Key: "$in", Value: bson.A{"d@x", "e@x"}}}}}, false},
		{"$in with a regex", bson.D{{Key: "email", Value: bson.D{{Key: "$in", Value: bson.A{bson.Regex{Pattern: "^l"}}}}}}, false},
		{"$all with two values", bson.D{{Key: "email", Value: bson.D{{Key: "$all", Value: bson.A{"m@x", "n@x"}}}}}, false},
		{"$not wrapping equality", bson.D{{Key: "email", Value: bson.D{
			{Key: "$not", Value: bson.D{{Key: "$eq", Value: "o@x"}}}}}}, false},
		{"$gt", bson.D{{Key: "email", Value: bson.D{{Key: "$gt", Value: "a"}}}}, false},
		{"$or with two clauses", bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: "email", Value: "p@x"}}, bson.D{{Key: "email", Value: "q@x"}}}}}, false},
		{"the field is absent", bson.D{{Key: "k", Value: 1}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, got := equalityInFilter(raw(c.filter), "email")
			if got != c.want {
				t.Errorf("equalityInFilter = %v, want %v (mongod %s it)",
					got, c.want, map[bool]string{true: "carries", false: "does not carry"}[c.want])
			}
		})
	}
}
