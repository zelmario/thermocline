package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The namespaces an aggregation reaches are the ones the gate used to miss. Each
// case here is a stage mongod authorizes separately from the collection the
// command was sent to.
func TestAggregateNeedsEveryNamespaceItTouches(t *testing.T) {
	need := func(db, coll string, actions ...string) string {
		return db + "." + coll + ":" + join(actions)
	}
	cases := []struct {
		name     string
		pipeline bson.A
		want     []string
	}{
		{
			"a $lookup source is read",
			bson.A{bson.D{{Key: "$lookup", Value: bson.D{
				{Key: "from", Value: "secret"}, {Key: "as", Value: "j"}}}}},
			[]string{need("shop", "secret", "find")},
		},
		{
			"a $unionWith source is read",
			bson.A{bson.D{{Key: "$unionWith", Value: "secret"}}},
			[]string{need("shop", "secret", "find")},
		},
		{
			"a $lookup nested in a $facet is still read",
			bson.A{bson.D{{Key: "$facet", Value: bson.D{{Key: "a", Value: bson.A{
				bson.D{{Key: "$lookup", Value: bson.D{
					{Key: "from", Value: "secret"}, {Key: "as", Value: "j"}}}},
			}}}}}},
			[]string{need("shop", "secret", "find")},
		},
		{
			"an $out target is written and cleared",
			bson.A{bson.D{{Key: "$out", Value: "target"}}},
			[]string{need("shop", "target", "insert", "remove")},
		},
		{
			"an $out target in another database",
			bson.A{bson.D{{Key: "$out", Value: bson.D{
				{Key: "db", Value: "wh"}, {Key: "coll", Value: "target"}}}}},
			[]string{need("wh", "target", "insert", "remove")},
		},
		{
			"a $merge target is written",
			bson.A{bson.D{{Key: "$merge", Value: bson.D{{Key: "into", Value: "target"}}}}},
			[]string{need("shop", "target", "insert", "update")},
		},
		{
			"a $merge target in another database",
			bson.A{bson.D{{Key: "$merge", Value: bson.D{{Key: "into", Value: bson.D{
				{Key: "db", Value: "wh"}, {Key: "coll", Value: "target"}}}}}}},
			[]string{need("wh", "target", "insert", "update")},
		},
		{
			"a pipeline that reads one collection and writes another",
			bson.A{
				bson.D{{Key: "$lookup", Value: bson.D{
					{Key: "from", Value: "secret"}, {Key: "as", Value: "j"}}}},
				bson.D{{Key: "$out", Value: "target"}},
			},
			[]string{need("shop", "secret", "find"), need("shop", "target", "insert", "remove")},
		},
		{
			"a pipeline touching nothing else",
			bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "n", Value: 1}}}}},
			nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := rawDoc(t, bson.D{
				{Key: "aggregate", Value: "orders"},
				{Key: "pipeline", Value: tc.pipeline},
				{Key: "cursor", Value: bson.D{}},
			})
			var got []string
			for _, n := range aggregateNeeds(cmd, "shop") {
				got = append(got, need(n.DB, n.Coll, n.Actions...))
			}
			if len(got) != len(tc.want) {
				t.Fatalf("needs = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("needs[%d] = %s, want %s", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func join(s []string) string {
	out := ""
	for i, v := range s {
		if i > 0 {
			out += "+"
		}
		out += v
	}
	return out
}

// mongod keys a $merge's privileges by what it does when a document matches and
// when none does. Asking for both actions every time refuses a user who may
// update a collection and not insert into it, running a merge that could never
// have inserted.
func TestMergeAsksOnlyForWhatItCanDo(t *testing.T) {
	cases := []struct {
		name  string
		merge bson.D
		want  string
	}{
		{"the defaults can do either", bson.D{{Key: "into", Value: "t"}}, "insert+update"},
		{"fail on match, insert otherwise", bson.D{{Key: "into", Value: "t"},
			{Key: "whenMatched", Value: "fail"}, {Key: "whenNotMatched", Value: "insert"}}, "insert"},
		{"merge on match, discard otherwise", bson.D{{Key: "into", Value: "t"},
			{Key: "whenMatched", Value: "merge"}, {Key: "whenNotMatched", Value: "discard"}}, "update"},
		{"replace on match, fail otherwise", bson.D{{Key: "into", Value: "t"},
			{Key: "whenMatched", Value: "replace"}, {Key: "whenNotMatched", Value: "fail"}}, "update"},
		{"keepExisting on match, insert otherwise", bson.D{{Key: "into", Value: "t"},
			{Key: "whenMatched", Value: "keepExisting"}, {Key: "whenNotMatched", Value: "insert"}}, "insert+update"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := rawDoc(t, bson.D{
				{Key: "aggregate", Value: "orders"},
				{Key: "pipeline", Value: bson.A{bson.D{{Key: "$merge", Value: tc.merge}}}},
			})
			needs := aggregateNeeds(cmd, "shop")
			if len(needs) != 1 {
				t.Fatalf("needs = %+v", needs)
			}
			if got := join(needs[0].Actions); got != tc.want {
				t.Errorf("actions = %s, want %s", got, tc.want)
			}
		})
	}
}

// A stage that describes a collection rather than reading it declares its own
// action. Judging those by find alone let a user holding find read index usage
// and plan cache entries, and open a change stream, that mongod would refuse.
func TestStagesThatDeclareTheirOwnPrivilege(t *testing.T) {
	cases := []struct {
		stage string
		want  string
	}{
		{"$indexStats", "indexStats"},
		{"$collStats", "collStats"},
		{"$planCacheStats", "planCacheRead"},
		{"$changeStream", "changeStream+find"},
	}
	for _, tc := range cases {
		t.Run(tc.stage, func(t *testing.T) {
			cmd := rawDoc(t, bson.D{
				{Key: "aggregate", Value: "orders"},
				{Key: "pipeline", Value: bson.A{bson.D{{Key: tc.stage, Value: bson.D{}}}}},
			})
			needs := aggregateNeeds(cmd, "shop")
			if len(needs) != 1 || needs[0].Coll != "orders" {
				t.Fatalf("needs = %+v", needs)
			}
			if got := join(needs[0].Actions); got != tc.want {
				t.Errorf("actions = %s, want %s", got, tc.want)
			}
		})
	}
}

// Skipping the collection's own validation rules is a privilege of its own.
func TestBypassingValidationIsItsOwnPrivilege(t *testing.T) {
	cmd := rawDoc(t, bson.D{
		{Key: "aggregate", Value: "orders"},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$out", Value: "target"}}}},
		{Key: "bypassDocumentValidation", Value: true},
	})
	needs := aggregateNeeds(cmd, "shop")
	if len(needs) != 1 || join(needs[0].Actions) != "insert+remove+bypassDocumentValidation" {
		t.Fatalf("needs = %+v", needs)
	}
}
