package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func pipelineCmd(t *testing.T, stages ...bson.D) bson.Raw {
	t.Helper()
	arr := bson.A{}
	for _, s := range stages {
		arr = append(arr, s)
	}
	raw, err := bson.Marshal(bson.D{
		{Key: "aggregate", Value: "orders"},
		{Key: "pipeline", Value: arr},
	})
	if err != nil {
		t.Fatal(err)
	}
	return bson.Raw(raw)
}

// The shapes this is for, and the limit read off each.
func TestNewestFirstPipelineTakesTheShape(t *testing.T) {
	sort := bson.D{{Key: "$sort", Value: bson.D{{Key: "createdAt", Value: -1}}}}
	limit := bson.D{{Key: "$limit", Value: 10}}
	match := bson.D{{Key: "$match", Value: bson.D{{Key: "k", Value: "a"}}}}
	proj := bson.D{{Key: "$project", Value: bson.D{
		{Key: "_id", Value: 0}, {Key: "createdAt", Value: 1}}}}
	unset := bson.D{{Key: "$unset", Value: "note"}}
	add := bson.D{{Key: "$addFields", Value: bson.D{{Key: "label", Value: "x"}}}}

	for _, tc := range []struct {
		name   string
		stages []bson.D
	}{
		{"sort and limit", []bson.D{sort, limit}},
		{"with a leading match", []bson.D{match, sort, limit}},
		{"with a trailing project keeping the field", []bson.D{sort, limit, proj}},
		{"with a trailing unset", []bson.D{sort, limit, unset}},
		{"with a trailing addFields on another field", []bson.D{sort, limit, add}},
		{"two leading matches, which a helper produces", []bson.D{match, match, sort, limit}},
		{"all of them", []bson.D{match, match, sort, limit, proj, add}},
	} {
		got, ok := newestFirstPipeline(pipelineCmd(t, tc.stages...), "createdAt")
		if !ok || got != 10 {
			t.Errorf("%s: limit %d ok %v, want 10 true", tc.name, got, ok)
		}
	}
}

// The shapes it must not take. Each of these would let the proof read a value
// the pipeline chose, or bound the wrong document, or reach another collection.
func TestNewestFirstPipelineRefusesTheRest(t *testing.T) {
	sort := bson.D{{Key: "$sort", Value: bson.D{{Key: "createdAt", Value: -1}}}}
	limit := bson.D{{Key: "$limit", Value: 10}}

	for _, tc := range []struct {
		name   string
		stages []bson.D
	}{
		{"ascending", []bson.D{
			{{Key: "$sort", Value: bson.D{{Key: "createdAt", Value: 1}}}}, limit}},
		{"sorted by another field", []bson.D{
			{{Key: "$sort", Value: bson.D{{Key: "total", Value: -1}}}}, limit}},
		{"no limit", []bson.D{sort}},
		{"limit before sort", []bson.D{limit, sort}},
		{"a project before the sort", []bson.D{
			{{Key: "$project", Value: bson.D{{Key: "createdAt", Value: 1}}}}, sort, limit}},
		{"a trailing sort, which moves the proof", []bson.D{sort, limit,
			{{Key: "$sort", Value: bson.D{{Key: "createdAt", Value: 1}}}}}},
		{"a trailing addFields rewriting the age", []bson.D{sort, limit,
			{{Key: "$addFields", Value: bson.D{{Key: "createdAt", Value: "$updatedAt"}}}}}},
		{"a trailing project computing the age", []bson.D{sort, limit,
			{{Key: "$project", Value: bson.D{{Key: "createdAt", Value: "$updatedAt"}}}}}},
		{"a trailing project rewriting a parent", []bson.D{sort, limit,
			{{Key: "$project", Value: bson.D{{Key: "createdAt", Value: bson.D{
				{Key: "$literal", Value: "now"}}}}}}}},
		{"a replaceWith", []bson.D{sort, limit,
			{{Key: "$replaceWith", Value: bson.D{{Key: "createdAt", Value: "$updatedAt"}}}}}},
		{"a lookup, which reads another collection", []bson.D{sort, limit,
			{{Key: "$lookup", Value: bson.D{{Key: "from", Value: "movies"}}}}}},
		{"a group", []bson.D{sort, limit,
			{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}}}}}},
		{"a zero limit", []bson.D{sort, {{Key: "$limit", Value: 0}}}},
	} {
		if _, ok := newestFirstPipeline(pipelineCmd(t, tc.stages...), "createdAt"); ok {
			t.Errorf("%s: taken, want refused", tc.name)
		}
	}
}

// Aged by the _id, where a projection dropping _id is a miss and not a lie.
func TestNewestFirstPipelineByObjectID(t *testing.T) {
	sort := bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: -1}}}}
	limit := bson.D{{Key: "$limit", Value: 5}}
	if _, ok := newestFirstPipeline(pipelineCmd(t, sort, limit), ""); !ok {
		t.Error("sort by _id descending refused")
	}
	drop := bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}}}}
	if _, ok := newestFirstPipeline(pipelineCmd(t, sort, limit, drop), ""); !ok {
		t.Error("dropping _id refused; it should be taken and then fail the proof")
	}
	rewrite := bson.D{{Key: "$addFields", Value: bson.D{{Key: "_id", Value: "$other"}}}}
	if _, ok := newestFirstPipeline(pipelineCmd(t, sort, limit, rewrite), ""); ok {
		t.Error("rewriting _id taken, want refused")
	}
}
