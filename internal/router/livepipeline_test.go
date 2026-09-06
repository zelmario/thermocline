package router

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// rawPipeline turns a pipeline written as Go values into the bson.RawValue the
// rewrite is given, which is how it arrives from a client's command.
func rawPipeline(t *testing.T, stages bson.A) bson.RawValue {
	t.Helper()
	doc, err := bson.Marshal(bson.D{{Key: "pipeline", Value: stages}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	val, err := bson.Raw(doc).LookupErr("pipeline")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	return val
}

func renderedPipeline(t *testing.T, out bson.A) string {
	t.Helper()
	doc, err := bson.Marshal(bson.D{{Key: "pipeline", Value: out}})
	if err != nil {
		t.Fatalf("marshal out: %v", err)
	}
	return bson.Raw(doc).String()
}

// Every stage that reads a collection of its own has to be given the tombstone
// filter, because the one in front of the pipeline never reaches it. Asserted
// on the rendered command rather than by walking it, so a stage that loses a
// field in the rewrite shows up too.
func TestLivePipelineFiltersJoinedCollections(t *testing.T) {
	cases := []struct {
		name   string
		stages bson.A
		want   []string
	}{
		{
			name: "lookup by field takes a pipeline it did not have",
			stages: bson.A{bson.D{{Key: "$lookup", Value: bson.D{
				{Key: "from", Value: "orders"},
				{Key: "localField", Value: "grp"},
				{Key: "foreignField", Value: "grp"},
				{Key: "as", Value: "sib"},
			}}}},
			want: []string{`"from": "orders"`, `"localField": "grp"`, `"as": "sib"`,
				`"pipeline": [{"$match": {"_tcDeletedAt": {"$exists": false}}}]`},
		},
		{
			name: "lookup with a pipeline keeps its own stages under the filter",
			stages: bson.A{bson.D{{Key: "$lookup", Value: bson.D{
				{Key: "from", Value: "orders"},
				{Key: "as", Value: "sib"},
				{Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "n", Value: 1}}}}}},
			}}}},
			want: []string{`"$match": {"_tcDeletedAt": {"$exists": false}}`, `"$match": {"n": {"$numberInt":"1"}}`},
		},
		{
			name: "a lookup naming no collection is left alone",
			stages: bson.A{bson.D{{Key: "$lookup", Value: bson.D{
				{Key: "as", Value: "d"},
				{Key: "pipeline", Value: bson.A{bson.D{{Key: "$documents", Value: bson.A{}}}}},
			}}}},
			want: []string{`"$lookup": {"as": "d","pipeline": [{"$documents": []}]}`},
		},
		{
			name:   "unionWith named as a string becomes the document form",
			stages: bson.A{bson.D{{Key: "$unionWith", Value: "orders"}}},
			want:   []string{`"coll": "orders"`, `"$match": {"_tcDeletedAt": {"$exists": false}}`},
		},
		{
			name: "unionWith with a pipeline",
			stages: bson.A{bson.D{{Key: "$unionWith", Value: bson.D{
				{Key: "coll", Value: "orders"},
				{Key: "pipeline", Value: bson.A{bson.D{{Key: "$count", Value: "c"}}}},
			}}}},
			want: []string{`"$match": {"_tcDeletedAt": {"$exists": false}}`, `"$count": "c"`},
		},
		{
			name: "graphLookup gains a restriction",
			stages: bson.A{bson.D{{Key: "$graphLookup", Value: bson.D{
				{Key: "from", Value: "orders"},
				{Key: "startWith", Value: "$parent"},
				{Key: "connectFromField", Value: "parent"},
				{Key: "connectToField", Value: "_id"},
				{Key: "as", Value: "tree"},
			}}}},
			want: []string{`"restrictSearchWithMatch": {"_tcDeletedAt": {"$exists": false}}`},
		},
		{
			name: "graphLookup keeps the restriction it had",
			stages: bson.A{bson.D{{Key: "$graphLookup", Value: bson.D{
				{Key: "from", Value: "orders"},
				{Key: "startWith", Value: "$parent"},
				{Key: "connectFromField", Value: "parent"},
				{Key: "connectToField", Value: "_id"},
				{Key: "as", Value: "tree"},
				{Key: "restrictSearchWithMatch", Value: bson.D{{Key: "live", Value: true}}},
			}}}},
			want: []string{`"restrictSearchWithMatch": {"$and": [{"live": true},{"_tcDeletedAt": {"$exists": false}}]}`},
		},
		{
			name: "a lookup nested inside another lookup",
			stages: bson.A{bson.D{{Key: "$lookup", Value: bson.D{
				{Key: "from", Value: "orders"},
				{Key: "as", Value: "sib"},
				{Key: "pipeline", Value: bson.A{
					bson.D{{Key: "$lookup", Value: bson.D{
						{Key: "from", Value: "items"},
						{Key: "localField", Value: "sku"},
						{Key: "foreignField", Value: "sku"},
						{Key: "as", Value: "it"},
					}}},
				}},
			}}}},
			want: []string{`"from": "items"`, `"from": "orders"`},
		},
		{
			name: "a lookup inside a facet",
			stages: bson.A{bson.D{{Key: "$facet", Value: bson.D{
				{Key: "one", Value: bson.A{bson.D{{Key: "$lookup", Value: bson.D{
					{Key: "from", Value: "orders"},
					{Key: "localField", Value: "grp"},
					{Key: "foreignField", Value: "grp"},
					{Key: "as", Value: "sib"},
				}}}}},
			}}}},
			want: []string{`"from": "orders"`, `"$match": {"_tcDeletedAt": {"$exists": false}}`},
		},
		{
			name:   "an ordinary stage is untouched",
			stages: bson.A{bson.D{{Key: "$sort", Value: bson.D{{Key: "n", Value: 1}}}}},
			want:   []string{`"$sort": {"n": {"$numberInt":"1"}}`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := renderedPipeline(t, livePipeline(rawPipeline(t, tc.stages)))
			// The pipeline's own leading filter, in every case.
			if !strings.HasPrefix(got, `{"pipeline": [{"$match": {"_tcDeletedAt": {"$exists": false}}}`) {
				t.Errorf("the outer filter is not first: %s", got)
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("missing %s in %s", want, got)
				}
			}
		})
	}
}

// A facet's sub-pipeline reads the same input as the pipeline it is in, which
// the outer filter has already narrowed, so it must not be given a filter of
// its own: that would refer to a field the documents reaching it may no longer
// carry after a $project.
func TestLivePipelineDoesNotFilterFacetInput(t *testing.T) {
	stages := bson.A{bson.D{{Key: "$facet", Value: bson.D{
		{Key: "counted", Value: bson.A{bson.D{{Key: "$count", Value: "c"}}}},
	}}}}
	got := renderedPipeline(t, livePipeline(rawPipeline(t, stages)))
	if strings.Contains(got, `"counted": [{"$match"`) {
		t.Errorf("a facet's own input was filtered again: %s", got)
	}
	if !strings.Contains(got, `"counted": [{"$count": "c"}]`) {
		t.Errorf("the facet lost its stages: %s", got)
	}
}
