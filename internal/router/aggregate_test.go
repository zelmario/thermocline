package router

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func aggCmd(t *testing.T, coll string, pipeline bson.A) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(bson.D{
		{Key: "aggregate", Value: coll},
		{Key: "pipeline", Value: pipeline},
		{Key: "cursor", Value: bson.D{}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// countDocuments is an aggregate, which is how the completeness guarantee was
// being bypassed by the most common count in every driver.
func TestCountDocumentsIsSeenAsAnAggregation(t *testing.T) {
	cmd := aggCmd(t, "orders", bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "status", Value: "paid"}}}},
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}}}},
	})
	s, ok := scopeOfAggregate(cmd, "shop")
	if !ok {
		t.Fatal("scopeOfAggregate refused to read a countDocuments pipeline")
	}
	if s.primary != "shop.orders" {
		t.Errorf("primary = %q, want shop.orders", s.primary)
	}
	if s.bounded {
		t.Error("a $match on status was read as a time bound, which would let an unbounded scan through")
	}
}

// A leading $match is the only stage that limits how far back the scan reaches.
func TestLeadingMatchBoundsTheScan(t *testing.T) {
	from := time.Date(2026, 8, 14, 15, 0, 0, 0, time.UTC)
	cmd := aggCmd(t, "orders", bson.A{
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "created_at", Value: bson.D{{Key: "$gte", Value: from}}},
		}}},
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$status"}}}},
	})
	s, _ := scopeOfAggregate(cmd, "shop")
	if !s.bounded {
		t.Fatal("a leading $match with $gte was not read as a bound")
	}
	if !s.bound.Equal(from) {
		t.Errorf("bound = %v, want %v", s.bound, from)
	}
}

// A $match after another stage filters what has already been scanned. Reading
// it as a bound would claim the scan stopped where it did not.
func TestMatchAfterAnotherStageIsNotABound(t *testing.T) {
	from := time.Date(2026, 8, 14, 15, 0, 0, 0, time.UTC)
	cmd := aggCmd(t, "orders", bson.A{
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "created_at", Value: bson.D{{Key: "$gte", Value: from}}},
		}}},
	})
	s, _ := scopeOfAggregate(cmd, "shop")
	if s.bounded {
		t.Error("a $match behind a $sort was read as bounding the scan, which lets a full scan report as bounded")
	}
}

// Joined collections are read by key, not by time, so the guard has to know
// about them even though the command never names them as its target.
func TestJoinedCollectionsAreCollected(t *testing.T) {
	cmd := aggCmd(t, "orders", bson.A{
		bson.D{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "customers"},
			{Key: "localField", Value: "customer_id"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "customer"},
		}}},
		bson.D{{Key: "$unionWith", Value: "returns"}},
		bson.D{{Key: "$graphLookup", Value: bson.D{{Key: "from", Value: "referrals"}}}},
	})
	s, _ := scopeOfAggregate(cmd, "shop")

	want := map[string]bool{"shop.customers": false, "shop.returns": false, "shop.referrals": false}
	for _, ns := range s.secondary {
		if _, ok := want[ns]; !ok {
			t.Errorf("unexpected joined namespace %q", ns)
			continue
		}
		want[ns] = true
	}
	for ns, seen := range want {
		if !seen {
			t.Errorf("joined namespace %q was missed, so a short join would go unnoticed", ns)
		}
	}
}

// The object form of $unionWith names its collection differently.
func TestUnionWithObjectForm(t *testing.T) {
	cmd := aggCmd(t, "orders", bson.A{
		bson.D{{Key: "$unionWith", Value: bson.D{
			{Key: "coll", Value: "archive_orders"},
			{Key: "pipeline", Value: bson.A{}},
		}}},
	})
	s, _ := scopeOfAggregate(cmd, "shop")
	if len(s.secondary) != 1 || s.secondary[0] != "shop.archive_orders" {
		t.Errorf("secondary = %v, want [shop.archive_orders]", s.secondary)
	}
}

// Writing a short answer is worse than returning one, because it becomes the
// input to everything downstream.
func TestWritingStagesAreFlagged(t *testing.T) {
	for _, stage := range []string{"$out", "$merge"} {
		cmd := aggCmd(t, "orders", bson.A{
			bson.D{{Key: stage, Value: "summary"}},
		})
		s, _ := scopeOfAggregate(cmd, "shop")
		if !s.writes {
			t.Errorf("%s was not recognised as writing its result", stage)
		}
	}
}

// The database-level form has no collection, so there is nothing to be complete
// about and nothing to refuse.
func TestDatabaseLevelAggregateHasNoScope(t *testing.T) {
	raw, err := bson.Marshal(bson.D{
		{Key: "aggregate", Value: int32(1)},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$currentOp", Value: bson.D{}}}}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, ok := scopeOfAggregate(raw, "admin"); ok {
		t.Error("aggregate:1 was given a namespace to be complete about")
	}
}
