package router

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/watermark"
)

// The proof is that the last document returned is itself newer than the claim.
// Everything this gets wrong is a wrong answer to a client, so the cases that
// must NOT prove anything matter more than the one that must.
func TestProvenNewestFirst(t *testing.T) {
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	mark := watermark.Mark{Claimed: true, From: from}
	inside, outside := from.Add(time.Hour), from.Add(-time.Hour)

	find := func(sort bson.D, limit int) bson.Raw {
		d := bson.D{{Key: "find", Value: "orders"}, {Key: "filter", Value: bson.D{}}}
		if sort != nil {
			d = append(d, bson.E{Key: "sort", Value: sort})
		}
		if limit > 0 {
			d = append(d, bson.E{Key: "limit", Value: limit})
		}
		return rawDoc(t, d)
	}
	replyOf := func(ages ...time.Time) bson.Raw {
		docs := make(bson.A, 0, len(ages))
		for i, at := range ages {
			docs = append(docs, bson.D{{Key: "_id", Value: i}, {Key: "at", Value: at}})
		}
		return rawDoc(t, bson.D{
			{Key: "cursor", Value: bson.D{
				{Key: "id", Value: int64(0)},
				{Key: "ns", Value: "shop.orders"},
				{Key: "firstBatch", Value: docs}}},
			{Key: "ok", Value: 1.0}})
	}
	desc := bson.D{{Key: "at", Value: -1}}

	t.Run("the last of the N is inside the claim", func(t *testing.T) {
		if !provenNewestFirst(find(desc, 2), replyOf(inside, inside), "at", true, mark) {
			t.Error("this is the case the whole thing exists for")
		}
	})

	// Each of these would be a wrong answer if it proved anything.
	bad := []struct {
		name  string
		cmd   bson.Raw
		reply bson.Raw
	}{
		{"the last one is older than the claim", find(desc, 2), replyOf(inside, outside)},
		{"fewer returned than asked for, so the fast tier ran out",
			find(desc, 3), replyOf(inside, inside)},
		{"no limit, so the query wants every match", find(desc, 0), replyOf(inside, inside)},
		{"sorted oldest first", find(bson.D{{Key: "at", Value: 1}}, 2), replyOf(inside, inside)},
		{"sorted by another field", find(bson.D{{Key: "other", Value: -1}}, 2), replyOf(inside, inside)},
		{"another field leading a compound sort",
			find(bson.D{{Key: "other", Value: -1}, {Key: "at", Value: -1}}, 2), replyOf(inside, inside)},
		{"no sort at all", find(nil, 2), replyOf(inside, inside)},
		{"an empty batch", find(desc, 2), replyOf()},
		// The sort runs on the stored field and the proof reads the returned
		// one, which a computed projection can replace.
		{"a projection computing the age field", withProjection(t, find(desc, 2),
			bson.D{{Key: "at", Value: "$updatedAt"}}), replyOf(inside, inside)},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			if provenNewestFirst(c.cmd, c.reply, "at", true, mark) {
				t.Error("proved complete, which would answer from a tier that may not hold it all")
			}
		})
	}

	t.Run("the age field leading a compound sort is fine", func(t *testing.T) {
		// A later key only breaks ties, and everything that ties is newer than
		// the claim too, so it is here.
		cmd := find(bson.D{{Key: "at", Value: -1}, {Key: "other", Value: 1}}, 2)
		if !provenNewestFirst(cmd, replyOf(inside, inside), "at", true, mark) {
			t.Error("a tie-break key should not stop this")
		}
	})

	t.Run("no claim proves nothing", func(t *testing.T) {
		if provenNewestFirst(find(desc, 2), replyOf(inside, inside), "at", true, watermark.Mark{}) {
			t.Error("proved complete with nothing claimed")
		}
	})
	t.Run("nothing says how this collection ages", func(t *testing.T) {
		if provenNewestFirst(find(desc, 2), replyOf(inside, inside), "at", false, mark) {
			t.Error("proved complete without knowing which field the claim is measured in")
		}
	})
}

// skip is the case the proof was not written with in mind, and it holds.
//
// The fast tier holds every document newer than T, so among those its ordering
// is the collection's ordering. If the last document returned is newer than T,
// then everything skipped over was newer still, and every document not returned
// is older. Ranks skip+1 through skip+N on the fast tier are therefore the
// collection's.
func TestProvenNewestFirstWithSkip(t *testing.T) {
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	mark := watermark.Mark{Claimed: true, From: from}
	inside, outside := from.Add(time.Hour), from.Add(-time.Hour)

	cmd := rawDoc(t, bson.D{
		{Key: "find", Value: "orders"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "at", Value: -1}}},
		{Key: "skip", Value: 5},
		{Key: "limit", Value: 2},
	})
	replyOf := func(ages ...time.Time) bson.Raw {
		docs := make(bson.A, 0, len(ages))
		for i, at := range ages {
			docs = append(docs, bson.D{{Key: "_id", Value: i}, {Key: "at", Value: at}})
		}
		return rawDoc(t, bson.D{
			{Key: "cursor", Value: bson.D{
				{Key: "id", Value: int64(0)}, {Key: "ns", Value: "shop.orders"},
				{Key: "firstBatch", Value: docs}}},
			{Key: "ok", Value: 1.0}})
	}

	if !provenNewestFirst(cmd, replyOf(inside, inside), "at", true, mark) {
		t.Error("a skip with the last one inside the claim is still proven")
	}
	if provenNewestFirst(cmd, replyOf(inside, outside), "at", true, mark) {
		t.Error("the last one is older than the claim, so nothing is proven")
	}
}

// A negative limit is MongoDB's way of asking for one batch, and it is not a
// count of documents this can reason about.
func TestProvenNewestFirstRefusesANegativeLimit(t *testing.T) {
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	mark := watermark.Mark{Claimed: true, From: from}
	cmd := rawDoc(t, bson.D{
		{Key: "find", Value: "orders"},
		{Key: "sort", Value: bson.D{{Key: "at", Value: -1}}},
		{Key: "limit", Value: -2},
	})
	reply := rawDoc(t, bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: int64(0)}, {Key: "ns", Value: "shop.orders"},
			{Key: "firstBatch", Value: bson.A{
				bson.D{{Key: "_id", Value: 1}, {Key: "at", Value: from.Add(time.Hour)}},
				bson.D{{Key: "_id", Value: 2}, {Key: "at", Value: from.Add(time.Hour)}}}}}},
		{Key: "ok", Value: 1.0}})
	if provenNewestFirst(cmd, reply, "at", true, mark) {
		t.Error("a negative limit was read as a count")
	}
}

func withProjection(t *testing.T, cmd bson.Raw, proj bson.D) bson.Raw {
	t.Helper()
	var d bson.D
	if err := bson.Unmarshal(cmd, &d); err != nil {
		t.Fatal(err)
	}
	return rawDoc(t, append(d, bson.E{Key: "projection", Value: proj}))
}

func TestProvenNewestFirstKeepsAPlainProjection(t *testing.T) {
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	mark := watermark.Mark{Claimed: true, From: from}
	cmd := withProjection(t, rawDoc(t, bson.D{{Key: "find", Value: "orders"}, {Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "at", Value: -1}}}, {Key: "limit", Value: 1}}),
		bson.D{{Key: "at", Value: 1}, {Key: "n", Value: 1}})
	reply := rawDoc(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "shop.orders"},
		{Key: "firstBatch", Value: bson.A{bson.D{{Key: "_id", Value: 1}, {Key: "at", Value: from.Add(time.Hour)}}}}}},
		{Key: "ok", Value: 1.0}})
	if !provenNewestFirst(cmd, reply, "at", true, mark) {
		t.Error("an include of the age field changes nothing about its value, and should still prove")
	}
}
