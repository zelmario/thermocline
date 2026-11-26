package livecache

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// An update's looked-up document can already hold a later write to the same
// document in the same batch. Between applying the first and the second, the
// cache's position is the first update and its entry holds the second's value.
func TestLookupAheadIsHeld(t *testing.T) {
	ev := func(i uint32, price float64) bson.Raw {
		return raw(t, bson.D{
			{Key: "operationType", Value: "update"},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "products"}}},
			{Key: "documentKey", Value: bson.D{{Key: "_id", Value: "p1"}}},
			{Key: "clusterTime", Value: bson.Timestamp{T: 20, I: i}},
			{Key: "fullDocument", Value: product("p1", "a", price)},
			{Key: "updated", Value: bson.A{"price"}},
		})
	}
	none := func(string) bool { return false }
	q := listQuery(t, "a", 3)

	c := New(1 << 20)
	c.install(q, []bson.Raw{raw(t, product("p1", "a", 1))}, true, 0)
	c.Advance(bson.Timestamp{T: 19})
	// Both updates looked up after the second: the first already carries
	// price 3, which the database only had at {20, 3}. Between them a write
	// to another collection, as a comment was between two views of an
	// article on the box, closes the first update's time, so nothing else
	// keeps the listing from answering at {20, 1} with {20, 3}'s price.
	other := raw(t, bson.D{
		{Key: "operationType", Value: "insert"},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "comments"}}},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: "c1"}}},
		{Key: "clusterTime", Value: bson.Timestamp{T: 20, I: 2}},
		{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: "c1"}}},
	})
	batch := []bson.Raw{ev(1, 3), other, ev(3, 3)}
	fs := newFillState()
	c.holdLookups(batch, false, fs)
	c.handle(batch[0], none, fs)
	c.handle(batch[1], none, fs)
	if got, hit := c.Get(q); hit {
		t.Fatalf("answered between the two updates with %s", got)
	}
	c.handle(batch[2], none, fs)
	c.Advance(bson.Timestamp{T: 20, I: 3})
	if _, hit := c.Get(q); !hit {
		t.Fatal("still held once the stream applied the second update")
	}

	// One update of a document in a batch has nothing later to hold.
	c2 := New(1 << 20)
	c2.install(q, []bson.Raw{raw(t, product("p1", "a", 1))}, true, 0)
	c2.Advance(bson.Timestamp{T: 19})
	fs2 := newFillState()
	one := []bson.Raw{ev(1, 3)}
	c2.holdLookups(one, false, fs2)
	c2.handle(one[0], none, fs2)
	c2.Advance(bson.Timestamp{T: 20, I: 1})
	if _, hit := c2.Get(q); !hit {
		t.Fatal("a single update held the namespace")
	}

	// A batch cut short may end before writes its lookups hold: the
	// namespace's entries go before it is applied.
	c3 := New(1 << 20)
	c3.install(q, []bson.Raw{raw(t, product("p1", "a", 1))}, true, 0)
	c3.Advance(bson.Timestamp{T: 19})
	fs3 := newFillState()
	c3.holdLookups(one, true, fs3)
	if _, hit := c3.Get(q); hit {
		t.Fatal("a cut batch's namespace kept its entries")
	}
}
