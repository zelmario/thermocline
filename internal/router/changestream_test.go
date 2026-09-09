package router

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
)

// A spent announcement answered for every delete of that document for the next
// thirty seconds, including the application's own.
//
// The evictor removes a document and the feeder applies that delete and spends
// the notice. The notice stays until its TTL, because a client reading its
// events late still has to be able to recognise the eviction. Until the notice
// recorded which delete it was spent on, the only way to use it was the window,
// and the window cannot tell one delete of a document from another.
//
// Measured on a fuzzer round: the evictor removed nine documents, the feeder
// applied them, and the application deleted the same nine twenty-two seconds
// later. All nine were dropped, so a client watching the collection was never
// told records it had deleted were gone.
func TestASpentNoticeAnswersForOneDeleteOnly(t *testing.T) {
	announced := time.Now().Add(-22 * time.Second)
	eviction := bson.Timestamp{T: 1000, I: 4}
	notes := map[string]evictionNote{
		"shop.orders|x": {left: 0, at: announced, spent: []bson.Timestamp{eviction}},
	}

	if !isEviction(notes, "shop.orders|x", announced, eviction) {
		t.Error("the eviction the notice was spent on was not recognised")
	}
	// The application's own delete, well inside the window and a different
	// oplog entry.
	later := bson.Timestamp{T: 1022, I: 1}
	if isEviction(notes, "shop.orders|x", time.Now(), later) {
		t.Error("a delete the notice was not spent on was read as an eviction")
	}
}

// An announcement the feeder has not applied yet may well be about this delete,
// and nothing can say otherwise until it catches up. Suppressing is the side
// that does not tell an application its data changed when it did not.
func TestAnUnclaimedNoticeStillSuppresses(t *testing.T) {
	notes := map[string]evictionNote{
		"shop.orders|x": {left: 1, at: time.Now()},
	}
	if !isEviction(notes, "shop.orders|x", time.Now(), bson.Timestamp{T: 9, I: 9}) {
		t.Error("an eviction the feeder has not applied yet was let through as a delete")
	}
}

// A notice written by a feeder from before the positions existed has no way to
// answer exactly, and a rolling upgrade must not stop suppressing evictions for
// an hour. The window is what answers for those.
func TestANoticeWithoutPositionsFallsBackToTheWindow(t *testing.T) {
	notes := map[string]evictionNote{
		"shop.orders|x": {left: 0, at: time.Now().Add(-5 * time.Second)},
	}
	if !isEviction(notes, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}) {
		t.Error("a notice from before the upgrade stopped recognising its own eviction")
	}
	notes["shop.orders|x"] = evictionNote{left: 0, at: time.Now().Add(-2 * time.Hour)}
	if isEviction(notes, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}) {
		t.Error("a notice from two hours ago still answered for a delete")
	}
}

// And a document nothing announced is never an eviction.
func TestADeleteWithNoNoticeIsNotAnEviction(t *testing.T) {
	if isEviction(map[string]evictionNote{}, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}) {
		t.Error("a delete with no announcement behind it was read as an eviction")
	}
}

// The list is capped, because a document can be evicted and warmed all day and
// the notice lives an hour.
func TestSpentPositionsAreCapped(t *testing.T) {
	var many []bson.Timestamp
	for i := 0; i < meta.MaxSpentRecorded+4; i++ {
		many = append(many, bson.Timestamp{T: uint32(1000 + i), I: 1})
	}
	n := meta.EvictionNotice{Spent: many[len(many)-meta.MaxSpentRecorded:]}
	if known, yes := n.SpentOn(many[len(many)-1]); !known || !yes {
		t.Error("the newest position was not kept")
	}
	if known, yes := n.SpentOn(many[0]); !known || yes {
		t.Error("a position older than the cap was still answered for")
	}
}
