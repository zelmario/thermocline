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

	if !isEviction(notes, "shop.orders|x", announced, eviction, "") {
		t.Error("the eviction the notice was spent on was not recognised")
	}
	// The application's own delete, well inside the window and a different
	// oplog entry.
	later := bson.Timestamp{T: 1022, I: 1}
	if isEviction(notes, "shop.orders|x", time.Now(), later, "") {
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
	if !isEviction(notes, "shop.orders|x", time.Now(), bson.Timestamp{T: 9, I: 9}, "") {
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
	if !isEviction(notes, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}, "") {
		t.Error("a notice from before the upgrade stopped recognising its own eviction")
	}
	notes["shop.orders|x"] = evictionNote{left: 0, at: time.Now().Add(-2 * time.Hour)}
	if isEviction(notes, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}, "") {
		t.Error("a notice from two hours ago still answered for a delete")
	}
}

// And a document nothing announced is never an eviction.
func TestADeleteWithNoNoticeIsNotAnEviction(t *testing.T) {
	if isEviction(map[string]evictionNote{}, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}, "") {
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

// A copy-up read late arrived as an ordinary insert, because promotedAt
// describes one promotion and a record outlives several.
//
// The document is copied up, evicted, and copied up again. One residency record
// survives all of it, and its promotedAt is the *later* promotion's, so the
// earlier copy-up's insert is older than the record accounting for it. The
// window read that as somebody else's write and let it through: an application
// shown an insert it never made, four of them in one fuzzer round at seed
// 90801, every promotedAt one to two seconds after the event it was judging.
//
// The feeder writes down which insert it spent each echo on, so both copy-ups
// are recognised however late they are read.
func TestAnEarlierCopyUpIsStillRecognisedAfterAnotherPromotion(t *testing.T) {
	first := bson.Timestamp{T: 1000, I: 1}
	second := bson.Timestamp{T: 1002, I: 3}
	// The record as it stands after the second copy-up. promotedAt moved with
	// it and is now the later promotion's, which is the whole difficulty: the
	// earlier insert is older than the record that has to account for it. The
	// measured gap, from the sieve instrumented at seed 90801, was promotedAt
	// 06:57:18.386 against an event at 06:57:16.169.
	promotedAt := time.Now().Add(-60 * time.Second)
	firstAt := promotedAt.Add(-2200 * time.Millisecond)
	promoted := map[string]promotion{
		"shop.orders|x": {
			at:     promotedAt,
			echoed: []bson.Timestamp{first, second},
		},
	}
	// Drained well after the promotion, which is what a consumer that stores a
	// resume token and comes back to it does.
	recent := time.Now().Add(-promotionWindow)

	if !isPromotion(promoted, "shop.orders|x", firstAt, first, recent) {
		t.Error("the earlier copy-up was delivered as an insert the application never made")
	}
	if !isPromotion(promoted, "shop.orders|x", promotedAt, second, recent) {
		t.Error("the later copy-up was delivered as an insert the application never made")
	}
	// And neither of them hides the application's own insert of that same _id.
	own := bson.Timestamp{T: 1120, I: 7}
	if isPromotion(promoted, "shop.orders|x", time.Now(), own, recent) {
		t.Error("an application's own insert was hidden as a promotion")
	}
}

// While an echo is outstanding the feeder has not written that insert down yet,
// so a miss means "not yet", not "not ours". Unconditional the way an unclaimed
// eviction announcement is, and for the same reason.
func TestAnOutstandingEchoStillSuppresses(t *testing.T) {
	promoted := map[string]promotion{
		"shop.orders|x": {
			at:      time.Now(),
			pending: true,
			echoed:  []bson.Timestamp{{T: 1000, I: 1}},
		},
	}
	if !isPromotion(promoted, "shop.orders|x", time.Now(), bson.Timestamp{T: 1090, I: 2},
		time.Now().Add(-promotionWindow)) {
		t.Error("a promotion whose echo the feeder has not spent yet was let through")
	}
}

// A record written by a feeder from before the positions existed has to go on
// answering through a rolling upgrade, so the window still decides for it.
func TestARecordWithoutPositionsFallsBackToTheWindow(t *testing.T) {
	recent := time.Now().Add(-promotionWindow)
	promoted := map[string]promotion{
		"shop.orders|x": {at: time.Now().Add(-2 * time.Second)},
	}
	if !isPromotion(promoted, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}, recent) {
		t.Error("a record from before the upgrade stopped recognising its own promotion")
	}
}

// An insert after the record ended is the application writing that _id again,
// whatever the record's history says.
func TestAnInsertAfterTheRecordEndedIsTheApplicationsOwn(t *testing.T) {
	ended := time.Now().Add(-30 * time.Second)
	promoted := map[string]promotion{
		"shop.orders|x": {
			at:     time.Now().Add(-90 * time.Second),
			ended:  ended,
			echoed: []bson.Timestamp{{T: 1000, I: 1}},
		},
	}
	if isPromotion(promoted, "shop.orders|x", ended.Add(time.Second), bson.Timestamp{T: 1100, I: 1},
		time.Now().Add(-promotionWindow)) {
		t.Error("an insert after the document left the fast tier was hidden as a promotion")
	}
}

// The evictor ends a hold while the copy-up it belongs to is still in flight.
//
// endedAt has two writers. The feeder writes it when the application deletes
// the document, which is what the check above is for. The evictor writes it as
// it evicts, on purpose, so the row outlives the hold for a change stream to
// read. A record ended by the evictor describes a promotion whose *hold* is
// over, not one whose insert has been accounted for.
//
// The numbers are the measured ones, from the sieve instrumented on 2026-09-24
// during a two-shop run: residency recorded at T, the evictor ending the hold
// 73ms later, and the copy-up's own insert reaching the sieve 5ms after that
// with its echo still outstanding and nothing spent. The shop reported it as an
// order placed that nobody placed.
func TestAHoldEndedByTheEvictorDoesNotUnmaskItsOwnCopyUp(t *testing.T) {
	promotedAt := time.Now().Add(-500 * time.Millisecond)
	ended := promotedAt.Add(73 * time.Millisecond)
	eventAt := ended.Add(5 * time.Millisecond)
	promoted := map[string]promotion{
		"shop.orders|x": {
			at:      promotedAt,
			ended:   ended,
			pending: true,
			// Nothing spent yet, which is the state that makes this the
			// copy-up's own insert and not a later one.
			echoed: nil,
		},
	}
	if !isPromotion(promoted, "shop.orders|x", eventAt, bson.Timestamp{T: 1790259343, I: 15},
		time.Now().Add(-promotionWindow)) {
		t.Error("a copy-up whose hold the evictor ended first was delivered as an insert nobody made")
	}
}

// And the other direction still holds: once the echo is spent, an insert after
// the record ended is the application writing that _id again.
//
// This is what the reordering had to keep. The ended check moved below the echo
// check, so the guard against hiding an application's own insert is now that
// the copy-up has been accounted for, which it has by the time a later insert
// of the same _id can exist.
func TestASpentEchoStillLetsTheApplicationsReinsertThrough(t *testing.T) {
	ended := time.Now().Add(-10 * time.Second)
	promoted := map[string]promotion{
		"shop.orders|x": {
			at:      ended.Add(-5 * time.Second),
			ended:   ended,
			pending: false,
			echoed:  []bson.Timestamp{{T: 1000, I: 1}},
		},
	}
	if isPromotion(promoted, "shop.orders|x", ended.Add(2*time.Second), bson.Timestamp{T: 1100, I: 1},
		time.Now().Add(-promotionWindow)) {
		t.Error("an application's own insert after the eviction was hidden as a promotion")
	}
}

// The list is capped, because a document can be copied up and evicted all day.
func TestEchoedPositionsAreCapped(t *testing.T) {
	rec := meta.ResidencyRecord{}
	for i := range meta.MaxEchoRecorded + 4 {
		rec.Echoed = append(rec.Echoed, bson.Timestamp{T: uint32(1000 + i), I: 1})
	}
	if len(rec.Echoed) <= meta.MaxEchoRecorded {
		t.Fatal("this test is not exercising the cap")
	}
	// What the $slice keeps is the newest, so the oldest is the one to lose.
	kept := rec.Echoed[len(rec.Echoed)-meta.MaxEchoRecorded:]
	capped := meta.ResidencyRecord{Echoed: kept}
	if known, yes := capped.EchoedAt(kept[0]); !known || !yes {
		t.Error("a position inside the cap was not recognised")
	}
	if known, yes := capped.EchoedAt(bson.Timestamp{T: 999, I: 1}); !known || yes {
		t.Error("a position that was never recorded was recognised")
	}
	if known, _ := (meta.ResidencyRecord{}).EchoedAt(bson.Timestamp{T: 1, I: 1}); known {
		t.Error("a record with no positions claimed to know the answer")
	}
}

// An echo nothing ever spent must not hide an application's own insert for as
// long as the record lives.
//
// The feeder only spends a notice whose promotion is inside its echo window, so
// an older one is never going to be spent. While the record died with its hold
// that self-corrected within minutes. Once the record started outliving the
// hold by an hour, so that a consumer reading late could still recognise a
// copy-up, an unspent echo would have gone on hiding every insert of that _id
// for that hour.
//
// Both halves matter: the copy-up's own insert is still recognised, because it
// sits within the promotion window of the promotion that caused it.
func TestAnEchoNobodySpentStopsAnsweringForLaterInserts(t *testing.T) {
	promotedAt := time.Now().Add(-45 * time.Minute)
	promoted := map[string]promotion{
		"shop.orders|x": {at: promotedAt, pending: true},
	}
	recent := time.Now().Add(-promotionWindow)

	// The copy-up's own insert, whenever it is read.
	if !isPromotion(promoted, "shop.orders|x", promotedAt.Add(20*time.Millisecond),
		bson.Timestamp{T: 1000, I: 1}, recent) {
		t.Error("a copy-up whose echo was never spent was delivered as an insert")
	}
	// The application writing that same _id three quarters of an hour later.
	if isPromotion(promoted, "shop.orders|x", time.Now(), bson.Timestamp{T: 9000, I: 1}, recent) {
		t.Error("an unspent echo hid an application's own insert made long afterwards")
	}
}

// A pending echo that has outlived the outstanding window still says the list
// of positions is missing the latest promotion, so a miss in it cannot be read
// as "not ours". The feeder used to look for echoes only while it believed a
// router was promoting, so an occasional copy-up left exactly this record:
// the first promotion's position written down, the second's never spent.
// Measured with test/echorace, a consumer seventy seconds late was shown the
// second copy-up as an insert, two rounds of two.
func TestAPendingEchoMakesThePositionListIncomplete(t *testing.T) {
	first := bson.Timestamp{T: 1000, I: 1}
	second := bson.Timestamp{T: 1080, I: 4}
	promotedAt := time.Now().Add(-70 * time.Second)
	promoted := map[string]promotion{
		"shop.orders|x": {at: promotedAt, pending: true, echoed: []bson.Timestamp{first}},
	}
	recent := time.Now().Add(-promotionWindow)
	if !isPromotion(promoted, "shop.orders|x", promotedAt.Add(9*time.Millisecond), second, recent) {
		t.Error("a copy-up whose echo was never spent was delivered as an insert the application never made")
	}
	// The application writing the _id again long after is still shown.
	if isPromotion(promoted, "shop.orders|x", time.Now(), bson.Timestamp{T: 1150, I: 2}, recent) {
		t.Error("an application's own insert, a minute after the promotion, was hidden")
	}
}

// A withdrawn echo makes the list incomplete in the same way. The promoter that
// withdrew it lost a race to insert, and the winner's insert is the echo it was
// withdrawing, so the feeder never wrote that position down. Measured with
// test/echorace, 32 reads across two routers: five rounds of five.
func TestAWithdrawnEchoMakesThePositionListIncomplete(t *testing.T) {
	first := bson.Timestamp{T: 1000, I: 1}
	promotedAt := time.Now().Add(-70 * time.Second)
	promoted := map[string]promotion{
		"shop.orders|x": {at: promotedAt, uncertain: true, echoed: []bson.Timestamp{first}},
	}
	recent := time.Now().Add(-promotionWindow)
	if !isPromotion(promoted, "shop.orders|x", promotedAt.Add(12*time.Millisecond), bson.Timestamp{T: 1080, I: 9}, recent) {
		t.Error("the race winner's copy-up was delivered as an insert the application never made")
	}
	if isPromotion(promoted, "shop.orders|x", time.Now(), bson.Timestamp{T: 1150, I: 2}, recent) {
		t.Error("an application's own insert, a minute after the promotion, was hidden")
	}
}

// The window hid the application's own insert of a document that a copy-up put
// on the fast tier a moment later, because it judged the promotion against now
// rather than against the event. Inside the TTL the exact list is complete, so
// the list answers: the application's insert is shown and the copy-up's is not.
// Measured on 2026-09-30, an order deleted through a second router while a
// warm was moving it: the order's insert missing from a stream read from before
// it, 6 of 6.
func TestAnInsertBeforeARecentPromotionIsShown(t *testing.T) {
	promotedAt := time.Now().Add(-2 * time.Second)
	echo := bson.Timestamp{T: 1002, I: 4}
	own := bson.Timestamp{T: 1000, I: 1}
	recent := time.Now().Add(-promotionWindow)
	rec := promotion{at: promotedAt, echoed: []bson.Timestamp{echo}}
	promoted := map[string]promotion{"shop.orders|x": rec}

	if isPromotion(promoted, "shop.orders|x", promotedAt.Add(-1500*time.Millisecond), own, recent) {
		t.Error("the application's insert from before the promotion was hidden")
	}
	if !isPromotion(promoted, "shop.orders|x", promotedAt.Add(20*time.Millisecond), echo, recent) {
		t.Error("the copy-up's own insert was delivered as the application's")
	}

	// The floor still decides wherever the list may be short.
	for name, p := range map[string]promotion{
		"withdrawn": {at: promotedAt, echoed: []bson.Timestamp{echo}, uncertain: true},
		"no list":   {at: promotedAt},
	} {
		promoted := map[string]promotion{"shop.orders|x": p}
		if !isPromotion(promoted, "shop.orders|x", promotedAt.Add(-1500*time.Millisecond), own, recent) {
			t.Errorf("%s: an insert the list cannot account for was let through inside the window", name)
		}
	}
	// And so does a record the TTL may have cleared between two promotions: an
	// insert older than the TTL before this one could be an echo it forgot.
	if !isPromotion(promoted, "shop.orders|x", promotedAt.Add(-2*time.Hour), own, recent) {
		t.Error("an insert older than a record's lifetime was let through inside the window")
	}
}

// An insert the feeder recorded as an echo is a promotion even when the record
// ended before it by the clocks. Wall times do not follow the oplog at this
// scale: a copy-up committed as a transaction carried 25.242 and the delete
// after it 25.220.
func TestARecordedEchoIsHiddenWhateverTheClocksSay(t *testing.T) {
	echo := bson.Timestamp{T: 1790789725, I: 2}
	promotedAt := time.Now().Add(-2 * time.Second)
	ended := promotedAt.Add(150 * time.Millisecond)
	promoted := map[string]promotion{"shop.orders|x": {at: promotedAt, ended: ended, echoed: []bson.Timestamp{echo}}}
	if !isPromotion(promoted, "shop.orders|x", ended.Add(18*time.Millisecond), echo, time.Now().Add(-promotionWindow)) {
		t.Error("a copy-up the feeder recorded was shown as an insert because its wall time fell after the record's end")
	}
}

// An application's delete of a document the evictor has announced and not yet
// removed is the application's: the notice names the evictor's session, and
// only a delete in that session is the eviction. It used to be hidden from the
// stream as one, and the archive kept the document.
func TestAnnouncedDocumentDeletedByTheApplicationIsShown(t *testing.T) {
	notes := map[string]evictionNote{"shop.orders|x": {left: 1, at: time.Now(), sessions: []string{"aa11"}}}
	if isEviction(notes, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}, "") {
		t.Error("a delete made outside any transaction was hidden as the evictor's")
	}
	if isEviction(notes, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}, "bb22") {
		t.Error("a delete in another session was hidden as the evictor's")
	}
	if !isEviction(notes, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}, "aa11") {
		t.Error("the evictor's own delete was shown")
	}
	legacy := map[string]evictionNote{"shop.orders|x": {left: 1, at: time.Now()}}
	if !isEviction(legacy, "shop.orders|x", time.Now(), bson.Timestamp{T: 1, I: 1}, "") {
		t.Error("a notice naming no session no longer hides its eviction")
	}
}
