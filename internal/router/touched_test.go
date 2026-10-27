package router

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/checkpoint"
)

func TestTouchTracker(t *testing.T) {
	val := func(v any) bson.RawValue {
		typ, b, err := bson.MarshalValue(v)
		if err != nil {
			t.Fatal(err)
		}
		return bson.RawValue{Type: typ, Value: b}
	}
	ts := func(s uint32) bson.Timestamp { return bson.Timestamp{T: s, I: 1} }
	at := func(s uint32) uint64 { return checkpoint.PackTS(ts(s)) }

	tr := newTouchTracker()
	tr.touch("update", "shop", "orders", val(int32(1)), at(5))
	if _, _, ok := tr.ask("shop.orders", ts(10), ts(20), []bson.RawValue{val(int32(1))}, false); ok {
		t.Fatal("answered before the stream had a position")
	}
	tr.advance(at(10), false)
	tr.touch("update", "shop", "orders", val(int32(1)), at(12))
	tr.touch("delete", "shop", "orders", val("gone"), at(13))
	tr.advanceIn(at(20), true, 1)

	// rest is where the oplog has to take over; at or past until means none.
	ask := func(name string, from, until uint32, deletesOnly, want, wantOK bool, wantRest bson.Timestamp, ids ...any) {
		t.Helper()
		var vs []bson.RawValue
		for _, id := range ids {
			vs = append(vs, val(id))
		}
		got, rest, ok := tr.ask("shop.orders", ts(from), ts(until), vs, deletesOnly)
		if ok != wantOK || (ok && got != want) || (ok && !got && rest != wantRest) {
			t.Errorf("%s: touched=%v rest=%v ok=%v, want %v %v %v", name, got, rest, ok, want, wantRest, wantOK)
		}
	}
	all := ts(20)
	ask("an update in the window", 10, 15, false, true, true, all, int32(1))
	ask("the same _id as a double", 10, 15, false, true, true, all, 1.0)
	ask("the same _id as a long", 10, 15, false, true, true, all, int64(1))
	ask("an update is not a delete", 10, 15, true, false, true, all, int32(1))
	ask("a delete in the window", 10, 15, true, true, true, all, "gone")
	ask("before the window", 13, 15, false, false, true, all, int32(1), "gone")
	ask("an _id never touched", 10, 15, false, false, true, all, int32(2))
	ask("a window starting before the stream", 9, 15, false, false, false, all, int32(2))
	// Past where the stream has got: memory up to 20, the oplog after it.
	ask("a window past the stream", 10, 30, false, false, true, ts(20), int32(2))

	// A mark that is not inclusive leaves the entry at it to the oplog.
	tr.advance(at(25), false)
	ask("a window ending at a mark", 10, 25, false, false, true, bson.Timestamp{T: 25, I: 0}, int32(2))

	if !tr.deletedSince("shop.orders", ts(10)) || tr.deletedSince("shop.orders", ts(13)) || tr.deletedSince("shop.carts", ts(10)) {
		t.Error("deletedSince: want a delete after 10 in shop.orders only")
	}

	tr.touch("modify", "shop", "orders", bson.RawValue{}, at(16))
	ask("DDL touches every document", 15, 20, false, true, true, all, int32(2))
	tr.touch("dropDatabase", "shop", "", bson.RawValue{}, at(22))
	ask("a dropped database touches every document", 21, 22, true, true, true, all, int32(2))
}

// The narrow freshness bar: the newest write to a database once the tracker
// has read through a position, inserts included, and nothing older than the
// tracker's start.
func TestLastWriteIn(t *testing.T) {
	ts := func(s uint32) bson.Timestamp { return bson.Timestamp{T: s, I: 1} }
	at := func(s uint32) uint64 { return checkpoint.PackTS(ts(s)) }
	short := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		t.Cleanup(cancel)
		return ctx
	}

	tr := newTouchTracker()
	if _, ok := tr.lastWriteIn(short(), "shop", at(5), 1); ok {
		t.Fatal("answered before the tracker had a position")
	}
	tr.advanceIn(at(10), true, 1)
	tr.touch("insert", "shop", "orders", bson.RawValue{}, at(12))
	tr.touch("insert", "other", "things", bson.RawValue{}, at(14))
	tr.advanceIn(at(15), true, 1)

	if need, ok := tr.lastWriteIn(short(), "shop", at(15), 1); !ok || need != at(12) {
		t.Errorf("shop through 15: need %v ok %v, want the insert at 12", need, ok)
	}
	if need, ok := tr.lastWriteIn(short(), "quiet", at(15), 1); !ok || need != at(10) {
		t.Errorf("a database with no write seen: need %v ok %v, want the tracker's start", need, ok)
	}
	if _, ok := tr.lastWriteIn(short(), "shop", at(16), 1); ok {
		t.Error("answered for a position the tracker has not read")
	}

	// A wait for a position the tracker reaches while waiting.
	done := make(chan uint64, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		need, _ := tr.lastWriteIn(ctx, "shop", at(20), 1)
		done <- need
	}()
	time.Sleep(10 * time.Millisecond)
	tr.touch("update", "shop", "orders", bson.RawValue{}, at(18))
	tr.advanceIn(at(20), true, 1)
	if need := <-done; need != at(18) {
		t.Errorf("waited for 20: need %v, want the update at 18", need)
	}

	// Anything that may touch everything is a write to every database.
	tr.touchAll(at(21))
	tr.advanceIn(at(22), true, 1)
	if need, _ := tr.lastWriteIn(short(), "quiet", at(22), 1); need != at(21) {
		t.Errorf("after a touch of everything: need %v, want 21", need)
	}

	// A position from another term is another history: the tracker read the
	// old primary past where the new one's lastWrite stands, and has not read
	// the new primary's writes.
	if _, ok := tr.lastWriteIn(short(), "shop", at(22), 2); ok {
		t.Error("answered for a position written in a term the tracker has not read")
	}
	tr.advanceIn(at(23), true, 2)
	if _, ok := tr.lastWriteIn(short(), "shop", at(22), 2); !ok {
		t.Error("refused once the tracker read the same term")
	}
}

// reach waits for the stream to read up to a position, and gives up at its
// bound rather than wait for a stream that is not moving.
func TestTouchTrackerReach(t *testing.T) {
	tr := newTouchTracker()
	tr.advance(10, true)
	ctx := context.Background()
	if !tr.reach(ctx, 10, time.Millisecond) {
		t.Fatal("a position already read was not reached")
	}
	go func() {
		time.Sleep(2 * time.Millisecond)
		tr.advance(15, false)
		time.Sleep(2 * time.Millisecond)
		tr.advance(20, true)
	}()
	if !tr.reach(ctx, 20, time.Second) {
		t.Fatal("the stream reached 20 and the wait did not see it")
	}
	start := time.Now()
	if tr.reach(ctx, 30, 5*time.Millisecond) {
		t.Fatal("a position the stream never read was reached")
	}
	if waited := time.Since(start); waited > 200*time.Millisecond {
		t.Fatalf("gave up after %v, the bound is 5ms", waited)
	}
}

// A point read's check that waits for the stream must still see a delete the
// stream reads while it waits: the document was deleted inside the window, and
// the answer is no, not "nothing pending".
func TestNoDeletePendingSeesADeleteReadWhileWaiting(t *testing.T) {
	tr := newTouchTracker()
	tr.startAt(checkpoint.PackTS(bson.Timestamp{T: 100, I: 1}))
	tr.advance(checkpoint.PackTS(bson.Timestamp{T: 105, I: 1}), true)
	r := &Router{touches: tr}
	_, raw, _ := bson.MarshalValue(int32(7))
	id := bson.RawValue{Type: bson.TypeInt32, Value: raw}
	go func() {
		time.Sleep(2 * time.Millisecond)
		tr.touch("delete", "shop", "orders", id, checkpoint.PackTS(bson.Timestamp{T: 108, I: 1}))
		tr.advance(checkpoint.PackTS(bson.Timestamp{T: 110, I: 1}), true)
	}()
	from, until := bson.Timestamp{T: 104, I: 1}, bson.Timestamp{T: 109, I: 1}
	if r.noDeletePendingIn(context.Background(), "shop.orders", id, from, until) {
		t.Fatal("a delete inside the window, read while the check waited, was answered as nothing pending")
	}
	if r.Stats.OplogChecksWaited.Load() != 1 {
		t.Fatalf("the check did not take the waited path (waited %d)", r.Stats.OplogChecksWaited.Load())
	}
}
