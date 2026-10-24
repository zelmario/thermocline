package router

import (
	"testing"

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
	tr.advance(at(20), true)

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
