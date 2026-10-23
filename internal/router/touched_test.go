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
	if _, known := tr.question("shop.orders", ts(10), ts(20), []bson.RawValue{val(int32(1))}, false); known {
		t.Fatal("answered before the stream had a position")
	}
	tr.advance(at(10), false)
	tr.touch("update", "shop", "orders", val(int32(1)), at(12))
	tr.touch("delete", "shop", "orders", val("gone"), at(13))
	tr.advance(at(20), false)

	ask := func(name string, from, until uint32, deletesOnly bool, want, wantKnown bool, ids ...any) {
		t.Helper()
		var vs []bson.RawValue
		for _, id := range ids {
			vs = append(vs, val(id))
		}
		got, known := tr.question("shop.orders", ts(from), ts(until), vs, deletesOnly)
		if known != wantKnown || (known && got != want) {
			t.Errorf("%s: touched=%v known=%v, want %v %v", name, got, known, want, wantKnown)
		}
	}
	ask("an update in the window", 10, 15, false, true, true, int32(1))
	ask("the same _id as a double", 10, 15, false, true, true, 1.0)
	ask("the same _id as a long", 10, 15, false, true, true, int64(1))
	ask("an update is not a delete", 10, 15, true, false, true, int32(1))
	ask("a delete in the window", 10, 15, true, true, true, "gone")
	ask("before the window", 13, 15, false, false, true, int32(1), "gone")
	ask("an _id never touched", 10, 15, false, false, true, int32(2))
	ask("a window starting before the stream", 9, 15, false, false, false, int32(2))
	ask("a window the stream has not passed", 10, 20, false, false, false, int32(2))

	tr.advance(at(25), true)
	ask("at the end, after an event outside a transaction", 10, 25, false, true, true, int32(1))
	tr2 := newTouchTracker()
	tr2.advance(at(10), false)
	tr2.advance(at(30), false)
	if _, known := tr2.question("shop.orders", ts(10), ts(30), []bson.RawValue{val(int32(1))}, false); known {
		t.Error("answered at its end from a mark that may not cover it")
	}
	tr.touch("modify", "shop", "orders", bson.RawValue{}, at(16))
	tr.advance(at(21), false)
	ask("DDL touches every document", 15, 20, false, true, true, int32(2))
	tr.touch("dropDatabase", "shop", "", bson.RawValue{}, at(22))
	tr.advance(at(23), false)
	ask("a dropped database touches every document", 21, 22, true, true, true, int32(2))
}
