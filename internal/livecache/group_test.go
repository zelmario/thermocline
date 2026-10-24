package livecache

import (
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func groupQuery(t *testing.T, pipeline bson.A) Query {
	t.Helper()
	q, ok := ParseGroupAgg("shop.orders", raw(t, bson.D{{Key: "aggregate", Value: "orders"}, {Key: "pipeline", Value: pipeline}, {Key: "cursor", Value: bson.D{}}}))
	if !ok {
		t.Fatalf("not understood: %v", pipeline)
	}
	return q
}

func order(id string, at time.Time, total any, items ...bson.D) bson.D {
	a := bson.A{}
	for _, it := range items {
		a = append(a, it)
	}
	return bson.D{{Key: "_id", Value: id}, {Key: "at", Value: at}, {Key: "total", Value: total}, {Key: "items", Value: a}}
}

func item(sku string, qty int32) bson.D {
	return bson.D{{Key: "sku", Value: sku}, {Key: "qty", Value: qty}}
}

// groups reads a grouped reply as _id -> the rest, printed.
func groups(t *testing.T, reply bson.Raw) map[string]string {
	t.Helper()
	b, ok := batchOfRaw(reply)
	if !ok {
		t.Fatalf("no batch in %v", reply)
	}
	out := map[string]string{}
	for _, d := range b {
		e, _ := d.Elements()
		var parts []string
		for _, x := range e[1:] {
			parts = append(parts, x.Key()+"="+x.Value().String())
		}
		out[e[0].Value().String()] = strings.Join(parts, " ")
	}
	return out
}

func TestGroupParse(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, p := range []bson.A{
		{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}}},
		{bson.D{{Key: "$match", Value: bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: day}}}}}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$hour", Value: "$at"}}}, {Key: "revenue", Value: bson.D{{Key: "$sum", Value: "$total"}}}}}}},
		{bson.D{{Key: "$unwind", Value: "$items"}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$items.sku"}, {Key: "sold", Value: bson.D{{Key: "$sum", Value: "$items.qty"}}}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "sold", Value: -1}}}}, bson.D{{Key: "$limit", Value: int32(10)}}},
	} {
		groupQuery(t, p)
	}
	for _, p := range []bson.A{
		{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$a.b"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}}},
		{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(2)}}}}}}},
		{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "n", Value: bson.D{{Key: "$avg", Value: "$x"}}}}}}},
		{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "other", Value: 1}}}}},
		{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$week", Value: "$at"}}}}}}},
		{bson.D{{Key: "$match", Value: bson.D{{Key: "x", Value: 1}}}}},
		{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}}}}, bson.D{{Key: "$project", Value: bson.D{{Key: "x", Value: 1}}}}},
	} {
		if _, ok := ParseGroupAgg("shop.orders", raw(t, bson.D{{Key: "aggregate", Value: "orders"}, {Key: "pipeline", Value: p}, {Key: "cursor", Value: bson.D{}}})); ok {
			t.Errorf("accepted %v", p)
		}
	}
}

// Sales by hour kept from the stream: an insert adds to its hour, an update
// moves its total, a change of hour moves it between groups, a delete takes it
// away, and the types are mongod's.
func TestGroupFollowsTheStream(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	q := groupQuery(t, bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: day}, {Key: "$lt", Value: day.Add(24 * time.Hour)}}}}}},
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$hour", Value: "$at"}}},
			{Key: "orders", Value: bson.D{{Key: "$sum", Value: int32(1)}}}, {Key: "revenue", Value: bson.D{{Key: "$sum", Value: "$total"}}}}}},
	})
	c := New(1 << 20)
	c.install(q, []bson.Raw{
		raw(t, order("o1", day.Add(1*time.Hour), int32(10))),
		raw(t, order("o2", day.Add(1*time.Hour+time.Minute), int32(5))),
		raw(t, order("o3", day.Add(2*time.Hour), 2.5)),
	}, true, 0)
	at := uint32(1)
	apply := func(op, id string, d bson.D) {
		at++
		ch := Change{NS: "shop.orders", Op: op, ID: strVal(t, id), At: bson.Timestamp{T: at}}
		if d != nil {
			ch.Doc = raw(t, d)
		}
		c.Apply(ch)
	}
	check := func(want map[string]string) {
		t.Helper()
		got, hit := c.Get(q)
		if !hit {
			t.Fatal("miss")
		}
		g := groups(t, got)
		if len(g) != len(want) {
			t.Fatalf("groups %v, want %v", g, want)
		}
		for k, v := range want {
			if g[k] != v {
				t.Fatalf("group %s = %q, want %q (all %v)", k, g[k], v, g)
			}
		}
	}
	check(map[string]string{
		intStr(1): "orders=" + intStr(2) + " revenue=" + intStr(15),
		intStr(2): "orders=" + intStr(1) + " revenue=" + dblStr("2.5"),
	})
	apply("insert", "o4", order("o4", day.Add(1*time.Hour), int64(1)))
	apply("insert", "o5", order("o5", day.Add(48*time.Hour), int32(100))) // outside the $match
	check(map[string]string{
		intStr(1): "orders=" + intStr(3) + " revenue=" + longStr(16),
		intStr(2): "orders=" + intStr(1) + " revenue=" + dblStr("2.5"),
	})
	apply("update", "o3", order("o3", day.Add(1*time.Hour), 2.5)) // moves hour 2 -> 1
	check(map[string]string{
		intStr(1): "orders=" + intStr(4) + " revenue=" + dblStr("18.5"),
	})
	apply("delete", "o3", nil)
	apply("delete", "o4", nil)
	check(map[string]string{
		intStr(1): "orders=" + intStr(2) + " revenue=" + intStr(15),
	})
	// A total that is not a number is left out of the sum, as mongod does.
	apply("update", "o2", order("o2", day.Add(1*time.Hour), "five"))
	check(map[string]string{
		intStr(1): "orders=" + intStr(2) + " revenue=" + intStr(10),
	})
}

// Top sellers: the items are unwound, the sort and limit are applied, and a
// tie across the limit is not answered, since mongod picks either.
func TestGroupUnwindSortLimit(t *testing.T) {
	q := groupQuery(t, bson.A{
		bson.D{{Key: "$unwind", Value: "$items"}},
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$items.sku"}, {Key: "sold", Value: bson.D{{Key: "$sum", Value: "$items.qty"}}}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "sold", Value: -1}}}},
		bson.D{{Key: "$limit", Value: int32(2)}},
	})
	now := time.Now()
	c := New(1 << 20)
	c.install(q, []bson.Raw{
		raw(t, order("o1", now, 1, item("a", 5), item("b", 3))),
		raw(t, order("o2", now, 1, item("c", 1), item("a", 1))),
		raw(t, order("o3", now, 1)), // no items: $unwind drops it
	}, true, 0)
	got, hit := c.Get(q)
	if !hit {
		t.Fatal("miss")
	}
	b, _ := batchOfRaw(got)
	if len(b) != 2 || b[0].Lookup("_id").StringValue() != "a" || b[1].Lookup("_id").StringValue() != "b" {
		t.Fatalf("top two %v", b)
	}
	// c reaches 3 and ties b at the limit.
	c.Apply(Change{NS: "shop.orders", Op: "update", ID: strVal(t, "o2"), Doc: raw(t, order("o2", now, 1, item("c", 3), item("a", 1))), At: bson.Timestamp{T: 5}})
	if _, hit := c.Get(q); hit {
		t.Fatal("answered a tie across the limit")
	}
	// An array where a single value was expected refuses the shape.
	c.Apply(Change{NS: "shop.orders", Op: "insert", ID: strVal(t, "o9"), Doc: raw(t, order("o9", now, 1, bson.D{{Key: "sku", Value: bson.A{"x"}}, {Key: "qty", Value: int32(1)}})), At: bson.Timestamp{T: 6}})
	c.Apply(Change{NS: "shop.orders", Op: "delete", ID: strVal(t, "o2"), At: bson.Timestamp{T: 7}})
	if _, hit := c.Get(q); hit {
		t.Fatal("answered after an array group key arrived")
	}
}

func TestExactSumTypes(t *testing.T) {
	v := func(x any) bson.RawValue {
		typ, b, _ := bson.MarshalValue(x)
		return bson.RawValue{Type: typ, Value: b}
	}
	var s exactSum
	s.add(v(int32(2147483647)), 1)
	s.add(v(int32(1)), 1)
	if got := s.value(); got.Type != bson.TypeInt64 || got.Int64() != 2147483648 {
		t.Fatalf("ints past int32: %v", got)
	}
	s.add(v(int32(1)), -1)
	if got := s.value(); got.Type != bson.TypeInt32 {
		t.Fatalf("back within int32: %v", got)
	}
	// 0.1 added and taken away a thousand times leaves exactly nothing.
	for k := 0; k < 1000; k++ {
		s.add(v(0.1), 1)
	}
	for k := 0; k < 1000; k++ {
		s.add(v(0.1), -1)
	}
	if got := s.value(); got.Type != bson.TypeInt32 || got.Int32() != 2147483647 {
		t.Fatalf("after the doubles left: %v", got)
	}
}

func intStr(n int32) string  { return bson.RawValue{Type: bson.TypeInt32, Value: i32(n)}.String() }
func longStr(n int64) string { return bson.RawValue{Type: bson.TypeInt64, Value: i64(n)}.String() }
func dblStr(s string) string { return `{"$numberDouble":"` + s + `"}` }

func i32(n int32) []byte {
	_, b, _ := bson.MarshalValue(n)
	return b
}

func i64(n int64) []byte {
	_, b, _ := bson.MarshalValue(n)
	return b
}
