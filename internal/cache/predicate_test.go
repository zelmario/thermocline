package cache

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func raw(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var (
	sep1 = bson.NewDateTimeFromTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	oct1 = bson.NewDateTimeFromTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	oct9 = bson.NewDateTimeFromTime(time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))
	sep9 = bson.NewDateTimeFromTime(time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC))
)

// The shop's monthly report: a closed month counted. What the whole change is
// for, so it is spelled out the way the shop sends it.
func monthlyReport(t *testing.T) bson.Raw {
	return raw(t, bson.D{
		{Key: "aggregate", Value: "orders"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "at", Value: bson.D{
				{Key: "$gte", Value: sep1}, {Key: "$lt", Value: oct1}}}}}},
			bson.D{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: nil},
				{Key: "orders", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
		}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "shop"},
		{Key: "lsid", Value: bson.D{{Key: "id", Value: "x"}}},
	})
}

func TestMonthlyReportKeptAcrossTodaysOrders(t *testing.T) {
	r := ReadsOf("aggregate", monthlyReport(t))
	if !r.Known || r.AllFields {
		t.Fatalf("report not understood: %+v", r)
	}
	today := Write{Kind: WriteInsert, Inserted: []bson.Raw{
		raw(t, bson.D{{Key: "_id", Value: 1}, {Key: "at", Value: oct9}, {Key: "status", Value: "new"}}),
	}}
	if r.MayChange(today) {
		t.Error("an order placed outside the month dropped the month's report")
	}
	inMonth := Write{Kind: WriteInsert, Inserted: []bson.Raw{
		raw(t, bson.D{{Key: "_id", Value: 2}, {Key: "at", Value: sep9}}),
	}}
	if !r.MayChange(inMonth) {
		t.Error("an order inside the month kept the month's report")
	}
	both := Write{Kind: WriteInsert, Inserted: append(today.Inserted, inMonth.Inserted...)}
	if !r.MayChange(both) {
		t.Error("a batch with one order inside the month kept the report")
	}
	status := Write{Kind: WriteUpdate, Fields: []string{"status"}}
	if r.MayChange(status) {
		t.Error("a status change dropped a report that never reads status")
	}
	moved := Write{Kind: WriteUpdate, Fields: []string{"at"}}
	if !r.MayChange(moved) {
		t.Error("a change to the date the report filters on kept the report")
	}
	if !r.MayChange(AnyWrite) {
		t.Error("a delete kept the report")
	}
}

func TestInsertMatching(t *testing.T) {
	doc := func(v any) bson.Raw { return raw(t, bson.D{{Key: "_id", Value: 9}, {Key: "a", Value: v}}) }
	cases := []struct {
		name   string
		filter bson.D
		doc    bson.Raw
		cannot bool
	}{
		{"equal number", bson.D{{Key: "a", Value: 5}}, doc(int32(5)), false},
		{"different number", bson.D{{Key: "a", Value: 5}}, doc(int32(6)), true},
		{"int against double, same value", bson.D{{Key: "a", Value: 5}}, doc(5.0), false},
		{"long against double, beyond 2^53", bson.D{{Key: "a", Value: int64(1<<53 + 1)}}, doc(float64(1 << 53)), true},
		{"range below", bson.D{{Key: "a", Value: bson.D{{Key: "$gte", Value: 10}}}}, doc(int32(3)), true},
		{"range inside", bson.D{{Key: "a", Value: bson.D{{Key: "$gte", Value: 1}, {Key: "$lt", Value: 10}}}}, doc(int32(3)), false},
		{"$lt boundary", bson.D{{Key: "a", Value: bson.D{{Key: "$lt", Value: 3}}}}, doc(int32(3)), true},
		{"$lte boundary", bson.D{{Key: "a", Value: bson.D{{Key: "$lte", Value: 3}}}}, doc(int32(3)), false},
		{"string is left to the collation", bson.D{{Key: "a", Value: "x"}}, doc("y"), false},
		{"array element may match", bson.D{{Key: "a", Value: 5}}, doc(bson.A{1, 2}), false},
		{"missing field", bson.D{{Key: "b", Value: 5}}, doc(int32(1)), false},
		{"null condition", bson.D{{Key: "a", Value: nil}}, doc(int32(1)), false},
		{"different types", bson.D{{Key: "a", Value: 5}}, doc(sep1), false},
		{"dotted path", bson.D{{Key: "a.b", Value: 5}}, doc(int32(1)), false},
		{"$in is not judged", bson.D{{Key: "a", Value: bson.D{{Key: "$in", Value: bson.A{1, 2}}}}}, doc(int32(3)), false},
		{"$ne is not judged", bson.D{{Key: "a", Value: bson.D{{Key: "$ne", Value: 3}}}}, doc(int32(3)), false},
		{"$and, one part fails", bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "a", Value: 1}}, bson.D{{Key: "a", Value: bson.D{{Key: "$gt", Value: 0}}}}}}}, doc(int32(2)), true},
		{"$or, one branch may match", bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: "a", Value: 1}}, bson.D{{Key: "b", Value: 2}}}}}, doc(int32(5)), false},
		{"$or, every branch fails", bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: "a", Value: 1}}, bson.D{{Key: "a", Value: 2}}}}}, doc(int32(5)), true},
		{"$expr is not judged", bson.D{{Key: "$expr", Value: bson.D{{Key: "$eq", Value: bson.A{"$a", 1}}}}}, doc(int32(5)), false},
		{"embedded document equality", bson.D{{Key: "a", Value: bson.D{{Key: "x", Value: 1}}}}, doc(int32(5)), false},
		{"date range", bson.D{{Key: "a", Value: bson.D{{Key: "$gte", Value: sep1}, {Key: "$lt", Value: oct1}}}}, doc(oct9), true},
		{"objectid", bson.D{{Key: "a", Value: bson.ObjectID{1}}}, doc(bson.ObjectID{2}), true},
	}
	for _, c := range cases {
		if got := cannotMatch(raw(t, c.filter), c.doc); got != c.cannot {
			t.Errorf("%s: cannotMatch=%v, want %v", c.name, got, c.cannot)
		}
	}
}

func TestReadsOf(t *testing.T) {
	filter := bson.D{{Key: "status", Value: "open"}}
	cases := []struct {
		name   string
		cmd    bson.D
		known  bool
		all    bool
		fields []string
	}{
		{"find returns whole documents", bson.D{{Key: "find", Value: "c"}, {Key: "filter", Value: filter}}, true, true, nil},
		{"find with an inclusion projection", bson.D{{Key: "find", Value: "c"}, {Key: "filter", Value: filter},
			{Key: "projection", Value: bson.D{{Key: "total", Value: 1}, {Key: "_id", Value: 0}}},
			{Key: "sort", Value: bson.D{{Key: "at", Value: -1}}}}, true, false, []string{"status", "at", "total"}},
		{"find with $slice returns whole documents", bson.D{{Key: "find", Value: "c"},
			{Key: "projection", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$slice", Value: 2}}}}}}, true, true, nil},
		{"find with an exclusion projection", bson.D{{Key: "find", Value: "c"},
			{Key: "projection", Value: bson.D{{Key: "secret", Value: 0}}}}, true, true, nil},
		{"find with min", bson.D{{Key: "find", Value: "c"}, {Key: "min", Value: bson.D{{Key: "a", Value: 1}}}}, false, false, nil},
		{"find with an option nobody listed", bson.D{{Key: "find", Value: "c"}, {Key: "showRecordId", Value: true}}, false, false, nil},
		{"count", bson.D{{Key: "count", Value: "c"}, {Key: "query", Value: filter}}, true, false, []string{"status"}},
		{"count with $where", bson.D{{Key: "count", Value: "c"}, {Key: "query", Value: bson.D{{Key: "$where", Value: "true"}}}}, true, true, nil},
		{"distinct", bson.D{{Key: "distinct", Value: "c"}, {Key: "key", Value: "items.0.sku"}, {Key: "query", Value: filter}}, true, false, []string{"status", "items"}},
		{"aggregate without a reshape", bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: filter}}}}}, true, true, nil},
		{"aggregate grouping", bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: filter}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$channel"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: "$total"}}}}}}}}},
			true, false, []string{"status", "channel", "total"}},
		{"$$ROOT reads everything", bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "d", Value: bson.D{{Key: "$push", Value: "$$ROOT"}}}}}}}}},
			true, true, nil},
		{"a $lookup after the reshape", bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$a"}}}},
			bson.D{{Key: "$lookup", Value: bson.D{{Key: "from", Value: "c"}}}}}}}, false, false, nil},
		{"$unwind is not understood", bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$unwind", Value: "$items"}}, bson.D{{Key: "$count", Value: "n"}}}}}, false, false, nil},
		{"an inclusion $project reshapes", bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$project", Value: bson.D{{Key: "total", Value: 1}, {Key: "twice", Value: bson.D{{Key: "$multiply", Value: bson.A{"$net", 2}}}}}}}}}},
			true, false, []string{"net", "total"}},
		{"an exclusion $project does not", bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$project", Value: bson.D{{Key: "secret", Value: 0}}}}, bson.D{{Key: "$limit", Value: 5}}}}}, true, true, nil},
		{"a command that is not a read", bson.D{{Key: "insert", Value: "c"}}, false, false, nil},
	}
	for _, c := range cases {
		name := c.cmd[0].Key
		r := ReadsOf(name, raw(t, c.cmd))
		if r.Known != c.known {
			t.Errorf("%s: known=%v, want %v", c.name, r.Known, c.known)
			continue
		}
		if !c.known {
			continue
		}
		if r.AllFields != c.all {
			t.Errorf("%s: all=%v, want %v (fields %v)", c.name, r.AllFields, c.all, r.Fields)
			continue
		}
		if c.all {
			continue
		}
		for _, want := range c.fields {
			found := false
			for _, f := range r.Fields {
				if f == want {
					found = true
				}
			}
			if !found {
				t.Errorf("%s: fields %v lack %q", c.name, r.Fields, want)
			}
		}
	}
}

func TestLeadingMatchesCombine(t *testing.T) {
	cmd := raw(t, bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "a", Value: bson.D{{Key: "$gte", Value: 10}}}}}},
		bson.D{{Key: "$match", Value: bson.D{{Key: "b", Value: 1}}}},
		bson.D{{Key: "$count", Value: "n"}},
	}}})
	r := ReadsOf("aggregate", cmd)
	insert := func(a, b int32) Write {
		return Write{Kind: WriteInsert, Inserted: []bson.Raw{raw(t, bson.D{{Key: "a", Value: a}, {Key: "b", Value: b}})}}
	}
	if r.MayChange(insert(1, 1)) {
		t.Error("failing the first $match dropped the answer")
	}
	if r.MayChange(insert(20, 2)) {
		t.Error("failing the second $match dropped the answer")
	}
	if !r.MayChange(insert(20, 1)) {
		t.Error("passing both kept the answer")
	}
	// A $match that is not leading filters documents an earlier stage made, so
	// it says nothing about which inserted documents can take part.
	late := raw(t, bson.D{{Key: "aggregate", Value: "c"}, {Key: "pipeline", Value: bson.A{
		bson.D{{Key: "$addFields", Value: bson.D{{Key: "a", Value: "$z"}}}},
		bson.D{{Key: "$match", Value: bson.D{{Key: "a", Value: 1}}}},
		bson.D{{Key: "$count", Value: "n"}},
	}}})
	if !ReadsOf("aggregate", late).MayChange(insert(5, 5)) {
		t.Error("a $match after $addFields was used to judge an insert")
	}
}

func TestUpdates(t *testing.T) {
	r := Reads{Known: true, Fields: []string{"status", "items"}}
	cases := []struct {
		name string
		u    bson.D
		may  bool
	}{
		{"unrelated $set", bson.D{{Key: "$set", Value: bson.D{{Key: "note", Value: 1}}}}, false},
		{"a field read", bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "x"}}}}, true},
		{"inside a field read", bson.D{{Key: "$inc", Value: bson.D{{Key: "items.0.qty", Value: 1}}}}, true},
		{"positional inside a field read", bson.D{{Key: "$set", Value: bson.D{{Key: "items.$[e].qty", Value: 1}}}}, true},
		{"a parent of a field read", bson.D{{Key: "$unset", Value: bson.D{{Key: "status", Value: ""}}}}, true},
		{"a sibling with a shared prefix", bson.D{{Key: "$set", Value: bson.D{{Key: "statusAt", Value: 1}}}}, false},
		{"$rename onto a field read", bson.D{{Key: "$rename", Value: bson.D{{Key: "old", Value: "status"}}}}, true},
		{"replacement", bson.D{{Key: "note", Value: 1}}, true},
		{"unknown operator", bson.D{{Key: "$frob", Value: bson.D{{Key: "note", Value: 1}}}}, true},
	}
	for _, c := range cases {
		w := WriteOfUpdate([]bson.Raw{raw(t, bson.D{{Key: "q", Value: bson.D{}}, {Key: "u", Value: c.u}})})
		if got := r.MayChange(w); got != c.may {
			t.Errorf("%s: MayChange=%v, want %v", c.name, got, c.may)
		}
	}
	up := WriteOfUpdate([]bson.Raw{raw(t, bson.D{{Key: "q", Value: bson.D{}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "note", Value: 1}}}}}, {Key: "upsert", Value: true}})})
	if !r.MayChange(up) {
		t.Error("an upsert, which may insert, kept the answer")
	}
	pipe := WriteOfUpdate([]bson.Raw{raw(t, bson.D{{Key: "q", Value: bson.D{}},
		{Key: "u", Value: bson.A{bson.D{{Key: "$set", Value: bson.D{{Key: "note", Value: 1}}}}}}})})
	if !r.MayChange(pipe) {
		t.Error("a pipeline update kept the answer")
	}
	if !(Reads{Known: true, AllFields: true}).MayChange(Write{Kind: WriteUpdate, Fields: []string{"note"}}) {
		t.Error("an answer of whole documents kept across an update")
	}
}

// A change event names the element it replaced, and the answer read a field
// inside the elements. Found writing eventWrite, before anything ran it.
func TestElementPathsOverlapTheirFields(t *testing.T) {
	r := Reads{Known: true, Fields: []string{"items.qty"}}
	for _, p := range []string{"items.0", "items.3.qty", "items"} {
		if !r.MayChange(Write{Kind: WriteUpdate, Fields: []string{p}}) {
			t.Errorf("a change to %s kept an answer reading items.qty", p)
		}
	}
	if r.MayChange(Write{Kind: WriteUpdate, Fields: []string{"itemsCount"}}) {
		t.Error("a field sharing only a prefix of characters counted as the same field")
	}
}

// One write judged against one held answer, the cost every write pays per
// answer held on its collection, under the cache's lock.
func BenchmarkMayChangeInsert(b *testing.B) {
	t := &testing.T{}
	r := ReadsOf("aggregate", monthlyReport(t))
	w := Write{Kind: WriteInsert, Inserted: []bson.Raw{
		raw(t, bson.D{{Key: "_id", Value: 1}, {Key: "at", Value: oct9}, {Key: "status", Value: "new"},
			{Key: "customer", Value: "c000123"}, {Key: "total", Value: 41.5}}),
	}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if r.MayChange(w) {
			b.Fatal("changed")
		}
	}
}

func BenchmarkMayChangeUpdate(b *testing.B) {
	r := Reads{Known: true, Fields: []string{"at", "customer", "status"}}
	w := Write{Kind: WriteUpdate, Fields: []string{"note", "items.0.qty"}}.Prepared()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.MayChange(w)
	}
}

func BenchmarkReadsOfReport(b *testing.B) {
	cmd := monthlyReport(&testing.T{})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ReadsOf("aggregate", cmd)
	}
}

func TestFieldPrefix(t *testing.T) {
	for in, want := range map[string]string{
		"a":            "a",
		"a.b.c":        "a.b.c",
		"items.0.qty":  "items",
		"items.$.qty":  "items",
		"items.$[e].q": "items",
		"0":            "",
		"$[]":          "",
		"a.10":         "a",
		"a.b0":         "a.b0",
	} {
		if got := fieldPrefix(in); got != want {
			t.Errorf("fieldPrefix(%q) = %q, want %q", in, got, want)
		}
	}
	// An empty prefix is a change to the whole document.
	if !(Reads{Known: true, Fields: []string{"x"}}).MayChange(Write{Kind: WriteUpdate, Fields: []string{"$[]"}}) {
		t.Error("a change at the root kept an answer")
	}
}
