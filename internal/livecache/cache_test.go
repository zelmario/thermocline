package livecache

import (
	"fmt"
	"strings"
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

func ids(t *testing.T, reply bson.Raw) []string {
	t.Helper()
	arr, ok := reply.Lookup("cursor", "firstBatch").ArrayOK()
	if !ok {
		t.Fatalf("no batch in %v", reply)
	}
	vals, _ := bson.Raw(arr).Values()
	out := []string{}
	for _, v := range vals {
		out = append(out, v.Document().Lookup("_id").StringValue())
	}
	return out
}

func product(id, cat string, price float64) bson.D {
	return bson.D{{Key: "_id", Value: id}, {Key: "category", Value: cat}, {Key: "price", Value: price}, {Key: "stock", Value: 10}}
}

func listQuery(t *testing.T, cat string, limit int) Query {
	q, ok := ParseFind("shop.products", raw(t, bson.D{
		{Key: "find", Value: "products"},
		{Key: "filter", Value: bson.D{{Key: "category", Value: cat}}},
		{Key: "sort", Value: bson.D{{Key: "price", Value: 1}}},
		{Key: "limit", Value: int32(limit)},
		{Key: "projection", Value: bson.D{{Key: "stock", Value: 0}}},
	}))
	if !ok {
		t.Fatal("a category listing was not understood")
	}
	return q
}

// A price change moves a product within its listing, and a category change
// moves it to another, instead of the listings being thrown away.
func TestChangesAreAppliedInPlace(t *testing.T) {
	c := New(1 << 20)
	qa, qb := listQuery(t, "a", 3), listQuery(t, "b", 3)
	if _, hit := c.Get(qa); hit {
		t.Fatal("hit on an empty cache")
	}
	c.install(qa, []bson.Raw{raw(t, product("p1", "a", 1)), raw(t, product("p2", "a", 2)), raw(t, product("p3", "a", 3))}, true, 0)
	c.install(qb, []bson.Raw{raw(t, product("p9", "b", 5))}, true, 0)

	c.Apply(Change{NS: "shop.products", Op: "update", ID: bson.RawValue{Type: bson.TypeString, Value: raw(t, bson.D{{Key: "x", Value: "p1"}}).Lookup("x").Value},
		Doc: raw(t, product("p1", "a", 2.5)), Updated: []string{"price"}, At: bson.Timestamp{T: 10}})
	got, hit := c.Get(qa)
	if !hit {
		t.Fatal("the listing was dropped by a price change")
	}
	if g := ids(t, got); len(g) != 3 || g[0] != "p2" || g[1] != "p1" || g[2] != "p3" {
		t.Fatalf("after a price change the listing is %v, want p2 p1 p3", g)
	}
	if _, err := bson.Raw(got).LookupErr("cursor", "firstBatch", "0", "stock"); err == nil {
		t.Fatal("the projection did not take stock out")
	}

	c.Apply(Change{NS: "shop.products", Op: "update", ID: strVal(t, "p2"), Doc: raw(t, product("p2", "b", 1)),
		Updated: []string{"category"}, At: bson.Timestamp{T: 11}})
	ga, _ := c.Get(qa)
	gb, _ := c.Get(qb)
	if a, b := ids(t, ga), ids(t, gb); len(a) != 2 || a[0] != "p1" || len(b) != 2 || b[0] != "p2" {
		t.Fatalf("after moving p2 to b: a=%v b=%v", a, b)
	}

	c.Apply(Change{NS: "shop.products", Op: "delete", ID: strVal(t, "p3"), At: bson.Timestamp{T: 12}})
	ga, _ = c.Get(qa)
	if a := ids(t, ga); len(a) != 1 || a[0] != "p1" {
		t.Fatalf("after deleting p3: %v", a)
	}
}

func strVal(t *testing.T, s string) bson.RawValue {
	return raw(t, bson.D{{Key: "v", Value: s}}).Lookup("v")
}

// An entry that is the head of a longer list cannot take a document that sorts
// after its end, and cannot answer past what it holds.
func TestPartialEntryKnowsItsEdge(t *testing.T) {
	c := New(1 << 20)
	q := listQuery(t, "a", 2)
	c.install(q, []bson.Raw{raw(t, product("p1", "a", 1)), raw(t, product("p2", "a", 2))}, false, 0)
	c.Apply(Change{NS: "shop.products", Op: "insert", ID: strVal(t, "p5"), Doc: raw(t, product("p5", "a", 9)), At: bson.Timestamp{T: 5}})
	got, hit := c.Get(q)
	if !hit {
		t.Fatal("miss")
	}
	if g := ids(t, got); len(g) != 2 || g[1] != "p2" {
		t.Fatalf("got %v", g)
	}
	c.Apply(Change{NS: "shop.products", Op: "delete", ID: strVal(t, "p2"), At: bson.Timestamp{T: 6}})
	if _, hit := c.Get(q); hit {
		t.Fatal("answered two from an entry that holds one of an unknown longer list")
	}
}

// A collection this router just wrote to is answered by mongod until the
// stream has caught up with the write.
func TestReadYourWrites(t *testing.T) {
	c := New(1 << 20)
	q := listQuery(t, "a", 1)
	c.install(q, []bson.Raw{raw(t, product("p1", "a", 1))}, true, 0)
	c.NoteWrite("shop.products", bson.Timestamp{T: 20})
	if _, hit := c.Get(q); hit {
		t.Fatal("served before the stream reached the write")
	}
	c.Advance(bson.Timestamp{T: 20})
	if _, hit := c.Get(q); !hit {
		t.Fatal("not served once the stream reached the write")
	}
}

// With a bound, an acknowledged write may go unapplied for that long and no
// longer, measured from the oldest one the stream has not reached.
func TestBoundedStaleness(t *testing.T) {
	c := New(1 << 20)
	c.SetMaxStaleness(50 * time.Millisecond)
	t0 := time.Unix(1_000_000, 0)
	clock := t0
	c.now = func() time.Time { return clock }
	q := listQuery(t, "a", 1)
	c.install(q, []bson.Raw{raw(t, product("p1", "a", 1))}, true, 0)
	served := func() bool { _, hit := c.Get(q); return hit }

	c.NoteWrite("shop.products", bson.Timestamp{T: 10})
	clock = t0.Add(40 * time.Millisecond)
	c.NoteWrite("shop.products", bson.Timestamp{T: 20})
	if !served() {
		t.Fatal("not served with every unapplied write inside the bound")
	}
	// The first write is now 60ms old and the second 20ms: the first decides.
	clock = t0.Add(60 * time.Millisecond)
	if served() {
		t.Fatal("served with a write older than the bound still unapplied")
	}
	// The stream reaches the first, and the second is inside the bound.
	c.Advance(bson.Timestamp{T: 10})
	if !served() {
		t.Fatal("not served once only a write inside the bound was left")
	}
	clock = t0.Add(95 * time.Millisecond)
	if served() {
		t.Fatal("served with the second write past the bound")
	}
	c.Advance(bson.Timestamp{T: 20})
	if !served() {
		t.Fatal("not served once the stream reached every write")
	}
	// A whole database's write holds every namespace in it the same way.
	c.NoteWriteDB("shop", bson.Timestamp{T: 30})
	clock = t0.Add(200 * time.Millisecond)
	if served() {
		t.Fatal("served past a database write older than the bound")
	}
}

// Counts go up on an insert and are forgotten on a delete, which does not say
// what the document held.
func TestCounts(t *testing.T) {
	c := New(1 << 20)
	q, ok := ParseCountAgg("shop.products", raw(t, bson.D{
		{Key: "aggregate", Value: "products"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "category", Value: "a"}}}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}},
		}},
		{Key: "cursor", Value: bson.D{}},
	}))
	if !ok {
		t.Fatal("countDocuments was not understood")
	}
	c.install(q, nil, true, 4)
	c.Apply(Change{NS: "shop.products", Op: "insert", ID: strVal(t, "p7"), Doc: raw(t, product("p7", "a", 3)), At: bson.Timestamp{T: 3}})
	got, hit := c.Get(q)
	if !hit {
		t.Fatal("miss")
	}
	if n := got.Lookup("cursor", "firstBatch", "0", "n").Int32(); n != 5 {
		t.Fatalf("count %d, want 5", n)
	}
	c.Apply(Change{NS: "shop.products", Op: "update", ID: strVal(t, "p7"), Doc: raw(t, product("p7", "a", 4)), Updated: []string{"price"}, At: bson.Timestamp{T: 4}})
	if _, hit := c.Get(q); !hit {
		t.Fatal("a price change dropped a count over category")
	}
	c.Apply(Change{NS: "shop.products", Op: "delete", ID: strVal(t, "p7"), At: bson.Timestamp{T: 5}})
	if _, hit := c.Get(q); hit {
		t.Fatal("a count survived a delete whose document it cannot see")
	}
}

// Numbers are one key whatever their type, and arrays are not answered here.
func TestEqualityAsMongoJudgesIt(t *testing.T) {
	q1, _ := ParseFind("s.c", raw(t, bson.D{{Key: "find", Value: "c"}, {Key: "filter", Value: bson.D{{Key: "n", Value: int32(1)}}}, {Key: "limit", Value: int32(5)}}))
	q2, _ := ParseFind("s.c", raw(t, bson.D{{Key: "find", Value: "c"}, {Key: "filter", Value: bson.D{{Key: "n", Value: 1.0}}}, {Key: "limit", Value: int32(5)}}))
	if q1.entryKey() != q2.entryKey() {
		t.Fatal("{n: 1} and {n: 1.0} are different keys")
	}
	for _, f := range []bson.D{
		{{Key: "tags", Value: bson.A{"a"}}},
		{{Key: "n", Value: nil}},
		{{Key: "n", Value: bson.D{{Key: "$gt", Value: 1}}}},
		{{Key: "a.b", Value: 1}},
	} {
		if _, ok := ParseFind("s.c", raw(t, bson.D{{Key: "find", Value: "c"}, {Key: "filter", Value: f}, {Key: "limit", Value: int32(5)}})); ok {
			t.Errorf("filter %v was accepted", f)
		}
	}
	c := New(1 << 20)
	c.install(q1, []bson.Raw{raw(t, bson.D{{Key: "_id", Value: "x"}, {Key: "n", Value: bson.A{1, 2}}})}, true, 0)
	if _, hit := c.Get(q1); hit {
		t.Fatal("served an answer that matched through an array")
	}
}

func TestBatchOfReadsAReply(t *testing.T) {
	q := listQuery(t, "a", 5)
	r := findReply(q, []bson.Raw{raw(t, product("p1", "a", 1))})
	got, ok := batchOf(r).([]map[string]any)
	if !ok || len(got) != 1 || got[0]["_id"] != "p1" {
		t.Fatalf("batchOf read %v from %v", batchOf(r), r)
	}
}

// An update whose document was too large for the stream to carry arrives with
// no document at all, and one whose document was deleted before the lookup
// arrives with null. The first still exists, so its listings must go rather
// than lose it; the second is gone and is taken out.
func TestOversizedUpdateIsNotADelete(t *testing.T) {
	event := func(full any) bson.Raw {
		d := bson.D{
			{Key: "operationType", Value: "update"},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "products"}}},
			{Key: "documentKey", Value: bson.D{{Key: "_id", Value: "p1"}}},
			{Key: "clusterTime", Value: bson.Timestamp{T: 30}},
			{Key: "updated", Value: bson.A{"stock"}},
		}
		if full != nil {
			d = append(d, bson.E{Key: "fullDocument", Value: full})
		}
		return raw(t, d)
	}
	skip := func(string) bool { return false }
	fill := func(c *Cache) Query {
		q := listQuery(t, "a", 2)
		c.install(q, []bson.Raw{raw(t, product("p1", "a", 1)), raw(t, product("p2", "a", 2))}, true, 0)
		return q
	}

	c := New(1 << 20)
	q := fill(c)
	c.handle(event(nil), skip, newFillState())
	if _, hit := c.Get(q); hit {
		t.Fatal("a listing was still answered after an update too large to see, which may have changed it")
	}

	c = New(1 << 20)
	q = fill(c)
	c.handle(event(primitiveNull{}), skip, newFillState())
	got, hit := c.Get(q)
	if !hit {
		t.Fatal("a deleted document's listing was dropped instead of taking it out")
	}
	if ids := ids(t, got); len(ids) != 1 || ids[0] != "p2" {
		t.Fatalf("after its document was deleted the listing holds %v, want [p2]", ids)
	}
}

// primitiveNull marshals as BSON null.
type primitiveNull struct{}

func (primitiveNull) MarshalBSONValue() (byte, []byte, error) { return byte(bson.TypeNull), nil, nil }

// A transaction's commit holds every namespace until the stream passes it: its
// statements were noted before the commit, at times its changes do not carry.
func TestCommitHoldsEverything(t *testing.T) {
	c := New(1 << 20)
	q := listQuery(t, "a", 1)
	c.install(q, []bson.Raw{raw(t, product("p1", "a", 1))}, true, 0)
	c.NoteWriteAll(bson.Timestamp{T: 40})
	if _, hit := c.Get(q); hit {
		t.Fatal("served before the stream reached a commit")
	}
	c.Advance(bson.Timestamp{T: 40})
	if _, hit := c.Get(q); !hit {
		t.Fatal("not served once the stream reached the commit")
	}
}

// A fill read at P0 is brought up to date with the changes kept while it was
// out, and only those after P0: a change at or before P0 is already in what
// was read, and applying it again would count an insert twice.
func TestFillReplaysOnlyLaterChanges(t *testing.T) {
	c := New(1 << 20)
	q, ok := ParseCount("shop.products", raw(t, bson.D{
		{Key: "count", Value: "products"},
		{Key: "query", Value: bson.D{{Key: "category", Value: "a"}}},
	}))
	if !ok {
		t.Fatal("a count was not understood")
	}
	insert := func(id string, at uint32) Change {
		return Change{NS: "shop.products", Op: "insert", ID: strVal(t, id), Doc: raw(t, product(id, "a", 1)), At: bson.Timestamp{T: at}}
	}
	f := &inflight{q: q, key: q.entryKey(), p0: bson.Timestamp{T: 10},
		buf: []Change{insert("p9", 9), insert("p10", 10), insert("p11", 11)}}
	c.installAt(f, nil, true, 5)
	reply, hit := c.Get(q)
	if !hit {
		t.Fatal("the count was not installed")
	}
	if n, _ := reply.Lookup("n").AsInt64OK(); n != 6 {
		t.Fatalf("count %d after a read of 5 at P0 and one insert after it, want 6", n)
	}

	lq := listQuery(t, "a", 10)
	g := &inflight{q: lq, key: lq.entryKey(), p0: bson.Timestamp{T: 10}, buf: []Change{
		{NS: "shop.products", Op: "update", ID: strVal(t, "p1"), Doc: raw(t, product("p1", "a", 5)), At: bson.Timestamp{T: 11}},
		{NS: "shop.products", Op: "delete", ID: strVal(t, "p2"), At: bson.Timestamp{T: 12}},
		insert("p3", 13),
	}}
	c.installAt(g, []bson.Raw{raw(t, product("p1", "a", 1)), raw(t, product("p2", "a", 2))}, true, 0)
	got, hit := c.Get(lq)
	if !hit {
		t.Fatal("the listing was not installed")
	}
	if ids := ids(t, got); fmt.Sprint(ids) != "[p3 p1]" {
		t.Fatalf("listing %v after the kept changes, want [p3 p1]: p1 moved to price 5, p2 deleted, p3 added at 1", ids)
	}
}

// A write that names its document holds back only what it can change. See
// NoteWrites.
func TestWritesHoldOnlyWhatTheyChange(t *testing.T) {
	c := New(1 << 20)
	c.SetMaxStaleness(0)
	qa, qb := listQuery(t, "a", 2), listQuery(t, "b", 2)
	c.install(qa, []bson.Raw{raw(t, product("p1", "a", 1)), raw(t, product("p2", "a", 2))}, true, 0)
	c.install(qb, []bson.Raw{raw(t, product("p3", "b", 1))}, true, 0)
	c.Advance(bson.Timestamp{T: 10})
	served := func(q Query) bool { _, hit := c.Get(q); return hit }

	c.NoteWrites([]WriteNote{{NS: "shop.products", IDs: []bson.RawValue{strVal(t, "p1")}, Fields: []string{"stock"}}}, bson.Timestamp{T: 20})
	if served(qa) {
		t.Fatal("a listing holding the document changed by _id was served before the stream applied it")
	}
	if !served(qb) {
		t.Fatal("a listing not holding the document changed by _id was held back")
	}
	c.Advance(bson.Timestamp{T: 20})
	if !served(qa) {
		t.Fatal("held back after the stream applied the write")
	}

	c.NoteWrites([]WriteNote{{NS: "shop.products", Docs: []bson.Raw{raw(t, product("p4", "b", 3))}}}, bson.Timestamp{T: 30})
	if served(qb) {
		t.Fatal("the listing an inserted document belongs in was served before the stream applied it")
	}
	if !served(qa) {
		t.Fatal("a listing the inserted document does not belong in was held back")
	}
	c.Advance(bson.Timestamp{T: 30})

	// A change to a field a query sorts by can move the document into a
	// listing that does not hold it, so it holds the namespace.
	c.NoteWrites([]WriteNote{{NS: "shop.products", IDs: []bson.RawValue{strVal(t, "p3")}, Fields: []string{"price"}}}, bson.Timestamp{T: 40})
	if served(qa) {
		t.Fatal("a sort field change held only the listings holding the document")
	}
	c.Advance(bson.Timestamp{T: 40})

	// An entry filled after a classified write inherits it. A count is a
	// shape nothing had asked for when the insert was noted, so the insert
	// could not name the entry it belongs in, and the count was read at 45,
	// before the insert at 50.
	c.NoteWrites([]WriteNote{{NS: "shop.products", Docs: []bson.Raw{raw(t, product("p10", "d", 1))}}}, bson.Timestamp{T: 50})
	qc, ok := ParseCount("shop.products", raw(t, bson.D{
		{Key: "count", Value: "products"},
		{Key: "query", Value: bson.D{{Key: "category", Value: "d"}}},
	}))
	if !ok {
		t.Fatal("a count was not understood")
	}
	f := &inflight{q: qc, key: qc.entryKey(), p0: bson.Timestamp{T: 45}}
	c.installAt(f, nil, true, 0)
	if served(qc) {
		t.Fatal("a count read at 45 answered before the stream applied an insert at 50 that it counts")
	}
	c.Advance(bson.Timestamp{T: 50})
	if !served(qc) {
		t.Fatal("still held after the stream applied the write")
	}
}

// An entry keeps what its projection leaves, plus _id and the fields it
// filters and sorts by, and nothing else: the document applied from the
// stream is trimmed the same way as the one filled.
func TestEntriesKeepOnlyWhatTheyAnswer(t *testing.T) {
	c := New(1 << 20)
	q, ok := ParseFind("shop.products", raw(t, bson.D{
		{Key: "find", Value: "products"},
		{Key: "filter", Value: bson.D{{Key: "category", Value: "a"}}},
		{Key: "sort", Value: bson.D{{Key: "price", Value: 1}}},
		{Key: "limit", Value: int32(5)},
		{Key: "projection", Value: bson.D{{Key: "description", Value: 0}}},
	}))
	if !ok {
		t.Fatal("not understood")
	}
	big := strings.Repeat("x", 1000)
	withText := func(id string, price float64) bson.Raw {
		return raw(t, append(product(id, "a", price), bson.E{Key: "description", Value: big}))
	}
	c.install(q, []bson.Raw{withText("p1", 1), withText("p2", 2)}, true, 0)
	if s := c.Stats(); s.Bytes > 800 {
		t.Fatalf("two products held in %d bytes, which is their descriptions as well", s.Bytes)
	}
	c.Apply(Change{NS: "shop.products", Op: "insert", ID: strVal(t, "p0"), Doc: withText("p0", 0.5), At: bson.Timestamp{T: 1}})
	if s := c.Stats(); s.Bytes > 1100 {
		t.Fatalf("three products held in %d bytes after one came from the stream whole", s.Bytes)
	}
	got, hit := c.Get(q)
	if !hit {
		t.Fatal("not answered")
	}
	if ids := ids(t, got); fmt.Sprint(ids) != "[p0 p1 p2]" {
		t.Fatalf("answered %v", ids)
	}
	first, _ := got.Lookup("cursor", "firstBatch").Array().Values()
	d := first[0].Document()
	if _, err := d.LookupErr("description"); err == nil {
		t.Fatal("the projected away field came back")
	}
	if _, err := d.LookupErr("stock"); err != nil {
		t.Fatal("a field the projection keeps was lost")
	}
}
