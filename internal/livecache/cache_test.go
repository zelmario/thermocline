package livecache

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
	c.handle(event(nil), skip)
	if _, hit := c.Get(q); hit {
		t.Fatal("a listing was still answered after an update too large to see, which may have changed it")
	}

	c = New(1 << 20)
	q = fill(c)
	c.handle(event(primitiveNull{}), skip)
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
