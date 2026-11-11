package livecache

import (
	"context"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The shapes a verify-mode sweep on 2026-11-11 found most often among those
// the cache declined (tc-nmg): a find with no limit, by _id or over a small
// collection, and a count of _id $in.

func allQuery(t *testing.T, filter bson.D, extra ...bson.E) Query {
	t.Helper()
	cmd := bson.D{{Key: "find", Value: "products"}, {Key: "filter", Value: filter}, {Key: "sort", Value: bson.D{{Key: "_id", Value: 1}}}}
	cmd = append(cmd, extra...)
	q, ok := ParseFind("shop.products", raw(t, cmd))
	if !ok {
		t.Fatalf("a find with no limit was not understood: %v", cmd)
	}
	if !q.All {
		t.Fatal("a find with no limit was not read as the whole match")
	}
	return q
}

func TestFindWithNoLimit(t *testing.T) {
	refused := []bson.D{
		// mongod's first batch would be cut short where this gives it all.
		{{Key: "find", Value: "products"}, {Key: "filter", Value: bson.D{}}, {Key: "batchSize", Value: int32(10)}},
		{{Key: "find", Value: "products"}, {Key: "filter", Value: bson.D{}}, {Key: "singleBatch", Value: true}},
		{{Key: "find", Value: "products"}, {Key: "filter", Value: bson.D{}}, {Key: "skip", Value: int32(maxWindow + 1)}},
	}
	for _, cmd := range refused {
		if _, ok := ParseFind("shop.products", raw(t, cmd)); ok {
			t.Errorf("answered here, but mongod's answer differs: %v", cmd)
		}
	}
	allQuery(t, bson.D{}, bson.E{Key: "batchSize", Value: int32(maxWindow)}, bson.E{Key: "singleBatch", Value: true})

	c := New(1 << 20)
	q := allQuery(t, bson.D{{Key: "category", Value: "a"}})
	docs := []bson.Raw{raw(t, product("p1", "a", 1)), raw(t, product("p2", "a", 2))}

	// A head of a longer list answers a limit, never the whole match.
	c.install(q, docs, false, 0)
	if _, hit := c.Get(q); hit {
		t.Fatal("the whole match answered from an entry that is only its head")
	}
	c.install(q, docs, true, 0)
	got, hit := c.Get(q)
	if !hit {
		t.Fatal("the whole match missed an entry that holds all of it")
	}
	if g := ids(t, got); len(g) != 2 {
		t.Fatalf("answer %v, want p1 p2", g)
	}
	applyAll(c, Change{NS: "shop.products", Op: "insert", ID: strVal(t, "p3"), Doc: raw(t, product("p3", "a", 3)), At: bson.Timestamp{T: 5}})
	got, hit = c.Get(q)
	if !hit {
		t.Fatal("an insert dropped a whole match it belongs in")
	}
	if g := ids(t, got); len(g) != 3 || g[2] != "p3" {
		t.Fatalf("after an insert the answer is %v, want p1 p2 p3", g)
	}

	// Grown past what an entry holds, it is a head again.
	at := uint32(10)
	for i := 0; len(c.entries[q.entryKey()].docs) <= maxWindow && i < 2*maxWindow; i++ {
		id := "q" + strings.Repeat("0", 4-len(itoa(i))) + itoa(i)
		applyAll(c, Change{NS: "shop.products", Op: "insert", ID: strVal(t, id), Doc: raw(t, product(id, "a", 1)), At: bson.Timestamp{T: at}})
		at++
	}
	for i := 0; c.entries[q.entryKey()] != nil && c.entries[q.entryKey()].complete && i < 2*fillWindow; i++ {
		id := "r" + itoa(1000+i)
		applyAll(c, Change{NS: "shop.products", Op: "insert", ID: strVal(t, id), Doc: raw(t, product(id, "a", 1)), At: bson.Timestamp{T: at}})
		at++
	}
	if _, hit := c.Get(q); hit {
		t.Fatal("the whole match answered from an entry that gave up its tail")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for ; i > 0; i /= 10 {
		s = string(rune('0'+i%10)) + s
	}
	return s
}

// mongod ends a first batch at 16MB; a reply from here has no cursor for the
// rest, so an answer larger than that is mongod's to give.
func TestFirstBatchBoundedLikeMongods(t *testing.T) {
	c := New(1 << 30)
	q := allQuery(t, bson.D{{Key: "category", Value: "big"}})
	pad := strings.Repeat("x", 6<<20)
	var docs []bson.Raw
	for _, id := range []string{"b1", "b2", "b3"} {
		docs = append(docs, raw(t, bson.D{{Key: "_id", Value: id}, {Key: "category", Value: "big"}, {Key: "pad", Value: pad}}))
	}
	c.install(q, docs, true, 0)
	if _, hit := c.Get(q); hit {
		t.Fatal("an 18MB first batch was answered")
	}
	c.install(q, docs[:2], true, 0)
	if _, hit := c.Get(q); !hit {
		t.Fatal("a 12MB first batch missed")
	}
}

func inCount(t *testing.T, vals ...any) Query {
	t.Helper()
	q, ok := ParseCountAgg("shop.products", raw(t, bson.D{
		{Key: "aggregate", Value: "products"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A(vals)}}}}}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}},
		}},
		{Key: "cursor", Value: bson.D{}},
	}))
	if !ok {
		t.Fatalf("a count of _id $in %v was not understood", vals)
	}
	return q
}

func TestIDInAsMongoJudgesIt(t *testing.T) {
	// One entry for one set, whatever the order, the repeats or the number
	// types it is written with.
	a := inCount(t, int32(2), int32(1), 1.0, int64(3))
	b := inCount(t, int64(3), 2.0, int32(1))
	if a.entryKey() != b.entryKey() {
		t.Fatal("the same set of _ids is two entries")
	}
	if a.entryKey() == inCount(t, int32(1), int32(2)).entryKey() {
		t.Fatal("two sets of _ids are one entry")
	}
	for _, f := range []bson.D{
		// Elsewhere $in matches an array holding the value.
		{{Key: "tags", Value: bson.D{{Key: "$in", Value: bson.A{"a"}}}}},
		// null matches a missing field, a regex by pattern.
		{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{nil}}}}},
		{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{bson.Regex{Pattern: "^p"}}}}}},
		{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{}}}}},
		{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{"p1"}}, {Key: "$gt", Value: "a"}}}},
		{{Key: "_id", Value: bson.D{{Key: "$in", Value: "p1"}}}},
	} {
		if _, ok := parseFilter(raw(t, f)); ok {
			t.Errorf("filter %v taken, mongod judges it in ways this does not", f)
		}
	}

	c := New(1 << 20)
	q := inCount(t, "p1", "p2", "p3")
	c.install(q, nil, true, 1)
	applyAll(c, Change{NS: "shop.products", Op: "insert", ID: strVal(t, "p9"), Doc: raw(t, product("p9", "a", 1)), At: bson.Timestamp{T: 3}})
	applyAll(c, Change{NS: "shop.products", Op: "insert", ID: strVal(t, "p2"), Doc: raw(t, product("p2", "a", 1)), At: bson.Timestamp{T: 4}})
	got, hit := c.Get(q)
	if !hit {
		t.Fatal("miss")
	}
	if n := got.Lookup("cursor", "firstBatch", "0", "n").Int32(); n != 2 {
		t.Fatalf("count %d, want 2: p9 is not in the set and p2 is", n)
	}
	applyAll(c, Change{NS: "shop.products", Op: "delete", ID: strVal(t, "p2"), At: bson.Timestamp{T: 5}})
	if _, hit := c.Get(q); hit {
		t.Fatal("a count survived a delete")
	}

	// A find by _id $in keeps only its own documents. Sorted, since where a
	// new member goes in an unsorted list is mongod's plan's to say.
	fq, ok := ParseFind("shop.products", raw(t, bson.D{
		{Key: "find", Value: "products"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{"p1", "p4"}}}}}},
		{Key: "sort", Value: bson.D{{Key: "_id", Value: 1}}},
	}))
	if !ok || !fq.All {
		t.Fatal("a find by _id $in with no limit was not understood")
	}
	c.install(fq, []bson.Raw{raw(t, product("p1", "a", 1))}, true, 0)
	applyAll(c, Change{NS: "shop.products", Op: "insert", ID: strVal(t, "p5"), Doc: raw(t, product("p5", "a", 1)), At: bson.Timestamp{T: 6}})
	applyAll(c, Change{NS: "shop.products", Op: "insert", ID: strVal(t, "p4"), Doc: raw(t, product("p4", "a", 1)), At: bson.Timestamp{T: 7}})
	got, hit = c.Get(fq)
	if !hit {
		t.Fatal("miss on a find by _id $in")
	}
	if g := ids(t, got); len(g) != 2 || g[0] != "p1" || g[1] != "p4" {
		t.Fatalf("answer %v, want p1 p4", g)
	}
	// A fill that returned a document outside the set is not this filter's
	// answer, and the shape is not trusted.
	c.install(fq, []bson.Raw{raw(t, product("p7", "a", 1))}, true, 0)
	if _, hit := c.Get(fq); hit {
		t.Fatal("an entry holding a document outside its set answered")
	}
}

// A driver configured with serverApi sends these on every command, and was
// never answered here.
func TestStableAPIClient(t *testing.T) {
	api := func(extra ...bson.E) []bson.E {
		return append([]bson.E{{Key: "apiVersion", Value: "1"}}, extra...)
	}
	find := func(extra ...bson.E) bson.D {
		return append(bson.D{{Key: "find", Value: "products"}, {Key: "filter", Value: bson.D{{Key: "category", Value: "a"}}}, {Key: "limit", Value: int32(5)}}, extra...)
	}
	count := func(extra ...bson.E) bson.D {
		return append(bson.D{{Key: "count", Value: "products"}, {Key: "query", Value: bson.D{{Key: "category", Value: "a"}}}}, extra...)
	}
	for _, cmd := range []bson.D{
		find(api()...),
		find(api(bson.E{Key: "apiStrict", Value: true}, bson.E{Key: "apiDeprecationErrors", Value: true})...),
		find(api(bson.E{Key: "apiStrict", Value: false})...),
	} {
		q, ok := ParseFind("shop.products", raw(t, cmd))
		if !ok {
			t.Errorf("refused %v", cmd)
			continue
		}
		plain, _ := ParseFind("shop.products", raw(t, find()))
		if q.entryKey() != plain.entryKey() {
			t.Errorf("%v is its own entry, but its answer is the plain find's", cmd)
		}
	}
	if _, ok := ParseCount("shop.products", raw(t, count(api(bson.E{Key: "apiStrict", Value: true})...))); !ok {
		t.Error("a count under apiVersion 1 was refused")
	}
	if _, ok := ParseCountAgg("shop.products", raw(t, append(bson.D{
		{Key: "aggregate", Value: "products"},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "category", Value: "a"}}}}, bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}}}},
		{Key: "cursor", Value: bson.D{}},
	}, api()...))); !ok {
		t.Error("a counting aggregate under apiVersion 1 was refused")
	}
	if _, ok := ParseGroupAgg("shop.products", raw(t, append(bson.D{
		{Key: "aggregate", Value: "products"},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$category"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}}}},
		{Key: "cursor", Value: bson.D{}},
	}, api()...))); !ok {
		t.Error("a $group under apiVersion 1 was refused")
	}

	// mongod answers these with an error.
	for _, cmd := range []bson.D{
		find(bson.E{Key: "apiVersion", Value: "2"}),
		find(bson.E{Key: "apiVersion", Value: int32(1)}),
		find(api(bson.E{Key: "apiStrict", Value: "yes"})...),
		find(bson.E{Key: "apiStrict", Value: true}),
		find(bson.E{Key: "apiDeprecationErrors", Value: false}),
	} {
		if _, ok := ParseFind("shop.products", raw(t, cmd)); ok {
			t.Errorf("answered %v, which mongod refuses", cmd)
		}
	}
	if _, ok := ParseCount("shop.products", raw(t, count(bson.E{Key: "apiStrict", Value: true}))); ok {
		t.Error("answered a count with apiStrict and no apiVersion")
	}
}

// A count on a tiered namespace remembers what it counted. A copy-up inserts a
// document the archive half already counted, an eviction deletes one that
// still exists, and a number alone could tell neither from the real thing.
func TestTieredCountKeepsItsMembers(t *testing.T) {
	c := New(1 << 20)
	c.Tiers(func(string) bool { return true }, nil, nil)
	c.GroupArchive(func(context.Context, string, bson.Timestamp, bson.D, bson.D) ([]bson.Raw, bool) { return nil, false })
	q, ok := ParseCount("shop.products", raw(t, bson.D{{Key: "count", Value: "products"}, {Key: "query", Value: bson.D{{Key: "category", Value: "a"}}}}))
	if !ok {
		t.Fatal("count not understood")
	}
	count := func() (int32, bool) {
		got, hit := c.Get(q)
		if !hit {
			return 0, false
		}
		return got.Lookup("n").Int32(), true
	}
	// p1 is on the fast tier, p2 only in the archive; the fill holds both.
	c.install(q, []bson.Raw{raw(t, product("p1", "a", 1)), raw(t, product("p2", "a", 2))}, true, 0)
	if n, hit := count(); !hit || n != 2 {
		t.Fatalf("count %d hit %v, want 2", n, hit)
	}
	at := uint32(1)
	ch := func(op, id string, d bson.D) {
		x := Change{NS: "shop.products", Op: op, ID: strVal(t, id), At: bson.Timestamp{T: at}}
		if d != nil {
			x.Doc = raw(t, d)
		}
		at++
		applyAll(c, x)
	}
	ch("insert", "p2", product("p2", "a", 2))
	if n, _ := count(); n != 2 {
		t.Fatalf("a copy-up of a counted document made the count %d, want 2", n)
	}
	ch("insert", "p3", product("p3", "a", 3))
	ch("insert", "p4", product("p4", "b", 3))
	if n, _ := count(); n != 3 {
		t.Fatalf("after a new member and a stranger the count is %d, want 3", n)
	}
	ch("replace", "p3", product("p3", "b", 3))
	if n, hit := count(); !hit || n != 2 {
		t.Fatalf("after a member left by a replace the count is %d hit %v, want 2", n, hit)
	}
	ch("update", "p4", product("p4", "a", 3))
	if n, _ := count(); n != 3 {
		t.Fatalf("after a stranger joined by an update the count is %d, want 3", n)
	}
	// A delete of a document it never counted changes nothing; untiered, any
	// delete dropped every count of the namespace.
	ch("delete", "p9", nil)
	if n, hit := count(); !hit || n != 3 {
		t.Fatalf("a delete of a stranger: count %d hit %v, want 3", n, hit)
	}
	// A delete of a member may be an eviction of a document that still
	// exists, so the count is read again.
	ch("delete", "p1", nil)
	if _, hit := count(); hit {
		t.Fatal("answered after a counted document left the fast tier")
	}
}

// A fill that read more than an entry remembers is not asked for again for a
// while: it read that many documents, from each tier, to be refused.
func TestOversizeFillWaits(t *testing.T) {
	c := New(1 << 20)
	q := inCount(t, "p1")
	c.failed(q, "too many", true)
	c.requestFill(q, q.shapeID())
	if c.Filling(q) {
		t.Fatal("an oversized fill was asked for again at once")
	}
	c.failed(inCount(t, "p2"), "network", false)
	c.requestFill(inCount(t, "p2"), inCount(t, "p2").shapeID())
	if !c.Filling(inCount(t, "p2")) {
		t.Fatal("a fill that failed for another reason waits too")
	}
}
