package meta

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// rawValueFor marshals v and returns it as the _id value a change event carries.
func rawValueFor(t *testing.T, v any) bson.RawValue {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "_id", Value: v}})
	if err != nil {
		t.Fatalf("marshal %v: %v", v, err)
	}
	rv, err := bson.Raw(raw).LookupErr("_id")
	if err != nil {
		t.Fatalf("lookup _id: %v", err)
	}
	return rv
}

// MongoDB allows any BSON type as an _id, so DocID has to survive all of them
// and never collide across namespaces.
func TestDocIDIsStableAndDistinct(t *testing.T) {
	oid := bson.NewObjectID()

	values := []struct {
		name string
		val  any
	}{
		{"objectid", oid},
		{"string", "order-1"},
		{"int32", int32(1)},
		{"int64", int64(1)},
		{"compound", bson.D{{Key: "tenant", Value: "acme"}, {Key: "seq", Value: 7}}},
		{"binary", bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}},
	}

	seen := make(map[string]string)
	for _, v := range values {
		id := DocID("app.orders", rawValueFor(t, v.val))

		if again := DocID("app.orders", rawValueFor(t, v.val)); again != id {
			t.Errorf("%s: DocID is not stable: %q then %q", v.name, id, again)
		}
		if prev, dup := seen[id]; dup {
			t.Errorf("%s collides with %s, both produced %q", v.name, prev, id)
		}
		seen[id] = v.name

		// The same document id in another collection must be a different key,
		// otherwise one collection's eviction notice would silence another's.
		if other := DocID("app.invoices", rawValueFor(t, v.val)); other == id {
			t.Errorf("%s: DocID does not separate namespaces", v.name)
		}
	}
}

// int32(1) and int64(1) compare equal in MongoDB but are different BSON types.
// Encoding the type byte keeps them apart, which is what we want for a key.
func TestDocIDSeparatesNumericTypes(t *testing.T) {
	a := DocID("app.orders", rawValueFor(t, int32(1)))
	b := DocID("app.orders", rawValueFor(t, int64(1)))
	if a == b {
		t.Errorf("int32(1) and int64(1) produced the same DocID %q", a)
	}
}

func TestTombstoneSetsTheField(t *testing.T) {
	raw, err := bson.Marshal(Tombstone(bson.NewObjectID().Timestamp()))
	if err != nil {
		t.Fatalf("marshal tombstone: %v", err)
	}
	set, err := bson.Raw(raw).LookupErr("$set")
	if err != nil {
		t.Fatalf("tombstone has no $set: %v", err)
	}
	if _, err := set.Document().LookupErr(TombstoneField); err != nil {
		t.Errorf("tombstone $set does not contain %s: %v", TombstoneField, err)
	}
}

// One pair of tiers can carry two Thermocline deployments, and the second one's
// bookkeeping database is not named in the first one's config. Reporting it to
// an operator as application data tells them to tier somebody's checkpoints.
func TestIsBookkeeping(t *testing.T) {
	cases := []struct {
		name  string
		colls []string
		want  bool
	}{
		{"another deployment's bookkeeping", []string{"checkpoints", "watermarks", "positions", "residency"}, true},
		{"the key registry on the fast tier", []string{"keys"}, true},
		{"one bookkeeping collection alone", []string{"markers"}, true},
		{"an application database", []string{"orders", "customers"}, false},
		// The one that matters: a database carrying both is somebody's data
		// with an unlucky name in it, and staying quiet about it would hide the
		// collections an application cannot see.
		{"application data beside a lookalike", []string{"checkpoints", "orders"}, false},
		{"an empty database says nothing either way", nil, false},
		// MongoDB's own collections belong to the server, not to a deployment.
		// One of these appearing beside the bookkeeping used to turn the whole
		// database into application data, and the advice that followed was to
		// tier another deployment's checkpoints. Profiling is enough to put
		// system.profile there.
		{"bookkeeping that has been profiled", []string{"checkpoints", "residency", "system.profile"}, true},
		{"bookkeeping with a view in it", []string{"watermarks", "system.views"}, true},
		{"system collections alone are not bookkeeping", []string{"system.profile"}, false},
		// Still false, because the application collection is still there: the
		// system prefix must not become a way to smuggle one past.
		{"application data beside a system collection", []string{"checkpoints", "orders", "system.profile"}, false},
	}
	for _, c := range cases {
		if got := IsBookkeeping(c.colls); got != c.want {
			t.Errorf("%s: IsBookkeeping(%v) = %v, want %v", c.name, c.colls, got, c.want)
		}
	}
}

// The eviction notice rules cannot be checked without a database, because the
// counting lives in the update itself so that an evictor and a feeder racing
// cannot both win. Run `make dev` and set TC_TEST_COLD to exercise them.
//
//	TC_TEST_COLD='mongodb://127.0.0.1:28018/?replicaSet=rsCold' go test ./internal/meta
func testCold(t *testing.T) (*mongo.Client, func()) {
	t.Helper()
	uri := os.Getenv("TC_TEST_COLD")
	if uri == "" {
		t.Skip("set TC_TEST_COLD to a cold tier URI to run the eviction notice tests")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ctx := context.Background()
	clear := func() {
		_, _ = client.Database(DB).Collection(Evicting).DeleteMany(ctx,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^tctest\\."}}}})
		// The same prefix, in the collection the own-DDL tests write to. This
		// database is a real deployment's, shared with whatever else is running
		// against the cluster, so a test that leaves rows behind is litter in
		// somebody's bookkeeping.
		_, _ = client.Database(DB).Collection(OwnDDL).DeleteMany(ctx,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^tctest\\."}}}})
	}
	clear()
	return client, func() { clear(); _ = client.Disconnect(ctx) }
}

const noticeID = "tctest.notice|0700"

// One eviction, one delete for the feeder to explain, one announcement spent.
func TestEvictionNoticeIsSpentOnce(t *testing.T) {
	c, cleanup := testCold(t)
	defer cleanup()
	ctx := context.Background()

	if err := AnnounceEviction(ctx, c, []string{noticeID}); err != nil {
		t.Fatalf("announce: %v", err)
	}
	have, err := ConsumeEvictionNotices(ctx, c, []string{noticeID})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if have[noticeID] != 1 {
		t.Fatalf("announced once, the feeder sees %d", have[noticeID])
	}
	if err := SpendEvictionNotices(ctx, c, map[string][]bson.Timestamp{
		noticeID: {{T: 100, I: 1}},
	}); err != nil {
		t.Fatalf("spend: %v", err)
	}
	have, _ = ConsumeEvictionNotices(ctx, c, []string{noticeID})
	if have[noticeID] != 0 {
		t.Errorf("the announcement survived being spent, so the next delete of this "+
			"document would be read as an eviction: %d left", have[noticeID])
	}
}

// A document can be evicted, warmed back up and evicted again while the feeder
// is behind. Two deletes are coming and both are announced, which is why this
// is a count and not a flag.
func TestEvictionNoticeCountsTwoInFlight(t *testing.T) {
	c, cleanup := testCold(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := AnnounceEviction(ctx, c, []string{noticeID}); err != nil {
			t.Fatalf("announce: %v", err)
		}
	}
	have, _ := ConsumeEvictionNotices(ctx, c, []string{noticeID})
	if have[noticeID] != 2 {
		t.Fatalf("announced twice, the feeder sees %d", have[noticeID])
	}
	if err := SpendEvictionNotices(ctx, c, map[string][]bson.Timestamp{
		noticeID: {{T: 100, I: 1}},
	}); err != nil {
		t.Fatalf("spend: %v", err)
	}
	have, _ = ConsumeEvictionNotices(ctx, c, []string{noticeID})
	if have[noticeID] != 1 {
		t.Errorf("one of two spent leaves %d, so the second delete would be tombstoned",
			have[noticeID])
	}
}

// A deployment upgraded mid-flight has notices already written without the
// count. Reading one as zero would treat a real eviction as an application
// delete, which is the loss this whole mechanism exists to prevent.
func TestNoticeWithoutACountIsWorthOne(t *testing.T) {
	c, cleanup := testCold(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := c.Database(DB).Collection(Evicting).InsertOne(ctx,
		bson.D{{Key: "_id", Value: noticeID}, {Key: "at", Value: time.Now().UTC()}}); err != nil {
		t.Fatalf("insert an old-style notice: %v", err)
	}
	have, _ := ConsumeEvictionNotices(ctx, c, []string{noticeID})
	if have[noticeID] != 1 {
		t.Errorf("a notice written before the count existed is worth %d, want 1", have[noticeID])
	}
}

// Covers is what decides whether a create or a createIndexes on the fast tier
// reaches a client's change stream, and every branch of it is a way to get that
// wrong in one of two directions: show an application DDL it did not do, or
// hide DDL it did.
func TestOwnDDLCovers(t *testing.T) {
	ts := func(sec, i uint32) bson.Timestamp { return bson.Timestamp{T: sec, I: i} }
	closed := OwnDDLRecord{Spans: []OwnDDLSpan{{From: ts(100, 2), To: ts(100, 9)}}}
	open := OwnDDLRecord{Spans: []OwnDDLSpan{{From: ts(100, 2)}}}

	cases := []struct {
		name string
		rec  OwnDDLRecord
		pos  bson.Timestamp
		want bool
	}{
		{"inside a closed span", closed, ts(100, 5), true},
		{"on the lower bound", closed, ts(100, 2), true},
		{"on the upper bound", closed, ts(100, 9), true},
		{"before it", closed, ts(100, 1), false},
		{"after it", closed, ts(100, 10), false},
		// The ordinal is what separates two writes in the same second, so a
		// comparison that comes down to seconds alone reads them as one.
		{"a later second, after the span", closed, ts(101, 1), false},
		{"an earlier second", closed, ts(99, 99), false},
		// An unfinished span means either that the DDL is still running or that
		// whatever ran it went away, and the second must not last: a row saying
		// "still building" for ever would hide an application's own builds.
		{"inside an open span", open, ts(100, 7), true},
		{"an open span, later but within the bound", open, ts(100+OpenSpanSeconds, 1), true},
		{"an open span, past the bound", open, ts(100+OpenSpanSeconds+1, 1), false},
		{"an open span, before it", open, ts(99, 1), false},
		// No record at all, and a zero position, which is what an event carries
		// on a server too old to report one. Neither is evidence of anything.
		{"no spans", OwnDDLRecord{}, ts(100, 5), false},
		{"a zero position", closed, bson.Timestamp{}, false},
	}
	for _, c := range cases {
		if got := c.rec.Covers(c.pos); got != c.want {
			t.Errorf("%s: Covers((%d,%d)) = %v, want %v", c.name, c.pos.T, c.pos.I, got, c.want)
		}
	}
}

// Two brackets are open on one namespace whenever a first copy-up creates the
// collection and the index build that follows it runs in a goroutine, and on a
// fast tier nothing else is writing to they read the same position. Closing by
// position therefore closed both, so whichever finished second decided both,
// and the later event stopped being covered. Each bracket carries its own
// token now, and this is the case that says so.
func TestTwoBracketsFromOnePositionCloseSeparately(t *testing.T) {
	c, cleanup := testCold(t)
	defer cleanup()
	ctx := context.Background()
	const ns = "tctest.ddl"
	from := bson.Timestamp{T: 500, I: 1}

	a, err := AnnounceOwnDDL(ctx, c, ns, from)
	if err != nil {
		t.Fatalf("announce the first: %v", err)
	}
	b, err := AnnounceOwnDDL(ctx, c, ns, from)
	if err != nil {
		t.Fatalf("announce the second: %v", err)
	}
	if a == b {
		t.Fatalf("two brackets share a token, so one close would still take both: %q", a)
	}
	// The later one finishes first, which is the order that used to lose.
	if err := CloseOwnDDL(ctx, c, ns, b, bson.Timestamp{T: 500, I: 20}); err != nil {
		t.Fatalf("close the second: %v", err)
	}
	if err := CloseOwnDDL(ctx, c, ns, a, bson.Timestamp{T: 500, I: 4}); err != nil {
		t.Fatalf("close the first: %v", err)
	}
	rows, err := ReadOwnDDL(ctx, c, []string{ns})
	if err != nil {
		t.Fatalf("read them back: %v", err)
	}
	rec := rows[ns]
	if n := len(rec.Spans); n != 2 {
		t.Fatalf("kept %d spans, want 2", n)
	}
	// Both events are still covered: the one at (500,3) inside the first
	// bracket and the one at (500,15) inside the second.
	for _, pos := range []bson.Timestamp{{T: 500, I: 3}, {T: 500, I: 15}} {
		if !rec.Covers(pos) {
			t.Errorf("(%d,%d) is not covered, so that event reaches the client", pos.T, pos.I)
		}
	}
}
