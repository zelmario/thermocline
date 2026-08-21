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
	if err := SpendEvictionNotices(ctx, c, map[string]int{noticeID: 1}); err != nil {
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
	if err := SpendEvictionNotices(ctx, c, map[string]int{noticeID: 1}); err != nil {
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
