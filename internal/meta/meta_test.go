package meta

import (
	"context"
	"fmt"
	"os"
	"reflect"
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

	if err := AnnounceEviction(ctx, c, []string{noticeID}, "test:0"); err != nil {
		t.Fatalf("announce: %v", err)
	}
	have, err := ReadEvictionNotices(ctx, c, []string{noticeID}, nil)
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
	have, _ = ReadEvictionNotices(ctx, c, []string{noticeID}, nil)
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
		if err := AnnounceEviction(ctx, c, []string{noticeID}, fmt.Sprintf("test:%d", i)); err != nil {
			t.Fatalf("announce: %v", err)
		}
	}
	have, _ := ReadEvictionNotices(ctx, c, []string{noticeID}, nil)
	if have[noticeID] != 2 {
		t.Fatalf("announced twice, the feeder sees %d", have[noticeID])
	}
	if err := SpendEvictionNotices(ctx, c, map[string][]bson.Timestamp{
		noticeID: {{T: 100, I: 1}},
	}); err != nil {
		t.Fatalf("spend: %v", err)
	}
	have, _ = ReadEvictionNotices(ctx, c, []string{noticeID}, nil)
	if have[noticeID] != 1 {
		t.Errorf("one of two spent leaves %d, so the second delete would be tombstoned",
			have[noticeID])
	}
}

// A spend is retried when its reply is lost, and the write may already have
// landed. The feeder keeps the positions it could not confirm as owed and sends
// them again at the next checkpoint, so the same spend arriving twice has to
// take the count down once. Otherwise the second takes the announcement of a
// later eviction of the same document, and that eviction is tombstoned.
func TestEvictionNoticeSpentTwiceForOneDelete(t *testing.T) {
	c, cleanup := testCold(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := AnnounceEviction(ctx, c, []string{noticeID}, fmt.Sprintf("test:%d", i)); err != nil {
			t.Fatalf("announce: %v", err)
		}
	}
	once := map[string][]bson.Timestamp{noticeID: {{T: 100, I: 1}}}
	for i := 0; i < 2; i++ {
		if err := SpendEvictionNotices(ctx, c, once); err != nil {
			t.Fatalf("spend: %v", err)
		}
	}
	have, _ := ReadEvictionNotices(ctx, c, []string{noticeID}, nil)
	if have[noticeID] != 1 {
		t.Errorf("one delete spent twice leaves %d of two announcements, want 1", have[noticeID])
	}
}

// And the reader's half. Positions the feeder still owes are subtracted from
// what the archive says is left, because a spend that has not landed has not
// taken the count down yet. One that landed and lost its reply has, and
// subtracting it again reads the next eviction of that document as the
// application deleting it.
func TestOwedPositionAlreadySpentIsNotSubtractedAgain(t *testing.T) {
	c, cleanup := testCold(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := AnnounceEviction(ctx, c, []string{noticeID}, fmt.Sprintf("test:%d", i)); err != nil {
			t.Fatalf("announce: %v", err)
		}
	}
	owed := map[string][]bson.Timestamp{noticeID: {{T: 100, I: 1}}}
	have, _ := ReadEvictionNotices(ctx, c, []string{noticeID}, owed)
	if have[noticeID] != 1 {
		t.Fatalf("two announced and one owed, the feeder sees %d, want 1", have[noticeID])
	}
	if err := SpendEvictionNotices(ctx, c, owed); err != nil {
		t.Fatalf("spend: %v", err)
	}
	have, _ = ReadEvictionNotices(ctx, c, []string{noticeID}, owed)
	if have[noticeID] != 1 {
		t.Errorf("the owed spend landed and the feeder sees %d left, want 1", have[noticeID])
	}
}

// The announcement is several writes in parallel under a deadline, and an
// archive that stalls past it takes some of them. Sent again, the ones that
// landed must not count twice: a spare reads the application's next delete of
// that document as an eviction, and the archive keeps what it deleted.
func TestOneActAnnouncesOnce(t *testing.T) {
	c, cleanup := testCold(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := AnnounceEviction(ctx, c, []string{noticeID}, "step:a"); err != nil {
			t.Fatalf("announce: %v", err)
		}
	}
	have, _ := ReadEvictionNotices(ctx, c, []string{noticeID}, nil)
	if have[noticeID] != 1 {
		t.Errorf("one act announced twice counts %d, want 1", have[noticeID])
	}
}

// And taking it back takes only what the act raised. A step whose announce
// failed does not know which notices landed, and withdrawing blind took the
// announcement of another eviction still waiting for its delete, which the
// feeder then read as the application's and tombstoned.
func TestAWithdrawalTakesOnlyItsOwn(t *testing.T) {
	c, cleanup := testCold(t)
	defer cleanup()
	ctx := context.Background()

	if err := AnnounceEviction(ctx, c, []string{noticeID}, "step:earlier"); err != nil {
		t.Fatalf("announce: %v", err)
	}
	// The failed step's announce never reached this document.
	if err := WithdrawEviction(ctx, c, []string{noticeID}, "step:failed"); err != nil {
		t.Fatalf("withdraw: %v", err)
	}
	have, _ := ReadEvictionNotices(ctx, c, []string{noticeID}, nil)
	if have[noticeID] != 1 {
		t.Fatalf("a withdrawal by a step that announced nothing here left %d, want 1", have[noticeID])
	}
	if err := AnnounceEviction(ctx, c, []string{noticeID}, "step:failed"); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if err := WithdrawEviction(ctx, c, []string{noticeID}, "step:failed"); err != nil {
		t.Fatalf("withdraw: %v", err)
	}
	have, _ = ReadEvictionNotices(ctx, c, []string{noticeID}, nil)
	if have[noticeID] != 1 {
		t.Errorf("withdrawing its own left %d, want the earlier eviction's 1", have[noticeID])
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
	have, _ := ReadEvictionNotices(ctx, c, []string{noticeID}, nil)
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

// Which clock stamps each field of a residency row, which is the question that
// decides whether a hold means what it says.
//
// Every field here is written on one machine and judged on another, and they do
// not all get judged by the same one. hotUntil is read by the evictor, expiresAt
// by the archive's own TTL monitor, and promotedAt is weighed against the
// wall-clock time of a change stream event from the fast tier's primary. So the
// first two are stamped from the archive's clock, which is the one thing both
// of their readers share with the writer, and the third stays on the writer's
// because the window it is judged in is generous enough that skew can only
// widen it.
//
// The failure this prevents is quiet: a promoter whose clock runs behind writes
// a hold that is already shorter than configured, the next eviction sweep
// removes a document that was fetched a moment ago, and the read after that
// fetches it again. Nothing in the logs says the clocks are the reason.
func TestResidencyIsStampedByTheArchive(t *testing.T) {
	promotedAt := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	pipe := ResidencyUpdate("shop.orders", bson.RawValue{}, promotedAt, 5*time.Minute)

	set := setStage(t, pipe)
	// The two the archive judges, as expressions over its own clock.
	wantHot := bson.D{{Key: "$add", Value: bson.A{"$$NOW", int64(300000)}}}
	if got := field(set, "hotUntil"); !reflect.DeepEqual(got, wantHot) {
		t.Errorf("hotUntil is %#v, want it counted from the archive's clock: %#v", got, wantHot)
	}
	// The hold plus NoticeTTL, because the row has to outlive the hold by long
	// enough for a late change stream reader to recognise the copy-up.
	wantExp := bson.D{{Key: "$add", Value: bson.A{"$$NOW", (5*time.Minute + NoticeTTL).Milliseconds()}}}
	if got := field(set, "expiresAt"); !reflect.DeepEqual(got, wantExp) {
		t.Errorf("expiresAt is %#v, want %#v", got, wantExp)
	}
	// And the one that stays on the writer's clock, which is the "then" arm of
	// the $cond that decides whether this is a new promotion.
	cond, ok := field(set, "promotedAt").(bson.D)
	if !ok {
		t.Fatalf("promotedAt is %#v, want the $cond that reopens a record", field(set, "promotedAt"))
	}
	arms, ok := field(cond, "$cond").(bson.A)
	if !ok || len(arms) != 3 {
		t.Fatalf("promotedAt's $cond is %#v", field(cond, "$cond"))
	}
	if got, ok := arms[1].(time.Time); !ok || !got.Equal(promotedAt) {
		t.Errorf("promotedAt is %#v, want the writer's own instant %v", arms[1], promotedAt)
	}
}

// Ending a hold is the same question from the other side: "in the past" has to
// mean in the past to whoever is asking, and that is the evictor reading it
// against the archive's clock. endedAt keeps the caller's, because what it is
// weighed against is a change stream event's own time.
func TestEndingAHoldUsesTheArchivesClock(t *testing.T) {
	endedAt := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	set := setStage(t, HoldIsOver(endedAt))

	if got := field(set, "hotUntil"); got != "$$NOW" {
		t.Errorf("hotUntil is %#v, want the archive's own clock", got)
	}
	if got, ok := field(set, "endedAt").(time.Time); !ok || !got.Equal(endedAt) {
		t.Errorf("endedAt is %#v, want the instant the caller passed", field(set, "endedAt"))
	}
}

// setStage returns the $set document of a one-stage pipeline.
func setStage(t *testing.T, pipe mongo.Pipeline) bson.D {
	t.Helper()
	if len(pipe) != 1 {
		t.Fatalf("pipeline has %d stages, want 1", len(pipe))
	}
	for _, e := range pipe[0] {
		if e.Key == "$set" {
			d, ok := e.Value.(bson.D)
			if !ok {
				t.Fatalf("$set is %T, want bson.D", e.Value)
			}
			return d
		}
	}
	t.Fatalf("no $set stage in %v", pipe[0])
	return nil
}

// stageSetting returns the $set stage of pipe that sets name.
func stageSetting(t *testing.T, pipe mongo.Pipeline, name string) bson.D {
	t.Helper()
	for _, stage := range pipe {
		for _, e := range stage {
			if d, ok := e.Value.(bson.D); ok && e.Key == "$set" && field(d, name) != nil {
				return d
			}
		}
	}
	t.Fatalf("no $set stage in %v sets %s", pipe, name)
	return nil
}

func field(d bson.D, name string) interface{} {
	for _, e := range d {
		if e.Key == name {
			return e.Value
		}
	}
	return nil
}

// Every instant one daemon writes here and another judges is stamped by the
// archive, and this is the list of them.
//
// The reason is the same each time and the consequences are not. A daemon
// writes a row; something else decides what it means by subtracting it from a
// clock that is not the writer's, and the daemons are not required to share a
// machine. Three of these are read by the archive's own TTL monitor, which
// nothing can correct for afterwards: a notice stamped by a clock an hour
// behind is born expired and gone within the minute, and an eviction notice
// that is gone is an eviction the feeder reads as the application's own delete.
// The archive tombstones its copy and the document is lost from both tiers.
//
// $currentDate is the whole fix. It cannot drift, there is no reading to take
// and no correction to apply, and it works on every version this supports.
func TestInstantsOthersJudgeComeFromTheArchive(t *testing.T) {
	// The announcement became a pipeline when the counter had to be floored
	// rather than incremented, so it stamps its instant with $$NOW in a $set
	// instead of with $currentDate. Both are the archive's own clock, which is
	// the property this test is about, so it accepts either shape rather than
	// the spelling one of them happened to use.
	t.Run("an eviction announcement", func(t *testing.T) {
		set := stageSetting(t, EvictionAnnouncement("test:0"), "pending")
		if got := field(set, "at"); got != "$$NOW" {
			t.Fatalf("at is %#v, want $$NOW: it is expired by the archive's TTL monitor, "+
				"and an early expiry loses documents from both tiers", got)
		}
	})

	cases := []struct {
		what   string
		update bson.D
		field  string
		why    string
	}{
		{
			what:   "this system's own DDL",
			update: OwnDDLAnnouncement(OwnDDLSpan{Tok: "t"}),
			field:  "at",
			why:    "expired by the archive's TTL monitor; an early expiry shows a client DDL it never issued",
		},
		{
			what:   "a daemon heartbeat",
			update: HeartbeatUpdate(),
			field:  "seen",
			why:    "the feeder suppresses copies only while it believes the other daemon is running",
		},
		{
			what:   "a daemon's report",
			update: DaemonReportUpdate(false, nil, nil),
			field:  "at",
			why:    "the router exports seconds_since_pass from it, and an alert rule watches that",
		},
	}

	for _, c := range cases {
		t.Run(c.what, func(t *testing.T) {
			var stamped bool
			for _, e := range c.update {
				if e.Key != "$currentDate" {
					continue
				}
				d, ok := e.Value.(bson.D)
				if !ok {
					t.Fatalf("$currentDate is %T, want bson.D", e.Value)
				}
				for _, f := range d {
					if f.Key == c.field {
						if f.Value != true {
							t.Fatalf("$currentDate on %q is %#v, want true", c.field, f.Value)
						}
						stamped = true
					}
				}
			}
			if !stamped {
				t.Errorf("%q is not stamped by the archive, and it is %s: %v", c.field, c.why, c.update)
			}
		})
	}

	// The hold on eviction is the odd one: it has to be written once and kept,
	// so it cannot use $currentDate, which always writes. $ifNull over $$NOW
	// says the same thing $setOnInsert did, in the archive's frame.
	t.Run("an operator's hold on eviction", func(t *testing.T) {
		set := setStage(t, PauseUpdate("restoring the archive"))
		since, ok := field(set, "since").(bson.D)
		if !ok {
			t.Fatalf("since is %#v, want an expression over the archive's clock", field(set, "since"))
		}
		arms, ok := field(since, "$ifNull").(bson.A)
		if !ok || len(arms) != 2 {
			t.Fatalf("since is %#v, want $ifNull with two arms", since)
		}
		if arms[0] != "$since" || arms[1] != "$$NOW" {
			t.Errorf("since is %v, want the existing value or the archive's clock", arms)
		}
	})
}

// The announcement counter may never be read down below zero, and an
// announcement has to lift a damaged row back to a state a notice is seen in.
//
// This is the arithmetic behind a measured loss. Two feeders watching one
// namespace each spend the same notice, which took pending to -1 under $inc.
// Nothing was lost at that moment. The next eviction of those documents
// announced them from -1 to 0, a notice counts only above zero, and the feeder
// read a real eviction as the application deleting the document: 40 of 40
// tombstoned in the archive and already gone from the fast tier, with the
// misconfiguration long since corrected. One feeder throughout lost none.
//
// So the spend floors at zero and the announcement lifts to at least one. The
// second is what heals a deployment whose counters are already negative, which
// flooring the spend cannot do on its own.
func TestTheEvictionCounterCannotStrandADocument(t *testing.T) {
	set := stageSetting(t, EvictionAnnouncement("test:0"), "pending")
	// The raise is the branch taken by an act not already counted; the other
	// leaves the count alone. See TestOneActAnnouncesOnce.
	cond, ok := field(set, "pending").(bson.D)
	if !ok {
		t.Fatalf("pending is %#v, want an expression", field(set, "pending"))
	}
	branches, ok := field(cond, "$cond").(bson.A)
	if !ok || len(branches) != 3 {
		t.Fatalf("pending is %#v, want $cond on whether this act is new", cond)
	}
	pending, ok := branches[1].(bson.D)
	if !ok {
		t.Fatalf("a new act sets pending to %#v, want an expression", branches[1])
	}
	arms, ok := field(pending, "$max").(bson.A)
	if !ok || len(arms) != 2 {
		t.Fatalf("an announcement sets pending with %#v, want $max over two arms: "+
			"+1 alone leaves a row that two feeders drove negative below the threshold "+
			"a notice is seen at, and the next eviction of that document is read as a delete",
			pending)
	}
	if arms[0] != 1 {
		t.Fatalf("the floor of an announcement is %#v, want 1: an announcement means at least "+
			"one eviction is pending", arms[0])
	}
}

// A copy-up that asked the fast tier opens a promotion whatever the record says.
//
// The record here is the one the two-router shop left behind: a promotion two
// minutes old with its echo spent, and a hold that a hold-only refresh kept
// running after the evictor had taken the document. ResidencyUpdate reads the
// running hold as a promotion still in progress and opens nothing, so the next
// copy-up's insert had no echo and reached both applications as a new order.
func TestAPromotionThatAskedAlwaysOpens(t *testing.T) {
	c, done := testCold(t)
	defer done()
	ctx := context.Background()
	coll := c.Database(DB).Collection(Residency)
	const id = "tctest.promo|0700"
	t.Cleanup(func() { _, _ = coll.DeleteOne(context.Background(), bson.D{{Key: "_id", Value: id}}) })

	old := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Millisecond)
	seed := func() {
		_, _ = coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
		if _, err := coll.InsertOne(ctx, bson.D{
			{Key: "_id", Value: id}, {Key: "promotedAt", Value: old},
			{Key: "hotUntil", Value: time.Now().UTC().Add(time.Minute)},
			{Key: "echoed", Value: bson.A{bson.Timestamp{T: 1000, I: 1}}},
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	read := func() (time.Time, bool, int) {
		var rec struct {
			PromotedAt time.Time        `bson:"promotedAt"`
			Pending    bool             `bson:"echoPending"`
			Echoed     []bson.Timestamp `bson:"echoed"`
		}
		if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&rec); err != nil {
			t.Fatalf("read: %v", err)
		}
		return rec.PromotedAt, rec.Pending, len(rec.Echoed)
	}
	idv := bson.RawValue{Type: bson.TypeString, Value: bsoncoreString("x")}
	now := time.Now().UTC()

	seed()
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, ResidencyUpdate("tctest.promo", idv, now, time.Minute)); err != nil {
		t.Fatalf("update: %v", err)
	}
	if at, pending, _ := read(); !at.Equal(old) || pending {
		t.Fatalf("the guess changed: promotedAt %v pending %v; this test no longer shows what it was written for", at, pending)
	}

	seed()
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, PromotionUpdate("tctest.promo", idv, now, time.Minute)); err != nil {
		t.Fatalf("update: %v", err)
	}
	at, pending, echoed := read()
	if at.Equal(old) || !pending {
		t.Errorf("a copy-up that asked opened no promotion: promotedAt %v, echoPending %v", at, pending)
	}
	if echoed != 1 {
		t.Errorf("the record's earlier echo positions went: %d left", echoed)
	}
}

func bsoncoreString(s string) []byte {
	b := make([]byte, 4, 5+len(s))
	n := len(s) + 1
	b[0], b[1], b[2], b[3] = byte(n), byte(n>>8), byte(n>>16), byte(n>>24)
	return append(append(b, s...), 0)
}

// Two copy-ups of one document both find the fast tier without it and both open
// a promotion. The second must join the first rather than move its instant past
// the only insert that lands; see PromotionUpdate. An older promotion, or an
// ended one, is opened again.
func TestARacingPromotionJoinsTheOneJustOpened(t *testing.T) {
	c, cleanup := testCold(t)
	defer cleanup()
	ctx := context.Background()
	coll := c.Database(DB).Collection(Residency)
	const ns = "tctest.promojoin"
	typ, b, _ := bson.MarshalValue(int32(1))
	id := bson.RawValue{Type: typ, Value: b}
	docID := DocID(ns, id)
	_, _ = coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: docID}})
	defer func() { _, _ = coll.DeleteOne(context.Background(), bson.D{{Key: "_id", Value: docID}}) }()

	promote := func(now time.Time) time.Time {
		t.Helper()
		if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: docID}},
			PromotionUpdate(ns, id, now, time.Minute), options.UpdateOne().SetUpsert(true)); err != nil {
			t.Fatalf("PromotionUpdate: %v", err)
		}
		var rec ResidencyRecord
		if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: docID}}).Decode(&rec); err != nil {
			t.Fatalf("read record: %v", err)
		}
		return rec.PromotedAt
	}

	first := time.Now().UTC().Truncate(time.Millisecond)
	if got := promote(first); !got.Equal(first) {
		t.Fatalf("first promotion at %v, want %v", got, first)
	}
	if got := promote(first.Add(50 * time.Millisecond)); !got.Equal(first) {
		t.Errorf("a copy-up 50ms later moved the promotion to %v; it must join the one opened at %v", got, first)
	}

	// Two minutes old: the stale record 9fe0346 was about, opened again.
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: docID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "promotedAt", Value: first.Add(-2 * time.Minute)}}}}); err != nil {
		t.Fatalf("age the promotion: %v", err)
	}
	if got := promote(first); !got.Equal(first) {
		t.Errorf("a promotion two minutes old was joined, promotedAt %v; it must open again at %v", got, first)
	}

	// Ended: the document left the fast tier, so this is a new promotion.
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: docID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "endedAt", Value: first}}}}); err != nil {
		t.Fatalf("end the promotion: %v", err)
	}
	later := first.Add(time.Second)
	if got := promote(later); !got.Equal(later) {
		t.Errorf("an ended promotion was joined, promotedAt %v; it must open again at %v", got, later)
	}
}

// A notice is judged by the sessions its announcements named only while every
// pending one named a session. The evictor before sessions announced with a
// pipeline that touched neither field, and its delete, outside a transaction,
// was refused by an earlier announcement's session and would have been
// tombstoned. Spent and announced again the old way, the notice is open.
func TestEvictionNoticeSessionsFollowThePending(t *testing.T) {
	c, cleanup := testCold(t)
	defer cleanup()
	ctx := context.Background()
	read := func() EvictionNotice {
		_, notes, err := ReadEvictionNoticesIn(ctx, c, []string{noticeID}, nil)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return notes[noticeID]
	}
	if err := AnnounceEvictionIn(ctx, c, []string{noticeID}, "step:a", "aa11"); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if n := read(); n.Accepts("") || !n.Accepts("aa11") {
		t.Fatalf("a notice announced in session aa11 accepts %v / %v, want only aa11", n.Accepts(""), n.Accepts("aa11"))
	}
	if err := SpendEvictionNotices(ctx, c, map[string][]bson.Timestamp{noticeID: {{T: 100, I: 1}}}); err != nil {
		t.Fatalf("spend: %v", err)
	}
	// The evictor from before sessions: only pending and by move.
	if _, err := c.Database(DB).Collection(Evicting).UpdateOne(ctx, bson.D{{Key: "_id", Value: noticeID}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "pending", Value: 1}}}, {Key: "$push", Value: bson.D{{Key: "by", Value: "old:1"}}}}); err != nil {
		t.Fatal(err)
	}
	if n := read(); !n.Accepts("") {
		t.Errorf("the older evictor's delete is refused by a spent announcement's session (named %v, pending %d); it would be tombstoned",
			n.Named, n.Left())
	}
	// A new announcement after it extends nothing: the notice stays open.
	if err := AnnounceEvictionIn(ctx, c, []string{noticeID}, "step:b", "bb22"); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if n := read(); !n.Accepts("") {
		t.Error("a session's announcement narrowed a notice an older evictor's announcement left open")
	}

	// Sent again after a lost reply: the act counts once, and so does its
	// session.
	other := noticeID + "x"
	named := func(n EvictionNotice) int {
		if n.Named == nil {
			return -1
		}
		return *n.Named
	}
	readOther := func() EvictionNotice {
		_, notes, err := ReadEvictionNoticesIn(ctx, c, []string{other}, nil)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return notes[other]
	}
	for i := 0; i < 2; i++ {
		if err := AnnounceEvictionIn(ctx, c, []string{other}, "step:c", "cc33"); err != nil {
			t.Fatalf("announce: %v", err)
		}
	}
	if n := readOther(); named(n) != n.Left() || n.Accepts("") || !n.Accepts("cc33") {
		t.Errorf("an announcement sent twice: named %d, pending %d, accepts none %v; want named = pending and only cc33",
			named(n), n.Left(), n.Accepts(""))
	}
	// A second announcement, withdrawn: named comes down with pending.
	if err := AnnounceEvictionIn(ctx, c, []string{other}, "step:d", "dd44"); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if err := WithdrawEviction(ctx, c, []string{other}, "step:d"); err != nil {
		t.Fatalf("withdraw: %v", err)
	}
	if n := readOther(); named(n) != n.Left() || n.Accepts("") {
		t.Errorf("after a withdrawal: named %d, pending %d, accepts none %v; want named = pending and closed",
			named(n), n.Left(), n.Accepts(""))
	}
	// Spent to nothing: no session survives for an older evictor to inherit.
	if err := SpendEvictionNotices(ctx, c, map[string][]bson.Timestamp{other: {{T: 200, I: 1}}}); err != nil {
		t.Fatalf("spend: %v", err)
	}
	if _, err := c.Database(DB).Collection(Evicting).UpdateOne(ctx, bson.D{{Key: "_id", Value: other}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "pending", Value: 1}}}, {Key: "$push", Value: bson.D{{Key: "by", Value: "old:2"}}}}); err != nil {
		t.Fatal(err)
	}
	if n := readOther(); !n.Accepts("") {
		t.Errorf("an older evictor's announcement inherited spent sessions (named %d, pending %d); its delete would be tombstoned",
			named(n), n.Left())
	}
}
