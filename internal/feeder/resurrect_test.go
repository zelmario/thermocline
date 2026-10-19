package feeder

import (
	"context"
	"io"
	"log/slog"
	"os"
	"regexp"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// A removal of a resurrected document that fails, and is tried again on the
// next flush, leaves one announcement for the one delete that is coming.
//
// The failed attempt left its announcement standing on the grounds that the
// retry's would have a delete to answer for. The retry announces too, so the
// document had two announcements and one delete, and the spare turned the
// application's next delete of that _id, within the hour, into an eviction:
// the archive kept a document the application had deleted.
//
// The failure is an account that can read the fast tier and not delete from
// it, which fails the delete the same way every time.
func TestFailedResurrectionRemovalLeavesOneAnnouncement(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tiers to run the resurrection removal test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	connect := func(uri string) *mongo.Client {
		c, err := mongo.Connect(options.Client().ApplyURI(uri))
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(func() { _ = c.Disconnect(context.WithoutCancel(ctx)) })
		return c
	}
	hot, cold := connect(hotURI), connect(coldURI)

	const ns = "tc_resurrect.docs"
	hc := hot.Database("tc_resurrect").Collection("docs")
	notices := cold.Database(meta.DB).Collection(meta.Evicting)
	admin := hot.Database("admin")
	t.Cleanup(func() {
		c := context.WithoutCancel(ctx)
		_ = hc.Drop(c)
		_, _ = notices.DeleteMany(c, bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}}})
		_ = admin.RunCommand(c, bson.D{{Key: "dropUser", Value: "tcnodelete"}}).Err()
	})
	_ = hc.Drop(ctx)
	_ = admin.RunCommand(ctx, bson.D{{Key: "dropUser", Value: "tcnodelete"}}).Err()
	if err := admin.RunCommand(ctx, bson.D{
		{Key: "createUser", Value: "tcnodelete"}, {Key: "pwd", Value: "tcnodelete"},
		{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: "tc_resurrect"}}}},
	}).Err(); err != nil {
		t.Fatalf("create the read-only account: %v", err)
	}
	readOnly := connect(regexp.MustCompile(`//[^@/]*@`).ReplaceAllString(hotURI, "//tcnodelete:tcnodelete@"))

	id := bson.NewObjectID()
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "v", Value: "put back"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := hc.InsertOne(ctx, bson.Raw(doc)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	idv := bson.Raw(doc).Lookup("_id")
	op := pendingOp{ns: ns, id: idv, doc: bson.Raw(doc)}
	docID := meta.DocID(ns, idv)
	pending := func() int {
		var n struct {
			Pending int `bson:"pending"`
		}
		if err := notices.FindOne(ctx, bson.D{{Key: "_id", Value: docID}}).Decode(&n); err != nil {
			return 0
		}
		return n.Pending
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	failing := &Feeder{hot: readOnly, cold: cold, log: quiet}
	if failing.removeResurrected(ctx, op) {
		t.Fatal("the removal reported done with an account that cannot delete")
	}
	if n, _ := hc.CountDocuments(ctx, bson.D{{Key: "_id", Value: id}}); n != 1 {
		t.Fatal("the document left the fast tier through an account that cannot delete")
	}
	if p := pending(); p != 0 {
		t.Errorf("a failed removal left %d announcements standing with no delete coming", p)
	}

	retry := &Feeder{hot: hot, cold: cold, log: quiet}
	if !retry.removeResurrected(ctx, op) {
		t.Fatal("the retry did not remove the document")
	}
	if p := pending(); p != 1 {
		t.Errorf("after the retry the document has %d announcements for the one delete coming; "+
			"the spare reads the application's next delete as an eviction", p)
	}
}

// A feeder stopping with notices a saved checkpoint covers writes them off on
// the way out, though the context it stops with is already cancelled.
//
// Those deletes are behind the checkpoint, so a restart does not replay them,
// and nothing else will ever take their count down. Left, each reads the
// application's next delete of that document as an eviction for an hour.
func TestStoppingFeederSettlesOwedNotices(t *testing.T) {
	coldURI := os.Getenv("TC_TEST_COLD")
	if coldURI == "" {
		t.Skip("set TC_TEST_COLD to a tier URI to run the owed notices test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cold, err := mongo.Connect(options.Client().ApplyURI(coldURI))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = cold.Disconnect(context.WithoutCancel(ctx)) })

	const ns = "tc_owed.docs"
	notices := cold.Database(meta.DB).Collection(meta.Evicting)
	t.Cleanup(func() {
		_, _ = notices.DeleteMany(context.WithoutCancel(ctx),
			bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}}})
	})
	idv := bson.Raw(bsoncore(t, bson.NewObjectID())).Lookup("_id")
	docID := meta.DocID(ns, idv)
	if err := meta.AnnounceEviction(ctx, cold, []string{docID}, "test:0"); err != nil {
		t.Fatalf("announce: %v", err)
	}

	f := &Feeder{cold: cold, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		owedNotices: map[string][]bson.Timestamp{docID: {{T: 1792170000, I: 1}}}}
	stopping, stop := context.WithCancel(ctx)
	stop()
	f.settleOwed(stopping)

	var n struct {
		Pending int `bson:"pending"`
	}
	if err := notices.FindOne(ctx, bson.D{{Key: "_id", Value: docID}}).Decode(&n); err != nil {
		t.Fatalf("read the notice: %v", err)
	}
	if n.Pending != 0 {
		t.Errorf("the notice still counts %d after the feeder stopped; no replay will spend it", n.Pending)
	}
	if len(f.owedNotices) != 0 {
		t.Error("the owed notices were written off and still held")
	}
}

func bsoncore(t *testing.T, id bson.ObjectID) []byte {
	t.Helper()
	b, err := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
