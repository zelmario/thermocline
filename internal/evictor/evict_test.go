package evictor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
)

// A batch that deletes fewer documents than it announced must leave the
// announcements of the documents it did delete alone.
//
// The evictor announces a batch and then removes it. If something else removes
// one of those documents in between, the delete reports fewer removals than
// were announced, and a bulk delete does not say which id it missed.
//
// The first answer to that took the surplus off the front of the batch, on the
// reasoning that every id gives one announcement back and the evicted ones get
// theirs again. They do not. The batch is scanned by _id ascending, so the
// front of it is the lowest ids, and if the missing documents are anywhere else
// the withdrawal lands on documents that were evicted. Their deletes then reach
// the feeder unannounced, which reads them as the application's and tombstones
// them: 141 documents on the pilot, by the fix meant to prevent exactly this.
//
// This cannot be arranged from a shell suite. Removing the documents before the
// evictor runs only means the scan does not find them, and there is no
// shortfall. The gap being tested is between the scan and the delete, so the
// test calls evict with the batch the scan would have produced.
func TestShortDeleteKeepsTheNoticesItEarned(t *testing.T) {
	uri := os.Getenv("TC_TEST_COLD")
	if uri == "" {
		t.Skip("set TC_TEST_COLD to a tier URI to run the short delete test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	// A cleanup, not a defer. Deferred calls run when the test function
	// returns and cleanups run after that, so a deferred disconnect closed the
	// client every cleanup below was about to use: they failed silently and the
	// collections stayed in the archive, where every router started since has
	// warned about them. Registered first, it runs last.
	t.Cleanup(func() { _ = cl.Disconnect(context.WithoutCancel(ctx)) })

	const ns = "tc_shortdelete.docs"
	coll := cl.Database("tc_shortdelete").Collection("docs")
	notices := cl.Database(meta.DB).Collection(meta.Evicting)
	t.Cleanup(func() {
		c := context.WithoutCancel(ctx)
		_ = coll.Drop(c)
		_, _ = notices.DeleteMany(c, bson.D{})
	})
	if err := coll.Drop(ctx); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := notices.DeleteMany(ctx, bson.D{}); err != nil {
		t.Fatalf("clear notices: %v", err)
	}

	// Ten candidates in _id order. Only the lowest five are on the fast tier,
	// standing in for five that something removed after the scan read them.
	const total, present = 10, 5
	ids := make([]bson.ObjectID, total)
	cands := make([]candidate, total)
	for i := range ids {
		ids[i] = bson.NewObjectID()
	}
	// NewObjectID is ascending within a second but the counter can wrap, so the
	// order is made explicit rather than assumed.
	for i := 0; i < total; i++ {
		for j := i + 1; j < total; j++ {
			if ids[j].Hex() < ids[i].Hex() {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	for i, id := range ids {
		raw := rawIDValue(t, id)
		cands[i] = candidate{id: raw, docID: meta.DocID(ns, raw)}
		if i < present {
			if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: id}}); err != nil {
				t.Fatalf("insert %d: %v", i, err)
			}
		}
	}

	e := &Evictor{
		hot:  cl,
		cold: cl,
		cfg:  config.EvictorConfig{FeederID: "test"},
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if _, err := e.evict(ctx, ns, coll, cands); err != nil {
		t.Fatalf("evict: %v", err)
	}

	left, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if left != 0 {
		t.Fatalf("fast tier still holds %d of them, so the delete under test did not happen", left)
	}

	// The five it evicted each need their announcement, or the feeder tombstones
	// a document nothing deleted.
	for i := 0; i < present; i++ {
		var got struct {
			Pending int `bson:"pending"`
		}
		err := notices.FindOne(ctx, bson.D{{Key: "_id", Value: cands[i].docID}}).Decode(&got)
		if err != nil {
			t.Fatalf("evicted document %d has no announcement (%v); its delete reaches the feeder unannounced and is tombstoned", i, err)
		}
		if got.Pending != 1 {
			t.Errorf("evicted document %d has pending=%d, want 1", i, got.Pending)
		}
	}

	// And the five that were already gone should never have been announced. A
	// notice standing for a delete that is not coming answers for the next one,
	// which could be the application's.
	for i := present; i < total; i++ {
		n, err := notices.CountDocuments(ctx, bson.D{{Key: "_id", Value: cands[i].docID}})
		if err != nil {
			t.Fatalf("count notice %d: %v", i, err)
		}
		if n != 0 {
			t.Errorf("document %d was not on the fast tier but was announced anyway", i)
		}
	}
}

func rawIDValue(t *testing.T, id bson.ObjectID) bson.RawValue {
	t.Helper()
	b, err := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rv, err := bson.Raw(b).LookupErr("_id")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	return rv
}

// An eviction ends only the holds it evicted under, and a hold placed after the
// batch was chosen is not one of them.
//
// The evictor checks holds when it chooses a batch and ends them after the
// delete, and a router can copy a document up or refresh a lapsed hold in
// between. Ending that hold says the document left the fast tier before the
// router's copy-up put it back, so the copy-up's own insert reads as the
// application writing the _id again and is handed to every change stream as an
// order nobody placed. Measured on 2026-09-25 with two applications behind two
// routers: a hold placed at 27.754, the evictor's delete at 27.794 and its end
// at 27.822, the other router's copy-up at 27.833, and both applications shown
// two seeded orders as new ones.
//
// Called with the batch the scan would have produced, for the reason the test
// above gives: the window is between the scan and the delete.
func TestEvictionEndsOnlyTheHoldsItJudged(t *testing.T) {
	uri := os.Getenv("TC_TEST_COLD")
	if uri == "" {
		t.Skip("set TC_TEST_COLD to a tier URI to run the hold test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	// A cleanup, not a defer. Deferred calls run when the test function
	// returns and cleanups run after that, so a deferred disconnect closed the
	// client every cleanup below was about to use: they failed silently and the
	// collections stayed in the archive, where every router started since has
	// warned about them. Registered first, it runs last.
	t.Cleanup(func() { _ = cl.Disconnect(context.WithoutCancel(ctx)) })

	const ns = "tc_holdend.docs"
	coll := cl.Database("tc_holdend").Collection("docs")
	residency := cl.Database(meta.DB).Collection(meta.Residency)
	notices := cl.Database(meta.DB).Collection(meta.Evicting)
	t.Cleanup(func() {
		c := context.WithoutCancel(ctx)
		_ = coll.Drop(c)
		_, _ = residency.DeleteMany(c, bson.D{{Key: "ns", Value: ns}})
		_, _ = notices.DeleteMany(c, bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}}})
	})
	if err := coll.Drop(ctx); err != nil {
		t.Fatalf("drop: %v", err)
	}

	// lapsed is what the scan chose it under: a hold that is over. placed is a
	// hold a router wrote after the scan, a copy-up two minutes long.
	lapsed, placed := bson.NewObjectID(), bson.NewObjectID()
	now := time.Now().UTC()
	hold := map[bson.ObjectID]time.Time{lapsed: now.Add(-time.Minute), placed: now.Add(2 * time.Minute)}
	var cands []candidate
	for _, id := range []bson.ObjectID{lapsed, placed} {
		raw := rawIDValue(t, id)
		c := candidate{id: raw, docID: meta.DocID(ns, raw)}
		cands = append(cands, c)
		if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: id}}); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := residency.InsertOne(ctx, bson.D{
			{Key: "_id", Value: c.docID}, {Key: "ns", Value: ns}, {Key: "doc", Value: id},
			{Key: "promotedAt", Value: hold[id].Add(-2 * time.Minute)},
			{Key: "hotUntil", Value: hold[id]},
			{Key: "expiresAt", Value: hold[id].Add(time.Hour)},
		}); err != nil {
			t.Fatalf("residency: %v", err)
		}
	}

	e := &Evictor{
		hot:  cl,
		cold: cl,
		cfg:  config.EvictorConfig{FeederID: "test"},
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if _, err := e.evict(ctx, ns, coll, cands); err != nil {
		t.Fatalf("evict: %v", err)
	}

	ended := func(c candidate) bool {
		var rec struct {
			Ended *time.Time `bson:"endedAt"`
		}
		if err := residency.FindOne(ctx, bson.D{{Key: "_id", Value: c.docID}}).Decode(&rec); err != nil {
			t.Fatalf("read residency: %v", err)
		}
		return rec.Ended != nil
	}
	if !ended(cands[0]) {
		t.Error("the hold the eviction was chosen under was not ended; a late consumer cannot tell the copy-up's insert from a new one")
	}
	if ended(cands[1]) {
		t.Error("a hold placed after the batch was chosen was ended by the eviction; the copy-up that placed it reaches change streams as an insert nobody made")
	}
}

// A document changed since it was mirrored stays on the fast tier, because the
// archive does not yet hold the version it would be evicting.
//
// Confirming the archive held the _id was the whole check, and a document
// updated a moment before its eviction lost the update: the fast tier's copy
// went, the feeder's lookup for the update came back empty and was skipped,
// and the archive kept the version from before. Measured on 2026-09-25, twenty
// acknowledged updates lost to one sweep.
//
// Needs the two tiers to be different clusters, which the tests above do not.
func TestEvictionLeavesADocumentChangedSinceMirrored(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tiers to run the changed-document test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hot, err := mongo.Connect(options.Client().ApplyURI(hotURI))
	if err != nil {
		t.Fatalf("connect hot: %v", err)
	}
	t.Cleanup(func() { _ = hot.Disconnect(context.WithoutCancel(ctx)) })
	cold, err := mongo.Connect(options.Client().ApplyURI(coldURI))
	if err != nil {
		t.Fatalf("connect cold: %v", err)
	}
	t.Cleanup(func() { _ = cold.Disconnect(context.WithoutCancel(ctx)) })

	const ns = "tc_changed.docs"
	hc := hot.Database("tc_changed").Collection("docs")
	cc := cold.Database("tc_changed").Collection("docs")
	notices := cold.Database(meta.DB).Collection(meta.Evicting)
	t.Cleanup(func() {
		c := context.WithoutCancel(ctx)
		_ = hc.Drop(c)
		_ = cc.Drop(c)
		_, _ = notices.DeleteMany(c, bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}}})
	})
	_ = hc.Drop(ctx)
	_ = cc.Drop(ctx)

	same, changed := bson.NewObjectID(), bson.NewObjectID()
	for _, d := range []struct {
		id      bson.ObjectID
		hot, cd string
	}{{same, "as mirrored", "as mirrored"}, {changed, "after", "before"}} {
		if _, err := hc.InsertOne(ctx, bson.D{{Key: "_id", Value: d.id}, {Key: "v", Value: d.hot}}); err != nil {
			t.Fatalf("insert hot: %v", err)
		}
		if _, err := cc.InsertOne(ctx, bson.D{{Key: "_id", Value: d.id}, {Key: "v", Value: d.cd}}); err != nil {
			t.Fatalf("insert cold: %v", err)
		}
	}
	var cands []candidate
	for _, id := range []bson.ObjectID{same, changed} {
		raw := rawIDValue(t, id)
		cands = append(cands, candidate{id: raw, docID: meta.DocID(ns, raw)})
	}

	e := &Evictor{hot: hot, cold: cold, cfg: config.EvictorConfig{FeederID: "test"},
		log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	removed, err := e.evict(ctx, ns, hc, cands)
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	if removed != 1 {
		t.Errorf("evict removed %d, want 1: only the document the archive holds as it is", removed)
	}
	if n, _ := hc.CountDocuments(ctx, bson.D{{Key: "_id", Value: changed}}); n != 1 {
		t.Error("a document changed since it was mirrored was evicted; its only current copy is gone")
	}
	if n, _ := hc.CountDocuments(ctx, bson.D{{Key: "_id", Value: same}}); n != 0 {
		t.Error("a document identical to its archived copy was not evicted")
	}
	if n, _ := notices.CountDocuments(ctx, bson.D{{Key: "_id", Value: cands[1].docID}}); n != 0 {
		t.Error("the changed document was announced as evicted, and the notice would answer for its next delete")
	}
}

// A document near the 16MB limit is still evicted. Since each delete insists on
// the exact document, carrying it whole in the filter made the delete command
// larger than the limit, and the batch failed on every sweep after: eviction of
// the namespace stopped. Such a document is deleted by _id after the
// comparison instead.
func TestEvictionTakesADocumentNearTheLimit(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tiers to run the large-document test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	hot, err := mongo.Connect(options.Client().ApplyURI(hotURI))
	if err != nil {
		t.Fatalf("connect hot: %v", err)
	}
	t.Cleanup(func() { _ = hot.Disconnect(context.WithoutCancel(ctx)) })
	cold, err := mongo.Connect(options.Client().ApplyURI(coldURI))
	if err != nil {
		t.Fatalf("connect cold: %v", err)
	}
	t.Cleanup(func() { _ = cold.Disconnect(context.WithoutCancel(ctx)) })

	const ns = "tc_bigevict.docs"
	hc := hot.Database("tc_bigevict").Collection("docs")
	cc := cold.Database("tc_bigevict").Collection("docs")
	notices := cold.Database(meta.DB).Collection(meta.Evicting)
	t.Cleanup(func() {
		c := context.WithoutCancel(ctx)
		_ = hc.Drop(c)
		_ = cc.Drop(c)
		_, _ = notices.DeleteMany(c, bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}}})
	})
	_ = hc.Drop(ctx)
	_ = cc.Drop(ctx)

	id := bson.NewObjectID()
	big := bson.D{{Key: "_id", Value: id}, {Key: "pad", Value: strings.Repeat("x", 16<<20-200)}}
	if _, err := hc.InsertOne(ctx, big); err != nil {
		t.Fatalf("insert hot: %v", err)
	}
	if _, err := cc.InsertOne(ctx, big); err != nil {
		t.Fatalf("insert cold: %v", err)
	}
	raw := rawIDValue(t, id)
	e := &Evictor{hot: hot, cold: cold, cfg: config.EvictorConfig{FeederID: "test"},
		log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	removed, err := e.evict(ctx, ns, hc, []candidate{{id: raw, docID: meta.DocID(ns, raw)}})
	if err != nil {
		t.Fatalf("evicting a 15MB document failed, and would fail on every sweep: %v", err)
	}
	if removed != 1 {
		t.Errorf("a 15MB document identical on both tiers was not evicted: removed %d", removed)
	}
}

// A document a hold was placed on between the scan and the announcement is left
// on the fast tier, and its announcement has to go with it.
//
// dropHeldAfterAnnouncing took it back by calling what was then named
// ConsumeEvictionNotices, which
// only reads the counts: the announcement stood for up to an hour with no
// eviction coming to answer it, and the application deleting that document in
// that hour would be read by the feeder as the eviction. The archive keeps the
// document live and the router answers it again.
func TestHeldAfterAnnouncingTakesTheAnnouncementBack(t *testing.T) {
	uri := os.Getenv("TC_TEST_COLD")
	if uri == "" {
		t.Skip("set TC_TEST_COLD to a tier URI to run the withdraw test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = cl.Disconnect(context.WithoutCancel(ctx)) })

	const ns = "tc_heldwithdraw.docs"
	residency := cl.Database(meta.DB).Collection(meta.Residency)
	notices := cl.Database(meta.DB).Collection(meta.Evicting)
	t.Cleanup(func() {
		c := context.WithoutCancel(ctx)
		_, _ = residency.DeleteMany(c, bson.D{{Key: "ns", Value: ns}})
		_, _ = notices.DeleteMany(c, bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}}})
	})

	id := bson.NewObjectID()
	raw := rawIDValue(t, id)
	c := candidate{id: raw, docID: meta.DocID(ns, raw)}
	if _, err := residency.InsertOne(ctx, bson.D{
		{Key: "_id", Value: c.docID}, {Key: "ns", Value: ns}, {Key: "doc", Value: id},
		{Key: "hotUntil", Value: time.Now().UTC().Add(2 * time.Minute)},
		{Key: "expiresAt", Value: time.Now().UTC().Add(time.Hour)},
	}); err != nil {
		t.Fatalf("residency: %v", err)
	}
	if err := meta.AnnounceEviction(ctx, cl, []string{c.docID}); err != nil {
		t.Fatalf("announce: %v", err)
	}

	e := &Evictor{
		hot:  cl,
		cold: cl,
		cfg:  config.EvictorConfig{FeederID: "test"},
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	left, err := e.dropHeldAfterAnnouncing(ctx, ns, []candidate{c})
	if err != nil {
		t.Fatalf("dropHeldAfterAnnouncing: %v", err)
	}
	if len(left) != 0 {
		t.Fatalf("a held document was kept in the batch: %d left", len(left))
	}
	have, err := meta.ReadEvictionNotices(ctx, cl, []string{c.docID})
	if err != nil {
		t.Fatalf("read notices: %v", err)
	}
	if have[c.docID] != 0 {
		t.Errorf("the announcement for a document left on the fast tier still stands at %d; "+
			"the application's next delete of it would be read as an eviction", have[c.docID])
	}
}

// A delete that fails behind the sweep takes back the announcements of the step
// that was announced while it ran.
//
// Since the delete of one step runs while the next is confirmed and announced,
// a failed delete stops the sweep with the next step announced and never
// deleted. Left standing, each of those announcements turns an application
// delete of that document within the hour into an eviction, and the archive
// keeps a document the application deleted. Nothing of that step was removed,
// so all of them come back.
func TestFailedDeleteBehindWithdrawsTheNextStep(t *testing.T) {
	hotURI, coldURI := os.Getenv("TC_TEST_HOT"), os.Getenv("TC_TEST_COLD")
	if hotURI == "" || coldURI == "" {
		t.Skip("set TC_TEST_HOT and TC_TEST_COLD to two tiers to run the failed delete test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hot, err := mongo.Connect(options.Client().ApplyURI(hotURI))
	if err != nil {
		t.Fatalf("connect hot: %v", err)
	}
	t.Cleanup(func() { _ = hot.Disconnect(context.WithoutCancel(ctx)) })
	cold, err := mongo.Connect(options.Client().ApplyURI(coldURI))
	if err != nil {
		t.Fatalf("connect cold: %v", err)
	}
	t.Cleanup(func() { _ = cold.Disconnect(context.WithoutCancel(ctx)) })

	const ns = "tc_behind.docs"
	hc := hot.Database("tc_behind").Collection("docs")
	cc := cold.Database("tc_behind").Collection("docs")
	notices := cold.Database(meta.DB).Collection(meta.Evicting)
	t.Cleanup(func() {
		c := context.WithoutCancel(ctx)
		_ = hc.Drop(c)
		_ = cc.Drop(c)
		_, _ = notices.DeleteMany(c, bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}}})
	})
	_ = hc.Drop(ctx)
	_ = cc.Drop(ctx)

	var cands []candidate
	for i := 0; i < 3; i++ {
		id := bson.NewObjectID()
		doc := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: i}}
		if _, err := hc.InsertOne(ctx, doc); err != nil {
			t.Fatalf("insert hot: %v", err)
		}
		if _, err := cc.InsertOne(ctx, doc); err != nil {
			t.Fatalf("insert cold: %v", err)
		}
		raw := rawIDValue(t, id)
		cands = append(cands, candidate{id: raw, docID: meta.DocID(ns, raw)})
	}

	e := &Evictor{hot: hot, cold: cold, cfg: config.EvictorConfig{FeederID: "test"},
		log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	// The step before this one, whose delete fails while this one is announced.
	var behind removal
	failed := errors.New("delete refused")
	behind.start(func() (int, error) { return 0, failed })

	if _, err := e.evictBehind(ctx, ns, hc, cands, &behind); !errors.Is(err, failed) {
		t.Fatalf("evictBehind returned %v, want the failed delete behind it", err)
	}
	if n, _ := hc.CountDocuments(ctx, bson.D{}); n != 3 {
		t.Errorf("the fast tier holds %d of 3; the step after a failed delete must not be removed", n)
	}
	pending, err := notices.CountDocuments(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + ns + "\\|"}}},
		{Key: "pending", Value: bson.D{{Key: "$gt", Value: 0}}},
	})
	if err != nil {
		t.Fatalf("count notices: %v", err)
	}
	if pending != 0 {
		t.Errorf("%d announcements stand for documents never deleted; an application delete of "+
			"any of them within the hour would be read as an eviction", pending)
	}
}
