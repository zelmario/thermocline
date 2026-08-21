package evictor

import (
	"context"
	"io"
	"log/slog"
	"os"
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
	defer func() { _ = cl.Disconnect(context.WithoutCancel(ctx)) }()

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
	if err := e.evict(ctx, ns, coll, cands); err != nil {
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
