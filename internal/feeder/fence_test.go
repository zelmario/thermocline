package feeder

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/lease"
	"github.com/zelmario/thermocline/internal/meta"
)

// A feeder paused past its lease was replaced by its standby, and its flush
// then landed after the standby's newer writes. The flush is fenced on the
// lease inside its own transaction, so one written without it writes nothing.
func TestFlushWritesNothingWithoutTheLease(t *testing.T) {
	coldURI := os.Getenv("TC_TEST_COLD")
	if coldURI == "" {
		t.Skip("set TC_TEST_COLD to run the flush fence test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cold, err := mongo.Connect(options.Client().ApplyURI(coldURI))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cold.Disconnect(context.WithoutCancel(ctx)) }()
	const ns = "tc_fence.docs"
	cc := cold.Database("tc_fence").Collection("docs")
	markers := cold.Database(meta.DB).Collection(meta.Markers)
	row := bson.D{{Key: "_id", Value: "lease:feeder:fence-flush"}}
	t.Cleanup(func() {
		c := context.WithoutCancel(ctx)
		_ = cc.Drop(c)
		_, _ = markers.DeleteOne(c, row)
	})
	_ = cc.Drop(ctx)
	_, _ = markers.DeleteOne(ctx, row)
	if err := cold.Database("tc_fence").CreateCollection(ctx, "docs"); err != nil {
		t.Fatal(err)
	}

	hold := lease.New(cold, "feeder", "fence-flush")
	if err := hold.Take(ctx); err != nil {
		t.Fatal(err)
	}
	f := &Feeder{cold: cold, hold: hold, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	flush := func(id int) error {
		doc := bson.D{{Key: "_id", Value: id}, {Key: "v", Value: "stale"}}
		models := map[string][]mongo.WriteModel{ns: {mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: id}}).SetReplacement(doc).SetUpsert(true)}}
		raw, _ := bson.Marshal(doc)
		ops := map[string][]pendingOp{ns: {{ns: ns, id: bson.Raw(raw).Lookup("_id"), doc: raw, kind: actionUpsert}}}
		_, err := f.writeFlush(ctx, []string{ns}, models, ops, time.Now().UTC())
		return err
	}

	if err := flush(1); err != nil {
		t.Fatalf("a flush with the lease held: %v", err)
	}
	if _, err := markers.UpdateOne(ctx, row, bson.D{{Key: "$set", Value: bson.D{{Key: "owner", Value: "standby"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := flush(2); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("a flush after the standby took the lease returned %v, want ErrLost", err)
	}
	if n, _ := cc.CountDocuments(ctx, bson.D{{Key: "_id", Value: 2}}); n != 0 {
		t.Error("the flush written without the lease reached the archive")
	}

	// The fallback for a flush the archive's cache cannot hold in one
	// transaction, two namespaces and then halves of one, is fenced too: it
	// was plain bulk writes, and it is the slowest path, the one most likely
	// to outlast the lease.
	const ns2 = "tc_fence.more"
	t.Cleanup(func() { _ = cold.Database("tc_fence").Collection("more").Drop(context.WithoutCancel(ctx)) })
	split := map[string][]mongo.WriteModel{}
	splitOps := map[string][]pendingOp{}
	for _, n := range []string{ns, ns2} {
		for id := 10; id < 14; id++ {
			doc := bson.D{{Key: "_id", Value: id}, {Key: "v", Value: "stale"}}
			split[n] = append(split[n], mongo.NewReplaceOneModel().
				SetFilter(bson.D{{Key: "_id", Value: id}}).SetReplacement(doc).SetUpsert(true))
			raw, _ := bson.Marshal(doc)
			splitOps[n] = append(splitOps[n], pendingOp{ns: n, id: bson.Raw(raw).Lookup("_id"), doc: raw, kind: actionUpsert})
		}
	}
	if _, err := f.writeSplit(ctx, []string{ns, ns2}, split, splitOps, time.Now().UTC()); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("the split flush after the standby took the lease returned %v, want ErrLost", err)
	}
	if _, err := f.writeSplit(ctx, []string{ns2}, map[string][]mongo.WriteModel{ns2: split[ns2]},
		map[string][]pendingOp{ns2: splitOps[ns2]}, time.Now().UTC()); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("a namespace written in halves after the standby took the lease returned %v, want ErrLost", err)
	}
	for _, c := range []*mongo.Collection{cc, cold.Database("tc_fence").Collection("more")} {
		if n, _ := c.CountDocuments(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: 10}}}}); n != 0 {
			t.Errorf("the split flush written without the lease reached %s: %d documents", c.Name(), n)
		}
	}
}
