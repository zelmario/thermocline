package feeder

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/zelmario/thermocline/internal/meta"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func testFeeder() *Feeder {
	return &Feeder{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// event builds a change stream document with only the fields handle reads.
func event(t *testing.T, op, db, coll string, extra bson.D) bson.Raw {
	t.Helper()
	doc := bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "resumetoken"}}},
		{Key: "operationType", Value: op},
		{Key: "ns", Value: bson.D{{Key: "db", Value: db}, {Key: "coll", Value: coll}}},
		{Key: "clusterTime", Value: bson.Timestamp{T: uint32(time.Now().Unix())}},
	}
	doc = append(doc, extra...)
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return raw
}

func docEvent(t *testing.T, op string, id any, full bson.D) bson.Raw {
	t.Helper()
	extra := bson.D{{Key: "documentKey", Value: bson.D{{Key: "_id", Value: id}}}}
	if full != nil {
		extra = append(extra, bson.E{Key: "fullDocument", Value: full})
	}
	return event(t, op, "app", "orders", extra)
}

// DDL events carry no documentKey. Classifying the operation has to happen
// before the _id lookup, or a dropped collection is reported as a malformed
// event and the operator never learns what actually happened.
func TestHandleDDLWithoutDocumentKey(t *testing.T) {
	for _, op := range []string{"drop", "dropDatabase", "rename"} {
		t.Run(op, func(t *testing.T) {
			f := testFeeder()
			if err := f.handle(context.Background(), event(t, op, "app", "orders", nil)); err != nil {
				t.Fatalf("handle(%s) returned %v, want nil", op, err)
			}
			if len(f.pending) != 0 {
				t.Errorf("handle(%s) buffered %d operations, want 0: destructive DDL must never reach the archive",
					op, len(f.pending))
			}
		})
	}
}

func TestHandleBuffersWork(t *testing.T) {
	tests := []struct {
		name     string
		ev       bson.Raw
		wantLen  int
		wantKind action
	}{
		{
			name:    "insert is upserted",
			ev:      docEvent(t, "insert", 1, bson.D{{Key: "_id", Value: 1}, {Key: "x", Value: 1}}),
			wantLen: 1, wantKind: actionUpsert,
		},
		{
			name:    "update is upserted as a whole document",
			ev:      docEvent(t, "update", 1, bson.D{{Key: "_id", Value: 1}, {Key: "x", Value: 2}}),
			wantLen: 1, wantKind: actionUpsert,
		},
		{
			name:    "delete waits to be resolved in bulk",
			ev:      docEvent(t, "delete", 1, nil),
			wantLen: 1, wantKind: actionDeletePending,
		},
		{
			name: "update whose document vanished is dropped",
			// updateLookup returns nothing when the document was deleted
			// before the lookup ran. The delete event is next in the stream.
			ev:      docEvent(t, "update", 1, nil),
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := testFeeder()
			if err := f.handle(context.Background(), tt.ev); err != nil {
				t.Fatalf("handle returned %v, want nil", err)
			}
			if len(f.pending) != tt.wantLen {
				t.Fatalf("buffered %d operations, want %d", len(f.pending), tt.wantLen)
			}
			if tt.wantLen > 0 && f.pending[0].kind != tt.wantKind {
				t.Errorf("buffered kind %v, want %v", f.pending[0].kind, tt.wantKind)
			}
		})
	}
}

// Thermocline's own bookkeeping must never be mirrored back into itself.
func TestHandleSkipsInternalNamespaces(t *testing.T) {
	for _, db := range []string{"admin", "local", "config", meta.DB} {
		f := testFeeder()
		ev := event(t, "insert", db, "whatever", bson.D{
			{Key: "documentKey", Value: bson.D{{Key: "_id", Value: 1}}},
			{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: 1}}},
		})
		if err := f.handle(context.Background(), ev); err != nil {
			t.Fatalf("handle returned %v, want nil", err)
		}
		if len(f.pending) != 0 {
			t.Errorf("db %q: buffered %d operations, want 0", db, len(f.pending))
		}
	}
}
