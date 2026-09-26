package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func event(t *testing.T, tok string) bson.RawValue {
	t.Helper()
	b, err := bson.Marshal(bson.D{{Key: "_id", Value: bson.D{{Key: "_data", Value: tok}}}, {Key: "operationType", Value: "update"}})
	if err != nil {
		t.Fatal(err)
	}
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: b}
}

// A cut reply says where it stopped, or a driver that read all of it resumes
// from mongod's token and never sees what was held.
func TestACutBatchCarriesItsLastEventsToken(t *testing.T) {
	reply, err := bson.Marshal(bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "nextBatch", Value: bson.A{}},
			{Key: "postBatchResumeToken", Value: bson.D{{Key: "_data", Value: "END"}}},
			{Key: "id", Value: int64(7)},
			{Key: "ns", Value: "d.c"},
		}},
		{Key: "ok", Value: 1.0},
	})
	if err != nil {
		t.Fatal(err)
	}
	sent := bson.A{event(t, "A"), event(t, "B")}
	msg, err := replaceStreamBatch(reply, "nextBatch", sent, lastEventID(sent), 1)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := msg.ReplyDoc()
	if err != nil {
		t.Fatal(err)
	}
	cur := doc.Lookup("cursor").Document()
	if got := cur.Lookup("postBatchResumeToken", "_data").StringValue(); got != "B" {
		t.Fatalf("token %q, want the last event's, B", got)
	}
	if n, _ := cur.Lookup("nextBatch").Array().Values(); len(n) != 2 {
		t.Fatalf("batch has %d events, want 2", len(n))
	}
	if id := cur.Lookup("id").Int64(); id != 7 {
		t.Fatalf("cursor id %d, want 7", id)
	}

	// Uncut, the reply's own token stands.
	msg, err = replaceStreamBatch(reply, "nextBatch", sent, bson.RawValue{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	doc, _ = msg.ReplyDoc()
	if got := doc.Lookup("cursor", "postBatchResumeToken", "_data").StringValue(); got != "END" {
		t.Fatalf("token %q, want mongod's own, END", got)
	}
}

func TestAnEmptyBatchHasNoLastToken(t *testing.T) {
	if v := lastEventID(nil); v.Type != 0 {
		t.Fatalf("got %v", v)
	}
}
