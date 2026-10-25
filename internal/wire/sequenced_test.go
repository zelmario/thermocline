package wire

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A rebuilt batch goes out as a payload sequence, so it can be as large as a
// driver's, and reads back the same through every accessor.
func TestNewMsgSequencedReadsBack(t *testing.T) {
	d1, _ := bson.Marshal(bson.D{{Key: "_id", Value: 1}})
	d2, _ := bson.Marshal(bson.D{{Key: "_id", Value: 2}})
	m, err := NewMsgSequenced(7, 0, bson.D{
		{Key: "insert", Value: "c"},
		{Key: "documents", Value: bson.A{bson.Raw(d1), bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: d2}}},
		{Key: "$db", Value: "shop"},
	}, "documents")
	if err != nil {
		t.Fatal(err)
	}
	body, err := m.CommandDoc()
	if err != nil {
		t.Fatal(err)
	}
	if _, lerr := body.LookupErr("documents"); lerr == nil {
		t.Error("the batch is in the body, where mongod caps it at 16MB")
	}
	if name, db, _ := m.CommandName(); name != "insert" || db != "shop" {
		t.Errorf("command %q on %q, want insert on shop", name, db)
	}
	docs := m.AllOf("documents")
	if len(docs) != 2 || docs[1].Lookup("_id").Int32() != 2 {
		t.Errorf("the sequence read back as %v", docs)
	}
	if int(m.Header.Length) != len(m.Raw) {
		t.Errorf("header length %d, message %d bytes", m.Header.Length, len(m.Raw))
	}
	in, err := m.Inlined()
	if err != nil || len(in.Lookup("documents").Array()) == 0 {
		t.Errorf("Inlined lost the batch: %v", err)
	}
}
