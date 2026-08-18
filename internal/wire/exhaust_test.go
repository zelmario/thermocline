package wire

import (
	"encoding/binary"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func msgWithFlags(t *testing.T, flags uint32, doc bson.D, checksum bool) *Message {
	t.Helper()
	body, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	total := HeaderLen + 4 + 1 + len(body)
	if checksum {
		total += 4
	}
	raw := make([]byte, 0, total)
	raw = binary.LittleEndian.AppendUint32(raw, uint32(total))
	raw = binary.LittleEndian.AppendUint32(raw, 7)
	raw = binary.LittleEndian.AppendUint32(raw, 0)
	raw = binary.LittleEndian.AppendUint32(raw, uint32(OpMsg))
	raw = binary.LittleEndian.AppendUint32(raw, flags)
	raw = append(raw, 0)
	raw = append(raw, body...)
	if checksum {
		raw = binary.LittleEndian.AppendUint32(raw, 0xdeadbeef)
	}
	return &Message{
		Header: Header{
			Length:     int32(len(raw)),
			RequestID:  7,
			ResponseTo: 0,
			OpCode:     OpMsg,
		},
		Raw: raw,
	}
}

// The server answers one request with several replies only when this bit is
// set, and this proxy reads one reply per request. Measured against mongod: a
// getMore with the bit set produced five replies, and the next command on that
// connection was answered with the leftovers.
func TestExhaustBitIsCleared(t *testing.T) {
	doc := bson.D{{Key: "getMore", Value: int64(1)}, {Key: "collection", Value: "c"}, {Key: "$db", Value: "shop"}}

	m := msgWithFlags(t, flagExhaustAllowed, doc, false)
	if !m.ExhaustAllowed() {
		t.Fatal("the fixture does not carry the exhaust bit")
	}
	out := m.ClearExhaustAllowed()
	if out.ExhaustAllowed() {
		t.Error("the exhaust bit survived")
	}
	name, db, err := out.CommandName()
	if err != nil || name != "getMore" || db != "shop" {
		t.Errorf("command after clearing = %q %q %v", name, db, err)
	}

	// A message that never asked for exhaust is returned untouched.
	plain := msgWithFlags(t, 0, doc, false)
	if plain.ClearExhaustAllowed() != plain {
		t.Error("a message without the bit was copied anyway")
	}
}

// A checksum covers the flags, so one present goes with the bit. The result has
// to stay a message the server can read.
func TestClearingExhaustDropsTheChecksum(t *testing.T) {
	doc := bson.D{{Key: "getMore", Value: int64(1)}, {Key: "$db", Value: "shop"}}
	m := msgWithFlags(t, flagExhaustAllowed|flagChecksumPresent, doc, true)

	out := m.ClearExhaustAllowed()
	if out.ExhaustAllowed() {
		t.Error("the exhaust bit survived")
	}
	flags := binary.LittleEndian.Uint32(out.Raw[HeaderLen : HeaderLen+4])
	if flags&flagChecksumPresent != 0 {
		t.Error("the checksum flag survived without the checksum")
	}
	if got := int(binary.LittleEndian.Uint32(out.Raw[0:4])); got != len(out.Raw) {
		t.Errorf("messageLength = %d, raw = %d", got, len(out.Raw))
	}
	if _, _, err := out.CommandName(); err != nil {
		t.Errorf("the trimmed message no longer reads: %v", err)
	}
}
