package wire

import (
	"encoding/binary"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// nested returns a document {"": {"": ... {}}} that is depth levels deep, built
// as bytes so a depth of millions costs no recursion here either. kind is 0x03
// for documents or 0x04 for arrays below the top.
func nested(depth int, kind byte) []byte {
	size := 5 + 7*(depth-1)
	out := make([]byte, 0, size)
	for i := 0; i < depth-1; i++ {
		out = binary.LittleEndian.AppendUint32(out, uint32(size-7*i))
		out = append(out, kind, 0)
	}
	out = binary.LittleEndian.AppendUint32(out, 5)
	out = append(out, 0)
	for i := 0; i < depth-1; i++ {
		out = append(out, 0)
	}
	return out
}

// opMsg frames a command document, and optionally one kind 1 sequence, as an
// OP_MSG.
func opMsg(cmd []byte, seqID string, seq ...[]byte) *Message {
	var body []byte
	body = binary.LittleEndian.AppendUint32(body, 0)
	body = append(body, 0)
	body = append(body, cmd...)
	if seqID != "" {
		s := make([]byte, 4)
		s = append(s, seqID...)
		s = append(s, 0)
		for _, d := range seq {
			s = append(s, d...)
		}
		binary.LittleEndian.PutUint32(s[0:4], uint32(len(s)))
		body = append(body, 1)
		body = append(body, s...)
	}
	raw := make([]byte, HeaderLen)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(HeaderLen+len(body)))
	binary.LittleEndian.PutUint32(raw[12:16], uint32(OpMsg))
	raw = append(raw, body...)
	return &Message{Header: Header{Length: int32(len(raw)), OpCode: OpMsg}, Raw: raw}
}

func TestNestedBuildsValidBSON(t *testing.T) {
	for _, kind := range []byte{0x03, 0x04} {
		doc := bson.Raw(nested(50, kind))
		if err := doc.Validate(); err != nil {
			t.Fatalf("kind %#x: nested() built invalid BSON: %v", kind, err)
		}
	}
}

func TestDepthAtTheLimitPasses(t *testing.T) {
	for _, kind := range []byte{0x03, 0x04} {
		if d := depthOver(nested(MaxDepth, kind), MaxDepth); d != 0 {
			t.Fatalf("kind %#x: depth %d refused at the limit (reported %d)", kind, MaxDepth, d)
		}
		if d := depthOver(nested(MaxDepth+1, kind), MaxDepth); d == 0 {
			t.Fatalf("kind %#x: depth %d passed a limit of %d", kind, MaxDepth+1, MaxDepth)
		}
	}
}

// The shape of the finding: a find whose filter is nested millions of levels,
// about 21 MB, under the 64 MB frame cap. It must be refused, and quickly,
// without the check itself recursing.
func TestMillionsDeepIsRefusedWithoutRecursion(t *testing.T) {
	m := opMsg(nested(3_000_000, 0x03), "")
	started := time.Now()
	if err := m.CheckDepth(); err == nil {
		t.Fatal("a document 3,000,000 levels deep passed CheckDepth")
	}
	if took := time.Since(started); took > time.Second {
		t.Fatalf("CheckDepth took %v; it should stop at the limit", took)
	}
}

// A driver moves an insert's documents into a kind 1 sequence, so a check of
// the command document alone would let a deep document through to the
// multikey and reserved field walkers.
func TestDepthInASequenceIsRefused(t *testing.T) {
	cmd := mustMarshalRaw(bson.D{{Key: "insert", Value: "c"}, {Key: "$db", Value: "test"}})
	ok := mustMarshalRaw(bson.D{{Key: "_id", Value: 1}})
	m := opMsg(cmd, "documents", ok, nested(MaxDepth+1, 0x04))
	if err := m.CheckDepth(); err == nil {
		t.Fatal("a deep document in a kind 1 sequence passed CheckDepth")
	}
	if err := opMsg(cmd, "documents", ok, nested(20, 0x04)).CheckDepth(); err != nil {
		t.Fatalf("a shallow sequence was refused: %v", err)
	}
}

func TestDepthInALegacyQueryIsRefused(t *testing.T) {
	build := func(doc []byte) *Message {
		var body []byte
		body = binary.LittleEndian.AppendUint32(body, 0)
		body = append(body, []byte("admin.$cmd")...)
		body = append(body, 0)
		body = binary.LittleEndian.AppendUint32(body, 0)
		body = binary.LittleEndian.AppendUint32(body, 1)
		body = append(body, doc...)
		raw := make([]byte, HeaderLen)
		binary.LittleEndian.PutUint32(raw[0:4], uint32(HeaderLen+len(body)))
		binary.LittleEndian.PutUint32(raw[12:16], uint32(OpQuery))
		raw = append(raw, body...)
		return &Message{Header: Header{Length: int32(len(raw)), OpCode: OpQuery}, Raw: raw}
	}
	if err := build(nested(MaxDepth+1, 0x03)).CheckDepth(); err == nil {
		t.Fatal("a deep legacy hello passed CheckDepth")
	}
	hello := mustMarshalRaw(bson.D{{Key: "isMaster", Value: 1}})
	if err := build(hello).CheckDepth(); err != nil {
		t.Fatalf("a plain legacy hello was refused: %v", err)
	}
}

// Every value type has to be stepped over by its own length. A type read with
// the wrong length lands the walk mid-value, and from there it either stops
// early, which would wave through a deep document after it, or misreads.
func TestEveryTypeIsSteppedOver(t *testing.T) {
	oid := bson.NewObjectID()
	doc := bson.D{
		{Key: "double", Value: 1.5},
		{Key: "string", Value: "x"},
		{Key: "doc", Value: bson.D{{Key: "a", Value: 1}}},
		{Key: "array", Value: bson.A{1, "two"}},
		{Key: "binary", Value: bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}},
		{Key: "undefined", Value: bson.Undefined{}},
		{Key: "oid", Value: oid},
		{Key: "bool", Value: true},
		{Key: "date", Value: bson.DateTime(1)},
		{Key: "null", Value: nil},
		{Key: "regex", Value: bson.Regex{Pattern: "a.*", Options: "i"}},
		{Key: "dbptr", Value: bson.DBPointer{DB: "db.c", Pointer: oid}},
		{Key: "js", Value: bson.JavaScript("1")},
		{Key: "symbol", Value: bson.Symbol("s")},
		{Key: "codews", Value: bson.CodeWithScope{Code: "x", Scope: bson.D{{Key: "y", Value: 1}}}},
		{Key: "int32", Value: int32(1)},
		{Key: "ts", Value: bson.Timestamp{T: 1, I: 1}},
		{Key: "int64", Value: int64(1)},
		{Key: "dec", Value: bson.NewDecimal128(1, 1)},
		{Key: "min", Value: bson.MinKey{}},
		{Key: "max", Value: bson.MaxKey{}},
	}
	head := mustMarshalRaw(doc)
	// The deep part comes last, so it is reached only if every value before it
	// was stepped over by the right length.
	deep := bson.Raw(nested(MaxDepth, 0x03))
	full := mustMarshalRaw(append(doc, bson.E{Key: "deep", Value: deep}))
	if d := depthOver(head, MaxDepth); d != 0 {
		t.Fatalf("a shallow document of every type was refused (%d)", d)
	}
	if d := depthOver(full, MaxDepth); d == 0 {
		t.Fatal("a too-deep value after every other type was missed: some type is stepped over by the wrong length")
	}
}

func mustMarshalRaw(d bson.D) []byte {
	b, err := bson.Marshal(d)
	if err != nil {
		panic(err)
	}
	return b
}
