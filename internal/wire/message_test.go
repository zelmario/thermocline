package wire

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustMarshal(t *testing.T, d bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// buildMsg assembles an OP_MSG with one kind 0 section and any kind 1 sequences,
// which is exactly what a driver puts on the wire.
func buildMsg(t *testing.T, cmd bson.D, sequences map[string][]bson.D) []byte {
	t.Helper()
	var body bytes.Buffer
	_ = binary.Write(&body, binary.LittleEndian, uint32(0)) // flags

	body.WriteByte(0)
	body.Write(mustMarshal(t, cmd))

	for id, docs := range sequences {
		var seq bytes.Buffer
		seq.WriteString(id)
		seq.WriteByte(0)
		for _, d := range docs {
			seq.Write(mustMarshal(t, d))
		}
		body.WriteByte(1)
		_ = binary.Write(&body, binary.LittleEndian, uint32(seq.Len()+4))
		body.Write(seq.Bytes())
	}

	total := HeaderLen + body.Len()
	out := make([]byte, HeaderLen)
	binary.LittleEndian.PutUint32(out[0:4], uint32(total))
	binary.LittleEndian.PutUint32(out[4:8], 7)
	binary.LittleEndian.PutUint32(out[8:12], 0)
	binary.LittleEndian.PutUint32(out[12:16], uint32(OpMsg))
	return append(out, body.Bytes()...)
}

func TestReadAndCommandName(t *testing.T) {
	raw := buildMsg(t, bson.D{
		{Key: "find", Value: "orders"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "$db", Value: "app"},
	}, nil)

	msg, err := Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	name, db, err := msg.CommandName()
	if err != nil {
		t.Fatalf("CommandName: %v", err)
	}
	if name != "find" || db != "app" {
		t.Errorf("CommandName = (%q, %q), want (find, app)", name, db)
	}
	if len(msg.Raw) != len(raw) {
		t.Errorf("Raw is %d bytes, want %d: forwarding depends on the frame surviving intact",
			len(msg.Raw), len(raw))
	}
}

// Drivers move bulk arrays into kind 1 sequences, so an update command's
// document contains no updates at all. A proxy reading only the command
// document sees a write with nothing in it and cannot inspect the filter.
func TestPayloadsFindsSequencedWrites(t *testing.T) {
	raw := buildMsg(t,
		bson.D{{Key: "update", Value: "orders"}, {Key: "$db", Value: "app"}},
		map[string][]bson.D{
			"updates": {
				{{Key: "q", Value: bson.D{{Key: "_id", Value: 7}}},
					{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: 1}}}}}},
			},
		})

	msg, err := Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if got := msg.CountOf("updates"); got != 1 {
		t.Fatalf("CountOf(updates) = %d, want 1", got)
	}
	stmt, ok := msg.FirstOf("updates")
	if !ok {
		t.Fatal("FirstOf(updates) found nothing in the kind 1 sequence")
	}
	q, err := stmt.LookupErr("q")
	if err != nil {
		t.Fatalf("statement has no q: %v", err)
	}
	id, err := q.Document().LookupErr("_id")
	if err != nil {
		t.Fatalf("filter has no _id: %v", err)
	}
	if id.Int32() != 7 {
		t.Errorf("_id = %v, want 7", id.Int32())
	}
}

// The same command can arrive with the array inline instead. Both shapes have
// to work or write misses are missed for half of all clients.
func TestFirstOfFindsInlineWrites(t *testing.T) {
	raw := buildMsg(t, bson.D{
		{Key: "delete", Value: "orders"},
		{Key: "deletes", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: 9}}},
			{Key: "limit", Value: 1},
		}}},
		{Key: "$db", Value: "app"},
	}, nil)

	msg, err := Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := msg.CountOf("deletes"); got != 1 {
		t.Fatalf("CountOf(deletes) = %d, want 1", got)
	}
	stmt, ok := msg.FirstOf("deletes")
	if !ok {
		t.Fatal("FirstOf(deletes) found nothing inline")
	}
	if _, err := stmt.LookupErr("q"); err != nil {
		t.Errorf("statement has no q: %v", err)
	}
}

func TestNewMsgRoundTrips(t *testing.T) {
	msg, err := NewMsg(11, 22, bson.D{{Key: "ok", Value: 1.0}})
	if err != nil {
		t.Fatalf("NewMsg: %v", err)
	}

	back, err := Read(bytes.NewReader(msg.Raw))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if back.Header.ResponseTo != 22 {
		t.Errorf("responseTo = %d, want 22: a driver matches replies to requests by this",
			back.Header.ResponseTo)
	}
	doc, err := back.ReplyDoc()
	if err != nil {
		t.Fatalf("ReplyDoc: %v", err)
	}
	if !replyIsOK(doc) {
		t.Error("round tripped reply lost its ok field")
	}
}

func replyIsOK(doc bson.Raw) bool {
	v, err := doc.LookupErr("ok")
	return err == nil && v.Double() == 1
}

// A fire-and-forget write expects no reply. Sending one leaves the connection a
// message out of step for everything that follows on it.
func TestMoreToCome(t *testing.T) {
	raw := buildMsg(t, bson.D{{Key: "insert", Value: "orders"}}, nil)
	binary.LittleEndian.PutUint32(raw[HeaderLen:HeaderLen+4], 1<<1)

	msg, err := Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !msg.MoreToCome() {
		t.Error("MoreToCome = false, want true")
	}

	plain := buildMsg(t, bson.D{{Key: "insert", Value: "orders"}}, nil)
	msg, _ = Read(bytes.NewReader(plain))
	if msg.MoreToCome() {
		t.Error("MoreToCome = true on a normal message")
	}
}

// The length prefix is four bytes a client chooses, and Read allocates what it
// says before it has seen a byte of the body. The guard is what stops that.
//
// Asserting only that some error came back is not enough, and this test used to
// do exactly that. Remove the upper bound and it still passed: Read allocated
// the declared gigabyte, then failed reading the body out of a sixteen-byte
// reader, and an error is an error. The guard was gone and the check was green.
//
// So the error has to be the guard's own, by its words, and the body has to be
// available: readerOf gives Read as many bytes as it asks for, so a short read
// cannot stand in for a refusal. Without the bound this allocates whatever the
// header claims, which is the failure being guarded against rather than a test
// artifact -- 1 GB here, and 2 GB is what a client can ask for.
func TestReadRejectsImplausibleLength(t *testing.T) {
	for _, c := range []struct {
		name   string
		length uint32
	}{
		{"a gigabyte", 1 << 30},
		{"just past the cap", uint32(maxMessageLen) + 1},
		{"shorter than its own header", 4},
		{"nothing at all", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			head := make([]byte, HeaderLen)
			binary.LittleEndian.PutUint32(head[0:4], c.length)

			_, err := Read(readerOf(head))
			if err == nil {
				t.Fatalf("Read accepted a declared length of %d", c.length)
			}
			if !strings.Contains(err.Error(), "implausible message length") {
				t.Fatalf("rejected for the wrong reason: %v\n"+
					"the length guard has to refuse this before anything is allocated", err)
			}
		})
	}

	// And a length the cap allows is not refused by it, or the guard could be
	// tightened to nothing and still pass everything above.
	head := make([]byte, HeaderLen)
	binary.LittleEndian.PutUint32(head[0:4], uint32(maxMessageLen))
	if _, err := Read(readerOf(head)); err != nil && strings.Contains(err.Error(), "implausible") {
		t.Errorf("the largest allowed message was refused as implausible: %v", err)
	}
}

// readerOf returns the header followed by as many zero bytes as are asked for,
// so Read never fails for want of a body. A bytes.Reader would run out, and a
// short read is the wrong reason for this test to pass.
func readerOf(head []byte) io.Reader {
	return io.MultiReader(bytes.NewReader(head), zeroes{})
}

type zeroes struct{}

func (zeroes) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// A legacy OP_QUERY names its database in the collection rather than in $db, so
// reading only $db left it empty.
//
// That is not cosmetic. The router builds a namespace as db + "." + coll and
// asks whether it is tiered; an empty database makes that ".orders", which
// nothing manages, so the completeness guard and the copy-up were skipped and
// the fast tier's partial answer went back as success.
func TestOpQueryCarriesItsDatabaseInTheCollectionName(t *testing.T) {
	cmd, err := bson.Marshal(bson.D{
		{Key: "count", Value: "orders"},
		{Key: "query", Value: bson.D{}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var body []byte
	body = binary.LittleEndian.AppendUint32(body, 0) // flags
	body = append(body, []byte("shop.$cmd")...)      // fullCollectionName
	body = append(body, 0)
	body = binary.LittleEndian.AppendUint32(body, 0) // numberToSkip
	body = binary.LittleEndian.AppendUint32(body, 1) // numberToReturn
	body = append(body, cmd...)

	raw := make([]byte, HeaderLen)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(HeaderLen+len(body)))
	binary.LittleEndian.PutUint32(raw[12:16], uint32(OpQuery))
	raw = append(raw, body...)

	m := &Message{Raw: raw}
	m.Header.OpCode = OpQuery

	name, db, err := m.CommandName()
	if err != nil {
		t.Fatalf("CommandName: %v", err)
	}
	if name != "count" {
		t.Fatalf("name = %q, want count", name)
	}
	if db != "shop" {
		t.Fatalf("db = %q, want shop; an empty database makes every tiering guard skip the namespace", db)
	}
}

// A frame whose header is well formed and whose body is not.
//
// Length has to be at least HeaderLen to get past Read, so HeaderLen exactly is
// a legal frame carrying no body at all: the shortest thing a client can send
// that Read will hand on. Everything downstream then indexes into a body that
// is not there, and the bounds checks are what stand between that and an
// index-out-of-range panic in a proxy holding somebody's connection.
//
// Both survived being removed. Nothing built a frame this short, because
// buildMsg always writes a body.
func TestTruncatedFrameIsNotIndexedInto(t *testing.T) {
	for _, c := range []struct {
		name string
		body []byte
	}{
		{"no body at all", nil},
		{"one byte of flags", []byte{0}},
		{"three bytes of flags", []byte{0, 0, 0}},
		{"flags but no section", []byte{0, 0, 0, 0}},
		{"a section kind and nothing else", []byte{0, 0, 0, 0, 0}},
	} {
		t.Run(c.name, func(t *testing.T) {
			raw := make([]byte, HeaderLen+len(c.body))
			binary.LittleEndian.PutUint32(raw[0:4], uint32(len(raw)))
			binary.LittleEndian.PutUint32(raw[12:16], uint32(OpMsg))
			copy(raw[HeaderLen:], c.body)

			msg, err := Read(bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("Read refused a frame of its own declared length: %v", err)
			}

			// None of these may panic. What they answer matters less than that
			// they answer: a truncated frame is a client's to send.
			_ = msg.MoreToCome()
			_ = msg.ExhaustAllowed()
			if _, _, err := msg.CommandName(); err == nil {
				t.Errorf("CommandName found a command in %d bytes of body", len(c.body))
			}
			if _, err := msg.CommandDoc(); err == nil {
				t.Errorf("CommandDoc found a document in %d bytes of body", len(c.body))
			}
		})
	}
}
