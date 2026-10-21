// Package wire reads and writes the MongoDB wire protocol.
//
// It does the minimum a proxy needs: frame messages, find the command name, and
// rewrite a reply when necessary. Whole messages are forwarded as the bytes
// they arrived as, because decoding every document would be the single largest
// cost in a component that sits in front of every query.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Opcodes this proxy understands.
const (
	OpReply = 1
	OpQuery = 2004
	OpMsg   = 2013
	// OpCompressed wraps another message. Nothing here can read one, which is
	// why the router refuses to negotiate compression at all.
	OpCompressed = 2012
)

// HeaderLen is the fixed size of a wire protocol message header.
const HeaderLen = 16

// maxMessageLen guards against a malformed or hostile length prefix. The server
// caps messages at 48 MB; this leaves room and refuses anything absurd.
const maxMessageLen = 64 * 1024 * 1024

// Header is the fixed prefix on every message.
type Header struct {
	Length     int32
	RequestID  int32
	ResponseTo int32
	OpCode     int32
}

// Message is one whole wire protocol message, header included, exactly as it
// arrived. Raw is the full frame so it can be forwarded untouched.
type Message struct {
	Header Header
	Raw    []byte
}

// Body returns the message payload after the header.
func (m *Message) Body() []byte { return m.Raw[HeaderLen:] }

// Read reads one whole message.
func Read(r io.Reader) (*Message, error) {
	var head [HeaderLen]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err
	}

	h := Header{
		Length:     int32(binary.LittleEndian.Uint32(head[0:4])),
		RequestID:  int32(binary.LittleEndian.Uint32(head[4:8])),
		ResponseTo: int32(binary.LittleEndian.Uint32(head[8:12])),
		OpCode:     int32(binary.LittleEndian.Uint32(head[12:16])),
	}
	if h.Length < HeaderLen || h.Length > maxMessageLen {
		return nil, fmt.Errorf("wire: implausible message length %d", h.Length)
	}

	raw := make([]byte, h.Length)
	copy(raw, head[:])
	if _, err := io.ReadFull(r, raw[HeaderLen:]); err != nil {
		return nil, err
	}
	return &Message{Header: h, Raw: raw}, nil
}

// Write sends a message.
func Write(w io.Writer, m *Message) error {
	_, err := w.Write(m.Raw)
	return err
}

// ErrNotCommand is returned when a message carries no command document.
var ErrNotCommand = errors.New("wire: message has no command document")

// OP_MSG flag bits this proxy cares about.
const (
	flagChecksumPresent = 1 << 0
	flagMoreToCome      = 1 << 1
)

// MoreToCome reports whether the sender expects no reply. Fire-and-forget
// writes set this, and replying to one desynchronises the connection.
func (m *Message) MoreToCome() bool {
	if m.Header.OpCode != OpMsg || len(m.Raw) < HeaderLen+4 {
		return false
	}
	flags := binary.LittleEndian.Uint32(m.Raw[HeaderLen : HeaderLen+4])
	return flags&flagMoreToCome != 0
}

// CommandDoc returns the kind 0 section, which holds the command itself.
//
// Payloads split across kind 1 sections (the documents of an insert, the
// updates of an update) are left where they are. Nothing this proxy decides
// needs them, and pulling them together would mean copying the bulk of every
// write that passes through.
func (m *Message) CommandDoc() (bson.Raw, error) {
	if m.Header.OpCode == OpQuery {
		return opQueryCommand(m)
	}
	if m.Header.OpCode != OpMsg {
		return nil, ErrNotCommand
	}

	body := m.Body()
	if len(body) < 4 {
		return nil, ErrNotCommand
	}
	flags := binary.LittleEndian.Uint32(body[0:4])

	end := len(body)
	if flags&flagChecksumPresent != 0 {
		if end < 4+4 {
			return nil, ErrNotCommand
		}
		end -= 4
	}

	pos := 4
	for pos < end {
		kind := body[pos]
		pos++
		switch kind {
		case 0:
			doc, err := readDoc(body, pos, end)
			if err != nil {
				return nil, err
			}
			return doc, nil
		case 1:
			if pos+4 > end {
				return nil, ErrNotCommand
			}
			size := int(int32(binary.LittleEndian.Uint32(body[pos : pos+4])))
			if size < 4 || pos+size > end {
				return nil, ErrNotCommand
			}
			pos += size
		default:
			return nil, fmt.Errorf("wire: unknown section kind %d", kind)
		}
	}
	return nil, ErrNotCommand
}

func readDoc(body []byte, pos, end int) (bson.Raw, error) {
	if pos+4 > end {
		return nil, ErrNotCommand
	}
	size := int(int32(binary.LittleEndian.Uint32(body[pos : pos+4])))
	if size < 5 || pos+size > end {
		return nil, fmt.Errorf("wire: command document runs past the message")
	}
	return bson.Raw(body[pos : pos+size]), nil
}

// CommandName returns the command's name, which is the first field of the
// command document, and the database it targets.
func (m *Message) CommandName() (name, db string, err error) {
	doc, err := m.CommandDoc()
	if err != nil {
		return "", "", err
	}
	elems, err := doc.Elements()
	if err != nil || len(elems) == 0 {
		return "", "", ErrNotCommand
	}
	name = elems[0].Key()

	if v, lookupErr := doc.LookupErr("$db"); lookupErr == nil {
		db, _ = v.StringValueOK()
	}
	// A legacy OP_QUERY does not carry $db. It names the database in the
	// collection instead, as "shop.$cmd", and reading only $db therefore left
	// the database empty for every command that arrived that way.
	//
	// That matters because the router builds a namespace as db + "." + coll and
	// asks whether it is tiered. An empty database makes that ".orders", which
	// is managed by nothing, so the completeness guard, the copy-up and the
	// unique key checks were all skipped and the fast tier's partial answer went
	// back as success. The authorization check fails closed on the same input,
	// which is the right way round but hides the problem rather than showing it.
	if db == "" && m.Header.OpCode == OpQuery {
		if full, ok := m.OpQueryCollection(); ok {
			if i := strings.IndexByte(full, '.'); i > 0 {
				db = full[:i]
			}
		}
	}
	return name, db, nil
}

// NewMsg builds an OP_MSG carrying one document, in reply to responseTo.
func NewMsg(requestID, responseTo int32, doc bson.D) (*Message, error) {
	payload, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("wire: marshal reply: %w", err)
	}
	return NewMsgRaw(requestID, responseTo, payload), nil
}

// NewMsgRaw builds an OP_MSG carrying a document already encoded.
func NewMsgRaw(requestID, responseTo int32, payload bson.Raw) *Message {

	length := HeaderLen + 4 + 1 + len(payload)
	raw := make([]byte, length)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(length))
	binary.LittleEndian.PutUint32(raw[4:8], uint32(requestID))
	binary.LittleEndian.PutUint32(raw[8:12], uint32(responseTo))
	binary.LittleEndian.PutUint32(raw[12:16], uint32(OpMsg))
	binary.LittleEndian.PutUint32(raw[16:20], 0) // flags
	raw[20] = 0                                  // section kind 0
	copy(raw[21:], payload)

	return &Message{
		Header: Header{Length: int32(length), RequestID: requestID, ResponseTo: responseTo, OpCode: OpMsg},
		Raw:    raw,
	}
}

// ReplyDoc returns the single document carried by an OP_MSG or OP_REPLY.
func (m *Message) ReplyDoc() (bson.Raw, error) {
	switch m.Header.OpCode {
	case OpMsg:
		return m.CommandDoc()
	case OpReply:
		// int32 flags, int64 cursorID, int32 startingFrom, int32 numberReturned
		const preamble = 4 + 8 + 4 + 4
		body := m.Body()
		if len(body) < preamble+5 {
			return nil, ErrNotCommand
		}
		return readDoc(body, preamble, len(body))
	default:
		return nil, ErrNotCommand
	}
}

// flagExhaustAllowed tells the server the sender will accept several replies to
// one request, each but the last carrying moreToCome.
const flagExhaustAllowed = 1 << 16

// ExhaustAllowed reports whether the sender asked for exhaust replies.
func (m *Message) ExhaustAllowed() bool {
	if m.Header.OpCode != OpMsg || len(m.Raw) < HeaderLen+4 {
		return false
	}
	flags := binary.LittleEndian.Uint32(m.Raw[HeaderLen : HeaderLen+4])
	return flags&flagExhaustAllowed != 0
}

// ClearExhaustAllowed returns the message with the exhaust bit cleared, so the
// server answers it once.
//
// A server sends multiple replies to one request only when this bit is set, and
// this proxy reads exactly one reply per request. The rest stay in the socket,
// and every later command on that connection is answered by the one before it.
// Measured against a real mongod: one getMore with the bit set produced five
// replies, and through the router the answer to the following ping was a batch
// of documents from the cursor. With upstream connections pooled, that
// connection then carries another client's documents to whoever borrows it
// next.
//
// So the bit is cleared on the way through, which the wire protocol allows:
// setting it never guaranteed multiple replies. An exhaust-capable driver loses
// a round-trip optimisation and gets the same documents. This is the same
// remedy as stripping the compressor list, and for the same reason: a flag the
// client sets for itself must not decide whether this proxy can still follow
// the conversation.
//
// A checksum covers the flags, so one present is removed along with the bit.
// mongod accepts a message without one, and the router is a new hop that never
// forwarded the client's bytes unchanged anyway.
func (m *Message) ClearExhaustAllowed() *Message {
	if !m.ExhaustAllowed() {
		return m
	}
	raw := make([]byte, len(m.Raw))
	copy(raw, m.Raw)

	flags := binary.LittleEndian.Uint32(raw[HeaderLen : HeaderLen+4])
	flags &^= flagExhaustAllowed
	if flags&flagChecksumPresent != 0 && len(raw) >= HeaderLen+4+4 {
		flags &^= flagChecksumPresent
		raw = raw[:len(raw)-4]
		binary.LittleEndian.PutUint32(raw[0:4], uint32(len(raw)))
	}
	binary.LittleEndian.PutUint32(raw[HeaderLen:HeaderLen+4], flags)

	out := *m
	out.Raw = raw
	return &out
}
