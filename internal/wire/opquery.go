package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// opQueryCommand extracts the command from a legacy OP_QUERY.
//
// Drivers still open a connection with an OP_QUERY hello, because until the
// server answers they do not know what it supports. Everything after the
// handshake is OP_MSG, so this exists purely so the first message on a
// connection can be understood.
//
// Layout: int32 flags, cstring fullCollectionName, int32 numberToSkip,
// int32 numberToReturn, document query.
func opQueryCommand(m *Message) (bson.Raw, error) {
	body := m.Body()
	if len(body) < 4 {
		return nil, ErrNotCommand
	}
	pos := 4

	nul := bytes.IndexByte(body[pos:], 0)
	if nul < 0 {
		return nil, ErrNotCommand
	}
	pos += nul + 1

	pos += 8 // numberToSkip, numberToReturn
	if pos > len(body) {
		return nil, ErrNotCommand
	}
	return readDoc(body, pos, len(body))
}

// OpQueryCollection returns the fullCollectionName of a legacy query, such as
// "admin.$cmd".
func (m *Message) OpQueryCollection() (string, bool) {
	if m.Header.OpCode != OpQuery {
		return "", false
	}
	body := m.Body()
	if len(body) < 5 {
		return "", false
	}
	nul := bytes.IndexByte(body[4:], 0)
	if nul < 0 {
		return "", false
	}
	return string(body[4 : 4+nul]), true
}

// NewReply builds an OP_REPLY carrying one document, which is what a legacy
// OP_QUERY handshake expects back.
func NewReply(requestID, responseTo int32, doc bson.D) (*Message, error) {
	payload, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("wire: marshal reply: %w", err)
	}

	const preamble = 4 + 8 + 4 + 4
	length := HeaderLen + preamble + len(payload)
	raw := make([]byte, length)

	binary.LittleEndian.PutUint32(raw[0:4], uint32(length))
	binary.LittleEndian.PutUint32(raw[4:8], uint32(requestID))
	binary.LittleEndian.PutUint32(raw[8:12], uint32(responseTo))
	binary.LittleEndian.PutUint32(raw[12:16], uint32(OpReply))

	binary.LittleEndian.PutUint32(raw[16:20], 0)         // responseFlags
	binary.LittleEndian.PutUint64(raw[20:28], 0)         // cursorID
	binary.LittleEndian.PutUint32(raw[28:32], 0)         // startingFrom
	binary.LittleEndian.PutUint32(raw[32:36], uint32(1)) // numberReturned
	copy(raw[36:], payload)

	return &Message{
		Header: Header{Length: int32(length), RequestID: requestID, ResponseTo: responseTo, OpCode: OpReply},
		Raw:    raw,
	}, nil
}
