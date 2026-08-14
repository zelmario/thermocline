package wire

import (
	"bytes"
	"encoding/binary"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Payloads returns the kind 1 document sequences, keyed by identifier.
//
// Drivers move bulk arrays out of the command document and into these
// sequences: the documents of an insert, the updates of an update, the deletes
// of a delete. A proxy that only reads the command document sees an update
// command with no updates in it, so anything inspecting a write has to look in
// both places.
func (m *Message) Payloads() map[string][]bson.Raw {
	if m.Header.OpCode != OpMsg {
		return nil
	}
	body := m.Body()
	if len(body) < 4 {
		return nil
	}
	flags := binary.LittleEndian.Uint32(body[0:4])

	end := len(body)
	if flags&flagChecksumPresent != 0 {
		if end < 8 {
			return nil
		}
		end -= 4
	}

	var out map[string][]bson.Raw
	pos := 4
	for pos < end {
		kind := body[pos]
		pos++
		switch kind {
		case 0:
			doc, err := readDoc(body, pos, end)
			if err != nil {
				return out
			}
			pos += len(doc)
		case 1:
			if pos+4 > end {
				return out
			}
			size := int(int32(binary.LittleEndian.Uint32(body[pos : pos+4])))
			if size < 4 || pos+size > end {
				return out
			}
			secEnd := pos + size
			p := pos + 4

			nul := bytes.IndexByte(body[p:secEnd], 0)
			if nul < 0 {
				return out
			}
			id := string(body[p : p+nul])
			p += nul + 1

			var docs []bson.Raw
			for p < secEnd {
				doc, err := readDoc(body, p, secEnd)
				if err != nil {
					break
				}
				docs = append(docs, doc)
				p += len(doc)
			}
			if out == nil {
				out = make(map[string][]bson.Raw, 1)
			}
			out[id] = docs
			pos = secEnd
		default:
			return out
		}
	}
	return out
}

// FirstOf returns the first document of a bulk array, whether the driver put it
// in the command document or in a kind 1 sequence.
func (m *Message) FirstOf(field string) (bson.Raw, bool) {
	if payloads := m.Payloads(); payloads != nil {
		if docs, ok := payloads[field]; ok && len(docs) > 0 {
			return docs[0], true
		}
	}

	doc, err := m.CommandDoc()
	if err != nil {
		return nil, false
	}
	val, err := doc.LookupErr(field)
	if err != nil {
		return nil, false
	}
	arr, ok := val.ArrayOK()
	if !ok {
		return nil, false
	}
	vals, err := arr.Values()
	if err != nil || len(vals) == 0 {
		return nil, false
	}
	sub, ok := vals[0].DocumentOK()
	if !ok {
		return nil, false
	}
	return sub, true
}

// AllOf returns every document of a bulk array, from wherever the driver put
// it. Needed by anything that has to inspect a whole batch rather than decide
// on its first statement, such as checking a multi-document insert for keys
// that are already taken.
func (m *Message) AllOf(field string) []bson.Raw {
	if payloads := m.Payloads(); payloads != nil {
		if docs, ok := payloads[field]; ok {
			return docs
		}
	}

	doc, err := m.CommandDoc()
	if err != nil {
		return nil
	}
	val, err := doc.LookupErr(field)
	if err != nil {
		return nil
	}
	arr, ok := val.ArrayOK()
	if !ok {
		return nil
	}
	vals, err := arr.Values()
	if err != nil {
		return nil
	}
	out := make([]bson.Raw, 0, len(vals))
	for _, v := range vals {
		if sub, ok := v.DocumentOK(); ok {
			out = append(out, sub)
		}
	}
	return out
}

// CountOf reports how many documents a bulk array holds, from either place.
func (m *Message) CountOf(field string) int {
	if payloads := m.Payloads(); payloads != nil {
		if docs, ok := payloads[field]; ok {
			return len(docs)
		}
	}
	doc, err := m.CommandDoc()
	if err != nil {
		return 0
	}
	val, err := doc.LookupErr(field)
	if err != nil {
		return 0
	}
	arr, ok := val.ArrayOK()
	if !ok {
		return 0
	}
	vals, err := arr.Values()
	if err != nil {
		return 0
	}
	return len(vals)
}
