package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// MaxDepth is the deepest nesting of documents and arrays a message may carry.
//
// Everything that inspects a command walks it recursively: the query shape
// advisor, the reserved field check, the multikey check, the hello rewrite.
// None of them had a limit, and a message nested a few million levels deep fits
// in the 64 MB frame. Walking it overflows the goroutine's stack, and Go treats
// that as a fatal error rather than a panic, so the recover in serve does not
// catch it and the whole router exits. One unauthenticated message took every
// client down with it.
//
// So depth is checked once, here, before any guard reads the message. mongod
// refuses such documents itself, at maxBSONDepth (200 by default), but only
// after the router has walked them. The limit is 1000, the highest value
// maxBSONDepth accepts, so nothing a mongod could be configured to accept is
// refused here, and every walker stays within a few hundred kilobytes of stack.
const MaxDepth = 1000

// CheckDepth returns an error if any document in the message nests deeper than
// MaxDepth: the command document, every document of a kind 1 sequence, and a
// legacy query's filter and field selector.
func (m *Message) CheckDepth() error {
	for _, doc := range m.documents() {
		if d := depthOver(doc, MaxDepth); d > 0 {
			return fmt.Errorf("wire: document nested deeper than %d levels", MaxDepth)
		}
	}
	return nil
}

// documents returns every document the message carries, as raw bytes. A
// malformed frame yields what could be read before the fault; mongod refuses
// the rest.
func (m *Message) documents() [][]byte {
	body := m.Body()
	var out [][]byte
	switch m.Header.OpCode {
	case OpQuery:
		if len(body) < 4 {
			return nil
		}
		pos := 4
		nul := bytes.IndexByte(body[pos:], 0)
		if nul < 0 {
			return nil
		}
		pos += nul + 1 + 8
		// The query, then an optional returnFieldsSelector.
		for pos < len(body) {
			doc, err := readDoc(body, pos, len(body))
			if err != nil {
				break
			}
			out = append(out, doc)
			pos += len(doc)
		}
	case OpMsg:
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
				out = append(out, doc)
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
				p += nul + 1
				for p < secEnd {
					doc, err := readDoc(body, p, secEnd)
					if err != nil {
						break
					}
					out = append(out, doc)
					p += len(doc)
				}
				pos = secEnd
			default:
				return out
			}
		}
	}
	return out
}

// depthOver walks one BSON document without recursion and returns the depth it
// reached once that passed limit, or 0 if it never did. The top level is depth
// 1. A malformed document stops the walk early and counts as within the limit:
// it is not this check's to refuse, and every reader after it rejects it.
func depthOver(doc []byte, limit int) int {
	if len(doc) < 5 {
		return 0
	}
	// ends holds where each open document ends, innermost last.
	ends := make([]int, 1, 16)
	ends[0] = int(int32(binary.LittleEndian.Uint32(doc[0:4])))
	if ends[0] > len(doc) || ends[0] < 5 {
		return 0
	}
	pos := 4
	for len(ends) > 0 {
		end := ends[len(ends)-1]
		if pos >= end {
			return 0
		}
		t := doc[pos]
		pos++
		if t == 0 {
			ends = ends[:len(ends)-1]
			continue
		}
		nul := bytes.IndexByte(doc[pos:end], 0)
		if nul < 0 {
			return 0
		}
		pos += nul + 1

		// n is the value's length. A length prefix that does not fit leaves it
		// negative, which ends the walk below.
		n := -1
		switch t {
		case 0x03, 0x04: // document, array
			if pos+4 > end {
				return 0
			}
			size := int(int32(binary.LittleEndian.Uint32(doc[pos : pos+4])))
			if size < 5 || pos+size > end {
				return 0
			}
			if len(ends)+1 > limit {
				return len(ends) + 1
			}
			ends = append(ends, pos+size)
			pos += 4
			continue
		case 0x01, 0x09, 0x11, 0x12: // double, datetime, timestamp, int64
			n = 8
		case 0x02, 0x0D, 0x0E: // string, javascript, symbol
			if l := lenAt(doc, pos, end); l >= 0 {
				n = 4 + l
			}
		case 0x05: // binary: length, subtype, bytes
			if l := lenAt(doc, pos, end); l >= 0 {
				n = 5 + l
			}
		case 0x06, 0x0A, 0x7F, 0xFF: // undefined, null, maxKey, minKey
			n = 0
		case 0x07: // objectId
			n = 12
		case 0x08: // bool
			n = 1
		case 0x0B: // regex: pattern and options, two cstrings
			n = 0
			for i := 0; i < 2; i++ {
				z := bytes.IndexByte(doc[pos+n:end], 0)
				if z < 0 {
					return 0
				}
				n += z + 1
			}
		case 0x0C: // dbPointer
			if l := lenAt(doc, pos, end); l >= 0 {
				n = 4 + l + 12
			}
		case 0x0F: // code with scope: a total length covers both parts
			n = lenAt(doc, pos, end)
		case 0x10: // int32
			n = 4
		case 0x13: // decimal128
			n = 16
		default:
			return 0
		}
		if n < 0 || pos+n > end {
			return 0
		}
		pos += n
	}
	return 0
}

// lenAt reads the int32 length prefix at pos, or -1 if it does not fit.
func lenAt(doc []byte, pos, end int) int {
	if pos+4 > end {
		return -1
	}
	return int(int32(binary.LittleEndian.Uint32(doc[pos : pos+4])))
}
