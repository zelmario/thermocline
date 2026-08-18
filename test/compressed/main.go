// A client that compresses without being invited to.
//
// The handshake never agrees a compressor: the router removes the list from the
// client's hello and from the reply, in both directions, so a conforming driver
// never sends OP_COMPRESSED. This asks what happens when one arrives anyway,
// because the router cannot read it and forwards it, and everything it forwards
// unread is a command with no guard on it.
//
// The test is an insert of an _id the archive still holds. Sent plainly it is
// refused, because the fast tier cannot see the archived document and the guard
// can. Sent compressed, nothing reads it.
package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	opMsg        = 2013
	opCompressed = 2012
	headerLen    = 16
)

func frame(reqID int32, opcode int32, body []byte) []byte {
	total := headerLen + len(body)
	buf := make([]byte, 0, total)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(total))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(reqID))
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(opcode))
	return append(buf, body...)
}

func msgBody(doc bson.D) []byte {
	b, err := bson.Marshal(doc)
	if err != nil {
		panic(err)
	}
	body := binary.LittleEndian.AppendUint32(nil, 0)
	body = append(body, 0)
	return append(body, b...)
}

// compressedFrame wraps an OP_MSG body in OP_COMPRESSED with zlib, which is
// compressor id 2.
func compressedFrame(reqID int32, inner []byte) []byte {
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(inner); err != nil {
		panic(err)
	}
	_ = w.Close()

	body := binary.LittleEndian.AppendUint32(nil, uint32(opMsg))
	body = binary.LittleEndian.AppendUint32(body, uint32(len(inner)))
	body = append(body, 2)
	body = append(body, buf.Bytes()...)
	return frame(reqID, opCompressed, body)
}

func read(c net.Conn) (int32, bson.Raw, error) {
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	head := make([]byte, headerLen)
	if _, err := io.ReadFull(c, head); err != nil {
		return 0, nil, err
	}
	total := int(binary.LittleEndian.Uint32(head[0:4]))
	opcode := int32(binary.LittleEndian.Uint32(head[12:16]))
	rest := make([]byte, total-headerLen)
	if _, err := io.ReadFull(c, rest); err != nil {
		return 0, nil, err
	}
	if opcode == opCompressed {
		if len(rest) < 9 {
			return opcode, nil, fmt.Errorf("short compressed frame")
		}
		zr, err := zlib.NewReader(bytes.NewReader(rest[9:]))
		if err != nil {
			return opcode, nil, err
		}
		plain, err := io.ReadAll(zr)
		if err != nil {
			return opcode, nil, err
		}
		return opcode, bson.Raw(plain[5:]), nil
	}
	return opcode, bson.Raw(rest[5:]), nil
}

func main() {
	addr := os.Args[1]
	c, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("dial:", err)
		os.Exit(1)
	}
	defer c.Close()

	// Ask for compression in the handshake, so the reply says whether the peer
	// agreed. The router removes the list in both directions, so it should not.
	if _, err := c.Write(frame(1, opMsg, msgBody(bson.D{
		{Key: "hello", Value: 1},
		{Key: "compression", Value: bson.A{"zlib"}},
		{Key: "client", Value: bson.D{
			{Key: "driver", Value: bson.D{{Key: "name", Value: "compressprobe"}, {Key: "version", Value: "1"}}},
			{Key: "os", Value: bson.D{{Key: "type", Value: "Linux"}}}}},
		{Key: "$db", Value: "admin"},
	}))); err != nil {
		fmt.Println("hello:", err)
		os.Exit(1)
	}
	_, r, err := read(c)
	if err != nil {
		fmt.Println("hello reply:", err)
		os.Exit(1)
	}
	agreed := "none"
	if v, e := r.LookupErr("compression"); e == nil {
		agreed = v.String()
	}
	fmt.Printf("compressors the peer agreed to: %s\n", agreed)

	// Plainly first: this is the control, and it must be refused.
	if _, err := c.Write(frame(2, opMsg, msgBody(bson.D{
		{Key: "insert", Value: "tiered"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: 5}, {Key: "via", Value: "plain"}}}},
		{Key: "$db", Value: "probe"},
	}))); err != nil {
		fmt.Println("plain insert:", err)
		os.Exit(1)
	}
	_, r, err = read(c)
	if err != nil {
		fmt.Println("plain reply:", err)
		os.Exit(1)
	}
	fmt.Printf("plain insert:      %s\n", verdict(r))

	// And now compressed, which nothing here can read.
	inner := msgBody(bson.D{
		{Key: "insert", Value: "tiered"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: 6}, {Key: "via", Value: "zlib"}}}},
		{Key: "$db", Value: "probe"},
	})
	if _, err := c.Write(compressedFrame(3, inner)); err != nil {
		fmt.Println("compressed insert:", err)
		os.Exit(1)
	}
	op, r, err := read(c)
	if err != nil {
		fmt.Printf("compressed insert: no reply (%v)\n", err)
		return
	}
	fmt.Printf("compressed insert: %s (reply opcode %d)\n", verdict(r), op)
}

func verdict(r bson.Raw) string {
	if v, e := r.LookupErr("writeErrors"); e == nil {
		return "refused: " + v.String()[:120]
	}
	if v, e := r.LookupErr("errmsg"); e == nil {
		return "refused: " + v.String()
	}
	if v, e := r.LookupErr("n"); e == nil {
		return "ACCEPTED n=" + v.String()
	}
	return r.String()
}
