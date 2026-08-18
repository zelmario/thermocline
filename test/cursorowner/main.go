// Can one client read another client's cursor?
//
// The router serves getMore itself for cursors over archive answers, from a
// registry that belongs to the router rather than to a connection. That is
// deliberate: a real MongoDB cursor is server-scoped and a driver's pool issues
// the getMore from whichever connection is free. The question this asks is
// whether anything else scopes it, because the ids are handed out in sequence
// from a known starting point.
//
// Connection A opens a cursor over a collection whose documents are in the
// archive. Connection B, which never opened anything, sends a getMore for that
// id and names a different collection of its own.
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	opMsg     = 2013
	headerLen = 16
)

func send(c net.Conn, reqID int32, doc bson.D) error {
	body, err := bson.Marshal(doc)
	if err != nil {
		return err
	}
	total := headerLen + 4 + 1 + len(body)
	buf := make([]byte, 0, total)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(total))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(reqID))
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = binary.LittleEndian.AppendUint32(buf, opMsg)
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = append(buf, 0)
	buf = append(buf, body...)
	_, err = c.Write(buf)
	return err
}

func read(c net.Conn) (bson.Raw, error) {
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	head := make([]byte, headerLen)
	if _, err := io.ReadFull(c, head); err != nil {
		return nil, err
	}
	total := int(binary.LittleEndian.Uint32(head[0:4]))
	rest := make([]byte, total-headerLen)
	if _, err := io.ReadFull(c, rest); err != nil {
		return nil, err
	}
	return bson.Raw(rest[5:]), nil
}

func hello(c net.Conn) error {
	if err := send(c, 1, bson.D{{Key: "hello", Value: 1},
		{Key: "client", Value: bson.D{
			{Key: "driver", Value: bson.D{{Key: "name", Value: "stealprobe"}, {Key: "version", Value: "1"}}},
			{Key: "os", Value: bson.D{{Key: "type", Value: "Linux"}}}}},
		{Key: "$db", Value: "admin"}}); err != nil {
		return err
	}
	_, err := read(c)
	return err
}

func main() {
	addr := os.Args[1]
	a, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("dial A:", err)
		os.Exit(1)
	}
	defer a.Close()
	b, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("dial B:", err)
		os.Exit(1)
	}
	defer b.Close()
	if err := hello(a); err != nil {
		fmt.Println("hello A:", err)
		os.Exit(1)
	}
	if err := hello(b); err != nil {
		fmt.Println("hello B:", err)
		os.Exit(1)
	}

	// A opens a cursor over the tiered collection. Its documents are only in the
	// archive, so the router answers it and owns the cursor.
	if err := send(a, 10, bson.D{
		{Key: "find", Value: "tiered"}, {Key: "filter", Value: bson.D{}},
		{Key: "batchSize", Value: 2}, {Key: "$db", Value: "probe"},
	}); err != nil {
		fmt.Println("A find:", err)
		os.Exit(1)
	}
	r, err := read(a)
	if err != nil {
		fmt.Println("A find reply:", err)
		os.Exit(1)
	}
	idv, err := r.LookupErr("cursor", "id")
	if err != nil {
		fmt.Println("A got no cursor:", r.String()[:200])
		os.Exit(1)
	}
	id, _ := idv.Int64OK()
	fmt.Printf("A opened cursor %d (router owned: %v)\n", id, id >= 1<<62)

	// B never opened anything. It names the cursor's own collection, because
	// naming a different one is refused for a different reason: mongod requires
	// the getMore's namespace to be the cursor's, and the router does too. This
	// is about ownership on its own.
	if err := send(b, 20, bson.D{
		{Key: "getMore", Value: id}, {Key: "collection", Value: "tiered"},
		{Key: "batchSize", Value: 2}, {Key: "$db", Value: "probe"},
	}); err != nil {
		fmt.Println("B getMore:", err)
		os.Exit(1)
	}
	rb, err := read(b)
	if err != nil {
		fmt.Println("B getMore reply:", err)
		os.Exit(1)
	}
	if ok, oerr := rb.LookupErr("ok"); oerr == nil {
		if d, dok := ok.DoubleOK(); dok && d == 0 {
			msg, _ := rb.LookupErr("errmsg")
			code, _ := rb.LookupErr("code")
			fmt.Printf("B was refused: code=%s %s\n", code.String(), msg.String())
			return
		}
	}
	batch, berr := rb.LookupErr("cursor", "nextBatch")
	if berr != nil {
		fmt.Println("B got:", rb.String())
		return
	}
	ns, _ := rb.LookupErr("cursor", "ns")
	fmt.Printf("B READ ANOTHER CLIENT'S CURSOR: ns=%s batch=%s\n", ns.String(), batch.String())
}
