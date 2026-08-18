// A raw wire protocol probe for the OP_MSG exhaustAllowed flag.
//
// The Go driver does not expose exhaust cursors, so the only way to find out
// what a server, or a proxy in front of one, does with the flag is to speak the
// protocol directly. This connects, seeds a collection, opens a cursor, sends a
// getMore with exhaustAllowed set, and then sends a ping.
//
// The ping is the whole point. If the peer answered the getMore with more than
// one message and only one was read, the next reply on that connection is the
// leftover, and the ping's answer is somebody else's documents.
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
	opMsg              = 2013
	headerLen          = 16
	flagExhaustAllowed = 1 << 16
	flagMoreToCome     = 1 << 1
)

func send(c net.Conn, reqID int32, flags uint32, doc bson.D) error {
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
	buf = binary.LittleEndian.AppendUint32(buf, flags)
	buf = append(buf, 0) // section kind 0
	buf = append(buf, body...)
	_, err = c.Write(buf)
	return err
}

type reply struct {
	responseTo int32
	moreToCome bool
	doc        bson.Raw
}

func read(c net.Conn, wait time.Duration) (*reply, error) {
	_ = c.SetReadDeadline(time.Now().Add(wait))
	head := make([]byte, headerLen)
	if _, err := io.ReadFull(c, head); err != nil {
		return nil, err
	}
	total := int(binary.LittleEndian.Uint32(head[0:4]))
	responseTo := int32(binary.LittleEndian.Uint32(head[8:12]))
	rest := make([]byte, total-headerLen)
	if _, err := io.ReadFull(c, rest); err != nil {
		return nil, err
	}
	flags := binary.LittleEndian.Uint32(rest[0:4])
	// Section kind 0 at rest[4], document from rest[5].
	return &reply{
		responseTo: responseTo,
		moreToCome: flags&flagMoreToCome != 0,
		doc:        bson.Raw(rest[5:]),
	}, nil
}

func first(doc bson.Raw, keys ...string) string {
	for _, k := range keys {
		if v, err := doc.LookupErr(k); err == nil {
			return k + "=" + v.String()
		}
	}
	return "?"
}

func main() {
	addr := os.Args[1]
	label := os.Args[2]
	c, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("dial:", err)
		os.Exit(1)
	}
	defer c.Close()

	must := func(step string, err error) {
		if err != nil {
			fmt.Printf("%s: %s: %v\n", label, step, err)
			os.Exit(1)
		}
	}

	// Handshake.
	must("hello", send(c, 1, 0, bson.D{
		{Key: "hello", Value: 1},
		{Key: "client", Value: bson.D{{Key: "application", Value: bson.D{{Key: "name", Value: "exhaustprobe"}}},
			{Key: "driver", Value: bson.D{{Key: "name", Value: "exhaustprobe"}, {Key: "version", Value: "1"}}},
			{Key: "os", Value: bson.D{{Key: "type", Value: "Linux"}}}}},
		{Key: "$db", Value: "admin"},
	}))
	_, err = read(c, 5*time.Second)
	must("hello reply", err)

	// Seed, ten documents so one cursor needs several batches.
	docs := bson.A{}
	for i := 0; i < 10; i++ {
		docs = append(docs, bson.D{{Key: "_id", Value: i}, {Key: "v", Value: i}})
	}
	must("drop", send(c, 2, 0, bson.D{{Key: "drop", Value: "exh"}, {Key: "$db", Value: "probe"}}))
	_, _ = read(c, 5*time.Second)
	must("insert", send(c, 3, 0, bson.D{
		{Key: "insert", Value: "exh"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "probe"},
	}))
	r, err := read(c, 5*time.Second)
	must("insert reply", err)
	fmt.Printf("%s seeded: %s\n", label, first(r.doc, "n", "ok"))

	// Open a cursor two documents at a time.
	must("find", send(c, 4, 0, bson.D{
		{Key: "find", Value: "exh"}, {Key: "filter", Value: bson.D{}},
		{Key: "batchSize", Value: 2}, {Key: "$db", Value: "probe"},
	}))
	r, err = read(c, 5*time.Second)
	must("find reply", err)
	cur, err := r.doc.LookupErr("cursor", "id")
	must("cursor id", err)
	id, _ := cur.Int64OK()
	fmt.Printf("%s cursor id %d\n", label, id)

	// The getMore, with exhaustAllowed set.
	must("getMore", send(c, 5, flagExhaustAllowed, bson.D{
		{Key: "getMore", Value: id}, {Key: "collection", Value: "exh"},
		{Key: "batchSize", Value: 2}, {Key: "$db", Value: "probe"},
	}))
	extra := 0
	for {
		r, err = read(c, 2*time.Second)
		if err != nil {
			break
		}
		extra++
		fmt.Printf("%s   reply %d responseTo=%d moreToCome=%v %s\n",
			label, extra, r.responseTo, r.moreToCome, first(r.doc, "cursor", "ok"))
		if !r.moreToCome {
			break
		}
	}
	fmt.Printf("%s getMore produced %d reply/replies\n", label, extra)

	// And now the question that matters: is the connection still in step?
	must("ping", send(c, 6, 0, bson.D{{Key: "ping", Value: 1}, {Key: "$db", Value: "admin"}}))
	r, err = read(c, 5*time.Second)
	if err != nil {
		fmt.Printf("%s ping: %v\n", label, err)
		return
	}
	inStep := r.responseTo == 6
	fmt.Printf("%s ping reply responseTo=%d (want 6) inStep=%v %s\n",
		label, r.responseTo, inStep, first(r.doc, "cursor", "ok"))
}
