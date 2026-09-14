// A raw wire protocol probe for moreToCome on a command that returns data.
//
// The router treats moreToCome as "no reply is coming": it writes the message
// to the fast tier and returns without reading one. That is right for a write,
// which is the only thing any driver sets the flag on. It is right for anything
// else only if the peer also sends nothing.
//
// If mongod answers a moreToCome find, the reply stays in the socket and the
// answer to whatever command comes next is somebody else's documents. That is
// the exhaust finding's shape, and that one was real: one getMore with the
// exhaust bit set produced five replies from mongod, and the ping after it was
// answered with a batch of the cursor's documents.
//
// So this sends a find with moreToCome set, then a ping, and asks whether the
// ping's reply is the ping's. Run it against mongod directly to learn what
// mongod does, and through the router to learn what the router does with that.
//
//	go run ./test/unackread 127.0.0.1:28017 direct
//	go run ./test/unackread 127.0.0.1:27600 router
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
	opMsg          = 2013
	headerLen      = 16
	flagMoreToCome = 1 << 1
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
	return &reply{responseTo: responseTo, doc: bson.Raw(rest[5:])}, nil
}

func first(doc bson.Raw, keys ...string) string {
	for _, k := range keys {
		if v, err := doc.LookupErr(k); err == nil {
			s := v.String()
			if len(s) > 90 {
				s = s[:90] + "..."
			}
			return k + "=" + s
		}
	}
	return "?"
}

func main() {
	addr, label := os.Args[1], os.Args[2]
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

	must("hello", send(c, 1, 0, bson.D{
		{Key: "hello", Value: 1},
		{Key: "client", Value: bson.D{
			{Key: "application", Value: bson.D{{Key: "name", Value: "unackreadprobe"}}},
			{Key: "driver", Value: bson.D{{Key: "name", Value: "unackreadprobe"}, {Key: "version", Value: "1"}}},
			{Key: "os", Value: bson.D{{Key: "type", Value: "Linux"}}}}},
		{Key: "$db", Value: "admin"},
	}))
	_, err = read(c, 5*time.Second)
	must("hello reply", err)

	docs := bson.A{}
	for i := 0; i < 6; i++ {
		docs = append(docs, bson.D{{Key: "_id", Value: i}, {Key: "v", Value: i}})
	}
	must("drop", send(c, 2, 0, bson.D{{Key: "drop", Value: "unr"}, {Key: "$db", Value: "probe"}}))
	_, _ = read(c, 5*time.Second)
	must("insert", send(c, 3, 0, bson.D{
		{Key: "insert", Value: "unr"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "probe"},
	}))
	r, err := read(c, 5*time.Second)
	must("insert reply", err)
	fmt.Printf("%s seeded: %s\n", label, first(r.doc, "n", "ok"))

	// The question. A find that asks for documents, with the flag that says no
	// reply is coming.
	must("find", send(c, 4, flagMoreToCome, bson.D{
		{Key: "find", Value: "unr"}, {Key: "filter", Value: bson.D{}},
		{Key: "batchSize", Value: 2}, {Key: "$db", Value: "probe"},
	}))
	if r, err = read(c, 2*time.Second); err == nil {
		fmt.Printf("%s   the find WAS answered: responseTo=%d %s\n", label, r.responseTo, first(r.doc, "cursor", "ok"))
	} else {
		fmt.Printf("%s   the find was not answered, which is what the flag asks for\n", label)
	}

	// And whether the connection is still in step for the next command.
	must("ping", send(c, 9, 0, bson.D{{Key: "ping", Value: 1}, {Key: "$db", Value: "admin"}}))
	r, err = read(c, 5*time.Second)
	if err != nil {
		fmt.Printf("%s ping: %v\n", label, err)
		return
	}
	fmt.Printf("%s ping reply responseTo=%d (want 9) inStep=%v %s\n",
		label, r.responseTo, r.responseTo == 9, first(r.doc, "cursor", "ok"))
}
