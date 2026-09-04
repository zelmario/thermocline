package router

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// cursorNotFound is mongod's code for a getMore naming a cursor it has never
// heard of, or has forgotten.
const cursorNotFound = 43

// explainCursorNotFound says what a CursorNotFound behind several routers means.
//
// A read answered from the archive is served from a cursor this router holds in
// its own memory, so another router cannot continue it. Drivers pin a cursor to
// the server that created it, so naming every router in the connection string
// works. One load balanced address in front of several does not: the driver sees
// one server, its pool sends the getMore down whichever connection is free, and
// it lands on a router that never had the cursor.
//
// What the application saw then was a bare code 43 from the fast tier, and the
// limitation this closes was written as "it cannot tell you more than that". It
// cannot tell you *which* it is: a cursor id says nothing about who minted it,
// and measured on PSMDB 8.0 mongod's own ids cover the whole positive int64
// range, so an id from another router is indistinguishable from an expired one.
//
// But it can say that much. Naming the two possibilities and how to tell them
// apart is the difference between an error somebody can act on and one they
// have to have read this file to understand. The code stays 43, because a driver
// decides what to retry from the code and not from the words.
func (c *conn) explainCursorNotFound(msg *wire.Message, reply *wire.Message) (*wire.Message, bool) {
	rdoc, err := reply.ReplyDoc()
	if err != nil {
		return nil, false
	}
	v, verr := rdoc.LookupErr("code")
	if verr != nil {
		return nil, false
	}
	code, cok := v.AsInt64OK()
	if !cok || code != cursorNotFound {
		return nil, false
	}
	var doc bson.D
	if err := bson.Unmarshal(rdoc, &doc); err != nil {
		return nil, false
	}
	added := false
	for i := range doc {
		if doc[i].Key != "errmsg" {
			continue
		}
		was, ok := doc[i].Value.(string)
		if !ok {
			return nil, false
		}
		doc[i].Value = was + " (thermocline: this router did not mint that cursor id and the fast " +
			"tier does not know it either, so it is one of two things. An expired cursor, or a " +
			"cursor another router opened: a read answered from the archive is served from that " +
			"router's own memory and no other can continue it. A cursor id says nothing about who " +
			"minted it, so this cannot say which. If several routers sit behind one load balanced " +
			"address, that is the cause: name every router in the connection string instead, so " +
			"the driver pins the cursor to the one that made it.)"
		added = true
		break
	}
	if !added {
		return nil, false
	}
	out, nerr := wire.NewMsg(reply.Header.RequestID, msg.Header.RequestID, doc)
	if nerr != nil {
		return nil, false
	}
	c.log.Warn("a getMore named a cursor neither this router nor the fast tier knows",
		"meaning", "an expired cursor, or one another router opened, which cannot be told apart "+
			"from the id",
		"action", "if several routers sit behind one address, name them all in the connection "+
			"string so the driver pins each cursor to the router that made it")
	return out, true
}
