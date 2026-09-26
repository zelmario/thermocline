package router

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// maxTieredIDBytes is the largest _id a document in a tiered collection may
// have.
//
// The feeder follows the fast tier's change stream, and mongod will not return
// an event past 16MB plus 16KB. An insert or update event carries the _id three
// times: in the resume token, which encodes the document key in hex and so
// holds it twice over, in documentKey, and inside the document. Measured on
// 2026-09-26: an insert of a document with a 7MB _id made an event of
// 29,360,471 bytes, the feeder reopened on it once a second, and nothing after
// it was mirrored for any namespace. There is no reading past an event mongod
// will not return, so the feeder cannot skip it, and the write that caused it
// had succeeded.
//
// An _id this size or smaller leaves an event inside mongod's 16KB of room for
// any document up to 16MB. Larger ones fit only while the document is small
// enough, which is a property of every later update and not something a write
// can be judged by. MongoDB refused any index key past 1,024 bytes, _id
// included, until 4.2.
const maxTieredIDBytes = 4096

// largeID reports the size of a value that would become a document's _id when
// it is too large.
func largeID(v bson.RawValue) (int, bool) {
	if v.Type == 0 {
		return 0, false
	}
	return len(v.Value), len(v.Value) > maxTieredIDBytes
}

// largeIDInDoc is largeID for a document written whole, an insert or a
// replacement.
func largeIDInDoc(doc bson.Raw) (int, bool) {
	v, err := doc.LookupErr("_id")
	if err != nil {
		return 0, false
	}
	return largeID(v)
}

// largeIDInUpdate reads the _id an update statement would write: a
// replacement's own, one set by an operator, or an upserted filter's equality.
// Only an upsert creates a document, and an _id cannot be changed otherwise,
// but mongod is the one to say so and this is only about size.
func largeIDInUpdate(u bson.RawValue, filter bson.Raw, upsert bool) (int, bool) {
	if doc, ok := u.DocumentOK(); ok {
		first, _ := doc.IndexErr(0)
		if k := first.Key(); len(k) == 0 || k[0] != '$' {
			if n, bad := largeIDInDoc(doc); bad {
				return n, true
			}
		} else {
			for _, op := range []string{"$set", "$setOnInsert"} {
				if sub, ok := doc.Lookup(op).DocumentOK(); ok {
					if n, bad := largeIDInDoc(sub); bad {
						return n, true
					}
				}
			}
		}
	}
	if upsert && len(filter) > 0 {
		if v, ok := equalityInFilter(filter, "_id"); ok {
			return largeID(v)
		}
	}
	return 0, false
}

func largeIDMessage(ns string, n int) string {
	return fmt.Sprintf("thermocline: refusing this write to %s because a document's _id is %d bytes, "+
		"and a collection this system tiers takes an _id of at most %d. The change stream that "+
		"carries writes to the archive holds each _id three times in every event, mongod will not "+
		"return an event past 16MB, and one it will not return stops mirroring for every "+
		"collection. The limit applies only to collections this system tiers.", ns, n, maxTieredIDBytes)
}

// guardLargeID refuses an insert, update or findAndModify on a tiered
// collection that would write an _id past maxTieredIDBytes.
func (c *conn) guardLargeID(msg *wire.Message, db, name string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	coll, ok := stringField(cmd, name)
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}

	n, bad := 0, false
	switch name {
	case "insert":
		for _, doc := range msg.AllOf("documents") {
			if n, bad = largeIDInDoc(doc); bad {
				break
			}
		}
	case "update":
		for _, st := range msg.AllOf("updates") {
			q, _ := st.Lookup("q").DocumentOK()
			if n, bad = largeIDInUpdate(st.Lookup("u"), q, boolField(st, "upsert")); bad {
				break
			}
		}
	case "findAndModify", "findandmodify":
		q, _ := cmd.Lookup("query").DocumentOK()
		n, bad = largeIDInUpdate(cmd.Lookup("update"), q, boolField(cmd, "upsert"))
	}
	if !bad {
		return nil, false
	}
	return c.refuseLargeID(msg, ns, n)
}

func (c *conn) refuseLargeID(msg *wire.Message, ns string, n int) (*wire.Message, bool) {
	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a write whose _id is too large for a tiered collection",
		"ns", ns, "id_bytes", n, "limit", maxTieredIDBytes,
		"because", "every change stream event holds the _id three times, and an event mongod will "+
			"not return stops mirroring for every collection")
	out, err := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID, largeIDMessage(ns, n))
	if err != nil {
		return nil, false
	}
	return out, true
}
