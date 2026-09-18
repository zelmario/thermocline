package router

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// The field names this system writes into the archive's own copies, and what
// happens when an application writes one itself.
//
// `_tcDeletedAt` is how an archived document is marked as one the application
// deleted. Every read of the archive carries a predicate excluding those,
// because returning one would resurrect a deleted document. Nothing stopped an
// application writing a field of that name, and it is an ordinary field name
// that mongod accepts.
//
// Measured: an application inserted {_id: "x", n: 5000, _tcDeletedAt: <a date>}
// into a tiered collection through the router. Both tiers took it, and the
// archive's copy is then a live document wearing a tombstone's mark.
// `countDocuments({n: 5000})` answered 0 where the same document in an untiered
// collection answered 1, with ok on both. A lookup by _id still found it,
// because a point read asks the fast tier too, so the collection answered two
// different ways about one document.
//
// The read is the smaller half. A fast tier rebuilt from that archive, which is
// the documented recovery, drops the document altogether: it reads as deleted
// and nothing says otherwise.
//
// So the write is refused, at the door, in the application's own terms. The
// alternative was to let it through and refuse the reads afterwards, which
// tells the application nothing about which of its writes caused it.
var reservedFields = map[string]bool{
	meta.TombstoneField:   true,
	meta.BodyDroppedField: true,
}

// reservedFieldNames is the same set in the order the hot path reads it.
var reservedFieldNames = []string{meta.TombstoneField, meta.BodyDroppedField}

// reservedTopLevel names a reserved field in a document, looking only at the top
// level, which is where the mark lives. A nested field of the same name is
// nobody's business: the predicate that excludes tombstones reads the top.
func reservedTopLevel(doc bson.Raw) (string, bool) {
	// Two lookups rather than a walk of the elements. This is on the write path
	// for every document written to a tiered collection, and Elements()
	// materialises a slice to answer a question about two names: measured, 1345
	// ns/op and 12 allocations against 584 bytes, where a lookup per name is
	// allocation free. See reserved_bench_test.go, which was written before
	// this and not after.
	for _, f := range reservedFieldNames {
		if _, err := doc.LookupErr(f); err == nil {
			return f, true
		}
	}
	return "", false
}

// reservedInUpdate names a reserved field an update would leave on a document.
//
// Three shapes, because an update has three. A replacement document is read at
// its top level like an insert. An update with operators is read through the
// operators that write a named field. A pipeline is read through the stages
// that do, which are the same two names.
func reservedInUpdate(u bson.RawValue) (string, bool) {
	switch u.Type {
	case bson.TypeEmbeddedDocument:
		doc, ok := u.DocumentOK()
		if !ok {
			return "", false
		}
		elems, err := doc.Elements()
		if err != nil {
			return "", false
		}
		operators := false
		for _, e := range elems {
			if len(e.Key()) > 0 && e.Key()[0] == '$' {
				operators = true
				break
			}
		}
		if !operators {
			// A replacement document: what it says is what the document
			// becomes.
			return reservedTopLevel(doc)
		}
		// Every operator that names the field, rather than a list of the ones
		// that seemed likely. The first version of this read $set,
		// $setOnInsert and $addFields, and the predicate that hides a document
		// is {$exists: false}, so a field of any type hides it: measured,
		// $currentDate and $min each wrote the mark and were accepted, and
		// $inc and $push would have on a document that did not have it yet.
		// An allow-list is the wrong shape for a rule about a name.
		for _, e := range elems {
			key := e.Key()
			if len(key) == 0 || key[0] != '$' {
				continue
			}
			// $unset takes the field away, which is how an operator cleans up a
			// document that has one, and $rename names its target in the value
			// rather than the key and is read below.
			if key == "$unset" || key == "$rename" {
				continue
			}
			if d, ok := e.Value().DocumentOK(); ok {
				if f, bad := reservedTopLevel(d); bad {
					return f, true
				}
			}
		}
		// $rename names its target in the value rather than the key, which is
		// the one operator that writes a field the keys never mention.
		if v, err := doc.LookupErr("$rename"); err == nil {
			if d, ok := v.DocumentOK(); ok {
				if elems, err := d.Elements(); err == nil {
					for _, e := range elems {
						if to, ok := e.Value().StringValueOK(); ok && reservedFields[to] {
							return to, true
						}
					}
				}
			}
		}
	case bson.TypeArray:
		// An aggregation pipeline update.
		vals, aok := arrayValues(u)
		if !aok {
			return "", false
		}
		for _, stage := range vals {
			sd, ok := stage.DocumentOK()
			if !ok {
				continue
			}
			if f, bad := reservedInStage(sd); bad {
				return f, true
			}
		}
	}
	return "", false
}

// reservedInStage reads one aggregation stage for a reserved field it writes.
//
// $project is here because it can introduce a field as much as $set can, and
// $unset is not because in a pipeline it removes one.
func reservedInStage(sd bson.Raw) (string, bool) {
	for _, op := range []string{"$set", "$addFields", "$replaceWith", "$replaceRoot", "$project"} {
		if v, err := sd.LookupErr(op); err == nil {
			if d, ok := v.DocumentOK(); ok {
				if f, bad := reservedTopLevel(d); bad {
					return f, true
				}
			}
		}
	}
	return "", false
}

// reservedInStages names a reserved field an aggregation pipeline writes.
//
// The same reading as a pipeline update, over a whole pipeline rather than one
// update's stages, for the $merge that mongod performs rather than this router.
func reservedInStages(stages []bson.RawValue) (string, bool) {
	for _, stage := range stages {
		sd, ok := stage.DocumentOK()
		if !ok {
			continue
		}
		if f, bad := reservedInStage(sd); bad {
			return f, true
		}
	}
	return "", false
}

// guardReservedField refuses a write that would leave one of this system's own
// marks on an application document in a tiered collection.
//
// Only tiered collections. In an untiered one the field means nothing, because
// nothing mirrors it and no read of it carries the predicate that reads the
// mark, and refusing there would be this proxy inventing a rule about data it
// does not touch.
func (c *conn) guardReservedField(msg *wire.Message, db, name string) (*wire.Message, bool) {
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

	var field string
	switch name {
	case "insert":
		for _, doc := range msg.AllOf("documents") {
			if f, bad := reservedTopLevel(doc); bad {
				field = f
				break
			}
		}
	case "update":
		for _, st := range msg.AllOf("updates") {
			if v, err := st.LookupErr("u"); err == nil {
				if f, bad := reservedInUpdate(v); bad {
					field = f
					break
				}
			}
		}
	case "findAndModify", "findandmodify":
		if v, err := cmd.LookupErr("update"); err == nil {
			if f, bad := reservedInUpdate(v); bad {
				field = f
			}
		}
	}
	if field == "" {
		return nil, false
	}

	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a write that would put this system's own mark on an application document",
		"ns", ns, "field", field,
		"because", "the archive marks a deleted document with this field, and every read of the "+
			"archive excludes the documents that carry it",
		"effect", "the document would be written to both tiers and then be invisible to every read "+
			"that falls through, and a fast tier rebuilt from the archive would not have it at all")

	out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this write to "+ns+" because it sets "+field+", which is the field "+
			"this system marks archived documents deleted with. Every read that falls through to "+
			"the archive excludes documents carrying it, so this one would be written successfully "+
			"and then be invisible, and a fast tier rebuilt from the archive would not have it at "+
			"all. Rename the field. It is reserved only in collections this system tiers.")
	if rerr != nil {
		return nil, false
	}
	return out, true
}
