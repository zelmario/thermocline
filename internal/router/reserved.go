package router

import (
	"bytes"
	"strings"

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

// reservedPath reports whether a field path writes a reserved field.
//
// A path, not a name. "_tcDeletedAt.x" creates a top level _tcDeletedAt holding
// a subdocument, and the predicate that hides a tombstone is {$exists: false},
// so a subdocument hides the document exactly as a date does. Measured: $set of
// that path was accepted, and the same collection then answered 0 and 1 about
// one document depending on which tier could answer.
//
// The keys of an update operator are paths. The keys of an insert or a
// replacement are not: {"_tcDeletedAt.x": 1} inserted is one field with a dot
// in its name, which the predicate does not read. That is why reservedTopLevel
// stays as it is rather than growing a prefix test.
func reservedPath(key string) (string, bool) {
	// relates, not a prefix test written again here. uniqueupdate.go already
	// owns the notion "writing path a changes what path b holds", and it is the
	// same question this is asking. Two copies of one rule is how the index
	// comparisons drifted apart once already; see the comment on relates.
	for _, f := range reservedFieldNames {
		if relates(key, f) {
			return f, true
		}
	}
	return "", false
}

// reservedCommonPrefix is what every reserved name starts with, as bytes, so a
// document can be ruled out without being parsed. Computed rather than written
// down, because the names are meta's to choose.
var reservedCommonPrefix = func() []byte {
	if len(reservedFieldNames) == 0 {
		return nil
	}
	p := reservedFieldNames[0]
	for _, f := range reservedFieldNames[1:] {
		for !strings.HasPrefix(f, p) {
			p = p[:len(p)-1]
		}
	}
	return []byte(p)
}()

// reservedInPaths names a reserved field written by a document whose keys are
// field paths, which is what an update operator and a $set, $addFields or
// $project stage carry.
func reservedInPaths(doc bson.Raw) (string, bool) {
	// A document that does not contain the names' common prefix anywhere
	// cannot carry one, and deciding that is a scan of the raw bytes rather
	// than a parse. Elements() materialises a slice per operator on every
	// update to a tiered collection: measured, reading the operators by path
	// instead of by name took BenchmarkReservedInUpdate from 591 ns/op and 7
	// allocations to 1269 and 15, and this brings it back.
	if !bytes.Contains(doc, reservedCommonPrefix) {
		return "", false
	}
	elems, err := doc.Elements()
	if err != nil {
		return "", false
	}
	for _, e := range elems {
		if f, bad := reservedPath(e.Key()); bad {
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
				// Paths, not names: an operator's keys are paths, so
				// $set of "_tcDeletedAt.x" writes the mark's name at the
				// top level just as $set of "_tcDeletedAt" does.
				if f, bad := reservedInPaths(d); bad {
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
						to, ok := e.Value().StringValueOK()
						if !ok {
							continue
						}
						if f, bad := reservedPath(to); bad {
							return f, true
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
	// These name the fields they write, and their keys are paths.
	for _, op := range []string{"$set", "$addFields", "$project"} {
		if v, err := sd.LookupErr(op); err == nil {
			if d, ok := v.DocumentOK(); ok {
				if f, bad := reservedInPaths(d); bad {
					return f, true
				}
			}
		}
	}
	// A replacing stage does not name fields at all: it says what the whole
	// document becomes, as one expression. $replaceWith takes that expression
	// directly and $replaceRoot wraps it in newRoot, and both were read at the
	// same depth, so {$replaceRoot: {newRoot: {_tcDeletedAt: t}}} was looked up
	// for a reserved key on the wrapper, where the only key is newRoot. The
	// documented spelling of the stage went straight through.
	if v, err := sd.LookupErr("$replaceWith"); err == nil {
		if f, bad := reservedInNewRoot(v); bad {
			return f, true
		}
	}
	if v, err := sd.LookupErr("$replaceRoot"); err == nil {
		if d, ok := v.DocumentOK(); ok {
			if nr, nerr := d.LookupErr("newRoot"); nerr == nil {
				if f, bad := reservedInNewRoot(nr); bad {
					return f, true
				}
			}
		}
	}
	// $merge carries a pipeline of its own. whenMatched can be a list of stages
	// rather than one of the named modes, and those stages run against the
	// document already in the target collection, so this is a write to a tiered
	// collection that no stage of the outer pipeline mentions. Measured: a
	// whenMatched of [{$set: {_tcDeletedAt: "$$NOW"}}] put the mark on a live
	// document and every read that falls through then skipped it.
	//
	// whenNotMatched is not read, because its only values are insert, discard
	// and fail. The document it inserts is the outer pipeline's output, which
	// the loop above has already read.
	if v, err := sd.LookupErr("$merge"); err == nil {
		if d, ok := v.DocumentOK(); ok {
			if wm, werr := d.LookupErr("whenMatched"); werr == nil {
				// ArrayOK rather than Array: whenMatched is a string in every
				// ordinary merge, and RawValue.Array panics on the wrong type.
				if raw, isArr := wm.ArrayOK(); isArr {
					if arr, aerr := raw.Values(); aerr == nil {
						if f, bad := reservedInStages(arr); bad {
							return f, true
						}
					}
				}
			}
		}
	}
	return "", false
}

// reservedInNewRoot names a reserved field the document a replacing stage
// builds would carry.
//
// A literal document shows its keys. $mergeObjects shows the keys of whichever
// of its arguments are literal, which is how a pipeline adds one field and
// keeps the rest. $setField is the one expression that writes a field named in
// a value rather than spelled as a key, the same shape as $rename above.
//
// Anything else builds its keys from data: a field path promoting a
// subdocument, $arrayToObject, a conditional. Those are not read here and are
// not refused either. It is the same residue as a $merge of documents that
// already carry the field, and it is in docs/limitations.md for the same
// reason: refusing every expression that can evaluate to a document would
// refuse most of the pipelines that legitimately write one.
func reservedInNewRoot(v bson.RawValue) (string, bool) {
	d, ok := v.DocumentOK()
	if !ok {
		return "", false
	}
	elems, err := d.Elements()
	if err != nil || len(elems) == 0 {
		return "", false
	}
	// A single key beginning with $ is an expression rather than a literal
	// document. mongod refuses a document that mixes the two, so the first key
	// settles which this is.
	if k := elems[0].Key(); len(k) > 0 && k[0] == '$' {
		arg := elems[0].Value()
		switch k {
		case "$mergeObjects":
			if raw, isArr := arg.ArrayOK(); isArr {
				vals, verr := raw.Values()
				if verr != nil {
					return "", false
				}
				for _, a := range vals {
					if f, bad := reservedInNewRoot(a); bad {
						return f, true
					}
				}
				return "", false
			}
			return reservedInNewRoot(arg)
		case "$literal":
			if ld, lok := arg.DocumentOK(); lok {
				return reservedTopLevel(ld)
			}
		case "$setField":
			sf, sok := arg.DocumentOK()
			if !sok {
				return "", false
			}
			if fv, ferr := sf.LookupErr("field"); ferr == nil {
				if name, nok := fv.StringValueOK(); nok && reservedFields[name] {
					return name, true
				}
			}
			if iv, ierr := sf.LookupErr("input"); ierr == nil {
				return reservedInNewRoot(iv)
			}
		}
		return "", false
	}
	// A literal document. Its keys are names rather than paths, exactly as a
	// replacement document's are.
	return reservedTopLevel(d)
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

// reservedInUpsertFilter names a reserved field an upsert would carry from its
// own filter into the document it creates.
//
// A filter is a write path, which is the part that is easy to miss. An upsert
// that matches nothing builds the new document from the filter's equality
// fields and then applies the update to it, so `{_tcDeletedAt: t}` with
// `upsert: true` writes the mark without any update operator naming it.
// Measured against mongod 6.0 rather than reasoned about: three such statements
// created three documents and the router's own countDocuments then answered 1.
//
// Equality, read with the same function the unique key registry reads a filter
// by, because it is the same question: what does mongod carry from here into
// the document. Sharing it is what stops the two drifting apart.
func reservedInUpsertFilter(filter bson.Raw) (string, bool) {
	for _, f := range reservedFieldNames {
		if _, ok := equalityInFilter(filter, f); ok {
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
		// The filter of anything that would upsert, after the update itself:
		// the update is the cheaper read and names the field more often.
		for _, st := range upsertStatements(msg, cmd, name) {
			if f, bad := reservedInUpsertFilter(st.filter); bad {
				field = f
				break
			}
		}
	}
	if field == "" {
		c.guardReplacedRoots(msg, cmd, name)
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

// guardReplacedRoots appends a check to every pipeline update that replaces the
// document, so the statement fails rather than leave a reserved field at the top
// level.
//
// reservedInNewRoot reads what a replacing stage writes where it is spelled out.
// A field path or another expression builds the keys from the document itself:
// [{$replaceWith: {$mergeObjects: ["$$ROOT", "$payload"]}}] lifts a nested
// payload._tcDeletedAt, which an insert is allowed to carry, to the top, and the
// document then reads as deleted in the archive. Refusing every such pipeline
// would refuse most that legitimately write a document, so the check runs where
// the document is known: after the client's own stages, a document carrying
// neither field is passed on unchanged, and one carrying either fails the
// statement with an error naming the field. No write this system accepts leaves
// either at the top level of a fast tier document, so nothing else is touched.
func (c *conn) guardReplacedRoots(msg *wire.Message, cmd bson.Raw, name string) {
	guard := bson.D{{Key: "$replaceWith", Value: bson.D{{Key: "$cond", Value: bson.D{
		{Key: "if", Value: bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$" + meta.TombstoneField}}, "missing"}}},
			bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$" + meta.BodyDroppedField}}, "missing"}}},
		}}}},
		{Key: "then", Value: "$$ROOT"},
		{Key: "else", Value: bson.D{{Key: "$toDate", Value: "thermocline: refusing this update because the " +
			"document it builds carries " + meta.TombstoneField + " or " + meta.BodyDroppedField + " at the top " +
			"level, which this system marks archived documents with; rename the field"}}},
	}}}}}
	replaces := func(v bson.RawValue) bool {
		arr, ok := v.ArrayOK()
		if !ok {
			return false
		}
		vals, _ := arr.Values()
		for _, st := range vals {
			d, dok := st.DocumentOK()
			if !dok {
				continue
			}
			for _, k := range []string{"$replaceWith", "$replaceRoot"} {
				if _, err := d.LookupErr(k); err == nil {
					return true
				}
			}
		}
		return false
	}
	guarded := func(v bson.RawValue) bson.A {
		arr, _ := v.ArrayOK()
		vals, _ := arr.Values()
		out := make(bson.A, 0, len(vals)+1)
		for _, st := range vals {
			out = append(out, st)
		}
		return append(out, guard)
	}

	var rewritten bson.D
	switch name {
	case "update":
		stmts := msg.AllOf("updates")
		any := false
		for _, st := range stmts {
			if replaces(st.Lookup("u")) {
				any = true
				break
			}
		}
		if !any {
			return
		}
		keep := make([]bson.Raw, 0, len(stmts))
		for _, st := range stmts {
			u := st.Lookup("u")
			if !replaces(u) {
				keep = append(keep, st)
				continue
			}
			elems, err := st.Elements()
			if err != nil {
				return
			}
			d := bson.D{}
			for _, e := range elems {
				if e.Key() == "u" {
					d = append(d, bson.E{Key: "u", Value: guarded(u)})
					continue
				}
				d = append(d, bson.E{Key: e.Key(), Value: e.Value()})
			}
			raw, err := bson.Marshal(d)
			if err != nil {
				return
			}
			keep = append(keep, raw)
		}
		var err error
		if rewritten, err = rebuildStatements(cmd, "updates", keep); err != nil {
			return
		}
	case "findAndModify", "findandmodify":
		u := cmd.Lookup("update")
		if !replaces(u) {
			return
		}
		elems, err := cmd.Elements()
		if err != nil {
			return
		}
		for _, e := range elems {
			if e.Key() == "update" {
				rewritten = append(rewritten, bson.E{Key: "update", Value: guarded(u)})
				continue
			}
			rewritten = append(rewritten, bson.E{Key: e.Key(), Value: e.Value()})
		}
	default:
		return
	}
	out, err := wire.NewMsg(msg.Header.RequestID, 0, rewritten)
	if err != nil {
		return
	}
	if msg.MoreToCome() {
		out = out.WithMoreToCome()
	}
	c.sendInstead = out
}
