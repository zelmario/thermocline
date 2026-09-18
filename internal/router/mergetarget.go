package router

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// $merge decides what to do by looking at what is already there, and it looks on
// the fast tier, which is the one place that does not hold every document.
//
// whenMatched needs to find the existing document. whenNotMatched decides what
// happens when it does not. Both questions were answered by a tier holding only
// part of the collection, so a $merge whose target document had been archived
// matched nothing, took the whenNotMatched branch, and created a document built
// from the pipeline's output in its place. The feeder then mirrored that over
// the archived record.
//
// Measured on a tiered collection holding {_id: 5, name: "alice", tier: "gold",
// balance: 100} in the archive only, against an untiered reference holding the
// same document, with
// [{$documents: [{_id: 5, balance: 200}]}, {$merge: {into: ..., whenMatched:
// "merge", whenNotMatched: "insert"}}]. The reference produced
// {_id: 5, name: "alice", tier: "gold", balance: 200}. The tiered collection
// produced {_id: 5, balance: 200}, on both tiers. name and tier were destroyed,
// and the whole thing reported success. whenMatched "fail" showed the same
// blindness the other way round: MongoDB failed because the document exists, and
// this accepted because it could not see it.
//
// This is the upsert defect again, reached by a different command, and the fix
// has the same shape: decide it before it runs, and make the fast tier hold what
// the statement is about to look for.
//
// Which documents those are is knowable. $merge matches on its `on` fields,
// which default to _id, against the documents its pipeline produces. So the
// pipeline is run once without its final stage, the values it would match on are
// collected, and the archived documents carrying those values are copied up.
// Then the client's own command runs, once, against a tier that now holds every
// document the merge could touch.
//
// The cost is reading the input twice, and it is only paid when the target is
// tiered and its fast tier does not claim to hold everything.

// guardMergeTarget brings archived documents up before a $merge can shadow them.
func (c *conn) guardMergeTarget(ctx context.Context, msg *wire.Message, cmd bson.Raw, db string) (*wire.Message, bool) {
	stages, ok := pipelineStages(cmd)
	if !ok || len(stages) == 0 {
		return nil, false
	}
	final, fok := stages[len(stages)-1].DocumentOK()
	if !fok {
		return nil, false
	}
	mergeVal, err := final.LookupErr("$merge")
	if err != nil {
		return nil, false
	}

	target, onFields, ok := mergeTarget(mergeVal, db)
	if !ok {
		return nil, false
	}
	if inTransaction(cmd) {
		// mongod refuses $merge in a transaction, the same way it refuses $out,
		// so there is nothing here to protect. Measured rather than assumed: the
		// command comes back "$merge cannot be used in a transaction".
		//
		// Worth returning for rather than letting it fall through. Everything
		// below reads the input once to work out what the merge would match and
		// copies the archived documents carrying those values up, and paying
		// that for a command that is about to be refused is a read of the
		// archive and a write to the fast tier that nothing asked for.
		return nil, false
	}
	// A pipeline that sets one of this system's own marks, merging into a
	// collection this system tiers. The sink in aggwrite.go checks the
	// documents themselves, and it only sees the ones the router writes: a
	// $merge whose input the fast tier can answer alone is forwarded whole and
	// mongod performs the write, so this is the only place to catch it.
	//
	// The stages are what can be read here, which makes this narrower than the
	// sink's check and worth being honest about: a $merge of documents that
	// already carry the field, from a collection where it is allowed because
	// nothing tiers it, is not visible in any stage and is not caught. That
	// residue is in docs/limitations.md.
	if c.r.cfg.Manages(target) {
		if f, bad := reservedInStages(stages); bad {
			c.r.Stats.Refused.Add(1)
			c.cls = ClassRefused
			c.log.Warn("refused a $merge that would put this system's own mark on a document",
				"into", target, "field", f)
			out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
				"thermocline: refusing this write to "+target+" because the pipeline sets "+f+
					", which is the field this system marks archived documents deleted with. A "+
					"document carrying it is written to both tiers and is then invisible to every "+
					"read that falls through to the archive, and a fast tier rebuilt from the "+
					"archive does not have it at all. Rename the field; it is reserved only in "+
					"collections this system tiers.")
			if rerr != nil {
				return nil, false
			}
			return out, true
		}
	}

	if !c.r.cfg.Manages(target) {
		// Untiered, so the fast tier is the whole of it and it can see
		// everything the merge might match.
		return nil, false
	}
	// A fast tier holding all of history has nothing archived that is not also
	// here, so the merge can see whatever it would have seen.
	if mark, known := c.r.completeness(ctx, target); known && mark.Claimed && mark.CoversAll() {
		return nil, false
	}

	idxs := c.r.uniqueIndexes(ctx, target)
	keys, produced, err := c.mergeKeys(ctx, cmd, stages, db, onFields, uniqueFields(idxs))
	if err != nil {
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a $merge whose target could not be checked", "ns", target, "err", err)
		out, rerr := c.r.commandError(0, msg.Header.RequestID,
			"thermocline: refusing this $merge because the documents it would match could not be "+
				"determined, and a $merge that does not find the archived document creates one in its "+
				"place and overwrites it: "+err.Error())
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	if len(keys) == 0 {
		return nil, false
	}

	n, cerr := c.r.copyUpMatching(ctx, target, keys, copyUpOptions{let: letOf(cmd), collation: docField(cmd, "collation")})
	if cerr != nil {
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a $merge that could have overwritten archived documents", "ns", target, "err", cerr)
		out, rerr := c.r.commandError(0, msg.Header.RequestID,
			"thermocline: refusing this $merge because the archive could not be checked, and a $merge "+
				"that does not find the archived document creates a new one in its place and "+
				"overwrites it: "+cerr.Error())
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	if n > 0 {
		c.cls = ClassMiss
		c.log.Info("copied documents up so a $merge would find them rather than replace them",
			"ns", target, "documents", n)
	}

	// The copy-up settles what the merge can see. What it is about to write
	// there is a different question, and the fast tier's unique indexes cannot
	// answer it either: see guardMergeKeys.
	if len(idxs) > 0 {
		if out, done := c.guardMergeKeys(ctx, msg, target, db, idxs, onFields, produced, keys, mergeVal); done {
			return out, true
		}
	}
	return nil, false
}

// uniqueFields is every field the target's unique indexes are built on, so the
// pipeline's output can be projected to exactly what the registry needs.
func uniqueFields(idxs []uniqueIndex) []string {
	var out []string
	for _, idx := range idxs {
		out = append(out, idx.fields...)
		out = append(out, idx.partialFields...)
	}
	return out
}

// mergeTarget reads where a $merge writes and what it matches on.
//
// into takes a bare collection name in the same database or a document naming
// both. on defaults to _id, and takes one field or several.
func mergeTarget(v bson.RawValue, db string) (ns string, on []string, ok bool) {
	on = []string{"_id"}

	if coll, isStr := v.StringValueOK(); isStr {
		return db + "." + coll, on, true
	}
	doc, isDoc := v.DocumentOK()
	if !isDoc {
		return "", nil, false
	}

	intoVal, err := doc.LookupErr("into")
	if err != nil {
		return "", nil, false
	}
	outDB, outColl := db, ""
	if s, isStr := intoVal.StringValueOK(); isStr {
		outColl = s
	} else if idoc, isDoc := intoVal.DocumentOK(); isDoc {
		if d, derr := idoc.LookupErr("db"); derr == nil {
			if s, sok := d.StringValueOK(); sok {
				outDB = s
			}
		}
		if cv, cerr := idoc.LookupErr("coll"); cerr == nil {
			if s, sok := cv.StringValueOK(); sok {
				outColl = s
			}
		}
	}
	if outColl == "" {
		return "", nil, false
	}

	if onVal, oerr := doc.LookupErr("on"); oerr == nil {
		if s, sok := onVal.StringValueOK(); sok {
			on = []string{s}
		} else if arr, aok := arrayValues(onVal); aok {
			on = on[:0]
			for _, e := range arr {
				if s, sok := e.StringValueOK(); sok {
					on = append(on, s)
				}
			}
			if len(on) == 0 {
				return "", nil, false
			}
		} else {
			// Present and neither a string nor an array of strings. Falling
			// through here left `on` at its default of _id, so the router read
			// a command the client did not send and acted on it: `on: 5` came
			// back as "E11000 ... the existing document with this key is in the
			// archive", where mongod says "$merge 'on' field must be either a
			// string or an array of strings, but found int". Refusing to
			// understand it forwards the stage and lets mongod say that.
			return "", nil, false
		}
	}
	return outDB + "." + outColl, on, true
}

// mergeKeys runs the pipeline without its final stage and returns a filter
// matching every document the $merge could act on.
//
// The pipeline runs on the fast tier, which is where it would have run anyway.
// This is deliberately not the archive: the input's own completeness is a
// separate question that the aggregate guard has already decided, and reading it
// from somewhere else here would answer a question nobody asked.
func (c *conn) mergeKeys(ctx context.Context, cmd bson.Raw, stages []bson.RawValue, db string,
	on []string, extra []string) (bson.Raw, []bson.Raw, error) {
	aggVal, err := cmd.LookupErr("aggregate")
	if err != nil {
		return nil, nil, fmt.Errorf("read the aggregate target: %w", err)
	}

	read := make(bson.A, 0, len(stages)-1)
	for _, st := range stages[:len(stages)-1] {
		doc, ok := st.DocumentOK()
		if !ok {
			return nil, nil, fmt.Errorf("a stage could not be read")
		}
		read = append(read, doc)
	}
	// Only the fields matched on, so a large result costs the keys rather than
	// the documents. extra carries the fields of the target's unique indexes,
	// which are needed for the same reason: what the merge is about to write
	// there has to be checked against the registry, and nothing else in the
	// document matters.
	proj := bson.D{}
	want := map[string]bool{}
	for _, f := range append(append([]string{}, on...), extra...) {
		if f == "" || want[f] {
			continue
		}
		want[f] = true
		proj = append(proj, bson.E{Key: f, Value: 1})
	}
	if len(proj) > 0 {
		read = append(read, bson.D{{Key: "$project", Value: proj}})
	}

	var aggAny any
	if err := aggVal.Unmarshal(&aggAny); err != nil {
		return nil, nil, fmt.Errorf("read the aggregate target: %w", err)
	}
	// The pipeline is the client's own, so it runs with the client's own
	// variables. A $lookup or a $match in it may refer to them, and a rerun
	// without them fails on a pipeline the client wrote correctly.
	rerun := bson.D{
		{Key: "aggregate", Value: aggAny},
		{Key: "pipeline", Value: read},
		{Key: "cursor", Value: bson.D{}},
	}
	if let := letOf(cmd); len(let) > 0 {
		rerun = append(rerun, bson.E{Key: "let", Value: let})
	}
	cur, err := c.r.hot.Database(db).RunCommandCursor(ctx, rerun)
	if err != nil {
		return nil, nil, fmt.Errorf("run the pipeline to find what it would merge: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var clauses bson.A
	var produced []bson.Raw
	seen := 0
	for cur.Next(ctx) {
		if seen >= maxCopyUpForWrite {
			return nil, nil, fmt.Errorf("this $merge would touch more than %d documents; warm the target first",
				maxCopyUpForWrite)
		}
		doc := make(bson.Raw, len(cur.Current))
		copy(doc, cur.Current)
		clause := bson.D{}
		complete := true
		for _, f := range on {
			v, lerr := doc.LookupErr(f)
			if lerr != nil {
				complete = false
				break
			}
			var av any
			if uerr := v.Unmarshal(&av); uerr != nil {
				complete = false
				break
			}
			clause = append(clause, bson.E{Key: f, Value: av})
		}
		if !complete {
			// A document missing one of the fields matched on cannot match an
			// existing one either, so there is nothing to bring up for it.
			continue
		}
		clauses = append(clauses, clause)
		produced = append(produced, doc)
		seen++
	}
	if err := cur.Err(); err != nil {
		return nil, nil, fmt.Errorf("read what the pipeline would merge: %w", err)
	}
	if len(clauses) == 0 {
		return nil, nil, nil
	}

	filter, err := bson.Marshal(bson.D{{Key: "$or", Value: clauses}})
	if err != nil {
		return nil, nil, fmt.Errorf("build the filter: %w", err)
	}
	return filter, produced, nil
}
