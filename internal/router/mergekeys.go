package router

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/wire"
)

// $merge writes documents, and a document it creates holds unique values just
// as an inserted one does.
//
// The target guard above makes the fast tier hold what the merge will match on,
// which settles the `on` fields: mongod then sees the archived document and
// updates it rather than replacing it. It settles nothing about any *other*
// unique index on the target. The archive holds that key, the fast tier's index
// cannot see it, and $merge inserts.
//
// Measured on a tiered collection with a unique index on email, holding
// {_id: 1, email: "taken@x.com"} in the archive alone, against
// [{$documents: [{_id: 2, email: "taken@x.com"}]}, {$merge: {on: "_id"}}]:
// MongoDB refused the same statement with E11000. The router accepted it, the
// document landed on the fast tier, the archive refused to mirror it, and a
// count of that key through the router answered 1 where two documents exist.
// This is the upsert defect reached by a third command; see uniqueupdate.go.
//
// So the keys are claimed before the merge runs, from the documents the pipeline
// produced, which the target guard has already read once.

// guardMergeKeys claims the unique keys a $merge is about to write, and refuses
// the command when one of them belongs to a live document in the archive.
func (c *conn) guardMergeKeys(ctx context.Context, msg *wire.Message, ns, db string,
	idxs []uniqueIndex, on []string, produced []bson.Raw, keys bson.Raw, mergeVal bson.RawValue) (*wire.Message, bool) {
	if len(produced) == 0 {
		return nil, false
	}
	whenMatched, whenNotMatched := mergeModes(mergeVal)

	existing, err := c.mergeExisting(ctx, ns, db, on, keys)
	if err != nil {
		// The same stance the insert and update paths take: losing the cross
		// tier half of a unique index is bad, refusing every $merge because a
		// lookup failed is worse.
		c.log.Warn("could not read what a $merge would match, so its keys were not checked",
			"ns", ns, "err", err,
			"effect", "a duplicate of an archived document could be written")
		return nil, false
	}

	var claimed []string
	for _, doc := range produced {
		k, kok := onKey(doc, on)
		if !kok {
			continue
		}
		ex, matched := existing[k]
		result, want := mergeResult(doc, ex, matched, whenMatched, whenNotMatched)
		if !want {
			continue
		}
		var owner bson.RawValue
		if matched {
			owner, _ = ex.LookupErr("_id")
		}
		for i := range idxs {
			if !c.r.constrains(ctx, db, idxs[i], result) {
				continue
			}
			kid, ok := keyID(ns, idxs[i], result)
			if !ok {
				continue
			}
			got, terr := c.r.tryClaim(ctx, ns, idxs[i], kid, owner, keyValues(idxs[i], result))
			if terr != nil {
				c.r.releaseKeys(ctx, claimed)
				c.log.Warn("could not check a $merge against the key registry, letting it through",
					"ns", ns, "err", terr)
				return nil, false
			}
			if got {
				claimed = append(claimed, kid)
				continue
			}
			c.r.releaseKeys(ctx, claimed)
			c.r.Stats.DuplicateKeys.Add(1)
			c.cls = ClassRefused
			c.log.Debug("a $merge would duplicate an archived document", "ns", ns, "index", idxs[i].name)
			// mongod reports a duplicate key from inside a $merge by failing the
			// aggregate, not by returning a cursor with a write error in it.
			out, rerr := c.r.duplicateKeyReply(msg.Header.RequestID, msg.Header.RequestID, ns, idxs[i].name)
			if rerr != nil {
				return nil, false
			}
			return out, true
		}
	}

	// Held until the reply, so an aggregation the fast tier rejects gives its
	// keys back rather than blocking those values until something collides.
	c.pendingKeys = append(c.pendingKeys, claimed...)
	return nil, false
}

// mergeModes reads whenMatched and whenNotMatched, with the server's defaults.
// A pipeline for whenMatched reads as an empty mode: what it computes is not
// something to reimplement here.
func mergeModes(v bson.RawValue) (whenMatched, whenNotMatched string) {
	whenMatched, whenNotMatched = "merge", "insert"
	doc, ok := v.DocumentOK()
	if !ok {
		return whenMatched, whenNotMatched
	}
	if m, err := doc.LookupErr("whenMatched"); err == nil {
		if s, sok := m.StringValueOK(); sok {
			whenMatched = s
		} else {
			whenMatched = ""
		}
	}
	if m, err := doc.LookupErr("whenNotMatched"); err == nil {
		if s, sok := m.StringValueOK(); sok {
			whenNotMatched = s
		}
	}
	return whenMatched, whenNotMatched
}

// mergeResult is the document the merge will leave behind, and whether its keys
// are this guard's business at all.
//
// Only the fields the pipeline projected are present, which is every field the
// unique indexes are built on and every field their conditions read. That is
// what a key is made of, so a projection is enough and the whole document is
// not needed.
func mergeResult(produced, existing bson.Raw, matched bool, whenMatched, whenNotMatched string) (bson.Raw, bool) {
	if !matched {
		// discard writes nothing, and fail is mongod's own error.
		return produced, whenNotMatched == "insert"
	}
	switch whenMatched {
	case "replace":
		return produced, true
	case "merge":
		return overlay(existing, produced), true
	default:
		// keepExisting changes nothing; fail is mongod's own error; a pipeline
		// computes a value this cannot know. None of them is an insert of a new
		// key, which is what was measured going wrong.
		return nil, false
	}
}

// overlay lays produced over existing, which is what whenMatched "merge" does
// to the fields both carry.
func overlay(existing, produced bson.Raw) bson.Raw {
	out := bson.D{}
	seen := map[string]bool{}
	if pel, err := produced.Elements(); err == nil {
		for _, e := range pel {
			seen[e.Key()] = true
			out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
		}
	}
	if eel, err := existing.Elements(); err == nil {
		for _, e := range eel {
			if seen[e.Key()] {
				continue
			}
			out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
		}
	}
	raw, err := bson.Marshal(out)
	if err != nil {
		return produced
	}
	return raw
}

// onKey identifies a document by the values the merge matches on, as bytes so
// two spellings of one number stay apart and an ObjectId compares as itself.
func onKey(doc bson.Raw, on []string) (string, bool) {
	out := make([]byte, 0, 32)
	for _, f := range on {
		v, ok := lookupPath(doc, f)
		if !ok {
			return "", false
		}
		out = append(out, byte(v.Type))
		out = append(out, v.Value...)
		out = append(out, 0)
	}
	return string(out), true
}

// mergeExisting reads the documents the merge will match, keyed the same way the
// produced ones are. It reads the fast tier, after the copy-up, which is where
// the merge itself will look.
func (c *conn) mergeExisting(ctx context.Context, ns, db string, on []string, keys bson.Raw) (map[string]bson.Raw, error) {
	out := map[string]bson.Raw{}
	if len(keys) == 0 {
		return out, nil
	}
	_, coll := splitNS(ns)
	cur, err := c.r.hot.Database(db).Collection(coll).Find(ctx, keys,
		options.Find().SetLimit(maxCopyUpForWrite))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	for cur.Next(ctx) {
		doc := make(bson.Raw, len(cur.Current))
		copy(doc, cur.Current)
		if k, ok := onKey(doc, on); ok {
			out[k] = doc
		}
	}
	return out, cur.Err()
}
