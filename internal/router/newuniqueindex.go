package router

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// A unique index on a tiered collection is declared over documents the fast
// tier does not have, and neither half of that was handled.
//
// **The documents already in the archive were never claimed.** They are the only
// ones the constraint needs help with: the fast tier's own index enforces itself
// over what the fast tier holds, and the registry exists for the rest. A
// collection that has been running for a year has most of its documents in the
// archive, and none of them had ever been written through a router that knew
// about this index. Measured with the index created after the document was
// archived: the archive holds held@x.com, the index is created through the
// router, and an insert of a second held@x.com is accepted. It was written down
// -- the rollout list says to run `cline keys --rebuild` afterwards -- and a
// documented step is not a guarantee.
//
// **And the build itself was allowed over data that already violated it.**
// mongod refuses createIndex with unique when two live documents share a key.
// Split the pair across the tiers and the fast tier's build sees only one of
// each, so it succeeds. Measured: two documents with sku "x", one on each tier,
// and mongod refuses the same pair in one collection with 11000 while the router
// answers ok. The application is then told a constraint holds that does not, and
// nothing will ever tell it otherwise.
//
// Both are answered by the same walk, and it runs **before** the build rather
// than after it. Before, because that is the only order in which the answer can
// be a refusal: an index already built cannot be un-built without taking away
// something the client was told it had. The cost is that createIndexes waits for
// a walk of both tiers, which is the honest price of the guarantee and is next
// to a build that scans the fast tier anyway.

// guardNewUniqueIndex claims the keys of the documents ns already holds, and
// refuses the build if two of them share one.
//
// It reports true when it has answered the client itself.
func (c *conn) guardNewUniqueIndex(ctx context.Context, msg *wire.Message, cmd bson.Raw, db string) (*wire.Message, bool) {
	coll, ok := stringField(cmd, "createIndexes")
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}
	idxs := proposedUniqueIndexes(msg)
	if len(idxs) == 0 {
		return nil, false
	}
	return c.claimForUniqueIndex(ctx, msg, ns, idxs)
}

// claimForUniqueIndex walks both tiers, claims the keys of every document ns
// already holds under idxs, and refuses when the two tiers between them already
// break the constraint.
//
// Shared by the two doors into the same state. createIndexes with unique: true
// is the obvious one; a collMod that turns an existing index unique is the
// other, and it reached none of this until it was measured doing so.
func (c *conn) claimForUniqueIndex(ctx context.Context, msg *wire.Message, ns string,
	idxs []uniqueIndex) (*wire.Message, bool) {

	// Its own deadline. The client's context lasts as long as the client is
	// willing to wait, and abandoning the walk halfway leaves the registry
	// holding rows for an index that is about to be built anyway.
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), backfillLimit)
	defer cancel()

	started := time.Now()
	c.log.Info("a unique index is being created on a tiered collection, checking the documents "+
		"already there and claiming their keys",
		"ns", ns, "indexes", indexNames(idxs),
		"why", "the fast tier's own build sees only the documents the fast tier holds, so a "+
			"duplicate whose other half is in the archive would not stop it, and the archive's "+
			"documents have no claim on their keys until something makes one",
		"note", "this walks both tiers, so the command waits for it")

	claimed, conflicts, clash, err := c.r.claimExistingKeys(bg, ns, idxs)
	if errors.Is(err, errArchiveBehind) {
		// Not built over keys it could not judge. See clashIsFreshOrBehind.
		c.r.releaseIndexClaims(ns, indexNames(idxs))
		if out, rerr := c.staleRefusal(msg, err); rerr == nil {
			return out, true
		}
		return nil, false
	}
	if err != nil {
		// The same stance the write paths take when the registry cannot be
		// reached: refusing a command because a bookkeeping collection could
		// not answer is a worse failure than the one being prevented.
		c.log.Warn("could not check a new unique index against the documents already there, "+
			"letting it through",
			"ns", ns, "err", err, "claimed", claimed,
			"effect", "the index is built over the fast tier alone, and a duplicate whose other "+
				"half is in the archive is neither refused now nor refused later",
			"action", "run cline keys --rebuild, which reports any duplicates it finds")
		return nil, false
	}
	if conflicts == 0 {
		c.log.Info("claimed the existing keys for a new unique index",
			"ns", ns, "claimed", claimed, "took", time.Since(started).String())
		// Held so a build the fast tier rejects gives them back, the same way an
		// insert's keys are.
		c.pendingIndexKeys = append(c.pendingIndexKeys, indexNames(idxs)...)
		c.pendingIndexNS = ns
		return nil, false
	}

	// Refused, and the rows this walk wrote go back: the index is not going to
	// exist, so a claim under its name would sit there until something created
	// an index of that name again and found its keys already taken.
	c.r.releaseIndexClaims(ns, indexNames(idxs))
	c.r.Stats.DuplicateKeys.Add(1)
	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a unique index over documents that already break it",
		"ns", ns, "indexes", indexNames(idxs), "conflicts", conflicts, "key", clash,
		"why", "the fast tier would have built it, because the duplicates are split across the "+
			"tiers and its own build sees only the half that is here",
		"action", "resolve the duplicates and create the index again; cline keys --rebuild "+
			"reports them")
	// 11000 rather than the generic refusal code the rest of this router uses.
	// This is the one refusal that is not a limitation being announced: it is
	// the answer mongod itself gives to the same command over the same
	// documents, and an application that handles a failed unique index build
	// already handles it. Sending CommandFailed here would make the tiered pair
	// behave differently from the collection it is pretending to be, which is
	// the whole thing this router exists not to do.
	out, rerr := c.r.duplicateKeyError(msg.Header.RequestID, msg.Header.RequestID,
		"E11000 duplicate key error collection: "+ns+" index: "+indexNames(idxs)[0]+
			" dup key: "+clash+" :: thermocline: refusing to build this unique index, because "+
			ns+" already holds "+strconv.Itoa(conflicts)+" key(s) held by more than one live "+
			"document. "+
			"The fast tier would have built it: the duplicates are split across the tiers and "+
			"its own build sees only the half that is here, so the index would exist and the "+
			"constraint would not. Resolve the duplicates and create it again; cline keys "+
			"--rebuild reports them.")
	if rerr != nil {
		return nil, false
	}
	return out, true
}

// releaseIndexKeysIfBuildFailed gives the claims back when the build the walk
// ran for did not happen.
//
// The keys were claimed before the command was forwarded, which is the only
// order that lets a duplicate be refused rather than discovered afterwards. A
// build the fast tier then turns down for its own reasons -- a name already
// used, a key pattern it will not accept -- would otherwise leave the registry
// enforcing an index nobody has.
func (c *conn) releaseIndexKeysIfBuildFailed(ctx context.Context, reply *wire.Message) {
	names, ns := c.pendingIndexKeys, c.pendingIndexNS
	c.pendingIndexKeys, c.pendingIndexNS = nil, ""
	if len(names) == 0 {
		return
	}
	if reply != nil {
		if rdoc, err := reply.ReplyDoc(); err == nil && replyOK(rdoc) {
			return
		}
	}
	c.log.Info("a unique index build was refused by the fast tier, giving its claims back",
		"ns", ns, "indexes", names)
	c.r.releaseIndexClaims(ns, names)
}

// proposedUniqueIndexes reads the unique indexes a createIndexes asks for.
//
// Through AllOf rather than the command document, because a driver may send the
// specifications in a payload section rather than inline and both are ordinary.
func proposedUniqueIndexes(msg *wire.Message) []uniqueIndex {
	if msg == nil {
		return nil
	}
	var out []uniqueIndex
	for _, spec := range msg.AllOf("indexes") {
		u, uerr := spec.LookupErr("unique")
		if uerr != nil {
			continue
		}
		if b, ok := u.BooleanOK(); !ok || !b {
			continue
		}
		key, kok := spec.Lookup("key").DocumentOK()
		if !kok {
			continue
		}
		elems, eerr := key.Elements()
		if eerr != nil || len(elems) == 0 {
			continue
		}
		name, _ := lookupString(spec, "name")
		if name == "" {
			// mongod derives one from the key when the client sends none, and
			// so does this: the name is what the registry rows are filed under
			// and what the refusal has to name.
			name = derivedIndexName(elems)
		}
		idx := uniqueIndex{name: name}
		for _, e := range elems {
			idx.fields = append(idx.fields, e.Key())
		}
		if v, err := spec.LookupErr("sparse"); err == nil {
			idx.sparse, _ = v.BooleanOK()
		}
		if v, err := spec.LookupErr("partialFilterExpression"); err == nil {
			if d, ok := v.DocumentOK(); ok {
				idx.partial = d
				idx.partialFields = partialFieldNames(d)
			}
		}
		if v, err := spec.LookupErr("collation"); err == nil {
			if d, ok := v.DocumentOK(); ok {
				idx.collation = d
			}
		}
		out = append(out, idx)
	}
	return out
}

// derivedIndexName is mongod's own default: every field and its direction,
// joined by underscores.
func derivedIndexName(elems []bson.RawElement) string {
	name := ""
	for i, e := range elems {
		if i > 0 {
			name += "_"
		}
		dir := "1"
		if n, ok := intOf64(e.Value()); ok {
			dir = strconv.FormatInt(n, 10)
		} else if s, ok := e.Value().StringValueOK(); ok {
			dir = s
		}
		name += e.Key() + "_" + dir
	}
	return name
}

func indexNames(idxs []uniqueIndex) []string {
	out := make([]string, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, i.name)
	}
	return out
}

// backfillLimit bounds the walk. A collection this does not finish in an hour is
// one whose operator should be running cline keys --rebuild deliberately, with
// the output in front of them, rather than having a command do it quietly.
const backfillLimit = time.Hour
