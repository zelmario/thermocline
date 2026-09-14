package router

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/promote"
	"github.com/zelmario/thermocline/internal/wire"
)

// Two commands that neither guard nor fallback can make transparent, and that
// were going through as though they could.
//
// A rename is the sharper of the two. The feeder ignores renames on purpose:
// replaying collection-level changes is exactly what would let an accident on
// the fast tier reach the archive, and that rule is the foundation everything
// else rests on. But it means a renamed collection moves on one tier and not
// the other. Afterwards, reads of the new name are not tiered at all, because
// the new name is not in feeder.namespaces, so they answer from whatever the
// fast tier holds; and reads of the old name find an empty fast tier and answer
// from an archive that nothing writes to any more. The collection is in two
// halves and neither half says so.
//
// mapReduce is the older problem in a different costume. It reads every
// document it is pointed at, on whichever tier it runs, and it can write its
// output into a collection. Pointed at a fast tier holding three of ten
// documents it produces a total of three and reports success.
//
// Neither can be done across two clusters, so both are refused, which is the
// standing rule here: a refusal is a limitation, a wrong answer is a bug.

// guardDDL refuses the collection-level commands that would split a tiered
// collection between the tiers, and the reads that cannot be bounded.
//
// It reports true when it has answered the client itself.
func (c *conn) guardDDL(ctx context.Context, msg *wire.Message, db, name string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}

	switch name {
	case "renameCollection":
		return c.guardRename(ctx, msg, cmd)

	case "createIndexes", "collMod":
		if out, done := c.guardTTL(ctx, msg, cmd, db, name); done {
			return out, true
		}
		if name != "createIndexes" {
			return nil, false
		}
		// After the TTL refusal, because a TTL index is turned down whatever
		// else it is and walking both tiers first would be a scan for a command
		// that is not going to run.
		return c.guardNewUniqueIndex(ctx, msg, cmd, db)

	case "convertToCapped", "cloneCollectionAsCapped", "create":
		if out, done := c.guardUnmirrorable(ctx, msg, cmd, db, name); done {
			return out, true
		}
		// After it, not before: a time series create carries expireAfterSeconds
		// too, and the reason it is refused is the bucket collection rather
		// than the expiry.
		if out, done := c.guardTTL(ctx, msg, cmd, db, name); done {
			return out, true
		}
		if name == "create" {
			return c.refuseUnackedCreateWithOptions(msg, cmd, db)
		}
		return nil, false

	case "applyOps":
		return c.refuseApplyOps(ctx, msg, cmd, db)

	case "dropIndexes":
		return c.refuseUnackedIndexDrop(msg, cmd, db)

	case "mapReduce", "mapreduce":
		coll, ok := stringField(cmd, name)
		if !ok {
			return nil, false
		}
		// Where it writes is a separate question from what it reads, and the
		// answer is the same either way: it cannot be done across two clusters.
		if out, done := c.refuseMapReduceOutput(msg, cmd, db); done {
			return out, true
		}
		ns := db + "." + coll
		base, _, tiered := c.r.tieredBase(ctx, ns)
		if !tiered {
			return nil, false
		}
		// A complete fast tier can answer it, the same as any other read.
		if mark, known := c.r.completeness(ctx, base); known && mark.Claimed && mark.CoversAll() {
			return nil, false
		}
		c.r.Stats.Incomplete.Add(1)
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a mapReduce over an incomplete collection", "ns", ns)
		out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: refusing this mapReduce because the fast tier does not hold all of "+ns+
				", and mapReduce reads every document it is given, so it would total only the part "+
				"that is here and report success. It cannot be run against the archive either, "+
				"because its output can land in a collection, which would then be written to the "+
				"wrong tier. Warm "+ns+", or express this as an aggregation, which can be.")
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	return nil, false
}

// Renaming a tiered collection moves two collections, not one.
//
// The feeder ignores renames on purpose, because replaying destructive DDL into
// the archive is what would let one accident on the fast tier destroy it. That
// left rename refused outright: the fast tier would have moved and the archive
// would not, so reads of the new name would answer from the fast tier alone and
// reads of the old one from an archive nothing writes to.
//
// Refusing was safe and it is not the same as working. Renaming a collection is
// ordinary, and an application that rotates collections by name cannot use this
// system at all while it is refused.
//
// So it is done properly, when it can be. Both names have to be tiered, because
// the namespace list is configuration and a rename into a name it does not
// carry stops the tiering silently. Mirroring has to be caught up, or writes to
// the old name are still in flight and would land in an archive collection that
// no longer exists. A dropTarget that would destroy archived documents is
// refused, because that is the destructive crossing the whole rule exists to
// stop.
//
// The fast tier does the rename itself, forwarded as the client sent it, so its
// own authorization decides. Only once it has said yes does the archive follow.

// renameMaxLag is how far behind mirroring may be for a rename to be safe.
//
// A write to the old name that has not reached the archive yet has nowhere to
// go once the archive collection is renamed. The window cannot be closed
// entirely from here, so it is made small and stated rather than assumed.
const renameMaxLag = 2 * time.Second

// pendingRename is a rename the fast tier has accepted, waiting for the archive
// to follow.
type pendingRename struct {
	from string
	to   string
}

func (c *conn) guardRename(ctx context.Context, msg *wire.Message, cmd bson.Raw) (*wire.Message, bool) {
	from, ok := stringField(cmd, "renameCollection")
	if !ok {
		return nil, false
	}
	to, _ := lookupString(cmd, "to")
	// renameCollection names both sides in full, as db.collection, because it
	// is run against admin.
	fromTiered, toTiered := c.r.cfg.Manages(from), c.r.cfg.Manages(to)
	if !fromTiered && !toTiered {
		return nil, false
	}

	refuse := func(why string) (*wire.Message, bool) {
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a rename of a tiered collection", "from", from, "to", to, "because", why)
		out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: refusing to rename "+from+" to "+to+": "+why)
		if rerr != nil {
			return nil, false
		}
		return out, true
	}

	if fromTiered != toTiered {
		return refuse("one of the two names is tiered and the other is not, so the collection would " +
			"stop being tiered without anything saying so. Put both names in feeder.namespaces, or " +
			"neither")
	}

	// dropTarget replaces whatever is at the new name. On the fast tier that is
	// the client's business; in the archive it is the one thing that must never
	// happen by accident.
	if boolField(cmd, "dropTarget") {
		if held, err := c.r.archiveHolds(ctx, to); err != nil {
			return refuse("the archive could not be checked, and a dropTarget rename that cannot be " +
				"checked could destroy archived documents: " + err.Error())
		} else if held {
			return refuse("dropTarget would destroy the archived documents of " + to +
				", which no fast tier command is allowed to do. Move or remove them first")
		}
	}

	if mirroring := c.r.mirroringLag(ctx); !mirroring.known {
		return refuse("mirroring lag is unknown, so writes to " + from +
			" may still be in flight and would land in an archive collection that no longer exists")
	} else if mirroring.lag > renameMaxLag {
		return refuse("mirroring is " + mirroring.lag.Round(time.Millisecond).String() + " behind, limit " +
			renameMaxLag.String() + ": writes to " + from + " are still in flight and would land in " +
			"an archive collection that no longer exists. Retry once the feeder has caught up")
	}

	// Everything the archive needs is decided; the fast tier decides the rest,
	// including whether this client may do it at all.
	c.rename = &pendingRename{from: from, to: to}
	return nil, false
}

// archiveHolds reports whether the archive has any live document for ns.
func (r *Router) archiveHolds(ctx context.Context, ns string) (bool, error) {
	db, coll := splitNS(ns)
	err := r.cold.Database(db).Collection(coll).
		FindOne(ctx, bson.D{meta.NotArchivedDeleted()}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// finishRename moves the archive and the bookkeeping after the fast tier has
// accepted a rename.
//
// A failure here leaves the two tiers under different names, which is the split
// this used to refuse outright. It cannot be silent: the log says exactly what
// to run, because an operator with two commands beats a proxy guessing.
func (c *conn) finishRename(ctx context.Context, accepted bool) {
	op := c.rename
	c.rename = nil
	if op == nil {
		return
	}
	if !accepted {
		c.log.Debug("the fast tier refused the rename, so the archive is untouched",
			"from", op.from, "to", op.to)
		return
	}

	fromDB, fromColl := splitNS(op.from)
	err := c.r.cold.Database("admin").RunCommand(ctx, bson.D{
		{Key: "renameCollection", Value: op.from},
		{Key: "to", Value: op.to},
	}).Err()
	switch {
	case err == nil:
		c.log.Info("renamed the archive to follow the fast tier", "from", op.from, "to", op.to)
	case isNamespaceNotFound(err):
		// Nothing has been mirrored yet, so there is nothing to move. The
		// feeder will create the new name on its next write.
		c.log.Info("the archive had nothing under the old name, so nothing to rename",
			"from", op.from, "to", op.to)
	default:
		c.log.Error("the fast tier renamed and the archive did not, so the tiers are split",
			"from", op.from, "to", op.to, "err", err,
			"remedy", "run db.adminCommand({renameCollection:'"+op.from+"',to:'"+op.to+"'}) on the archive")
		return
	}
	_ = fromDB
	_ = fromColl

	// The completeness claim belongs to the collection, not to its name.
	wm := c.r.cold.Database(meta.DB).Collection(meta.Watermarks)
	var mark bson.Raw
	if derr := wm.FindOne(ctx, bson.D{{Key: "_id", Value: op.from}}).Decode(&mark); derr == nil {
		doc := bson.D{}
		if elems, eerr := mark.Elements(); eerr == nil {
			for _, e := range elems {
				if e.Key() == "_id" {
					continue
				}
				doc = append(doc, bson.E{Key: e.Key(), Value: e.Value()})
			}
			if _, uerr := wm.UpdateOne(ctx, bson.D{{Key: "_id", Value: op.to}},
				bson.D{{Key: "$set", Value: doc}}, options.UpdateOne().SetUpsert(true)); uerr != nil {
				c.log.Warn("could not move the completeness watermark; the new name claims nothing "+
					"until something records it", "to", op.to, "err", uerr)
			} else {
				_, _ = wm.DeleteOne(ctx, bson.D{{Key: "_id", Value: op.from}})
			}
		}
	}

	// And this router's own memory of both names, which is now about the wrong
	// collection. The name-keyed caches are not covered by the DDL sweep that
	// handles drop and collMod, because a rename is not in that list: what is
	// cached under the old name describes a collection that has moved, and what
	// is cached under the new one describes whatever used to be called that.
	//
	// A stale claim is the one that matters, because it is the one that decides
	// whether a read may skip the archive. See forgetCompleteness.
	//
	// No reproduction to point at, and that is stated rather than dressed up: a
	// rename does enough work that ClaimCacheTTL expires during it, so the
	// window closes on its own before a client can be shown a wrong answer. The
	// invalidation is here because holding a cache entry that names a
	// collection which has moved is wrong on its face, and because the next
	// change to either side of it should not have to rediscover that the timing
	// happens to save it.
	for _, ns := range []string{op.from, op.to} {
		c.r.forgetCompleteness(ns)
		c.r.forgetIndexes(ns)
		c.r.forgetHint(ns)
		c.r.forgetView(ns)
		c.r.forgetCollation(ns)
		// A rename puts a different collection under each of these names, so
		// the age brackets describe the wrong one: the field a collection is
		// aged by can be typed differently in the collection that has arrived.
		// The same argument forgetDropped makes for a drop and a recreate.
		c.r.forgetBrackets(ns)
		// And the indexer's memory, which is the one that does not correct
		// itself. It remembers that a namespace's collection exists on the fast
		// tier with the archive's options and its indexes built. After a rename
		// neither name means what it meant: nothing is under the old one, and
		// something else is under the new. A later write to the old name then
		// creates the collection implicitly, with no collation, no validator
		// and no index but _id_, and a unique index the archive still carries
		// stops being enforced on either tier. That consequence is not
		// hypothetical -- it is the one measured for a drop the router never
		// saw, and written up in docs/router.md.
		c.r.indexer.Forget(ns)
	}
	// This list is forgetDropped's, minus one deliberate difference: that one
	// uses staleIndexes where this uses forgetIndexes above, because a rename
	// leaves the new name holding a real collection whose listing should be
	// re-read rather than merely aged out. Everything else has to match, and
	// two of these were missing until the lists were compared. See
	// forgetEverythingIn, which was made to delegate for exactly this reason:
	// two lists disagreeing about what a name still means is how the next
	// reader gets it wrong.
	//
	// No reproduction for the two added here, and that is stated rather than
	// dressed up. This router's own fast tier watch carries the rename a moment
	// later and calls forgetDropped for both names, which closes the window on
	// an ordinary deployment. It does not close it where that watch is down --
	// a privilege the router's account lacks, or a tier it cannot reach -- and
	// that is a state the router already reports at ERROR and docs/router.md
	// already describes. Correctness here should not rest on a stream that is
	// documented as optional.

	// Unique key claims are the registry's, and they name the collection.
	if _, kerr := c.r.keys().UpdateMany(ctx,
		bson.D{{Key: "ns", Value: op.from}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "ns", Value: op.to}}}}); kerr != nil {
		c.log.Warn("could not move the unique key claims, so a value freed under the old name may "+
			"still look taken", "from", op.from, "to", op.to, "err", kerr)
	}

	// Residency records carry the namespace inside their own _id, so they are
	// removed rather than rewritten. The cost is that documents promoted moments
	// ago lose their hold and may be evicted early, which is a delay and not a
	// loss: the archive has every one of them.
	if _, rerr := c.r.cold.Database(meta.DB).Collection(meta.Residency).
		DeleteMany(ctx, bson.D{{Key: "ns", Value: op.from}}); rerr != nil {
		c.log.Warn("could not clear residency for the old name", "ns", op.from, "err", rerr)
	}

	c.r.forgetIndexes(op.from)
	c.r.forgetIndexes(op.to)
	c.r.forgetHint(op.from)
	c.r.forgetHint(op.to)
	// Both names: the old one no longer holds a collection, and the new one
	// holds a different collection from whatever the indexer last saw there.
	c.r.indexer.Forget(op.from)
	c.r.indexer.Forget(op.to)
	c.r.forgetBrackets(op.from)
	c.r.forgetBrackets(op.to)
	c.r.forgetView(op.from)
	c.r.forgetView(op.to)
}

// isNamespaceNotFound reports whether an error is mongod saying the source
// collection does not exist.
func isNamespaceNotFound(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Code == 26 // NamespaceNotFound
	}
	return false
}

// A TTL index on a tiered collection does the opposite of what it says.
//
// The fast tier holds the recent part of the collection and the archive holds
// the rest, and this command reaches the fast tier. So the index expires the
// documents that were recent enough to be cached, the feeder reads those
// expiries as the application deleting them and the archive tombstones its own
// copies, and the older documents the rule was written for are in the archive
// where no TTL index is watching them.
//
// Measured on a tiered collection with five documents a minute old on both
// tiers and five over a year old in the archive alone, asking through the
// router for anything older than thirty seconds to expire: the five recent ones
// were destroyed on both tiers and the five year-old ones survived. A retention
// rule that deletes this month and keeps last year, silently, and it reported
// success.
//
// So it is refused, which is the standing rule here: a refusal is a limitation,
// a wrong answer is a bug. A collection nothing tiers is not affected, because
// it lives on the fast tier alone and a TTL index there means exactly what it
// says.
//
// The same goes for a collMod that turns an existing index into a TTL one,
// which is the same instruction written differently.
func (c *conn) guardTTL(ctx context.Context, msg *wire.Message, cmd bson.Raw, db, name string) (*wire.Message, bool) {
	coll, ok := stringField(cmd, name)
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if _, _, tiered := c.r.tieredBase(ctx, ns); !tiered {
		return nil, false
	}
	if !asksForTTL(cmd, name) {
		return nil, false
	}

	// Named the way the client spelled it. A clustered collection's TTL is not
	// an index the client created, and telling somebody who sent a create that
	// their TTL index is refused leaves them looking for an index they never
	// asked for.
	what := "a TTL index on " + ns
	if _, err := cmd.LookupErr("expireAfterSeconds"); err == nil && (name == "create" || name == "collMod") {
		what = "expireAfterSeconds on " + ns + ", which is a clustered collection's own TTL"
	}

	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a TTL on a tiered collection", "ns", ns, "command", name, "as", what)
	out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing "+what+", because it would delete the newest "+
			"documents and keep the oldest. This command reaches the fast tier, which holds "+
			"only the recent part of "+ns+": those copies would expire and be recorded as "+
			"deletions in the archive, while the older documents the rule is written for sit "+
			"in the archive where no TTL index is watching them. If "+ns+" really should have "+
			"a retention rule, put the index on the archive yourself and understand that the "+
			"archive is what keeps this data.")
	if rerr != nil {
		return nil, false
	}
	return out, true
}

// asksForTTL reports whether a create, createIndexes or collMod would leave a
// TTL index behind.
//
// Three spellings, and the third is not an index in a list at all. A clustered
// collection's TTL is a top-level expireAfterSeconds on the create that makes
// it, and on any collMod that changes it afterwards, so a check that reads a
// createIndexes command's `indexes` array and a collMod's `index` document
// never sees it. mongod expires those documents exactly as it expires a capped
// collection's oldest, and nothing announces either.
//
// Measured on PSMDB 8.0.26: a collection created with
// {clusteredIndex: {key: {_id: 1}, unique: true}, expireAfterSeconds: 1} and
// twenty documents whose _id values were dates an hour old was empty 45 seconds
// later, with nothing else running.
func asksForTTL(cmd bson.Raw, name string) bool {
	if name == "create" || name == "collMod" {
		if v, err := cmd.LookupErr("expireAfterSeconds"); err == nil {
			// "off" removes the expiry rather than asking for one, and it is
			// the command an operator needs to undo this on a collection that
			// already has it. Refusing that would leave them with a tiered
			// collection quietly expiring and no way through the router to
			// stop it.
			if sv, sok := v.StringValueOK(); sok && sv == "off" {
				return false
			}
			return true
		}
	}
	if name == "create" {
		// Nothing else on a create can leave one behind: an index list is not
		// part of the command.
		return false
	}
	if name == "collMod" {
		idx, err := cmd.LookupErr("index")
		if err != nil {
			return false
		}
		doc, ok := idx.DocumentOK()
		if !ok {
			return false
		}
		_, err = doc.LookupErr("expireAfterSeconds")
		return err == nil
	}
	v, err := cmd.LookupErr("indexes")
	if err != nil {
		return false
	}
	specs, err := v.Array().Values()
	if err != nil {
		return false
	}
	for _, spec := range specs {
		doc, ok := spec.DocumentOK()
		if !ok {
			continue
		}
		if _, err := doc.LookupErr("expireAfterSeconds"); err == nil {
			return true
		}
	}
	return false
}

// Turning a tiered collection into one the feeder cannot mirror.
//
// The feeder refuses a capped or time series namespace at startup, and says
// why: a capped collection removes its own oldest documents when it fills and
// nothing announces those, so the archive would tombstone the history it is
// there to keep. That check runs when the feeder starts. Nothing stopped a
// client turning a collection into one afterwards.
//
// Measured on a tiered collection holding 200 documents of about a kilobyte,
// with convertToCapped to 4KB through the router. Three things happened and the
// command reported success:
//
//   - the fast tier went from 200 documents to 4, immediately
//   - the feeder then refused to start at all, so mirroring stopped for every
//     namespace, not just this one
//   - and once anything claimed the fast tier was complete, which the warmer
//     does routinely, the router answered countDocuments with 4 where the
//     archive holds 200
//
// The archive survived the conversion itself, because MongoDB rebuilds the
// collection rather than deleting its documents one by one, and the feeder
// ignores that. It would not survive what came next: every insert into a full
// capped collection pushes out the oldest, and those are ordinary deletes that
// nothing announces.
//
// So it is refused. create is only refused when it asks for capped or time
// series, because create is otherwise an ordinary thing to send at a namespace
// that already exists.
func (c *conn) guardUnmirrorable(ctx context.Context, msg *wire.Message, cmd bson.Raw, db, name string) (*wire.Message, bool) {
	coll, kind, why, ok := unmirrorableTarget(cmd, name)
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if _, _, tiered := c.r.tieredBase(ctx, ns); !tiered {
		return nil, false
	}

	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a command that would make a tiered collection unmirrorable",
		"ns", ns, "command", name, "kind", kind)
	out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing to make "+ns+" "+kind+", because the feeder cannot mirror one: "+
			why+". The feeder refuses to start against one for the same reason, so this would "+
			"stop mirroring for every namespace behind it as well.")
	if rerr != nil {
		return nil, false
	}
	return out, true
}

// unmirrorableTarget names the collection a command would make unmirrorable,
// what it would become, and why that cannot be mirrored.
//
// The kinds are the feeder's, and they have to stay the feeder's. Its startup
// check refuses five things and this used to refuse two of them, so a client
// could make a tiered namespace into a third and the next feeder restart would
// refuse to run at all, which stops mirroring for every namespace rather than
// for the one somebody changed. The reachable shape is an adoption: create
// fails with NamespaceExists once the fast tier holds the collection, and in an
// adoption it does not hold it yet.
//
// Kept as prose here rather than shared with internal/feeder/collkind.go,
// because the two answer different questions: that one reads a listCollections
// entry describing what a collection *is*, this one reads a command describing
// what one would *become*, and the fields are spelled differently in each.
func unmirrorableTarget(cmd bson.Raw, name string) (coll, kind, why string, ok bool) {
	const cappedWhy = "a capped collection removes its own oldest documents when it fills and " +
		"nothing announces those, so the archive would record them as deletions and lose the " +
		"history it is there to keep, and the fast tier would be truncated the moment this ran " +
		"while it still holds documents the archive is waiting for"
	switch name {
	case "convertToCapped":
		if v, vok := stringField(cmd, "convertToCapped"); vok {
			return v, "a capped collection", cappedWhy, true
		}
	case "cloneCollectionAsCapped":
		// The target is the new collection, not the one being read.
		if v, vok := stringField(cmd, "toCollection"); vok {
			return v, "a capped collection", cappedWhy, true
		}
	case "create":
		v, vok := stringField(cmd, "create")
		if !vok {
			return "", "", "", false
		}
		if safeBool(cmd.Lookup("capped")) {
			return v, "a capped collection", cappedWhy, true
		}
		if _, terr := cmd.LookupErr("timeseries"); terr == nil {
			return v, "a time series collection",
				"a time series collection keeps its documents in a system bucket collection, " +
					"which is not the collection the change stream describes", true
		}
		if _, eerr := cmd.LookupErr("encryptedFields"); eerr == nil {
			return v, "encrypted with queryable encryption",
				"the state that makes its fields searchable lives in companion enxcol_ " +
					"collections that nothing here tiers, so the archive would hold the " +
					"documents without it and an encrypted read that falls through would be " +
					"answered from documents whose index state was never mirrored", true
		}
		if _, verr := cmd.LookupErr("viewOn"); verr == nil {
			return v, "a view",
				"a view holds no documents of its own, only a pipeline over another collection, " +
					"so there is nothing to mirror and the collection it reads from is what " +
					"wants tiering", true
		}
	}
	return "", "", "", false
}

// safeBool reads a flag the way mongod reads create's capped option.
//
// MongoDB declares that one safeBool in its IDL, which accepts a number as well
// as a boolean and calls anything non-zero true. BooleanOK does not, so
// {create: "orders", capped: 1, size: 4096} made a capped collection while this
// guard read no boolean and let it through, which is the capped refusal
// reopened by a type.
//
// Measured on PSMDB 8.0.26: capped as an int32 1, as a long 1 and as a double
// 1.0 each created a capped collection. The same latitude is not offered
// elsewhere and this must not be used as though it were: renameCollection's
// dropTarget and an update's upsert are plain bools, and mongod refuses a
// number for either with "the wrong type 'int', expected type 'bool'". A guard
// that coerced those would be accepting commands the server never would.
func safeBool(v bson.RawValue) bool {
	if b, ok := v.BooleanOK(); ok {
		return b
	}
	if n, ok := intOf64(v); ok {
		return n != 0
	}
	return false
}

// pendingIndexDrop is a unique index the fast tier is being asked to drop,
// waiting to see whether it did.
type pendingIndexDrop struct {
	ns    string
	names []string
}

// noteIndexDrop records the unique indexes a dropIndexes would remove, before
// the fast tier removes them and the answer is no longer readable.
//
// Only unique ones. A plain index is a decision about speed and the two tiers
// are allowed to differ: cline indexes exists to report that, and an operator
// may keep one on the archive for the reads that fall through. A unique index
// is a constraint, and a constraint that holds on one tier and not the other is
// not a slower system, it is a different one.
func (c *conn) noteIndexDrop(ctx context.Context, cmd bson.Raw, db, coll string) {
	c.indexDrop = nil
	ns := db + "." + coll
	if _, _, tiered := c.r.tieredBase(ctx, ns); !tiered {
		return
	}
	unique, err := uniqueIndexNames(ctx, c.r.hot, db, coll)
	if err != nil || len(unique) == 0 {
		return
	}

	var names []string
	switch v := cmd.Lookup("index"); v.Type {
	case bson.TypeString:
		name, _ := v.StringValueOK()
		if name == "*" {
			for n := range unique {
				names = append(names, n)
			}
		} else if _, ok := unique[name]; ok {
			names = append(names, name)
		}
	case bson.TypeArray:
		vals, aerr := v.Array().Values()
		if aerr != nil {
			return
		}
		for _, e := range vals {
			if name, sok := e.StringValueOK(); sok {
				if _, uok := unique[name]; !uok {
					continue
				}
				names = append(names, name)
			}
		}
	case bson.TypeEmbeddedDocument:
		// By key pattern rather than by name. Resolving that would mean
		// comparing key documents, so the archive is told to drop the same
		// pattern and MongoDB does the resolving.
		names = append(names, "")
	}
	if len(names) == 0 {
		return
	}
	sort.Strings(names)
	c.indexDrop = &pendingIndexDrop{ns: ns, names: names}
}

// finishIndexDrop drops from the archive the unique indexes the fast tier just
// dropped.
//
// The router enforces uniqueness from the fast tier's indexes, so once one is
// gone from there it accepts values the archive still refuses. The feeder then
// cannot mirror those documents: it says so, once, and leaves them on the fast
// tier alone, where the evictor will not touch them either because a document
// the archive does not hold is never evicted. Measured before this: a unique
// index dropped through the router, then the duplicate the application was now
// entitled to write, and afterwards the fast tier held two documents and the
// archive held one.
//
// Leaving it also made the constraint come back. The indexer builds the fast
// tier's indexes from the archive's, so the next router start put it back, and
// a constraint that is off now and on again after a restart is worse than
// either.
//
// A failure here is said out loud with the command to run, the same as a
// rename: an operator with one command beats a proxy guessing.
func (c *conn) finishIndexDrop(ctx context.Context, accepted bool, cmd bson.Raw) {
	op := c.indexDrop
	c.indexDrop = nil
	if op == nil || !accepted {
		return
	}
	db, coll := splitNS(op.ns)

	spec := bson.RawValue{}
	if len(op.names) == 1 && op.names[0] == "" {
		spec = cmd.Lookup("index")
	}
	drop := bson.D{{Key: "dropIndexes", Value: coll}}
	if spec.Type != 0 {
		drop = append(drop, bson.E{Key: "index", Value: spec})
	} else {
		drop = append(drop, bson.E{Key: "index", Value: op.names})
	}

	err := c.r.cold.Database(db).RunCommand(ctx, drop).Err()
	switch {
	case err == nil:
		c.log.Info("dropped the same unique index from the archive",
			"ns", op.ns, "indexes", op.names,
			"why", "uniqueness is enforced from the fast tier's indexes, so an archive that "+
				"still refuses these values would stop the documents being mirrored at all")
	case isIndexNotFound(err) || isNamespaceNotFound(err):
		c.log.Debug("the archive did not have that index", "ns", op.ns, "indexes", op.names)
	default:
		c.log.Error("the fast tier dropped a unique index and the archive kept it",
			"ns", op.ns, "indexes", op.names, "err", err,
			"effect", "a value the router now accepts will be refused by the archive, and that "+
				"document will stay on the fast tier alone",
			"remedy", "run db."+coll+".dropIndex(...) on the archive for these")
	}
}

// isIndexNotFound reports whether an error is MongoDB saying there is no such
// index, which is the ordinary answer when the archive never had it.
func isIndexNotFound(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Name == "IndexNotFound" || ce.Code == 27
	}
	return false
}

// refuseMapReduceOutput refuses a mapReduce that would write into a tiered
// collection.
//
// The guard above asks whether the collection being read is complete. Nothing
// asked where the output goes, and every way it can go is wrong here. `replace`
// is implemented the way $out is, by building the result beside the target and
// renaming over it, and a rename is not mirrored, so the result would reach the
// fast tier and nothing else. `merge` and `reduce` write per document, so they
// meet a fast tier holding part of the collection: an upsert against a document
// that lives only in the archive creates one in its place, and the feeder then
// mirrors that over the archived record.
//
// $out has all of this handled, in aggwrite.go, because an aggregation says what
// it produces in a form this router can run itself. A mapReduce says it in
// JavaScript. So this is a refusal, which is the standing rule: a refusal is a
// limitation, a wrong answer is a bug.
func (c *conn) refuseMapReduceOutput(msg *wire.Message, cmd bson.Raw, db string) (*wire.Message, bool) {
	target, ok := mapReduceOutput(cmd, db)
	if !ok || !c.r.cfg.Manages(target) {
		return nil, false
	}
	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a mapReduce writing into a tiered collection", "into", target)
	out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this mapReduce because its output collection "+target+" is tiered. "+
			"A replacing output is written beside the target and renamed over it, and a rename is "+
			"not mirrored, so the result would reach the fast tier and nothing else; a merging one "+
			"writes per document against a tier that holds only part of the collection. Express it "+
			"as an aggregation ending in $out or $merge, which this router performs on both tiers, "+
			"or send the output to a collection nothing tiers.")
	if rerr != nil {
		return nil, false
	}
	return out, true
}

// refuseApplyOps turns down an applyOps that writes into a tiered collection.
//
// applyOps applies oplog entries straight to a collection. It passes none of
// the guards a write command passes: no _id checked against the archive, no
// unique key claimed, no archived document brought up first. Measured through
// the router with {_id: 800, v: "archived, precious", tier: "gold"} in the
// archive and nothing on the fast tier: an applyOps insert of that _id was
// applied, and six seconds later the archive held {_id: 800, v: "clobber via
// applyOps"}. The same insert sent as an insert is refused.
//
// Refused rather than guarded. Guarding it means reimplementing every one of
// those checks against an internal command whose entries may be inserts,
// updates, deletes or commands in any mixture, for a command MongoDB documents
// as internal and no application sends. A refusal is a limitation; a wrong
// answer is a bug, and this one loses data.
//
// Only when it touches a tiered namespace. An applyOps against anything else is
// none of this system's business and is forwarded whole.
func (c *conn) refuseApplyOps(ctx context.Context, msg *wire.Message, cmd bson.Raw, db string) (*wire.Message, bool) {
	_ = ctx
	arr, err := cmd.LookupErr("applyOps")
	if err != nil {
		return nil, false
	}
	entries, aok := arr.ArrayOK()
	if !aok {
		return nil, false
	}
	vals, aerr := entries.Values()
	if aerr != nil {
		return nil, false
	}
	touched := ""
	for _, v := range vals {
		doc, ok := v.DocumentOK()
		if !ok {
			continue
		}
		ns, nok := stringField(doc, "ns")
		if !nok || ns == "" {
			continue
		}
		// A command entry names db.$cmd, and the collection it acts on is the
		// value of the command itself: {op: "c", ns: "shop.$cmd", o: {drop:
		// "orders"}}. Left alone, an applyOps carrying a create or a drop of a
		// tiered collection would walk past the guards above that refuse those
		// same commands when they are sent as commands.
		if edb, isCmd := strings.CutSuffix(ns, ".$cmd"); isCmd {
			if inner, ok := doc.Lookup("o").DocumentOK(); ok {
				if elems, eerr := inner.Elements(); eerr == nil && len(elems) > 0 {
					if coll, cok := elems[0].Value().StringValueOK(); cok && coll != "" {
						if c.r.cfg.Manages(edb + "." + coll) {
							touched = edb + "." + coll
							break
						}
					}
				}
			}
			continue
		}
		if c.r.cfg.Manages(ns) {
			touched = ns
			break
		}
	}
	if touched == "" {
		return nil, false
	}
	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused an applyOps writing into a tiered collection", "ns", touched)
	out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this applyOps because "+touched+" is tiered. applyOps applies oplog "+
			"entries straight to the collection, so it passes none of the checks a write command "+
			"passes: an _id or a unique value the archive still holds is not seen, and the archived "+
			"document is then overwritten by what the feeder mirrors. Send the same change as an "+
			"ordinary write, which this router resolves across both tiers.")
	if rerr != nil {
		return nil, false
	}
	return out, true
}

// mapReduceOutput reads the namespace a mapReduce would write into, and whether
// it writes into one at all. `out: {inline: 1}` returns the result to the client
// and touches no collection.
func mapReduceOutput(cmd bson.Raw, db string) (string, bool) {
	v, err := cmd.LookupErr("out")
	if err != nil {
		return "", false
	}
	if coll, isStr := v.StringValueOK(); isStr {
		return db + "." + coll, true
	}
	doc, isDoc := v.DocumentOK()
	if !isDoc {
		return "", false
	}
	outDB := db
	if d, derr := doc.LookupErr("db"); derr == nil {
		if s, sok := d.StringValueOK(); sok && s != "" {
			outDB = s
		}
	}
	for _, mode := range []string{"replace", "merge", "reduce"} {
		if mv, merr := doc.LookupErr(mode); merr == nil {
			if s, sok := mv.StringValueOK(); sok && s != "" {
				return outDB + "." + s, true
			}
		}
	}
	return "", false
}

// refuseUnackedIndexDrop turns down a dropIndexes on a tiered collection that
// the client is not waiting for an answer to.
//
// A unique index dropped here has one to drop behind it on the archive, or the
// archive goes on refusing values the router now accepts. Both halves of that
// live on the reply: noteIndexDrop reads which indexes are going before the
// fast tier removes them, because afterwards there is nothing left to ask, and
// finishIndexDrop drops them on the archive only once the reply says the fast
// tier accepted it. With moreToCome set there is no reply, so the second half
// can never run, and doing it before forwarding would drop the archive's index
// for a command the fast tier might turn down.
//
// Measured in the adoption shape, the archive carrying its own unique index,
// before this:
//
//	acknowledged     both tiers end with _id_, and both duplicates mirror
//	unacknowledged   the fast tier ends with _id_ and the archive keeps code_1,
//	                 so of the two documents the application was now entitled
//	                 to write, one reached the archive and one did not
//
// That second document lives on the fast tier alone. Nothing tells the
// application, and the evictor will not remove a document the archive does not
// hold, so it is not lost; the archive is simply incomplete from then on.
//
// Refused rather than guarded, which is the applyOps reasoning: a refusal is a
// limitation and a wrong answer is a bug. Nothing here is lost by refusing,
// because an operator dropping an index is present to see that it is still
// there, and no driver sends DDL fire-and-forget.
func (c *conn) refuseUnackedIndexDrop(msg *wire.Message, cmd bson.Raw, db string) (*wire.Message, bool) {
	if !msg.MoreToCome() {
		return nil, false
	}
	coll, ok := stringField(cmd, "dropIndexes")
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}
	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused an unacknowledged dropIndexes on a tiered collection",
		"ns", ns,
		"reason", "the archive's copy of the index is dropped only once the reply says the fast tier dropped its own, and this command has no reply",
		"effect", "the index is still there on both tiers; nothing was changed",
		"action", "send the same dropIndexes without writeConcern {w: 0}")
	out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this dropIndexes because "+ns+" is tiered and the command asks for no "+
			"acknowledgement. The archive's copy of the index is dropped behind the fast tier's, and "+
			"only the reply says whether the fast tier dropped it. Send it without writeConcern {w: 0}.")
	if rerr != nil {
		return nil, false
	}
	return out, true
}

// refuseUnackedCreateWithOptions turns down a create that carries options the
// archive needs and asks for no answer.
//
// A collection's options are carried down by promote.MirrorCollection, which
// runs on the reply to the create. With moreToCome there is no reply, so the
// archive's copy is left to the feeder's first mirrored insert, and that
// carries no options at all. MirrorCollection's own failure message already
// names what that costs: "the archive's copy will be created by the first
// mirrored insert, with no options, and a collation decides which documents
// match".
//
// Measured before this, create with collation {locale: "en", strength: 2} on a
// tiered namespace, with a control:
//
//	acknowledged     both tiers en/2, and find({name: "ALICE"}) is 1 on both
//	unacknowledged   fast tier en/2 and the archive none, and the same find is
//	                 1 on the fast tier and 0 on the archive
//
// So the answer depends on which tier answers, and a collation is fixed at
// creation: it cannot be added to a collection that already holds documents.
// This is the one in this family that never heals.
//
// Only a create that carries something worth carrying is refused. A bare
// create is let through, because the collection the feeder's first insert
// makes is the same collection. Which options those are is asked of promote
// rather than listed again here.
func (c *conn) refuseUnackedCreateWithOptions(msg *wire.Message, cmd bson.Raw, db string) (*wire.Message, bool) {
	if !msg.MoreToCome() {
		return nil, false
	}
	coll, ok := stringField(cmd, "create")
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}
	elems, err := cmd.Elements()
	if err != nil {
		return nil, false
	}
	var carried []string
	for _, e := range elems {
		k := e.Key()
		// The command's own name, and the envelope every command carries.
		// Neither describes the collection.
		if k == "create" || sessionFields[k] || k == "comment" || k == "maxTimeMS" {
			continue
		}
		if promote.CarriedToTheArchive(k) {
			carried = append(carried, k)
		}
	}
	if len(carried) == 0 {
		return nil, false
	}
	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused an unacknowledged create that carries options the archive needs",
		"ns", ns, "options", carried,
		"reason", "the archive's copy is created with these options on the reply to this command, and this command has no reply",
		"effect", "nothing was created; without this the archive would get the collection from the first mirrored insert, with none of them",
		"action", "send the same create without writeConcern {w: 0}")
	out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this create because "+ns+" is tiered, it carries "+strings.Join(carried, ", ")+
			", and it asks for no acknowledgement. The archive's copy is created with those options on the reply "+
			"to this command; without one it would be created by the first mirrored insert carrying none of them, "+
			"and a collation cannot be added afterwards. Send it without writeConcern {w: 0}.")
	if rerr != nil {
		return nil, false
	}
	return out, true
}
