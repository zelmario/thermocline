package router

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmar/thermocline/internal/config"
	"github.com/zelmar/thermocline/internal/wire"
)

// Aggregations were the largest hole in the completeness guarantee. Every
// driver's countDocuments is an aggregate, so the single most common way an
// application counts anything went past the guard entirely: measured against a
// partly filled fast tier, one returned 1 where the answer was 45,904, and
// reported success.
//
// Two things make an aggregation harder to guard than a find.
//
// The first is that only a leading $match limits which documents are read. A
// $match later in the pipeline filters what earlier stages already scanned, so
// it says nothing about how far back the scan reached, and reading it as a
// bound would let a short answer through.
//
// The second is that a pipeline can read collections the command never names,
// through $lookup, $graphLookup and $unionWith. Those are read by join key
// rather than by time, so no partial watermark covers them: a joined collection
// has to be whole, or the join quietly loses rows.

// metadataStages describe the collection rather than read its documents, so no
// completeness question arises. db.coll.stats() is an aggregate with $collStats
// at the head, and refusing that breaks an ordinary administrative call for no
// benefit: there are no documents in the answer to be short of.
var metadataStages = map[string]bool{
	"$collStats": true, "$indexStats": true, "$planCacheStats": true,
	"$currentOp": true, "$listSessions": true, "$listLocalSessions": true,
	"$shardedDataDistribution": true, "$documents": true,
	// A change stream reads events, not documents, so there is no set of
	// documents for the answer to be short of and the completeness question
	// does not arise. Sending it to the archive instead is not merely wrong,
	// it is impossible: the archive rewrite puts a $match in front of the
	// pipeline to hide tombstones, and $changeStream has to be the first
	// stage, so the archive rejects it and the read is refused. Which is what
	// happened to every change stream on a tiered collection.
	"$changeStream": true,
}

// aggScope is what one aggregate command reads and writes.
type aggScope struct {
	primary   string
	secondary []string
	bound     time.Time
	bounded   bool
	writes    bool
	// match is the leading $match, kept so a transaction can be given the
	// documents it selects rather than being refused.
	match bson.Raw
}

// scopeOfAggregate reads an aggregate command into the parts completeness
// depends on.
//
// ok is false when there is no collection to be complete about, which is the
// database-level `aggregate: 1` form behind $currentOp and $listLocalSessions.
func scopeOfAggregate(cmd bson.Raw, db string) (aggScope, bool) {
	coll, ok := stringField(cmd, "aggregate")
	if !ok {
		return aggScope{}, false
	}
	s := aggScope{primary: db + "." + coll}

	pv, err := cmd.LookupErr("pipeline")
	if err != nil {
		return s, true
	}
	arr, ok := pv.ArrayOK()
	if !ok {
		return s, true
	}
	stages, err := arr.Values()
	if err != nil {
		return s, true
	}

	for i, st := range stages {
		doc, ok := st.DocumentOK()
		if !ok {
			continue
		}
		elems, err := doc.Elements()
		if err != nil || len(elems) == 0 {
			continue
		}
		name := elems[0].Key()
		val := elems[0].Value()

		// A pipeline that starts by describing the collection never reads its
		// documents, whatever it does afterwards.
		if i == 0 && metadataStages[name] {
			return aggScope{}, false
		}

		if i == 0 && name == "$match" {
			if mdoc, ok := val.DocumentOK(); ok {
				s.bound, s.bounded = lowerTimeBound(mdoc)
				s.match = mdoc
			}
		}

		switch name {
		case "$lookup", "$graphLookup":
			if from, ok := stringField(subDoc(val), "from"); ok {
				s.secondary = append(s.secondary, db+"."+from)
			}
		case "$unionWith":
			// Both spellings: {$unionWith: "other"} and {$unionWith: {coll: "other"}}.
			if c, ok := val.StringValueOK(); ok {
				s.secondary = append(s.secondary, db+"."+c)
			} else if from, ok := stringField(subDoc(val), "coll"); ok {
				s.secondary = append(s.secondary, db+"."+from)
			}
		case "$out", "$merge":
			s.writes = true
		}
	}
	return s, true
}

// subDoc returns val as a document, or an empty one when it is not.
func subDoc(val bson.RawValue) bson.Raw {
	doc, ok := val.DocumentOK()
	if !ok {
		return nil
	}
	return doc
}

// checkAggregate reports why an aggregation cannot be trusted, or ok when it
// can.
func (r *Router) checkAggregate(ctx context.Context, s aggScope) (because, remedy string, ok bool) {
	if r.cfg.Manages(s.primary) {
		mark, known := r.completeness(ctx, s.primary)
		switch {
		case !known:
			return "the completeness watermark could not be read",
				"the archive is unreachable, so completeness cannot be established; retry once it is back", false
		case !mark.Claimed:
			return "nothing has recorded that the fast tier is complete for " + s.primary,
				"run the warmer, or if the fast tier holds all of history run: cline watermark --ns " + s.primary + " --complete-all", false
		case s.bounded && !mark.Complete(s.bound):
			return "this aggregation reaches earlier than " + mark.From.Format(time.RFC3339) +
					", which the fast tier does not hold completely for " + s.primary,
				"bound the pipeline with a leading $match inside that range, or aggregate against the archive directly", false
		case !s.bounded && !mark.CoversAll():
			return "this aggregation has no leading $match bounding how far back it reads, so it scans all of " + s.primary,
				"start the pipeline with a $match inside " + mark.From.Format(time.RFC3339) +
					", or aggregate against the archive directly", false
		}
	}

	// A joined collection is read by key, not by time, so nothing less than the
	// whole collection makes the join whole.
	for _, ns := range s.secondary {
		if !r.cfg.Manages(ns) {
			continue
		}
		mark, known := r.completeness(ctx, ns)
		if !known {
			return "the completeness watermark for the joined collection " + ns + " could not be read",
				"the archive is unreachable; retry once it is back", false
		}
		if !mark.CoversAll() {
			return "this pipeline joins " + ns + ", which is tiered and not held in full by the fast tier",
				"a join reads by key rather than by time, so the joined collection has to be whole: warm all of " +
					ns + ", or run the aggregation against the archive", false
		}
	}
	return "", "", true
}

// guardAggregate refuses an aggregation the fast tier cannot answer in full,
// before it runs.
//
// Pre-flight, unlike find, which is inspected on the way back. An aggregation
// can end in $out or $merge, and refusing after the fact would leave a
// truncated collection already written to disk.
func (c *conn) guardAggregate(ctx context.Context, msg *wire.Message, db string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	// A change stream now reaches the fast tier, which is where it belongs, but
	// what it will report is not what the application expects, so say so rather
	// than let it be discovered in production.
	if coll, isCS := changeStreamCollection(cmd, db); isCS {
		if c.r.cfg.Manages(coll) {
			c.r.Stats.ChangeStreams.Add(1)
			c.log.Info("a change stream was opened on a tiered collection",
				"ns", coll,
				"note", "eviction and copy-up move documents in and out of the fast tier, and the "+
					"events for those are hidden from this stream so they cannot be read as "+
					"application deletes and inserts",
				"caveat", "an event whose announcement cannot be read is passed through rather "+
					"than dropped, because missing a real delete would be worse than showing an eviction")
		}
		return nil, false
	}

	scope, ok := scopeOfAggregate(cmd, db)
	if !ok {
		return nil, false
	}

	// A view names itself, not the collection it reads, so completeness has to
	// be judged against the collection underneath it.
	if base, viaView, tiered := c.r.tieredBase(ctx, scope.primary); viaView && tiered {
		view := scope.primary
		scope.primary = base
		because, _, fine := c.r.checkAggregate(ctx, scope)
		if fine {
			return nil, false
		}
		c.r.Stats.Incomplete.Add(1)
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a read through a view over an incomplete collection",
			"view", view, "reads", base, "because", because)
		out, err := c.r.refuseView(msg.Header.RequestID, msg.Header.RequestID, view, base, because)
		if err != nil {
			return nil, false
		}
		return out, true
	}

	because, remedy, fine := c.r.checkAggregate(ctx, scope)
	if fine {
		return nil, false
	}

	c.r.Stats.Incomplete.Add(1)

	// Same rule as a find: an aggregation inside a transaction cannot be sent to
	// the other cluster without leaving the caller's snapshot behind. So it gets
	// the same answer as a find, which is to move the documents to the tier the
	// transaction is already reading and replay it.
	//
	// This is not a niche path. Every driver implements countDocuments as an
	// aggregate, so without it the most common read in the system after find was
	// the one shape a transaction could not perform.
	if inTransaction(cmd) {
		switch out, res := c.warmScopeForTxn(ctx, msg, scope); res {
		case txnWarmReplay:
			c.r.Stats.TxnReplays.Add(1)
			c.cls = ClassTxnWarm
			return out, true
		case txnWarmComplete:
			// Neither tier is missing anything this pipeline reads, so the fast
			// tier can run it.
			return nil, false
		}
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a transactional aggregation that spans both tiers", "ns", scope.primary)
		out, err := c.r.txnCannotSpanTiers(msg.Header.RequestID, msg.Header.RequestID, scope.primary, because)
		if err != nil {
			return nil, false
		}
		return out, true
	}

	// The archive holds every document, so it can answer in full what the fast
	// tier cannot.
	if c.r.cfg.Completeness == config.Archive {
		coll, _ := stringField(cmd, "aggregate")

		// A pipeline that writes needs its two halves run on different
		// clusters: the input read from the archive, the write performed on the
		// fast tier, where the application will look for the result.
		if scope.writes {
			if out, done := c.runWritePipeline(ctx, msg, cmd, db, coll, scope); done {
				return out, true
			}
		} else {
			out, err := c.answerFromArchive(ctx, msg, db, coll)
			if err == nil {
				c.log.Debug("aggregation answered from the archive", "ns", scope.primary, "because", because)
				return out, true
			}
			c.log.Warn("could not answer this aggregation from the archive, refusing instead",
				"ns", scope.primary, "err", err)
		}
	}

	if c.r.cfg.Completeness == config.Permissive && !scope.writes {
		c.log.Warn("aggregation may be short", "ns", scope.primary, "because", because)
		return nil, false
	}

	// A permissive router still refuses to persist a short answer. Returning one
	// is a wrong number on a screen; writing one with $out or $merge makes it
	// the input to everything downstream.
	note := ""
	if scope.writes && c.r.cfg.Completeness == config.Permissive {
		note = " This pipeline writes its result with $out or $merge, which is refused even under permissive completeness."
	}

	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused an aggregation that would return a short answer",
		"ns", scope.primary, "because", because, "writes", scope.writes)

	out, err := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this aggregation because "+because+". "+remedy+"."+note)
	if err != nil {
		return nil, false
	}
	return out, true
}

// changeStreamCollection reports whether an aggregate opens a change stream,
// and on what.
//
// The database-wide and cluster-wide forms use `aggregate: 1` and have no
// collection, and those are not tiered by definition, so only the collection
// form is named here.
func changeStreamCollection(cmd bson.Raw, db string) (string, bool) {
	pv, err := cmd.LookupErr("pipeline")
	if err != nil {
		return "", false
	}
	arr, ok := pv.ArrayOK()
	if !ok {
		return "", false
	}
	vals, err := arr.Values()
	if err != nil || len(vals) == 0 {
		return "", false
	}
	first, ok := vals[0].DocumentOK()
	if !ok {
		return "", false
	}
	elems, err := first.Elements()
	if err != nil || len(elems) == 0 || elems[0].Key() != "$changeStream" {
		return "", false
	}
	coll, ok := stringField(cmd, "aggregate")
	if !ok {
		return "", true
	}
	return db + "." + coll, true
}
