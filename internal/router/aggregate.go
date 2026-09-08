package router

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/wire"
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
	// scopeUnknown says the walk over this pipeline's sub-pipelines stopped
	// before the bottom, so secondary is not the whole list. Anything it
	// contains may be tiered, so the pipeline is refused rather than judged on
	// a list known to be short. See joinedCollections.
	scopeUnknown bool
	// match is the leading $match, kept so a transaction can be given the
	// documents it selects rather than being refused.
	match bson.Raw
	// named records, for a secondary that the pipeline reached through a view,
	// how to say so in a refusal: the name the pipeline used, already formatted
	// as a parenthesis. Empty for a collection named directly.
	named map[string]string
}

// scopeOfAggregate reads an aggregate command into the parts completeness
// depends on.
//
// ok is false when there is no collection to be complete about, which is the
// database-level `aggregate: 1` form behind $currentOp and $listLocalSessions.
//
// ageOf says which field a namespace's age is read from, because a $match's
// lower bound only bears on a completeness claim when the two are measured the
// same way. See lowerTimeBound.
func scopeOfAggregate(cmd bson.Raw, db string, ageOf func(string) (string, bool)) (aggScope, bool) {
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

	// leading tracks the run of $match stages at the front of the pipeline. A
	// stage that cannot be read ends it: not knowing what a stage does is not
	// the same as knowing it is a filter, and a $match after it would otherwise
	// be read as bounding what the collection was read for.
	leading := true
	for i, st := range stages {
		doc, ok := st.DocumentOK()
		if !ok {
			leading = false
			continue
		}
		elems, err := doc.Elements()
		if err != nil || len(elems) == 0 {
			leading = false
			continue
		}
		name := elems[0].Key()
		val := elems[0].Value()

		// A pipeline that starts by describing the collection never reads its
		// documents, whatever it does afterwards.
		if i == 0 && metadataStages[name] {
			return aggScope{}, false
		}

		// Every $match at the front of the pipeline, not only the first.
		// Consecutive ones are a conjunction, so a bound in any of them bounds
		// what the pipeline reads, and a helper that prepends its own filter is
		// the ordinary way a second one appears. Reading only the first sent
		// such a pipeline to the archive and the flat one beside it to the fast
		// tier.
		//
		// Only at the front. A $match after a $group or a $project is about the
		// documents those produced, and a bound there says nothing about how
		// far back the collection was read.
		if leading && name == "$match" {
			if mdoc, ok := val.DocumentOK(); ok {
				af, aok := ageOf(s.primary)
				if t, ok := lowerTimeBound(mdoc, af, aok); ok && (!s.bounded || t.After(s.bound)) {
					s.bound, s.bounded = t, true
				}
				if i == 0 {
					// The filter for copying documents up stays the first one,
					// which is the broadest of them and so the safe choice.
					s.match = mdoc
				}
			}
		} else {
			leading = false
		}

		switch name {
		case "$out", "$merge":
			s.writes = true
		}
	}
	var scopeKnown bool
	s.secondary, scopeKnown = joinedCollections(stages, db, 0)
	s.scopeUnknown = !scopeKnown
	return s, true
}

// joinedCollections lists every collection a pipeline reads besides the one it
// was run against.
//
// It recurses, because the stages that name another collection may be nested.
// $facet holds whole sub-pipelines, and $lookup and $unionWith both take one, so
// a tiered collection can be joined from a depth the top-level scan never
// reaches. That is not a contrived pipeline: a $facet computing several
// aggregates at once, each joining a reference collection, is ordinary
// reporting, and reporting over history is the workload tiering is for.
//
// Only the positions that MongoDB defines as sub-pipelines are followed. A
// blind walk would pick up a user field that happens to be called coll inside a
// $match and refuse a query over a collection nobody is tiering.
func joinedCollections(stages []bson.RawValue, db string, depth int) ([]string, bool) {
	// The limit exists to stop a hand-built document recursing without end, and
	// it used to be 10 with a comment saying that was beyond anything MongoDB
	// would plan. It is not. Measured on 8.0.26: a $lookup nested 14 deep runs,
	// and 20 is where mongod itself says MaxSubPipelineDepthExceeded.
	//
	// So the limit is mongod's, and reaching it fails closed. Returning nil at
	// the boundary meant the collections below it were invisible to the
	// completeness check, so the aggregation ran on the fast tier unjudged.
	// Measured before this: a tiered collection with ten documents in the
	// archive and three on the fast tier, read by a $lookup nested 11 deep,
	// answered 3 with no error, where the same pipeline at depth 9 was refused.
	if depth > maxSubPipelineDepth {
		return nil, false
	}
	var out []string
	complete := true

	sub := func(val bson.RawValue) {
		if arr, ok := val.ArrayOK(); ok {
			if vals, err := arr.Values(); err == nil {
				nested, ok := joinedCollections(vals, db, depth+1)
				out = append(out, nested...)
				if !ok {
					complete = false
				}
			}
		}
	}

	for _, st := range stages {
		doc, ok := st.DocumentOK()
		if !ok {
			continue
		}
		elems, err := doc.Elements()
		if err != nil || len(elems) == 0 {
			continue
		}
		name, val := elems[0].Key(), elems[0].Value()

		switch name {
		case "$lookup", "$graphLookup":
			d := subDoc(val)
			if from, ok := stringField(d, "from"); ok {
				out = append(out, db+"."+from)
			}
			if pv, perr := d.LookupErr("pipeline"); perr == nil {
				sub(pv)
			}
		case "$unionWith":
			// Both spellings: {$unionWith: "other"} and {$unionWith: {coll: "other"}}.
			if c, ok := val.StringValueOK(); ok {
				out = append(out, db+"."+c)
				break
			}
			d := subDoc(val)
			if from, ok := stringField(d, "coll"); ok {
				out = append(out, db+"."+from)
			}
			if pv, perr := d.LookupErr("pipeline"); perr == nil {
				sub(pv)
			}
		case "$facet":
			// Each field is an independent pipeline over the same input.
			fields, ferr := subDoc(val).Elements()
			if ferr != nil {
				continue
			}
			for _, f := range fields {
				sub(f.Value())
			}
		}
	}
	return out, complete
}

// maxSubPipelineDepth is mongod's own limit on nested sub-pipelines, which is
// where this stops looking. See joinedCollections for what reaching it does.
const maxSubPipelineDepth = 20

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
//
// on is every claim the answer rests on, in the state this call read them.
// Read here rather than looked up again by the caller, because a claim narrows
// between two reads and the later value is the one that would fail to notice:
// what a cursor has to be judged against is what authorised it, not what stood
// a moment afterwards. See tieredcursor.go.
func (r *Router) checkAggregate(ctx context.Context, s aggScope) (because, remedy string, on []nsClaim, ok bool) {
	// An aggregation that writes is judged strictly, however it reads. Its
	// input feeds $out or $merge, so a claim that has become false does not
	// shorten an answer a client can retry, it writes a short answer into a
	// collection. router.claim_grace was not offered for that. See completeness.
	claimOf := r.completenessForRead
	if s.writes {
		claimOf = r.completeness
	}
	// A pipeline whose collections could not all be found is refused before
	// anything else. Judging it on the collections that were found is judging
	// it on a list known to be incomplete.
	if s.scopeUnknown {
		return "this pipeline nests sub-pipelines deeper than " + itoaInt(maxSubPipelineDepth) +
				", so the collections it reads could not all be established",
			"flatten it, or run it against the archive directly, which holds every document " +
				"of everything this system tiers", nil, false
	}
	if r.cfg.Manages(s.primary) {
		mark, known := claimOf(ctx, s.primary)
		switch {
		case !known:
			return "the completeness watermark could not be read",
				"the archive is unreachable, so completeness cannot be established; retry once it is back", nil, false
		case !mark.Claimed:
			return "nothing has recorded that the fast tier is complete for " + s.primary,
				"run the warmer, or if the fast tier holds all of history run: cline watermark --ns " + s.primary + " --complete-all", nil, false
		case s.bounded && !mark.Complete(s.bound):
			return "this aggregation reaches earlier than " + mark.From.Format(time.RFC3339) +
					", which the fast tier does not hold completely for " + s.primary,
				"bound the pipeline with a leading $match inside that range, or aggregate against the archive directly", nil, false
		case !s.bounded && !mark.CoversAll():
			return "this aggregation has no leading $match bounding how far back it reads, so it scans all of " + s.primary,
				"start the pipeline with a $match inside " + mark.From.Format(time.RFC3339) +
					", or aggregate against the archive directly", nil, false
		}
		on = append(on, nsClaim{ns: s.primary, from: mark.From, claimed: mark.Claimed})
	}

	// A joined collection is read by key, not by time, so nothing less than the
	// whole collection makes the join whole.
	for _, ns := range s.secondary {
		if !r.cfg.Manages(ns) {
			continue
		}
		mark, known := claimOf(ctx, ns)
		if !known {
			return "the completeness watermark for the joined collection " + ns + " could not be read",
				"the archive is unreachable; retry once it is back", nil, false
		}
		if !mark.CoversAll() {
			return "this pipeline joins " + ns + s.named[ns] + ", which is tiered and not held in full by the fast tier",
				"a join reads by key rather than by time, so the joined collection has to be whole: warm all of " +
					ns + ", or run the aggregation against the archive", nil, false
		}
		on = append(on, nsClaim{ns: ns, from: mark.From, claimed: mark.Claimed})
	}
	return "", "", on, true
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
			// A resumed stream is worth saying out loud, because the way it
			// fails is silence. A resume token is a position in one replica
			// set's oplog, and change streams are served from the fast tier, so
			// a token minted on the cluster being adopted is not a position in
			// it. MongoDB accepts it rather than refusing it: measured, the same
			// token delivered the write that followed it against the cluster it
			// came from, and delivered nothing at all through the router, with
			// ok: 1. Refusing it would mean parsing resume tokens, which are an
			// undocumented encoding that changes between releases, so this names
			// it instead.
			if from, resumed := changeStreamResumedFrom(cmd); resumed {
				c.log.Warn("a change stream was resumed from a token or timestamp",
					"ns", coll, "from", from,
					"caveat", "a resume token is a position in one replica set's oplog, and this "+
						"stream is served from the fast tier, so a token minted on the cluster that "+
						"was adopted names nothing here",
					"effect", "MongoDB accepts such a token and delivers no events, with ok: 1, so a "+
						"consumer resuming across the cutover loses everything the old cluster saw "+
						"after its last token",
					"action", "start consumers fresh after a cutover, and check this stream is "+
						"delivering rather than merely open")
			}
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

	// Before the scope, because the scope is about what the pipeline reads and
	// this is about where it writes. A pipeline beginning with $documents reads
	// nothing at all, so it has no scope, and it can still $out into a tiered
	// collection: see guardTieredOut.
	if out, done := c.guardTieredOut(ctx, msg, cmd, db); done {
		return out, true
	}

	scope, ok := scopeOfAggregate(cmd, db, c.r.ageFieldOf)
	if !ok {
		return nil, false
	}
	// A joined view is as invisible to the completeness check as a queried one,
	// and it is reached by the same kind of name.
	c.r.resolveJoinedViews(ctx, &scope)

	// A view names itself, not the collection it reads, so completeness has to
	// be judged against the collection underneath it.
	if base, viaView, tiered := c.r.tieredBase(ctx, scope.primary); viaView && tiered {
		view := scope.primary
		scope.primary = base
		because, _, authorised, fine := c.r.checkAggregate(ctx, scope)
		if fine {
			// Same as the collection case below: the cursor this leaves open
			// outlives the claims that authorised it. Set here too, because a
			// view returns from this branch and never reaches that one.
			c.aggAuth = authorised
			return nil, false
		}
		// The archive can be asked the same question when it holds the view
		// defined the same way. Falling through leaves the completeness branch
		// below to send it there, which is what it does for a collection.
		if c.r.cfg.Completeness == config.Archive && c.r.archiveCanAnswerView(ctx, view) {
			c.log.Debug("a view the fast tier cannot answer, sent to the archive",
				"view", view, "reads", base, "because", because)
		} else {
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
	}

	because, remedy, authorised, fine := c.r.checkAggregate(ctx, scope)
	if fine {
		// The fast tier is about to run this, and the answer may not fit in one
		// batch: an aggregation returns 101 documents and a cursor for the rest.
		// That cursor outlives the claims above exactly as a find's does, so
		// they are carried to the reply and registered there. See
		// tieredcursor.go for what the find twin of this was measured to cost.
		c.aggAuth = authorised
		return nil, false
	}

	// A read-only pipeline asking for the newest N is sent to the fast tier
	// instead, and judged on the way back the way a find is. The reason this
	// command is judged early does not apply to one that only reads, and the
	// archive is still where it goes if the proof does not hold.
	//
	// Before the count below, not after: this pipeline may well be answered in
	// full, and counting it incomplete here would report the opposite of what
	// happens. See aggnewest.go, which counts it if the proof fails.
	if c.deferNewestAgg(ctx, cmd, scope.primary, scope.writes) {
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

		// Every document of what this system tiers. A collection it does not
		// tier is a different matter, and a pipeline reading one is about to be
		// answered by the wrong tier for that half of it: the feeder mirrors the
		// namespaces it is told to and nothing else, so the archive's copy of an
		// untiered collection is whatever it held on the day it was adopted.
		//
		// Measured on a $unionWith of an untiered collection with a view over a
		// tiered one: the answer was 10 where the two clusters together hold 11,
		// the missing document being the one written to the untiered collection
		// after the cutover.
		//
		// Refused, which is the standing rule here: a refusal is a limitation
		// and a wrong answer is a bug. It was a warning for one afternoon, on
		// the reasoning that a report joining a reference collection that has
		// not changed since the adoption is right more often than it is wrong.
		// That reasoning trades somebody else's correctness for our convenience,
		// and the answer it lets through is short with no error on it, which is
		// the failure this project treats as worst.
		if out, done := c.refuseUntieredFromArchive(msg, scope); done {
			return out, true
		}

		// Judged before it runs, so the fast tier has not seen it and nothing
		// has consulted this client's permissions yet.
		if denied, ok := c.mayRead(msg, db, coll); !ok {
			return denied, true
		}

		// A pipeline that writes needs its two halves run on different
		// clusters: the input read from the archive, the write performed on the
		// fast tier, where the application will look for the result.
		if scope.writes {
			if out, done := c.runWritePipeline(ctx, msg, cmd, db, coll, scope, true); done {
				return out, true
			}
		} else {
			out, err := c.answerFromArchive(ctx, msg, db, coll, true)
			if err == nil {
				c.log.Debug("aggregation answered from the archive", "ns", scope.primary, "because", because)
				return out, true
			}
			if out, passed := c.r.serverErrorReply(msg.Header.RequestID, msg.Header.RequestID, err); passed {
				c.log.Debug("the archive rejected this aggregation; returning its own error",
					"ns", scope.primary, "err", err)
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

// changeStreamResumedFrom reports which of the resume options a change stream
// carries, and whether it carries any. See the warning at the caller.
func changeStreamResumedFrom(cmd bson.Raw) (string, bool) {
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
	spec, ok := first.Lookup("$changeStream").DocumentOK()
	if !ok {
		return "", false
	}
	for _, opt := range []string{"resumeAfter", "startAfter", "startAtOperationTime"} {
		if _, err := spec.LookupErr(opt); err == nil {
			return opt, true
		}
	}
	return "", false
}

// refuseUntieredFromArchive refuses a pipeline the archive would answer that also
// reads a collection this system does not tier.
//
// The archive holds every document of a tiered collection, which is what makes
// the fallback correct. It holds whatever it happened to hold of an untiered one:
// the feeder mirrors the namespaces it is told to tier and nothing else, so
// anything written to an untiered collection since the cutover is on the fast
// tier alone. A pipeline that reads both is answered by one tier, and for that
// half the answer is short.
//
// Measured on a $unionWith of an untiered collection with a view over a tiered
// one: 10 where the two clusters together hold 11, the missing document being the
// one written to the untiered collection after the cutover. Neither the pilot nor
// the comparison harness can see that. The pilot's untiered collections were
// restored into the archive and never written to again, and the harness asks the
// archive the same question and gets the same short answer.
func (c *conn) refuseUntieredFromArchive(msg *wire.Message, s aggScope) (*wire.Message, bool) {
	var untiered []string
	for _, ns := range append([]string{s.primary}, s.secondary...) {
		if ns == "" || c.r.cfg.Manages(ns) {
			continue
		}
		untiered = append(untiered, ns)
	}
	if len(untiered) == 0 {
		return nil, false
	}
	sort.Strings(untiered)

	// The operator has said these are reference data that has not moved since
	// the cutover, which is a claim about their application that this cannot
	// check. Say what is being answered and what it may be short of, every
	// time: the alternative to a refusal is a warning nobody sees once.
	if c.r.cfg.AllowUntieredJoin {
		c.log.Warn("answering from the archive a pipeline that reads collections nothing tiers",
			"ns", s.primary, "untiered", untiered,
			"reason", "router.allow_untiered_join is set, so this is answered rather than refused",
			"effect", "the answer is short by anything written to those collections since the "+
				"archive was adopted, and there is no error on it",
			"action", "unset router.allow_untiered_join to have it refused instead")
		return nil, false
	}

	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a pipeline the archive would answer that reads collections nothing tiers",
		"ns", s.primary, "untiered", untiered,
		"reason", "the archive holds whatever it held of those on the day it was adopted, because "+
			"the feeder mirrors only what it is told to tier",
		"effect", "answering would have been short by everything written to them since the cutover, "+
			"with no error on it",
		"action", "tier the collections this pipeline reads, or warm the tiered ones so the fast "+
			"tier can answer it whole")
	out, err := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this pipeline because the fast tier cannot answer it whole and it "+
			"reads "+strings.Join(untiered, ", ")+", which nothing here tiers. The archive would "+
			"answer the rest of it, and it holds only what those collections contained when it was "+
			"adopted: everything written to them since is on the fast tier alone, so the answer "+
			"would be short with no error on it. Tier them, or warm the tiered collections this "+
			"pipeline reads so the fast tier can answer it whole.")
	if err != nil {
		return nil, false
	}
	return out, true
}

func itoaInt(n int) string { return strconv.Itoa(n) }
