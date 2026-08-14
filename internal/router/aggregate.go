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

// aggScope is what one aggregate command reads and writes.
type aggScope struct {
	primary   string
	secondary []string
	bound     time.Time
	bounded   bool
	writes    bool
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

		if i == 0 && name == "$match" {
			if mdoc, ok := val.DocumentOK(); ok {
				s.bound, s.bounded = lowerTimeBound(mdoc)
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
	scope, ok := scopeOfAggregate(cmd, db)
	if !ok {
		return nil, false
	}

	because, remedy, fine := c.r.checkAggregate(ctx, scope)
	if fine {
		return nil, false
	}

	c.r.Stats.Incomplete.Add(1)
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
	c.log.Warn("refused an aggregation that would return a short answer",
		"ns", scope.primary, "because", because, "writes", scope.writes)

	out, err := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this aggregation because "+because+". "+remedy+"."+note)
	if err != nil {
		return nil, false
	}
	return out, true
}
