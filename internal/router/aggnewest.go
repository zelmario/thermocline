package router

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/wire"
)

// The newest ten is the commonest question a dashboard asks, and a BI tool asks
// it as an aggregation rather than as a find. The find is proved from the fast
// tier's own answer and costs nothing; the same question as a pipeline was
// judged before it ran, could not be proved, and went to the archive every
// time. Measured on a shop of 3,000 orders with the fast tier holding 314, the
// cache off so thirty identical questions are thirty questions:
//
//	30 as a find:        51 ms, none reaching the archive
//	30 as an aggregate: 828 ms, every one reaching the archive
//
// An aggregation is judged before it runs for a reason that is still good: it
// can end in $out or $merge, and refusing after the fact would leave a
// truncated collection on disk. That reason does not apply to a pipeline that
// only reads. Such a pipeline can be sent to the fast tier and judged on the
// way back exactly as a find is, and if the proof does not hold the archive
// answers, which is where it was going anyway.
//
// The shape is deliberately narrow: an optional leading $match, then a $sort
// leading with the field the collection ages by, descending, then a $limit.
// A $project or $addFields before the $sort could remove or rewrite the field
// the proof reads, and one after the $limit hides it from the reply, so neither
// is admitted. Widening this is a measurement, not a guess.

// newestFirstPipeline reads the limit off a pipeline of that shape.
func newestFirstPipeline(cmd bson.Raw, ageField string) (int, bool) {
	arr, ok := cmd.Lookup("pipeline").ArrayOK()
	if !ok {
		return 0, false
	}
	stages, err := arr.Values()
	if err != nil || len(stages) < 2 || len(stages) > 3 {
		return 0, false
	}
	if len(stages) == 3 {
		if name, _ := soleStage(stages[0]); name != "$match" {
			return 0, false
		}
		stages = stages[1:]
	}

	name, sortDoc := soleStage(stages[0])
	if name != "$sort" || !sortsNewestFirst(sortDoc, ageField) {
		return 0, false
	}
	name, _ = soleStage(stages[1])
	if name != "$limit" {
		return 0, false
	}
	lim, ok := stages[1].DocumentOK()
	if !ok {
		return 0, false
	}
	return positiveInt(lim.Lookup("$limit"))
}

// soleStage returns a stage's operator and its document argument, for a stage
// that has exactly one operator. A stage with more than one is not one of these.
func soleStage(v bson.RawValue) (string, bson.Raw) {
	doc, ok := v.DocumentOK()
	if !ok {
		return "", nil
	}
	elems, err := doc.Elements()
	if err != nil || len(elems) != 1 {
		return "", nil
	}
	arg, _ := elems[0].Value().DocumentOK()
	return elems[0].Key(), arg
}

// deferNewestAgg reports whether this aggregation should be sent to the fast
// tier and judged on the way back, rather than answered from the archive now.
//
// Only when the archive is where it would otherwise go. Under any other
// completeness setting the pre-flight decision is the whole behaviour, and
// changing when it is made would change what an operator asked for.
func (c *conn) deferNewestAgg(ctx context.Context, cmd bson.Raw, ns string, writes bool) bool {
	if writes || c.r.cfg.Completeness != config.Archive || inTransaction(cmd) {
		return false
	}
	mark, known := c.r.completeness(ctx, ns)
	if !known || !mark.Claimed || mark.From.IsZero() {
		return false
	}
	af, aok := c.r.ageFieldOf(ns)
	if !aok {
		return false
	}
	_, ok := newestFirstPipeline(cmd, af)
	return ok
}

// interceptAggNewest judges a deferred pipeline by its own answer, and sends it
// to the archive when the proof does not hold.
//
// It reports whether it handled the reply. Permissions are not consulted here,
// unlike the pre-flight path: the fast tier has already run this command under
// this client's own account. That is the same reason the find fallback does not
// consult them either.
func (c *conn) interceptAggNewest(ctx context.Context, msg *wire.Message, db string, reply *wire.Message) (*wire.Message, bool) {
	cmd, cerr := msg.CommandDoc()
	if cerr != nil {
		return nil, false
	}
	coll, ok := stringField(cmd, "aggregate")
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if !c.deferNewestAgg(ctx, cmd, ns, false) {
		return nil, false
	}

	rdoc, derr := reply.ReplyDoc()
	if derr != nil || !replyOK(rdoc) {
		return nil, false
	}
	af, _ := c.r.ageFieldOf(ns)
	limit, _ := newestFirstPipeline(cmd, af)
	mark, _ := c.r.completeness(ctx, ns)

	if provenBatch(rdoc, limit, af, mark) && c.r.sortSafeAges(ctx, ns, af) {
		c.r.Stats.NewestFirst.Add(1)
		return reply, true
	}

	c.r.Stats.Incomplete.Add(1)
	// The fast tier ran this and may be holding the rest of it. Nothing will
	// ever ask for that rest. See superseded.go.
	c.dropSupersededCursor(ctx, rdoc, db)
	out, aerr := c.answerFromArchive(ctx, msg, db, coll)
	if aerr == nil {
		c.log.Debug("aggregation answered from the archive", "ns", ns,
			"because", "the fast tier's own answer did not prove it holds the newest of them")
		return out, true
	}
	if out, passed := c.r.serverErrorReply(reply.Header.RequestID, msg.Header.RequestID, aerr); passed {
		c.log.Debug("the archive rejected this aggregation; returning its own error", "ns", ns, "err", aerr)
		return out, true
	}
	c.log.Warn("could not answer this aggregation from the archive", "ns", ns, "err", aerr)
	return nil, false
}
