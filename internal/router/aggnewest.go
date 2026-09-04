package router

import (
	"context"
	"strings"

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
// The shape is an optional leading $match, then a $sort leading with the field
// the collection ages by descending, then a $limit, then any number of stages
// that only reshape the documents already chosen.
//
// Nothing may come between the front of the pipeline and the $limit, because a
// $project or $addFields there could remove or rewrite the field the sort and
// the proof both read. After the $limit the N are already chosen, and reshaping
// them cannot change which N they are.
//
// What may not appear anywhere is a stage naming another collection. A $lookup
// or a $unionWith reads that collection by key rather than by time, so no
// watermark covers it, and this pipeline may be here precisely because the
// joined collection is the incomplete one.
//
// A trailing stage that drops the age field is not a danger, it is a miss: the
// proof reads the age off the last document returned, does not find it, and the
// query goes to the archive as it did before.
//
// A trailing stage that *rewrites* it is a danger, and this is the part that
// took two goes. The proof reads a date out of the reply and compares it to the
// claim, so a stage putting some other date at that path is telling the proof
// what to believe:
//
//	[{$sort:{date:-1}},{$limit:10},{$addFields:{date:"$updatedAt"}}]
//
// would be proved against updatedAt. A trailing $sort is the same defect in
// another form: the proof needs the *oldest* of the N, which is the last one
// the sort put there, and re-sorting moves a different document into that
// place. So a trailing stage may drop the age field or leave it alone, and may
// not compute it, and may not reorder.

// reshapeKeepsAge reports whether a stage after the $limit leaves the field the
// proof reads either untouched or absent.
func reshapeKeepsAge(name string, arg bson.Raw, ageField string) bool {
	root := ageField
	if root == "" {
		root = "_id"
	}
	switch name {
	case "$unset":
		// It can only remove, and removing makes the proof fail rather than lie.
		return true
	case "$project", "$addFields", "$set":
		elems, err := arg.Elements()
		if err != nil {
			return false
		}
		for _, e := range elems {
			if !pathsTouch(e.Key(), root) {
				continue
			}
			// $project says include or exclude with a flag, and that is the
			// only mention of this field that changes nothing about its value.
			// $addFields and $set have no such form: any mention is a rewrite.
			if name != "$project" || !plainProjectionFlag(e.Value()) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// pathsTouch reports whether two dotted field paths can name the same value,
// which includes one being inside the other.
func pathsTouch(a, b string) bool {
	return a == b ||
		strings.HasPrefix(a, b+".") ||
		strings.HasPrefix(b, a+".")
}

// plainProjectionFlag reports whether a projection value is an include or
// exclude rather than an expression computing a new one.
func plainProjectionFlag(v bson.RawValue) bool {
	if _, ok := v.BooleanOK(); ok {
		return true
	}
	n, ok := intOf(v)
	return ok && (n == 0 || n == 1)
}

// newestFirstPipeline reads the limit off a pipeline of that shape.
func newestFirstPipeline(cmd bson.Raw, ageField string) (int, bool) {
	arr, ok := cmd.Lookup("pipeline").ArrayOK()
	if !ok {
		return 0, false
	}
	stages, err := arr.Values()
	if err != nil || len(stages) < 2 {
		return 0, false
	}
	// Any number of leading $match stages, because a filter does not enter the
	// proof at all and two of them are one filter. A driver or a helper that
	// prepends its own is the ordinary way a second one appears.
	for len(stages) > 0 {
		if name, _ := soleStage(stages[0]); name != "$match" {
			break
		}
		stages = stages[1:]
	}
	if len(stages) < 2 {
		return 0, false
	}

	name, sortDoc := soleStage(stages[0])
	if name != "$sort" || !sortsNewestFirst(sortDoc, ageField) {
		return 0, false
	}
	if name, _ = soleStage(stages[1]); name != "$limit" {
		return 0, false
	}
	for _, st := range stages[2:] {
		n, arg := soleStage(st)
		if !reshapeKeepsAge(n, arg, ageField) {
			return 0, false
		}
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
	// Anything that writes has returned above, so this is a read and
	// router.claim_grace applies.
	mark, known := c.r.completenessForRead(ctx, ns)
	if !known || !mark.Claimed || mark.From.IsZero() {
		return false
	}
	af, aok := c.r.ageFieldOf(ns)
	if !aok {
		return false
	}
	limit, ok := newestFirstPipeline(cmd, af)
	if !ok {
		return false
	}
	return limit <= firstBatchCap(cmd)
}

// firstBatchCap is the most documents the fast tier's first batch can carry,
// which is what decides whether deferring is worth anything.
//
// The proof needs the whole of the N in that batch, so a limit larger than it
// cannot be proved however the query goes. Sending it to the fast tier anyway
// would have it sort everything it holds, return a batch too short to prove
// anything, and then be asked again at the archive. That is a sort nobody reads,
// paid on a pipeline that reaches far enough back to be worth caring about.
//
// The default is mongod's own first batch for an aggregation, and a client that
// asks for more is taken at its word. Fewer can still come back, because the
// batch is also capped by 16 MB, and that case is the same miss as any other:
// the proof fails and the archive answers.
func firstBatchCap(cmd bson.Raw) int {
	if cur, ok := cmd.Lookup("cursor").DocumentOK(); ok {
		if n, ok := positiveInt(cur.Lookup("batchSize")); ok {
			return n
		}
	}
	return defaultFirstBatch
}

// defaultFirstBatch is what mongod puts in an aggregation's first batch when
// nothing asks for a size.
const defaultFirstBatch = 101

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
	mark, _ := c.r.completenessForRead(ctx, ns)

	if provenBatch(rdoc, limit, af, mark) && c.r.sortSafeAges(ctx, ns, af) {
		c.r.Stats.NewestFirst.Add(1)
		return reply, true
	}

	c.r.Stats.Incomplete.Add(1)
	// The fast tier ran this and may be holding the rest of it. Nothing will
	// ever ask for that rest. See superseded.go.
	c.dropSupersededCursor(ctx, rdoc, db)
	out, aerr := c.answerFromArchive(ctx, msg, db, coll, true)
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
