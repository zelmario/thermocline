package router

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// explain was not a command this router knew, so it went straight to the fast
// tier and came back describing the fast tier.
//
// That is misleading in a specific way. The plan is for a query that may not run
// there at all: a read reaching past the completeness watermark is answered from
// the archive, which has different indexes and therefore a different plan. Worse,
// executionStats reports nReturned from the fast tier's partial copy, so an
// explain of a query that returns ten documents can say three. A number that
// wrong is worth correcting even in a diagnostic.
//
// Refusing explain is not an option. Compass runs it, every index review runs it,
// and refusing would break the workflow people use to check that this router has
// not broken their queries. So the reply is passed through intact and a
// thermocline field is added beside it, saying whether the fast tier can answer
// the query this plan describes, and if not, why and what would actually happen.
// Adding a field leaves every existing path in the document where tools expect
// it.

// annotateExplain adds this router's verdict to an explain reply.
//
// It returns done=false when there is nothing to say, which is the common case:
// a query the fast tier can answer in full gets its plan back untouched.
func (c *conn) annotateExplain(ctx context.Context, msg *wire.Message, db string, reply *wire.Message) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	inner, ok := cmd.Lookup("explain").DocumentOK()
	if !ok {
		// The inline form explains the command it is part of, so that command
		// is its own inner one.
		if !inlineExplain(cmd) {
			return nil, false
		}
		inner = cmd
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return nil, false
	}

	because, ok := c.explainVerdict(ctx, inner, db)
	if !ok {
		return nil, false
	}

	elems, eerr := rdoc.Elements()
	if eerr != nil {
		return nil, false
	}
	out := make(bson.D, 0, len(elems)+1)
	for _, e := range elems {
		var v any
		if e.Value().Unmarshal(&v) != nil {
			return nil, false
		}
		out = append(out, bson.E{Key: e.Key(), Value: v})
	}
	out = append(out, bson.E{Key: "thermocline", Value: bson.D{
		{Key: "fastTierComplete", Value: false},
		{Key: "because", Value: because},
		{Key: "note", Value: "this plan and any executionStats describe the fast tier only. " +
			"The query itself would not be answered there: it is either served from the archive, " +
			"which has its own indexes and so its own plan, or refused. Counts here are from a " +
			"partial copy and will read low."},
	}})

	built, berr := wire.NewMsg(reply.Header.RequestID, msg.Header.RequestID, out)
	if berr != nil {
		return nil, false
	}
	return built, true
}

// explainVerdict reports why the fast tier cannot answer the explained command,
// or ok=false when it can and there is nothing to add.
func (c *conn) explainVerdict(ctx context.Context, inner bson.Raw, db string) (because string, ok bool) {
	elems, err := inner.Elements()
	if err != nil || len(elems) == 0 {
		return "", false
	}
	// A command's name is its first field, which is how the explained command
	// names itself too.
	name := elems[0].Key()

	if name == "aggregate" {
		scope, sok := scopeOfAggregate(inner, db, c.r.ageFieldOf)
		if !sok {
			return "", false
		}
		c.r.resolveJoinedViews(ctx, &scope)
		if base, viaView, tiered := c.r.tieredBase(ctx, scope.primary); viaView && tiered {
			scope.primary = base
		}
		why, _, _, fine := c.r.checkAggregate(ctx, scope)
		if fine {
			return "", false
		}
		return why, true
	}

	var coll string
	switch name {
	case "find":
		coll, ok = stringField(inner, "find")
	case "count", "distinct":
		coll, ok = simpleReadCollection(inner, name)
	default:
		return "", false
	}
	if !ok {
		return "", false
	}

	ns := db + "." + coll
	base, viaView, tiered := c.r.tieredBase(ctx, ns)
	if !tiered {
		return "", false
	}
	judged := ns
	if viaView {
		judged = base
	}

	// An explain never performs the command, so this is a read however the
	// explained command would be classified. See explain.go's own rule.
	mark, known := c.r.completenessForRead(ctx, judged)
	if !known {
		return "the completeness watermark could not be read, so the archive is unreachable", true
	}
	if !mark.Claimed {
		return "nothing has recorded that the fast tier is complete for " + judged, true
	}

	af, aok := c.r.ageFieldOf(judged)
	bound, bounded := explainBound(inner, name, af, aok)
	switch {
	case bounded && mark.Complete(bound):
		return "", false
	case !bounded && mark.CoversAll():
		return "", false
	case bounded:
		return "this query reaches earlier than " + mark.From.Format(time.RFC3339) +
			", which the fast tier does not hold completely for " + judged, true
	default:
		return "this query has nothing bounding how far back it reads, so it scans all of " +
			judged + ", and the fast tier is complete only from " + mark.From.Format(time.RFC3339), true
	}
}

// explainBound reads the lower time bound out of whichever field this command
// keeps its selector in.
func explainBound(inner bson.Raw, name, ageField string, known bool) (bound time.Time, bounded bool) {
	field := "filter"
	if name != "find" {
		field = "query"
	}
	doc, ok := inner.Lookup(field).DocumentOK()
	if !ok {
		return time.Time{}, false
	}
	return lowerTimeBound(doc, ageField, known)
}

// inlineExplain reports whether a command plans instead of running.
//
// aggregate takes explain: true in the command itself rather than being wrapped
// in an explain command, and both forms are ordinary: the shell sends the
// wrapped one and several drivers send this. A command that only plans must not
// have anything done on its behalf.
func inlineExplain(cmd bson.Raw) bool {
	v, err := cmd.LookupErr("explain")
	if err != nil {
		return false
	}
	if b, ok := v.BooleanOK(); ok {
		return b
	}
	// Some drivers send a document carrying the verbosity. Anything but a false
	// boolean is an explain.
	_, isDoc := v.DocumentOK()
	return isDoc
}

// inlineExplainOf is inlineExplain for a message whose command has not been
// read yet.
func inlineExplainOf(msg *wire.Message) bool {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return false
	}
	return inlineExplain(cmd)
}
