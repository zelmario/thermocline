package router

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmar/thermocline/internal/meta"
	"github.com/zelmar/thermocline/internal/wire"
)

// A pipeline ending in $out or $merge was refused whenever its input reached
// past what the fast tier holds. The reason was sound: the input can only be
// read in full from the archive, and running the whole pipeline there would
// write the result to the archive, which is not where the application will look
// for it, and which only the feeder is allowed to write to.
//
// Refusing is still wrong, though. A nightly rollup that stops working the day
// tiering is switched on is exactly the kind of surprise this project exists to
// avoid.
//
// So the pipeline is split at its last stage. Everything before it is read from
// the archive, which holds every document and can answer it in full. The result
// is then handed back to the fast tier as the input to the original write stage,
// using $documents, so the write is performed by mongod with the caller's own
// $out or $merge options, on the cluster the application reads from, and it
// mirrors down to the archive like any other write.

// maxMaterialize caps how many documents a write pipeline's result may carry
// through the router, and how large they may be in total.
//
// The result has to be handed to the fast tier in one command, so it is bounded
// by what a command can hold. Aggregations that write are usually rollups, and
// a rollup that reduces to less than this is the normal case; one that does not
// is refused with a reason rather than truncated.
const (
	maxMaterialize      = 50000
	maxMaterializeBytes = 12 << 20
)

// runWritePipeline reads a $out or $merge pipeline's input from the archive and
// performs its final stage on the fast tier.
func (c *conn) runWritePipeline(ctx context.Context, msg *wire.Message, cmd bson.Raw, db, coll string, s aggScope) (*wire.Message, bool) {
	stages, ok := pipelineStages(cmd)
	if !ok || len(stages) == 0 {
		return nil, false
	}
	// Stages go back to the driver as documents. A bson.RawValue cannot be
	// marshalled as one on its own.
	final, fok := stages[len(stages)-1].DocumentOK()
	if !fok {
		return nil, false
	}
	read := make([]bson.Raw, 0, len(stages)-1)
	for _, st := range stages[:len(stages)-1] {
		doc, dok := st.DocumentOK()
		if !dok {
			return nil, false
		}
		read = append(read, doc)
	}

	docs, err := c.materializeFromArchive(ctx, db, coll, read)
	if err != nil {
		c.r.Stats.Refused.Add(1)
		c.log.Warn("could not run a writing aggregation against the archive",
			"ns", s.primary, "err", err)
		out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: refusing this aggregation because its input reaches past what the fast tier holds, "+
				"and reading it from the archive failed: "+err.Error())
		if rerr != nil {
			return nil, false
		}
		return out, true
	}

	// $out replaces its target by building a temporary collection and renaming
	// it over the top. The feeder ignores renames on purpose, because replaying
	// destructive DDL into the archive is what would let one accident on the
	// fast tier destroy it, so a result written that way would never mirror.
	//
	// When the target is tiered that matters, and the replacement is done as
	// ordinary writes instead: the inserts mirror, and the rows that are gone
	// are tombstoned like any other delete. The cost is that the switch is no
	// longer atomic. $merge needs none of this, because it writes documents
	// rather than swapping collections.
	if outDB, outColl, isOut := outTarget(final, db); isOut && c.r.cfg.Manages(outDB+"."+outColl) {
		if out, done := c.replaceThroughFastTier(ctx, msg, outDB, outColl, docs, s); done {
			return out, true
		}
		return nil, false
	}

	// $documents needs a database-level aggregation, and the write stage names
	// its own target, so the fast tier is asked to do exactly the caller's last
	// stage over exactly the rows the archive produced.
	pipeline := bson.A{bson.D{{Key: "$documents", Value: docs}}, final}
	res := c.r.hot.Database(db).RunCommand(ctx, bson.D{
		{Key: "aggregate", Value: 1},
		{Key: "pipeline", Value: pipeline},
		{Key: "cursor", Value: bson.D{}},
	})
	if res.Err() != nil {
		c.log.Warn("could not write the result of an aggregation to the fast tier",
			"ns", s.primary, "err", res.Err())
		out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: this aggregation's input was read from the archive, but writing its result to the "+
				"fast tier failed: "+res.Err().Error())
		if rerr != nil {
			return nil, false
		}
		return out, true
	}

	c.r.Stats.Archived.Add(1)
	c.log.Info("aggregation read from the archive and written to the fast tier",
		"ns", s.primary, "documents", len(docs))

	// mongod answers a writing aggregation with an empty, already-exhausted
	// cursor, and so does this.
	out, err := cursorReply(msg.Header.RequestID, msg.Header.RequestID, 0, db+"."+coll, "firstBatch", bson.A{})
	if err != nil {
		return nil, false
	}
	return out, true
}

// materializeFromArchive runs the reading part of a pipeline against the archive
// and collects its output.
func (c *conn) materializeFromArchive(ctx context.Context, db, coll string, read []bson.Raw) (bson.A, error) {
	// Tombstoned documents are records the application deleted. Excluding them
	// first means the pipeline sees the same collection the fast tier would
	// have shown it.
	pipeline := bson.A{bson.D{{Key: "$match", Value: bson.D{meta.NotArchivedDeleted()}}}}
	for _, st := range read {
		pipeline = append(pipeline, st)
	}

	c.archiveReadIsFresh(ctx, db+"."+coll, time.Now())

	cur, err := c.r.cold.Database(db).Collection(coll).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	out := bson.A{}
	size := 0
	for cur.Next(ctx) {
		if len(out) >= maxMaterialize {
			return nil, fmt.Errorf("this pipeline produces more than %d documents, which is more than can be "+
				"handed back to the fast tier in one command; narrow it, or warm the range it reads so it can "+
				"run on the fast tier directly", maxMaterialize)
		}
		size += len(cur.Current)
		if size > maxMaterializeBytes {
			return nil, fmt.Errorf("this pipeline produces more than %d MB of output, which is more than can be "+
				"handed back to the fast tier in one command; narrow it, or warm the range it reads so it can "+
				"run on the fast tier directly", maxMaterializeBytes>>20)
		}
		doc := make(bson.Raw, len(cur.Current))
		copy(doc, cur.Current)
		out = append(out, doc)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// pipelineStages returns the pipeline's stages, in order.
func pipelineStages(cmd bson.Raw) ([]bson.RawValue, bool) {
	pv, err := cmd.LookupErr("pipeline")
	if err != nil {
		return nil, false
	}
	arr, ok := pv.ArrayOK()
	if !ok {
		return nil, false
	}
	stages, err := arr.Values()
	if err != nil {
		return nil, false
	}
	return stages, true
}

// outTarget reads a $out stage's destination. It accepts both spellings: a bare
// collection name in the same database, and a document naming both.
func outTarget(stage bson.Raw, db string) (string, string, bool) {
	v, err := stage.LookupErr("$out")
	if err != nil {
		return "", "", false
	}
	if coll, ok := v.StringValueOK(); ok {
		return db, coll, true
	}
	doc, ok := v.DocumentOK()
	if !ok {
		return "", "", false
	}
	outDB := db
	if d, derr := doc.LookupErr("db"); derr == nil {
		if sv, sok := d.StringValueOK(); sok {
			outDB = sv
		}
	}
	if cv, cerr := doc.LookupErr("coll"); cerr == nil {
		if sv, sok := cv.StringValueOK(); sok {
			return outDB, sv, true
		}
	}
	return "", "", false
}

// replaceThroughFastTier performs $out's replacement as ordinary writes, so the
// feeder sees them and the archive follows.
func (c *conn) replaceThroughFastTier(ctx context.Context, msg *wire.Message, db, coll string, docs bson.A, s aggScope) (*wire.Message, bool) {
	target := c.r.hot.Database(db).Collection(coll)

	if _, err := target.DeleteMany(ctx, bson.D{}); err != nil {
		c.log.Warn("could not clear the target of an $out", "ns", db+"."+coll, "err", err)
		out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: this aggregation's input was read from the archive, but clearing "+db+"."+coll+
				" before writing the result failed: "+err.Error())
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	if len(docs) > 0 {
		if _, err := target.InsertMany(ctx, docs); err != nil {
			c.log.Warn("could not write the result of an $out", "ns", db+"."+coll, "err", err)
			out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
				"thermocline: this aggregation's input was read from the archive, but writing its result to "+
					db+"."+coll+" failed: "+err.Error())
			if rerr != nil {
				return nil, false
			}
			return out, true
		}
	}

	c.r.Stats.Archived.Add(1)
	c.log.Info("aggregation read from the archive and written to the fast tier",
		"ns", s.primary, "into", db+"."+coll, "documents", len(docs),
		"note", "written as ordinary writes so the result mirrors to the archive")

	out, err := cursorReply(msg.Header.RequestID, msg.Header.RequestID, 0, db+"."+coll, "firstBatch", bson.A{})
	if err != nil {
		return nil, false
	}
	return out, true
}
