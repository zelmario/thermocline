package router

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

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
// the archive, which holds every document and can answer it in full, and the
// result is streamed into the fast tier, where the application reads. How it
// lands depends on what the last stage is and where it points:
//
//	$merge                    batches of the result are fed back through the
//	                          caller's own $merge stage, so mongod applies its
//	                          whenMatched and whenNotMatched rules unchanged.
//	$out, untiered target     written to a temporary collection and renamed over
//	                          the target, which is how mongod implements $out
//	                          itself, so the switch stays atomic.
//	$out, tiered target       written as ordinary deletes and inserts. The
//	                          rename above is invisible to the feeder, which
//	                          ignores renames on purpose, so a result written
//	                          that way would never reach the archive. The cost
//	                          is that this one switch is not atomic.
//
// Nothing is held in memory beyond one batch, so the size of the result is
// bounded by what the tiers can store rather than by this process.

// writeBatch is how many result documents are sent to the fast tier at a time.
const writeBatch = 1000

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

	sink, err := c.newWriteSink(ctx, final, db)
	if err != nil {
		return c.writePipelineError(msg, s, "preparing the target failed", err)
	}

	n, err := c.streamFromArchive(ctx, db, coll, read, sink.write)
	if err != nil {
		sink.abandon(ctx)
		return c.writePipelineError(msg, s, "reading its input from the archive failed", err)
	}
	if err := sink.commit(ctx); err != nil {
		sink.abandon(ctx)
		return c.writePipelineError(msg, s, "writing its result to the fast tier failed", err)
	}

	c.r.Stats.Archived.Add(1)
	c.cls = ClassArchive
	c.log.Info("aggregation read from the archive and written to the fast tier",
		"ns", s.primary, "into", sink.target, "documents", n, "how", sink.how)

	// mongod answers a writing aggregation with an empty, already-exhausted
	// cursor, and so does this.
	out, err := cursorReply(msg.Header.RequestID, msg.Header.RequestID, 0, db+"."+coll, "firstBatch", bson.A{})
	if err != nil {
		return nil, false
	}
	return out, true
}

func (c *conn) writePipelineError(msg *wire.Message, s aggScope, what string, err error) (*wire.Message, bool) {
	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("a writing aggregation could not be completed", "ns", s.primary, "what", what, "err", err)
	out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: this aggregation's input reaches past what the fast tier holds, so it was run against "+
			"the archive, and "+what+": "+err.Error())
	if rerr != nil {
		return nil, false
	}
	return out, true
}

// writeSink is where a write pipeline's result goes, and how it gets there.
type writeSink struct {
	r      *Router
	how    string
	target string

	// merge feeds batches back through the caller's own stage.
	mergeStage bson.Raw
	mergeDB    string

	// out writes documents into a collection, either the target itself or a
	// temporary one that is renamed over it at the end.
	outColl  *mongo.Collection
	rename   bool
	tempName string
	finalNS  string
}

// newWriteSink prepares the destination and reports how the write will be done.
func (c *conn) newWriteSink(ctx context.Context, final bson.Raw, db string) (*writeSink, error) {
	outDB, outColl, isOut := outTarget(final, db)
	if !isOut {
		return &writeSink{
			r: c.r, how: "$merge", target: db,
			mergeStage: final, mergeDB: db,
		}, nil
	}

	target := outDB + "." + outColl
	if c.r.cfg.Manages(target) {
		// The target is tiered, so the archive has to receive these documents.
		// Only ordinary writes reach it: the feeder ignores the rename $out
		// would otherwise use, because replaying destructive DDL into the
		// archive is what would let one accident on the fast tier destroy it.
		coll := c.r.hot.Database(outDB).Collection(outColl)
		if _, err := coll.DeleteMany(ctx, bson.D{}); err != nil {
			return nil, fmt.Errorf("clear %s: %w", target, err)
		}
		return &writeSink{
			r: c.r, how: "replaced in place, so the result mirrors to the archive",
			target: target, outColl: coll,
		}, nil
	}

	// Untiered, so nothing has to mirror and the switch can stay atomic the way
	// mongod's own $out is: build it beside the target, then rename over it.
	temp := fmt.Sprintf("tmp.thermocline.out.%d", time.Now().UnixNano())
	return &writeSink{
		r: c.r, how: "written beside the target and renamed over it",
		target: target, outColl: c.r.hot.Database(outDB).Collection(temp),
		rename: true, tempName: outDB + "." + temp, finalNS: target,
	}, nil
}

func (s *writeSink) write(ctx context.Context, batch []any) error {
	if len(batch) == 0 {
		return nil
	}
	if s.outColl != nil {
		_, err := s.outColl.InsertMany(ctx, batch)
		return err
	}
	// $merge applies per document, so feeding it in batches gives the same
	// result as feeding it everything at once.
	return s.r.hot.Database(s.mergeDB).RunCommand(ctx, bson.D{
		{Key: "aggregate", Value: 1},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$documents", Value: batch}},
			s.mergeStage,
		}},
		{Key: "cursor", Value: bson.D{}},
	}).Err()
}

func (s *writeSink) commit(ctx context.Context) error {
	if !s.rename {
		return nil
	}
	// An $out over a collection that does not exist yet still has to leave one
	// behind, so the temporary collection is created even when empty.
	if err := s.r.hot.Database(splitDB(s.tempName)).CreateCollection(ctx, splitColl(s.tempName)); err != nil {
		// Already created by the first insert, which is the usual case.
		_ = err
	}
	return s.r.hot.Database("admin").RunCommand(ctx, bson.D{
		{Key: "renameCollection", Value: s.tempName},
		{Key: "to", Value: s.finalNS},
		{Key: "dropTarget", Value: true},
	}).Err()
}

// abandon removes a half-written temporary collection. Leaving one behind would
// be litter that nothing else ever cleans up.
func (s *writeSink) abandon(ctx context.Context) {
	if !s.rename {
		return
	}
	if err := s.r.hot.Database(splitDB(s.tempName)).Collection(splitColl(s.tempName)).
		Drop(context.WithoutCancel(ctx)); err != nil {
		s.r.log.Warn("could not remove a temporary collection left by a failed aggregation",
			"ns", s.tempName, "err", err)
	}
}

func splitDB(ns string) string   { db, _ := splitNS(ns); return db }
func splitColl(ns string) string { _, coll := splitNS(ns); return coll }

// streamFromArchive runs the reading part of a pipeline against the archive and
// hands its output to sink in batches.
func (c *conn) streamFromArchive(ctx context.Context, db, coll string, read []bson.Raw, sink func(context.Context, []any) error) (int, error) {
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
		return 0, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	batch := make([]any, 0, writeBatch)
	total := 0
	for cur.Next(ctx) {
		doc := make(bson.Raw, len(cur.Current))
		copy(doc, cur.Current)
		batch = append(batch, doc)
		total++
		if len(batch) >= writeBatch {
			if err := sink(ctx, batch); err != nil {
				return total, err
			}
			batch = batch[:0]
		}
	}
	if err := cur.Err(); err != nil {
		return total, err
	}
	if err := sink(ctx, batch); err != nil {
		return total, err
	}
	return total, nil
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
