package router

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
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

// runWritePipeline reads a $out or $merge pipeline's input and performs its
// final stage on the fast tier, and on the archive where the target is tiered.
//
// fromArchive says where the input comes from. It is the archive whenever the
// pipeline reads more than the fast tier holds, which is why this exists; it is
// the fast tier for a $out whose target is tiered but whose input the fast tier
// can answer, where the reason to be here is the target rather than the input.
// See runTieredOut.
func (c *conn) runWritePipeline(ctx context.Context, msg *wire.Message, cmd bson.Raw, db, coll string,
	s aggScope, fromArchive bool) (*wire.Message, bool) {
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

	var n int
	if fromArchive {
		n, err = c.streamFromArchive(ctx, db, coll, read, sink.write)
	} else {
		aggVal, verr := cmd.LookupErr("aggregate")
		if verr != nil {
			sink.abandon(ctx)
			return c.writePipelineError(msg, s, "reading the aggregate target failed", verr)
		}
		n, err = c.streamFromFastTier(ctx, db, aggVal, read, sink.write)
	}
	if err != nil {
		sink.abandon(ctx)
		where := "the archive"
		if !fromArchive {
			where = "the fast tier"
		}
		return c.writePipelineError(msg, s, "reading its input from "+where+" failed", err)
	}
	if err := sink.commit(ctx); err != nil {
		sink.abandon(ctx)
		return c.writePipelineError(msg, s, "writing its result to the fast tier failed", err)
	}

	if fromArchive {
		c.r.Stats.Archived.Add(1)
		c.cls = ClassArchive
	}
	from := "the archive"
	if !fromArchive {
		from = "the fast tier"
	}
	c.log.Info("aggregation performed here rather than forwarded",
		"ns", s.primary, "read_from", from, "into", sink.target, "documents", n, "how", sink.how,
		"note", "$out into a tiered collection cannot be forwarded: mongod does it by renaming over "+
			"the target, and a rename is not mirrored")

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

	// claimNS is the target, set only when it is tiered and carries a unique
	// index, and it means these documents claim their keys as they are written.
	//
	// $out clears both tiers first, so nothing it writes can duplicate what was
	// there. The claims are for afterwards: a document written here and later
	// evicted lives in the archive alone, and without a claim the registry has
	// nothing to enforce. Measured: a $out of two documents left no rows in the
	// registry, and once the evictor had taken them off the fast tier an insert
	// of one of their keys was accepted, leaving one live copy in the archive
	// and another on the fast tier that the archive refused to mirror.
	claimNS string

	// merge feeds batches back through the caller's own stage.
	mergeStage bson.Raw
	mergeDB    string

	// out writes documents into a collection, either the target itself or a
	// temporary one that is renamed over it at the end.
	outColl  *mongo.Collection
	rename   bool
	tempName string
	finalNS  string

	// A tiered target is filled from a temporary collection rather than
	// directly, because clearing it is part of the write and $out is allowed to
	// read the collection it replaces.
	//
	// Measured with the target cleared first, on a tiered collection holding
	// three documents, aggregate([{$match: {n: {$gte: 2}}}, {$out: itself}]):
	// nought documents left on either tier, where MongoDB leaves two. The
	// input had been deleted before it was read. This is what mongod's own
	// temporary collection is for, and the only difference here is that the
	// last step cannot be a rename, because a rename is not mirrored.
	tieredColl    *mongo.Collection
	tieredArchive *mongo.Collection
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
		//
		// The result is built in a temporary collection and moved in at the
		// end, because clearing the target is part of this write and the
		// pipeline is allowed to be reading the target. See writeSink.
		temp := fmt.Sprintf("tmp.thermocline.out.%d", time.Now().UnixNano())
		return &writeSink{
			r: c.r, how: "replaced on both tiers, so the result is all that is left",
			target: target, claimNS: target,
			outColl:       c.r.hot.Database(outDB).Collection(temp),
			tempName:      outDB + "." + temp,
			tieredColl:    c.r.hot.Database(outDB).Collection(outColl),
			tieredArchive: c.r.cold.Database(outDB).Collection(outColl),
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

// fill moves a tiered $out's result from the temporary collection into the
// target, having first cleared what the target held on both tiers.
func (s *writeSink) fill(ctx context.Context) error {
	if _, err := s.tieredColl.DeleteMany(ctx, bson.D{}); err != nil {
		return fmt.Errorf("clear %s: %w", s.target, err)
	}
	// Clearing the fast tier only reaches what the fast tier holds. A document
	// that has been evicted lives in the archive alone, so nothing above
	// deletes it and a read after this would return it beside the result.
	// Measured: a $out promising exactly one document answered with two, the
	// extra one being an evicted document from before it ran.
	//
	// $out says the target holds the result and nothing else, so the archive's
	// live copies are marked deleted as well. This is the one place the router
	// removes archived data, and it is deliberate: a drop is not replayed into
	// the archive precisely so that one accident cannot destroy it, and a $out
	// is not an accident, it is an application saying what this collection
	// should now contain.
	//
	// After the fast tier, not before. If this fails, the collection still
	// reads as what it held, because the archive is what answers once the fast
	// tier is empty, and the caller gets an error rather than a half-replaced
	// collection reported as success.
	//
	// It is also the expensive half, and worth knowing about: this reaches
	// every live document the archive holds for the namespace, however many
	// that is, where clearing the fast tier only reaches the working set. A
	// $out over a collection with years of history behind it is a large write,
	// so it says how large.
	res, err := s.tieredArchive.UpdateMany(ctx,
		bson.D{meta.NotArchivedDeleted()}, meta.Tombstone(time.Now().UTC()))
	if err != nil {
		return fmt.Errorf("clear the archive's copy of %s: %w", s.target, err)
	}
	if res.ModifiedCount > 0 {
		s.r.log.Info("$out marked the archive's copies deleted",
			"ns", s.target, "documents", res.ModifiedCount,
			"why", "$out says the target holds the result and nothing else, and clearing "+
				"the fast tier reaches only what the fast tier held")
	}

	// Then the result, in the batches the registry claims keys in.
	cur, err := s.outColl.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("read back the result for %s: %w", s.target, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	batch := make([]any, 0, writeBatch)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if cerr := s.claim(ctx, batch); cerr != nil {
			return cerr
		}
		if _, ierr := s.tieredColl.InsertMany(ctx, batch); ierr != nil {
			return ierr
		}
		batch = batch[:0]
		return nil
	}
	for cur.Next(ctx) {
		doc := make(bson.Raw, len(cur.Current))
		copy(doc, cur.Current)
		batch = append(batch, doc)
		if len(batch) >= writeBatch {
			if ferr := flush(); ferr != nil {
				return ferr
			}
		}
	}
	if cerr := cur.Err(); cerr != nil {
		return cerr
	}
	return flush()
}

func (s *writeSink) write(ctx context.Context, batch []any) error {
	if len(batch) == 0 {
		return nil
	}
	if s.outColl != nil {
		// Into the temporary collection. The claims are made when the target is
		// filled from it, because a key claimed for a result that never lands
		// blocks that value for nothing.
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
	if s.tieredColl != nil {
		if err := s.fill(ctx); err != nil {
			return err
		}
		s.dropTemp(ctx)
		return nil
	}
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
	if s.tempName == "" {
		return
	}
	s.dropTemp(ctx)
}

func (s *writeSink) dropTemp(ctx context.Context) {
	if err := s.r.hot.Database(splitDB(s.tempName)).Collection(splitColl(s.tempName)).
		Drop(context.WithoutCancel(ctx)); err != nil {
		s.r.log.Warn("could not remove a temporary collection left by an aggregation",
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

	// No budget: this is the reading half of an $out or $merge, which streams
	// in batches and is expected to be long. Cutting its freshness wait to the
	// caller's maxTimeMS would trade a slow pipeline for a stale one.
	c.archiveReadIsFresh(ctx, db+"."+coll, time.Now(), 0)

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

// streamFromFastTier is the same reading half against the fast tier, for a
// pipeline the fast tier can answer in full.
//
// It takes the aggregate target as the client wrote it, because a pipeline
// beginning with $documents names no collection: its target is the number 1, and
// asking for a collection called "1" is not the same command.
func (c *conn) streamFromFastTier(ctx context.Context, db string, aggVal bson.RawValue, read []bson.Raw,
	sink func(context.Context, []any) error) (int, error) {
	pipeline := bson.A{}
	for _, st := range read {
		pipeline = append(pipeline, st)
	}

	var target any
	if err := aggVal.Unmarshal(&target); err != nil {
		return 0, fmt.Errorf("read the aggregate target: %w", err)
	}
	cur, err := c.r.hot.Database(db).RunCommandCursor(ctx, bson.D{
		{Key: "aggregate", Value: target},
		{Key: "pipeline", Value: pipeline},
		{Key: "cursor", Value: bson.D{}},
	})
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

// runTieredOut performs a $out whose target this system tiers, whatever the
// pipeline reads.
//
// mongod implements $out by building the result beside the target and renaming
// over it. The feeder ignores a rename on purpose, because replaying destructive
// DDL into the archive is what would let one accident on the fast tier destroy
// it. So a forwarded $out into a tiered collection puts the result on the fast
// tier and nowhere else, and leaves the archive holding the collection as it
// was, live.
//
// Measured on a tiered collection: two documents written by $out, the archive
// holding none of them sixty seconds later, its own earlier document still live,
// and a count through the router answering 1 where the fast tier held 2. $out
// says the target holds the result and nothing else, and the durable tier said
// otherwise.
//
// The input side is a separate question from the target. fromArchive says the
// pipeline reads more than the fast tier holds, in which case the input comes
// from the archive exactly as it does for an incomplete read; otherwise the fast
// tier answers it, which is what it would have done had this been forwarded.
func (c *conn) guardTieredOut(ctx context.Context, msg *wire.Message, cmd bson.Raw, db string) (*wire.Message, bool) {
	stages, ok := pipelineStages(cmd)
	if !ok || len(stages) == 0 {
		return nil, false
	}
	final, fok := stages[len(stages)-1].DocumentOK()
	if !fok {
		return nil, false
	}
	outDB, outColl, isOut := outTarget(final, db)
	if !isOut || !c.r.cfg.Manages(outDB+"."+outColl) {
		// Not a $out, or a target nothing tiers. A forwarded $merge is ordinary
		// updates and inserts, which mirror by themselves.
		return nil, false
	}
	if inTransaction(cmd) {
		// $out is not allowed in a transaction, so mongod refuses it for us.
		return nil, false
	}

	// Where the input comes from is a separate question, and the same one a
	// read-only pipeline asks. A pipeline with no input collection reads
	// nothing the archive could add to.
	coll, named := stringField(cmd, "aggregate")
	scope := aggScope{primary: db + "." + coll, writes: true}
	fromArchive := false
	if named {
		if sc, sok := scopeOfAggregate(cmd, db, c.r.ageFieldOf); sok {
			scope = sc
			if _, _, fine := c.r.checkAggregate(ctx, scope); !fine {
				if c.r.cfg.Completeness != config.Archive {
					// Reading the input from the archive is exactly what strict
					// and permissive decline, so this stays out of the way and
					// the ordinary refusal stands.
					return nil, false
				}
				fromArchive = true
			}
		}
	}
	return c.runWritePipeline(ctx, msg, cmd, db, coll, scope, fromArchive)
}

// claim reserves the unique keys of the documents about to be written, so the
// registry holds them once they are evicted. See writeSink.claimNS.
func (s *writeSink) claim(ctx context.Context, batch []any) error {
	if s.claimNS == "" {
		return nil
	}
	idxs := s.r.uniqueIndexes(ctx, s.claimNS)
	if len(idxs) == 0 {
		return nil
	}
	docs := make([]bson.Raw, 0, len(batch))
	for _, d := range batch {
		raw, ok := d.(bson.Raw)
		if !ok {
			// Not a document this can key. Writing it unclaimed is what used to
			// happen to all of them, and it is better than failing the write.
			continue
		}
		docs = append(docs, raw)
	}
	_, clashes, err := s.r.claimKeys(ctx, s.claimNS, docs, idxs, false)
	if err != nil {
		s.r.log.Warn("could not claim the keys a $out writes",
			"ns", s.claimNS, "err", err,
			"effect", "these documents are unprotected once they are evicted; "+
				"run cline keys --rebuild")
		return nil
	}
	if len(clashes) > 0 {
		// The pipeline produced two documents with one unique value, which
		// mongod would refuse when the second reached the index.
		return fmt.Errorf("E11000 duplicate key error collection: %s index: %s :: "+
			"this pipeline produces more than one document with the same unique value",
			s.claimNS, clashes[0].index)
	}
	return nil
}
