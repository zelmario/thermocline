package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// longAgo is an operation time no oplog holds, which is how a consumer that
// has fallen behind is arranged here. See resyncEvery.
var longAgo = bson.Timestamp{T: 1, I: 1}

// resync is a consumer finding out it has fallen further behind than the oplog,
// and recovering the way an application recovers: note a cluster time, read
// everything, tail from the time it noted.
//
// It reports the time to tail from, and false only when it could not get far
// enough to have one.
func (s *shop) resync(ctx context.Context) (bson.Timestamp, bool) {
	s.lastResync = time.Now()
	s.resyncs.Add(1)

	// Told, or not told. A stream that opens here has started somewhere other
	// than where it was asked to, and everything between is missing from a
	// consumer that has no way to know.
	cur, err := s.orders.Watch(ctx, mongo.Pipeline{},
		options.ChangeStream().SetStartAtOperationTime(&longAgo))
	switch {
	case err == nil:
		_ = cur.Close(context.WithoutCancel(ctx))
		s.violation("a consumer behind the oplog opened its stream anyway, and was never told what it missed")
	case isHistoryLost(err):
		s.told.Add(1)
	default:
		s.errs.Add(1)
		fmt.Printf("  resync: falling behind the oplog answered %v rather than history lost\n", err)
	}

	// The time goes down before the read starts. Everything written after it
	// comes through the stream, and anything the scan also sees is simply seen
	// twice, which an index absorbs.
	t0, ok := s.clusterTime(ctx)
	if !ok {
		return bson.Timestamp{}, false
	}

	// Whatever this consumer has not been told about yet, it now never will be
	// by the stream: it happened in the stretch it fell behind by, and the scan
	// stands in for it. Taken after the time above, so a write that starts
	// later is after it and arrives the ordinary way.
	s.wrote.Range(func(k, _ any) bool {
		if _, ok := s.seenInsert.Load(k); !ok {
			s.coveredIns.Store(k, true)
		}
		return true
	})
	s.removed.Range(func(k, _ any) bool {
		if _, ok := s.seenDelete.Load(k); !ok {
			s.coveredDel.Store(k, true)
		}
		return true
	})

	// One cursor over the whole collection, in batches, while the tiers move.
	scan, err := s.orders.Find(ctx, bson.D{}, options.Find().
		SetBatchSize(500).SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		fmt.Printf("  resync: the scan was refused: %v\n", err)
		return bson.Timestamp{}, false
	}
	defer func() { _ = scan.Close(context.WithoutCancel(ctx)) }()
	fresh := map[string]bool{}
	dups := 0
	for scan.Next(ctx) {
		var d struct {
			ID bson.ObjectID `bson:"_id"`
		}
		if scan.Decode(&d) != nil {
			continue
		}
		id := d.ID.Hex()
		if fresh[id] {
			dups++
		}
		fresh[id] = true
	}
	if err := scan.Err(); err != nil {
		fmt.Printf("  resync: the scan failed part way: %v\n", err)
		return bson.Timestamp{}, false
	}
	if dups > 0 {
		s.violation("a resync's scan of every order returned %d of them twice", dups)
	}
	s.scanned.Add(int64(len(fresh)))

	s.idxMu.Lock()
	s.index = fresh
	s.idxMu.Unlock()
	return t0, true
}

// clusterTime is where the fast tier is now, as a reply to this application
// reports it. A find on a collection nothing tiers, so the router forwards it.
func (s *shop) clusterTime(ctx context.Context) (bson.Timestamp, bool) {
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return bson.Timestamp{}, false
	}
	defer sess.EndSession(ctx)
	sctx := mongo.NewSessionContext(ctx, sess)
	if err := s.db.RunCommand(sctx, bson.D{
		{Key: "find", Value: "store_marker"}, {Key: "limit", Value: 1}}).Err(); err != nil {
		fmt.Printf("  resync: could not read a cluster time: %v\n", err)
		return bson.Timestamp{}, false
	}
	t := sess.OperationTime()
	if t == nil {
		fmt.Println("  resync: the reply carried no operation time")
		return bson.Timestamp{}, false
	}
	return *t, true
}

// keep applies one event to the index.
func (s *shop) keep(id string, present bool) {
	s.idxMu.Lock()
	if present {
		s.index[id] = true
	} else {
		delete(s.index, id)
	}
	s.idxMu.Unlock()
}

// finished says this application has stopped writing, and waits until every
// application sharing the collection has too, so that the consumer judged at
// the end has been shown everything anyone wrote.
func (s *shop) finished(ctx context.Context) {
	if s.ledger == nil {
		return
	}
	_, _ = s.ledger.UpdateOne(ctx, bson.D{{Key: "_id", Value: fmt.Sprintf("done:%d", s.part)}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "at", Value: time.Now()}}}},
		options.UpdateOne().SetUpsert(true))
	deadline := time.Now().Add(30 * time.Minute)
	for time.Now().Before(deadline) {
		n, err := s.ledger.CountDocuments(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^done:"}}}})
		if err == nil && int(n) >= s.parts {
			return
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Println("  the other applications never said they had finished; judging the index anyway")
}

// checkIndex is the consumer's promise, judged against the tiers: what it keeps
// is every order that exists, and nothing else.
//
// The truth is read straight from both tiers. Every order the fast tier holds
// exists, and so does every order in the archive without a tombstone, which is
// what makes the archive's live set a superset once mirroring has caught up.
// Given a few seconds to do that first, because the cancellations of the last
// seconds are tombstoned by the feeder, not by anything here.
func (s *shop) checkIndex(ctx context.Context) {
	if s.resyncEvery <= 0 || s.truthHot == "" || s.truthCold == "" {
		return
	}
	time.Sleep(5 * time.Second)
	ids := func(uri string, filter bson.D) (map[string]bool, bool) {
		c, err := mongo.Connect(options.Client().ApplyURI(uri))
		if err != nil {
			return nil, false
		}
		defer func() { _ = c.Disconnect(context.WithoutCancel(ctx)) }()
		cur, err := c.Database(s.db.Name()).Collection("orders").Find(ctx, filter,
			options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
		if err != nil {
			return nil, false
		}
		out := map[string]bool{}
		for cur.Next(ctx) {
			var d struct {
				ID bson.ObjectID `bson:"_id"`
			}
			if cur.Decode(&d) == nil {
				out[d.ID.Hex()] = true
			}
		}
		return out, cur.Err() == nil
	}
	hot, ok1 := ids(s.truthHot, bson.D{})
	cold, ok2 := ids(s.truthCold, bson.D{{Key: "_tcDeletedAt", Value: bson.D{{Key: "$exists", Value: false}}}})
	if !ok1 || !ok2 {
		s.errs.Add(1)
		fmt.Println("  could not read the tiers to judge the consumer's index")
		return
	}
	truth := cold
	for id := range hot {
		truth[id] = true
	}

	s.idxMu.Lock()
	defer s.idxMu.Unlock()
	var missing, extra []string
	for id := range truth {
		if !s.index[id] {
			missing = append(missing, id)
		}
	}
	for id := range s.index {
		if !truth[id] {
			extra = append(extra, id)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	fmt.Printf("index: the consumer keeps %s orders, the tiers hold %s\n", commas(len(s.index)), commas(len(truth)))
	if len(missing) > 0 {
		s.violation("the consumer's index is missing %d orders the tiers hold, first %s", len(missing), missing[0])
	}
	if len(extra) > 0 {
		s.violation("the consumer's index keeps %d orders the tiers do not hold, first %s", len(extra), extra[0])
	}
}
