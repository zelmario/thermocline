package feeder

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// prefetch reads the archive's copies of the documents a flush is about to
// write, several at a time and outside the flush's transaction, so that the
// transaction finds them in the archive's cache.
//
// mongod applies a bulk write one operation after the other, and a replace by
// _id first reads the document it replaces. On an archive whose data is many
// times its memory that read is a disk read, so a flush ran at one disk read
// at a time while the disk could do many. Measured on staging at 22,000 ops/s
// with the archive held to 3,000 reads/s: the feeder mirrored about 1,000
// events a second, about a millisecond each, spent 4 to 6 of every 5 seconds
// in the transaction, and fell behind by 40 seconds, while the archive's disk
// did 1,600 reads a second and its CPU sat 60% idle.
//
// The reads change nothing and decide nothing: what the transaction writes
// and in what order is as before, and a prefetch that fails or is cut short
// only leaves the transaction to read from disk as it did. It runs beside the
// questions flush asks first, so the two overlap. wait may be called more
// than once; flush also defers it, so an early return stops the reads.
//
// The projection names a field no document has, which the _id index cannot
// answer, so mongod fetches each document and returns only its _id.
func (f *Feeder) prefetch(ctx context.Context, ops []pendingOp) (wait func()) {
	ids := map[string][]bson.RawValue{}
	var order []string
	for _, op := range ops {
		if op.id.Type == 0 {
			continue
		}
		if _, seen := ids[op.ns]; !seen {
			order = append(order, op.ns)
		}
		ids[op.ns] = append(ids[op.ns], op.id)
	}
	type chunk struct {
		ns  string
		ids []bson.RawValue
	}
	var chunks []chunk
	for _, ns := range order {
		v := ids[ns]
		for len(v) > 0 {
			n := min(len(v), prefetchChunk)
			chunks = append(chunks, chunk{ns, v[:n]})
			v = v[n:]
		}
	}
	if len(chunks) == 0 {
		return func() {}
	}
	pctx, cancel := context.WithTimeout(ctx, prefetchBudget)
	work := make(chan chunk, len(chunks))
	for _, c := range chunks {
		work <- c
	}
	close(work)
	var wg sync.WaitGroup
	for range min(prefetchLanes, len(chunks)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range work {
				db, coll := splitNS(c.ns)
				cur, err := f.cold.Database(db, options.Database().SetReadPreference(readpref.Primary())).
					Collection(coll).Find(pctx,
					bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: c.ids}}}},
					options.Find().SetProjection(bson.D{{Key: "_tcPrefetch", Value: 1}}).
						SetBatchSize(int32(len(c.ids))))
				if err != nil {
					continue
				}
				for cur.Next(pctx) {
				}
				_ = cur.Close(context.WithoutCancel(pctx))
			}
		}()
	}
	started := time.Now()
	var once sync.Once
	return func() {
		once.Do(func() {
			wg.Wait()
			cancel()
			f.pace.prefetch += time.Since(started)
		})
	}
}

// Prefetch tuning. mongod fetches the documents of one read one after the
// other, so a read of 50 is 50 disk reads in a row: with 8 lanes of 50 the
// prefetch took 1.2 to 2.6 of every 5 seconds on staging and the feeder gained
// 8%. Short reads in more lanes are what put many disk reads in flight. The
// budget bounds what a stalled archive can add to a flush: past it the
// transaction reads from disk as it always did.
const (
	prefetchLanes  = 16
	prefetchChunk  = 16
	prefetchBudget = 2 * time.Second
)
