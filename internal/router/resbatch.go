package router

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// residencyBatcher sends the residency holds of concurrent copy-ups to the
// archive together.
//
// Every copy-up a read starts writes one hold, and during an adoption that is
// hundreds a second, each its own round trip and its own journal commit on the
// cluster the fast tier is there to relieve. Measured on staging from an empty
// fast tier at 9,000 ops/s against an archive held to 3,000 IOPS: 156,000
// holds in 300s, the archive's largest writer by lock time, and the run
// without them did 7% more and cut the update p99 from 323 to 99 ms.
//
// The holds are the same documents written the same way; only the trip is
// shared. A copy-up still waits for its own hold before it writes the
// document, so the order the hold exists for is kept. See copyUpFrom.
type residencyBatcher struct {
	coll *mongo.Collection

	mu      sync.Mutex
	pending []residencyReq
	kick    chan struct{}
}

type residencyReq struct {
	model mongo.WriteModel
	done  chan error
}

// residencyLinger is how long the first hold of a batch waits for others to
// join it, and residencyBatchMax how many one batch takes. Two milliseconds is
// a fraction of one archive write under load, and a copy-up from a read is
// asynchronous: the client already has its answer.
const (
	residencyLinger   = 2 * time.Millisecond
	residencyBatchMax = 256
)

func newResidencyBatcher(coll *mongo.Collection) *residencyBatcher {
	b := &residencyBatcher{coll: coll, kick: make(chan struct{}, 1)}
	go b.loop()
	return b
}

// write queues one hold and waits for the batch that carries it.
func (b *residencyBatcher) write(ctx context.Context, model mongo.WriteModel) error {
	req := residencyReq{model: model, done: make(chan error, 1)}
	b.mu.Lock()
	b.pending = append(b.pending, req)
	b.mu.Unlock()
	select {
	case b.kick <- struct{}{}:
	default:
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *residencyBatcher) loop() {
	for range b.kick {
		time.Sleep(residencyLinger)
		for {
			b.mu.Lock()
			n := min(len(b.pending), residencyBatchMax)
			batch := append([]residencyReq(nil), b.pending[:n]...)
			b.pending = b.pending[n:]
			b.mu.Unlock()
			if n == 0 {
				break
			}
			b.flush(batch)
		}
	}
}

// flush writes one batch and tells each hold how its own write went.
func (b *residencyBatcher) flush(batch []residencyReq) {
	models := make([]mongo.WriteModel, len(batch))
	for i, req := range batch {
		models[i] = req.model
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Unordered: independent holds, and one that cannot be written must not
	// stop the rest.
	_, err := b.coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	errs := make([]error, len(batch))
	var bwe mongo.BulkWriteException
	switch {
	case err == nil:
	case errors.As(err, &bwe) && bwe.WriteConcernError == nil:
		for _, we := range bwe.WriteErrors {
			if we.Index >= 0 && we.Index < len(errs) {
				errs[we.Index] = we
			}
		}
	default:
		for i := range errs {
			errs[i] = err
		}
	}
	for i, req := range batch {
		req.done <- errs[i]
	}
}

// residencyWriter is the router's batcher, made on first use so a Router
// built directly, as tests do, gets one too.
func (r *Router) residencyWriter() *residencyBatcher {
	r.resBatchOnce.Do(func() {
		r.resBatch = newResidencyBatcher(r.cold.Database(meta.DB).Collection(meta.Residency))
	})
	return r.resBatch
}
