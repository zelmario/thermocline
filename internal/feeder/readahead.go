package feeder

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// streamItem is one thing the change stream handed over: an event, an empty
// poll, or the error that ended the stream. It carries what the main loop
// used to ask the stream right after TryNext, taken at that same moment.
type streamItem struct {
	ev       bson.Raw // the event, copied; nil for an empty poll or an error
	token    bson.Raw // the stream's resume token after this item
	batchEnd bool     // the getMore batch this event came in is used up
	err      error    // the stream's error, when TryNext gave up on it
}

// readStream reads the change stream ahead of the feeder's writes, one getMore
// batch at a time, and hands each batch over whole.
//
// The loop used to be strictly in turn: ask the fast tier for the next batch,
// then write it to the archive, then ask again. Asking is not free. With
// updateLookup the fast tier reads every updated document into its event, and
// on staging at 22,000 ops/s the feeder spent 0.9 to 1.9 of every 5 seconds
// waiting for the next batch while it was tens of seconds behind, time the
// archive sat with nothing to write. Read ahead, the next batch is fetched
// while the current one is written.
//
// The order of events, the tokens and the batch ends are exactly what the
// loop saw before; only when it sees them moves. At most two batches wait
// here, the one in the channel and the one being read, so what read-ahead
// holds is bounded by two getMores.
//
// It is the stream's only reader, and it ends when ctx does or the stream
// fails, closing done.
func readStream(ctx context.Context, stream *mongo.ChangeStream, out chan<- []streamItem, done chan<- struct{}) {
	defer close(done)
	send := func(b []streamItem) bool {
		select {
		case out <- b:
			return true
		case <-ctx.Done():
			return false
		}
	}
	var batch []streamItem
	for {
		if stream.TryNext(ctx) {
			batch = append(batch, streamItem{
				ev:       append(bson.Raw(nil), stream.Current...),
				token:    append(bson.Raw(nil), stream.ResumeToken()...),
				batchEnd: stream.RemainingBatchLength() == 0,
			})
			if stream.RemainingBatchLength() == 0 {
				if !send(batch) {
					return
				}
				batch = nil
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}
		item := streamItem{token: append(bson.Raw(nil), stream.ResumeToken()...), err: stream.Err()}
		if !send(append(batch, item)) {
			return
		}
		batch = nil
		if item.err != nil {
			return
		}
	}
}
