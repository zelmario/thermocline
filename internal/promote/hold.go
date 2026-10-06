package promote

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// HeldForTest runs between the fast tier's answer and the hold, where an
// eviction sweep can land. Tests only; nil in every daemon.
var HeldForTest func(ns string, ids []bson.RawValue, have []bool)

// SettleWait bounds how long HoldPresent waits for an eviction already
// announced for a document it found on the fast tier. The evictor deletes a
// moment after it announces, so this is an upper bound for a stalled sweep,
// not a wait anyone pays in the ordinary case.
const SettleWait = 2 * time.Second

// InFlightFor bounds how old an announcement can be and still belong to an
// eviction that has yet to delete. One older than this is left standing from a
// delete that matched nothing, which meta.AnnounceEviction keeps on purpose for
// up to an hour, and waiting on it would cost every caller the whole SettleWait.
const InFlightFor = 30 * time.Second

// HoldPresent asks the fast tier which of ids it holds, holds those against
// eviction for hold, and reports which ones the caller may count on staying.
//
// Asking and then holding is two steps, and an eviction sweep fits between
// them. The evictor's half of the handshake is announce, read the holds,
// delete: a sweep that read the holds before this one was written deletes the
// document anyway. For a copy-up the statement it was for then runs against a
// fast tier without it: measured in the fuzzer's churn round, seed 16394, an
// updateMany $inc reported 55 modified where MongoDB modified 56. For a warm,
// the claim it writes next describes a document already gone.
//
// So the hold is followed by reading the announcements, the order the insert
// path in the router uses: whichever side runs second sees the other. A
// document with an eviction in flight is waited for until it either leaves the
// fast tier, and is then reported absent so the caller copies it, or the
// evictor takes the announcement back on seeing the hold. A wait that runs out
// leaves it reported held, which is what this did before.
//
// left names the positions an eviction took away while this waited. Their
// archived copy has to be read again rather than taken from a read made before
// this, because the evictor only removes a document once the archive holds its
// current version, which may be newer than that read. See RereadArchived.
//
// An error writing the hold is returned with the answer, which still stands:
// the caller decides whether a document it could not hold is worth refusing
// over.
func HoldPresent(ctx context.Context, hot *mongo.Collection, cold *mongo.Client, ns string, ids []bson.RawValue, hold time.Duration) (have []bool, left []int, holdErr error, err error) {
	have, err = Held(ctx, hot, ids)
	if err != nil {
		return nil, nil, nil, err
	}
	if HeldForTest != nil {
		HeldForTest(ns, ids, have)
	}
	var present []bson.RawValue
	var at []int
	for i := range ids {
		if have[i] {
			present = append(present, ids[i])
			at = append(at, i)
		}
	}
	if len(present) == 0 {
		return have, nil, nil, nil
	}
	holdErr = holdOnly(ctx, cold, ns, present, hold)
	gone := awaitEvictions(ctx, hot, cold, ns, present)
	for j, i := range at {
		if gone[j] {
			have[i] = false
			left = append(left, i)
		}
	}
	return have, left, holdErr, nil
}

// holdOnly keeps documents the fast tier already holds there for a while, and
// says nothing else about them. See meta.HoldOnly.
func holdOnly(ctx context.Context, cold *mongo.Client, ns string, ids []bson.RawValue, hold time.Duration) error {
	models := make([]mongo.WriteModel, 0, len(ids))
	for _, id := range ids {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: meta.DocID(ns, id)}}).
			SetUpdate(meta.HoldOnly(ns, id, hold)).
			SetUpsert(true))
	}
	_, err := cold.Database(meta.DB).Collection(meta.Residency).
		BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}

// awaitEvictions reports which of present an eviction already announced has
// taken off the fast tier, waiting up to SettleWait for the ones still there.
// A question it cannot ask ends the wait with what it knows.
func awaitEvictions(ctx context.Context, hot *mongo.Collection, cold *mongo.Client, ns string, present []bson.RawValue) []bool {
	gone := make([]bool, len(present))
	docIDs := make([]string, len(present))
	for i, id := range present {
		docIDs[i] = meta.DocID(ns, id)
	}
	deadline := time.Now().Add(SettleWait)
	for {
		pending, err := pendingEvictions(ctx, cold, docIDs)
		if err != nil {
			return gone
		}
		var ask []int
		for i, d := range docIDs {
			if pending[d] && !gone[i] {
				ask = append(ask, i)
			}
		}
		if len(ask) == 0 {
			return gone
		}
		sub := make([]bson.RawValue, len(ask))
		for j, i := range ask {
			sub[j] = present[i]
		}
		have, herr := Held(ctx, hot, sub)
		if herr != nil {
			return gone
		}
		waiting := 0
		for j, i := range ask {
			if have[j] {
				waiting++
			} else {
				gone[i] = true
			}
		}
		if waiting == 0 || time.Now().After(deadline) {
			return gone
		}
		select {
		case <-ctx.Done():
			return gone
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// pendingEvictions reports which of docIDs carry a recent eviction
// announcement the feeder has not yet spent.
//
// Recent by the archive's own clock, because $$NOW is what wrote the notice's
// at, so no two machines' clocks are compared here.
func pendingEvictions(ctx context.Context, cold *mongo.Client, docIDs []string) (map[string]bool, error) {
	cur, err := cold.Database(meta.DB).Collection(meta.Evicting).Find(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}},
		{Key: "$expr", Value: bson.D{{Key: "$gte", Value: bson.A{
			"$at", bson.D{{Key: "$subtract", Value: bson.A{"$$NOW", InFlightFor.Milliseconds()}}},
		}}}},
	})
	if err != nil {
		return nil, fmt.Errorf("read eviction announcements: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	out := map[string]bool{}
	for cur.Next(ctx) {
		var n meta.EvictionNotice
		if cur.Decode(&n) != nil || n.Left() <= 0 {
			continue
		}
		out[n.ID] = true
	}
	return out, cur.Err()
}

// RereadArchived returns the archive's current live copy of each id, keyed by
// the _id's type and bytes, for the documents HoldPresent reported an eviction
// took. An id with no live copy any more is absent from the map.
func RereadArchived(ctx context.Context, archived *mongo.Collection, ids []bson.RawValue) (map[string]bson.Raw, error) {
	want := make(bson.A, len(ids))
	for i, id := range ids {
		want[i] = id
	}
	cur, err := archived.Find(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: want}}},
		meta.NotArchivedDeleted(),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	found := map[string]bson.Raw{}
	for cur.Next(ctx) {
		if id, lerr := cur.Current.LookupErr("_id"); lerr == nil {
			found[RawKey(id)] = append(bson.Raw(nil), cur.Current...)
		}
	}
	return found, cur.Err()
}

// RawKey is an _id as a map key, by its type and stored bytes.
func RawKey(id bson.RawValue) string {
	return string(id.Type) + string(id.Value)
}
