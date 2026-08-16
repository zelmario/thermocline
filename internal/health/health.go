// Package health reads the live signals that decide whether eviction may run.
//
// A bulk delete campaign is one of the more destructive things you can point at
// a busy replica set. It generates an oplog entry per document, competes for
// write tickets, and dirties cache. Every reading here is a way that campaign
// can hurt the cluster it runs against, plus one that is not about the cluster
// at all: how far behind the archive is.
package health

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmar/thermocline/internal/config"
	"github.com/zelmar/thermocline/internal/meta"
)

// Reading is one sample of the signals eviction depends on. A zero value with
// the matching Missing entry means the check could not be taken.
type Reading struct {
	FeederLag      time.Duration
	ReplicationLag time.Duration
	CacheDirty     float64
	WriteTickets   int
	// WriteTicketsTotal is the size of the pool WriteTickets is free out of.
	// MongoDB 7.0 made this dynamic, so the free count alone says nothing.
	WriteTicketsTotal int
	OplogWindow       time.Duration

	// Missing names the checks that could not be taken, usually a permissions
	// problem or a MongoDB version that reports the metric elsewhere.
	Missing []string
}

func (r *Reading) missing(name string) { r.Missing = append(r.Missing, name) }

func (r Reading) isMissing(name string) bool {
	for _, m := range r.Missing {
		if m == name {
			return true
		}
	}
	return false
}

// Verdict is the decision for one sample.
type Verdict struct {
	OK     bool
	Reason string
}

// Evaluate decides whether eviction may proceed.
//
// The mirroring check fails closed and every other check fails open. That split
// is deliberate: mirroring lag is a correctness guard, because removing a
// document the archive has not received destroys it. The rest are performance
// guards, and refusing to evict forever because one metric moved in a new
// MongoDB release would be its own kind of outage.
func Evaluate(r Reading, limits config.BackoffConfig, maxFeederLag time.Duration) Verdict {
	if r.isMissing("feeder lag") {
		return Verdict{Reason: "mirroring lag is unknown, so removing anything could lose it"}
	}
	if r.FeederLag > maxFeederLag {
		return Verdict{Reason: fmt.Sprintf("mirroring is %s behind, limit %s", r.FeederLag.Round(time.Second), maxFeederLag)}
	}
	if !r.isMissing("replication lag") && r.ReplicationLag > limits.MaxReplicationLag.D() {
		return Verdict{Reason: fmt.Sprintf("replication is %s behind, limit %s", r.ReplicationLag.Round(time.Second), limits.MaxReplicationLag)}
	}
	if !r.isMissing("cache dirty") && r.CacheDirty > limits.MaxCacheDirty {
		return Verdict{Reason: fmt.Sprintf("cache is %.1f%% dirty, limit %.1f%%", r.CacheDirty*100, limits.MaxCacheDirty*100)}
	}
	if !r.isMissing("write tickets") && r.WriteTicketsTotal > 0 {
		// A fraction, not a count. MongoDB 7.0 replaced the fixed pool of 128
		// tickets with one sized to the host and resized under load; a
		// container can report a total of 8. Any absolute floor large enough to
		// be useful on the old servers is unreachable on the new ones, and
		// eviction would pause forever on a perfectly healthy cluster.
		free := float64(r.WriteTickets) / float64(r.WriteTicketsTotal)
		if free < limits.MinWriteTicketsFree {
			return Verdict{Reason: fmt.Sprintf("%d of %d write tickets free (%.0f%%), limit %.0f%%",
				r.WriteTickets, r.WriteTicketsTotal, free*100, limits.MinWriteTicketsFree*100)}
		}
	}
	if !r.isMissing("oplog window") && r.OplogWindow < limits.MinOplogWindow.D() {
		return Verdict{Reason: fmt.Sprintf("oplog window is %s, limit %s", r.OplogWindow.Round(time.Minute), limits.MinOplogWindow)}
	}
	return Verdict{OK: true}
}

// Checker samples both tiers.
type Checker struct {
	hot      *mongo.Client
	cold     *mongo.Client
	feederID string
}

// New returns a Checker. feederID must match the running feeder's id.
func New(hot, cold *mongo.Client, feederID string) *Checker {
	return &Checker{hot: hot, cold: cold, feederID: feederID}
}

// Sample takes one reading. It never returns an error for a single unavailable
// metric; those land in Reading.Missing so Evaluate can decide.
func (c *Checker) Sample(ctx context.Context) Reading {
	var r Reading
	c.sampleFeederLag(ctx, &r)
	c.sampleServerStatus(ctx, &r)
	c.sampleReplicationLag(ctx, &r)
	c.sampleOplogWindow(ctx, &r)
	return r
}

// sampleFeederLag reads how far behind the archive is, from what the feeder
// publishes. No inter-process channel needed: the record is the signal.
//
// The position is preferred over the checkpoint because the checkpoint moves
// only once per feeder.checkpoint_every, which would make eviction throughput
// depend on a durability setting that has nothing to do with it. Raise
// checkpoint_every to a minute for a large deployment and a perfectly healthy
// feeder starts looking a minute behind.
//
// Reading the fresher record is safe here because it is not what makes eviction
// safe. Nothing is removed unless the archive is confirmed to hold that
// specific document, checked per batch against the cold tier; this lag gate is
// a coarse net over that, for stopping a sweep while mirroring is struggling.
func (c *Checker) sampleFeederLag(ctx context.Context, r *Reading) {
	var rec struct {
		EventAt time.Time `bson:"eventAt"`
		SavedAt time.Time `bson:"savedAt"`
	}
	var pos struct {
		At      time.Time `bson:"at"`
		SavedAt time.Time `bson:"savedAt"`
		EveryMS int64     `bson:"everyMS"`
	}
	if perr := c.cold.Database(meta.DB).Collection(meta.PositionsColl).
		FindOne(ctx, bson.D{{Key: "_id", Value: c.feederID}}).Decode(&pos); perr == nil && !pos.At.IsZero() {
		// A position left behind by a feeder that has stopped must not read as
		// "caught up". Four missed publications says the publisher is gone, and
		// the checkpoint below is then the honest answer.
		every := time.Duration(pos.EveryMS) * time.Millisecond
		if every <= 0 {
			every = time.Second
		}
		stale := 4 * every
		if stale < 2*time.Second {
			stale = 2 * time.Second
		}
		if time.Since(pos.SavedAt) <= stale {
			latest := pos.At
			if pos.SavedAt.After(latest) {
				latest = pos.SavedAt
			}
			r.FeederLag = max(time.Since(latest), 0)
			return
		}
	}

	err := c.cold.Database(meta.DB).Collection(meta.Checkpoints).
		FindOne(ctx, bson.D{{Key: "_id", Value: c.feederID}}).Decode(&rec)
	if err != nil || rec.EventAt.IsZero() {
		r.missing("feeder lag")
		return
	}
	// An idle feeder saves its position without new events, so lag is measured
	// from the newer of the two. Otherwise a quiet database looks like an
	// outage and eviction stalls for no reason.
	latest := rec.EventAt
	if rec.SavedAt.After(latest) {
		latest = rec.SavedAt
	}
	r.FeederLag = max(time.Since(latest), 0)
}

func (c *Checker) sampleServerStatus(ctx context.Context, r *Reading) {
	var status bson.Raw
	err := c.hot.Database("admin").RunCommand(ctx, bson.D{
		{Key: "serverStatus", Value: 1},
		{Key: "metrics", Value: false},
		{Key: "locks", Value: false},
	}).Decode(&status)
	if err != nil {
		r.missing("cache dirty")
		r.missing("write tickets")
		return
	}

	dirty, okDirty := lookupNumber(status, "wiredTiger", "cache", "tracked dirty bytes in the cache")
	cacheMax, okCache := lookupNumber(status, "wiredTiger", "cache", "maximum bytes configured")
	if okDirty && okCache && cacheMax > 0 {
		r.CacheDirty = dirty / cacheMax
	} else {
		r.missing("cache dirty")
	}

	// MongoDB 7.0 moved ticket accounting under queues.execution. Try the new
	// location first, then the old one.
	free, okFree := lookupNumber(status, "queues", "execution", "write", "available")
	total, okTotal := lookupNumber(status, "queues", "execution", "write", "totalTickets")

	if !okFree {
		free, okFree = lookupNumber(status, "wiredTiger", "concurrentTransactions", "write", "available")
		out, okOut := lookupNumber(status, "wiredTiger", "concurrentTransactions", "write", "out")
		if okFree && okOut {
			total, okTotal = free+out, true
		}
	}
	if okFree && okTotal && total > 0 {
		r.WriteTickets = int(free)
		r.WriteTicketsTotal = int(total)
	} else {
		r.missing("write tickets")
	}
}

func (c *Checker) sampleReplicationLag(ctx context.Context, r *Reading) {
	var status struct {
		Members []struct {
			StateStr   string    `bson:"stateStr"`
			OptimeDate time.Time `bson:"optimeDate"`
		} `bson:"members"`
	}
	err := c.hot.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).
		Decode(&status)
	if err != nil {
		r.missing("replication lag")
		return
	}

	var primary time.Time
	oldest := time.Time{}
	secondaries := 0
	for _, m := range status.Members {
		switch m.StateStr {
		case "PRIMARY":
			primary = m.OptimeDate
		case "SECONDARY":
			secondaries++
			if oldest.IsZero() || m.OptimeDate.Before(oldest) {
				oldest = m.OptimeDate
			}
		}
	}
	if primary.IsZero() || secondaries == 0 {
		// A single-node set has nothing to lag behind. Report zero rather than
		// missing, so a development cluster does not look unhealthy.
		r.ReplicationLag = 0
		return
	}
	r.ReplicationLag = max(primary.Sub(oldest), 0)
}

func (c *Checker) sampleOplogWindow(ctx context.Context, r *Reading) {
	oplog := c.hot.Database("local").Collection("oplog.rs")

	first, okFirst := oplogEdge(ctx, oplog, 1)
	last, okLast := oplogEdge(ctx, oplog, -1)
	if !okFirst || !okLast {
		r.missing("oplog window")
		return
	}
	r.OplogWindow = max(last.Sub(first), 0)
}

func oplogEdge(ctx context.Context, coll *mongo.Collection, direction int) (time.Time, bool) {
	var doc struct {
		TS bson.Timestamp `bson:"ts"`
	}
	err := coll.FindOne(ctx, bson.D{},
		options.FindOne().
			SetSort(bson.D{{Key: "$natural", Value: direction}}).
			SetProjection(bson.D{{Key: "ts", Value: 1}}),
	).Decode(&doc)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(int64(doc.TS.T), 0).UTC(), true
}

// lookupNumber walks a path in a raw document and coerces the leaf to float64.
// serverStatus mixes int32, int64 and double for counters depending on
// magnitude and release, so the type has to be tolerated rather than asserted.
func lookupNumber(doc bson.Raw, path ...string) (float64, bool) {
	val, err := doc.LookupErr(path...)
	if err != nil {
		return 0, false
	}
	switch val.Type {
	case bson.TypeInt32:
		return float64(val.Int32()), true
	case bson.TypeInt64:
		return float64(val.Int64()), true
	case bson.TypeDouble:
		return val.Double(), true
	default:
		return 0, false
	}
}
