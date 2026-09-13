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
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
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

	// FreeDisk is the fraction of the fast tier's filesystem still free, from
	// dbStats. The warmer acts on it; the evictor deliberately does not.
	FreeDisk float64

	// FeederID is the id the lag was looked for under, carried so a reading
	// that could not be taken can say which checkpoint it went looking for.
	// A wrong id and a stopped feeder look identical without it.
	FeederID string

	// Missing names the checks that could not be taken, usually a permissions
	// problem or a MongoDB version that reports the metric elsewhere.
	Missing []string
}

func (r *Reading) missing(name string) { r.Missing = append(r.Missing, name) }

// IsMissingSignal reports whether a named reading could not be taken, for
// callers outside this package that act on one of them.
func (r Reading) IsMissingSignal(name string) bool { return r.isMissing(name) }

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
		// Named, because the two ways to get here are a feeder that is not
		// running and an id that names a checkpoint nothing writes, and they
		// look identical from a log line. evictor.feeder_id used to default to
		// the literal "default" rather than to feeder.id, so a deployment that
		// renamed its feeder and left the evictor alone paused for ever while
		// reporting a mirroring problem it did not have.
		return Verdict{Reason: fmt.Sprintf("mirroring lag is unknown for feeder id %q, so removing "+
			"anything could lose it: either no feeder with that id is running, or the id is wrong "+
			"(check evictor.feeder_id against feeder.id)", r.FeederID)}
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

	// live watches the published position move, which is how this decides
	// whether the feeder is still running. The alternative is subtracting the
	// feeder's clock from this one, and this process and the feeder are not
	// required to be on the same host. See checkpoint.Liveness.
	live checkpoint.Liveness

	// skew is how far this host's clock is ahead of the archive's, as last
	// measured, so the feeder's published instant and this one can be compared
	// in the archive's frame. See LagFrom.
	//
	// Guarded because a Checker is a long-lived object handed to a daemon, and
	// nothing about it says samples have to be taken one at a time.
	skewMu    sync.Mutex
	skew      time.Duration
	skewAt    time.Time
	skewKnown bool
}

// New returns a Checker. feederID must match the running feeder's id.
func New(hot, cold *mongo.Client, feederID string) *Checker {
	return &Checker{hot: hot, cold: cold, feederID: feederID}
}

// Sample takes one reading. It never returns an error for a single unavailable
// metric; those land in Reading.Missing so Evaluate can decide.
func (c *Checker) Sample(ctx context.Context) Reading {
	r := Reading{FeederID: c.feederID}
	c.sampleFeederLag(ctx, &r)
	c.sampleServerStatus(ctx, &r)
	c.sampleReplicationLag(ctx, &r)
	c.sampleOplogWindow(ctx, &r)
	c.sampleFreeDisk(ctx, &r)
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
	// Decoded into the record the feeder writes rather than a copy of its
	// fields, so there is one definition of what is in there.
	var pos checkpoint.PositionRecord
	if perr := c.cold.Database(meta.DB).Collection(meta.PositionsColl).
		FindOne(ctx, bson.D{{Key: "_id", Value: c.feederID}}).Decode(&pos); perr == nil && !pos.At.IsZero() {
		// A position left behind by a feeder that has stopped must not read as
		// "caught up": the checkpoint below is then the honest answer. It is
		// decided by watching the record move, because the feeder need not be
		// on this host and subtracting its clock from this one is only as sound
		// as the difference between two machines nothing keeps in step.
		theirs, theirsKnown := pos.Skew()
		mine, mineKnown := c.archiveSkew(ctx)
		if c.live.Observe(pos, time.Now(), mine, mineKnown) {
			latest := pos.At
			if pos.SavedAt.After(latest) {
				latest = pos.SavedAt
			}
			r.FeederLag = LagFrom(time.Now(), latest, mine, theirs, mineKnown && theirsKnown)
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

// sampleFreeDisk reads what fraction of the fast tier's filesystem is still
// free, from dbStats, which reports the filesystem the data sits on.
//
// This is the one signal that ends a deployment rather than slowing it. The
// warmer fills the fast tier, the fast tier is by definition smaller than the
// archive, and a warm window is bounded by time rather than by size, so a
// window chosen from the age of the data instead of the size of the tier fills
// the disk. On the first day of an adoption the evictor cannot make room
// either, because a new replica set's oplog window is its own age.
//
// Only the warmer acts on it. Pausing the evictor when a disk is nearly full
// would hold back the one thing that frees space.
func (c *Checker) sampleFreeDisk(ctx context.Context, r *Reading) {
	var stats struct {
		FsUsedSize  int64 `bson:"fsUsedSize"`
		FsTotalSize int64 `bson:"fsTotalSize"`
	}
	err := c.hot.Database("admin").RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&stats)
	if err != nil || stats.FsTotalSize <= 0 {
		r.missing("free disk")
		return
	}
	r.FreeDisk = float64(stats.FsTotalSize-stats.FsUsedSize) / float64(stats.FsTotalSize)
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
