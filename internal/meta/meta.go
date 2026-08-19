// Package meta holds the names and record shapes Thermocline stores alongside
// user data. Bookkeeping lives in its own database on the cold tier, so user
// documents are never modified to carry Thermocline's state.
package meta

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// DB is the database holding all Thermocline bookkeeping. It lives on the cold
// tier, which is the durable side of the pair, and the unique key registry of
// the same name lives on the fast tier.
//
// It is a variable rather than a constant because one pair of tiers can carry
// more than one Thermocline deployment, and two deployments sharing a
// bookkeeping database would each read the other's watermarks, residency holds
// and singleton markers as their own. Set it from configuration through SetDB
// and never after a daemon has started; config.Load is the single place that
// does, so every command picks it up before it opens a connection.
var DB = DefaultDB

const (
	// DefaultDB is the bookkeeping database a deployment gets when the config
	// does not name one. It is what every existing deployment already uses, so
	// the name only has to be set when a second deployment shares these tiers.
	DefaultDB = "_thermocline"

	// Residency tracks how long a document should stay in the fast tier. One
	// small record per resident document, so extending a document's stay
	// rewrites 60 bytes instead of the whole document.
	Residency = "residency"

	// Evicting is the announcement channel between evictor and feeder. The
	// evictor writes here before deleting from the fast tier, which is how the
	// feeder tells an eviction apart from an application delete.
	Evicting = "evicting"

	// Checkpoints stores one change stream resume token per feeder.
	Checkpoints = "checkpoints"

	// PositionsColl holds how far each feeder has mirrored, published far more
	// often than the durable checkpoint because a fallback read waits on it.
	PositionsColl = "positions"

	// Warming is the announcement channel between warmer and feeder, mirroring
	// what Evicting does in the other direction. A document copied up into the
	// fast tier produces an insert that the feeder would otherwise mirror
	// straight back down, into the archive it was just read from.
	Warming = "warming"

	// Markers holds small singleton documents, currently the warmer's
	// heartbeat. It lets the feeder skip echo suppression entirely when no
	// warmer is running, which is almost always.
	Markers = "markers"

	// WarmerActiveMarker is the _id of the warmer's heartbeat in Markers.
	WarmerActiveMarker = "warmerActive"

	// EvictionPausedMarker is the _id of an operator's hold on eviction.
	//
	// The evictor's own safety check is per document and it is presence only:
	// nothing is removed unless the archive is confirmed to hold that document.
	// That is exactly right when the archive is merely behind, and not enough
	// when it has been restored. A restore can leave the archive holding an
	// older version of a document, which the check confirms as present, and the
	// fast tier's newer copy is then removed. Mirroring lag usually makes this
	// impossible, because a restored checkpoint sends the feeder back over the
	// gap, but a restore of the data alone leaves the lag looking healthy.
	//
	// So it is an explicit switch rather than something inferred. Set it before
	// restoring and clear it afterwards.
	EvictionPausedMarker = "evictionPaused"

	// PromotingActiveMarker is the _id of the router's heartbeat in Markers.
	//
	// The router copies documents up on a miss, and each of those inserts is a
	// change event the feeder would mirror straight back into the archive it was
	// just read from. This marker is what lets the feeder skip looking for those
	// echoes entirely when no promotion has happened recently, which is the
	// normal state of a system whose fast tier is holding what it should.
	PromotingActiveMarker = "promotingActive"

	// DaemonStats is where the background daemons publish what their last pass
	// did, one document each.
	//
	// They are separate processes with no listening socket, so their counters
	// only ever reached a log line, which nothing can alert on. Publishing here
	// and exporting from the router is the same choice already made for
	// mirroring lag, and for the same reason: a gauge a daemon scrapes from
	// itself goes silent exactly when the daemon dies, while one read from
	// elsewhere shows the time since its last pass climbing, which an alert can
	// see.
	DaemonStats = "daemonstats"

	// CacheAdvice is where the router publishes what it has learned about which
	// queries would be worth caching. It lives on the cold tier like every
	// other piece of bookkeeping, so `cline cache` can read it without needing
	// to reach the router itself.
	CacheAdvice = "cacheadvice"

	// Watermarks records how far back the fast tier is known to be complete,
	// one per namespace. This is what lets a range query be answered from the
	// fast tier alone rather than falling through to the archive.
	Watermarks = "watermarks"

	// TombstoneField marks a cold document whose original the application
	// deleted. The document stays for auditing, but reads must not return it.
	TombstoneField = "_tcDeletedAt"

	// BodyDroppedField marks a tombstone that could not keep the document it
	// stands for. See MinimalTombstone.
	BodyDroppedField = "_tcBodyDropped"
)

// SetDB names the bookkeeping database for this process.
//
// Call it once, before anything opens a collection. Every entry point reaches
// it through config.Load, so a daemon that read a config is already pointed at
// the right database; a caller that skips config keeps DefaultDB.
func SetDB(name string) {
	if name == "" {
		name = DefaultDB
	}
	DB = name
}

// DocID is a stable, comparable identifier for one user document across
// namespaces. MongoDB allows any BSON type as an _id, so the bytes are encoded
// rather than interpreted. The result is safe as an _id itself, which lets a
// single collection cover every namespace.
func DocID(ns string, id bson.RawValue) string {
	buf := make([]byte, 0, 1+len(id.Value))
	buf = append(buf, byte(id.Type))
	buf = append(buf, id.Value...)
	return ns + "|" + hex.EncodeToString(buf)
}

// ResidencyRecord is one row of the residency sidecar. NS and Doc are kept
// alongside the encoded id so the evictor can reconstruct a delete against the
// fast tier without decoding DocID.
type ResidencyRecord struct {
	ID         string        `bson:"_id"`
	NS         string        `bson:"ns"`
	Doc        bson.RawValue `bson:"doc"`
	HotUntil   time.Time     `bson:"hotUntil"`
	PromotedAt time.Time     `bson:"promotedAt,omitempty"`
}

// EvictionNotice is one row of the evicting announcement collection. Notices
// expire through a TTL index; they only have to outlive the feeder's lag.
type EvictionNotice struct {
	ID string    `bson:"_id"`
	At time.Time `bson:"at"`
}

// ClearEvictionNotices removes eviction announcements for documents that have
// come back into the fast tier.
//
// A notice means "this document is leaving the fast tier", and the feeder reads
// it to tell an eviction apart from an application delete. Copying a document
// back up makes the notice false, and a notice that outlives its eviction is
// dangerous: the application deletes the document, the feeder finds the stale
// notice, reads the delete as another eviction, and never tombstones it. The
// record stays live in the archive and a later read brings it back.
//
// Notices expire on a TTL, so this only matters inside that window, which is
// exactly when a copy-up is most likely.
func ClearEvictionNotices(ctx context.Context, cold *mongo.Client, docIDs []string) error {
	if len(docIDs) == 0 {
		return nil
	}
	_, err := cold.Database(DB).Collection(Evicting).DeleteMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}}})
	if err != nil {
		return fmt.Errorf("clear eviction notices: %w", err)
	}
	return nil
}

// Tombstone is the update applied to a cold document when the application
// deletes its original.
func Tombstone(at time.Time) bson.D {
	return bson.D{{Key: "$set", Value: bson.D{{Key: TombstoneField, Value: at}}}}
}

// BodyDroppedField marks a tombstone that could not keep the document it
// stands for.
//
// An archived document at the 16 MB limit has no room for TombstoneField, so
// marking it deleted fails and the archive keeps a live copy of a document the
// application has removed. A later read then hands it back, which the design
// calls out as worse than staleness: deleting a document and then finding it
// again is not a slow answer, it is a wrong one. Worse, the feeder cannot get
// past the event at all, so all mirroring stops.
//
// So correctness wins over the audit copy: the body is dropped to make room for
// the mark, and this field says that is what happened, for the one document in
// a deployment that is ever likely to hit it.
// MinimalTombstone is what an archived document becomes when it is too large to
// be marked deleted any other way. It keeps the identity and the mark and
// nothing else.
func MinimalTombstone(id bson.RawValue, at time.Time) bson.D {
	return bson.D{
		{Key: "_id", Value: id},
		{Key: TombstoneField, Value: at},
		{Key: BodyDroppedField, Value: true},
	}
}

// NotArchivedDeleted matches archived documents whose original is still live.
// Anything copying data out of the archive must use it, or documents the
// application deleted come back.
func NotArchivedDeleted() bson.E {
	return bson.E{Key: TombstoneField, Value: bson.D{{Key: "$exists", Value: false}}}
}

// Watermark records how far back the fast tier is complete for one namespace.
//
// CompleteFrom means every live document newer than this instant is present in
// the fast tier. A query bounded inside that range can be answered there alone;
// anything reaching past it has to consult the archive.
type Watermark struct {
	NS           string    `bson:"_id"`
	CompleteFrom time.Time `bson:"completeFrom"`
	UpdatedAt    time.Time `bson:"updatedAt"`
}

// WarmingNotice is one row of the warming announcement collection.
type WarmingNotice struct {
	ID string    `bson:"_id"`
	At time.Time `bson:"at"`
}

// ActiveMarker is a heartbeat written by a daemon so others can cheaply tell
// whether it is running.
// DaemonReport is one background daemon's account of itself.
//
// Counters are cumulative since the daemon started, which is what a Prometheus
// counter wants; a restart shows as a reset, which Prometheus already
// understands. The names are the daemon's own, so a daemon can add a counter
// without the router needing to know about it.
type DaemonReport struct {
	ID       string           `bson:"_id"`
	At       time.Time        `bson:"at"`
	DryRun   bool             `bson:"dryRun"`
	Counters map[string]int64 `bson:"counters"`
}

// PublishDaemonReport records what a daemon has done so far.
func PublishDaemonReport(ctx context.Context, cold *mongo.Client, id string, dryRun bool, counters map[string]int64) error {
	_, err := cold.Database(DB).Collection(DaemonStats).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "at", Value: time.Now().UTC()},
			{Key: "dryRun", Value: dryRun},
			{Key: "counters", Value: counters},
		}}},
		options.UpdateOne().SetUpsert(true))
	return err
}

// ReadDaemonReports returns what every daemon last published.
func ReadDaemonReports(ctx context.Context, cold *mongo.Client) ([]DaemonReport, error) {
	cur, err := cold.Database(DB).Collection(DaemonStats).Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	var out []DaemonReport
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// PauseRecord is an operator's hold on eviction.
type PauseRecord struct {
	ID     string    `bson:"_id"`
	Since  time.Time `bson:"since"`
	Reason string    `bson:"reason"`
}

// PauseEviction stops the evictor until it is resumed.
func PauseEviction(ctx context.Context, cold *mongo.Client, reason string) error {
	_, err := cold.Database(DB).Collection(Markers).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: EvictionPausedMarker}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "reason", Value: reason}}},
			{Key: "$setOnInsert", Value: bson.D{{Key: "since", Value: time.Now().UTC()}}},
		},
		options.UpdateOne().SetUpsert(true))
	return err
}

// ResumeEviction lifts the hold.
func ResumeEviction(ctx context.Context, cold *mongo.Client) error {
	_, err := cold.Database(DB).Collection(Markers).
		DeleteOne(ctx, bson.D{{Key: "_id", Value: EvictionPausedMarker}})
	return err
}

// EvictionPaused reports whether eviction is held, and why.
func EvictionPaused(ctx context.Context, cold *mongo.Client) (PauseRecord, bool, error) {
	var rec PauseRecord
	err := cold.Database(DB).Collection(Markers).
		FindOne(ctx, bson.D{{Key: "_id", Value: EvictionPausedMarker}}).Decode(&rec)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return PauseRecord{}, false, nil
	}
	if err != nil {
		return PauseRecord{}, false, err
	}
	return rec, true, nil
}

type ActiveMarker struct {
	ID   string    `bson:"_id"`
	Seen time.Time `bson:"seen"`
}

// CacheSuggestion is one row of what the router learned about a query: how
// often it is asked, how long its answer stays the same, and what TTL that
// justifies.
//
// Published rather than only logged, because the question it answers ("what
// would caching be worth here") is one an operator asks days after the traffic
// happened.
type CacheSuggestion struct {
	Shape      string        `bson:"shape"`
	NS         string        `bson:"ns"`
	Executions uint64        `bson:"executions"`
	Repeats    uint64        `bson:"repeats"`
	Params     int           `bson:"params"`
	Changes    uint64        `bson:"changes"`
	StableMS   int64         `bson:"stableMs"`
	SuggestMS  int64         `bson:"suggestMs"`
	Stable     time.Duration `bson:"-"`
	Suggested  time.Duration `bson:"-"`
}

// CacheAdviceReport is one router's whole answer, replaced each time it
// publishes.
type CacheAdviceReport struct {
	ID          string            `bson:"_id"`
	At          time.Time         `bson:"at"`
	Shapes      int               `bson:"shapes"`
	Hits        uint64            `bson:"hits"`
	Misses      uint64            `bson:"misses"`
	Enabled     bool              `bson:"enabled"`
	Suggestions []CacheSuggestion `bson:"suggestions"`
}

// PublishCacheAdvice records what one router has learned.
func PublishCacheAdvice(ctx context.Context, cold *mongo.Client, rep CacheAdviceReport) error {
	rep.At = time.Now().UTC()
	for i := range rep.Suggestions {
		rep.Suggestions[i].StableMS = rep.Suggestions[i].Stable.Milliseconds()
		rep.Suggestions[i].SuggestMS = rep.Suggestions[i].Suggested.Milliseconds()
	}
	_, err := cold.Database(DB).Collection(CacheAdvice).ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: rep.ID}}, rep, options.Replace().SetUpsert(true))
	return err
}

// ReadCacheAdvice returns what every router has published.
func ReadCacheAdvice(ctx context.Context, cold *mongo.Client) ([]CacheAdviceReport, error) {
	cur, err := cold.Database(DB).Collection(CacheAdvice).Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()
	var out []CacheAdviceReport
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	for i := range out {
		for j := range out[i].Suggestions {
			out[i].Suggestions[j].Stable = time.Duration(out[i].Suggestions[j].StableMS) * time.Millisecond
			out[i].Suggestions[j].Suggested = time.Duration(out[i].Suggestions[j].SuggestMS) * time.Millisecond
		}
	}
	return out, nil
}
