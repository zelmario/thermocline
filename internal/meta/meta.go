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

// bookkeepingCollections is every collection Thermocline creates in a
// bookkeeping database, including the unique key registry, which lives in the
// database of the same name on the fast tier.
var bookkeepingCollections = map[string]bool{
	Residency: true, Evicting: true, Checkpoints: true, PositionsColl: true,
	Warming: true, Markers: true, DaemonStats: true, CacheAdvice: true,
	Watermarks: true, "keys": true,
}

// IsBookkeeping reports whether a database holding exactly these collections is
// some Thermocline deployment's bookkeeping rather than application data.
//
// The database this deployment uses is known from its own config. Another one's
// is not, and one pair of tiers carrying two deployments is the situation DB is
// a variable for, so it has to be recognised rather than named. Reporting it as
// application data tells an operator to tier another deployment's checkpoints,
// which is advice that would do harm if followed.
//
// It is recognised by content and not by name, because the name is whatever
// somebody's meta.db said. An empty database is not bookkeeping: there is
// nothing to go on, and nothing to warn about either.
func IsBookkeeping(collections []string) bool {
	if len(collections) == 0 {
		return false
	}
	for _, c := range collections {
		if !bookkeepingCollections[c] {
			return false
		}
	}
	return true
}

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

// NoticeTTL is how long an eviction or warming announcement is kept.
//
// It is the other half of evictor.max_feeder_lag and the two have to be read
// together. The evictor announces a removal, then makes it. The feeder reads
// the delete `lag` later and asks whether it was announced: if the notice has
// expired by then the delete reads as the application's, the archive tombstones
// its copy, and the document is gone from both tiers with nothing said.
//
// So a deployment that lets mirroring fall further behind than this is one
// where eviction destroys data. The config refuses that combination rather than
// leaving it to be discovered; see EvictorConfig.Validate.
const NoticeTTL = time.Hour

// EvictionNotice is one row of the evicting announcement collection. Notices
// expire through a TTL index; they only have to outlive the feeder's lag.
type EvictionNotice struct {
	ID string    `bson:"_id"`
	At time.Time `bson:"at"`
	// Pending is how many announced evictions of this document the feeder has
	// not seen the delete for yet. A count and not a flag, because a document
	// can be evicted, warmed back up and evicted again while the feeder is
	// still behind, and one flag cannot answer for two deletes.
	Pending int `bson:"pending"`
}

// AnnounceEviction records that these documents are about to leave the fast
// tier, so the feeder can tell those deletes apart from an application's.
//
// The count goes up rather than the notice being replaced, and the feeder takes
// one down per delete it applies. Nothing else may remove one.
//
// A copy-up used to delete the notice outright, on the reasoning that a
// document back in the fast tier is not leaving it. That is true of the next
// eviction and false of the one already in flight. If the feeder has not
// applied the delete event yet, the notice it needs is gone by the time it
// looks, so it reads a real eviction as an application delete and tombstones a
// record nothing deleted. The warm's own insert does not put that right,
// because the feeder suppresses it as an echo of the copy-up, so the archive is
// left holding a tombstone over a live document the fast tier has already
// dropped.
//
// Measured on the pilot with a warm and an evictor fighting over the same
// collections while a client read them: 66 batches of "deletes not announced by
// the evictor", and an archive left holding 200 tombstoned trips and 45
// tombstoned sales that nothing had ever deleted.
func AnnounceEviction(ctx context.Context, cold *mongo.Client, docIDs []string) error {
	if len(docIDs) == 0 {
		return nil
	}
	now := time.Now().UTC()
	models := make([]mongo.WriteModel, len(docIDs))
	for i, id := range docIDs {
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: id}}).
			SetUpdate(bson.D{
				{Key: "$inc", Value: bson.D{{Key: "pending", Value: 1}}},
				{Key: "$set", Value: bson.D{{Key: "at", Value: now}}},
			}).
			SetUpsert(true)
	}
	if _, err := cold.Database(DB).Collection(Evicting).
		BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("announce eviction: %w", err)
	}
	return nil
}

// ConsumeEvictionNotices takes one announcement down for each document whose
// eviction the feeder has just applied, and removes the notice when none is
// left. It reports which of docIDs had an announcement to take.
func ConsumeEvictionNotices(ctx context.Context, cold *mongo.Client, docIDs []string) (map[string]int, error) {
	if len(docIDs) == 0 {
		return nil, nil
	}
	coll := cold.Database(DB).Collection(Evicting)
	cur, err := coll.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}, {Key: "pending", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("read eviction notices: %w", err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	have := map[string]int{}
	for cur.Next(ctx) {
		var rec EvictionNotice
		if derr := cur.Decode(&rec); derr != nil {
			return nil, fmt.Errorf("decode eviction notice: %w", derr)
		}
		// A notice written before this field existed counts as one.
		if rec.Pending <= 0 {
			rec.Pending = 1
		}
		have[rec.ID] = rec.Pending
	}
	if cerr := cur.Err(); cerr != nil {
		return nil, fmt.Errorf("read eviction notices: %w", cerr)
	}
	return have, nil
}

// SpendEvictionNotices writes down the announcements a batch actually used.
//
// Separate from reading them so the caller can decide, in event order, which
// deletes each notice answers for, and so nothing is spent if the batch cannot
// be applied.
func SpendEvictionNotices(ctx context.Context, cold *mongo.Client, spent map[string]int) error {
	if len(spent) == 0 {
		return nil
	}
	coll := cold.Database(DB).Collection(Evicting)
	models := make([]mongo.WriteModel, 0, len(spent))
	ids := make([]string, 0, len(spent))
	for id, n := range spent {
		ids = append(ids, id)
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: id}}).
			SetUpdate(bson.D{{Key: "$inc", Value: bson.D{{Key: "pending", Value: -n}}}}))
	}
	if _, err := coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("spend eviction notices: %w", err)
	}
	if _, err := coll.DeleteMany(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
		{Key: "pending", Value: bson.D{{Key: "$lte", Value: 0}}},
	}); err != nil {
		return fmt.Errorf("spend eviction notices: %w", err)
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

	// Gauges are values that go up and down, and may be negative, so they
	// cannot travel in Counters: Prometheus reads a counter that went backwards
	// as a restart. The evictor's clock skew against the archive is the reason
	// this exists, and it was the one signal on the alerting list that could not
	// be alerted on, because it was only ever written to a log.
	Gauges map[string]float64 `bson:"gauges,omitempty"`
}

// PublishDaemonReport records what a daemon has done so far.
func PublishDaemonReport(ctx context.Context, cold *mongo.Client, id string, dryRun bool,
	counters map[string]int64, gauges map[string]float64) error {
	_, err := cold.Database(DB).Collection(DaemonStats).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "at", Value: time.Now().UTC()},
			{Key: "dryRun", Value: dryRun},
			{Key: "counters", Value: counters},
			{Key: "gauges", Value: gauges},
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

// IDsAreNotObjectIDs reports whether the archive holds a live document in this
// namespace whose _id is not an ObjectId.
//
// A warm or eviction rule with no date_field reads a document's age from the
// timestamp inside its _id, which only exists in an ObjectId. Against any other
// _id type the rule is not merely approximate, it addresses nothing:
// MongoDB brackets range predicates by BSON type, so `_id < ObjectId(cutoff)`
// and `_id >= ObjectId(floor)` match no integer, string or UUID _id at all.
// Sorting uses the canonical type order; queries do not.
//
// The consequences are silent in both directions and neither looks like a
// misconfiguration. The evictor scans nothing and tiers nothing, for ever. The
// warmer copies nothing, concludes the window is done and records a
// completeness claim over a fast tier it never filled. Measured on an adopted
// collection of ten integer-keyed documents: the warmer reported "warm
// complete, scanned 0" and claimed the collection complete for all of history,
// and the router then answered countDocuments({}) with 0 where the original
// cluster answers 10. The collection had disappeared, and every part of the
// system reported success.
//
// One document is enough to answer, and the query is by _id type against an
// index that always exists.
func IDsAreNotObjectIDs(ctx context.Context, cold *mongo.Client, db, coll string) (bool, error) {
	n, err := cold.Database(db).Collection(coll).CountDocuments(ctx,
		bson.D{
			NotArchivedDeleted(),
			{Key: "_id", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$type", Value: "objectId"}}}}},
		},
		options.Count().SetLimit(1))
	if err != nil {
		return false, fmt.Errorf("check the _id type of %s.%s: %w", db, coll, err)
	}
	return n > 0, nil
}
