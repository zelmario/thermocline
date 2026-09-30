// Package meta holds the names and record shapes Thermocline stores alongside
// user data. Bookkeeping lives in its own database on the cold tier, so user
// documents are never modified to carry Thermocline's state.
package meta

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
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
	Watermarks: true, OwnDDL: true, Unmirrored: true, "keys": true,
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
// MongoDB's own collections are not counted either way. They belong to the
// server rather than to anybody's deployment, and one of them appearing beside
// the bookkeeping turned the whole database into application data: profiling
// writes system.profile, a view writes system.views, and every collection in
// the database was then reported as an application's and an operator told to
// tier another deployment's checkpoints. The caller's own loop skips system
// collections four lines further down, for the same reason, and this is the one
// place the list was not applied.
//
// A database holding nothing but system collections falls through to the empty
// case, which says nothing either way, and that is right: there is no
// bookkeeping in it to recognise.
func IsBookkeeping(collections []string) bool {
	seen := 0
	for _, c := range collections {
		if strings.HasPrefix(c, "system.") {
			continue
		}
		if !bookkeepingCollections[c] {
			return false
		}
		seen++
	}
	return seen > 0
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

// IsScratch reports whether a collection name is one the router made for
// itself. See ScratchPrefix.
func IsScratch(coll string) bool { return strings.HasPrefix(coll, ScratchPrefix) }

const (
	// DefaultDB is the bookkeeping database a deployment gets when the config
	// does not name one. It is what every existing deployment already uses, so
	// the name only has to be set when a second deployment shares these tiers.
	DefaultDB = "_thermocline"

	// ScratchPrefix names the collections the router builds for itself inside
	// the application's own database, and it exists so everything else can tell
	// them apart from the application's data.
	//
	// A tiered $out is assembled in one of these and renamed over its target,
	// because the last step of an $out cannot be a rename when the target is
	// tiered. That collection lives in the application's database, which is
	// where it has to live for the rename to be possible, so three things have
	// to know it is not theirs: the feeder must not mirror it, the catalog must
	// not show it to a client, and a leftover has to be recognisable as litter.
	//
	// Measured by reading the feeder's own pipeline: with feeder.namespaces
	// empty, which is the supported way to say "mirror everything", the match
	// covered this collection. Its inserts were mirrored into the archive and
	// the rename that consumes it never is, so the archive kept a full copy of
	// every tiered $out for ever, and the router's own startup report then named
	// it as a collection the archive holds and the fast tier does not.
	ScratchPrefix = "tmp.thermocline."

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

	// OwnDDL records the collection creations and index builds this system
	// performs on the fast tier for itself, so a client's change stream can
	// tell them from the application's own.
	//
	// The fast tier is repaired rather than merely filled: before the first
	// copy-up into a namespace the collection is created there with the
	// archive's options, and the archive's indexes are built on it. Both happen
	// on the application's own namespace, so neither the scratch prefix nor the
	// bookkeeping database hides them, and with showExpandedEvents a client is
	// told its collection was just created and its indexes with it.
	//
	// One row per namespace, holding the oplog positions of that DDL. A
	// namespace and not a document id, because DDL has no document; positions
	// and not a window, because the application builds indexes on these same
	// collections and hiding its own DDL would be worse than showing an extra
	// create. See OwnDDLRecord.
	OwnDDL = "ownddl"

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

	// Unmirrored names the documents the archive refused and the feeder gave up
	// on, one row each.
	//
	// It exists because the archive being a superset of the fast tier is what
	// makes a fall-through read correct, and this is the one state in which it
	// is not. The feeder stops retrying a document the archive's unique index
	// refuses, because no retry of it can ever succeed, and says so at ERROR.
	// The document then stays on the fast tier, which is the half that works.
	//
	// What nothing recorded was which document. A read the fast tier cannot
	// claim is answered from the archive, and there is the one place this
	// document is not, so the answer was short by it and said ok. Measured on
	// three documents written at the fast tier with the archive holding a
	// unique index the third breaks: a count answered 2, a find returned two of
	// three, and findOne by _id returned the third, because a point read asks
	// both tiers. The same collection answering differently depending on how it
	// is asked.
	//
	// A log line cannot be read by the router, and a counter cannot name a
	// namespace. This can, and it lives in the archive beside every other piece
	// of bookkeeping so a restarted router still knows.
	Unmirrored = "unmirrored"
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

// UnmirroredRecord is one document the archive refused, which the feeder has
// stopped trying to mirror.
//
// Keyed by DocID so one collection covers every namespace, and carrying NS on
// its own so the router can ask "has this namespace any" without decoding.
type UnmirroredRecord struct {
	ID    string        `bson:"_id"`
	NS    string        `bson:"ns"`
	Doc   bson.RawValue `bson:"doc"`
	Why   string        `bson:"why"`
	First time.Time     `bson:"first"`
	Last  time.Time     `bson:"last"`
}

// RecordUnmirrored notes a document the archive will not take.
//
// Upserted rather than inserted, and First is left alone on a repeat: the same
// document is refused again every time the application writes it, and the
// question an operator asks is how long this has been true.
func RecordUnmirrored(ctx context.Context, cold *mongo.Client, ns string, id bson.RawValue, why string, now time.Time) error {
	_, err := cold.Database(DB).Collection(Unmirrored).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: DocID(ns, id)}},
		bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "ns", Value: ns},
				{Key: "doc", Value: id},
				{Key: "why", Value: why},
				{Key: "last", Value: now},
			}},
			{Key: "$setOnInsert", Value: bson.D{{Key: "first", Value: now}}},
		},
		options.UpdateOne().SetUpsert(true))
	return err
}

// ClearUnmirrored removes the row for one document, for when the archive has
// taken it after all.
//
// Whoever resolves the duplicate does not run a command here afterwards, so
// nothing would ever clear these if only an operator could. The feeder clears
// on the write that finally succeeds, which is the event that makes the row
// untrue.
func ClearUnmirrored(ctx context.Context, cold *mongo.Client, ns string, id bson.RawValue) error {
	_, err := cold.Database(DB).Collection(Unmirrored).DeleteOne(ctx,
		bson.D{{Key: "_id", Value: DocID(ns, id)}})
	return err
}

// ClearUnmirroredMany forgets the rows for a batch of documents and reports how
// many rows the namespace has left.
//
// One round trip for the batch rather than one per document, and the count is
// what decides whether the namespace is whole again. Counting rather than
// assuming matters: the batch that clears a row is usually the retry that
// followed a refusal, and that retry has had the refused document taken out of
// it, so "this batch succeeded" says nothing about the document the row is
// actually about.
func ClearUnmirroredMany(ctx context.Context, cold *mongo.Client, ns string, ids []bson.RawValue) (int64, error) {
	coll := cold.Database(DB).Collection(Unmirrored)
	keys := make(bson.A, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, DocID(ns, id))
	}
	if len(keys) > 0 {
		if _, err := coll.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: keys}}}}); err != nil {
			return 0, err
		}
	}
	return coll.CountDocuments(ctx, bson.D{{Key: "ns", Value: ns}})
}

// UnmirroredNamespaces names every namespace holding at least one such
// document, which is what a reader needs to know before it trusts the archive
// to be a superset.
func UnmirroredNamespaces(ctx context.Context, cold *mongo.Client) (map[string]int, error) {
	cur, err := cold.Database(DB).Collection(Unmirrored).Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()
	out := map[string]int{}
	for cur.Next(ctx) {
		var rec UnmirroredRecord
		if err := cur.Decode(&rec); err != nil {
			return nil, err
		}
		out[rec.NS]++
	}
	return out, cur.Err()
}

// AllUnmirrored lists every such row, for the command that reports what is wrong
// with a deployment.
func AllUnmirrored(ctx context.Context, cold *mongo.Client) ([]UnmirroredRecord, error) {
	cur, err := cold.Database(DB).Collection(Unmirrored).Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()
	var out []UnmirroredRecord
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// UnmirroredIn lists the rows for one namespace, so a refusal can name a
// document rather than a count.
func UnmirroredIn(ctx context.Context, cold *mongo.Client, ns string) ([]UnmirroredRecord, error) {
	cur, err := cold.Database(DB).Collection(Unmirrored).Find(ctx, bson.D{{Key: "ns", Value: ns}})
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()
	var out []UnmirroredRecord
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
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
	// EndedAt is when the document this record is about left the fast tier,
	// whether the application deleted it or the evictor removed it. The hold is
	// over from then, and so is the promotion: an insert of this _id at or
	// before it is the copy-up's own, and one after it is the application
	// writing the _id again. See the feeder's dropResidency.
	EndedAt time.Time `bson:"endedAt,omitempty"`
	// ExpiresAt is when this row may be removed, and it is deliberately not
	// HotUntil.
	//
	// The two used to be one field, and a row was destroyed as soon as its hold
	// was over. That is the third thing this row is for: the hold, the
	// promotion, and the evidence that the copy-up's insert was a promotion at
	// all. A change stream consumer is judged when it reads, and it reads when
	// it likes, so the evidence has to outlive the hold by more than the
	// consumer's lag.
	//
	// Measured, with the hold and the expiry sharing a field: three documents
	// copied up and then deleted by the application, the rows ended and gone
	// within one TTL pass, and the stream read eighty seconds later delivered
	// all three copy-ups as inserts -- byte for byte what the fast tier's own
	// unfiltered stream delivered. Everything else about the suppression was
	// correct; there was simply nothing left to consult.
	//
	// NoticeTTL past the hold, which is what an eviction announcement gets and
	// for the same reason. The bound is stated rather than hidden: a consumer
	// further behind than that reads a copy-up as an insert, exactly as one
	// further behind than NoticeTTL reads an eviction as a delete.
	ExpiresAt time.Time `bson:"expiresAt,omitempty"`
	// Echoed is the oplog position of each copy-up insert this record's
	// promotions have caused, newest last and capped.
	//
	// promotedAt cannot answer for more than one promotion, and a record
	// describes as many as the document has had. A document copied up, evicted
	// and copied up again leaves one record whose promotedAt is the *later*
	// promotion's, so the earlier copy-up's insert is older than the record
	// that should account for it and reads as somebody else's write. Measured
	// with the sieve instrumented: haveRecord=true, promotedAt 06:57:18.386,
	// eventAt 06:57:16.169, and four copy-ups delivered to an application as
	// inserts it never made.
	//
	// A cluster time and not a wall clock, for the reason EvictionNotice.Spent
	// gives: the feeder and the router read the same field off the same event,
	// so the values compare by construction and no two entries share one.
	Echoed []bson.Timestamp `bson:"echoed,omitempty"`
}

// ResidencyUpdate is the write a copy-up makes to the residency record, as an
// aggregation pipeline so that one write can both refresh a live hold and
// reopen an ended one.
//
// $setOnInsert used to carry promotedAt and echoPending, on the reasoning that
// a record exists exactly while the document is on the fast tier, so creating
// one *is* the promotion. That reasoning was true when the evictor deleted the
// record; it stopped being true when the evictor started ending it instead, so
// that a change stream consumer reading late can still recognise the copy-up's
// insert. An ended record is now the ordinary state of a document that has been
// evicted, and a copy-up that found one left promotedAt describing the previous
// promotion and echoPending unset, so the new promotion announced no echo and
// was never suppressed at all.
//
// Reopening is the same decision $setOnInsert was making, stated properly: a
// record with no promotedAt has never described one, and a record that has
// ended describes one that is over. Either way the promotion beginning now is
// this record's, and neither its history nor anything the previous promotion
// wrote down is disturbed -- echoed in particular, which is the evidence the
// whole change belongs to.
//
// A hold on a document the fast tier already holds is refreshed and nothing
// more. That is the defect the $setOnInsert was written for: a multi-document
// write records residency for every document it matches, most of them already
// there, and moving promotedAt onto those hid five of an application's own
// inserts from its own change stream. Announcing an echo for them would be
// worse still, and a copy-up that turns out to insert nothing takes its echo
// back; see withdrawEcho.
//
// The warmer writes the same row through here. A warm and a copy-up are the
// same thing to a change stream reader -- this system putting a document on the
// fast tier that the archive already held -- and the reader has one place to
// look. The warmer used to write a hold and nothing else, so isPromotion had
// nothing to recognise and every warmed document reached the application as an
// insert of a record that could be years old.
//
// A pipeline update, which needs MongoDB 4.2, the floor this project already
// supports.
func ResidencyUpdate(ns string, id bson.RawValue, now time.Time, hold time.Duration) mongo.Pipeline {
	// Missing promotedAt: never promoted, so this is the first. Present
	// endedAt: the last promotion is over, so this is a new one.
	//
	// And a hold that has lapsed, which is the one that was missing. endedAt is
	// written by whoever took the document off the fast tier, and the evictor
	// writes it *after* removing the document, in a second update that only
	// warns if it fails. A read landing in between copies the document back up
	// while the record still says the first promotion is current, so promotedAt
	// is not refreshed and echoPending is not re-announced. Every route in
	// isPromotion then declines once that timestamp is older than
	// promotionWindow, and the copy-up's insert reaches the application as an
	// order nobody placed.
	//
	// Measured: eight of eight second copy-ups delivered as inserts, and one in
	// a two-shop run of 142 copy-ups where the evictor was doing the removing
	// for real. A lapsed hold is exactly the state a document must be in for
	// the evictor to have taken it, so this asks the record what the evictor
	// would have asked rather than waiting to be told.
	//
	// Safe in the other direction: a read of a document the fast tier still
	// holds refreshes residency without causing an insert, and the echo this
	// re-announces is withdrawn by the same path that already withdraws one for
	// a document found to be present. See WithdrawEchoes.
	reopening := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$promotedAt"}}, "missing"}}},
		bson.D{{Key: "$ne", Value: bson.A{bson.D{{Key: "$type", Value: "$endedAt"}}, "missing"}}},
		bson.D{{Key: "$lte", Value: bson.A{"$hotUntil", "$$NOW"}}},
	}}}
	return mongo.Pipeline{
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "ns", Value: ns},
			// $literal, because a document's _id is the application's and a
			// string one beginning with $ would otherwise be read as a field
			// path rather than as the value it is.
			{Key: "doc", Value: bson.D{{Key: "$literal", Value: id}}},
			// Refreshed on every read, because that is what the hold is for: a
			// document read again should stay hot longer.
			//
			// Counted from the archive's own clock rather than from the
			// writer's. Both the reader of this field and the expiry below are
			// somewhere else: the evictor decides whether the hold has passed,
			// and the archive's TTL monitor decides when the row goes. A
			// promoter whose clock runs behind wrote a hold that was already
			// shorter than configured by the difference, and an eviction sweep
			// then removed a document that had just been fetched, which the
			// next read fetches again. $$NOW is the server's, so nobody's
			// clock comes into it and there is nothing to correct.
			{Key: "hotUntil", Value: after(hold)},
			// When the row goes, which is later than when the hold does, by
			// NoticeTTL. See ResidencyRecord.ExpiresAt: the gap is what a
			// change stream consumer reading late has to recognise a copy-up
			// by, and a row expiring early is that consumer being shown an
			// insert the application never made.
			{Key: "expiresAt", Value: after(hold + NoticeTTL)},
			// This one stays on the writer's clock, deliberately. It is weighed
			// against the wall-clock time of a change stream event, which comes
			// from the fast tier's primary, and the window it is weighed in is
			// generous enough that skew can only widen it. Moving it into the
			// archive's frame would put a third clock in a comparison that
			// already errs on the safe side. See isPromotion.
			{Key: "promotedAt", Value: bson.D{{Key: "$cond", Value: bson.A{reopening, now, "$promotedAt"}}}},
			// The insert this record precedes is about to appear on the
			// feeder's change stream, and mirroring it would write the
			// archive's own document back into the archive. This says one is
			// expected, and the feeder clears it as it suppresses, so only that
			// one insert is suppressed and a later genuine one is not. See
			// promotion echoes in the feeder.
			{Key: "echoPending", Value: bson.D{{Key: "$cond", Value: bson.A{reopening, true, "$echoPending"}}}},
			// The document is back, so the record is no longer ended. Left
			// standing it would answer "the application wrote this _id again"
			// for the copy-up's own insert.
			{Key: "endedAt", Value: bson.D{{Key: "$cond", Value: bson.A{reopening, "$$REMOVE", "$endedAt"}}}},
			// A new promotion starts with its own echo announced and nothing
			// withdrawn yet. See WithdrawEchoes.
			{Key: "echoUncertain", Value: bson.D{{Key: "$cond", Value: bson.A{reopening, "$$REMOVE", "$echoUncertain"}}}},
		}}},
	}
}

// WithdrawEchoes removes the promise that a copy-up is coming.
//
// Residency is recorded before the documents are moved, which is what lets a
// change stream consumer tell a copy-up from an insert the moment the event
// arrives, and it announces an echo at the same time. A document the fast tier
// turned out to already hold produces no insert and therefore no echo, so the
// announcement has to be taken back. Left standing it is spent on whatever
// write reaches that document next, and that mirror is dropped: the archive
// keeps the older version and nothing says so.
//
// Both promoters need this and for the same reason, so it lives here rather
// than in either of them. The warmer went without it for as long as it
// announced no echoes, and gained the need for it the moment it started.
//
// Only the echo. promotedAt stays, because the record may describe a genuine
// promotion by the other path, and taking its timestamp away lets every
// promotion through as an ordinary insert.
func WithdrawEchoes(ctx context.Context, cold *mongo.Client, docIDs []string) error {
	if len(docIDs) == 0 {
		return nil
	}
	_, err := cold.Database(DB).Collection(Residency).UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}}},
		bson.D{
			{Key: "$unset", Value: bson.D{{Key: "echoPending", Value: ""}}},
			// The withdrawal is itself a guess. The duplicate key that prompted it
			// says the fast tier held the document, not who put it there, and
			// when two promoters race it was the other one, whose insert is the
			// echo being withdrawn. The feeder then never writes that insert's
			// position down, and a change stream consumer reading late would take
			// the list of positions as complete and be shown the copy-up as an
			// insert nobody made. This tells the router the list may be short.
			// A new promotion clears it; see ResidencyUpdate.
			{Key: "$set", Value: bson.D{{Key: "echoUncertain", Value: true}}},
		})
	return err
}

// after is an instant that far past the archive's own clock, for a field
// something on the archive will judge. $$NOW is the server's reading, and $add
// with a date treats the other operand as milliseconds.
func after(d time.Duration) bson.D {
	return bson.D{{Key: "$add", Value: bson.A{"$$NOW", d.Milliseconds()}}}
}

// HoldOnly keeps a document on the fast tier for at least hold, and says
// nothing else: no promotion, no echo, no reopening.
//
// ResidencyUpdate is for a document about to be copied up, and for a document
// with no record it announces an echo, the promise that the copy-up's insert
// is coming. The router's hold against eviction is placed on a document the
// fast tier already has, which produces no insert, and an echo announced for it
// is spent on whatever write reaches that document next: that mirror is
// dropped and the archive keeps the older version. So this sets the hold and
// the row's expiry, only ever later than they were, and leaves promotedAt,
// echoPending and endedAt exactly as it found them.
func HoldOnly(ns string, id bson.RawValue, hold time.Duration) mongo.Pipeline {
	later := func(field string, d time.Duration) bson.D {
		return bson.D{{Key: "$max", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$" + field, "$$NOW"}}}, after(d)}}}
	}
	return mongo.Pipeline{
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "ns", Value: ns},
			{Key: "doc", Value: bson.D{{Key: "$literal", Value: id}}},
			{Key: "hotUntil", Value: later("hotUntil", hold)},
			{Key: "expiresAt", Value: later("expiresAt", hold+NoticeTTL)},
		}}},
	}
}

// HoldIsOver is the update that ends a hold: the document has left the fast
// tier, so nothing is holding it there any more.
//
// hotUntil goes to the archive's own clock for the same reason ResidencyUpdate
// sets it from there: the evictor is what reads it, and "in the past" has to
// mean in the past to the one asking. endedAt keeps the caller's clock, because
// what it is weighed against is the wall-clock time of a change stream event.
// They were one instant when both were simply "now", and they answer different
// questions to different readers.
func HoldIsOver(endedAt time.Time) mongo.Pipeline {
	return mongo.Pipeline{
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "endedAt", Value: endedAt},
			{Key: "hotUntil", Value: "$$NOW"},
		}}},
	}
}

// MaxEchoRecorded is how many promotion positions a residency record keeps. A
// document can be copied up, evicted and copied up again, so one is not enough;
// the record is cleared shortly after the document leaves, so a handful covers
// every promotion it can still be asked about.
const MaxEchoRecorded = 8

// EchoedAt reports whether one of this record's promotions caused the insert at
// this oplog position.
//
// A record with no Echoed list at all was written by a feeder from before the
// field existed, and there is no way to tell: the caller falls back to the
// window it used to use rather than stop suppressing during an upgrade. The
// same shape as EvictionNotice.SpentOn, and for the same reason.
func (r ResidencyRecord) EchoedAt(at bson.Timestamp) (known, yes bool) {
	if len(r.Echoed) == 0 {
		return false, false
	}
	for _, t := range r.Echoed {
		if t.T == at.T && t.I == at.I {
			return true, true
		}
	}
	return true, false
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
	//
	// A pointer, so a notice written before this field existed can be told from
	// one the feeder has spent. Both read as zero otherwise, and they mean
	// opposite things: the first is one unclaimed eviction, the second is none
	// left. That distinction is what lets a spent notice stay until its TTL,
	// which is what a change stream read late needs. See SpendEvictionNotices.
	Pending *int `bson:"pending"`
	// Spent is the oplog position of each delete this notice has been spent on,
	// newest last and capped.
	//
	// Without it a spent notice went on answering for every delete of the same
	// document for the next thirty seconds, including the application's own.
	// Measured: the evictor removed a document, the feeder applied it, and the
	// application deleted the same document twenty-two seconds later; the
	// change stream dropped that delete, so a client watching the collection
	// was never told a record it had deleted was gone. Nine of them in one
	// fuzzer round.
	//
	// A cluster time and not a wall clock, because it has to be the same value
	// on both sides: the feeder and the router read the same oplog entry from
	// the same change stream, so its position identifies it exactly, and no two
	// entries share one. wallTime would have done as well from 6.0 and this
	// supports 4.2.
	Spent []bson.Timestamp `bson:"spent,omitempty"`
}

// SpentOn reports whether this notice has already been spent on the delete at
// this oplog position.
//
// A notice with no Spent list at all was written by a feeder from before the
// field existed, and there is no way to tell: the caller falls back to the
// window it used to use rather than stop suppressing during an upgrade.
func (n EvictionNotice) SpentOn(at bson.Timestamp) (known, yes bool) {
	if len(n.Spent) == 0 {
		return false, false
	}
	for _, t := range n.Spent {
		if t.T == at.T && t.I == at.I {
			return true, true
		}
	}
	return true, false
}

// MaxSpentRecorded is how many spent positions a notice keeps. A document can
// be evicted, warmed and evicted again, so one is not enough; a notice lives an
// hour, so a handful is plenty.
const MaxSpentRecorded = 8

// Left reports how many announced evictions of this document are still
// unclaimed. A notice from before the field existed counts as one.
func (n EvictionNotice) Left() int {
	if n.Pending == nil {
		return 1
	}
	return *n.Pending
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
	models := make([]mongo.WriteModel, len(docIDs))
	for i, id := range docIDs {
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: id}}).
			SetUpdate(EvictionAnnouncement()).
			SetUpsert(true)
	}
	if _, err := cold.Database(DB).Collection(Evicting).
		BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("announce eviction: %w", err)
	}
	return nil
}

// WithdrawEviction takes back one announcement per document, for a removal that
// was announced and then found nothing to remove.
//
// Only by the process that announced it and only once it knows its own delete
// matched nothing, so no delete event is coming for the announcement to answer.
// Anything less exact is the trade AnnounceEviction describes, and there the
// announcement is left standing.
//
// Floored at zero for the reason SpendEvictionNotices gives.
func WithdrawEviction(ctx context.Context, cold *mongo.Client, docIDs []string) error {
	if len(docIDs) == 0 {
		return nil
	}
	_, err := cold.Database(DB).Collection(Evicting).UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}}},
		mongo.Pipeline{{{Key: "$set", Value: bson.D{
			{Key: "pending", Value: bson.D{{Key: "$max", Value: bson.A{
				0,
				bson.D{{Key: "$add", Value: bson.A{
					bson.D{{Key: "$ifNull", Value: bson.A{"$pending", 1}}}, -1,
				}}},
			}}}},
		}}}})
	if err != nil {
		return fmt.Errorf("withdraw eviction announcement: %w", err)
	}
	// A row this left with nothing pending and nothing spent answers for no
	// delete, and it is not harmless: the router's sieve reads a notice with no
	// spent positions by the old window, so any delete of the document in the
	// next thirty seconds was hidden as an eviction. Removed in the same
	// breath, and only in that state, so a row another announcement or a spent
	// eviction still stands on is left alone.
	if _, err := cold.Database(DB).Collection(Evicting).DeleteMany(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: docIDs}}},
		{Key: "pending", Value: bson.D{{Key: "$lte", Value: 0}}},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "spent", Value: bson.D{{Key: "$exists", Value: false}}}},
			bson.D{{Key: "spent", Value: bson.D{{Key: "$size", Value: 0}}}},
		}},
	}); err != nil {
		return fmt.Errorf("remove an empty eviction notice: %w", err)
	}
	return nil
}

// EvictionAnnouncement is the write that announces one eviction: another one
// pending, and the row's life restarted.
//
// The instant comes from the archive, not from the evictor. Nothing reads the
// field to date the announcement; it is there for the TTL index, and the TTL
// monitor that acts on it is the archive's. An evictor whose clock ran behind
// wrote notices that were already part expired, and far enough behind they were
// born expired and gone within the minute. A notice that is gone is an eviction
// the feeder reads as the application's own delete: the archive tombstones its
// copy and the document is lost from both tiers, with nothing said anywhere.
// $currentDate takes the reading where it is judged, so there is nothing left
// to correct for and nothing to measure.
func EvictionAnnouncement() mongo.Pipeline {
	return mongo.Pipeline{
		{{Key: "$set", Value: bson.D{
			// At least one, not simply one more.
			//
			// Flooring the spend at zero stops a counter going negative from
			// here on, and does nothing for a deployment whose counters are
			// already negative: a row left at -1 by two feeders would be
			// announced up to 0, and a notice counts only above zero, so the
			// next real eviction of that document would still be read as the
			// application deleting it. An announcement means at least one
			// eviction is pending, so saying that heals the row at the moment
			// it matters, and says the same thing as +1 for every row that was
			// never damaged.
			{Key: "pending", Value: bson.D{{Key: "$max", Value: bson.A{
				1,
				bson.D{{Key: "$add", Value: bson.A{
					bson.D{{Key: "$ifNull", Value: bson.A{"$pending", 0}}}, 1,
				}}},
			}}}},
			{Key: "at", Value: "$$NOW"},
		}}},
	}
}

// ReadEvictionNotices reports how many announcements each of docIDs has left to
// claim. It writes nothing: the feeder decides in event order which deletes
// they answer for and spends them with SpendEvictionNotices once the checkpoint
// covers those deletes, and a process taking back its own announcement uses
// WithdrawEviction.
//
// It was called ConsumeEvictionNotices and its comment said it took one down,
// which it had stopped doing when spending moved to the checkpoint. The evictor
// believed the name: it called this to take back the announcements for
// documents it then left on the fast tier, and each stood for up to an hour,
// ready to make the application's next delete of that document read as an
// eviction.
func ReadEvictionNotices(ctx context.Context, cold *mongo.Client, docIDs []string) (map[string]int, error) {
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
		// A spent notice is not an announcement. It stays in the collection
		// until its TTL now, for the change stream sieve to read, and the
		// feeder must not claim it: doing so would treat an application's own
		// delete as an eviction and leave the archive holding a live copy of a
		// document that was removed.
		if left := rec.Left(); left > 0 {
			have[rec.ID] = left
		}
	}
	if cerr := cur.Err(); cerr != nil {
		return nil, fmt.Errorf("read eviction notices: %w", cerr)
	}
	return have, nil
}

// SpendEvictionNotices writes down the announcements a batch actually used, and
// which delete each one answered for.
//
// Separate from reading them so the caller can decide, in event order, which
// deletes each notice answers for, and so nothing is spent if the batch cannot
// be applied.
//
// The positions are what let the router's sieve be exact rather than
// approximate. A spent notice stays until its TTL so a change stream read late
// can still recognise the eviction, and until these were recorded the only way
// to use it was "any delete of this document in the next thirty seconds", which
// swallowed the application's own. See EvictionNotice.Spent.
func SpendEvictionNotices(ctx context.Context, cold *mongo.Client, spent map[string][]bson.Timestamp) error {
	if len(spent) == 0 {
		return nil
	}
	coll := cold.Database(DB).Collection(Evicting)
	models := make([]mongo.WriteModel, 0, len(spent))
	for id, at := range spent {
		n := len(at)
		if n == 0 {
			continue
		}
		// Floored at zero, and that is not tidiness: a counter that goes
		// negative destroys documents later, with the thing that made it
		// negative long since put right.
		//
		// $inc took it below zero whenever one eviction was spent twice, and
		// two feeders watching one namespace do exactly that. The lease stops
		// two feeders with the same id and nothing stops two with different
		// ids, which is one edited config away. Nothing is lost at that moment,
		// because the repeat carries the same oplog timestamp and is recognised
		// as the same delete. The damage is what it leaves behind: pending at
		// -1, so the *next* eviction of that document announces it to 0, a
		// notice counts only above zero, and the feeder reads a real eviction
		// as the application deleting the document and tombstones the archive's
		// copy. Gone from both tiers with nothing said, which is the outcome
		// the announcement channel exists to prevent.
		//
		// Measured on 40 documents: two feeders, one eviction, no loss and
		// pending at -1; then one feeder, restore, evict again, and all 40 were
		// tombstoned. The same sequence with one feeder throughout left pending
		// at 0 and lost none.
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: id}}).
			SetUpdate(mongo.Pipeline{
				{{Key: "$set", Value: bson.D{
					{Key: "pending", Value: bson.D{{Key: "$max", Value: bson.A{
						0,
						bson.D{{Key: "$subtract", Value: bson.A{
							bson.D{{Key: "$ifNull", Value: bson.A{"$pending", 0}}},
							n,
						}}},
					}}}},
					{Key: "spent", Value: bson.D{{Key: "$slice", Value: bson.A{
						bson.D{{Key: "$concatArrays", Value: bson.A{
							bson.D{{Key: "$ifNull", Value: bson.A{"$spent", bson.A{}}}},
							at,
						}}},
						-MaxSpentRecorded,
					}}}},
				}}},
			}))
	}
	if _, err := coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("spend eviction notices: %w", err)
	}
	// Spent, not removed. It used to be deleted here, which left the router
	// with nothing to recognise the eviction by, and the router is asked when
	// it hands the delete event to a client rather than when the delete
	// happened. A client that is behind, which is what any client resuming from
	// a stored token is, then saw a delete for a document the archive still
	// holds. The notice now lives out its TTL and the feeder ignores it once
	// spent; see ReadEvictionNotices and the sieve in the router.
	return nil
}

// OwnDDLSpan brackets one piece of DDL this system did on the fast tier: the
// oplog position before it started and the position once it had finished.
//
// A bracket rather than the position itself, because the position itself is not
// obtainable. The obvious source is the command reply's operationTime, and it
// is the wrong value: it is the node's latest logical time when it answered,
// not the oplog position of the write it just did. Measured on PSMDB 8.0, a
// single copy-up into an empty namespace: the create arrived at clusterTime
// (T,1), the createIndexes at (T,7), and operationTime came back (T,8), which
// matches neither event. A fix built on it suppressed nothing at all while
// passing its own unit tests.
//
// From is read before the command and To after it, both from the fast tier's
// own lastWrite position, so the entry the command writes falls strictly
// inside. That is exact enough to be safe in the only direction that matters:
// the application's own DDL on this namespace is outside every span unless it
// happens while this system is building on the same collection.
type OwnDDLSpan struct {
	From bson.Timestamp `bson:"from"`
	// To is unset while the DDL is still running. See OwnDDLRecord.Covers for
	// how far an unfinished span reaches.
	To bson.Timestamp `bson:"to,omitempty"`
	// Tok identifies this bracket, so the close finds the one it opened.
	//
	// Matching on From instead was wrong, and not only untidy. Two brackets can
	// be open on one namespace at once -- EnsureCollection runs inline and the
	// index build that follows it runs in a goroutine -- and on a fast tier
	// nothing else is writing to, both read the same lastWrite position. One
	// close then set `to` on both, so whichever closed second decided both, and
	// if that was the earlier of the two the later event stopped being covered
	// and reached the client after all: the defect back again, on an idle
	// cluster only, which is the hardest kind to see.
	Tok string `bson:"tok,omitempty"`
}

// OwnDDLRecord is one namespace's row in OwnDDL.
type OwnDDLRecord struct {
	// ID is the namespace, db.coll.
	ID    string       `bson:"_id"`
	At    time.Time    `bson:"at"`
	Spans []OwnDDLSpan `bson:"spans,omitempty"`
}

// OpenSpanSeconds is how far an unfinished span reaches, in cluster time.
//
// A span is opened before the DDL and closed after it, so an open one means
// either that the DDL is still running or that whatever was running it went
// away. The first wants covering and the second must not last: a namespace
// whose row said "still building" for ever would hide the application's own
// index builds on that collection until the row's TTL took it.
//
// Five minutes covers an index build on a fast tier of any size this system is
// meant for, and a build slower than that has its event delivered, which is the
// direction everything here errs in.
const OpenSpanSeconds = 300

// Covers reports whether this system was doing DDL on this namespace at this
// oplog position.
func (d OwnDDLRecord) Covers(pos bson.Timestamp) bool {
	if pos.T == 0 && pos.I == 0 {
		return false
	}
	for _, s := range d.Spans {
		if before(pos, s.From) {
			continue
		}
		if s.To.T == 0 && s.To.I == 0 {
			if pos.T <= s.From.T+OpenSpanSeconds {
				return true
			}
			continue
		}
		if !before(s.To, pos) {
			return true
		}
	}
	return false
}

// before reports whether a comes strictly before b in the oplog.
func before(a, b bson.Timestamp) bool {
	if a.T != b.T {
		return a.T < b.T
	}
	return a.I < b.I
}

// MaxOwnDDLRecorded is how many spans one namespace's row keeps.
//
// A namespace ordinarily contributes two, the create and the one createIndexes
// that carries every index, and the Indexer does that once per namespace per
// process. Retries and a fast tier collection dropped and repaired again add
// more, so a handful of room is right; a row lives an hour.
//
// Overflowing drops the oldest, which shows an extra create rather than hiding
// one of the application's own index builds. That is the safe direction, and it
// is the same choice the sieve makes everywhere else: passing an event through
// is recoverable and hiding one is silent.
const MaxOwnDDLRecorded = 16

// AnnounceOwnDDL opens a span for DDL this system is about to do on ns.
//
// Before the command, and that ordering is the point. Recording afterwards
// leaves a window in which the event has been delivered and nothing yet says it
// was ours, which is the same window recordResidency is written first to avoid:
// the client's next getMore can arrive inside one round trip to the archive.
// It returns the token the close has to be given.
func AnnounceOwnDDL(ctx context.Context, cold *mongo.Client, ns string, from bson.Timestamp) (string, error) {
	tok := bson.NewObjectID().Hex()
	_, err := cold.Database(DB).Collection(OwnDDL).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: ns}},
		OwnDDLAnnouncement(OwnDDLSpan{From: from, Tok: tok}),
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return "", fmt.Errorf("announce own ddl for %s: %w", ns, err)
	}
	return tok, nil
}

// OwnDDLAnnouncement is the write that opens a span for DDL this system is
// about to do on the fast tier.
//
// The instant comes from the archive for the reason EvictionAnnouncement's
// does: it exists for the TTL index, and an early expiry here reaches a client
// as a create or a createIndexes on its own collection that nobody issued.
func OwnDDLAnnouncement(span OwnDDLSpan) bson.D {
	return bson.D{
		{Key: "$currentDate", Value: bson.D{{Key: "at", Value: true}}},
		{Key: "$push", Value: bson.D{{Key: "spans", Value: bson.D{
			{Key: "$each", Value: bson.A{span}},
			{Key: "$slice", Value: -MaxOwnDDLRecorded},
		}}}},
	}
}

// CloseOwnDDL closes the span AnnounceOwnDDL opened, named by its token.
//
// By token and not by position: see OwnDDLSpan.Tok for what matching on the
// position did when two brackets were open on one namespace at once.
func CloseOwnDDL(ctx context.Context, cold *mongo.Client, ns, tok string, to bson.Timestamp) error {
	if tok == "" {
		return nil
	}
	_, err := cold.Database(DB).Collection(OwnDDL).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: ns}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "spans.$[s].to", Value: to}}}},
		options.UpdateOne().SetArrayFilters([]interface{}{
			bson.D{{Key: "s.tok", Value: tok}},
		}))
	if err != nil {
		return fmt.Errorf("close own ddl for %s: %w", ns, err)
	}
	return nil
}

// ReadOwnDDL reads the rows for these namespaces, one query for the batch.
func ReadOwnDDL(ctx context.Context, cold *mongo.Client, nss []string) (map[string]OwnDDLRecord, error) {
	if len(nss) == 0 {
		return nil, nil
	}
	cur, err := cold.Database(DB).Collection(OwnDDL).Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: nss}}}})
	if err != nil {
		return nil, fmt.Errorf("read own ddl: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()
	out := make(map[string]OwnDDLRecord, len(nss))
	for cur.Next(ctx) {
		var r OwnDDLRecord
		if err := cur.Decode(&r); err != nil {
			continue
		}
		out[r.ID] = r
	}
	return out, cur.Err()
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
//
// Written through HeartbeatUpdate, so that the instant comes from the archive:
// the daemon that writes one and the daemon that reads it are different
// processes and may be different machines, and what the reader does is subtract
// it from its own clock.
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
		DaemonReportUpdate(dryRun, counters, gauges),
		options.UpdateOne().SetUpsert(true))
	return err
}

// DaemonReportUpdate is the write a daemon makes to say what it has done.
//
// Stamped by the archive, because what is done with the instant is subtracting
// it from somebody else's now: the router exports
// thermocline_daemon_seconds_since_pass from this field and there is an alert
// rule on that number being large. A daemon on a machine whose clock ran behind
// the router's looked like a daemon that had stopped.
func DaemonReportUpdate(dryRun bool, counters map[string]int64, gauges map[string]float64) bson.D {
	return bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "dryRun", Value: dryRun},
			{Key: "counters", Value: counters},
			{Key: "gauges", Value: gauges},
		}},
		{Key: "$currentDate", Value: bson.D{{Key: "at", Value: true}}},
	}
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
	// A pipeline rather than $set plus $setOnInsert, so that "since" can come
	// from the archive's clock and still only be written once: $ifNull keeps
	// whatever is there, which is what $setOnInsert was for. A pause is held
	// for as long as an operator leaves it, the router exports how long from
	// this field, and cline can be run from anywhere at all.
	_, err := cold.Database(DB).Collection(Markers).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: EvictionPausedMarker}},
		PauseUpdate(reason),
		options.UpdateOne().SetUpsert(true))
	return err
}

// PauseUpdate is the write that holds eviction, and records when the hold
// began if it is new.
//
// A pipeline rather than $set plus $setOnInsert, so that "since" can come from
// the archive's clock and still be written only once: $ifNull keeps whatever is
// already there, which is what $setOnInsert was for. The router exports how
// long the hold has lasted by subtracting this from its own now, and cline can
// be run from any machine at all.
func PauseUpdate(reason string) mongo.Pipeline {
	return mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{
		// $literal, because a reason beginning with $ would otherwise be read
		// as a field path.
		{Key: "reason", Value: bson.D{{Key: "$literal", Value: reason}}},
		{Key: "since", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$since", "$$NOW"}}}},
	}}}}
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

// HeartbeatUpdate is the write that says "this daemon is still going", stamped
// by the archive rather than by the daemon.
//
// Both heartbeats are read by the feeder, which asks whether the mark is recent
// enough to mean the other daemon is running. Ahead of the writer by more than
// markerFresh and the feeder decides nothing is running: it stops suppressing
// the copies a warm or a copy-up makes, mirrors them back into the archive they
// came from, and a client watching that collection is shown inserts nobody
// made.
func HeartbeatUpdate() bson.D {
	return bson.D{{Key: "$currentDate", Value: bson.D{{Key: "seen", Value: true}}}}
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
