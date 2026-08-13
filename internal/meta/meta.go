// Package meta holds the names and record shapes Thermocline stores alongside
// user data. Bookkeeping lives in its own database on the cold tier, so user
// documents are never modified to carry Thermocline's state.
package meta

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	// DB is the database holding all Thermocline bookkeeping. It lives on the
	// cold tier, which is the durable side of the pair.
	DB = "_thermocline"

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

	// Watermarks records how far back the fast tier is known to be complete,
	// one per namespace. This is what lets a range query be answered from the
	// fast tier alone rather than falling through to the archive.
	Watermarks = "watermarks"

	// TombstoneField marks a cold document whose original the application
	// deleted. The document stays for auditing, but reads must not return it.
	TombstoneField = "_tcDeletedAt"
)

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
type ActiveMarker struct {
	ID   string    `bson:"_id"`
	Seen time.Time `bson:"seen"`
}
