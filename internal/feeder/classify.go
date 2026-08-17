package feeder

import (
	"github.com/zelmario/thermocline/internal/config"
)

// action is what the feeder does with one change event.
type action int

const (
	// actionSkip leaves the cold tier untouched.
	actionSkip action = iota
	// actionUpsert writes the full document into the cold tier.
	actionUpsert
	// actionTombstone marks the cold copy as deleted by the application.
	actionTombstone
	// actionHalt stops the feeder. Used when policy says never to guess.
	actionHalt
	// actionDeletePending is a delete whose meaning is not known yet. Deletes
	// are resolved in bulk at flush time, because one lookup per delete would
	// be far too slow while the evictor is running.
	actionDeletePending
)

func (a action) String() string {
	switch a {
	case actionUpsert:
		return "upsert"
	case actionTombstone:
		return "tombstone"
	case actionHalt:
		return "halt"
	default:
		return "skip"
	}
}

// classifyOp maps a change stream operationType to an action.
//
// The rule that matters: nothing destructive ever crosses into the cold tier.
// Drops, renames and collection-level DDL are ignored rather than replayed,
// which is what stops an accident on the fast tier from destroying the archive.
// Deletes are decided separately by classifyDelete.
func classifyOp(op string) (action, bool) {
	switch op {
	case "insert", "update", "replace":
		return actionUpsert, true
	case "delete":
		// Caller must consult classifyDelete.
		return actionSkip, false
	default:
		// drop, dropDatabase, rename, shardCollection, refineCollectionShardKey,
		// reshardCollection, invalidate, and anything added in a future release.
		return actionSkip, true
	}
}

// classifyDelete decides what a delete on the fast tier means for the archive.
//
// The evictor announces every document it is about to remove, so an announced
// delete is a document moving down a tier and the archive must keep it. An
// unannounced delete came from the application, and the archive must stop
// returning it.
//
// When nothing announced the delete, policy decides. Both mistakes are
// recoverable, and they fail in opposite directions: tombstoning an evicted
// document hides data that is still there, while ignoring an application
// delete resurrects data somebody asked to remove.
func classifyDelete(announced bool, policy config.UnannouncedDeletes) action {
	if announced {
		return actionSkip
	}
	switch policy {
	case config.Ignore:
		return actionSkip
	case config.Halt:
		return actionHalt
	default:
		return actionTombstone
	}
}

// skipNamespace reports whether a namespace is Thermocline's own bookkeeping or
// MongoDB's internal state. Mirroring either would be a loop or a mistake.
func skipNamespace(db string) bool {
	switch db {
	case "admin", "local", "config":
		return true
	}
	return db == metaDB
}
