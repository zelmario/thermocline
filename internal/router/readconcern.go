package router

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
)

// A read that falls through to the archive used to lose the caller's read
// concern entirely, because the whole readConcern document was stripped before
// the command was sent on.
//
// The stripping is half right. A readConcern can carry afterClusterTime or
// atClusterTime, and those are positions in the fast tier's oplog: sent to the
// archive they name an instant on a timeline that cluster has never heard of.
// They have to go. The *level* is not like that. It is a statement about
// durability that any replica set can honour, and dropping it silently turned
// `readConcern: majority` into a local read.
//
// On the old single-node archive that was a distinction without a difference,
// since a lone member's local reads are majority-committed by definition. On a
// real archive it is the difference between an answer that cannot be rolled
// back and one that can, and the caller asked for the former.

// archiveLevel is the read concern the archive should be given, and whether the
// caller's request can be honoured at all.
//
// An empty level means "send none, let the server default apply", which is what
// a command carrying no readConcern deserves.
func archiveLevel(cmd bson.Raw) (level string, honourable bool) {
	val, err := cmd.LookupErr("readConcern")
	if err != nil {
		return "", true
	}
	doc, ok := val.DocumentOK()
	if !ok {
		return "", true
	}
	lv, err := doc.LookupErr("level")
	if err != nil {
		// afterClusterTime with no level. The level was the default, and the
		// cluster time is not portable, so nothing carries over.
		return "", true
	}
	s, ok := lv.StringValueOK()
	if !ok {
		return "", true
	}

	switch s {
	case "local", "available", "majority":
		return s, true
	case "snapshot":
		// Outside a transaction, snapshot means a majority-committed snapshot,
		// and majority is what the archive can promise. The atClusterTime that
		// may accompany it is the fast tier's and is dropped, so this is a
		// snapshot of the archive rather than of the instant the caller named.
		return "majority", true
	case "linearizable":
		// Linearizable promises that the read reflects every write that
		// completed before it, on a primary confirmed still to be primary. It
		// is defined for a single document on the node that owns it. Answering
		// it from another cluster cannot mean that, and quietly downgrading a
		// caller who asked for the strongest guarantee in the system is worse
		// than telling them.
		return "", false
	default:
		return "", true
	}
}

// archiveReadConcern renders a level as the document to send with a command.
func archiveReadConcern(level string) (bson.D, bool) {
	if level == "" {
		return nil, false
	}
	return bson.D{{Key: "level", Value: level}}, true
}

// archiveReadConcernFor turns a level into a driver read concern, for the paths
// that go through the driver rather than building a command.
func archiveReadConcernFor(level string) *readconcern.ReadConcern {
	switch level {
	case "local":
		return readconcern.Local()
	case "available":
		return readconcern.Available()
	case "majority":
		return readconcern.Majority()
	default:
		return nil
	}
}

// mustLevel is archiveLevel's level, for the callers that have already checked
// the request can be honoured.
func mustLevel(cmd bson.Raw) string {
	level, _ := archiveLevel(cmd)
	return level
}

// maxTimeBudget reads the caller's maxTimeMS, or zero when they set none.
//
// It is what stops the freshness wait from outlasting a deadline the client
// already chose. Callers that set nothing get zero and wait the configured
// archive_wait as before.
func maxTimeBudget(cmd bson.Raw) time.Duration {
	val, err := cmd.LookupErr("maxTimeMS")
	if err != nil {
		return 0
	}
	n, ok := intOf(val)
	if !ok || n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Millisecond
}
