package router

import (
	"errors"
	"fmt"
	"strings"
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
		// and majority is what the archive can promise, as long as the caller
		// left the instant to the server.
		//
		// An atClusterTime is an instant in the fast tier's history, and the
		// archive has none of those: it holds what was mirrored since, deletes
		// included. Read there, a snapshot at a past instant returned a
		// document created after it and missed one deleted after it (testtxn,
		// "a snapshot read at a past cluster time": 1,2 where mongod says
		// 1,3). Drivers send one on every read after the first in a snapshot
		// session, so this is not exotic.
		if _, err := doc.LookupErr("atClusterTime"); err == nil {
			return "", false
		}
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

// errReadConcern marks an archive read refused for its read concern, so the
// refusal the client gets names that reason rather than the incompleteness that
// sent the read to the archive.
var errReadConcern = errors.New("the archive cannot honour this read concern")

// archiveConcernError is the error an archive read returns for a read concern
// archiveLevel calls unhonourable.
func archiveConcernError(cmd bson.Raw) error {
	return fmt.Errorf("%w: %s", errReadConcern, archiveRefusal(cmd))
}

// concernReason is archiveRefusal's text, read back off an archiveConcernError.
func concernReason(err error) string {
	return strings.TrimPrefix(err.Error(), errReadConcern.Error()+": ")
}

// archiveRefusal says why a read concern archiveLevel calls unhonourable cannot
// be answered from the archive, and what the caller can do instead.
func archiveRefusal(cmd bson.Raw) string {
	if doc, ok := cmd.Lookup("readConcern").DocumentOK(); ok {
		if _, err := doc.LookupErr("atClusterTime"); err == nil {
			return "readConcern atClusterTime names an instant in the fast tier's history, which the " +
				"archive does not have: it holds what was mirrored since, so a document created after " +
				"that instant would be read and one deleted after it missed; warm this range, or read " +
				"without atClusterTime"
		}
	}
	return "readConcern linearizable cannot be answered from the archive: it promises a read " +
		"confirmed on the primary that owns the document; warm this range, or read it with majority"
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
