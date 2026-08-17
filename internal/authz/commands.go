package authz

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// What a command needs before it may run.
//
// The mapping is deliberately explicit and deliberately incomplete. Every entry
// here was written by looking up what mongod itself requires, and a command that
// is not here is refused rather than guessed at. See the package comment for why
// that is the right way round.

// Need is one authorization question: may this user perform these actions on
// this namespace.
type Need struct {
	DB      string
	Coll    string
	Actions []string
	// Cluster marks an action that is not about a namespace at all, such as
	// listDatabases.
	Cluster bool
}

// NoAuthNeeded lists the commands that carry no privilege at all, because they
// are how a connection is set up and torn down rather than a way to reach data.
//
// Keep this list short and keep it justified. Anything that can name a
// collection does not belong here, however harmless it looks: the question is
// not whether the command returns documents, it is whether it can be pointed at
// data the caller may not see.
var NoAuthNeeded = map[string]bool{
	"hello": true, "isMaster": true, "ismaster": true,
	"ping": true, "buildInfo": true, "buildinfo": true,
	"saslStart": true, "saslContinue": true, "logout": true,
	"getnonce": true, "authenticate": true,
	"endSessions": true, "startSession": true, "refreshSessions": true,
	"killCursors": true, "getLastError": true,
	"commitTransaction": true, "abortTransaction": true,
	"whatsmyuri": true, "getCmdLineOpts": false,
}

// Needs returns what a command requires, or an error naming the command when it
// is not one this package knows how to judge.
//
// cmd is the command document and db the database it was sent to.
func Needs(name, db string, cmd bson.Raw) ([]Need, error) {
	coll := func() string {
		if v, err := cmd.LookupErr(name); err == nil {
			if s, ok := v.StringValueOK(); ok {
				return s
			}
		}
		return ""
	}

	switch name {
	// Reads.
	case "find", "count", "distinct", "aggregate":
		// aggregate can end in $out or $merge, which writes. The router already
		// has to understand those stages for tiering, and the write target is
		// checked separately by the caller, which knows how to resolve them.
		return []Need{{DB: db, Coll: coll(), Actions: []string{"find"}}}, nil
	case "getMore":
		// The namespace travels in the command, not the cursor, so it can be
		// judged the same way. A cursor the caller was allowed to open cannot
		// become one it may not read, but judging it again costs nothing and
		// removes the need to trust that.
		c := ""
		if v, err := cmd.LookupErr("collection"); err == nil {
			c, _ = v.StringValueOK()
		}
		return []Need{{DB: db, Coll: c, Actions: []string{"find"}}}, nil

	// Writes.
	case "insert":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"insert"}}}, nil
	case "update":
		// An update carrying upsert:true can create, which mongod requires
		// insert for as well as update.
		actions := []string{"update"}
		if anyStatementUpserts(cmd, "updates") {
			actions = append(actions, "insert")
		}
		return []Need{{DB: db, Coll: coll(), Actions: actions}}, nil
	case "delete":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"remove"}}}, nil
	case "findAndModify", "findandmodify":
		// It can update, remove or, with upsert, insert, and which one is not
		// always decidable from the command alone, so ask for what it can do.
		actions := []string{"find", "update", "remove"}
		if boolField(cmd, "upsert") {
			actions = append(actions, "insert")
		}
		return []Need{{DB: db, Coll: coll(), Actions: actions}}, nil

	// Collection and index management.
	case "create":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"createCollection"}}}, nil
	case "drop":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"dropCollection"}}}, nil
	case "createIndexes":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"createIndex"}}}, nil
	case "dropIndexes":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"dropIndex"}}}, nil
	case "listIndexes":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"listIndexes"}}}, nil
	case "collMod":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"collMod"}}}, nil

	// Database level.
	case "listCollections":
		return []Need{{DB: db, Actions: []string{"listCollections"}}}, nil
	case "dbStats":
		return []Need{{DB: db, Actions: []string{"dbStats"}}}, nil
	case "collStats":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"collStats"}}}, nil
	case "dropDatabase":
		return []Need{{DB: db, Actions: []string{"dropDatabase"}}}, nil

	// Cluster level.
	case "listDatabases":
		return []Need{{Cluster: true, Actions: []string{"listDatabases"}}}, nil
	}

	return nil, fmt.Errorf("thermocline: refusing %q because it does not know what privilege the command requires, "+
		"and running it against the fast tier on the router's own connection would skip the check entirely", name)
}

// anyStatementUpserts reports whether any statement in a write command creates
// on a miss.
func anyStatementUpserts(cmd bson.Raw, field string) bool {
	arr, err := cmd.LookupErr(field)
	if err != nil {
		return false
	}
	vals, err := arr.Array().Values()
	if err != nil {
		return false
	}
	for _, v := range vals {
		doc, ok := v.DocumentOK()
		if !ok {
			continue
		}
		if boolField(doc, "upsert") {
			return true
		}
	}
	return false
}

func boolField(doc bson.Raw, key string) bool {
	v, err := doc.LookupErr(key)
	if err != nil {
		return false
	}
	b, ok := v.BooleanOK()
	return ok && b
}
