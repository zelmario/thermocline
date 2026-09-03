package authz

import (
	"fmt"
	"strings"

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
	"whatsmyuri": true,
	// connectionStatus reports the caller's own authentication state and
	// nothing else, and mongod requires no privilege for it: measured, a user
	// with read on one database is allowed to run it. mongosh sends it on
	// connect.
	"connectionStatus": true,
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
	case "find", "count", "distinct":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"find"}}}, nil
	case "aggregate":
		// mongod requires find on the collection an aggregation reads, and an
		// aggregation whose first stage is an initial source does not read it:
		// it describes the collection, or produces its own documents. From
		// getPrivilegesForAggregate: find is required when the pipeline is
		// empty, or when "the first stage of the pipeline is not an initial
		// source", in which case "the pipeline is implicitly reading documents
		// from the underlying collection".
		//
		// Everything a pipeline needs beyond that comes from its stages, one at
		// a time, and the caller adds those: the collections a $lookup or a
		// $unionWith reads, the target an $out or a $merge writes, and the
		// actions a stage declares for itself. See aggregateNeeds in the
		// router.
		if startsWithInitialSource(cmd) {
			return nil, nil
		}
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
		return []Need{{DB: db, Coll: coll(), Actions: bypassing(cmd, "insert")}}, nil
	case "update":
		// An update carrying upsert:true can create, which mongod requires
		// insert for as well as update.
		actions := []string{"update"}
		if anyStatementUpserts(cmd, "updates") {
			actions = append(actions, "insert")
		}
		return []Need{{DB: db, Coll: coll(), Actions: bypassing(cmd, actions...)}}, nil
	case "delete":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"remove"}}}, nil
	case "findAndModify", "findandmodify":
		// It can update, remove or, with upsert, insert, and which one is not
		// always decidable from the command alone, so ask for what it can do.
		actions := []string{"find", "update", "remove"}
		if boolField(cmd, "upsert") {
			actions = append(actions, "insert")
		}
		return []Need{{DB: db, Coll: coll(), Actions: bypassing(cmd, actions...)}}, nil

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
	case "renameCollection":
		// mongod asks for renameCollectionSameDB on the database when both
		// names are in it. A rename across databases needs a compound set on
		// both sides, and this does not know how to judge that, so it stays
		// refused by name rather than guessed at.
		from, fok := stringField(cmd, "renameCollection")
		to, tok := stringField(cmd, "to")
		if !fok || !tok {
			return nil, fmt.Errorf("thermocline: refusing a renameCollection whose names could not be read")
		}
		fromDB, _, _ := strings.Cut(from, ".")
		toDB, _, _ := strings.Cut(to, ".")
		if fromDB == "" || fromDB != toDB {
			return nil, fmt.Errorf("thermocline: refusing to judge a renameCollection across databases, " +
				"which needs privileges on both sides that this does not know how to check")
		}
		return []Need{{DB: fromDB, Actions: []string{"renameCollectionSameDB"}}}, nil

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

	// explain runs nothing itself: it asks the planner what the command inside
	// it would do, and mongod requires exactly the privileges of that command.
	// Judging it by name alone would refuse every explain a driver sends, and
	// judging it as harmless would let a user read a collection's shape and its
	// index bounds through a command they may not run.
	case "explain":
		inner, ierr := cmd.LookupErr("explain")
		if ierr != nil {
			return nil, fmt.Errorf("thermocline: refusing an explain whose command could not be read")
		}
		doc, dok := inner.DocumentOK()
		if !dok {
			return nil, fmt.Errorf("thermocline: refusing an explain whose command could not be read")
		}
		elems, eerr := doc.Elements()
		if eerr != nil || len(elems) == 0 {
			return nil, fmt.Errorf("thermocline: refusing an explain whose command could not be read")
		}
		innerName := elems[0].Key()
		if innerName == "explain" {
			return nil, fmt.Errorf("thermocline: refusing an explain of an explain")
		}
		// The inner command carries its own database in $db when the driver
		// sends one, and otherwise belongs to the database the explain went to.
		innerDB := db
		if v, verr := doc.LookupErr("$db"); verr == nil {
			if s, sok := v.StringValueOK(); sok && s != "" {
				innerDB = s
			}
		}
		return Needs(innerName, innerDB, doc)

	// Cluster level.
	case "listDatabases":
		return []Need{{Cluster: true, Actions: []string{"listDatabases"}}}, nil

	// The diagnostic surface, which is what a driver, a shell and a monitoring
	// agent spend most of their commands on. None of it was here, and an
	// unjudged command is refused, so with terminate_auth on the router refused
	// all of it. Measured through a router with the setting on: fourteen out of
	// fourteen ordinary administrative commands refused, and the Go driver's own
	// connection setup sends getParameter, so every shop connection produced a
	// refusal nobody had asked for.
	//
	// Every action below was verified against mongod rather than remembered: a
	// custom role granting exactly these, and nothing else, makes each command
	// allowed for a user who otherwise has read on one database. The two that
	// need no privilege at all were established the same way, by a read-only
	// user being allowed to run them.
	case "getParameter":
		return []Need{{Cluster: true, Actions: []string{"getParameter"}}}, nil
	case "serverStatus":
		return []Need{{Cluster: true, Actions: []string{"serverStatus"}}}, nil
	case "replSetGetStatus":
		return []Need{{Cluster: true, Actions: []string{"replSetGetStatus"}}}, nil
	case "hostInfo":
		return []Need{{Cluster: true, Actions: []string{"hostInfo"}}}, nil
	case "getLog":
		return []Need{{Cluster: true, Actions: []string{"getLog"}}}, nil
	case "getCmdLineOpts":
		return []Need{{Cluster: true, Actions: []string{"getCmdLineOpts"}}}, nil
	// currentOp and killOp are spelled inprog and killop as actions, which is
	// the one place in this list where the action is not the command's name.
	case "currentOp":
		return []Need{{Cluster: true, Actions: []string{"inprog"}}}, nil
	case "killOp":
		return []Need{{Cluster: true, Actions: []string{"killop"}}}, nil

	// Database level.
	case "usersInfo":
		return []Need{{DB: db, Actions: []string{"viewUser"}}}, nil
	case "dbHash":
		return []Need{{DB: db, Actions: []string{"dbHash"}}}, nil
	// Reading the profiling level and setting it are the same command, and
	// mongod requires enableProfiler for both.
	case "profile":
		return []Need{{DB: db, Actions: []string{"enableProfiler"}}}, nil

	// Collection level.
	case "validate":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"validate"}}}, nil
	// reIndex is only allowed on a standalone, and both tiers here are replica
	// sets, so mongod refuses it with IllegalOperation whatever the privileges
	// are. Judged anyway, because the refusal that reaches the client should be
	// mongod's own rather than this proxy declining to think about it.
	case "reIndex":
		return []Need{{DB: db, Coll: coll(), Actions: []string{"reIndex"}}}, nil
	}

	return nil, fmt.Errorf("thermocline: refusing %q because it does not know what privilege the command requires, "+
		"and running it against the fast tier on the router's own connection would skip the check entirely", name)
}

// InitialSources are the stages that produce their own input, so a pipeline
// beginning with one never reads the collection it was sent to.
//
// Taken from the stages whose LiteParsed reports isInitialSource. Each declares
// what it needs for itself, which is not find: $indexStats needs indexStats,
// $planCacheStats needs planCacheRead, $collStats needs collStats.
var InitialSources = map[string]bool{
	"$collStats": true, "$indexStats": true, "$planCacheStats": true,
	"$currentOp": true, "$listSessions": true, "$listLocalSessions": true,
	"$listCatalog": true, "$shardedDataDistribution": true,
	"$documents": true, "$changeStream": true, "$queue": true,
}

// startsWithInitialSource reports whether the pipeline's first stage produces
// its own input.
func startsWithInitialSource(cmd bson.Raw) bool {
	first, ok := firstStageName(cmd)
	if !ok {
		// No pipeline, or one this cannot read. An empty pipeline reads the
		// collection, and an unreadable one is judged as if it did.
		return false
	}
	return InitialSources[first]
}

// firstStageName returns the name of a pipeline's first stage.
func firstStageName(cmd bson.Raw) (string, bool) {
	pv, err := cmd.LookupErr("pipeline")
	if err != nil {
		return "", false
	}
	arr, ok := pv.ArrayOK()
	if !ok {
		return "", false
	}
	vals, verr := arr.Values()
	if verr != nil || len(vals) == 0 {
		return "", false
	}
	doc, dok := vals[0].DocumentOK()
	if !dok {
		return "", false
	}
	elems, eerr := doc.Elements()
	if eerr != nil || len(elems) == 0 {
		return "", false
	}
	return elems[0].Key(), true
}

// stringField returns a string-valued field of a command.
func stringField(cmd bson.Raw, key string) (string, bool) {
	v, err := cmd.LookupErr(key)
	if err != nil {
		return "", false
	}
	return v.StringValueOK()
}

// bypassing adds the privilege that skipping document validation requires.
//
// A command carrying bypassDocumentValidation: true asks the server to write
// documents its own schema rules would reject, and mongod requires the
// bypassDocumentValidation action for it. Ignoring the field would let a user
// who may write a collection write into it what the collection says is invalid.
func bypassing(cmd bson.Raw, actions ...string) []string {
	if boolField(cmd, "bypassDocumentValidation") {
		return append(actions, "bypassDocumentValidation")
	}
	return actions
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
