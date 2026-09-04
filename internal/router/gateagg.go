package router

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/authz"
	"github.com/zelmario/thermocline/internal/wire"
)

// An aggregation names more collections than the one it was sent to.
//
// $lookup, $unionWith and $graphLookup read another collection. $out and $merge
// write one, and either can name a different database. mongod asks the
// authorization question separately for every one of them: find on each
// collection read, insert and remove on an $out target, insert and update on a
// $merge target.
//
// While authentication was proxied none of that had to happen here, because the
// fast tier received the command with the client's own identity on it and
// applied its own rules. With terminate_auth on, the upstream connection is the
// router's own and the fast tier will do whatever it is asked, so a namespace
// the gate does not judge is a namespace with no check on it at all: a user with
// find on one collection could read any other through a $lookup, and overwrite
// any other through an $out.
//
// The stage walk is the one the tiering guards already use, so a pipeline shape
// they can see into is one this can see into too, and a nested $lookup inside a
// $facet is judged like a top-level one.
func aggregateNeeds(cmd bson.Raw, db string) []authz.Need {
	stages, ok := pipelineStages(cmd)
	if !ok || len(stages) == 0 {
		return nil
	}

	var needs []authz.Need
	joined, _ := joinedCollections(stages, db, 0)
	for _, ns := range joined {
		jdb, jcoll := splitNS(ns)
		needs = append(needs, authz.Need{DB: jdb, Coll: jcoll, Actions: []string{"find"}})
	}

	// A stage that describes the collection rather than reading it declares its
	// own action, and find is not that action. A user holding find and nothing
	// else could otherwise read index usage through $indexStats, plan cache
	// entries through $planCacheStats, or open a change stream, each of which
	// mongod judges by a privilege of its own.
	coll, hasColl := stringField(cmd, "aggregate")
	if hasColl {
		if first, ok := firstStage(stages); ok {
			if actions, declared := stageActions[first]; declared {
				needs = append(needs, authz.Need{DB: db, Coll: coll, Actions: actions})
			}
		}
	}

	final, fok := stages[len(stages)-1].DocumentOK()
	if !fok {
		return needs
	}
	bypass := boolValue(cmd, "bypassDocumentValidation")
	if outDB, outColl, isOut := outTarget(final, db); isOut {
		// $out replaces the collection wholesale, which mongod requires remove
		// for as well as insert.
		actions := []string{"insert", "remove"}
		if bypass {
			actions = append(actions, "bypassDocumentValidation")
		}
		needs = append(needs, authz.Need{DB: outDB, Coll: outColl, Actions: actions})
		return needs
	}
	if mergeVal, merr := final.LookupErr("$merge"); merr == nil {
		if target, _, tok := mergeTarget(mergeVal, db); tok {
			mdb, mcoll := splitNS(target)
			actions := mergeActions(mergeVal)
			if bypass {
				actions = append(actions, "bypassDocumentValidation")
			}
			needs = append(needs, authz.Need{DB: mdb, Coll: mcoll, Actions: actions})
		}
	}
	return needs
}

// stageActions is what a stage requires for itself, for the stages that produce
// their own input and therefore carry no implicit find.
//
// Taken from each stage's own requiredPrivileges in mongod. $changeStream is
// the one that also keeps find, because reading a collection's changes is
// reading the collection: its action set is {changeStream, find}.
var stageActions = map[string][]string{
	"$indexStats":     {"indexStats"},
	"$collStats":      {"collStats"},
	"$planCacheStats": {"planCacheRead"},
	"$changeStream":   {"changeStream", "find"},
}

// mergeActions is what a $merge requires of its target, which depends on what it
// does when a document matches and when none does.
//
// mongod keys these by the pair: a merge that can only update needs update, one
// that can only insert needs insert, and one that can do either needs both.
// Asking for both every time refuses a user who may update a collection and may
// not insert into it, running a $merge that could never have inserted.
func mergeActions(v bson.RawValue) []string {
	whenMatched, whenNotMatched := "merge", "insert"
	if doc, ok := v.DocumentOK(); ok {
		if s, sok := stringField(doc, "whenMatched"); sok {
			whenMatched = s
		} else if _, perr := doc.LookupErr("whenMatched"); perr == nil {
			// A whenMatched given as a pipeline rather than a mode.
			whenMatched = "pipeline"
		}
		if s, sok := stringField(doc, "whenNotMatched"); sok {
			whenNotMatched = s
		}
	}
	canInsert := whenNotMatched == "insert"
	canUpdate := whenMatched != "fail"
	switch {
	case canInsert && canUpdate:
		return []string{"insert", "update"}
	case canInsert:
		return []string{"insert"}
	default:
		return []string{"update"}
	}
}

// firstStage returns the name of the pipeline's first stage.
func firstStage(stages []bson.RawValue) (string, bool) {
	doc, ok := stages[0].DocumentOK()
	if !ok {
		return "", false
	}
	elems, err := doc.Elements()
	if err != nil || len(elems) == 0 {
		return "", false
	}
	return elems[0].Key(), true
}

// boolValue reports whether a top-level field is boolean true.
func boolValue(doc bson.Raw, key string) bool {
	v, err := doc.LookupErr(key)
	if err != nil {
		return false
	}
	b, ok := v.BooleanOK()
	return ok && b
}

// bulkWriteNeeds authorizes MongoDB 8.0's bulkWrite, which no other write
// command resembles.
//
// It is sent to the admin database, lists its collections in nsInfo, and refers
// to them from each operation by position, so there is no collection name where
// authorization would look for one. Refusing it by name was safe and it was also
// the whole of the 8.0 write path, and this system's own tiering guards already
// decompose it. Judged the same way: each operation against the namespace it
// names, with the action that operation performs.
//
// The second result is false when the command cannot be read at all, which the
// caller turns into a refusal rather than an empty set of requirements.
func bulkWriteNeeds(cmd bson.Raw, msg *wire.Message) ([]authz.Need, bool) {
	ops, ok := parseBulkWrite(cmd, msg)
	if !ok {
		return nil, false
	}
	needs := make([]authz.Need, 0, len(ops))
	for _, op := range ops {
		db, coll := splitNS(op.ns)
		if coll == "" {
			return nil, false
		}
		var actions []string
		switch op.kind {
		case "insert":
			actions = []string{"insert"}
		case "update":
			actions = []string{"update"}
			if op.stmt.upsert {
				actions = append(actions, "insert")
			}
		case "delete":
			actions = []string{"remove"}
		default:
			return nil, false
		}
		needs = append(needs, authz.Need{DB: db, Coll: coll, Actions: actions})
	}
	return needs, true
}

// aggregateCommand returns the aggregation a command is, or wraps.
//
// An explain asks the planner what its inner command would do, and the planner
// resolves the same $lookup sources and the same $out target. So the namespaces
// it reaches are the namespaces the aggregation itself reaches, and they are
// judged the same way.
func aggregateCommand(name, db string, cmd bson.Raw) (bson.Raw, string, bool) {
	if name == "aggregate" {
		return cmd, db, true
	}
	if name != "explain" {
		return nil, "", false
	}
	inner, err := cmd.LookupErr("explain")
	if err != nil {
		return nil, "", false
	}
	doc, ok := inner.DocumentOK()
	if !ok {
		return nil, "", false
	}
	if _, aok := stringField(doc, "aggregate"); !aok {
		return nil, "", false
	}
	innerDB := db
	if v, verr := doc.LookupErr("$db"); verr == nil {
		if s, sok := v.StringValueOK(); sok && s != "" {
			innerDB = s
		}
	}
	return doc, innerDB, true
}
