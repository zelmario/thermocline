package router

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// A command that describes a tiered collection has to find one on the fast tier,
// and four of them did not.
//
// Reads of such a collection work: the router falls through to the archive,
// which holds every document of what it is told to tier. The commands that ask
// *about* the collection rather than for its documents go to the fast tier
// alone, and a fast tier that has never held it answers that it does not exist.
//
// Measured on a tiered collection the archive holds six documents of and the
// fast tier has never had, with the router running throughout:
//
//	countDocuments                  6
//	find().itcount()                6
//	distinct                        6
//	collStats command               6
//	db.coll.stats()                 ERROR  Unable to retrieve storageStats in $collStats
//	getIndexes()                    ERROR  ns does not exist: catdb.tdbstats
//	validate()                      ERROR  Collection 'catdb.tdbstats' does not exist to validate
//	collMod                         ERROR  ns does not exist
//
// The collStats *command* answers only because mongod is lenient about it: asked
// about a namespace that does not exist it replies ok with a count of nought,
// which this router then corrects to six. The $collStats *stage* errors instead,
// and that is the one mongosh sends for db.coll.stats(). So two spellings of one
// question, and the one an operator types by hand is the one that fails.
//
// This is the catalog defect again, on four more commands. listCollections had
// it and was fixed by repairing rather than by answering: asking is what makes
// the router create the fast tier's copy the way the archive has it, with its
// options and its indexes, which is what lets the collection be listed and asked
// for its stats. The reasoning was already written where that create happens.
// Only listCollections ever triggered it.

// describesCollection names the commands that ask about a collection rather than
// about its documents, and takes the collection's name from each.
//
// aggregate is not here because an aggregation is usually a read; it is handled
// separately, for the two stages that describe the collection they run on.
var describesCollection = map[string]bool{
	"listIndexes": true,
	"validate":    true,
	"collMod":     true,
	// collStats did not error, because mongod is lenient about the command
	// form, and it is here anyway. Without it the two spellings left the fast
	// tier in different states: db.coll.stats() created the collection and
	// db.runCommand({collStats}) did not, so the same question answered from
	// nothing one way and from a real collection the other, with nindexes 0
	// against the truth.
	"collStats": true,
}

// describedStages are the aggregation stages that ask about the collection
// rather than reading it, and error rather than returning nothing when it is
// missing.
var describedStages = map[string]bool{
	"$collStats":  true,
	"$indexStats": true,
}

// ensureDescribable gives the fast tier a tiered collection the archive holds
// before a command that would otherwise be told it does not exist.
//
// Before the command rather than on the way back, because there is nothing to
// correct in an error: the reply names a namespace, not a count, and retrying
// after a repair would send the client's command twice. It is also the cheap
// order. EnsureCollection is memoised, so once the fast tier has the collection
// this is a map lookup, and the create it may do is one this router already does
// for every managed namespace as it starts.
//
// It cannot conjure a collection out of nothing: the create reads the archive's
// options and does nothing when the archive does not hold the collection either,
// so a name that exists nowhere still gets mongod's own answer. And it only ever
// touches namespaces the config manages, which are the ones this router creates
// on the fast tier at startup regardless, so no client can ask it into making
// something the operator did not configure.
func (c *conn) ensureDescribable(ctx context.Context, msg *wire.Message, name, db string) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return
	}
	coll, ok := describedCollection(cmd, name)
	if !ok {
		return
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return
	}
	// The same call the write paths make before an insert that would create the
	// collection bare, and it builds the archive's indexes on it afterwards, off
	// this path. So the first listIndexes after an adoption reports _id_ and the
	// rest arrive shortly: the fast tier's indexes are the fast tier's, which is
	// what listIndexes is asked for, and cline indexes is what reports both.
	c.r.ensureTieredCollection(ctx, ns)
}

// describedCollection reads the collection a describing command names, or
// reports false when this command does not describe one.
func describedCollection(cmd bson.Raw, name string) (string, bool) {
	if describesCollection[name] {
		return stringField(cmd, name)
	}
	if name != "aggregate" {
		return "", false
	}
	coll, ok := stringField(cmd, "aggregate")
	if !ok || !leadsWith(cmd, describedStages) {
		return "", false
	}
	return coll, true
}

// leadsWith reports whether a pipeline's first stage is one of the named ones.
//
// Only the first stage, because every stage this is asked about is only legal
// there. One function rather than one per caller: interceptAggCollStats asks
// about $collStats alone, because that is the reply it knows how to correct,
// and this file asks about both.
func leadsWith(cmd bson.Raw, stages map[string]bool) bool {
	v, err := cmd.LookupErr("pipeline")
	if err != nil {
		return false
	}
	vals, err := v.Array().Values()
	if err != nil || len(vals) == 0 {
		return false
	}
	stage, ok := vals[0].DocumentOK()
	if !ok {
		return false
	}
	elems, err := stage.Elements()
	if err != nil || len(elems) == 0 {
		return false
	}
	return stages[elems[0].Key()]
}
