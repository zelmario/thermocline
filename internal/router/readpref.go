package router

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// Every read this router forwards is served by the fast tier's primary,
// whatever read preference the client asked for.
//
// That is not an oversight, and it is worth writing down because the router
// answers hello as a mongos, and a mongos does route by read preference. A
// driver talking to this therefore attaches $readPreference to reads and has
// every reason to expect it to be honoured.
//
// It cannot be, as things stand. Authentication is proxied rather than
// terminated: the client's SASL exchange is forwarded to one upstream
// connection, and that connection is the only thing that carries the client's
// identity. Opening a second connection to a secondary would produce a
// connection authenticated as nobody. SCRAM is nonce-bound and one-shot, so the
// exchange cannot be replayed there, and this router never sees a password to
// redo it with. The one remaining option, using the router's own credentials on
// the secondary, is exactly the privilege bypass that authz.go exists to close.
//
// So the honest position is that this is a capacity limitation, not a wrong
// answer. A read served by the primary is strictly fresher than one the client
// asked a secondary for, so nothing returns anything false. What changes is
// where the load lands: an application that pins reporting to secondaries moves
// that whole read load onto the primary the day it is put behind this router,
// and that is the sort of thing that must be visible before the pilot rather
// than discovered during it.
//
// Measured on a three-member fast tier with the profiler recording on every
// node: read preference secondary, secondaryPreferred and nearest each landed
// on the primary, while the same query sent straight to the replica set landed
// on a secondary.
//
// Lifting it means terminating authentication in the router, which is an
// architectural change and not a fix to make quietly.

// readPrefMode returns the read preference mode a command carries.
//
// Drivers send it as $readPreference because this router answers as a mongos;
// against a replica set they would encode the same intent in the connection
// string and never put it on the wire.
func readPrefMode(cmd bson.Raw) (string, bool) {
	val, err := cmd.LookupErr("$readPreference")
	if err != nil {
		return "", false
	}
	doc, ok := val.DocumentOK()
	if !ok {
		return "", false
	}
	mode, ok := lookupString(doc, "mode")
	if !ok || mode == "" {
		return "", false
	}
	return mode, true
}

// wantsSecondary reports whether a read preference asks for a node other than
// the primary.
//
// primaryPreferred is not one of them: it is satisfied by the primary, which is
// what it gets, so there is nothing to say about it.
func wantsSecondary(mode string) bool {
	switch mode {
	case "secondary", "secondaryPreferred", "nearest":
		return true
	}
	return false
}

// noteReadPreference records that a client asked for a non-primary read and
// warns the first time on each connection.
//
// Once per connection per mode, because a reporting client sends the same
// preference on every query and a line for each would bury the fact rather than
// report it.
func (c *conn) noteReadPreference(msg *wire.Message, name string) {
	if !readsDocuments[name] {
		return
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return
	}
	mode, ok := readPrefMode(cmd)
	if !ok || !wantsSecondary(mode) {
		return
	}

	c.r.Stats.PrimaryOnlyReads.Add(1)
	if c.warnedReadPref[mode] {
		return
	}
	if c.warnedReadPref == nil {
		c.warnedReadPref = map[string]bool{}
	}
	c.warnedReadPref[mode] = true

	c.log.Warn("a read asked for a non-primary node and was served by the primary",
		"mode", mode, "command", name,
		"why", "authentication is proxied, so the one upstream connection carrying this client's "+
			"identity is the connection to the primary, and a second connection to a secondary "+
			"could not be authenticated as this client",
		"effect", "answers are correct and strictly fresher than asked for, but this client's read "+
			"load stays on the primary instead of spreading across the secondaries")
}

// readsDocuments lists the commands whose read preference would have decided
// which node answered.
var readsDocuments = map[string]bool{
	"find": true, "aggregate": true, "count": true, "distinct": true,
	"getMore": true,
}
