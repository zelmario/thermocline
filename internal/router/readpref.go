package router

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// Where a read actually lands, and why it took an architectural change to make
// that the node the client asked for.
//
// The router answers hello as a mongos, and a mongos routes by read preference,
// so a driver talking to this attaches $readPreference to reads and has every
// reason to expect it acted on. For a long time it was not, and the reason was
// not laziness. Authentication was proxied: the client's SASL exchange went to
// one upstream connection, and that connection was the only thing carrying the
// client's identity. A second connection to a secondary would have been
// authenticated as nobody, SCRAM being nonce-bound and one-shot with no password
// here to redo it with, and using the router's own credentials there is exactly
// the privilege bypass the command surface audit found as item 0.
//
// Measured then, on a three-member fast tier with the profiler recording on
// every node: secondary, secondaryPreferred and nearest each landed on the
// primary, while the same query sent straight to the replica set landed on a
// secondary.
//
// Terminating authentication removed the obstacle rather than working around
// it. Connections belong to the router now and are authenticated as the router,
// so any of them can carry any client's command, because who the caller is has
// been established and checked before the command gets here. Reads asking for a
// non-primary node are sent to one, from a pool, whenever nothing about the
// command ties it to the primary. See secondaryread.go for those rules and
// pool.go for the connections.
//
// So the limitation stands only with terminate_auth off, which is still the
// default. It is counted either way: SecondaryReads for the ones that moved,
// PrimaryOnlyReads for the ones that did not, and the difference is how you tell
// the feature working from the feature merely configured.

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

	// A read that is about to be sent to a secondary is not a primary-only
	// read, and warning about it would report a limitation that no longer
	// applies to this connection.
	if c.routeToSecondary(msg, name) {
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
		"why", "authentication is proxied on this router, so the one upstream connection carrying "+
			"this client's identity is the connection to the primary; set router.terminate_auth "+
			"to let the router authenticate clients itself and spread these reads",
		"effect", "answers are correct and strictly fresher than asked for, but this client's read "+
			"load stays on the primary instead of spreading across the secondaries")
}

// readsDocuments lists the commands whose read preference would have decided
// which node answered.
var readsDocuments = map[string]bool{
	"find": true, "aggregate": true, "count": true, "distinct": true,
	"getMore": true,
}
