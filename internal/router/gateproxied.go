package router

import (
	"bytes"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/authz"
	"github.com/zelmario/thermocline/internal/wire"
)

// While authentication is proxied, the fast tier decides what a client may do,
// and it decides when the command reaches it. The guards run before that. The
// multi-document write guard ran the client's filter on the archive and copied
// the matches up; the upsert, bulkWrite and findAndModify guards did the same;
// the insert guard answered duplicate keys from the archive and the key
// registry; createIndexes and collMod walked both tiers; filemd5 counted chunks
// on both; rename and dbHash read the archive. All of it with the router's own
// credentials, for a client the fast tier then refused, and several of them
// copied the archive's error text into the reply, which told a client with no
// rights at all what an archived document held.
//
// So for exactly those commands, the client's privileges are asked of the fast
// tier on the client's own connection, and judged with the same table the
// terminate_auth gate uses, before any guard runs. Asked every time rather
// than remembered: mongod applies a revoked role to a connection that is
// already open, and a copy here would not. The cost is one round trip to the
// fast tier, paid only by a command a guard is about to act on with
// credentials that are not the client's.

// guardedCommands are the commands a guard acts on before the fast tier has
// judged them. Reads are not here: count, distinct and aggregate ask with
// mayRun or mayRead before the router answers them, and a find reaches the
// fast tier first.
var guardedCommands = map[string]bool{
	"insert":           true,
	"update":           true,
	"delete":           true,
	"findAndModify":    true,
	"findandmodify":    true,
	"bulkWrite":        true,
	"create":           true,
	"drop":             true,
	"createIndexes":    true,
	"collMod":          true,
	"renameCollection": true,
	"filemd5":          true,
	"dbHash":           true,
}

// writesOneCollection are the guarded writes whose every guard acts only on a
// namespace this deployment tiers: each of guardMultiWrite, guardUpsert,
// guardSortedFindAndModify, guardBulkWrite, guardReservedField, guardLargeID,
// guardInsert, guardUpdateKeys and prepareTxnKeyCheck returns at
// !cfg.Manages(ns) before touching the archive or the key registry, and
// interceptWrite and interceptFindAndModify do the same after the reply.
// guardReplacedRoots runs for any namespace but only rewrites the client's own
// pipeline, which the fast tier then judges. So a write to a collection nothing
// tiers reaches no guard, and asking the fast tier first was a round trip and a
// 13 KB decode for nothing. Measured on 2026-11-23: a shop's carts and stock
// updates, untiered beside tiered orders in one database, paid it on every
// write. The DDL commands stay judged by database, because rename, create and
// drop act across collections.
var writesOneCollection = map[string]bool{
	"insert":        true,
	"update":        true,
	"delete":        true,
	"findAndModify": true,
	"findandmodify": true,
	"bulkWrite":     true,
}

// proxiedGate refuses a guarded command the fast tier would refuse, before any
// guard acts on it. It returns a reply to send when the answer is no.
//
// It fails closed only where it knows the answer. A command whose requirements
// the table does not know is left to the fast tier, as every command was
// before: refusing it here would turn a gap in the table into an outage.
func (c *conn) proxiedGate(msg *wire.Message, name, db string) (*wire.Message, bool) {
	if c.r.cfg.TerminateAuth || !guardedCommands[name] {
		return nil, false
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	needs, readable, err := needsOf(msg, name, db, cmd)
	if !readable || err != nil {
		return nil, false
	}
	reaches := false
	for _, need := range needs {
		if need.Cluster || !c.r.managesAnythingIn(need.DB) {
			continue
		}
		if writesOneCollection[name] && need.Coll != "" && !strings.HasPrefix(need.Coll, "system.") &&
			!c.r.cfg.Manages(need.DB+"."+need.Coll) {
			continue
		}
		reaches = true
		break
	}
	if !reaches {
		return nil, false
	}

	grants, users, ok := c.proxiedGrants()
	if !ok {
		// The fast tier could not be asked, and the guards are about to act
		// with the router's credentials. Not knowing is not permission.
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		out, rerr := c.r.commandError(0, msg.Header.RequestID,
			"thermocline: could not ask the fast tier what this client may do, so "+name+
				" was not run; retry it")
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	if len(users) == 0 {
		if c.r.authDisabled.Load() || c.authIsOff() {
			return nil, false
		}
		c.r.Stats.Refused.Add(1)
		c.r.Stats.Unauthorized.Add(1)
		c.cls = ClassRefused
		out, rerr := c.unauthorized(msg, "command "+name+" requires authentication")
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	need, action, denied := firstDenied(grants, needs)
	if !denied {
		return nil, false
	}
	c.r.Stats.Refused.Add(1)
	c.r.Stats.Unauthorized.Add(1)
	c.cls = ClassRefused
	c.log.Debug("refused a command before its guard, which the fast tier would not have run",
		"command", name, "ns", needNS(need), "action", action)
	out, rerr := c.unauthorized(msg, "not authorized on "+need.DB+" to execute command "+name)
	if rerr != nil {
		return nil, false
	}
	return out, true
}

// proxiedGrants asks the fast tier, on the client's own connection, what the
// client may do. users is who the connection is authenticated as, user@db.
func (c *conn) proxiedGrants() (grants authz.Grants, users []string, ok bool) {
	probe, err := wire.NewMsg(0, 0, bson.D{
		{Key: "connectionStatus", Value: 1},
		{Key: "showPrivileges", Value: true},
		{Key: "$db", Value: "admin"},
	})
	if err != nil {
		return authz.Grants{}, nil, false
	}
	rdoc, got := c.probe(probe)
	if !got || !replyOK(rdoc) {
		return authz.Grants{}, nil, false
	}
	// The question is asked every time; only the parsing of an answer seen
	// before is skipped. A root user's answer is 13 KB of privileges, and
	// decoding it cost 129us, 74 KB and 1,510 allocations on every write
	// (2026-11-23), which under sixteen shoppers was most of the router's CPU.
	// authInfo is compared byte for byte, so a revoked role changes the bytes
	// and is parsed again; the rest of the reply carries clock fields that
	// change on every call and say nothing about who may do what.
	info, err := rdoc.LookupErr("authInfo")
	if err != nil {
		return authz.Grants{}, nil, false
	}
	if c.proxSeen != nil && bytes.Equal(c.proxSeen, info.Value) {
		return c.proxGrants, append([]string(nil), c.proxUsers...), true
	}
	var res struct {
		AuthInfo struct {
			Users []struct {
				User string `bson:"user"`
				DB   string `bson:"db"`
			} `bson:"authenticatedUsers"`
			Privileges []authz.Privilege `bson:"authenticatedUserPrivileges"`
		} `bson:"authInfo"`
	}
	if err := bson.Unmarshal(rdoc, &res); err != nil {
		return authz.Grants{}, nil, false
	}
	g := authz.Grants{Privileges: res.AuthInfo.Privileges}
	for i, u := range res.AuthInfo.Users {
		if i == 0 {
			g.User, g.DB = u.User, u.DB
		}
		users = append(users, u.User+"@"+u.DB)
	}
	c.proxSeen = append(c.proxSeen[:0], info.Value...)
	c.proxGrants, c.proxUsers = g, users
	return g, append([]string(nil), users...), true
}

// authIsOff reports whether the fast tier runs without access control, which
// is the one case where an unauthenticated connection may do anything.
//
// connectionStatus cannot tell: with access control off and with a client that
// has not logged in, both answer no users and no privileges. A command that
// needs authentication can. listCollections on admin is refused with
// Unauthorized to a connection that has not authenticated, and answered when
// access control is off. Once seen off it is remembered for the router's life:
// turning it on needs a restart of every mongod, and this router's connections
// go with them.
func (c *conn) authIsOff() bool {
	probe, err := wire.NewMsg(0, 0, bson.D{
		{Key: "listCollections", Value: 1},
		{Key: "nameOnly", Value: true},
		{Key: "filter", Value: bson.D{{Key: "name", Value: "thermocline.none"}}},
		{Key: "$db", Value: "admin"},
	})
	if err != nil {
		return false
	}
	rdoc, got := c.probe(probe)
	if !got || !replyOK(rdoc) {
		return false
	}
	c.r.authDisabled.Store(true)
	return true
}

// takesIdentityFromTransport reports whether a command authenticates with a
// mechanism that reads the identity from the TLS connection rather than from
// anything the client sends, which through this proxy is the wrong connection.
//
// A client's own certificate ends at the router: its TLS is terminated here,
// and the connection to the fast tier is one the router opened, presenting the
// router's certificate when hot.uri names one. MONGODB-X509 sent through it
// therefore authenticated the client as the router, whatever the router's
// certificate maps to, and every client could ask for that. No client can
// authenticate as itself this way through a proxy, so it is refused.
func takesIdentityFromTransport(name string, cmd bson.Raw) bool {
	mech := func(doc bson.Raw) bool {
		m, ok := lookupString(doc, "mechanism")
		return ok && strings.EqualFold(m, "MONGODB-X509")
	}
	switch {
	case name == "authenticate" || name == "saslStart":
		return mech(cmd)
	case isHello(name):
		spec, ok := cmd.Lookup("speculativeAuthenticate").DocumentOK()
		return ok && mech(spec)
	}
	return false
}

// maySpeculate reports whether this connection may have the archive read ahead
// of its find, with the router's credentials, before the fast tier has judged
// the find.
//
// Under proxied authentication a connection that never authenticated had its
// filter and sort run on the archive anyway; the answer was thrown away when
// the fast tier refused the find, and the work was not. Now the read ahead is
// for a connection the fast tier has named a user for, or for any connection
// when access control is off. An authenticated user without read on the
// namespace can still cause one read ahead whose answer is discarded, which is
// load and not disclosure.
func (c *conn) maySpeculate() bool {
	if c.r.cfg.TerminateAuth || c.observedUser != "" || c.r.authDisabled.Load() {
		return true
	}
	return c.upstream != nil && c.authIsOff()
}
