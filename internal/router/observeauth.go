package router

import (
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Who a client is, while authentication is proxied.
//
// The router does not verify credentials in that mode: the client's SCRAM
// exchange is forwarded to the fast tier, which decides. The router only needs
// to know when an exchange finishes, and then asks the fast tier, on the
// client's own connection, who that connection now is.
//
// It used to read the name out of the client's first SCRAM message instead,
// and believe it once the fast tier said the exchange had succeeded. Three ways
// that name was not the one mongod authenticated: a legacy OP_QUERY saslStart
// carries no $db, so alice@payroll was recorded as alice@admin; a gs2 header
// with an authzid has no ",,", so a second n= placed later in the message was
// read instead of the first; and a hello's speculativeAuthenticate replaced
// the pending name, so a conversation finished later confirmed a name it never
// carried. The name keys the query cache and the live cache's per-user proof,
// so each was a way to be served another user's answers.
//
// connectionStatus answers with what mongod itself recorded, whatever the
// mechanism, so none of that parsing is needed.
//
// The point of knowing is the query cache. Its key carries who is asking, or
// one client would be served another's documents. With no name to use, the
// connection was the identity, and a pooled driver spreads a repeated question
// across its connections so the second asking lands where the first was never
// stored. Measured on a shop of 3,000 orders read through five connections:
// seven shapes, 378 lookups, no hits at all.

// noteProxiedAuth notes that an authentication exchange is under way, so its
// end is noticed.
func (c *conn) noteProxiedAuth(name string, cmd bson.Raw) {
	// A driver does not usually send saslStart at all. It puts the first SCRAM
	// message inside the handshake, as hello.speculativeAuthenticate, to save a
	// round trip, and only the rest of the conversation arrives as
	// saslContinue.
	if isHello(name) {
		if _, ok := cmd.Lookup("speculativeAuthenticate").DocumentOK(); ok {
			c.authPending = true
		}
		return
	}
	switch name {
	case "logout":
		// The connection is somebody else from here on, or nobody. Answers
		// cached under the old name must not be reachable from the new one, and
		// dropping the identity is what does that: the key changes with it.
		c.observedUser, c.authPending = "", false
	case "saslStart", "authenticate":
		c.authPending = true
	}
}

// confirmProxiedAuth asks the fast tier who this connection is once an
// exchange has finished.
func (c *conn) confirmProxiedAuth(name string, reply bson.Raw) {
	if !c.authPending {
		return
	}
	finished := false
	switch {
	case isHello(name):
		// A handshake that carried the first SCRAM message carries the fast
		// tier's answer to it in the same place. Without one, the driver
		// authenticates the long way and saslContinue decides.
		spec, ok := reply.Lookup("speculativeAuthenticate").DocumentOK()
		if !replyOK(reply) || !ok {
			return
		}
		done, _ := spec.Lookup("done").BooleanOK()
		finished = done
	case name == "saslStart" || name == "saslContinue":
		if !replyOK(reply) {
			// Refused. The connection is who it was before, which the fast
			// tier is asked below rather than assumed.
			finished = true
			break
		}
		done, _ := reply.Lookup("done").BooleanOK()
		finished = done
	case name == "authenticate":
		finished = true
	}
	if !finished {
		return
	}
	c.authPending = false
	c.observedUser = c.authenticatedAs()
}

// authenticatedAs asks the fast tier who this connection is authenticated as,
// as user@db, sorted and joined when there is more than one. Empty, which
// makes the connection the identity, when nobody is or the fast tier could not
// be asked: narrower than it needs to be, and never someone else.
func (c *conn) authenticatedAs() string {
	if c.upstream == nil {
		return ""
	}
	_, users, ok := c.proxiedGrants()
	if !ok || len(users) == 0 {
		return ""
	}
	sort.Strings(users)
	return strings.Join(users, ",")
}
