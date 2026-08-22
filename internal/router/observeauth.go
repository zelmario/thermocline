package router

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Who a client is, while authentication is proxied.
//
// The router does not verify credentials in that mode: the client's SCRAM
// exchange is forwarded to the fast tier, which decides. But the exchange goes
// through here, and the username is in it. The client's first SCRAM message is
//
//	n,,n=alice,r=<nonce>
//
// and the command it arrives in names the authentication database. So the
// router can read who is asking without taking any part in deciding whether
// they are who they say.
//
// It is only believed once the fast tier has said the handshake succeeded,
// which makes it exactly as trustworthy as the identity terminate_auth
// produces: the same cluster verified the same credentials either way.
//
// The point of knowing is the query cache. Its key carries who is asking, or
// one client would be served another's documents. With no name to use, the
// connection was the identity, and a pooled driver spreads a repeated question
// across its connections so the second asking lands where the first was never
// stored. Measured on a shop of 3,000 orders read through five connections:
// seven shapes, 378 lookups, no hits at all.

// noteProxiedAuth reads the username out of a SCRAM start the router is about
// to forward, and remembers it until the fast tier rules on it.
func (c *conn) noteProxiedAuth(name string, cmd bson.Raw) {
	// A driver does not usually send saslStart at all. It puts the first SCRAM
	// message inside the handshake, as hello.speculativeAuthenticate, to save a
	// round trip, and only the rest of the conversation arrives as
	// saslContinue. Reading only saslStart therefore reads nothing: measured,
	// six fresh connections authenticating as the same user shared no cache
	// entry at all, because no saslStart ever went past.
	if isHello(name) {
		spec, ok := cmd.Lookup("speculativeAuthenticate").DocumentOK()
		if !ok {
			return
		}
		c.observedPending = scramIdentity(spec, "db")
		return
	}

	switch name {
	case "logout":
		// The connection is somebody else from here on, or nobody. Answers
		// cached under the old name must not be reachable from the new one, and
		// dropping the identity is what does that: the key changes with it.
		c.observedUser, c.observedPending = "", ""
		return
	case "saslStart":
		c.observedPending = scramIdentity(cmd, "$db")
	}
}

// scramIdentity reads user@db out of a saslStart, whether it arrived as a
// command or embedded in a handshake. The two spell the database differently,
// which is the only reason this takes the field name.
//
// Only SCRAM carries the name in the payload. Anything else leaves the
// connection as the identity, which is narrower than it needs to be and never
// wrong.
func scramIdentity(cmd bson.Raw, dbField string) string {
	if mech, ok := stringField(cmd, "mechanism"); !ok || !strings.HasPrefix(mech, "SCRAM-") {
		return ""
	}
	payload, ok := saslPayload(cmd)
	if !ok {
		return ""
	}
	user, ok := scramUsername(payload)
	if !ok {
		return ""
	}
	db, _ := stringField(cmd, dbField)
	if db == "" {
		db = "admin"
	}
	return user + "@" + db
}

// confirmProxiedAuth believes a pending name once the fast tier has finished
// the handshake and accepted it.
func (c *conn) confirmProxiedAuth(name string, reply bson.Raw) {
	if c.observedPending == "" {
		return
	}
	// A handshake that carried the first SCRAM message carries the fast tier's
	// answer to it in the same place.
	if isHello(name) {
		if !replyOK(reply) {
			c.observedPending = ""
			return
		}
		spec, ok := reply.Lookup("speculativeAuthenticate").DocumentOK()
		if !ok {
			// The fast tier did not take up the speculative offer, so the
			// driver will authenticate the long way and saslContinue decides.
			return
		}
		if done, dok := spec.Lookup("done").BooleanOK(); dok && done {
			c.observedUser = c.observedPending
			c.observedPending = ""
		}
		return
	}
	if name != "saslStart" && name != "saslContinue" {
		return
	}
	if !replyOK(reply) {
		// Refused, so there is nobody to be. A failed attempt must not leave a
		// name behind for the next command to be cached under.
		c.observedPending = ""
		return
	}
	if done, ok := reply.Lookup("done").BooleanOK(); !ok || !done {
		// Still mid-conversation. The name stays pending.
		return
	}
	c.observedUser = c.observedPending
	c.observedPending = ""
}

// scramUsername reads the n= field out of a SCRAM client-first message.
//
// The message is a gs2 header, "n,," or "y,," or "p=name,,", followed by
// comma-separated attributes. The username is SASLprep'd by the client and its
// commas and equals signs arrive escaped, which is the only decoding owed here:
// getting that wrong would key one user's cache under another's name.
func scramUsername(payload []byte) (string, bool) {
	s := string(payload)
	// Past the gs2 header, which ends at the first empty field.
	if i := strings.Index(s, ",,"); i >= 0 {
		s = s[i+2:]
	}
	for _, field := range strings.Split(s, ",") {
		if !strings.HasPrefix(field, "n=") {
			continue
		}
		name := field[2:]
		if name == "" {
			return "", false
		}
		// =2C and =3D, in that order: unescaping the comma first could turn a
		// literal "=3D2C" into a comma that was never there.
		name = strings.ReplaceAll(name, "=2C", ",")
		name = strings.ReplaceAll(name, "=3D", "=")
		return name, true
	}
	return "", false
}
