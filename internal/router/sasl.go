package router

import (
	"context"
	"fmt"
	"strings"

	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/authn"
	"github.com/zelmario/thermocline/internal/wire"
)

// The client's half of terminating authentication: the router answers saslStart
// and saslContinue itself instead of passing them to the fast tier.
//
// Only SCRAM is terminated. Anything else a client asks for is refused by name
// rather than passed through, because passing it through is what the whole
// change is undoing: the upstream connection belongs to the router now, so a
// mechanism negotiated on it would authenticate the router's own account and
// hand the client whatever that account can reach.

// authConv is one client's authentication state.
type authConv struct {
	conv   *scram.ServerConversation
	mech   string
	db     string
	user   string
	convID int32
	// step counts replies sent, which is all that is needed to tell the first
	// leg of the conversation from the second.
	step int
}

// isSASL reports whether a command is part of authenticating.
func isSASL(name string) bool {
	switch name {
	case "saslStart", "saslContinue", "logout":
		return true
	}
	return false
}

// handleSASL answers an authentication command. It returns the reply to send.
func (c *conn) handleSASL(ctx context.Context, msg *wire.Message, name, db string) (*wire.Message, error) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return c.saslError(msg, "thermocline: could not read the authentication command")
	}

	switch name {
	case "logout":
		// Forgetting the identity is the whole of it. The upstream connection
		// is the router's and stays where it is.
		c.auth = nil
		c.grants = nil
		return wire.NewMsg(0, msg.Header.RequestID, bson.D{{Key: "ok", Value: 1.0}})
	case "saslStart":
		return c.saslStart(ctx, msg, cmd, db)
	case "saslContinue":
		return c.saslContinue(ctx, msg, cmd)
	}
	return c.saslError(msg, "thermocline: unsupported authentication command "+name)
}

func (c *conn) saslStart(ctx context.Context, msg *wire.Message, cmd bson.Raw, db string) (*wire.Message, error) {
	mech, _ := stringField(cmd, "mechanism")
	if mech != authn.SHA256 && mech != authn.SHA1 {
		// Named rather than silently downgraded. A client that asked for
		// something this cannot verify must hear so, because the alternative is
		// authenticating it by a route nobody checked.
		return c.saslError(msg, fmt.Sprintf(
			"thermocline: this proxy terminates authentication and can only verify SCRAM-SHA-256 and "+
				"SCRAM-SHA-1, but the client asked for %q", mech))
	}

	payload, ok := saslPayload(cmd)
	if !ok {
		return c.saslError(msg, "thermocline: the authentication payload is missing or not binary")
	}

	// The username travels inside the SCRAM message, not the command, so the
	// conversation has to be started before it is known which credentials to
	// look up. That is what the CredentialLookup callback is for: scram calls it
	// with the username once it has parsed the client's first message.
	hgf, err := authn.HashFor(mech)
	if err != nil {
		return c.saslError(msg, "thermocline: "+err.Error())
	}

	var lookupErr error
	var found bool
	server, err := hgf.NewServer(func(user string) (scram.StoredCredentials, error) {
		creds, ok, lerr := c.r.creds.Lookup(ctx, db, user)
		if lerr != nil {
			lookupErr = lerr
			return scram.StoredCredentials{}, lerr
		}
		if !ok {
			return scram.StoredCredentials{}, fmt.Errorf("no such user")
		}
		st, ok := creds[mech]
		if !ok {
			return scram.StoredCredentials{}, fmt.Errorf("user has no %s credentials", mech)
		}
		found = true
		c.pendingUser = user
		return st.Credentials()
	})
	if err != nil {
		return c.saslError(msg, "thermocline: could not start authentication")
	}

	conv := server.NewConversation()
	out, err := conv.Step(string(payload))
	if err != nil {
		if lookupErr != nil {
			// Reaching the fast tier failed, which is not the same as a bad
			// password and must not be reported as one.
			c.log.Warn("could not read credentials while authenticating", "db", db, "err", lookupErr)
			return c.saslError(msg, "thermocline: could not reach the fast tier to check credentials")
		}
		return c.authFailed(msg)
	}
	_ = found

	c.auth = &authConv{conv: conv, mech: mech, db: db, user: c.pendingUser, convID: 1, step: 1}
	return saslReply(msg, 1, false, out)
}

func (c *conn) saslContinue(ctx context.Context, msg *wire.Message, cmd bson.Raw) (*wire.Message, error) {
	if c.auth == nil {
		return c.saslError(msg, "thermocline: saslContinue without saslStart")
	}
	payload, ok := saslPayload(cmd)
	if !ok {
		return c.saslError(msg, "thermocline: the authentication payload is missing or not binary")
	}

	// A client that did not ask to skip the empty exchange sends one more
	// saslContinue with nothing in it once the conversation is already valid.
	if c.auth.conv.Valid() {
		return saslReply(msg, c.auth.convID, true, "")
	}

	out, err := c.auth.conv.Step(string(payload))
	if err != nil {
		c.auth = nil
		return c.authFailed(msg)
	}
	c.auth.step++

	if !c.auth.conv.Valid() {
		return saslReply(msg, c.auth.convID, false, out)
	}

	// Authenticated. Loading what this identity may do is part of finishing the
	// handshake rather than something done lazily on the first command: a
	// failure to read privileges has to stop the login, not silently produce an
	// identity with no grants that then reads as a permissions problem on every
	// command afterwards.
	grants, gerr := c.r.grants.Load(ctx, c.auth.db, c.auth.user)
	if gerr != nil {
		c.log.Warn("authenticated but could not read privileges", "user", c.auth.user, "err", gerr)
		c.auth = nil
		return c.saslError(msg, "thermocline: could not read this user's privileges from the fast tier")
	}
	c.grants = &grants
	c.log.Debug("authenticated", "user", c.auth.user, "db", c.auth.db, "mechanism", c.auth.mech)

	return saslReply(msg, c.auth.convID, true, out)
}

// saslReply builds the reply shape a driver expects from a SASL command.
func saslReply(msg *wire.Message, convID int32, done bool, payload string) (*wire.Message, error) {
	return wire.NewMsg(0, msg.Header.RequestID, bson.D{
		{Key: "conversationId", Value: convID},
		{Key: "done", Value: done},
		{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte(payload)}},
		{Key: "ok", Value: 1.0},
	})
}

// authFailed is the one error a client may see about credentials, and it says
// nothing about which part was wrong.
//
// Code 18 is AuthenticationFailed, which is what mongod returns, so a driver's
// own retry and reporting behave exactly as they would without a proxy here.
func (c *conn) authFailed(msg *wire.Message) (*wire.Message, error) {
	return wire.NewMsg(0, msg.Header.RequestID, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: "Authentication failed."},
		{Key: "code", Value: int32(18)},
		{Key: "codeName", Value: "AuthenticationFailed"},
	})
}

func (c *conn) saslError(msg *wire.Message, errmsg string) (*wire.Message, error) {
	return wire.NewMsg(0, msg.Header.RequestID, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: errmsg},
		{Key: "code", Value: int32(18)},
		{Key: "codeName", Value: "AuthenticationFailed"},
	})
}

// saslPayload reads the payload, which drivers send as binary and a few older
// ones as a string.
func saslPayload(cmd bson.Raw) ([]byte, bool) {
	v, err := cmd.LookupErr("payload")
	if err != nil {
		return nil, false
	}
	if _, data, ok := v.BinaryOK(); ok {
		return data, true
	}
	if s, ok := v.StringValueOK(); ok {
		return []byte(s), true
	}
	return nil, false
}

// stripSpeculativeAuth removes a saslStart a driver embedded in hello.
//
// Drivers put one there to save a round trip. Forwarded upstream it would run a
// SCRAM conversation on the router's own connection, in the client's name, on a
// connection already authenticated as the router's account. MongoDB allows one
// authenticated user per connection, so at best it fails confusingly and at
// worst it changes who that shared connection is. Removing it costs the client
// one round trip and it then authenticates through the path this file
// terminates.
func stripSpeculativeAuth(cmd bson.Raw) (bson.D, bool, error) {
	elems, err := cmd.Elements()
	if err != nil {
		return nil, false, err
	}
	var found bool
	out := make(bson.D, 0, len(elems))
	for _, e := range elems {
		if e.Key() == "speculativeAuthenticate" {
			found = true
			continue
		}
		var v any
		if err := e.Value().Unmarshal(&v); err != nil {
			return nil, false, err
		}
		out = append(out, bson.E{Key: e.Key(), Value: v})
	}
	return out, found, nil
}

// dropSpeculativeAuth rewrites a hello that carried an embedded saslStart.
//
// The message is left alone when there is nothing to remove, so the common case
// costs a lookup rather than a rebuild.
func (c *conn) dropSpeculativeAuth(msg *wire.Message) *wire.Message {
	// OP_MSG only, for the same reason unnegotiateCompression checks: an
	// OP_QUERY carries its database in the collection name rather than in the
	// document, so rebuilding one as an OP_MSG produces a command with no $db
	// and the fast tier rejects it with "BSON field 'request.$db' is missing".
	// That reads like a malformed client rather than a rewrite in the proxy.
	if msg.Header.OpCode != wire.OpMsg {
		return msg
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return msg
	}
	out, found, err := stripSpeculativeAuth(cmd)
	if err != nil || !found {
		return msg
	}
	rebuilt, err := wire.NewMsg(msg.Header.RequestID, msg.Header.ResponseTo, out)
	if err != nil {
		return msg
	}
	return rebuilt
}

// dropSpeculativeResult removes a speculative authentication result from a
// hello reply, so the driver authenticates through the router instead of
// believing it already has.
func dropSpeculativeResult(doc bson.D) bson.D {
	out := make(bson.D, 0, len(doc))
	for _, e := range doc {
		if e.Key == "speculativeAuthenticate" {
			continue
		}
		out = append(out, e)
	}
	return out
}

// dropClientMetadata removes the metadata from a client's hello before it is
// forwarded on a pooled connection.
//
// mongod accepts one client metadata document per connection, in the first
// hello, and rejects the rest. A pooled connection has already introduced
// itself, so every client handshake after that must arrive without one or the
// borrower is refused outright. See helloUpstream.
func (c *conn) dropClientMetadata(msg *wire.Message) *wire.Message {
	if msg.Header.OpCode != wire.OpMsg {
		return msg
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return msg
	}
	elems, err := cmd.Elements()
	if err != nil {
		return msg
	}
	var found bool
	out := make(bson.D, 0, len(elems))
	for _, e := range elems {
		if e.Key() == "client" {
			found = true
			continue
		}
		var v any
		if uerr := e.Value().Unmarshal(&v); uerr != nil {
			return msg
		}
		out = append(out, bson.E{Key: e.Key(), Value: v})
	}
	if !found {
		return msg
	}
	rebuilt, err := wire.NewMsg(msg.Header.RequestID, msg.Header.ResponseTo, out)
	if err != nil {
		return msg
	}
	return rebuilt
}

// answerHello replies to a handshake without forwarding it.
//
// On a pooled connection a client handshake cannot be forwarded at all. mongod
// accepts one client metadata document per connection and refuses the rest, and
// the legacy OP_QUERY handshake carries one that cannot be stripped on the way
// out, because an OP_QUERY names its database in the collection and rebuilding
// it as an OP_MSG produces a command the server rejects for a different reason.
//
// So the router answers instead. It has everything the answer needs: a hello
// reply taken from the fast tier when the pooled connection was opened, which is
// where the wire version, the document size limits and the session support come
// from, and its own credential source for saslSupportedMechs. The client is
// still told it is talking to a mongos, exactly as before.
//
// It also removes a round trip from every client's connect.
func (c *conn) answerHello(ctx context.Context, msg *wire.Message) (*wire.Message, bool) {
	tmpl, ok := c.r.helloTemplate()
	if !ok {
		return nil, false
	}
	out, err := mongosIdentity(tmpl)
	if err != nil {
		return nil, false
	}
	out = dropSpeculativeResult(out)

	// saslSupportedMechs is how a driver learns what to authenticate with
	// without a round trip of guessing, and it is per user, so it is answered
	// from the same place the passwords are checked against.
	if cmd, cerr := msg.CommandDoc(); cerr == nil {
		if v, lerr := cmd.LookupErr("saslSupportedMechs"); lerr == nil {
			if full, sok := v.StringValueOK(); sok {
				db, user, found := strings.Cut(full, ".")
				if found {
					if creds, exists, lerr := c.r.creds.Lookup(ctx, db, user); lerr == nil && exists {
						if mechs := authn.Mechanisms(creds); len(mechs) > 0 {
							out = append(out, bson.E{Key: "saslSupportedMechs", Value: mechs})
						}
					}
				}
			}
		}
	}

	var reply *wire.Message
	var err2 error
	if msg.Header.OpCode == wire.OpQuery {
		reply, err2 = wire.NewReply(0, msg.Header.RequestID, out)
	} else {
		reply, err2 = wire.NewMsg(0, msg.Header.RequestID, out)
	}
	if err2 != nil {
		return nil, false
	}
	return reply, true
}
