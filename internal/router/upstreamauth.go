package router

import (
	"fmt"
	"net"

	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/authn"
	"github.com/zelmario/thermocline/internal/wire"
)

// The other half of terminating authentication: the router authenticating its
// own connection to the fast tier.
//
// While authentication was proxied this was the client's job and the router only
// carried the bytes. Now the raw upstream connection belongs to the router, so
// the router has to log in on it, as itself, before any client command can be
// forwarded.
//
// This is the moment the trust model changes and it is worth being plain about
// it. From here on the fast tier is answering the router's account, not the
// caller's, so it will do whatever is asked of it. Nothing downstream of this
// function knows who the client was. That is why the gate refuses first and why
// it denies by default.

// authenticateUpstream logs the raw connection in as the router's own account.
func (r *Router) authenticateUpstream(conn net.Conn) error {
	if r.cfg.ServiceUser == "" {
		return fmt.Errorf("terminate_auth is on but hot.uri carries no username for the router's own account")
	}

	// SCRAM-SHA-256 only here, deliberately. This end is not accommodating
	// somebody else's old driver, it is the router talking to a server chosen by
	// the same operator, so there is no reason to offer the weaker mechanism.
	client, err := scram.SHA256.NewClient(r.cfg.ServiceUser, r.cfg.ServicePass, "")
	if err != nil {
		return fmt.Errorf("start authentication as %s: %w", r.cfg.ServiceUser, err)
	}
	conv := client.NewConversation()

	first, err := conv.Step("")
	if err != nil {
		return fmt.Errorf("first authentication message: %w", err)
	}

	reply, err := r.runUpstreamCommand(conn, bson.D{
		{Key: "saslStart", Value: int32(1)},
		{Key: "mechanism", Value: authn.SHA256},
		{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte(first)}},
		// Ask the server to finish in two legs rather than three. Without it
		// the server withholds done until an extra empty exchange, and every
		// new upstream connection pays a round trip for nothing.
		{Key: "options", Value: bson.D{{Key: "skipEmptyExchange", Value: true}}},
		{Key: "$db", Value: r.cfg.ServiceAuthDB},
	})
	if err != nil {
		return err
	}

	convID, done, payload, err := saslReplyFields(reply)
	if err != nil {
		return err
	}

	// The order matters and getting it wrong looks like a rejected password.
	// SCRAM's last message is the server proving itself to the client, so the
	// conversation only becomes valid once the client half has stepped over
	// that message. Checking validity before that step never succeeds, however
	// correct the credentials are.
	for {
		next, serr := conv.Step(string(payload))
		if serr != nil {
			return fmt.Errorf("authenticate as %s: %w", r.cfg.ServiceUser, serr)
		}
		if conv.Valid() {
			// The fast tier proved itself. done means it considers the exchange
			// finished too; without it, it is waiting for the empty leg that
			// skipEmptyExchange was meant to avoid, so send it.
			if done {
				return nil
			}
			_, err = r.runUpstreamCommand(conn, bson.D{
				{Key: "saslContinue", Value: int32(1)},
				{Key: "conversationId", Value: convID},
				{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte(next)}},
				{Key: "$db", Value: r.cfg.ServiceAuthDB},
			})
			return err
		}
		if done {
			// The server says the conversation is over and the client half does
			// not agree that the server proved itself. Refusing to use the
			// connection is the only safe reading.
			return fmt.Errorf("authenticate as %s: the fast tier ended the conversation unverified", r.cfg.ServiceUser)
		}

		reply, err = r.runUpstreamCommand(conn, bson.D{
			{Key: "saslContinue", Value: int32(1)},
			{Key: "conversationId", Value: convID},
			{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte(next)}},
			{Key: "$db", Value: r.cfg.ServiceAuthDB},
		})
		if err != nil {
			return err
		}
		convID, done, payload, err = saslReplyFields(reply)
		if err != nil {
			return err
		}
	}
}

// runUpstreamCommand sends one command on the raw connection and reads its reply.
func (r *Router) runUpstreamCommand(conn net.Conn, cmd bson.D) (bson.Raw, error) {
	msg, err := wire.NewMsg(0, 0, cmd)
	if err != nil {
		return nil, fmt.Errorf("build upstream command: %w", err)
	}
	if err := wire.Write(conn, msg); err != nil {
		return nil, fmt.Errorf("send upstream command: %w", err)
	}
	reply, err := wire.Read(conn)
	if err != nil {
		return nil, fmt.Errorf("read upstream reply: %w", err)
	}
	doc, err := reply.ReplyDoc()
	if err != nil {
		return nil, fmt.Errorf("read upstream reply body: %w", err)
	}
	if !replyOK(doc) {
		errmsg := "unknown error"
		if v, lerr := doc.LookupErr("errmsg"); lerr == nil {
			if s, ok := v.StringValueOK(); ok {
				errmsg = s
			}
		}
		return nil, fmt.Errorf("the fast tier refused the router's own login: %s", errmsg)
	}
	return doc, nil
}

// saslReplyFields reads the three things a SASL reply carries.
func saslReplyFields(doc bson.Raw) (convID int32, done bool, payload []byte, err error) {
	if v, lerr := doc.LookupErr("conversationId"); lerr == nil {
		convID, _ = v.Int32OK()
	}
	if v, lerr := doc.LookupErr("done"); lerr == nil {
		done, _ = v.BooleanOK()
	}
	if v, lerr := doc.LookupErr("payload"); lerr == nil {
		if _, data, ok := v.BinaryOK(); ok {
			payload = data
		} else if s, ok := v.StringValueOK(); ok {
			payload = []byte(s)
		}
	}
	return convID, done, payload, nil
}
