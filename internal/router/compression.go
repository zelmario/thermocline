package router

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// Wire compression is negotiated in the handshake, and if it is agreed this
// router stops being able to read anything.
//
// A client that connects with compressors=snappy sends its hello listing them,
// the fast tier agrees, and from then on both sides wrap every message in
// OP_COMPRESSED. This proxy understands OP_MSG and OP_QUERY. It does not
// understand OP_COMPRESSED, so every command becomes an opaque blob: the
// command name cannot be read, so no guard runs. Not one. No unique key is
// claimed, no completeness is checked, no document is copied up, and no write
// is stopped from duplicating an archived one.
//
// And nothing looks wrong. The bytes are forwarded either way, so the database
// answers normally and the tiering quietly stops being enforced.
//
// The fix is to make sure the negotiation never succeeds. The compressor list
// is removed from the handshake in both directions: from the client's hello, so
// the fast tier never agrees to compress its replies, and from the reply, so
// the client never starts compressing its commands. Both are needed. Removing
// only the reply leaves the server compressing at a client that asked for it.
//
// The cost is bandwidth on the client link, which is a real cost and the right
// one to pay. A slower proxy that keeps every guarantee beats a faster one that
// silently keeps none.

// stripCompression removes the compressor list from a handshake command, and
// reports whether it had to.
func stripCompression(cmd bson.Raw) (bson.D, bool) {
	elems, err := cmd.Elements()
	if err != nil {
		return nil, false
	}
	found := false
	out := make(bson.D, 0, len(elems))
	for _, e := range elems {
		if e.Key() == "compression" {
			found = true
			continue
		}
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	if !found {
		return nil, false
	}
	return out, true
}

// unnegotiateCompression rewrites a handshake on its way to the fast tier so no
// compressor can be agreed.
//
// It returns the message to forward, which is the original when there was
// nothing to remove.
func (c *conn) unnegotiateCompression(msg *wire.Message) *wire.Message {
	if msg.Header.OpCode != wire.OpMsg {
		return msg
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return msg
	}
	out, stripped := stripCompression(cmd)
	if !stripped {
		return msg
	}
	rewritten, err := wire.NewMsg(msg.Header.RequestID, msg.Header.ResponseTo, out)
	if err != nil {
		return msg
	}
	c.log.Debug("removed the compressor list from a handshake",
		"reason", "this proxy cannot read OP_COMPRESSED, and every guard it runs needs to read the command")
	return rewritten
}
