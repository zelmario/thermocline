package router

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// Finishing a command whose primary went away underneath it.
//
// A write issued while the fast tier has no primary used to fail, and the
// reason was the same one behind read preference and pooling: authentication was
// proxied, so the one upstream connection was bound to the client's SCRAM
// exchange and could not be replaced. There was nothing to replay onto. Now the
// connections are the router's, so a freshly elected primary is one checkout
// away and the command can simply be sent again.
//
// What decides the whole design is which commands may be sent again, and the
// answer is not "the ones that failed". A write that was already applied and
// whose acknowledgement was lost looks identical, from here, to one that never
// ran. Sending an $inc twice because a primary stepped down between the write
// and its reply would be a far worse failure than the one being fixed.
//
// So this replays exactly what a driver talking straight to the replica set
// would have replayed, and for the same reason it is safe there:
//
//   - A retryable write, meaning one carrying a session and a transaction number
//     outside a transaction. MongoDB records the result of such a statement
//     against those two values and returns the recorded result rather than
//     applying it twice. The bytes are re-sent unchanged, so that
//     deduplication works exactly as it would for the driver's own retry.
//   - A read, which can be repeated because it changes nothing.
//
// Everything else is left to fail, and that failure is the honest one: the
// client is told what happened and can decide. A multi-document write is not
// retryable in MongoDB either, so this is not a limitation this proxy invents.
//
// A transaction is also left alone. Its statements are replayed by the driver's
// own withTransaction helper when it sees a transient label, which is the
// contract the router already relies on elsewhere.

// failoverAttempts is how many times a command is sent again before giving up.
//
// An election is usually decided in a couple of seconds, and a command that
// cannot be placed after several tries is better refused than held while the
// client waits without being told anything.
const failoverAttempts = 3

// failoverBackoff is how long to wait between attempts, giving the set time to
// elect somebody.
const failoverBackoff = 700 * time.Millisecond

// replayAfterFailover re-sends a command whose primary went away, when doing so
// cannot apply it twice. It reports whether it produced a reply.
func (c *conn) replayAfterFailover(ctx context.Context, msg *wire.Message, name string) (*wire.Message, bool) {
	if !c.pooledPrimary() || !c.replayable(msg, name) {
		return nil, false
	}

	// The connection this failed on is finished, whatever happens next.
	c.dropPrimary()

	for attempt := 1; attempt <= failoverAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(failoverBackoff):
		}

		// Ask the driver, which tracks the topology continuously, rather than
		// guessing. If it does not know of a primary yet the election is still
		// running and there is nothing to send to.
		if err := c.r.refreshPrimary(ctx); err != nil {
			continue
		}
		if err := c.borrowPrimary(ctx); err != nil {
			continue
		}
		if err := wire.Write(c.upstream, msg); err != nil {
			c.dropPrimary()
			continue
		}
		reply, err := wire.Read(c.upstream)
		if err != nil {
			c.dropPrimary()
			continue
		}
		// A reply that still says the primary moved means this one moved too, or
		// the new one stepped down in the meantime. Same treatment.
		if rdoc, derr := reply.ReplyDoc(); derr == nil && isFailoverReply(rdoc) {
			c.dropPrimary()
			continue
		}
		c.r.Stats.FailoverReplays.Add(1)
		c.log.Info("replayed a command after the fast tier changed primary",
			"command", name, "attempt", attempt)
		return reply, true
	}
	return nil, false
}

// endAfterFailover ends a connection the router cannot point anywhere useful.
//
// While authentication is proxied, a client's upstream connection was made by
// that client's own SCRAM exchange against whichever member was primary at the
// time. The router has no credentials of its own, so it cannot replace that
// connection: every command the client sends afterwards goes to the same
// member, which is now a secondary and refuses all of them. Re-resolving the
// pinned address fixes the next client, not this one.
//
// Measured before this existed, with the shop running through the router while
// the fast tier's primary stepped down: 4,226 refusals and still counting three
// minutes after the election, on a set that had elected a new primary in one
// second. The shop never recovered, because nothing ever ended the connections.
//
// So the connection ends once the refusal has been delivered, which is what
// MongoDB itself does to the clients of a member that stops being primary. The
// driver reconnects, that handshake authenticates against the member that is
// primary now, and the retry lands. With `terminate_auth` on none of this
// applies: the connections are the router's, a new primary is one checkout
// away, and the command is replayed above rather than refused.
func (c *conn) endAfterFailover(ctx context.Context) {
	if c.pooledPrimary() {
		return
	}
	// Wait for the re-resolution this refusal triggered rather than racing the
	// client's reconnect to it. See awaitPrimaryProbe for what losing that race
	// looks like.
	c.r.awaitPrimaryProbe(ctx)
	c.closeAfterReply = true
}

// replayable reports whether a command can be sent again without the risk of
// applying it twice.
func (c *conn) replayable(msg *wire.Message, name string) bool {
	if msg.MoreToCome() {
		// Nothing is expecting a reply, so there is nothing to put right, and
		// re-sending would be the double application this exists to avoid.
		return false
	}
	cmd, err := msg.CommandDoc()
	if err != nil {
		return false
	}
	// Left to the driver's own replay, which is the contract already in use for
	// copy-ups inside a transaction.
	if inTransaction(cmd) {
		return false
	}
	if readsDocuments[name] || name == "listCollections" || name == "listIndexes" {
		return true
	}
	return isRetryableWrite(cmd, name)
}

// isRetryableWrite reports whether MongoDB would deduplicate a second attempt at
// this command.
//
// A session and a transaction number outside a transaction is what a driver
// attaches when it intends to be able to retry, and it is what mongod keys the
// recorded result on. Without both, a second attempt is a second application.
func isRetryableWrite(cmd bson.Raw, name string) bool {
	switch name {
	case "insert", "update", "delete", "findAndModify", "findandmodify":
	default:
		return false
	}
	if _, err := cmd.LookupErr("lsid"); err != nil {
		return false
	}
	if _, err := cmd.LookupErr("txnNumber"); err != nil {
		return false
	}
	// A multi-document write is not retryable in MongoDB, so it is not
	// deduplicated and must not be replayed here either.
	if hasMultiStatement(cmd, name) {
		return false
	}
	return true
}

// hasMultiStatement reports whether any statement in a write command names more
// than one document.
func hasMultiStatement(cmd bson.Raw, name string) bool {
	field := ""
	switch name {
	case "update":
		field = "updates"
	case "delete":
		field = "deletes"
	default:
		return false
	}
	arr, err := cmd.LookupErr(field)
	if err != nil {
		return false
	}
	vals, err := arr.Array().Values()
	if err != nil {
		return true
	}
	for _, v := range vals {
		doc, ok := v.DocumentOK()
		if !ok {
			continue
		}
		if statementIsMulti(doc, name) {
			return true
		}
	}
	return false
}

// isFailoverReply reports whether a reply says the node it reached is no longer
// the primary.
//
// The error label is checked first because it is what MongoDB tells drivers to
// act on, and the codes are there for servers and paths that do not attach one.
func isFailoverReply(doc bson.Raw) bool {
	if replyOK(doc) {
		return false
	}
	if v, err := doc.LookupErr("errorLabels"); err == nil {
		if vals, aerr := v.Array().Values(); aerr == nil {
			for _, l := range vals {
				if s, ok := l.StringValueOK(); ok && s == "RetryableWriteError" {
					return true
				}
			}
		}
	}
	v, err := doc.LookupErr("code")
	if err != nil {
		return false
	}
	code, ok := intOf(v)
	if !ok {
		return false
	}
	switch code {
	case 10107, // NotWritablePrimary
		13435, // NotPrimaryNoSecondaryOk
		13436, // NotPrimaryOrSecondary
		11602, // InterruptedDueToReplStateChange
		189,   // PrimarySteppedDown
		91:    // ShutdownInProgress
		return true
	}
	return false
}
