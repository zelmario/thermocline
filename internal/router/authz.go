package router

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// Authentication is proxied rather than terminated, so the fast tier holds the
// client's identity and decides what it may read. That works because almost
// everything reaches the fast tier: a read it refuses comes back as an error,
// and an error is not a miss, so nothing falls through.
//
// count, distinct and aggregations are the exception, and they are the
// exception for a good reason. They are judged before they run, because an
// aggregation can end in $out and refusing one after the fact would leave a
// truncated collection already written. Judged before they run means the fast
// tier never sees them, so nothing consults the client's permissions, and the
// answer then comes from the archive over a connection the router opened with
// its own credentials.
//
// The client has no session on the archive at all. It cannot be authenticated
// there, so the fast tier has to be asked on its behalf, on the connection that
// already carries its identity.

// mayRead asks the fast tier whether this client is allowed to read a
// collection, using the client's own authenticated connection.
//
// It returns a reply to send when the answer is no, which is the fast tier's
// own error rather than one invented here: the client should see exactly what
// it would have seen without a proxy in the way.
func (c *conn) mayRead(msg *wire.Message, db, coll string) (*wire.Message, bool) {
	rdoc, got := c.probeRead(db, coll)
	if !got {
		return nil, true
	}
	if replyOK(rdoc) {
		return nil, true
	}
	if !isAuthError(rdoc) {
		// Something else went wrong with a bounded find on a collection this
		// router is about to read anyway. That is not an authorisation answer,
		// so it is not treated as one.
		return nil, true
	}

	c.r.Stats.Unauthorized.Add(1)
	c.log.Warn("refused an archive answer to a client the fast tier would not have served",
		"ns", db+"."+coll)

	out, berr := wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, rewriteReplyIDs(rdoc))
	if berr != nil {
		return nil, true
	}
	return out, false
}

// mayRun asks the fast tier whether this client may run a whole aggregate
// command, by explaining it on the client's own connection.
//
// mayRead asks about one collection, and an aggregation the router runs itself
// reaches more than that: $lookup, $unionWith and $graphLookup read others,
// and $out and $merge write a target. guardTieredOut ran the whole pipeline on
// this router's own clients with no question asked, so a client allowed to read
// nothing could $out a list of its own documents over a tiered collection, and
// one allowed to write a target could copy into it a collection it could not
// read. mongod authorises an explain exactly as it would the command
// explained, and queryPlanner verbosity runs nothing, so one probe answers for
// every namespace the command names, the target included.
//
// Only an authorisation refusal counts, as for mayRead: an explain that fails
// for any other reason is not an answer about privileges. Under terminate_auth
// the gate decided this command before it ran and the connection carries the
// router's identity, so the probe would ask the wrong question.
func (c *conn) mayRun(msg *wire.Message, cmd bson.Raw, db string) (*wire.Message, bool) {
	if c.r.cfg.TerminateAuth {
		return nil, true
	}
	elems, err := cmd.Elements()
	if err != nil {
		return nil, true
	}
	inner := make(bson.D, 0, len(elems))
	for _, e := range elems {
		if explainDrops(e.Key()) {
			continue
		}
		inner = append(inner, bson.E{Key: e.Key(), Value: e.Value()})
	}
	probe, err := wire.NewMsg(0, 0, bson.D{
		{Key: "explain", Value: inner},
		{Key: "verbosity", Value: "queryPlanner"},
		{Key: "$db", Value: db},
	})
	if err != nil {
		return nil, true
	}
	rdoc, got := c.probe(probe)
	if !got || replyOK(rdoc) || !isAuthError(rdoc) {
		return nil, true
	}

	c.r.Stats.Unauthorized.Add(1)
	c.log.Warn("refused a command this router would have run with its own credentials, "+
		"which the fast tier would not have run for this client", "db", db)
	out, berr := wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, rewriteReplyIDs(rdoc))
	if berr != nil {
		return nil, true
	}
	return out, false
}

// explainDrops reports whether a command field stays out of the command an
// explain wraps: the generic arguments mongod takes only at the top level, or
// refuses inside an explain, such as a session or a transaction.
func explainDrops(key string) bool {
	if strings.HasPrefix(key, "$") {
		return true
	}
	switch key {
	case "lsid", "txnNumber", "autocommit", "startTransaction", "readConcern", "writeConcern",
		"apiVersion", "apiStrict", "apiDeprecationErrors", "maxTimeMS", "comment":
		return true
	}
	return false
}

// probeRead runs a bounded find on the client's own connection and returns the
// reply, or false if there was no usable one.
func (c *conn) probeRead(db, coll string) (bson.Raw, bool) {
	probe, err := wire.NewMsg(0, 0, bson.D{
		{Key: "find", Value: coll},
		{Key: "filter", Value: bson.D{}},
		{Key: "limit", Value: 1},
		{Key: "singleBatch", Value: true},
		{Key: "$db", Value: db},
	})
	if err != nil {
		return nil, false
	}
	return c.probe(probe)
}

// probe sends one command on the client's own connection and returns the reply
// document, or false if there was no usable one.
func (c *conn) probe(probe *wire.Message) (bson.Raw, bool) {
	if werr := wire.Write(c.upstream, probe); werr != nil {
		c.upstreamBroke()
		return nil, false
	}
	reply, rerr := wire.Read(c.upstream)
	if rerr != nil {
		c.upstreamBroke()
		return nil, false
	}
	rdoc, derr := reply.ReplyDoc()
	if derr != nil {
		return nil, false
	}
	return rdoc, true
}

// canRead reports whether this client may read a namespace, for the catalog
// merges, which name collections and databases the fast tier did not.
//
// The fast tier filters both listings by what the client is allowed to see. The
// archive's half of the answer is read over a connection the router opened with
// its own credentials, and a client has no session there at all, so adding a
// name to that answer without asking would tell a limited user what exists in
// places the fast tier had deliberately not mentioned. Same reasoning as
// mayRead above, and the same probe.
//
// Anything other than a refusal counts as allowed. A probe that could not be
// sent, or a collection that does not exist on the fast tier, is not an
// authorisation answer and is not read as one.
func (c *conn) canRead(db, coll string) bool {
	if c.r.cfg.TerminateAuth {
		// The connection to the fast tier carries the router's identity, not
		// this client's, so the probe would ask the wrong question and always
		// answer yes. It is not needed either: with authentication terminated
		// the gate decided this command before it ran, and listCollections and
		// listDatabases are authorised for a whole database and a whole cluster
		// respectively. A client that got a listing at all was entitled to
		// everything in it, so adding the archive's half widens nothing.
		return true
	}
	rdoc, got := c.probeRead(db, coll)
	if !got {
		return true
	}
	if replyOK(rdoc) {
		return true
	}
	return !isAuthError(rdoc)
}

// isAuthError reports whether a reply is an authorisation refusal.
func isAuthError(reply bson.Raw) bool {
	if code, ok := intOf(reply.Lookup("code")); ok && code == 13 {
		return true
	}
	if name, ok := lookupString(reply, "codeName"); ok && name == "Unauthorized" {
		return true
	}
	if msg, ok := lookupString(reply, "errmsg"); ok &&
		strings.Contains(strings.ToLower(msg), "not authorized") {
		return true
	}
	return false
}

// rewriteReplyIDs copies a reply document so it can be sent under a different
// request id.
func rewriteReplyIDs(reply bson.Raw) bson.D {
	elems, err := reply.Elements()
	if err != nil {
		return bson.D{{Key: "ok", Value: 0.0}, {Key: "errmsg", Value: "not authorized"},
			{Key: "code", Value: int32(13)}, {Key: "codeName", Value: "Unauthorized"}}
	}
	out := make(bson.D, 0, len(elems))
	for _, e := range elems {
		out = append(out, bson.E{Key: e.Key(), Value: e.Value()})
	}
	return out
}
