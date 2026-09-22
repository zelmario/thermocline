package router

import (
	"go.mongodb.org/mongo-driver/v2/bson"
)

// mongosIdentity rewrites a hello reply so the driver keeps talking to us.
//
// This is the detail that decides whether the proxy works at all. A driver uses
// hello for topology discovery: forward the fast tier's real reply and it reads
// the hosts array, connects straight to those replica set members, and never
// sends us another byte. Everything looks healthy until an archived document
// comes back missing.
//
// Answering as a mongos instead, with msg "isdbgrid" and no membership fields,
// makes the driver treat this as a sharded cluster. It then talks only to the
// routers it was given, and load balances across them.
//
// The rest of the reply is kept as the fast tier sent it, so wire version,
// document size limits and session support stay truthful rather than being
// guessed at by this proxy.
func mongosIdentity(reply bson.Raw) (bson.D, error) {
	elems, err := reply.Elements()
	if err != nil {
		return nil, err
	}

	// Fields that describe a replica set member. Leaving any of them in makes
	// the driver look past us.
	drop := map[string]bool{
		"hosts": true, "passives": true, "arbiters": true,
		"setName": true, "setVersion": true, "primary": true,
		"me": true, "secondary": true, "arbiterOnly": true,
		"passive": true, "hidden": true, "tags": true,
		"electionId": true, "lastWrite": true, "isreplicaset": true,
		"topologyVersion": true,
		// Not a replica set field, and dropped for a different reason: agreeing
		// a compressor makes every later message an OP_COMPRESSED blob this
		// proxy cannot read, which silently disables every guard it runs. See
		// compression.go.
		"compression": true,
	}

	out := make(bson.D, 0, len(elems)+1)
	var sawMsg, sawWritable bool
	for _, e := range elems {
		key := e.Key()
		if drop[key] {
			continue
		}
		if key == "msg" {
			sawMsg = true
			out = append(out, bson.E{Key: "msg", Value: "isdbgrid"})
			continue
		}
		// A mongos is always writable, because it has no primary state of its
		// own, and this answers as one. The fast tier's value is the state of
		// whichever member this client's connection happens to be pinned to,
		// and during an election that member is a secondary saying false.
		//
		// Measured across a forced step-down, sampling hello every 150ms on one
		// held connection: 153 of 173 replies said isWritablePrimary=false, and
		// it was still false 25 seconds later, because the pinned upstream was
		// the demoted node. A driver reads that as "this mongos cannot take
		// writes" and fails them with a server selection error before this
		// router's own failover retry is ever asked to run. That is what
		// testfailover saw under a loaded box: the client waited 2.4 seconds
		// and got "not primary", where the same write on a quiet box goes
		// through in 350ms.
		//
		// New connections never showed it, which is why it hid: each one is a
		// fresh handshake to whatever member the pool dials next.
		if key == "isWritablePrimary" || key == "ismaster" {
			sawWritable = true
			out = append(out, bson.E{Key: key, Value: true})
			continue
		}
		var v any
		if err := e.Value().Unmarshal(&v); err != nil {
			continue
		}
		out = append(out, bson.E{Key: key, Value: v})
	}
	if !sawMsg {
		out = append(out, bson.E{Key: "msg", Value: "isdbgrid"})
	}
	if !sawWritable {
		out = append(out, bson.E{Key: "isWritablePrimary", Value: true})
	}
	return out, nil
}

// isHello reports whether a command name is one of the handshake commands.
func isHello(name string) bool {
	switch name {
	case "hello", "isMaster", "ismaster":
		return true
	}
	return false
}
