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
	}

	out := make(bson.D, 0, len(elems)+1)
	var sawMsg bool
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
		var v any
		if err := e.Value().Unmarshal(&v); err != nil {
			continue
		}
		out = append(out, bson.E{Key: key, Value: v})
	}
	if !sawMsg {
		out = append(out, bson.E{Key: "msg", Value: "isdbgrid"})
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
