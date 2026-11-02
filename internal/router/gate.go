package router

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/authz"
	"github.com/zelmario/thermocline/internal/wire"
)

// The gate every command passes before anything else looks at it.
//
// Placement is the whole point. This runs before the completeness guards, before
// the copy-up, before the unique key checks and before the command reaches
// either tier, because all of those read or write data and none of them asks who
// is calling. While authentication was proxied they did not have to: the fast
// tier held the client's identity and refused what it should. With the upstream
// connection belonging to the router, this function is the only thing that does.
//
// It fails closed in every direction. No identity, no privileges, an unreadable
// command, a command whose requirements are unknown: all refused.

// gate decides whether the command may proceed. It returns a reply to send when
// the answer is no.
func (c *conn) gate(msg *wire.Message, name, db string) (*wire.Message, bool) {
	if !c.r.cfg.TerminateAuth {
		// Authentication is proxied, so the fast tier is still the one deciding
		// and this must not second-guess it.
		return nil, false
	}
	if isSASL(name) || authz.NoAuthNeeded[name] {
		return nil, false
	}

	if c.grants == nil {
		out, err := c.unauthorized(msg, "command "+name+" requires authentication")
		if err != nil {
			return nil, false
		}
		return out, true
	}

	cmd, err := msg.CommandDoc()
	if err != nil {
		out, rerr := c.unauthorized(msg, "the command could not be read, so it could not be authorized")
		if rerr != nil {
			return nil, false
		}
		return out, true
	}

	needs, readable, err := needsOf(msg, name, db, cmd)
	if !readable {
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		out, rerr := c.unauthorized(msg, "the bulkWrite could not be read, so it could not be authorized")
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	if err != nil {
		// Deny by default. See package authz for why an unknown command is
		// refused rather than guessed at.
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a command whose required privilege is unknown",
			"command", name, "db", db, "user", c.grants.User)
		out, rerr := c.r.commandError(0, msg.Header.RequestID, err.Error())
		if rerr != nil {
			return nil, false
		}
		return out, true
	}

	if need, action, denied := firstDenied(*c.grants, needs); denied {
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Debug("refused an unauthorized command",
			"command", name, "ns", needNS(need), "action", action, "user", c.grants.User)
		out, rerr := c.unauthorized(msg,
			"not authorized on "+need.DB+" to execute command "+name)
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	return nil, false
}

// needsOf returns what a command requires. readable is false only for a
// bulkWrite whose parts could not be read; err names a command the table does
// not know.
func needsOf(msg *wire.Message, name, db string, cmd bson.Raw) (needs []authz.Need, readable bool, err error) {
	// bulkWrite is judged from its own parts, because it names no collection
	// where authorization would look for one. See bulkWriteNeeds.
	if name == "bulkWrite" {
		// With its payload sequences: a driver sends nsInfo as one, and read
		// from the body every driver bulkWrite was refused as unreadable.
		// See wire.Inlined.
		bcmd := cmd
		if in, ierr := msg.Inlined(); ierr == nil {
			bcmd = in
		}
		bn, ok := bulkWriteNeeds(bcmd, msg)
		return bn, ok, nil
	}
	needs, err = authz.Needs(name, db, cmd)
	if err != nil {
		return nil, true, err
	}
	// An aggregation reaches namespaces its own name does not mention, and
	// every one of them is a namespace mongod would have judged separately. An
	// explain of one reaches exactly the same namespaces.
	if aggCmd, aggDB, isAgg := aggregateCommand(name, db, cmd); isAgg {
		needs = append(needs, aggregateNeeds(aggCmd, aggDB)...)
	}
	return needs, true, nil
}

// firstDenied returns the first need the grants do not cover, and the action
// that was missing.
func firstDenied(g authz.Grants, needs []authz.Need) (authz.Need, string, bool) {
	for _, need := range needs {
		for _, action := range need.Actions {
			ok := false
			if need.Cluster {
				ok = g.AllowedCluster(action)
			} else {
				ok = g.Allowed(need.DB, need.Coll, action)
			}
			if !ok {
				return need, action, true
			}
		}
	}
	return authz.Need{}, "", false
}

func needNS(need authz.Need) string {
	if need.Cluster {
		return "the cluster"
	}
	if need.Coll != "" {
		return need.DB + "." + need.Coll
	}
	return need.DB
}

// unauthorized answers the way mongod does, so a driver's error handling and an
// application's own catch of an authorization failure behave unchanged.
func (c *conn) unauthorized(msg *wire.Message, errmsg string) (*wire.Message, error) {
	return wire.NewMsg(0, msg.Header.RequestID, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: errmsg},
		{Key: "code", Value: int32(13)},
		{Key: "codeName", Value: "Unauthorized"},
	})
}
