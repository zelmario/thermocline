package router

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// dbHash over a tiered collection hashes one tier and calls it the collection.
//
// Every other command that reads documents is either checked for completeness
// and answered from the archive when the fast tier is short, or is a physical
// fact about one cluster and right to be. dbHash is neither. It reads every
// document in a collection and returns one hash, and a hash cannot be
// corrected: there is no way to combine the fast tier's with the archive's, and
// reading both tiers in full to compute one is not what a diagnostic should do.
//
// Measured on a tiered collection with ten documents in the archive and three
// on the fast tier, asking the router and the archive the same eleven
// document-reading commands: nine agreed and this was one of the two that did
// not. It answered a different hash, with nothing to say it had seen a third of
// the collection. dbHash is what somebody runs to compare two copies of a
// dataset, so a hash of part of it is the worst possible answer: it looks
// exactly like a hash.
//
// So it is refused for a tiered collection the fast tier does not claim in
// full, and allowed for one it does, which is when the hash is the collection's
// hash and the command means what it says.
func (c *conn) guardDBHash(ctx context.Context, msg *wire.Message, db string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	short := c.shortForDBHash(ctx, db, cmd)
	if len(short) == 0 {
		return nil, false
	}
	sort.Strings(short)
	// Named, but not all of them. `dbHash: 1` with nothing named covers every
	// collection in the database, and on a deployment that tiers a hundred of
	// them the reason disappeared into the list: measured at 108 names in one
	// error message, which is not something anybody reads.
	named := strings.Join(short, ", ")
	if len(short) > 6 {
		named = strings.Join(short[:6], ", ") +
			" and " + itoa(len(short)-6) + " more"
	}

	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a dbHash over a collection the fast tier does not hold in full",
		"db", db, "collections", short,
		"reason", "dbHash reads every document and returns one hash, and the fast tier holds part "+
			"of these, so the hash would describe that part",
		"effect", "answering would return a hash that looks like the collection's and is not, "+
			"which is the worst answer for a command used to compare two copies of a dataset",
		"action", "warm these collections so the fast tier holds them in full, name only the "+
			"collections it does hold, or run dbHash against the archive directly")
	out, cerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this dbHash because the fast tier holds only part of "+named+
			", so the hash would be of that part rather than of the collection. A hash cannot be "+
			"corrected across two tiers the way a count can. Warm them, name only the collections "+
			"the fast tier holds in full, or run dbHash against the archive directly.")
	if cerr != nil {
		return nil, false
	}
	return out, true
}

// shortForDBHash names the collections this dbHash would cover that the fast
// tier does not claim in full.
//
// With no collections named the command covers every collection in the
// database, which is the shape `db.runCommand({dbHash: 1})` sends and the one
// most likely to sweep a tiered collection in by accident.
func (c *conn) shortForDBHash(ctx context.Context, db string, cmd bson.Raw) []string {
	wanted, named := dbHashCollections(cmd)
	if !named {
		// Everything the configuration tiers in this database. Asking the fast
		// tier what it holds would miss exactly the case that matters, a tiered
		// collection whose copy there was dropped.
		for _, ns := range c.r.cfg.Managed {
			d, coll, ok := strings.Cut(ns, ".")
			if ok && d == db {
				wanted = append(wanted, coll)
			}
		}
	}

	var short []string
	for _, coll := range wanted {
		ns := db + "." + coll
		base, _, tiered := c.r.tieredBase(ctx, ns)
		if !tiered {
			continue
		}
		if mark, known := c.r.completenessForRead(ctx, base); known && mark.CoversAll() {
			continue
		}
		short = append(short, coll)
	}
	return short
}

// dbHashCollections reads the optional collections array, and reports whether
// the command named one at all.
func dbHashCollections(cmd bson.Raw) ([]string, bool) {
	v, err := cmd.LookupErr("collections")
	if err != nil {
		return nil, false
	}
	arr, aok := v.ArrayOK()
	if !aok {
		return nil, false
	}
	vals, verr := arr.Values()
	if verr != nil {
		return nil, false
	}
	var out []string
	for _, e := range vals {
		if s, ok := e.StringValueOK(); ok && s != "" {
			out = append(out, s)
		}
	}
	return out, len(out) > 0
}

func itoa(n int) string { return strconv.Itoa(n) }
