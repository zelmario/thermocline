package router

import (
	"context"

	"github.com/zelmario/thermocline/internal/wire"
)

// Two commands that neither guard nor fallback can make transparent, and that
// were going through as though they could.
//
// A rename is the sharper of the two. The feeder ignores renames on purpose:
// replaying collection-level changes is exactly what would let an accident on
// the fast tier reach the archive, and that rule is the foundation everything
// else rests on. But it means a renamed collection moves on one tier and not
// the other. Afterwards, reads of the new name are not tiered at all, because
// the new name is not in feeder.namespaces, so they answer from whatever the
// fast tier holds; and reads of the old name find an empty fast tier and answer
// from an archive that nothing writes to any more. The collection is in two
// halves and neither half says so.
//
// mapReduce is the older problem in a different costume. It reads every
// document it is pointed at, on whichever tier it runs, and it can write its
// output into a collection. Pointed at a fast tier holding three of ten
// documents it produces a total of three and reports success.
//
// Neither can be done across two clusters, so both are refused, which is the
// standing rule here: a refusal is a limitation, a wrong answer is a bug.

// guardDDL refuses the collection-level commands that would split a tiered
// collection between the tiers, and the reads that cannot be bounded.
//
// It reports true when it has answered the client itself.
func (c *conn) guardDDL(ctx context.Context, msg *wire.Message, db, name string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}

	switch name {
	case "renameCollection":
		from, ok := stringField(cmd, "renameCollection")
		if !ok {
			return nil, false
		}
		to, _ := lookupString(cmd, "to")
		// renameCollection names both sides in full, as db.collection, because
		// it is run against admin.
		if !c.r.cfg.Manages(from) && !c.r.cfg.Manages(to) {
			return nil, false
		}
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a rename of a tiered collection", "from", from, "to", to)
		out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: refusing to rename "+from+", which is tiered. The feeder ignores renames "+
				"on purpose, so the fast tier would move and the archive would not: reads of "+to+
				" would then answer from the fast tier alone, and reads of "+from+" from an archive "+
				"nothing writes to. Stop tiering this collection, rename it on both tiers, and put "+
				"the new name in feeder.namespaces.")
		if rerr != nil {
			return nil, false
		}
		return out, true

	case "mapReduce", "mapreduce":
		coll, ok := stringField(cmd, name)
		if !ok {
			return nil, false
		}
		ns := db + "." + coll
		base, _, tiered := c.r.tieredBase(ctx, ns)
		if !tiered {
			return nil, false
		}
		// A complete fast tier can answer it, the same as any other read.
		if mark, known := c.r.completeness(ctx, base); known && mark.Claimed && mark.CoversAll() {
			return nil, false
		}
		c.r.Stats.Incomplete.Add(1)
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a mapReduce over an incomplete collection", "ns", ns)
		out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: refusing this mapReduce because the fast tier does not hold all of "+ns+
				", and mapReduce reads every document it is given, so it would total only the part "+
				"that is here and report success. It cannot be run against the archive either, "+
				"because its output can land in a collection, which would then be written to the "+
				"wrong tier. Warm "+ns+", or express this as an aggregation, which can be.")
		if rerr != nil {
			return nil, false
		}
		return out, true
	}
	return nil, false
}
