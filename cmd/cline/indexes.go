package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/config"
)

// Nothing copies indexes between the tiers, and nothing should: index trees
// describe the whole collection, and carrying every one of them on the archive
// gives back most of what tiering buys. The rollout guide says as much, and
// tells operators to drop the ones the cheap tier does not need.
//
// What it does not say is which ones those are, and that is the gap this
// closes. The archive stopped being a place that only answers lookups by _id
// the moment router.completeness became "archive": it now answers range reads,
// countDocuments and aggregations whenever the fast tier cannot prove it holds
// the range. Those run with whatever indexes the archive happens to have, and
// on a multi-terabyte archive the difference between an index and a collection
// scan is not slow against fast. It is minutes against milliseconds.
//
// So an operator needs to see the divergence rather than infer it, and needs to
// see it before the query that finds out for them.

const indexesUsage = `Usage: cline indexes [--config FILE] [--ns db.coll]

Compare the indexes on both tiers, for the namespaces Thermocline manages.

The fast tier is expected to carry more of them; that is what makes the archive
cheap. What matters is the other direction: an index the archive is missing is
one that a read falling through has to do without, so any query filtering or
sorting on those fields scans the archive instead of seeking into it.

Nothing is changed. This reports, so the decision stays with the operator.
`

// indexInfo is one index as both tiers describe it.
type indexInfo struct {
	name string
	spec string
	// unique is worth calling out: a unique index on the archive is not what
	// enforces uniqueness across the tiers, the key registry is, so its absence
	// there is expected rather than a finding.
	unique bool
	// ttl says the index expires documents. It is the one index that must
	// never be copied to the archive: the archive exists to keep what the fast
	// tier has stopped holding, and a TTL index there would delete exactly
	// that. An operator copying the fast tier's indexes across by hand would
	// take this one with the rest, so it is named rather than left to be
	// noticed.
	ttl bool
	// ttlSecs is how long after the indexed date a document is deleted, quoted
	// in the report so an operator can tell a 90 day expiry from a 90 second one
	// without going to look.
	ttlSecs float64
	// keyJSON is the key pattern quoted so it can be pasted into a command.
	// spec is the readable form and is not always valid JavaScript, because an
	// index on a nested path has a dot in its field name.
	keyJSON string
}

func runIndexes(args []string) error {
	fs := flag.NewFlagSet("indexes", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	only := fs.String("ns", "", "one namespace as db.collection, default every managed one")
	commands := fs.Bool("commands", false, "also print createIndex commands for what the archive is missing")
	fs.Usage = func() { fmt.Print(indexesUsage); fs.PrintDefaults() }
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	hot, err := connect(ctx, cfg.Hot.URI, "hot")
	if err != nil {
		return err
	}
	defer func() { _ = hot.Disconnect(context.WithoutCancel(ctx)) }()

	cold, err := connect(ctx, cfg.Cold.URI, "cold")
	if err != nil {
		return err
	}
	defer func() { _ = cold.Disconnect(context.WithoutCancel(ctx)) }()

	namespaces := cfg.Feeder.Namespaces
	if *only != "" {
		namespaces = []string{*only}
	}
	if len(namespaces) == 0 {
		return errors.New("no namespaces configured, and none given with --ns")
	}
	sort.Strings(namespaces)

	missingTotal := 0
	var toCreate []nsMissing
	// Namespaces whose archive copy is being expired by an index of its own.
	var toStopExpiring []nsMissing
	for _, ns := range namespaces {
		db, coll := splitNamespace(ns)
		if db == "" || coll == "" {
			fmt.Printf("%s: not db.collection, skipped\n\n", ns)
			continue
		}

		hotIdx, herr := indexesOf(ctx, hot, db, coll)
		coldIdx, cerr := indexesOf(ctx, cold, db, coll)
		if herr != nil && cerr != nil {
			// Neither tier has heard of it. A configured namespace that does
			// not exist yet is normal, not a problem.
			continue
		}

		missing := absentFrom(hotIdx, coldIdx)
		// And the other way round, which is the direction that matters on the
		// day a deployment starts. The existing cluster becomes the archive
		// carrying every index the application has; the fast tier is new and
		// filled by the warmer, and an index is not a document. Reporting only
		// what the archive lacks would print "the archive can answer anything
		// the fast tier can" about a fast tier that can answer nothing except
		// by _id, which is true and useless.
		bare := absentFrom(coldIdx, hotIdx)

		fmt.Printf("%s\n", ns)
		fmt.Printf("  fast tier %2d index(es), archive %2d\n", len(hotIdx), len(coldIdx))

		if len(bare) > 0 {
			sort.Slice(bare, func(i, j int) bool { return bare[i].name < bare[j].name })
			fmt.Printf("  missing from the FAST TIER:\n")
			uniqueBare := 0
			for _, m := range bare {
				note := ""
				if m.ttl {
					note = "   (TTL: expiring on the fast tier would delete the archive's" +
						" copies too, through the feeder)"
				}
				if m.unique {
					note = "   (unique: the archive refuses writes this one rejects, and the" +
						" fast tier no longer does)"
					uniqueBare++
				}
				fmt.Printf("    %-34s %s%s\n", m.name, m.spec, note)
			}
			fmt.Printf("  the fast tier scans for these, which is the tier that exists not to\n")
			// A unique index in that list is not about speed at all, and saying
			// only "the fast tier scans for these" about one is misleading in
			// the direction that costs data.
			//
			// The router enforces uniqueness from the FAST TIER's indexes, so
			// while the archive has one the fast tier does not, the router
			// accepts a value the archive will refuse, the feeder cannot mirror
			// that document, and it is left on the fast tier alone. Measured: a
			// unique index dropped through the router, then a duplicate
			// written, and afterwards the fast tier held two documents and the
			// archive one.
			if uniqueBare > 0 {
				fmt.Printf("  one of those is unique, which is worse than slow: the router reads the\n" +
					"  FAST TIER's unique indexes to know what to protect, so it accepts a value\n" +
					"  the archive then refuses, and that document stays on the fast tier alone.\n" +
					"  Either build it on the fast tier, or drop it from the archive if the\n" +
					"  constraint is genuinely gone.\n")
			}
		}

		// A TTL index on the archive is not a missing index in either
		// direction, so nothing above prints it, and on an adoption it is the
		// archive deleting the history it was adopted to keep. It arrives with
		// the cluster: the application put it on what used to be its only
		// database, and the day that database became the archive the index went
		// on doing what it always did.
		var expiring []indexInfo
		for _, m := range coldIdx {
			if m.ttl {
				expiring = append(expiring, m)
			}
		}
		if len(expiring) > 0 {
			sort.Slice(expiring, func(i, j int) bool { return expiring[i].name < expiring[j].name })
			fmt.Printf("  the ARCHIVE expires documents of this collection:\n")
			for _, m := range expiring {
				fmt.Printf("    %-34s %s   (after %s)\n", m.name, m.spec,
					(time.Duration(m.ttlSecs) * time.Second).String())
			}
			fmt.Printf("  that history leaves the tier that exists to keep it, and a read falling\n" +
				"  through is then short by whatever expired, with no error on it\n")
			toStopExpiring = append(toStopExpiring, nsMissing{ns: ns, missing: expiring})
		}

		if len(missing) == 0 {
			if len(bare) == 0 && len(expiring) == 0 {
				fmt.Printf("  the archive can answer anything the fast tier can\n")
			}
			fmt.Print("\n")
			continue
		}
		sort.Slice(missing, func(i, j int) bool { return missing[i].name < missing[j].name })
		fmt.Printf("  missing from the archive:\n")
		for _, m := range missing {
			note := ""
			if m.unique {
				note = "   (unique: uniqueness across the tiers is the key registry's job," +
					" so this one is only about speed)"
			}
			if m.ttl {
				note = "   (TTL: do NOT create this one on the archive, it would delete" +
					" the copies the archive exists to keep)"
			}
			fmt.Printf("    %-34s %s%s\n", m.name, m.spec, note)
			missingTotal++
		}
		fmt.Printf("  a read that falls through and uses these will scan the archive\n\n")
		toCreate = append(toCreate, nsMissing{ns: ns, missing: missing, bare: bare})
	}

	// Before the index comparison's own verdict, because this one is about data
	// rather than speed, and because two tiers whose indexes agree perfectly can
	// still be losing history to an index they both have.
	printExpiring(toStopExpiring, *commands)

	if missingTotal == 0 {
		fmt.Println("Every index the fast tier has, the archive has too.")
		return nil
	}
	fmt.Printf("%d index(es) exist only on the fast tier.\n\n", missingTotal)
	fmt.Print(`Whether that is a problem depends on what still reaches the archive. A read is
answered there when the fast tier cannot prove it holds the range: an unbounded
query, one reaching past the completeness watermark, or anything at all while
the fast tier is still warming.

Keep an index on the archive when a query that falls through filters or sorts on
its fields. Drop it when nothing does, because that storage is the saving.

The router counts fallback reads per namespace on its dashboard, which is the
number to look at before deciding.
`)
	if *commands {
		printCreateCommands(toCreate)
	}
	return nil
}

// printExpiring says which tiered collections the archive is expiring documents
// of, and how to stop it.
//
// This is the finding that is not about speed. Every other line this command
// prints is about a read being slower than it could be; a TTL index on the
// archive is documents leaving the only tier that was keeping them. The fast
// tier never carries one, so the loss is invisible from there: a read the fast
// tier answers whole never asks the archive, and a read that falls through comes
// back short with no error on it.
//
// It reports, like the rest of the command. An index the application created is
// not ours to drop, and a deployment may want the expiry.
func printExpiring(groups []nsMissing, commands bool) {
	if len(groups) == 0 {
		return
	}
	total := 0
	for _, g := range groups {
		total += len(g.missing)
	}
	fmt.Printf("%d TTL index(es) on the archive are expiring documents of tiered collections.\n\n", total)
	fmt.Print(`The archive is the tier that keeps what the fast tier has stopped holding, and
these delete from it on a schedule. On an adoption they arrive with the cluster:
the application put them on what used to be its only database, and the day that
database became the archive they went on doing what they always did.

Keep them if the expiry is deliberate and the retention they encode is what the
data should get. Drop them if the archive is meant to hold that history, and
remember the fast tier never carries a TTL index, so nothing there is expiring.
`)
	if !commands {
		return
	}
	fmt.Print("\nRun these against the archive to stop the expiry, if that history should be kept:\n\n")
	for _, g := range groups {
		db, coll := splitNamespace(g.ns)
		for _, m := range g.missing {
			fmt.Printf("  db.getSiblingDB(%q).%s.dropIndex(%q)\n", db, coll, m.name)
		}
	}
	fmt.Print(`
Dropping the index does not bring back what has already expired. A document that
was deleted from the archive is gone unless the fast tier still holds it, and the
fast tier holds only what is recent enough to be there.
`)
}

// printCreateCommands writes the createIndex calls for what the archive is
// missing, so the decision stays with the operator and the transcription does
// not.
//
// Two things the fast tier's index carries are deliberately left out, and both
// would be taken along by anyone copying the definitions across by hand.
//
// A TTL index is not emitted at all. The archive exists to keep what the fast
// tier has stopped holding, and a TTL index there would delete exactly that.
// Nothing would report it: the documents would simply stop being there.
//
// unique is not emitted either. Uniqueness across the tiers is the key
// registry's job, and a unique index on the archive can refuse a write the
// feeder has to make: a document deleted and written again under the same
// value collides with the tombstoned copy of the old one, and mirroring stops
// on a collection that is behaving perfectly.
func printCreateCommands(groups []nsMissing) {
	// The fast tier first, because that is the one a new deployment is missing
	// and the one whose scans the application feels.
	//
	// unique is carried here and not to the archive, and the asymmetry is the
	// point. The fast tier holds a subset, and a subset of unique values is
	// unique, so the constraint is satisfiable; more than that, the router
	// reads the fast tier's unique indexes to learn what it must protect across
	// both tiers, so a fast tier without them means nothing is enforcing the
	// application's constraint at all.
	fastPrinted := false
	for _, g := range groups {
		for _, m := range g.bare {
			if m.ttl {
				continue
			}
			if !fastPrinted {
				fmt.Print("\nRun these against the fast tier, which is missing them:\n\n")
				fastPrinted = true
			}
			db, coll := splitNamespace(g.ns)
			opts := fmt.Sprintf("{name: %q", m.name)
			if m.unique {
				opts += ", unique: true"
			}
			fmt.Printf("  db.getSiblingDB(%q).%s.createIndex(%s, %s})\n",
				db, coll, m.keyJSON, opts)
		}
	}
	if fastPrinted {
		fmt.Print(`
The warmer builds these itself when it fills a namespace, so this list is for
collections it has no rule for. A TTL index is never listed: expiring documents
on the fast tier would have the feeder read those expiries as application
deletes and tombstone the archive's copies, so an index whose job is to remove
data would reach into the tier that exists to keep it.
`)
	}

	printed := false
	for _, g := range groups {
		for _, m := range g.missing {
			if m.ttl {
				continue
			}
			if !printed {
				fmt.Print("\nRun these against the archive to create what it is missing:\n\n")
				printed = true
			}
			db, coll := splitNamespace(g.ns)
			fmt.Printf("  db.getSiblingDB(%q).%s.createIndex(%s, {name: %q})\n",
				db, coll, m.keyJSON, m.name)
		}
	}
	if !printed {
		return
	}
	fmt.Print(`
Neither unique nor expireAfterSeconds is carried across, on purpose. Uniqueness
between the tiers is the key registry's job, and a unique index on the archive
can refuse a write the feeder has to make. A TTL index is not listed at all: the
archive keeps what the fast tier has stopped holding, and expiring it there
would delete exactly that.

Build them with the archive under no other load, and remember that an index the
archive does not need is storage the tiering was supposed to save.
`)
}

// nsMissing is one namespace's indexes that the archive does not have.
type nsMissing struct {
	ns      string
	missing []indexInfo // the archive does not have these
	bare    []indexInfo // the fast tier does not have these
}

// absentFrom returns the indexes in have that want does not carry.
//
// Matched on the key pattern as well as the name, because the same index under
// a different name is still that index.
func absentFrom(have, want map[string]indexInfo) []indexInfo {
	var out []indexInfo
	for _, h := range have {
		if h.name == "_id_" {
			continue
		}
		if _, ok := want[h.name]; ok {
			continue
		}
		found := false
		for _, w := range want {
			if w.spec == h.spec {
				found = true
				break
			}
		}
		if !found {
			out = append(out, h)
		}
	}
	return out
}

// indexesOf reads one collection's indexes from one tier.
func indexesOf(ctx context.Context, client *mongo.Client, db, coll string) (map[string]indexInfo, error) {
	cur, err := client.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	out := map[string]indexInfo{}
	for cur.Next(ctx) {
		var raw bson.Raw
		if derr := cur.Decode(&raw); derr != nil {
			continue
		}
		name, _ := raw.Lookup("name").StringValueOK()
		if name == "" {
			continue
		}
		info := indexInfo{name: name, spec: keyPattern(raw), keyJSON: keyPatternJSON(raw)}
		if u, ok := raw.Lookup("unique").BooleanOK(); ok && u {
			info.unique = true
		}
		if v, err := raw.LookupErr("expireAfterSeconds"); err == nil {
			info.ttl = true
			if secs, ok := v.AsInt64OK(); ok {
				info.ttlSecs = float64(secs)
			} else if f, ok := v.DoubleOK(); ok {
				info.ttlSecs = f
			}
		}
		out[name] = info
	}
	return out, cur.Err()
}

// keyPattern renders an index's keys the way a person writes them.
func keyPattern(raw bson.Raw) string {
	key, err := raw.LookupErr("key")
	if err != nil {
		return "{}"
	}
	doc, ok := key.DocumentOK()
	if !ok {
		return "{}"
	}
	elems, err := doc.Elements()
	if err != nil {
		return "{}"
	}
	parts := make([]string, 0, len(elems))
	for _, e := range elems {
		parts = append(parts, fmt.Sprintf("%s: %s", e.Key(), keyDirection(e.Value())))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// keyPatternJSON renders the key pattern so it can be pasted into a command.
//
// Field names are quoted, which keyPattern does not do because it is meant to
// be read. An index on a nested path has a dot in its field name, and
// {a.b: 1} is not valid JavaScript.
func keyPatternJSON(raw bson.Raw) string {
	key, err := raw.LookupErr("key")
	if err != nil {
		return "{}"
	}
	doc, ok := key.DocumentOK()
	if !ok {
		return "{}"
	}
	elems, err := doc.Elements()
	if err != nil {
		return "{}"
	}
	parts := make([]string, 0, len(elems))
	for _, e := range elems {
		parts = append(parts, fmt.Sprintf("%q: %s", e.Key(), keyDirection(e.Value())))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// keyDirection renders an index key's value the way somebody would type it.
//
// RawValue.String gives extended JSON, so a plain ascending index came out as
// {field: {"$numberInt":"1"}}, which is not what anyone wrote or wants to read.
func keyDirection(v bson.RawValue) string {
	switch v.Type {
	case bson.TypeInt32:
		if n, ok := v.Int32OK(); ok {
			return fmt.Sprintf("%d", n)
		}
	case bson.TypeInt64:
		if n, ok := v.Int64OK(); ok {
			return fmt.Sprintf("%d", n)
		}
	case bson.TypeDouble:
		if f, ok := v.DoubleOK(); ok {
			return strconv.FormatFloat(f, 'f', -1, 64)
		}
	case bson.TypeString:
		// "text", "2dsphere", "hashed" and friends.
		if s, ok := v.StringValueOK(); ok {
			return `"` + s + `"`
		}
	}
	return v.String()
}

func splitNamespace(ns string) (string, string) {
	i := strings.Index(ns, ".")
	if i <= 0 || i == len(ns)-1 {
		return "", ""
	}
	return ns[:i], ns[i+1:]
}
