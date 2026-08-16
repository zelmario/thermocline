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

	"github.com/zelmar/thermocline/internal/config"
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
}

func runIndexes(args []string) error {
	fs := flag.NewFlagSet("indexes", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	only := fs.String("ns", "", "one namespace as db.collection, default every managed one")
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

		var missing []indexInfo
		for _, h := range hotIdx {
			if _, ok := coldIdx[h.name]; ok {
				continue
			}
			// Match on the key pattern too: the same index under a different
			// name is still that index.
			found := false
			for _, c := range coldIdx {
				if c.spec == h.spec {
					found = true
					break
				}
			}
			if !found {
				missing = append(missing, h)
			}
		}

		fmt.Printf("%s\n", ns)
		fmt.Printf("  fast tier %2d index(es), archive %2d\n", len(hotIdx), len(coldIdx))
		if len(missing) == 0 {
			fmt.Printf("  the archive can answer anything the fast tier can\n\n")
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
			fmt.Printf("    %-34s %s%s\n", m.name, m.spec, note)
			missingTotal++
		}
		fmt.Printf("  a read that falls through and uses these will scan the archive\n\n")
	}

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
	return nil
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
		info := indexInfo{name: name, spec: keyPattern(raw)}
		if u, ok := raw.Lookup("unique").BooleanOK(); ok && u {
			info.unique = true
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
