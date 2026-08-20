package feeder

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Two kinds of collection cannot be mirrored, and mirroring them does damage
// rather than nothing, so the feeder refuses to start against either.
//
// A capped collection removes its own oldest documents when it fills. Nobody
// announces those removals, because the evictor did not make them, so the rule
// that tells an eviction from an application delete classifies them as the
// application deleting, and the archive tombstones its copies. The collection
// rolls over exactly as it was designed to and the archive quietly discards the
// history it exists to keep. Under unannounced_deletes: halt the feeder stops
// instead, which is merely useless; under the default it destroys data.
//
// A time series collection stores its documents in a system bucket collection
// rather than the one they were written to, so the change stream describes
// buckets and the archive would fill with a shape the application has never
// seen and cannot read back.
//
// Both are configuration mistakes rather than states to recover from, so this
// fails loudly at startup with the namespaces named. A feeder that started and
// skipped them would be mirroring less than its configuration says, which is
// the kind of quiet difference that is found months later.

// unmirrorable reports the configured namespaces that cannot be mirrored.
func (f *Feeder) unmirrorable(ctx context.Context) ([]string, error) {
	if len(f.cfg.Namespaces) == 0 {
		return nil, nil
	}

	// One listCollections per database, rather than per namespace.
	wanted := map[string]map[string]bool{}
	for _, ns := range f.cfg.Namespaces {
		db, coll := splitNS(ns)
		if db == "" || coll == "" {
			continue
		}
		if wanted[db] == nil {
			wanted[db] = map[string]bool{}
		}
		wanted[db][coll] = true
	}

	// Both tiers, because which one holds the collection depends on where the
	// deployment is in its life. Ordinarily the fast tier is where the
	// application's collections are and the archive's copies are made by the
	// feeder. In an adoption it is the other way round: the existing cluster
	// becomes the archive carrying every collection it ever had, and the fast
	// tier is new and empty, so a capped collection listed in feeder.namespaces
	// was invisible to a check that only looked at the fast tier. Measured: the
	// feeder started and reported it was mirroring the namespace.
	seen := map[string]bool{}
	var bad []string
	for _, tier := range []struct {
		name   string
		client *mongo.Client
	}{{"the fast tier", f.hot}, {"the archive", f.cold}} {
		for db, colls := range wanted {
			cur, err := tier.client.Database(db).ListCollections(ctx, bson.D{})
			if err != nil {
				// Not being able to look is not a reason to refuse to run. The
				// check exists to catch a configuration mistake, and a database
				// that cannot be listed will fail louder elsewhere.
				f.log.Warn("could not check collection types before mirroring",
					"db", db, "tier", tier.name, "err", err)
				continue
			}
			for cur.Next(ctx) {
				var raw bson.Raw
				if cur.Decode(&raw) != nil {
					continue
				}
				name, _ := raw.Lookup("name").StringValueOK()
				if !colls[name] {
					continue
				}
				kind, why := unmirrorableKind(raw)
				if kind == "" {
					continue
				}
				// One complaint per namespace, however many tiers hold it, so
				// the ordinary case does not report the same collection twice.
				ns := db + "." + name
				if seen[ns] {
					continue
				}
				seen[ns] = true
				bad = append(bad, fmt.Sprintf("%s is %s, which %s", ns, kind, why))
			}
			_ = cur.Close(ctx)
		}
	}
	sort.Strings(bad)
	return bad, nil
}

// unmirrorableKind names the problem with one collection, or empty when it has
// none.
func unmirrorableKind(info bson.Raw) (kind, why string) {
	if t, ok := info.Lookup("type").StringValueOK(); ok && t == "view" {
		return "a view",
			"holds no documents of its own, only a pipeline over another collection, so there is " +
				"nothing to mirror and the collection it reads from is what wants tiering"
	}
	if t, ok := info.Lookup("type").StringValueOK(); ok && t == "timeseries" {
		return "a time series collection",
			"stores its documents in a system bucket collection, so what the change stream " +
				"describes is not the collection the application reads"
	}
	opts, err := info.LookupErr("options")
	if err != nil {
		return "", ""
	}
	odoc, ok := opts.DocumentOK()
	if !ok {
		return "", ""
	}
	if capped, ok := odoc.Lookup("capped").BooleanOK(); ok && capped {
		return "a capped collection",
			"removes its own oldest documents when it fills, and nothing announces those, so " +
				"the archive would tombstone them and lose the history it is there to keep"
	}
	if _, terr := odoc.LookupErr("timeseries"); terr == nil {
		return "a time series collection",
			"stores its documents in a system bucket collection, so what the change stream " +
				"describes is not the collection the application reads"
	}
	return "", ""
}

// checkCollectionKinds refuses to run when any configured namespace cannot be
// mirrored.
func (f *Feeder) checkCollectionKinds(ctx context.Context) error {
	bad, err := f.unmirrorable(ctx)
	if err != nil {
		return err
	}
	if len(bad) == 0 {
		return nil
	}
	return fmt.Errorf("these namespaces cannot be mirrored:\n  %s\n"+
		"Remove them from feeder.namespaces. Mirroring them does not fail, it fills the "+
		"archive with something the application cannot use, or empties it as the collection rolls",
		strings.Join(bad, "\n  "))
}
