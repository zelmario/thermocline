package feeder

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Some kinds of collection cannot be mirrored, and mirroring them does damage
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
// A queryable encryption collection is the third, and it is the quietest of them.
// Its documents copy across the way any binary field would, so mirroring reports
// success; what does not copy is the state mongod keeps beside them, in enxcol_
// collections nothing here tiers. An encrypted read that falls through is then
// answered from an archive holding the documents without the state that makes
// their fields searchable.
//
// Both are configuration mistakes rather than states to recover from, so this
// fails loudly at startup with the namespaces named. A feeder that started and
// skipped them would be mirroring less than its configuration says, which is
// the kind of quiet difference that is found months later.

// unmirrorable reports the configured namespaces that cannot be mirrored.
//
// An empty namespace list means every collection of every database, so the check
// covers every collection of every database. It used to return nothing at all
// there, which left the config that mirrors the most doing the least checking: a
// capped collection anywhere in the deployment was mirrored, and the archive
// tombstoned its history at the first rollover.
func (f *Feeder) unmirrorable(ctx context.Context) ([]string, error) {
	// One listCollections per database, rather than per namespace. A nil set of
	// collections for a database means every collection in it.
	wanted := map[string]map[string]bool{}
	if len(f.cfg.Namespaces) == 0 {
		dbs, err := f.mirroredDatabases(ctx)
		if err != nil {
			// Not being able to look is not a reason to refuse to run, the same
			// as a database that cannot be listed below.
			f.log.Warn("could not list databases to check collection types before mirroring", "err", err)
			return nil, nil
		}
		for _, db := range dbs {
			wanted[db] = nil
		}
	}
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
				// A nil set is every collection, which is what an empty
				// namespace list asks for.
				if colls != nil && !colls[name] {
					continue
				}
				// mongod's own, which the change stream never reports anyway.
				if strings.HasPrefix(name, "system.") {
					continue
				}
				kind, why, namedOnly := unmirrorableKind(raw)
				if kind == "" {
					continue
				}
				// A kind that is only a problem when somebody named it. Nothing
				// of a view or a time series collection is ever mirrored: a view
				// holds no documents, and a time series collection's writes are
				// reported against its system bucket collection, which the
				// change stream and this check both leave alone. Naming one in
				// the config is a mistake, because it says that namespace is
				// being tiered and it never will be. Finding one while mirroring
				// everything is not: it is a collection nobody claimed, and
				// refusing to start over it would make an empty namespace list
				// unusable on any deployment that has a view.
				if namedOnly && len(f.cfg.Namespaces) == 0 {
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

// mirroredDatabases lists the databases an empty namespace list covers, from
// both tiers, minus the ones the feeder never mirrors.
func (f *Feeder) mirroredDatabases(ctx context.Context) ([]string, error) {
	seen := map[string]bool{}
	for _, client := range []*mongo.Client{f.hot, f.cold} {
		names, err := client.ListDatabaseNames(ctx, bson.D{})
		if err != nil {
			return nil, err
		}
		for _, db := range names {
			if skipNamespace(db) {
				continue
			}
			seen[db] = true
		}
	}
	out := make([]string, 0, len(seen))
	for db := range seen {
		out = append(out, db)
	}
	sort.Strings(out)
	return out, nil
}

// unmirrorableKind names the problem with one collection, or empty when it has
// none.
//
// namedOnly separates the two reasons a kind is reported. Some are mirrored and
// do damage: a capped collection loses its history in the archive, an encrypted
// one arrives without the state that makes it searchable. Others are never
// mirrored at all, and the mistake is only in the configuration claiming they
// are. See the caller for why that distinction decides whether an empty
// namespace list refuses to start.
func unmirrorableKind(info bson.Raw) (kind, why string, namedOnly bool) {
	if t, ok := info.Lookup("type").StringValueOK(); ok && t == "view" {
		return "a view",
			"holds no documents of its own, only a pipeline over another collection, so there is " +
				"nothing to mirror and the collection it reads from is what wants tiering", true
	}
	if t, ok := info.Lookup("type").StringValueOK(); ok && t == "timeseries" {
		return "a time series collection",
			"stores its documents in a system bucket collection, so what the change stream " +
				"describes is not the collection the application reads", true
	}
	opts, err := info.LookupErr("options")
	if err != nil {
		return "", "", false
	}
	odoc, ok := opts.DocumentOK()
	if !ok {
		return "", "", false
	}
	if capped, ok := odoc.Lookup("capped").BooleanOK(); ok && capped {
		return "a capped collection",
			"removes its own oldest documents when it fills, and nothing announces those, so " +
				"the archive would tombstone them and lose the history it is there to keep", false
	}
	if _, terr := odoc.LookupErr("timeseries"); terr == nil {
		return "a time series collection",
			"stores its documents in a system bucket collection, so what the change stream " +
				"describes is not the collection the application reads", true
	}
	// A clustered collection with a TTL, which is the capped collection above
	// wearing different clothes. mongod removes its own oldest documents,
	// nothing announces those removals, so the rule that tells an eviction from
	// an application delete calls them the application deleting and the archive
	// tombstones the history it exists to keep.
	//
	// The clustering on its own is not refused, and that is deliberate. It is a
	// storage decision about one tier; the documents are ordinary documents and
	// mirroring them is ordinary work. The archive's copy is simply not
	// clustered, which is the latitude a plain index already gets. Only the
	// expiry destroys anything.
	//
	// Measured on PSMDB 8.0.26: twenty documents whose _id values were dates an
	// hour old, in a collection created with expireAfterSeconds 1, were gone
	// from the fast tier 45 seconds later.
	if _, cerr := odoc.LookupErr("clusteredIndex"); cerr == nil {
		if _, eerr := odoc.LookupErr("expireAfterSeconds"); eerr == nil {
			return "a clustered collection with a TTL",
				"removes its own oldest documents the way a capped collection does, and nothing " +
					"announces those either, so the archive would record them as deletions", false
		}
	}
	// Queryable encryption. The documents themselves would copy across as any
	// other binary would, and that is what makes this worth refusing rather than
	// leaving: what does not copy is the state that makes them searchable. mongod
	// keeps it in companion enxcol_ collections beside the data, nothing here
	// tiers those, and an encrypted read that falls through would be answered
	// from an archive holding documents whose index state was never mirrored.
	if _, eerr := odoc.LookupErr("encryptedFields"); eerr == nil {
		return "encrypted with queryable encryption",
			"keeps the state that makes its fields searchable in companion enxcol_ collections " +
				"that nothing here tiers, so the archive would hold the documents without it", false
	}
	if name, ok := info.Lookup("name").StringValueOK(); ok && strings.HasPrefix(name, "enxcol_") {
		return "queryable encryption's own state collection",
			"belongs to mongod rather than to the application, and tiering it would move the " +
				"state out from under the collection it describes", false
	}
	return "", "", false
}

// kindRecheckEvery bounds how often one namespace's kind is asked again after
// deletes nothing announced.
//
// A minute, because the thing it catches destroys archived history for as long
// as it goes unnoticed, and the cost is one listCollections for a namespace
// that is already producing unannounced deletes. An application deleting its
// own documents pays it too, which is why it is bounded at all.
const kindRecheckEvery = time.Minute

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
	action := "Remove them from feeder.namespaces."
	if len(f.cfg.Namespaces) == 0 {
		// An empty list is "mirror everything", so there is nothing to remove
		// from. Naming the namespaces to mirror is the fix, and it is the
		// configuration a deployment wants anyway.
		action = "feeder.namespaces is empty, which means every collection of every database, " +
			"so these are included. List the namespaces to mirror instead."
	}
	return fmt.Errorf("these namespaces cannot be mirrored:\n  %s\n"+
		"%s Mirroring them does not fail, it fills the "+
		"archive with something the application cannot use, or empties it as the collection rolls",
		strings.Join(bad, "\n  "), action)
}

// checkNamespaceKind refuses a namespace whose kind cannot be mirrored, the
// first time an event for it arrives.
//
// The startup check cannot see a collection that does not exist yet, and that
// is the whole gap: name a namespace nothing has created, or leave
// feeder.namespaces empty so every namespace is named, start the feeder, and
// then create the collection capped. Measured, and it is what the comment at the
// top of this file describes happening: the collection rolls over as it was
// designed to, mongod removes its oldest documents with nothing to announce
// them, and the archive tombstones its copies. The history the archive exists to
// keep, discarded by a check that ran before there was anything to check.
//
// Only the kinds that are mirrored and do damage stop the feeder. That is the
// same distinction unmirrorableKind already draws with namedOnly: a view or a
// time series collection is never mirrored at all, because what the change
// stream carries is the bucket collection rather than the one the application
// writes to, so nothing of theirs reaches here to be stopped.
//
// Stopping is what the startup check does and it is what this does, for the
// reason written above: a feeder that started and skipped a namespace would be
// mirroring less than its configuration says, and the router would answer reads
// of that namespace from an archive holding none of it.
//
// One listCollections per namespace, once, and the fast tier only. That is
// where a collection appears while this runs: the application writes through the
// router to the fast tier, and the archive's side of the same question is what
// the startup check covers, both tiers of it.
//
// A namespace dropped is forgotten, so a collection recreated under the same
// name as a capped one is checked again.
func (f *Feeder) checkNamespaceKind(ctx context.Context, ns string) error {
	if f.hot == nil {
		// Nothing to ask. A feeder with no fast tier client cannot mirror
		// anything either, so there is nothing here to protect.
		return nil
	}
	if f.kindSeen == nil {
		f.kindSeen = map[string]bool{}
	}
	if f.kindSeen[ns] {
		return nil
	}
	db, coll := splitNS(ns)
	cur, err := f.hot.Database(db).ListCollections(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		// Not being able to look is not a reason to stop, the same as the
		// startup check, which warns and carries on. Nothing is remembered, so
		// the next event for this namespace asks again.
		f.log.Warn("could not check what kind of collection this is before mirroring it",
			"ns", ns, "err", err)
		return nil
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	var kind, why string
	var namedOnly bool
	for cur.Next(ctx) {
		var info bson.Raw
		if cur.Decode(&info) != nil {
			continue
		}
		kind, why, namedOnly = unmirrorableKind(info)
	}
	f.kindSeen[ns] = true
	if kind == "" || namedOnly {
		return nil
	}
	// Said here as well as returned, because the error goes up to the daemon
	// and out, and an operator reading the log file is the one who has to act.
	f.log.Error("stopping: a collection this feeder mirrors cannot be mirrored",
		"ns", ns, "kind", kind, "why", why,
		"when", "it was created after this feeder started, so the check at startup could not see it",
		"action", "remove "+ns+" from feeder.namespaces, or do not create it this way; then restart "+
			"the feeder. Its documents are on the fast tier and the archive holds whatever was "+
			"mirrored before this")
	return fmt.Errorf("%w: %s is %s, which %s", ErrUnmirrorableKind, ns, kind, why)
}

// forgetNamespaceKind drops what is remembered about ns, so a collection
// recreated under the same name is checked again.
func (f *Feeder) forgetNamespaceKind(ns string) { delete(f.kindSeen, ns) }

// forgetNamespaceKindsIn does the same for every namespace in one database,
// which is what a dropDatabase removes.
func (f *Feeder) forgetNamespaceKindsIn(db string) {
	prefix := db + "."
	for ns := range f.kindSeen {
		if strings.HasPrefix(ns, prefix) {
			delete(f.kindSeen, ns)
		}
	}
}
