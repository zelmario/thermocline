package router

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
)

// scratchLeftoverAge is how old a scratch collection has to be before a
// starting router treats it as litter rather than as somebody's work.
//
// Generous on purpose. These are named with the nanosecond the aggregation
// started, so age is readable from the name and no clock has to be trusted
// beyond this one's. But another router on the same fast tier may be running an
// $out right now, and dropping its half-built result would turn a leak into a
// wrong answer. An aggregation that has been assembling for an hour is either
// enormous or abandoned, and the second is far more likely.
const scratchLeftoverAge = time.Hour

// scratchNameRegex matches the names dropLeftoverScratch is allowed to consider.
var scratchNameRegex = "^" + regexp.QuoteMeta(meta.ScratchPrefix)

// scratchAge reports how long ago a scratch collection was created, from the
// nanosecond stamp in its own name, and whether the name carries one at all.
//
// A name without a readable stamp is left alone. It is either from a version
// that named these differently or not ours at all, and dropping a collection on
// a guess about its name is not a trade worth making.
func scratchAge(name string, now time.Time) (time.Duration, bool) {
	if !meta.IsScratch(name) {
		return 0, false
	}
	// tmp.thermocline.out.<unixnano>: the stamp is the last dot-separated part.
	rest := strings.TrimPrefix(name, meta.ScratchPrefix)
	i := strings.LastIndex(rest, ".")
	if i < 0 || i == len(rest)-1 {
		return 0, false
	}
	nanos, err := strconv.ParseInt(rest[i+1:], 10, 64)
	if err != nil || nanos <= 0 {
		return 0, false
	}
	return now.Sub(time.Unix(0, nanos)), true
}

// dropLeftoverScratch removes scratch collections a previous router left behind.
//
// A tiered $out assembles its result in one of these and renames it over the
// target, and every path that can fail drops it. A process that dies does not
// take that path: kill the router mid-aggregation and the collection stays, in
// the application's own database, for ever, because nothing else ever looks at
// it. It is litter rather than a wrong answer, which is why this runs at
// startup rather than on a timer.
func (r *Router) dropLeftoverScratch(ctx context.Context) {
	dbs := map[string]struct{}{}
	for _, ns := range r.cfg.Managed {
		if db, _ := splitNS(ns); db != "" {
			dbs[db] = struct{}{}
		}
	}
	// With nothing configured every namespace is managed, so ask the fast tier
	// which databases there are rather than guessing.
	if len(r.cfg.Managed) == 0 {
		names, err := r.hot.ListDatabaseNames(ctx, bson.D{})
		if err != nil {
			return
		}
		for _, db := range names {
			switch db {
			case "admin", "local", "config", meta.DB:
				continue
			}
			dbs[db] = struct{}{}
		}
	}

	now := time.Now()
	for db := range dbs {
		names, err := r.hot.Database(db).ListCollectionNames(ctx,
			bson.D{{Key: "name", Value: bson.D{{Key: "$regex", Value: scratchNameRegex}}}})
		if err != nil {
			continue
		}
		for _, name := range names {
			age, ok := scratchAge(name, now)
			if !ok || age < scratchLeftoverAge {
				continue
			}
			if derr := r.hot.Database(db).Collection(name).Drop(ctx); derr != nil {
				r.log.Warn("could not remove a scratch collection a previous router left behind",
					"ns", db+"."+name, "age", age.Truncate(time.Minute).String(), "err", derr)
				continue
			}
			r.log.Info("removed a scratch collection a previous router left behind",
				"ns", db+"."+name, "age", age.Truncate(time.Minute).String(),
				"meaning", "an aggregation writing into a tiered collection was interrupted "+
					"before it could rename its result into place")
		}
	}
}
