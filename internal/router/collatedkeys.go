package router

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// A collation decides which strings are one value. Under en/strength 2,
// Bob@x.com and bob@x.com are one key, and a unique index rejects the second.
//
// The registry derives its id from the value's bytes, and those two spellings
// have different bytes, so it made two keys where mongod makes one. Measured on
// a tiered collection whose archived document held Bob@x.com, the router
// accepted bob@x.com that mongod rejects.
//
// Reproducing the comparison here would mean reimplementing ICU, which is the
// same trap constrains avoids for a partial index's filter. The server is asked
// instead: one indexed lookup against the archive, saying the collation out
// loud, which is the same comparison mongod would have made.
//
// Only an index that carries a collation pays for this, and an index inherits
// its collection's default one, so the spec says so either way.

// collatedArchiveClash reports whether a live archived document other than
// owner already holds this key under idx's collation.
//
// It fails closed the other way from most of this file: if the archive cannot
// be asked, the write is not refused. Rejecting a write because bookkeeping is
// unreachable is a visible failure caused by the proxy, and the case this
// guards is one the fast tier's own index cannot see anyway.
func (r *Router) collatedArchiveClash(ctx context.Context, ns string, idx uniqueIndex, vals []any, owner bson.RawValue) bool {
	if len(idx.collation) == 0 || len(vals) != len(idx.fields) {
		return false
	}

	filter := bson.D{meta.NotArchivedDeleted()}
	for i, f := range idx.fields {
		// A missing field is indexed as null, exactly as keyID encodes it.
		if vals[i] == nil {
			filter = append(filter, bson.E{Key: f, Value: nil})
			continue
		}
		filter = append(filter, bson.E{Key: f, Value: vals[i]})
	}
	if !owner.IsZero() {
		// The document that already holds this value is allowed to keep it.
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$ne", Value: owner}}})
	}

	db, coll := splitNS(ns)
	err := r.cold.Database(db).Collection(coll).FindOne(ctx, filter,
		options.FindOne().
			SetCollation(collationFromSpec(idx.collation)).
			SetProjection(bson.D{{Key: "_id", Value: 1}}),
	).Err()
	switch {
	case err == nil:
		return true
	case errors.Is(err, mongo.ErrNoDocuments):
		return false
	default:
		r.log.Warn("could not ask the archive about a collated unique key, not enforcing it across tiers",
			"ns", ns, "index", idx.name, "err", err,
			"effect", "a duplicate of an archived document could be inserted, spelled differently")
		return false
	}
}

// collationFromSpec turns an index's reported collation into one a query can
// carry. The report includes the version of the collator that built the index,
// which is not something a query may ask for.
func collationFromSpec(spec bson.Raw) *options.Collation {
	var c struct {
		Locale          string `bson:"locale"`
		CaseLevel       bool   `bson:"caseLevel"`
		CaseFirst       string `bson:"caseFirst"`
		Strength        int    `bson:"strength"`
		NumericOrdering bool   `bson:"numericOrdering"`
		Alternate       string `bson:"alternate"`
		MaxVariable     string `bson:"maxVariable"`
		Normalization   bool   `bson:"normalization"`
		Backwards       bool   `bson:"backwards"`
	}
	if err := bson.Unmarshal(spec, &c); err != nil {
		return nil
	}
	return &options.Collation{
		Locale:          c.Locale,
		CaseLevel:       c.CaseLevel,
		CaseFirst:       c.CaseFirst,
		Strength:        c.Strength,
		NumericOrdering: c.NumericOrdering,
		Alternate:       c.Alternate,
		MaxVariable:     c.MaxVariable,
		Normalization:   c.Normalization,
		Backwards:       c.Backwards,
	}
}
