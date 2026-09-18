package router

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// The other door into a unique index.
//
// createIndexes with unique: true on a tiered collection is walked and claimed
// by guardNewUniqueIndex, because the fast tier's own build sees only the half
// of the data that is here and a duplicate whose other half is in the archive
// would not stop it. collMod reaches the same state by a different route:
//
//	collMod: "orders", index: {keyPattern: {email: 1}, prepareUnique: true}
//	collMod: "orders", index: {keyPattern: {email: 1}, unique: true}
//
// Measured on 6.0.26 before this existed, with one live document in the archive
// duplicating a value the fast tier holds. createIndexes was refused with
// E11000, as it should be. The two collMods above were both accepted, and left
// a unique index over a value held by two live documents with no rows at all in
// the key registry. Every later insert of that value was then accepted too,
// because the registry is what remembers a value the fast tier cannot see.
//
// prepareUnique on its own is not guarded. It stops the fast tier taking new
// duplicates, which is a narrowing rather than a claim, and the conversion is
// the moment the constraint starts being asserted about documents this tier
// cannot see.

// guardCollModUnique claims the keys of a tiered collection when a collMod
// turns one of its indexes unique.
func (c *conn) guardCollModUnique(ctx context.Context, msg *wire.Message, cmd bson.Raw, db string) (*wire.Message, bool) {
	coll, ok := stringField(cmd, "collMod")
	if !ok {
		return nil, false
	}
	ns := db + "." + coll
	if !c.r.cfg.Manages(ns) {
		return nil, false
	}
	spec, asks := collModAsksUnique(cmd)
	if !asks {
		return nil, false
	}

	idx, found := c.r.indexNamedBy(ctx, ns, spec)
	if !found {
		// mongod answers "cannot find index ... for ns ..." better than any
		// guess made here, and a collMod naming an index that is not there
		// changes nothing for it to protect.
		return nil, false
	}
	return c.claimForUniqueIndex(ctx, msg, ns, []uniqueIndex{idx})
}

// collModAsksUnique reports whether this collMod turns an index unique, and
// returns the `index` document that says so.
//
// unique: false is not this. Taking a constraint away needs no claim, and the
// registry's rows are about values rather than about the index that made them
// interesting.
func collModAsksUnique(cmd bson.Raw) (bson.Raw, bool) {
	spec, ok := cmd.Lookup("index").DocumentOK()
	if !ok {
		return nil, false
	}
	u, err := spec.LookupErr("unique")
	if err != nil {
		return nil, false
	}
	if b, bok := u.BooleanOK(); !bok || !b {
		return nil, false
	}
	return spec, true
}

// indexNamedBy finds the index a collMod's `index` document refers to, by name
// or by key pattern, and reads it into the shape the registry works in.
//
// Read from the fast tier rather than from the command, because a collMod
// carries only the change: sparse, partialFilterExpression and collation belong
// to the index as it already exists, and claiming keys without them would claim
// keys for documents the index does not index.
func (r *Router) indexNamedBy(ctx context.Context, ns string, spec bson.Raw) (uniqueIndex, bool) {
	wantName, _ := lookupString(spec, "name")
	wantKey, hasKey := spec.Lookup("keyPattern").DocumentOK()

	db, coll := splitNS(ns)
	cur, err := r.hot.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		return uniqueIndex{}, false
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	for cur.Next(ctx) {
		var found struct {
			Name                    string   `bson:"name"`
			Key                     bson.Raw `bson:"key"`
			Sparse                  bool     `bson:"sparse"`
			PartialFilterExpression bson.Raw `bson:"partialFilterExpression"`
			Collation               bson.Raw `bson:"collation"`
		}
		if derr := cur.Decode(&found); derr != nil {
			continue
		}
		switch {
		case wantName != "" && found.Name == wantName:
		case hasKey && sameKeyPattern(wantKey, found.Key):
		default:
			continue
		}
		elems, kerr := found.Key.Elements()
		if kerr != nil || len(elems) == 0 {
			return uniqueIndex{}, false
		}
		ui := uniqueIndex{
			name:          found.Name,
			sparse:        found.Sparse,
			partial:       found.PartialFilterExpression,
			partialFields: partialFieldNames(found.PartialFilterExpression),
			collation:     found.Collation,
		}
		for _, e := range elems {
			ui.fields = append(ui.fields, e.Key())
		}
		return ui, true
	}
	return uniqueIndex{}, false
}
