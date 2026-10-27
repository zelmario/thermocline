// Package indexprobe answers questions about a collection's documents from its
// indexes, where a query would otherwise have to read every document.
package indexprobe

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Leading returns the keys of the plain indexes that lead with field and hold
// every document: not partial, not sparse, and ascending or descending rather
// than hashed, text or geo.
func Leading(ctx context.Context, coll *mongo.Collection, field string) ([]bson.Raw, error) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	var keys []bson.Raw
	for cur.Next(ctx) {
		key, ok := cur.Current.Lookup("key").DocumentOK()
		if !ok {
			continue
		}
		// A partial or sparse index does not hold every document, so its
		// mark speaks only for the ones it holds.
		if _, err := cur.Current.LookupErr("partialFilterExpression"); err == nil {
			continue
		}
		if sp, _ := cur.Current.Lookup("sparse").BooleanOK(); sp {
			continue
		}
		elems, err := key.Elements()
		if err != nil || len(elems) == 0 || elems[0].Key() != field {
			continue
		}
		if _, ok := elems[0].Value().AsInt64OK(); !ok {
			// A hashed, text or 2dsphere index says nothing about arrays the
			// way a plain one does.
			if _, ok := elems[0].Value().DoubleOK(); !ok {
				continue
			}
		}
		keys = append(keys, append(bson.Raw(nil), key...))
	}
	return keys, cur.Err()
}

// NeverArray reports whether coll has an index leading with field that is not
// multikey, which proves no document it holds has an array there, and returns
// that index's key. Any doubt, an error or a plan that does not say, is a no.
func NeverArray(ctx context.Context, coll *mongo.Collection, field string) (bool, bson.Raw) {
	keys, err := Leading(ctx, coll, field)
	if err != nil {
		return false, nil
	}
	for _, key := range keys {
		var plan bson.Raw
		err := coll.Database().RunCommand(ctx, bson.D{
			{Key: "explain", Value: bson.D{
				{Key: "find", Value: coll.Name()},
				{Key: "filter", Value: bson.D{{Key: field, Value: bson.D{{Key: "$exists", Value: true}}}}},
				{Key: "hint", Value: key},
				{Key: "limit", Value: int32(1)},
			}},
			{Key: "verbosity", Value: "queryPlanner"},
		}).Decode(&plan)
		if err != nil {
			continue
		}
		if multi, said := MultikeyIn(plan.Lookup("queryPlanner", "winningPlan")); said && !multi {
			return true, key
		}
	}
	return false, nil
}

// MultikeyIn reads isMultiKey off the index scans of a plan, true if any of
// them is, and says whether any said at all.
func MultikeyIn(v bson.RawValue) (multi, said bool) {
	var elems []bson.RawElement
	switch v.Type {
	case bson.TypeEmbeddedDocument:
		elems, _ = v.Document().Elements()
	case bson.TypeArray:
		vals, _ := v.Array().Values()
		for _, x := range vals {
			m, s := MultikeyIn(x)
			multi, said = multi || m, said || s
		}
		return multi, said
	default:
		return false, false
	}
	for _, e := range elems {
		if e.Key() == "isMultiKey" {
			if b, ok := e.Value().BooleanOK(); ok {
				multi, said = multi || b, true
			}
			continue
		}
		m, s := MultikeyIn(e.Value())
		multi, said = multi || m, said || s
	}
	return multi, said
}
