package meta

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ReplaceModel writes doc over whatever filter matches, inserting it if
// nothing does, whatever field names the document uses.
//
// MongoDB 5.0 allows field names that start with $ or contain a dot, and an
// insert of one is accepted and mirrored like any other. A replace of one is
// not: mongod reads a replacement's top-level $ field as an update operator and
// refuses it. The feeder mirrors every update by replacing the whole document,
// so the first update to such a document failed its flush, the feeder retried
// it once a second, and nothing after it reached the archive for any namespace.
// Measured on 2026-09-27 with {$price: 5}: inserted and mirrored, then one $set
// on another field stopped mirroring for good.
//
// A pipeline update that replaces the document with its own literal value is
// the form mongod accepts for these names, and it stores the document as
// given. It is used only when the document needs it, because an ordinary
// replace is the cheaper write and is every other document.
func ReplaceModel(filter any, doc bson.Raw) mongo.WriteModel {
	if !hasAwkwardName(doc, 0) {
		return mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(doc).SetUpsert(true)
	}
	return mongo.NewUpdateOneModel().
		SetFilter(filter).
		SetUpdate(mongo.Pipeline{{{Key: "$replaceWith", Value: bson.D{{Key: "$literal", Value: doc}}}}}).
		SetUpsert(true)
}

// hasAwkwardName reports whether any field name in doc, at any depth, starts
// with $ or contains a dot.
func hasAwkwardName(doc bson.Raw, depth int) bool {
	if depth > 200 {
		// Deeper than mongod nests, so nothing real reaches this; the answer
		// that errs safe is the pipeline, which accepts every name.
		return true
	}
	elems, err := doc.Elements()
	if err != nil {
		return true
	}
	for _, e := range elems {
		k := e.Key()
		if strings.HasPrefix(k, "$") || strings.Contains(k, ".") {
			return true
		}
		if inner, ok := nested(e.Value()); ok && hasAwkwardName(inner, depth+1) {
			return true
		}
	}
	return false
}

// nested is the document an element holds, for an embedded document or an
// array, whose elements are named by position and are read the same way.
func nested(v bson.RawValue) (bson.Raw, bool) {
	switch v.Type {
	case bson.TypeEmbeddedDocument, bson.TypeArray:
		return bson.Raw(v.Value), true
	}
	return nil, false
}
