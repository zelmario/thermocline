package cache

import (
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A query has a shape and a set of parameters, and the two have to be told
// apart before anything can be learned.
//
//	db.users.countDocuments({country: "UY"})
//	db.users.countDocuments({country: "AR"})
//
// are one query asked twice, not two queries. The shape is what makes them the
// same: the command, the collection, the fields, the operators and the
// structure, with every value replaced by a placeholder. The parameters are the
// values that were taken out.
//
// This is what lets the router say "this shape is asked a thousand times an hour
// with forty different countries, and for each country the answer holds still
// for about five minutes". Neither half is visible without the split: shapes
// alone cannot measure stability, and whole commands alone never repeat often
// enough to look repetitive.
//
// Values are hashed rather than kept. What a query asked for is the
// application's business, and a report that quotes it is a report nobody can
// leave running.

// Shape returns the fingerprint of a command and a hash of the values it
// carried.
//
// The fingerprint is stable across runs and readable on purpose: it is what a
// report prints, and an operator has to recognise their own query in it.
func Shape(name, ns string, cmd bson.Raw) (shape string, params uint64) {
	var sb strings.Builder
	sb.WriteString(name)
	sb.WriteByte(' ')
	sb.WriteString(ns)
	h := fnv.New64a()

	elems, err := cmd.Elements()
	if err != nil {
		return sb.String(), 0
	}
	// Sorted, so two drivers that order the same command differently are
	// recognised as asking the same thing.
	keys := make([]string, 0, len(elems))
	byKey := make(map[string]bson.RawValue, len(elems))
	for _, e := range elems {
		k := e.Key()
		if skipInShape[k] || k == name {
			continue
		}
		keys = append(keys, k)
		byKey[k] = e.Value()
	}
	sort.Strings(keys)

	for _, k := range keys {
		sb.WriteByte(' ')
		sb.WriteString(k)
		sb.WriteByte('=')
		writeShape(&sb, h, byKey[k])
	}
	return sb.String(), h.Sum64()
}

// skipInShape are the fields that belong to a conversation rather than to the
// question. Two clients asking the same thing in different sessions are asking
// the same thing.
// SkipInShape reports whether a command field is left out when deciding that
// two commands are the same query.
//
// Exported because the router's cache key has to agree with it. The advisor
// recommends caching a shape; a key that separates what the shape joins makes
// that recommendation unactionable, and the application most affected is the
// ordinary one: every APM agent sets a distinct comment per request, and
// drivers compute a per-call maxTimeMS.
func SkipInShape(key string) bool { return skipInShape[key] }

var skipInShape = map[string]bool{
	"lsid": true, "txnNumber": true, "autocommit": true, "startTransaction": true,
	"$clusterTime": true, "$db": true, "$readPreference": true, "apiVersion": true,
	"apiStrict": true, "apiDeprecationErrors": true, "readConcern": true,
	"writeConcern": true, "comment": true, "maxTimeMS": true, "cursor": true,
}

// writeShape appends the structure of a value and feeds its contents to the
// parameter hash.
func writeShape(sb *strings.Builder, h interface{ Write([]byte) (int, error) }, v bson.RawValue) {
	switch v.Type {
	case bson.TypeEmbeddedDocument:
		doc, ok := v.DocumentOK()
		if !ok {
			sb.WriteString("?")
			return
		}
		elems, err := doc.Elements()
		if err != nil {
			sb.WriteString("?")
			return
		}
		sb.WriteByte('{')
		for i, e := range elems {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(e.Key())
			sb.WriteByte(':')
			writeShape(sb, h, e.Value())
		}
		sb.WriteByte('}')
	case bson.TypeArray:
		arr, ok := v.ArrayOK()
		if !ok {
			sb.WriteString("?")
			return
		}
		vals, err := arr.Values()
		if err != nil {
			sb.WriteString("?")
			return
		}
		sb.WriteByte('[')
		for i, e := range vals {
			if i > 0 {
				sb.WriteByte(',')
			}
			writeShape(sb, h, e)
		}
		sb.WriteByte(']')
	default:
		// A leaf. Its type is part of the shape, because 1 and "1" are
		// different questions; its value is a parameter.
		sb.WriteString("?")
		_, _ = h.Write([]byte(strconv.Itoa(int(v.Type))))
		_, _ = h.Write(v.Value)
	}
}

// ResultHash is the answer a reply carries, without the envelope it arrived in.
//
// Hashing the whole reply does not work, and the first version of this did.
// Every reply carries a $clusterTime and an operationTime, and a cursor reply
// carries a cursor id, all of which change on every execution. A count that has
// returned the same number for an hour therefore looked like a query changing
// several times a second, and the measured stability came out at a millisecond.
// Nothing was ever worth caching, which is the opposite of the finding.
//
// So the batch is what is hashed, or the fields of a plain reply that are the
// answer rather than the bookkeeping.
func ResultHash(reply bson.Raw) uint64 {
	h := fnv.New64a()
	if cur, err := reply.LookupErr("cursor"); err == nil {
		if doc, ok := cur.DocumentOK(); ok {
			for _, field := range []string{"firstBatch", "nextBatch"} {
				if b, berr := doc.LookupErr(field); berr == nil {
					_, _ = h.Write(b.Value)
					return h.Sum64()
				}
			}
		}
	}
	elems, err := reply.Elements()
	if err != nil {
		return 0
	}
	for _, e := range elems {
		if volatileInReply[e.Key()] {
			continue
		}
		_, _ = h.Write([]byte(e.Key()))
		_, _ = h.Write(e.Value().Value)
	}
	return h.Sum64()
}

// volatileInReply changes on every execution and says nothing about the answer.
var volatileInReply = map[string]bool{
	"$clusterTime": true, "operationTime": true, "$configTime": true,
	"$topologyTime": true, "electionId": true, "opTime": true,
	"lastCommittedOpTime": true, "$db": true,
}
