package main

import (
	"encoding/json"
	"strconv"
)

// The commands cline prints for an operator to paste into mongosh carry
// collection names, index names and key fields read from the archive, and any
// client allowed to create a collection chose them. MongoDB refuses only "$"
// and NUL in a collection name, so a name such as x;db.dropDatabase();db.x
// inserted unquoted, or inside quotes it could close, ran as the operator.

// jsString is s as a JavaScript string literal. JSON is one, for every string,
// which Go's %q is not: it writes \U escapes JavaScript does not have.
func jsString(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		return `""`
	}
	return string(b)
}

// collRef is a collection as mongosh code, quoted however it is spelled.
func collRef(db, coll string) string {
	return "db.getSiblingDB(" + jsString(db) + ").getCollection(" + jsString(coll) + ")"
}

// printable is s with control characters escaped, for a string read from the
// database and printed to the operator's terminal, where an escape sequence
// could rewrite what they see.
func printable(s string) string {
	q := strconv.QuoteToGraphic(s)
	return q[1 : len(q)-1]
}
