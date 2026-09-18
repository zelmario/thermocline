package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// count and distinct read a query the way find reads a filter, and an
// unreadable one was read as no query at all, so both were answered from the
// archive unbounded where mongod refuses the command.
func TestUnreadableQuery(t *testing.T) {
	cases := []struct {
		name string
		cmd  bson.D
		want bool
	}{
		{"an ordinary query", bson.D{{Key: "count", Value: "t"},
			{Key: "query", Value: bson.D{{Key: "k", Value: 1}}}}, false},
		{"an empty query", bson.D{{Key: "count", Value: "t"}, {Key: "query", Value: bson.D{}}}, false},
		// Absent is a count of everything, which this path answers.
		{"no query at all", bson.D{{Key: "count", Value: "t"}}, false},
		{"a query that is a number", bson.D{{Key: "count", Value: "t"}, {Key: "query", Value: 5}}, true},
		{"a query that is a string", bson.D{{Key: "count", Value: "t"}, {Key: "query", Value: "k"}}, true},
		{"a query that is an array", bson.D{{Key: "count", Value: "t"}, {Key: "query", Value: bson.A{1}}}, true},
		{"a query that is null", bson.D{{Key: "count", Value: "t"}, {Key: "query", Value: nil}}, true},
		{"a distinct with a bad query", bson.D{{Key: "distinct", Value: "t"},
			{Key: "key", Value: "k"}, {Key: "query", Value: true}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := bson.Marshal(c.cmd)
			if err != nil {
				t.Fatal(err)
			}
			if got := unreadableQuery(raw); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}
