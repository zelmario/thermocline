package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The entry to the guard that closes the other door into a unique index. A
// mutation here was invisible to go test until this existed: only testkeys.sh
// would have caught it, and a suite is a slower thing to learn from.
func TestCollModAsksUnique(t *testing.T) {
	cases := []struct {
		name string
		cmd  bson.D
		want bool
	}{
		{"turning an index unique", bson.D{{Key: "collMod", Value: "t"}, {Key: "index", Value: bson.D{
			{Key: "keyPattern", Value: bson.D{{Key: "email", Value: 1}}}, {Key: "unique", Value: true}}}}, true},
		{"by index name", bson.D{{Key: "collMod", Value: "t"}, {Key: "index", Value: bson.D{
			{Key: "name", Value: "email_1"}, {Key: "unique", Value: true}}}}, true},
		// prepareUnique stops the fast tier taking new duplicates, which is a
		// narrowing rather than a claim about documents it cannot see.
		{"prepareUnique on its own", bson.D{{Key: "collMod", Value: "t"}, {Key: "index", Value: bson.D{
			{Key: "keyPattern", Value: bson.D{{Key: "email", Value: 1}}}, {Key: "prepareUnique", Value: true}}}}, false},
		// Taking a constraint away needs no walk.
		{"unique false", bson.D{{Key: "collMod", Value: "t"}, {Key: "index", Value: bson.D{
			{Key: "name", Value: "email_1"}, {Key: "unique", Value: false}}}}, false},
		{"a TTL change", bson.D{{Key: "collMod", Value: "t"}, {Key: "index", Value: bson.D{
			{Key: "name", Value: "t_1"}, {Key: "expireAfterSeconds", Value: 60}}}}, false},
		{"unique as a string", bson.D{{Key: "collMod", Value: "t"}, {Key: "index", Value: bson.D{
			{Key: "name", Value: "email_1"}, {Key: "unique", Value: "yes"}}}}, false},
		{"a collMod touching no index", bson.D{{Key: "collMod", Value: "t"},
			{Key: "validationLevel", Value: "off"}}, false},
		{"index as a string", bson.D{{Key: "collMod", Value: "t"}, {Key: "index", Value: "email_1"}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := bson.Marshal(c.cmd)
			if err != nil {
				t.Fatal(err)
			}
			if _, got := collModAsksUnique(raw); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}
