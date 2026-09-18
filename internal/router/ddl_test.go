package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A TTL index on a tiered collection deletes the newest documents and keeps the
// oldest, which is why it is refused. There are three ways to write one and
// this covers all three: the indexes array of a createIndexes, the index
// document of a collMod, and a clustered collection's own expireAfterSeconds,
// which is a top-level field of create and of collMod rather than a property of
// any index in a list.
func TestAsksForTTL(t *testing.T) {
	clustered := bson.D{{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "unique", Value: true}}
	tests := []struct {
		name string
		cmd  bson.D
		on   string
		want bool
	}{
		{
			name: "a plain createIndexes",
			cmd: bson.D{{Key: "createIndexes", Value: "orders"},
				{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "n", Value: 1}}},
					{Key: "name", Value: "n_1"}}}}},
			on: "createIndexes", want: false,
		},
		{
			name: "a createIndexes carrying one",
			cmd: bson.D{{Key: "createIndexes", Value: "orders"},
				{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "when", Value: 1}}},
					{Key: "name", Value: "when_1"}, {Key: "expireAfterSeconds", Value: int64(60)}}}}},
			on: "createIndexes", want: true,
		},
		{
			name: "a collMod turning an index into one",
			cmd: bson.D{{Key: "collMod", Value: "orders"},
				{Key: "index", Value: bson.D{{Key: "name", Value: "when_1"},
					{Key: "expireAfterSeconds", Value: int64(60)}}}},
			on: "collMod", want: true,
		},
		{
			name: "a collMod that changes something else",
			cmd: bson.D{{Key: "collMod", Value: "orders"},
				{Key: "validationLevel", Value: "moderate"}},
			on: "collMod", want: false,
		},
		{
			// The door this table was written for. A clustered collection's TTL
			// is a top-level field, so a check that reads an indexes array or
			// an index document never sees it.
			name: "a create making a clustered collection expire",
			cmd: bson.D{{Key: "create", Value: "sessions"},
				{Key: "clusteredIndex", Value: clustered},
				{Key: "expireAfterSeconds", Value: int64(1)}},
			on: "create", want: true,
		},
		{
			name: "a create making one that does not",
			cmd: bson.D{{Key: "create", Value: "sessions"},
				{Key: "clusteredIndex", Value: clustered}},
			on: "create", want: false,
		},
		{
			name: "a collMod adding one afterwards",
			cmd: bson.D{{Key: "collMod", Value: "sessions"},
				{Key: "expireAfterSeconds", Value: int64(1)}},
			on: "collMod", want: true,
		},
		{
			// "off" takes the expiry away, and it is the command an operator
			// needs to undo this. Refusing it would leave a tiered collection
			// expiring with no way through the router to stop it.
			name: "a collMod turning one off again",
			cmd: bson.D{{Key: "collMod", Value: "sessions"},
				{Key: "expireAfterSeconds", Value: "off"}},
			on: "collMod", want: false,
		},
		{
			name: "an ordinary create",
			cmd:  bson.D{{Key: "create", Value: "orders"}},
			on:   "create", want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := asksForTTL(raw(t, tt.cmd), tt.on); got != tt.want {
				t.Errorf("asksForTTL = %v, want %v", got, tt.want)
			}
		})
	}
}

// The feeder refuses five kinds of collection when it starts. This is the same
// question asked of a client at runtime, and the reachable shape is an adoption:
// the archive holds the collection, the fast tier has never had it, so create
// succeeds where it would otherwise fail with NamespaceExists.
func TestUnmirrorableTarget(t *testing.T) {
	tests := []struct {
		name     string
		cmd      bson.D
		on       string
		wantColl string
	}{
		{
			name: "convertToCapped",
			cmd:  bson.D{{Key: "convertToCapped", Value: "orders"}, {Key: "size", Value: 4096}},
			on:   "convertToCapped", wantColl: "orders",
		},
		{
			name: "cloneCollectionAsCapped names its target, not its source",
			cmd: bson.D{{Key: "cloneCollectionAsCapped", Value: "orders"},
				{Key: "toCollection", Value: "orders_capped"}, {Key: "size", Value: 4096}},
			on: "cloneCollectionAsCapped", wantColl: "orders_capped",
		},
		{
			name: "create, capped",
			cmd:  bson.D{{Key: "create", Value: "orders"}, {Key: "capped", Value: true}, {Key: "size", Value: 4096}},
			on:   "create", wantColl: "orders",
		},
		{
			// mongod declares create's capped option safeBool, so a number is
			// accepted and anything non-zero is true. Measured on PSMDB
			// 8.0.26: each of these made a capped collection, while a guard
			// reading BooleanOK saw no boolean and let the command through.
			name: "create, capped as an int",
			cmd:  bson.D{{Key: "create", Value: "orders"}, {Key: "capped", Value: int32(1)}, {Key: "size", Value: 4096}},
			on:   "create", wantColl: "orders",
		},
		{
			name: "create, capped as a long",
			cmd:  bson.D{{Key: "create", Value: "orders"}, {Key: "capped", Value: int64(1)}, {Key: "size", Value: 4096}},
			on:   "create", wantColl: "orders",
		},
		{
			name: "create, capped as a double",
			cmd:  bson.D{{Key: "create", Value: "orders"}, {Key: "capped", Value: 1.0}, {Key: "size", Value: 4096}},
			on:   "create", wantColl: "orders",
		},
		{
			// Zero is false in every spelling, and a create that says so is an
			// ordinary create.
			name: "create, capped zero",
			cmd:  bson.D{{Key: "create", Value: "orders"}, {Key: "capped", Value: int32(0)}},
			on:   "create", wantColl: "",
		},
		{
			name: "create, capped false",
			cmd:  bson.D{{Key: "create", Value: "orders"}, {Key: "capped", Value: false}},
			on:   "create", wantColl: "",
		},
		{
			name: "create, time series",
			cmd: bson.D{{Key: "create", Value: "metrics"},
				{Key: "timeseries", Value: bson.D{{Key: "timeField", Value: "when"}}}},
			on: "create", wantColl: "metrics",
		},
		{
			name: "create, queryable encryption",
			cmd: bson.D{{Key: "create", Value: "patients"},
				{Key: "encryptedFields", Value: bson.D{{Key: "fields", Value: bson.A{}}}}},
			on: "create", wantColl: "patients",
		},
		{
			name: "create, a view at the tiered name",
			cmd: bson.D{{Key: "create", Value: "orders"},
				{Key: "viewOn", Value: "orders_raw"}},
			on: "create", wantColl: "orders",
		},
		{
			name: "create, an ordinary collection",
			cmd:  bson.D{{Key: "create", Value: "orders"}},
			on:   "create", wantColl: "",
		},
		{
			// Clustering alone is a storage decision and mirrors fine. The TTL
			// that can go with it is handled by asksForTTL, not here.
			name: "create, clustered with no TTL",
			cmd: bson.D{{Key: "create", Value: "sessions"},
				{Key: "clusteredIndex", Value: bson.D{{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
					{Key: "unique", Value: true}}}},
			on: "create", wantColl: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coll, kind, why, ok := unmirrorableTarget(raw(t, tt.cmd), tt.on)
			if tt.wantColl == "" {
				if ok {
					t.Fatalf("named %q as %q, and it is mirrorable", coll, kind)
				}
				return
			}
			if !ok {
				t.Fatalf("named nothing, want %q", tt.wantColl)
			}
			if coll != tt.wantColl {
				t.Errorf("named %q, want %q", coll, tt.wantColl)
			}
			if kind == "" {
				t.Errorf("named %q with no kind, and the kind is what the message says", coll)
			}
			if why == "" {
				t.Errorf("named %q as %q with no reason, and the reason is what the operator acts on", coll, kind)
			}
		})
	}
}

// A TTL index is one whose expireAfterSeconds is a number. mongod refuses any
// other type with "TTL index 'expireAfterSeconds' option must be numeric", and
// reading the field's presence alone had this system answer a malformed
// createIndexes with a refusal about its own TTL policy, which sends an
// operator looking for a policy when the command is simply wrong.
func TestAsksForTTLWantsANumber(t *testing.T) {
	spec := func(v interface{}) bson.D {
		return bson.D{
			{Key: "createIndexes", Value: "t"},
			{Key: "indexes", Value: bson.A{bson.D{
				{Key: "key", Value: bson.D{{Key: "d", Value: 1}}},
				{Key: "name", Value: "d_ttl"},
				{Key: "expireAfterSeconds", Value: v},
			}}},
		}
	}
	cases := []struct {
		name string
		cmd  bson.D
		want bool
	}{
		{"an int", spec(int32(60)), true},
		{"a long", spec(int64(60)), true},
		{"a double", spec(60.5), true},
		{"a string", spec("x"), false},
		{"a boolean", spec(true), false},
		{"a document", spec(bson.D{{Key: "a", Value: 1}}), false},
		{"no expireAfterSeconds at all", bson.D{
			{Key: "createIndexes", Value: "t"},
			{Key: "indexes", Value: bson.A{bson.D{
				{Key: "key", Value: bson.D{{Key: "d", Value: 1}}},
				{Key: "name", Value: "d_1"}}}}}, false},
		{"a non-array indexes", bson.D{
			{Key: "createIndexes", Value: "t"}, {Key: "indexes", Value: 5}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := bson.Marshal(c.cmd)
			if err != nil {
				t.Fatal(err)
			}
			if got := asksForTTL(raw, "createIndexes"); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}
