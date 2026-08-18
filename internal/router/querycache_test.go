package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// What may be cached is the whole safety of the feature, so each refusal has a
// case here.
func TestWhatMayBeCached(t *testing.T) {
	cases := []struct {
		name string
		cmd  bson.D
		want bool
	}{
		{"an ordinary count", bson.D{{Key: "count", Value: "orders"}}, true},
		{"an ordinary find", bson.D{{Key: "find", Value: "orders"}}, true},
		{"a write", bson.D{{Key: "insert", Value: "orders"}}, false},
		{"a getMore", bson.D{{Key: "getMore", Value: int64(7)}}, false},
		{"inside a transaction", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "lsid", Value: bson.D{{Key: "id", Value: "x"}}},
			{Key: "txnNumber", Value: int64(1)},
			{Key: "autocommit", Value: false},
		}, false},
		{"a read naming a cluster time", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "readConcern", Value: bson.D{{Key: "afterClusterTime", Value: bson.Timestamp{T: 1, I: 1}}}},
		}, false},
		{"a snapshot read", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "readConcern", Value: bson.D{{Key: "level", Value: "snapshot"}}},
		}, false},
		{"a linearizable read", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "readConcern", Value: bson.D{{Key: "level", Value: "linearizable"}}},
		}, false},
		{"a majority read", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}},
		}, true},
		// A read asking for a secondary may be answered by a member behind the
		// primary, and holding that would hand it to somebody who asked for the
		// primary.
		{"a secondary read", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
		}, false},
		{"a nearest read", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "nearest"}}},
		}, false},
		{"a read that asks for the primary", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "primary"}}},
		}, true},
		{"a collectionless aggregate", bson.D{{Key: "aggregate", Value: int32(1)}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := rawDoc(t, tc.cmd)
			name := tc.cmd[0].Key
			_, ok := cacheableRead(name, raw, "shop")
			if ok != tc.want {
				t.Errorf("cacheable = %v, want %v", ok, tc.want)
			}
		})
	}
}

// What a command changes decides what the cache has to forget, and a command
// that names its collections differently from every other one is where that
// goes wrong.
func TestWhatAWriteInvalidates(t *testing.T) {
	cases := []struct {
		name string
		db   string
		cmd  bson.D
		want []string
	}{
		{"an insert", "shop", bson.D{{Key: "insert", Value: "orders"}}, []string{"shop.orders"}},
		{"a delete", "shop", bson.D{{Key: "delete", Value: "orders"}}, []string{"shop.orders"}},
		{"a drop", "shop", bson.D{{Key: "drop", Value: "orders"}}, []string{"shop.orders"}},
		// renameCollection is run against admin and names both sides in full.
		// Prepending the database again produced "admin.shop.orders", which is
		// nobody's namespace, so neither side was dropped.
		{"a rename", "admin", bson.D{
			{Key: "renameCollection", Value: "shop.orders"},
			{Key: "to", Value: "shop.orders_old"},
		}, []string{"shop.orders", "shop.orders_old"}},
		{"a dropDatabase", "shop", bson.D{{Key: "dropDatabase", Value: 1}}, []string{"shop.*"}},
		{"an $out", "shop", bson.D{
			{Key: "aggregate", Value: "orders"},
			{Key: "pipeline", Value: bson.A{bson.D{{Key: "$out", Value: "rollup"}}}},
		}, []string{"shop.rollup"}},
		{"a read", "shop", bson.D{{Key: "find", Value: "orders"}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := writtenNamespaces(tc.cmd[0].Key, tc.db, rawDoc(t, tc.cmd))
			if len(got) != len(tc.want) {
				t.Fatalf("invalidates %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("invalidates[%d] = %s, want %s", i, got[i], tc.want[i])
				}
			}
		})
	}
}
