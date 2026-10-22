package router

import (
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// What each write is held to in the live cache, read off the command. A
// write classified finer than it can change would let the cache answer a
// writer with the state before its own write, so the cases that must stay
// whole matter more than the ones that may not.
func TestClassifyWrite(t *testing.T) {
	msg := func(cmd bson.D) (*wire.Message, bson.Raw) {
		m, err := wire.NewMsg(1, 0, cmd)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := m.CommandDoc()
		if err != nil {
			t.Fatal(err)
		}
		return m, doc
	}
	upd := func(q, u bson.D, extra ...bson.E) bson.D {
		st := bson.D{{Key: "q", Value: q}, {Key: "u", Value: u}}
		return append(st, extra...)
	}
	cases := []struct {
		name string
		cmd  bson.D
		want string
	}{
		{"insert names its documents",
			bson.D{{Key: "insert", Value: "orders"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: 1}, {Key: "customer", Value: "c1"}}}}, {Key: "$db", Value: "shop"}},
			"docs=1"},
		{"an update by _id names it and its fields",
			bson.D{{Key: "update", Value: "products"}, {Key: "updates", Value: bson.A{upd(bson.D{{Key: "_id", Value: "p1"}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "stock", Value: -1}}}, {Key: "$set", Value: bson.D{{Key: "seen.at", Value: 1}}}})}}, {Key: "$db", Value: "shop"}},
			"ids=1 fields=[stock seen]"},
		{"an upsert is whole: it may insert anything",
			bson.D{{Key: "update", Value: "carts"}, {Key: "updates", Value: bson.A{upd(bson.D{{Key: "_id", Value: "c1"}}, bson.D{{Key: "$push", Value: bson.D{{Key: "items", Value: 1}}}}, bson.E{Key: "upsert", Value: true})}}, {Key: "$db", Value: "shop"}},
			"whole"},
		{"an update by another field is whole",
			bson.D{{Key: "update", Value: "orders"}, {Key: "updates", Value: bson.A{upd(bson.D{{Key: "status", Value: "placed"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "shipped"}}}})}}, {Key: "$db", Value: "shop"}},
			"whole"},
		{"a replacement is whole: the old fields are not in the command",
			bson.D{{Key: "update", Value: "products"}, {Key: "updates", Value: bson.A{upd(bson.D{{Key: "_id", Value: "p1"}}, bson.D{{Key: "name", Value: "x"}})}}, {Key: "$db", Value: "shop"}},
			"whole"},
		{"a pipeline update is whole",
			bson.D{{Key: "update", Value: "products"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "p1"}}}, {Key: "u", Value: bson.A{bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: 1}}}}}}}}}, {Key: "$db", Value: "shop"}},
			"whole"},
		{"a delete by _id names it",
			bson.D{{Key: "delete", Value: "carts"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "c1"}}}, {Key: "limit", Value: 1}}}}, {Key: "$db", Value: "shop"}},
			"ids=1 fields=[]"},
		{"a delete by a range is whole",
			bson.D{{Key: "delete", Value: "carts"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: 1}}}}}, {Key: "limit", Value: 0}}}}, {Key: "$db", Value: "shop"}},
			"whole"},
		{"a findAndModify by _id names it and its fields",
			bson.D{{Key: "findAndModify", Value: "orders"}, {Key: "query", Value: bson.D{{Key: "_id", Value: 7}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "x"}}}}}, {Key: "$db", Value: "shop"}},
			"ids=1 fields=[status]"},
		{"a sorted findAndModify by status is whole",
			bson.D{{Key: "findAndModify", Value: "orders"}, {Key: "query", Value: bson.D{{Key: "status", Value: "placed"}}}, {Key: "sort", Value: bson.D{{Key: "at", Value: 1}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "shipped"}}}}}, {Key: "$db", Value: "shop"}},
			"whole"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, doc := msg(tc.cmd)
			name := tc.cmd[0].Key
			note, ok := classifyWrite(m, name, "shop", doc)
			if !ok {
				t.Fatal("not classified at all")
			}
			got := "whole"
			if !note.Whole {
				if len(note.Docs) > 0 {
					got = fmt.Sprintf("docs=%d", len(note.Docs))
				} else {
					got = fmt.Sprintf("ids=%d fields=%v", len(note.IDs), note.Fields)
				}
			}
			if got != tc.want {
				t.Errorf("held as %s, want %s", got, tc.want)
			}
		})
	}
}
