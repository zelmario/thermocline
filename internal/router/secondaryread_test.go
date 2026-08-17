package router

import (
	"io"
	"log/slog"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/wire"
)

// The routing rules, tested here rather than in a suite because a compliant
// driver cannot produce some of the commands they exist to refuse.
//
// The transaction case is the clearest example. A driver rejects a transaction
// on a non-primary read preference before anything reaches the wire, with
// "Read preference in a transaction must be primary, not: secondary", so a live
// suite trying to prove the guard measures nothing at all: no query runs, the
// profiler records nothing, and the check reports that the read landed nowhere.
// That is not the guard working, it is the test never happening, and the two
// look identical from outside.
//
// So the command is built by hand here, exactly as a client that did not check
// would send it.

func testConn(terminate bool) *conn {
	return &conn{
		r:   &Router{cfg: config.RouterConfig{TerminateAuth: terminate}},
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func msgOf(t *testing.T, doc bson.D) *wire.Message {
	t.Helper()
	m, err := wire.NewMsg(0, 0, doc)
	if err != nil {
		t.Fatalf("build message: %v", err)
	}
	return m
}

// secondaryRead is an ordinary read asking for a secondary, which is the one
// shape that should move.
func secondaryRead() bson.D {
	return bson.D{
		{Key: "find", Value: "orders"},
		{Key: "filter", Value: bson.D{}},
		{Key: "$db", Value: "shop"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
	}
}

func TestAnOrdinarySecondaryReadIsRouted(t *testing.T) {
	c := testConn(true)
	if !c.routeToSecondary(msgOf(t, secondaryRead()), "find") {
		t.Fatal("a plain read asking for a secondary was kept on the primary")
	}
}

// Everything below must stay on the primary. Getting one of these wrong does
// not make a slow system, it makes a transaction that half happened on one node.

func TestTransactionalReadStaysOnThePrimary(t *testing.T) {
	doc := secondaryRead()
	doc = append(doc,
		bson.E{Key: "lsid", Value: bson.D{{Key: "id", Value: bson.Binary{Subtype: 0x04, Data: make([]byte, 16)}}}},
		bson.E{Key: "txnNumber", Value: int64(1)},
		bson.E{Key: "autocommit", Value: false},
	)
	c := testConn(true)
	if c.routeToSecondary(msgOf(t, doc), "find") {
		t.Fatal("a read inside a transaction was sent to a secondary, where its own uncommitted writes do not exist")
	}
}

func TestAggregateThatWritesStaysOnThePrimary(t *testing.T) {
	for _, stage := range []string{"$out", "$merge"} {
		t.Run(stage, func(t *testing.T) {
			doc := bson.D{
				{Key: "aggregate", Value: "orders"},
				{Key: "pipeline", Value: bson.A{
					bson.D{{Key: "$match", Value: bson.D{}}},
					bson.D{{Key: stage, Value: "rollup"}},
				}},
				{Key: "$db", Value: "shop"},
				{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
			}
			c := testConn(true)
			if c.routeToSecondary(msgOf(t, doc), "aggregate") {
				t.Fatalf("an aggregation ending in %s was treated as a read", stage)
			}
		})
	}
}

func TestAggregateWithoutAWriteStageIsRouted(t *testing.T) {
	doc := bson.D{
		{Key: "aggregate", Value: "orders"},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{}}}}},
		{Key: "$db", Value: "shop"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
	}
	c := testConn(true)
	if !c.routeToSecondary(msgOf(t, doc), "aggregate") {
		t.Fatal("a plain aggregation asking for a secondary was kept on the primary")
	}
}

func TestPrimaryPreferenceStaysOnThePrimary(t *testing.T) {
	for _, mode := range []string{"primary", "primaryPreferred"} {
		t.Run(mode, func(t *testing.T) {
			doc := bson.D{
				{Key: "find", Value: "orders"},
				{Key: "$db", Value: "shop"},
				{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: mode}}},
			}
			c := testConn(true)
			if c.routeToSecondary(msgOf(t, doc), "find") {
				t.Fatalf("readPreference %s was sent to a secondary", mode)
			}
		})
	}
}

func TestWritesAreNeverRouted(t *testing.T) {
	for _, name := range []string{"insert", "update", "delete", "findAndModify"} {
		t.Run(name, func(t *testing.T) {
			doc := bson.D{
				{Key: name, Value: "orders"},
				{Key: "$db", Value: "shop"},
				{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
			}
			c := testConn(true)
			if c.routeToSecondary(msgOf(t, doc), name) {
				t.Fatalf("%s was sent to a secondary", name)
			}
		})
	}
}

// getMore is excluded here because it is routed by cursor affinity instead: the
// cursor exists on one node and its follow-ups have to go back to that
// connection, whatever the preference says.
func TestGetMoreIsNotRoutedByPreference(t *testing.T) {
	doc := bson.D{
		{Key: "getMore", Value: int64(42)},
		{Key: "collection", Value: "orders"},
		{Key: "$db", Value: "shop"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
	}
	c := testConn(true)
	if c.routeToSecondary(msgOf(t, doc), "getMore") {
		t.Fatal("getMore was routed by preference instead of following its cursor")
	}
}

// With authentication proxied there is no second connection to send anything
// down, so nothing may move however the command is addressed.
func TestNothingIsRoutedWhileAuthenticationIsProxied(t *testing.T) {
	c := testConn(false)
	if c.routeToSecondary(msgOf(t, secondaryRead()), "find") {
		t.Fatal("a read was sent to a secondary without terminated authentication, " +
			"where it could only be authenticated as the router itself")
	}
}
