package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func rawDoc(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// This is the test that guards the whole proxy.
//
// A driver uses the hello reply for topology discovery. If any replica set
// membership field survives, the driver reads it, connects straight to those
// members, and never sends the router another byte. Everything looks healthy
// until an archived document comes back missing, which is the hardest possible
// failure to trace back to its cause.
func TestMongosIdentityHidesTheReplicaSet(t *testing.T) {
	realReply := rawDoc(t, bson.D{
		{Key: "topologyVersion", Value: bson.D{{Key: "counter", Value: int64(1)}}},
		{Key: "hosts", Value: bson.A{"rs-fast-1:27017", "rs-fast-2:27017", "rs-fast-3:27017"}},
		{Key: "setName", Value: "rsFast"},
		{Key: "setVersion", Value: int32(1)},
		{Key: "isWritablePrimary", Value: true},
		{Key: "secondary", Value: false},
		{Key: "primary", Value: "rs-fast-1:27017"},
		{Key: "me", Value: "rs-fast-1:27017"},
		{Key: "electionId", Value: bson.NewObjectID()},
		{Key: "lastWrite", Value: bson.D{{Key: "opTime", Value: int64(7)}}},
		{Key: "maxBsonObjectSize", Value: int32(16777216)},
		{Key: "maxWireVersion", Value: int32(21)},
		{Key: "minWireVersion", Value: int32(0)},
		{Key: "ok", Value: 1.0},
	})

	out, err := mongosIdentity(realReply)
	if err != nil {
		t.Fatalf("mongosIdentity: %v", err)
	}
	got := rawDoc(t, out)

	// Anything that would let a driver find the real members has to be gone.
	leaks := []string{
		"hosts", "setName", "setVersion", "primary", "me",
		"secondary", "electionId", "lastWrite", "topologyVersion",
	}
	for _, field := range leaks {
		if _, err := got.LookupErr(field); err == nil {
			t.Errorf("hello reply still carries %q: a driver would bypass the router entirely", field)
		}
	}

	msg, err := got.LookupErr("msg")
	if err != nil {
		t.Fatal("hello reply has no msg field, so the driver will not treat this as a mongos")
	}
	if s, _ := msg.StringValueOK(); s != "isdbgrid" {
		t.Errorf(`msg = %q, want "isdbgrid"`, s)
	}
}

// The reply has to stay truthful about what the server can actually do. Those
// values come from the real server rather than being guessed here, so they must
// survive the rewrite intact.
func TestMongosIdentityKeepsServerCapabilities(t *testing.T) {
	reply := rawDoc(t, bson.D{
		{Key: "maxBsonObjectSize", Value: int32(16777216)},
		{Key: "maxMessageSizeBytes", Value: int32(48000000)},
		{Key: "maxWriteBatchSize", Value: int32(100000)},
		{Key: "maxWireVersion", Value: int32(21)},
		{Key: "minWireVersion", Value: int32(0)},
		{Key: "logicalSessionTimeoutMinutes", Value: int32(30)},
		{Key: "isWritablePrimary", Value: true},
		{Key: "ok", Value: 1.0},
	})

	out, err := mongosIdentity(reply)
	if err != nil {
		t.Fatalf("mongosIdentity: %v", err)
	}
	got := rawDoc(t, out)

	for _, field := range []string{
		"maxBsonObjectSize", "maxMessageSizeBytes", "maxWriteBatchSize",
		"maxWireVersion", "minWireVersion", "logicalSessionTimeoutMinutes",
		"isWritablePrimary", "ok",
	} {
		if _, err := got.LookupErr(field); err != nil {
			t.Errorf("rewrite dropped %q, which the driver needs to talk to us correctly", field)
		}
	}
}

// A server that already sends msg, such as a real mongos, must not end up with
// two of them.
func TestMongosIdentityReplacesExistingMsg(t *testing.T) {
	reply := rawDoc(t, bson.D{
		{Key: "msg", Value: "something else"},
		{Key: "ok", Value: 1.0},
	})
	out, err := mongosIdentity(reply)
	if err != nil {
		t.Fatalf("mongosIdentity: %v", err)
	}

	var count int
	for _, e := range out {
		if e.Key == "msg" {
			count++
			if e.Value != "isdbgrid" {
				t.Errorf("msg = %v, want isdbgrid", e.Value)
			}
		}
	}
	if count != 1 {
		t.Errorf("reply carries %d msg fields, want exactly 1", count)
	}
}

func TestIsHello(t *testing.T) {
	for _, name := range []string{"hello", "isMaster", "ismaster"} {
		if !isHello(name) {
			t.Errorf("isHello(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"find", "insert", "helloWorld", ""} {
		if isHello(name) {
			t.Errorf("isHello(%q) = true, want false", name)
		}
	}
}
