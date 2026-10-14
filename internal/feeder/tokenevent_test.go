package feeder

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
)

func mustRaw(t *testing.T, v any) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A checkpoint has to say which event its token names, or a feeder restarted
// with that namespace removed cannot tell a filtered event from a lost one.
func TestTokenEvent(t *testing.T) {
	tok := mustRaw(t, bson.D{{Key: "_data", Value: "826ACFD3FC0000000104"}})
	ev := mustRaw(t, bson.D{
		{Key: "_id", Value: tok},
		{Key: "operationType", Value: "insert"},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "app"}, {Key: "coll", Value: "b"}}},
		{Key: "clusterTime", Value: bson.Timestamp{T: 1792000000, I: 3}},
	})

	ns, ts := tokenEvent(ev, tok)
	if ns != "app.b" || ts != (bson.Timestamp{T: 1792000000, I: 3}) {
		t.Errorf("the event's own token: got (%q, %v), want (app.b, {1792000000 3})", ns, ts)
	}

	// The post-batch token past filtered entries is a different token and
	// names no event; naming this one would send a resume to the wrong place.
	pbrt := mustRaw(t, bson.D{{Key: "_data", Value: "826ACFD40100000001"}})
	if ns, _ := tokenEvent(ev, pbrt); ns != "" {
		t.Errorf("a post-batch token was taken for the event's: got %q", ns)
	}

	// A dropDatabase event has no collection, and a namespace of "app." would
	// match nothing in the config and so always look removed.
	dropDB := mustRaw(t, bson.D{
		{Key: "_id", Value: tok},
		{Key: "operationType", Value: "dropDatabase"},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "app"}}},
		{Key: "clusterTime", Value: bson.Timestamp{T: 1792000000, I: 3}},
	})
	if ns, _ := tokenEvent(dropDB, tok); ns != "" {
		t.Errorf("an event with no collection named %q", ns)
	}
}

func TestMirrorsNS(t *testing.T) {
	all := &Feeder{cfg: config.FeederConfig{}}
	if !all.mirrorsNS("app.anything") {
		t.Error("an empty feeder.namespaces mirrors everything, so nothing is ever removed from it")
	}
	some := &Feeder{cfg: config.FeederConfig{Namespaces: []string{"app.a", "shop.orders"}}}
	if !some.mirrorsNS("shop.orders") || some.mirrorsNS("app.b") || some.mirrorsNS("app") {
		t.Error("a listed namespace has to match exactly")
	}
}
