package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/livecache"
)

func TestProxyRefusal(t *testing.T) {
	reply := func(msg string) bson.Raw {
		b, _ := bson.Marshal(bson.D{{Key: "ok", Value: 0.0}, {Key: "errmsg", Value: msg}, {Key: "code", Value: 125}})
		return b
	}
	cases := []struct {
		msg  string
		want bool
	}{
		{errArchiveBehind.Error(), true},
		{"thermocline: refusing this delete because the fast tier matched nothing", true},
		{"not primary and secondaryOk=false", false},
		{"unknown operator: $foo", false},
		{"Thermocline: not ours", false},
	}
	for _, tc := range cases {
		if got := proxyRefusal(reply(tc.msg)); got != tc.want {
			t.Errorf("%q: got %v, want %v", tc.msg, got, tc.want)
		}
	}
	noMsg, _ := bson.Marshal(bson.D{{Key: "ok", Value: 0.0}})
	if proxyRefusal(noMsg) {
		t.Error("a reply without errmsg read as a refusal")
	}
}

func TestJudgeTimes(t *testing.T) {
	ts := func(i uint32) bson.Timestamp { return bson.Timestamp{T: 100, I: i} }
	cases := []struct {
		name string
		need bson.Timestamp
		pos  livecache.Position
		want []bson.Timestamp
	}{
		{"applied past need: both", ts(134), livecache.Position{Closed: ts(133), Applied: ts(135)}, []bson.Timestamp{ts(134), ts(135)}},
		{"applied at need: need alone", ts(135), livecache.Position{Closed: ts(133), Applied: ts(135)}, []bson.Timestamp{ts(135)}},
		{"applied before need: need alone", ts(136), livecache.Position{Closed: ts(133), Applied: ts(135)}, []bson.Timestamp{ts(136)}},
	}
	for _, tc := range cases {
		got := judgeTimes(tc.need, tc.pos)
		if len(got) != len(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		}
	}
}
