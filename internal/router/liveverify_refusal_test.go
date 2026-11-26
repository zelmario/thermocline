package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
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
