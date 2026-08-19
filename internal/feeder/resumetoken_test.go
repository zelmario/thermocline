package feeder

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/checkpoint"
)

func tokenRaw(t *testing.T, data string) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "_data", Value: data}})
	if err != nil {
		t.Fatalf("build token: %v", err)
	}
	return raw
}

// TestClusterTimeFromRealToken uses a token captured from a PSMDB 8.0 primary
// on 2026-08-19, alongside the operationTime that same server reported for the
// same moment: T=1787147262 I=3. The token reads one increment past it, which
// is MongoDB saying "nothing exists up to here" about an empty batch, and is
// exactly the claim the feeder needs to make.
func TestClusterTimeFromRealToken(t *testing.T) {
	ts, ok := clusterTimeFromToken(tokenRaw(t, "826A85B3FE000000042B0429296E1404"))
	if !ok {
		t.Fatal("a token from a real server was not decoded")
	}
	if ts.T != 1787147262 || ts.I != 4 {
		t.Fatalf("got T=%d I=%d, want T=1787147262 I=4", ts.T, ts.I)
	}
}

func TestClusterTimeFromTokenRefusesWhatItCannotRead(t *testing.T) {
	cases := []struct {
		name string
		tok  bson.Raw
	}{
		{"no token at all", nil},
		{"a document without _data", func() bson.Raw {
			raw, _ := bson.Marshal(bson.D{{Key: "other", Value: 1}})
			return raw
		}()},
		{"shorter than a timestamp", tokenRaw(t, "826A85B3FE")},
		{"not hex", tokenRaw(t, "zzzzzzzzzzzzzzzzzz")},
		// The one that matters for the future: a token whose first field is
		// not a timestamp. Reading four bytes out of it anyway would put a
		// fabricated instant in front of a read that waits on it.
		{"an unknown format", tokenRaw(t, "8100000000000000012B04")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if ts, ok := clusterTimeFromToken(tc.tok); ok {
				t.Fatalf("decoded %v from a token it should have refused", ts)
			}
		})
	}
}

// TestClusterTimeFromTokenOrdersLikeMongoDB is the property the barrier rests
// on: later tokens must decode to later cluster times, with the increment
// breaking ties inside one second. A comparison that ignored the increment
// would treat every event in the same second as simultaneous, which is the
// rounding that made the position appear to move backwards before.
func TestClusterTimeFromTokenOrdersLikeMongoDB(t *testing.T) {
	earlier, ok1 := clusterTimeFromToken(tokenRaw(t, "826A85B3FE000000032B0429296E1404"))
	later, ok2 := clusterTimeFromToken(tokenRaw(t, "826A85B3FE000000042B0429296E1404"))
	nextSecond, ok3 := clusterTimeFromToken(tokenRaw(t, "826A85B3FF000000012B0429296E1404"))
	if !ok1 || !ok2 || !ok3 {
		t.Fatal("one of the tokens did not decode")
	}
	if !(checkpoint.PackTS(earlier) < checkpoint.PackTS(later)) {
		t.Errorf("increment does not order: %v then %v", earlier, later)
	}
	if !(checkpoint.PackTS(later) < checkpoint.PackTS(nextSecond)) {
		t.Errorf("seconds do not order: %v then %v", later, nextSecond)
	}
}
