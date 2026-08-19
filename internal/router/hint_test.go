package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func raw(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// A hint arrives as whatever numeric type the driver encoded the direction
// with, so 1 as an int32 and 1 as a double name the same index. Order does not
// get the same latitude: {a:1,b:1} and {b:1,a:1} are two different indexes.
func TestSameKeyPattern(t *testing.T) {
	cases := []struct {
		name string
		a, b bson.D
		want bool
	}{
		{"identical", bson.D{{Key: "opt", Value: int32(1)}}, bson.D{{Key: "opt", Value: int32(1)}}, true},
		{"int32 and double are the same direction",
			bson.D{{Key: "opt", Value: int32(1)}}, bson.D{{Key: "opt", Value: 1.0}}, true},
		{"int64 too",
			bson.D{{Key: "opt", Value: int64(-1)}}, bson.D{{Key: "opt", Value: int32(-1)}}, true},
		{"a different direction is a different index",
			bson.D{{Key: "opt", Value: int32(1)}}, bson.D{{Key: "opt", Value: int32(-1)}}, false},
		{"a different field",
			bson.D{{Key: "opt", Value: int32(1)}}, bson.D{{Key: "other", Value: int32(1)}}, false},
		{"field order matters",
			bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(1)}},
			bson.D{{Key: "b", Value: int32(1)}, {Key: "a", Value: int32(1)}}, false},
		{"a prefix is not the index",
			bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(1)}},
			bson.D{{Key: "a", Value: int32(1)}}, false},
		{"an index kind is named by a string",
			bson.D{{Key: "loc", Value: "2dsphere"}}, bson.D{{Key: "loc", Value: "2dsphere"}}, true},
		{"and a different kind is a different index",
			bson.D{{Key: "loc", Value: "2dsphere"}}, bson.D{{Key: "loc", Value: "text"}}, false},
		// $natural is not an index, so nothing may match it and the hint is
		// left alone.
		{"$natural matches no index",
			bson.D{{Key: "opt", Value: int32(1)}}, bson.D{{Key: "$natural", Value: int32(1)}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameKeyPattern(raw(t, tc.a), raw(t, tc.b)); got != tc.want {
				t.Fatalf("sameKeyPattern = %v, want %v", got, tc.want)
			}
		})
	}
}

// Whether a sparse index's reach can be written down as a filter at all.
//
// "At least one of the fields is present" holds for an index whose key is all
// directions and not for one carrying a geo or text component: measured on
// PSMDB 8.0 with {loc: "2dsphere", n: 1} sparse over three documents, hinting
// it returned the two carrying loc and left out the one with n and no loc, so
// an $or over both fields would have put that document back.
func TestAllDirections(t *testing.T) {
	elems := func(d bson.D) []bson.RawElement {
		e, err := raw(t, d).Elements()
		if err != nil {
			t.Fatalf("elements: %v", err)
		}
		return e
	}
	cases := []struct {
		name string
		key  bson.D
		want bool
	}{
		{"ascending", bson.D{{Key: "a", Value: int32(1)}}, true},
		{"descending", bson.D{{Key: "a", Value: int32(-1)}}, true},
		{"compound directions",
			bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(-1)}}, true},
		{"a double is still a direction", bson.D{{Key: "a", Value: 1.0}}, true},
		{"2dsphere", bson.D{{Key: "loc", Value: "2dsphere"}}, false},
		{"text", bson.D{{Key: "words", Value: "text"}}, false},
		{"hashed", bson.D{{Key: "a", Value: "hashed"}}, false},
		{"one geo component is enough to disqualify a compound",
			bson.D{{Key: "loc", Value: "2dsphere"}, {Key: "n", Value: int32(1)}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := allDirections(elems(tc.key)); got != tc.want {
				t.Fatalf("allDirections = %v, want %v", got, tc.want)
			}
		})
	}
}
