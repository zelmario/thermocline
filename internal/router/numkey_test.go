package router

import (
	"math"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// onlyKeyID is the single key a document with no arrays takes, which is what
// every case in this file is about. Production code uses docKeys, because a
// document with an array takes one key per element: see multikey.go.
func onlyKeyID(ns string, idx uniqueIndex, doc bson.Raw) (string, bool) {
	kids, _ := docKeys(ns, idx, doc)
	if len(kids) != 1 {
		return "", false
	}
	return kids[0], true
}

// The same whole number written four ways is one key, because that is what
// mongod's unique index makes of it.
func TestKeyIDIsOneKeyPerWholeNumber(t *testing.T) {
	idx := uniqueIndex{name: "n_1", fields: []string{"n"}}
	dec, err := bson.ParseDecimal128("7.00")
	if err != nil {
		t.Fatal(err)
	}
	want := ""
	for _, v := range []any{int32(7), int64(7), float64(7), dec} {
		raw, err := bson.Marshal(bson.D{{Key: "n", Value: v}})
		if err != nil {
			t.Fatal(err)
		}
		got, ok := onlyKeyID("shop.tknum", idx, bson.Raw(raw))
		if !ok {
			t.Fatalf("%T: no key", v)
		}
		if want == "" {
			want = got
		} else if got != want {
			t.Errorf("%T %v: key %s, want %s", v, v, got, want)
		}
	}
}

// And numbers that are not equal stay apart, including the ones that only look
// close. A key that merged them would refuse a write mongod accepts.
func TestKeyIDKeepsDifferentNumbersApart(t *testing.T) {
	idx := uniqueIndex{name: "n_1", fields: []string{"n"}}
	seen := map[string]any{}
	for _, v := range []any{int64(7), int64(8), float64(7.5), float64(-7), int64(1 << 62)} {
		raw, err := bson.Marshal(bson.D{{Key: "n", Value: v}})
		if err != nil {
			t.Fatal(err)
		}
		got, ok := onlyKeyID("shop.tknum", idx, bson.Raw(raw))
		if !ok {
			t.Fatalf("%v: no key", v)
		}
		if prev, clash := seen[got]; clash {
			t.Errorf("%v and %v share key %s", v, prev, got)
		}
		seen[got] = v
	}
}

// A string that reads as a number is not a number, and a unique index does not
// treat it as one.
func TestKeyIDLeavesStringsAlone(t *testing.T) {
	idx := uniqueIndex{name: "n_1", fields: []string{"n"}}
	var ids []string
	for _, v := range []any{int64(7), "7"} {
		raw, err := bson.Marshal(bson.D{{Key: "n", Value: v}})
		if err != nil {
			t.Fatal(err)
		}
		got, _ := onlyKeyID("shop.tknum", idx, bson.Raw(raw))
		ids = append(ids, got)
	}
	if ids[0] == ids[1] {
		t.Errorf("the number 7 and the string \"7\" share key %s", ids[0])
	}
}

func dec(t *testing.T, s string) bson.Decimal128 {
	t.Helper()
	d, err := bson.ParseDecimal128(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// Each pair measured against a unique index on a plain mongod on 2026-09-27:
// same says mongod refused the second insert, distinct says it took both.
func TestKeyIDComparesNumbersByExactValueAsMongodDoes(t *testing.T) {
	idx := uniqueIndex{name: "n_1", fields: []string{"n"}}
	key := func(v any) string {
		raw, err := bson.Marshal(bson.D{{Key: "n", Value: v}})
		if err != nil {
			t.Fatal(err)
		}
		k, ok := onlyKeyID("shop.tknum", idx, bson.Raw(raw))
		if !ok {
			t.Fatalf("%v: no key", v)
		}
		return k
	}
	nan := math.NaN()
	for _, tc := range []struct {
		a, b any
		same bool
	}{
		{0.5, dec(t, "0.5"), true},
		{0.5, dec(t, "0.5000"), true},
		{dec(t, "0.50"), dec(t, "0.5"), true},
		{2.25, dec(t, "2.250"), true},
		{math.Inf(1), dec(t, "Infinity"), true},
		{math.Inf(-1), dec(t, "-Infinity"), true},
		{nan, dec(t, "NaN"), true},
		{nan, dec(t, "-NaN"), true},
		{1e19, dec(t, "1E+19"), true},
		{0.1, dec(t, "0.1"), false},
		{1e300, dec(t, "1E+300"), false},
		{5e-324, dec(t, "5E-324"), false},
		{0.3, dec(t, "0.3"), false},
		{math.Inf(1), math.Inf(-1), false},
	} {
		if got := key(tc.a) == key(tc.b); got != tc.same {
			t.Errorf("%v and %v: one key=%v, mongod says %v", tc.a, tc.b, got, tc.same)
		}
	}
}

// The _id guard maps the archive's answer back to the batch by the same rule.
func TestIDKeyIsOneKeyPerNumber(t *testing.T) {
	val := func(v any) bson.RawValue {
		ty, b, err := bson.MarshalValue(v)
		if err != nil {
			t.Fatal(err)
		}
		return bson.RawValue{Type: ty, Value: b}
	}
	for _, pair := range [][2]any{{int32(7), int64(7)}, {int64(8), 8.0}, {9.0, dec(t, "9")}, {0.5, dec(t, "0.5")}} {
		if idKey(val(pair[0])) != idKey(val(pair[1])) {
			t.Errorf("%v and %v are one _id to mongod and two here", pair[0], pair[1])
		}
	}
	if idKey(val("7")) == idKey(val(int32(7))) {
		t.Error("a string that reads as a number is not that number")
	}
}
