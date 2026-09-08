package router

import (
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
