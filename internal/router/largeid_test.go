package router

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func idDoc(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func idVal(t *testing.T, d bson.D) bson.RawValue {
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: idDoc(t, d)}
}

// Every way a statement can write an _id, at one byte either side of the limit.
func TestAnIDPastTheLimitIsFoundWhereverItIsWritten(t *testing.T) {
	// A string's value is its length, the bytes and a terminator.
	big := strings.Repeat("i", maxTieredIDBytes-4)
	fits := strings.Repeat("i", maxTieredIDBytes-5)
	for _, tc := range []struct {
		name   string
		u      bson.D
		filter bson.D
		upsert bool
	}{
		{"replacement", bson.D{{Key: "_id", Value: big}, {Key: "v", Value: 1}}, nil, false},
		{"$set", bson.D{{Key: "$set", Value: bson.D{{Key: "_id", Value: big}}}}, nil, true},
		{"$setOnInsert", bson.D{{Key: "$setOnInsert", Value: bson.D{{Key: "_id", Value: big}}}}, nil, true},
		{"upsert filter", bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: 1}}}}, bson.D{{Key: "_id", Value: big}}, true},
	} {
		var f bson.Raw
		if tc.filter != nil {
			f = idDoc(t, tc.filter)
		}
		if _, bad := largeIDInUpdate(idVal(t, tc.u), f, tc.upsert); !bad {
			t.Errorf("%s: an _id of %d bytes was let through", tc.name, len(big)+5)
		}
	}
	if _, bad := largeIDInDoc(idDoc(t, bson.D{{Key: "_id", Value: fits}})); bad {
		t.Errorf("an _id of exactly %d bytes was refused", maxTieredIDBytes)
	}
	// A filter only writes when it upserts.
	if _, bad := largeIDInUpdate(idVal(t, bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: 1}}}}),
		idDoc(t, bson.D{{Key: "_id", Value: big}}), false); bad {
		t.Errorf("a filter that does not upsert was refused")
	}
}
