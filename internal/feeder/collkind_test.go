package feeder

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A clustered collection can carry a TTL, and mongod then removes its own
// oldest documents exactly as a capped collection does. Nothing announces those
// removals, so the feeder reads them as the application deleting and the
// archive tombstones the history it exists to keep.
//
// That is word for word why capped is on this list, so the table is written
// against the whole list rather than the one entry: the five kinds that must be
// refused, the two clustered shapes that separate the TTL from the clustering,
// and an ordinary collection to prove the check is not simply saying no.
func TestUnmirrorableKind(t *testing.T) {
	info := func(d bson.D) bson.Raw {
		raw, err := bson.Marshal(d)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return raw
	}
	clustered := bson.D{{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "name", Value: "_id_"}, {Key: "unique", Value: true}}

	tests := []struct {
		name          string
		info          bson.Raw
		want          bool // must be refused
		wantNamedOnly bool
	}{
		{
			name: "an ordinary collection",
			info: info(bson.D{{Key: "name", Value: "orders"},
				{Key: "options", Value: bson.D{}}}),
			want: false,
		},
		{
			name: "capped",
			info: info(bson.D{{Key: "name", Value: "events"},
				{Key: "options", Value: bson.D{{Key: "capped", Value: true}, {Key: "size", Value: 4096}}}}),
			want: true,
		},
		{
			name: "a time series collection, by type",
			info: info(bson.D{{Key: "name", Value: "metrics"}, {Key: "type", Value: "timeseries"}}),
			want: true, wantNamedOnly: true,
		},
		{
			name: "a time series collection, by option",
			info: info(bson.D{{Key: "name", Value: "metrics"},
				{Key: "options", Value: bson.D{{Key: "timeseries", Value: bson.D{{Key: "timeField", Value: "when"}}}}}}),
			want: true, wantNamedOnly: true,
		},
		{
			name: "a view",
			info: info(bson.D{{Key: "name", Value: "recent"}, {Key: "type", Value: "view"}}),
			want: true, wantNamedOnly: true,
		},
		{
			name: "queryable encryption",
			info: info(bson.D{{Key: "name", Value: "patients"},
				{Key: "options", Value: bson.D{{Key: "encryptedFields", Value: bson.D{}}}}}),
			want: true,
		},
		{
			name: "one of its state collections",
			info: info(bson.D{{Key: "name", Value: "enxcol_.patients.esc"},
				{Key: "options", Value: bson.D{}}}),
			want: true,
		},
		{
			// Clustering on its own is a storage decision. The documents are
			// ordinary documents and mirroring them is ordinary work, so this
			// must not be refused: the archive's copy is simply not clustered,
			// which is the same latitude a plain index gets.
			name: "clustered, with no TTL",
			info: info(bson.D{{Key: "name", Value: "sessions"},
				{Key: "options", Value: bson.D{{Key: "clusteredIndex", Value: clustered}}}}),
			want: false,
		},
		{
			// Measured on PSMDB 8.0.26: created with expireAfterSeconds 1 and
			// _id values an hour old, mongod removed all twenty within 45
			// seconds with nothing else running.
			name: "clustered, with a TTL",
			info: info(bson.D{{Key: "name", Value: "sessions"},
				{Key: "options", Value: bson.D{{Key: "clusteredIndex", Value: clustered},
					{Key: "expireAfterSeconds", Value: int64(1)}}}}),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, why, namedOnly := unmirrorableKind(tt.info)
			if (kind != "") != tt.want {
				t.Fatalf("refused = %v (%q), want %v", kind != "", kind, tt.want)
			}
			if kind == "" {
				return
			}
			if why == "" {
				t.Errorf("%q is refused with no reason, and the reason is what the operator acts on", kind)
			}
			if namedOnly != tt.wantNamedOnly {
				t.Errorf("namedOnly = %v, want %v", namedOnly, tt.wantNamedOnly)
			}
		})
	}
}
