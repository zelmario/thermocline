package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// What the archive is asked for when a read falls through to it. Getting this
// wrong is silent in both directions: too weak and a caller who asked for
// majority gets an answer that can be rolled back, too strong and a read fails
// for a guarantee nobody asked for.
func TestArchiveLevel(t *testing.T) {
	cases := []struct {
		name         string
		cmd          bson.D
		wantLevel    string
		wantHonoured bool
	}{
		{"no read concern at all", bson.D{{Key: "find", Value: "orders"}}, "", true},
		{"local", bson.D{{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}}}}, "local", true},
		{"available", bson.D{{Key: "readConcern", Value: bson.D{{Key: "level", Value: "available"}}}}, "available", true},
		// The one that mattered: dropping the level turned a majority read into
		// a local one, which on a single-node archive was a distinction without
		// a difference and on a real one is the difference between an answer
		// that can be rolled back and one that cannot.
		{"majority", bson.D{{Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}}}, "majority", true},
		// Outside a transaction snapshot means a majority-committed snapshot,
		// and majority is what the archive can promise.
		{"snapshot", bson.D{{Key: "readConcern", Value: bson.D{{Key: "level", Value: "snapshot"}}}}, "majority", true},
		// Linearizable is defined for one document on the node that owns it.
		// Answering it from another cluster cannot mean that, and quietly
		// downgrading the strongest guarantee in the system is worse than
		// saying no.
		{"linearizable", bson.D{{Key: "readConcern", Value: bson.D{{Key: "level", Value: "linearizable"}}}}, "", false},
		// A cluster time is a position in the fast tier's oplog and means
		// nothing on the other cluster, so nothing carries over: the level was
		// the default and the default is what the archive gets.
		{"afterClusterTime with no level", bson.D{{Key: "readConcern", Value: bson.D{
			{Key: "afterClusterTime", Value: bson.Timestamp{T: 1, I: 1}}}}}, "", true},
		{"a level this does not know", bson.D{{Key: "readConcern", Value: bson.D{{Key: "level", Value: "future"}}}}, "", true},
		{"a readConcern that is not a document", bson.D{{Key: "readConcern", Value: 7}}, "", true},
	}
	for _, c := range cases {
		b, err := bson.Marshal(c.cmd)
		if err != nil {
			t.Fatalf("%s: marshal: %v", c.name, err)
		}
		level, honourable := archiveLevel(bson.Raw(b))
		if level != c.wantLevel || honourable != c.wantHonoured {
			t.Errorf("%s: got (%q, %v), want (%q, %v)", c.name, level, honourable, c.wantLevel, c.wantHonoured)
		}
	}
}
