package meta

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Any user may open a session with the warmer's id: mongod keys it by id and
// uid, so it is that user's own, and its inserts carry the warmer's id. A
// record that names the warmer's uid matches only that uid; one from before
// uids were recorded still matches by id.
func TestRegisterMatchesTheUIDItRecorded(t *testing.T) {
	hotURI := os.Getenv("TC_TEST_HOT")
	if hotURI == "" {
		t.Skip("set TC_TEST_HOT to a fast tier to run the session register test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hot, err := mongo.Connect(options.Client().ApplyURI(hotURI))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hot.Disconnect(context.Background()) }()

	sess, err := hot.StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer sess.EndSession(context.Background())

	uid := SessionUID(ctx, hot, sess)
	if len(uid) != 64 {
		t.Fatalf("SessionUID = %q, want the hex of a 32-byte uid", uid)
	}
	if err := RegisterSession(ctx, hot, sess.ID(), "test", uid); err != nil {
		t.Fatal(err)
	}
	id := SessionHex(sess.ID())
	defer func() {
		_, _ = hot.Database(DB).Collection(Sessions).DeleteOne(context.Background(), bson.D{{Key: "_id", Value: id}})
	}()

	reg := NewSessionRegister(hot)
	if yes, err := reg.Ours(ctx, id, uid); err != nil || !yes {
		t.Fatalf("the registered session with its own uid is not ours (%v)", err)
	}
	other := "00" + uid[2:]
	if yes, _ := reg.Ours(ctx, id, other); yes {
		t.Fatal("another user's session with the same id was taken for ours")
	}

	// A record with no uid, as an older warmer wrote it, matches by id.
	old := SessionHex(sess.ID())[:30] + "ff"
	if _, err := hot.Database(DB).Collection(Sessions).InsertOne(ctx,
		bson.D{{Key: "_id", Value: old}, {Key: "kind", Value: "warmer"}}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = hot.Database(DB).Collection(Sessions).DeleteOne(context.Background(), bson.D{{Key: "_id", Value: old}})
	}()
	if yes, _ := NewSessionRegister(hot).Ours(ctx, old, other); !yes {
		t.Fatal("a record without a uid stopped matching by id")
	}
}
