package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The username has to come out of a SCRAM client-first message exactly, because
// it becomes a cache key: reading it wrong keys one user's answers under
// another's name, which is the bypass the key exists to prevent.
func TestScramUsername(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string
		ok      bool
	}{
		{"ordinary", "n,,n=alice,r=rOprNGfwEbeRWgbNEkqO", "alice", true},
		{"a gs2 header naming a channel binding", "y,,n=alice,r=abc", "alice", true},
		{"authzid in the header", "n,a=admin,n=alice,r=abc", "alice", true},
		{"an escaped comma", "n,,n=a=2Cb,r=abc", "a,b", true},
		{"an escaped equals", "n,,n=a=3Db,r=abc", "a=b", true},
		// =3D first, then =2C: unescaping the comma first would turn the literal
		// text "=3D2C" into a comma that was never in the name.
		{"both escapes together", "n,,n=x=3D2Cy,r=abc", "x=2Cy", true},
		{"a name that looks like an attribute", "n,,n=r=3Dme,r=abc", "r=me", true},

		{"no name field", "n,,r=abc", "", false},
		{"an empty name", "n,,n=,r=abc", "", false},
		{"nothing at all", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := scramUsername([]byte(c.payload))
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v", ok, c.ok)
			}
			if ok && got != c.want {
				t.Errorf("username = %q, want %q", got, c.want)
			}
		})
	}
}

// The cache key must change with who is asking, or the sharing this enables
// becomes one client reading another's documents.
//
// Sharing between a user's own connections is the whole point: a pooled driver
// spreads a repeated question across them. Sharing between users is the bypass
// the key exists to prevent, and the two differ only by this string.
func TestIdentitySeparatesUsers(t *testing.T) {
	cmd := rawDoc(t, bson.D{{Key: "count", Value: "orders"}})

	alice1 := &conn{id: 1, observedUser: "alice@admin"}
	alice2 := &conn{id: 2, observedUser: "alice@admin"}
	bob := &conn{id: 3, observedUser: "bob@admin"}
	sameName := &conn{id: 4, observedUser: "alice@other"}
	anon1 := &conn{id: 5}
	anon2 := &conn{id: 6}

	key := func(c *conn) string { return c.cacheKey("count", "shop.orders", cmd) }

	if key(alice1) != key(alice2) {
		t.Error("one user's two connections do not share a key, which is the case this exists to serve")
	}
	if key(alice1) == key(bob) {
		t.Error("two users share a key, so one can be served the other's documents")
	}
	// Same username, different authentication database: a different principal.
	if key(alice1) == key(sameName) {
		t.Error("alice@admin and alice@other share a key, and they are not the same account")
	}
	// With nobody named, the connection is the identity, so two of them must
	// not share either.
	if key(anon1) == key(anon2) {
		t.Error("two unauthenticated connections share a key")
	}
	if key(anon1) == key(alice1) {
		t.Error("an unauthenticated connection shares a key with a named user")
	}
}

// A name is only worth anything once the fast tier has accepted it, and a
// refused or half-finished handshake must leave none behind.
func TestObservedUserOnlyAfterTheFastTierAgrees(t *testing.T) {
	start := rawDoc(t, bson.D{
		{Key: "saslStart", Value: int32(1)},
		{Key: "mechanism", Value: "SCRAM-SHA-256"},
		{Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,,n=alice,r=abc")}},
		{Key: "$db", Value: "admin"},
	})

	t.Run("refused", func(t *testing.T) {
		c := &conn{}
		c.noteProxiedAuth("saslStart", start)
		if c.observedPending == "" {
			t.Fatal("the name was not read at all, so the rest of this proves nothing")
		}
		c.confirmProxiedAuth("saslStart", rawDoc(t, bson.D{{Key: "ok", Value: 0.0}}))
		if c.observedUser != "" || c.observedPending != "" {
			t.Errorf("a refused handshake left %q pending %q", c.observedUser, c.observedPending)
		}
	})

	t.Run("still going", func(t *testing.T) {
		c := &conn{}
		c.noteProxiedAuth("saslStart", start)
		c.confirmProxiedAuth("saslStart", rawDoc(t, bson.D{
			{Key: "ok", Value: 1.0}, {Key: "done", Value: false}}))
		if c.observedUser != "" {
			t.Errorf("a half-finished handshake was believed: %q", c.observedUser)
		}
	})

	t.Run("accepted", func(t *testing.T) {
		c := &conn{}
		c.noteProxiedAuth("saslStart", start)
		c.confirmProxiedAuth("saslContinue", rawDoc(t, bson.D{
			{Key: "ok", Value: 1.0}, {Key: "done", Value: true}}))
		if c.observedUser != "alice@admin" {
			t.Errorf("observedUser = %q, want alice@admin", c.observedUser)
		}
	})

	t.Run("logout forgets", func(t *testing.T) {
		c := &conn{observedUser: "alice@admin"}
		c.noteProxiedAuth("logout", rawDoc(t, bson.D{{Key: "logout", Value: int32(1)}}))
		if c.observedUser != "" {
			t.Errorf("logout left %q behind, so the next user inherits the answers", c.observedUser)
		}
	})
}
