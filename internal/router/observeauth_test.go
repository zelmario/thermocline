package router

import (
	"io"
	"log/slog"
	"net"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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

// The name is the fast tier's, asked once an exchange finishes, and never one
// read out of what the client sent.
func TestObservedUserIsWhoTheFastTierSays(t *testing.T) {
	start := func(payload, db string) bson.Raw {
		d := bson.D{
			{Key: "saslStart", Value: int32(1)},
			{Key: "mechanism", Value: "SCRAM-SHA-256"},
			{Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte(payload)}},
		}
		if db != "" {
			d = append(d, bson.E{Key: "$db", Value: db})
		}
		return rawDoc(t, d)
	}
	done := rawDoc(t, bson.D{{Key: "ok", Value: 1.0}, {Key: "done", Value: true}})
	alicePayroll := bson.A{bson.D{{Key: "user", Value: "alice"}, {Key: "db", Value: "payroll"}}}

	t.Run("a legacy saslStart with no $db", func(t *testing.T) {
		c := gatedConn(t, &fastTier{users: alicePayroll})
		c.noteProxiedAuth("saslStart", start("n,,n=alice,r=abc", ""))
		c.confirmProxiedAuth("saslContinue", done)
		if c.observedUser != "alice@payroll" {
			t.Errorf("observedUser = %q, want alice@payroll; the old reading defaulted to admin", c.observedUser)
		}
	})

	t.Run("a second n= after an authzid", func(t *testing.T) {
		c := gatedConn(t, &fastTier{users: bson.A{bson.D{{Key: "user", Value: "eve"}, {Key: "db", Value: "admin"}}}})
		c.noteProxiedAuth("saslStart", start("n,a=eve,n=eve,r=abc,,n=root", "admin"))
		c.confirmProxiedAuth("saslContinue", done)
		if c.observedUser != "eve@admin" {
			t.Errorf("observedUser = %q, want eve@admin; the old reading took the forged n=root", c.observedUser)
		}
	})

	t.Run("a speculative name the fast tier did not take", func(t *testing.T) {
		c := gatedConn(t, &fastTier{users: alicePayroll})
		spec := rawDoc(t, bson.D{{Key: "hello", Value: int32(1)}, {Key: "speculativeAuthenticate", Value: bson.D{
			{Key: "saslStart", Value: int32(1)}, {Key: "mechanism", Value: "SCRAM-SHA-256"},
			{Key: "payload", Value: bson.Binary{Data: []byte("n,,n=root,r=abc")}}, {Key: "db", Value: "admin"}}}})
		c.noteProxiedAuth("hello", spec)
		c.confirmProxiedAuth("hello", rawDoc(t, bson.D{{Key: "ok", Value: 1.0}}))
		if c.observedUser != "" {
			t.Fatalf("a hello the fast tier declined set %q", c.observedUser)
		}
		c.confirmProxiedAuth("saslContinue", done)
		if c.observedUser != "alice@payroll" {
			t.Errorf("observedUser = %q, want alice@payroll; the old reading confirmed root", c.observedUser)
		}
	})

	t.Run("still going", func(t *testing.T) {
		f := &fastTier{users: alicePayroll}
		c := gatedConn(t, f)
		c.noteProxiedAuth("saslStart", start("n,,n=alice,r=abc", "payroll"))
		c.confirmProxiedAuth("saslStart", rawDoc(t, bson.D{{Key: "ok", Value: 1.0}, {Key: "done", Value: false}}))
		if c.observedUser != "" || len(f.probes) != 0 {
			t.Errorf("a half-finished exchange set %q after %v", c.observedUser, f.probes)
		}
	})

	t.Run("the fast tier cannot be asked", func(t *testing.T) {
		mine, theirs := net.Pipe()
		_ = theirs.Close()
		r := &Router{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
		c := &conn{r: r, upstream: mine, log: r.log}
		c.noteProxiedAuth("saslStart", start("n,,n=alice,r=abc", "payroll"))
		c.confirmProxiedAuth("saslContinue", done)
		if c.observedUser != "" {
			t.Errorf("observedUser = %q with no answer; the connection should be the identity", c.observedUser)
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
