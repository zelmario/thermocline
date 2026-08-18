// Package authn lets the router verify a client's identity itself, without ever
// being told a password.
//
// Until now authentication was proxied. The client's SCRAM conversation went
// straight through to the fast tier, which meant the one upstream connection
// carrying that identity was the only connection the router had, and three
// limitations followed from that single fact:
//
//   - Read preference could not be honoured. Answering a secondaryPreferred read
//     from a secondary needs a second connection, and a second connection would
//     be authenticated as nobody, because SCRAM is nonce-bound and one-shot.
//   - A write issued while the fast tier had no primary failed. Replaying it
//     against a freshly elected primary needs a connection authenticated as the
//     caller, and the router cannot make one.
//   - Connections could not be pooled. One upstream per client is correct for
//     proxied authentication and will not scale to tens of thousands of them.
//
// Terminating authentication lifts all three. The router runs the server half of
// SCRAM, learns who the client is, and then talks to both tiers over pooled
// connections of its own.
//
// # Why this does not need the password
//
// SCRAM was designed so a server can verify a password it does not store. What
// MongoDB keeps in admin.system.users per user and mechanism is a salt, an
// iteration count, a storedKey and a serverKey. The client proves knowledge of
// the password against the storedKey, and the server proves knowledge of it back
// with the serverKey. Neither is the password and neither can be turned into it.
//
// So the router reads those four values for the authenticating user, runs the
// conversation, and ends up certain who it is talking to while holding nothing
// it could replay against another system. This is the same position mongod is
// in, which is the point: it is not a new trust assumption, it is the one the
// database already makes.
//
// # What this obliges the router to do
//
// Everything, and this is the part to be careful about. While authentication was
// proxied the fast tier decided what a client could read, and it decided
// correctly for almost every command, because a read it refuses comes back as an
// error rather than as a miss. The exceptions were count, distinct and
// aggregate, which are judged before they run, and those exceptions were a
// privilege bypass: a user with no access read another database's collection out
// of the archive over a connection the router had opened with its own
// credentials.
//
// Once authentication is terminated, every command is in that position. The
// upstream connections belong to the router, so the fast tier will do whatever
// they ask, and the only thing standing between a client and somebody else's
// data is the router's own authorization check. That check therefore has to
// deny by default and be driven by the privileges the database itself reports,
// never by a list maintained here.
//
// See package authz for the half that decides what an identity may do. This
// package only establishes which identity it is.
package authn

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Mechanisms the router can terminate, strongest first. SCRAM-SHA-1 is still
// here because MongoDB still creates credentials for it and older drivers still
// ask for it; a user that has only SHA-1 credentials would otherwise be locked
// out by an upgrade of the proxy rather than of the database.
const (
	SHA256 = "SCRAM-SHA-256"
	SHA1   = "SCRAM-SHA-1"
)

// Stored is what MongoDB keeps for one user and one mechanism. None of it is
// the password, and none of it can be turned back into the password.
type Stored struct {
	Salt      string
	StoredKey string
	ServerKey string
	Iters     int
}

// Source reads user credentials from the fast tier and remembers them briefly.
//
// The fast tier is the authority on identity here, not the archive. It is the
// cluster the application authenticated against before Thermocline existed, so
// its users are the users, and adding a second place to define them would be a
// way to grant access that the database itself never agreed to.
type Source struct {
	hot *mongo.Client
	ttl time.Duration

	mu      sync.RWMutex
	entries map[string]cached
}

type cached struct {
	creds   map[string]Stored // by mechanism
	fetched time.Time
	found   bool
}

// New returns a Source reading from the fast tier.
func New(hot *mongo.Client, ttl time.Duration) *Source {
	return &Source{hot: hot, ttl: ttl, entries: make(map[string]cached)}
}

// Lookup returns the stored credentials for a user in an authentication
// database, and whether that user exists at all.
//
// A miss is cached as briefly as a hit. Not caching it invites a login storm
// against a mistyped username to fall through to the database on every attempt,
// and caching it for longer would keep a freshly created user locked out.
func (s *Source) Lookup(ctx context.Context, db, user string) (map[string]Stored, bool, error) {
	key := db + "." + user

	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if ok && time.Since(e.fetched) < s.ttl {
		return e.creds, e.found, nil
	}

	creds, found, err := s.load(ctx, db, user)
	if err != nil {
		return nil, false, err
	}

	s.mu.Lock()
	s.entries[key] = cached{creds: creds, fetched: time.Now(), found: found}
	s.mu.Unlock()
	return creds, found, nil
}

// Forget drops a cached entry, so a credential change takes effect on the next
// login rather than at the end of the TTL.
func (s *Source) Forget(db, user string) {
	s.mu.Lock()
	delete(s.entries, db+"."+user)
	s.mu.Unlock()
}

// load reads one user's SCRAM credentials out of admin.system.users.
//
// usersInfo would be the polite way to ask and it does not return the stored
// keys, deliberately, because they are what a server needs and a human does
// not. Reading the collection directly is therefore the only way, and it is why
// the router's own account needs read on admin.system.users and why that
// account should have nothing else.
func (s *Source) load(ctx context.Context, db, user string) (map[string]Stored, bool, error) {
	var doc struct {
		Credentials map[string]struct {
			Salt           string `bson:"salt"`
			StoredKey      string `bson:"storedKey"`
			ServerKey      string `bson:"serverKey"`
			IterationCount int    `bson:"iterationCount"`
		} `bson:"credentials"`
	}
	err := s.hot.Database("admin").Collection("system.users").FindOne(ctx,
		bson.D{{Key: "db", Value: db}, {Key: "user", Value: user}},
		options.FindOne().SetProjection(bson.D{{Key: "credentials", Value: 1}}),
	).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read credentials for %s.%s: %w", db, user, err)
	}

	out := make(map[string]Stored, len(doc.Credentials))
	for mech, c := range doc.Credentials {
		if mech != SHA256 && mech != SHA1 {
			continue
		}
		out[mech] = Stored{
			Salt:      c.Salt,
			StoredKey: c.StoredKey,
			ServerKey: c.ServerKey,
			Iters:     c.IterationCount,
		}
	}
	return out, true, nil
}

// Mechanisms lists what a user can authenticate with, strongest first. It is
// what hello reports in saslSupportedMechs, which is how a driver decides what
// to send without a round trip of guessing.
func Mechanisms(creds map[string]Stored) []string {
	var out []string
	if _, ok := creds[SHA256]; ok {
		out = append(out, SHA256)
	}
	if _, ok := creds[SHA1]; ok {
		out = append(out, SHA1)
	}
	return out
}

// Credentials turns what MongoDB stored into what the SCRAM server half needs.
//
// The salt is decoded on the way through. MongoDB stores it base64 encoded, and
// scram.KeyFactors wants the raw bytes because it does that encoding itself when
// it builds the challenge. Handing it the encoded form salts the proof with the
// wrong bytes, so every correct password is rejected, which reads like a
// credential problem rather than an encoding one.
func (st Stored) Credentials() (scram.StoredCredentials, error) {
	salt, err := base64.StdEncoding.DecodeString(st.Salt)
	if err != nil {
		return scram.StoredCredentials{}, fmt.Errorf("decode salt: %w", err)
	}
	stored, err := base64.StdEncoding.DecodeString(st.StoredKey)
	if err != nil {
		return scram.StoredCredentials{}, fmt.Errorf("decode storedKey: %w", err)
	}
	server, err := base64.StdEncoding.DecodeString(st.ServerKey)
	if err != nil {
		return scram.StoredCredentials{}, fmt.Errorf("decode serverKey: %w", err)
	}
	return scram.StoredCredentials{
		KeyFactors: scram.KeyFactors{Salt: string(salt), Iters: st.Iters},
		StoredKey:  stored,
		ServerKey:  server,
	}, nil
}

// HashFor returns the hash generator for a mechanism.
func HashFor(mech string) (scram.HashGeneratorFcn, error) {
	switch mech {
	case SHA256:
		return scram.SHA256, nil
	case SHA1:
		return scram.SHA1, nil
	}
	return nil, fmt.Errorf("unsupported mechanism %q", mech)
}
