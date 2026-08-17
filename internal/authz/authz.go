// Package authz decides what an authenticated client may do.
//
// While authentication was proxied this package had no reason to exist. The
// client's own connection carried its identity to the fast tier, so the fast
// tier decided, and it decided correctly for almost everything: a read it
// refuses comes back as an error, and an error is not a miss, so nothing fell
// through to the archive behind its back.
//
// Terminating authentication takes that away. The upstream connections belong
// to the router now, authenticated as the router's own account, and the fast
// tier will do whatever they ask. The only thing between a client and somebody
// else's data is the check in this package. That is a large thing to be true, so
// two rules follow from it.
//
// First, deny by default. A command whose required privilege is not known is
// refused, loudly, naming the command. That will refuse something mongod would
// have allowed, and that is the right way round: this project's rule is that a
// refusal is a limitation and a wrong answer is a bug. An unknown command
// allowed through is not a limitation, it is an unauthorised read waiting to
// happen.
//
// Second, the database decides, not this package. Privileges are read from
// MongoDB with usersInfo and showPrivileges, so roles, custom roles and role
// inheritance are all honoured because they were resolved by the server that
// owns them. Nothing here maintains a list of who may see what. A list kept in a
// proxy is a second source of truth about access, and the moment it disagrees
// with the database it is either refusing legitimate work or granting access
// nobody approved.
//
// This is the exact defect the command surface audit found as item 0, which is
// worth remembering as the thing being guarded against rather than as history: a
// user with read on one unrelated database got a count and a distinct out of the
// archive over a connection the router had opened with its own credentials.
// That was three commands. This is all of them.
package authz

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Resource is what a privilege applies to, in the shapes MongoDB uses.
//
// The empty string is a wildcard, and it means different things in each
// position, which is the part worth being careful about:
//
//	{db: "shop", collection: "orders"}  that one collection
//	{db: "shop", collection: ""}        every collection in shop
//	{db: "", collection: "orders"}      a collection called orders in any database
//	{db: "", collection: ""}            every collection in every database
//
// Which kind of resource this is has to be decided by which keys are present,
// not by their values, and getting that wrong is a privilege escalation rather
// than a missing feature.
//
// A real cluster answered usersInfo with four kinds: {db, collection},
// {cluster}, {anyResource} and {system_buckets}. Decoded into a plain struct the
// last one arrives with db and collection both empty, which is indistinguishable
// from "every collection in every database", so a user holding one
// system_buckets privilege would have been granted its actions on everything.
//
// So the keys are recorded as they arrive and an unrecognised resource covers
// nothing at all. A resource kind added by a future release then refuses work it
// should have allowed, which somebody will report, instead of quietly granting
// access nobody approved, which nobody will.
type Resource struct {
	DB          string
	Collection  string
	Cluster     bool
	AnyResource bool

	// kind is what the resource actually names, decided by the keys present.
	kind resourceKind
}

type resourceKind int

const (
	// kindUnknown is the zero value on purpose. A resource this package does
	// not recognise, including one it failed to decode, covers nothing.
	kindUnknown resourceKind = iota
	kindNamespace
	kindCluster
	kindAny
)

// UnmarshalBSON decides the kind from the keys MongoDB sent.
func (r *Resource) UnmarshalBSON(data []byte) error {
	raw := bson.Raw(data)
	elems, err := raw.Elements()
	if err != nil {
		return err
	}
	*r = Resource{}
	var sawDB, sawColl bool
	for _, e := range elems {
		switch e.Key() {
		case "db":
			sawDB = true
			r.DB, _ = e.Value().StringValueOK()
		case "collection":
			sawColl = true
			r.Collection, _ = e.Value().StringValueOK()
		case "cluster":
			if b, ok := e.Value().BooleanOK(); ok && b {
				r.Cluster = true
			}
		case "anyResource":
			if b, ok := e.Value().BooleanOK(); ok && b {
				r.AnyResource = true
			}
		}
	}

	switch {
	case r.AnyResource:
		r.kind = kindAny
	case r.Cluster:
		r.kind = kindCluster
	case sawDB && sawColl:
		r.kind = kindNamespace
	default:
		// system_buckets, and anything a later release invents.
		r.kind = kindUnknown
	}
	return nil
}

// Privilege is one resource and what may be done to it.
type Privilege struct {
	Resource Resource `bson:"resource"`
	Actions  []string `bson:"actions"`
}

// Grants is everything one authenticated user may do.
type Grants struct {
	User       string
	DB         string
	Privileges []Privilege
}

// Allowed reports whether the user may perform action on a namespace.
//
// An empty collection means the action is about the database itself, such as
// listCollections, and only a privilege covering the whole database or wider can
// satisfy it.
func (g Grants) Allowed(db, coll, action string) bool {
	for _, p := range g.Privileges {
		if !p.Resource.covers(db, coll) {
			continue
		}
		if hasAction(p.Actions, action) {
			return true
		}
	}
	return false
}

// AllowedCluster reports whether the user may perform a cluster-wide action,
// such as listDatabases.
func (g Grants) AllowedCluster(action string) bool {
	for _, p := range g.Privileges {
		if p.Resource.kind != kindCluster && p.Resource.kind != kindAny {
			continue
		}
		if hasAction(p.Actions, action) {
			return true
		}
	}
	return false
}

func hasAction(actions []string, want string) bool {
	for _, a := range actions {
		if a == want {
			return true
		}
	}
	return false
}

// covers reports whether this resource includes the namespace.
func (r Resource) covers(db, coll string) bool {
	switch r.kind {
	case kindAny:
		return true
	case kindNamespace:
		// Fall through to the matching below.
	default:
		// A cluster privilege is not about any particular collection, so it
		// never satisfies a namespace action. Treating it as if it did is how a
		// proxy accidentally grants read on everything to a monitoring user.
		// Unknown kinds cover nothing, for the reason on the type.
		return false
	}
	if r.DB != "" && r.DB != db {
		return false
	}
	if r.Collection != "" && r.Collection != coll {
		return false
	}
	// A privilege naming a specific collection cannot satisfy an action about
	// the database as a whole.
	if coll == "" && r.Collection != "" {
		return false
	}
	return true
}

// Source loads privileges from the fast tier and remembers them briefly.
//
// The fast tier is asked because it is where the users are, the same reason
// package authn reads credentials there. The cache exists because this is on the
// path of every command, and it is short because a revoked role has to take
// effect while somebody still cares.
type Source struct {
	hot *mongo.Client
	ttl time.Duration

	mu      sync.RWMutex
	entries map[string]cached
}

type cached struct {
	grants  Grants
	fetched time.Time
}

// New returns a Source reading from the fast tier.
func New(hot *mongo.Client, ttl time.Duration) *Source {
	return &Source{hot: hot, ttl: ttl, entries: make(map[string]cached)}
}

// Load returns what a user may do.
func (s *Source) Load(ctx context.Context, db, user string) (Grants, error) {
	key := db + "." + user

	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if ok && time.Since(e.fetched) < s.ttl {
		return e.grants, nil
	}

	g, err := s.load(ctx, db, user)
	if err != nil {
		return Grants{}, err
	}

	s.mu.Lock()
	s.entries[key] = cached{grants: g, fetched: time.Now()}
	s.mu.Unlock()
	return g, nil
}

// Forget drops a cached entry so a revoked role takes effect at once.
func (s *Source) Forget(db, user string) {
	s.mu.Lock()
	delete(s.entries, db+"."+user)
	s.mu.Unlock()
}

// load asks MongoDB what this user may do.
//
// inheritedPrivileges rather than privileges: the first is the resolved set
// including everything the user's roles bring in, which is the question being
// asked. The second is only what was granted to the user directly, and reading
// it would refuse almost every real user, since real users get their access
// through roles.
func (s *Source) load(ctx context.Context, db, user string) (Grants, error) {
	cmd := bson.D{
		{Key: "usersInfo", Value: bson.D{{Key: "user", Value: user}, {Key: "db", Value: db}}},
		{Key: "showPrivileges", Value: true},
	}
	var res struct {
		Users []struct {
			User                string      `bson:"user"`
			DB                  string      `bson:"db"`
			InheritedPrivileges []Privilege `bson:"inheritedPrivileges"`
		} `bson:"users"`
	}
	if err := s.hot.Database("admin").RunCommand(ctx, cmd).Decode(&res); err != nil {
		return Grants{}, fmt.Errorf("read privileges for %s.%s: %w", db, user, err)
	}
	if len(res.Users) == 0 {
		// Authenticated but unknown to usersInfo should not happen, and if it
		// does the safe reading is that this user may do nothing.
		return Grants{User: user, DB: db}, nil
	}
	u := res.Users[0]
	return Grants{User: u.User, DB: u.DB, Privileges: u.InheritedPrivileges}, nil
}

// SplitNS splits a namespace into database and collection.
func SplitNS(ns string) (string, string) {
	if i := strings.IndexByte(ns, '.'); i >= 0 {
		return ns[:i], ns[i+1:]
	}
	return ns, ""
}
