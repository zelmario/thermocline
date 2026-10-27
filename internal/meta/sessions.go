package meta

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// Sessions is the fast tier's register of the sessions this system writes in,
// so that a change event made in one can be told from the application's by
// its lsid alone.
//
// What it replaced was a record per document on the archive. A warm announced
// every document it was about to insert, and the feeder looked each one up and
// deleted it again, so that it would not mirror the warm back as an insert and
// the router would not hand it to a change stream as one. Measured on staging
// from an empty fast tier at 9,000 ops/s against an archive held to 3,000
// IOPS: a warm beside the application wrote 855,000 notices and 1.6 million
// residency updates in five minutes, the feeder fell 45 seconds behind, and
// under strict freshness 762 operations failed. The archive is the cluster
// the fast tier is there to relieve.
//
// A session is registered once, before its first transaction, and every
// insert made in a transaction of it carries its lsid in the oplog and in
// every change event. Registered first means a lookup that finds nothing is
// final: the event was made after the registration committed, so a session
// that is not there is not this system's. Lives on the fast tier, which is
// where the events are read and which is not the cluster under pressure.
const Sessions = "sessions"

// SessionDocMax is the largest document this system inserts in a registered
// session. A change event for a larger one can be split, and only one piece
// of a split event carries lsid, so such a document is announced and recorded
// with an echo as before sessions. The feeder reads warm announcements only
// for documents above it.
const SessionDocMax = 8 << 20

// SessionRecord is one registered session.
type SessionRecord struct {
	ID        string    `bson:"_id"`
	Kind      string    `bson:"kind"`
	At        time.Time `bson:"at"`
	ExpiresAt time.Time `bson:"expiresAt"`
}

// SessionKeep is how long a session stays registered after it was last
// renewed. A change stream consumer reading an event later than this judges a
// warm as an insert, which is the bound an eviction notice states as well, at
// a far longer reach: the record is one row per process, not per document.
const SessionKeep = 30 * 24 * time.Hour

// RegisterSession records a session as this system's, and returns once a
// majority of the fast tier holds the record, so no event made in it can be
// read before it.
func RegisterSession(ctx context.Context, hot *mongo.Client, lsid bson.Raw, kind string) error {
	id := SessionHex(lsid)
	if id == "" {
		return errors.New("register a session: it has no lsid")
	}
	now := time.Now().UTC()
	coll := hot.Database(DB).Collection(Sessions,
		options.Collection().SetWriteConcern(writeconcern.Majority()))
	_, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "kind", Value: kind}, {Key: "expiresAt", Value: now.Add(SessionKeep)}}},
			{Key: "$setOnInsert", Value: bson.D{{Key: "at", Value: now}}},
		},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("register the %s session: %w", kind, err)
	}
	return nil
}

// EnsureSessionIndexes keeps the register expiring.
func EnsureSessionIndexes(ctx context.Context, hot *mongo.Client) error {
	_, err := hot.Database(DB).Collection(Sessions).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expiresAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0).SetName("tc_sessions_expiry"),
	})
	return err
}

// SessionRegister answers whether a session is this system's, from memory once
// asked.
//
// Both answers are kept. A yes cannot change while the record lives. A no
// cannot either, because a session is registered before anything is written
// in it: an event in it is never read before its record.
type SessionRegister struct {
	hot *mongo.Client

	mu    sync.Mutex
	known map[string]bool
}

// sessionCacheMax bounds the answers kept. Past it they are forgotten and asked
// again, which costs a read and changes no answer.
const sessionCacheMax = 100_000

// NewSessionRegister reads the register on hot.
func NewSessionRegister(hot *mongo.Client) *SessionRegister {
	return &SessionRegister{hot: hot, known: map[string]bool{}}
}

// Ours reports whether the session named by its hex lsid id is one this system
// registered. "" is no session, and never ours.
func (s *SessionRegister) Ours(ctx context.Context, session string) (bool, error) {
	if s == nil || session == "" {
		return false, nil
	}
	s.mu.Lock()
	yes, ok := s.known[session]
	s.mu.Unlock()
	if ok {
		return yes, nil
	}
	// Majority, because the record was written at majority and the event that
	// sent this read was read from a change stream, which is majority too.
	coll := s.hot.Database(DB).Collection(Sessions,
		options.Collection().SetReadConcern(readconcern.Majority()))
	err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: session}},
		options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).Err()
	switch {
	case err == nil:
		yes = true
	case errors.Is(err, mongo.ErrNoDocuments):
		yes = false
	default:
		return false, fmt.Errorf("read the session register: %w", err)
	}
	s.mu.Lock()
	if len(s.known) >= sessionCacheMax {
		s.known = map[string]bool{}
	}
	s.known[session] = yes
	s.mu.Unlock()
	return yes, nil
}
