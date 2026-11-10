package meta

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// SessionRenew is how often a registered session's record is renewed while
// the session is in use. The record expires SessionKeep after it was last
// written, and a session registered once and used for longer than that has
// its inserts read as the application's from then on: mirrored back to the
// archive and handed to change streams as inserts nobody made.
const SessionRenew = 24 * time.Hour

// OwnSession is one registered session and what renewing it needs.
type OwnSession struct {
	S   *mongo.Session
	uid string
	at  time.Time
}

// Renew writes the session's record again once SessionRenew has passed since
// it was last written. An error leaves the session usable until the record
// expires, which is far off, and is tried again on the next use.
func (o *OwnSession) Renew(ctx context.Context, hot *mongo.Client, kind string) error {
	if time.Since(o.at) < SessionRenew {
		return nil
	}
	if err := RegisterSession(ctx, hot, o.S.ID(), kind, o.uid); err != nil {
		return err
	}
	o.at = time.Now()
	return nil
}

// StartOwnSession opens a session, learns its uid and registers it as kind.
// Transactions in it are committed with wc, the write concern their inserts
// would have had outside one.
//
// The session is never ended. An ended session goes back to the driver's
// pool, and a transaction of anything else made in it would carry an lsid the
// register names as this system's. See meta.Sessions.
func StartOwnSession(ctx context.Context, hot *mongo.Client, kind string, wc *writeconcern.WriteConcern) (*OwnSession, string, error) {
	opts := options.Session()
	if wc != nil {
		opts.SetDefaultTransactionOptions(options.Transaction().SetWriteConcern(wc))
	}
	s, err := hot.StartSession(opts)
	if err != nil {
		return nil, "", fmt.Errorf("start the %s session: %w", kind, err)
	}
	uid := SessionUID(ctx, hot, s)
	if err := RegisterSession(ctx, hot, s.ID(), kind, uid); err != nil {
		s.EndSession(context.WithoutCancel(ctx))
		return nil, "", err
	}
	// Idempotent, and the warmer, which runs it too, warns when it fails. A
	// register that does not expire costs one small row per session.
	_ = EnsureSessionIndexes(ctx, hot)
	return &OwnSession{S: s, uid: uid, at: time.Now()}, uid, nil
}

// SessionPool lends registered sessions to writers that run concurrently, as
// copy-ups do: a driver session is one client's, and two transactions cannot
// run in it at once.
//
// Bounded, and a writer that finds every session lent gets none and writes the
// way it did before sessions. Sessions are never ended (see StartOwnSession),
// so a pool that grew with every burst would keep every session it ever
// opened, and a writer waiting for one would wait on someone else's read of
// the archive.
type SessionPool struct {
	hot  *mongo.Client
	kind string
	wc   *writeconcern.WriteConcern

	mu     sync.Mutex
	free   []*OwnSession
	opened int
	max    int

	// noUID is said once: a session registered without its uid is matched by
	// id alone. See SessionRegister.Ours.
	noUID func()
	said  sync.Once
}

// NewSessionPool lends up to max sessions registered as kind. noUID, if set,
// is called once when a session's uid could not be learned.
func NewSessionPool(hot *mongo.Client, kind string, max int, wc *writeconcern.WriteConcern, noUID func()) *SessionPool {
	return &SessionPool{hot: hot, kind: kind, wc: wc, max: max, noUID: noUID}
}

// Get lends a session, opening and registering one when none is free and the
// pool is under its bound. nil with no error means every session is lent.
func (p *SessionPool) Get(ctx context.Context) (*OwnSession, error) {
	if p == nil {
		return nil, nil
	}
	p.mu.Lock()
	if n := len(p.free); n > 0 {
		o := p.free[n-1]
		p.free = p.free[:n-1]
		p.mu.Unlock()
		if err := o.Renew(ctx, p.hot, p.kind); err != nil && time.Since(o.at) > SessionKeep/2 {
			// Far enough from the last write that the record may lapse
			// before the next renewal. Not lent until it is renewed.
			p.Put(o)
			return nil, err
		}
		return o, nil
	}
	if p.opened >= p.max {
		p.mu.Unlock()
		return nil, nil
	}
	p.opened++
	p.mu.Unlock()
	o, uid, err := StartOwnSession(ctx, p.hot, p.kind, p.wc)
	if err != nil {
		p.mu.Lock()
		p.opened--
		p.mu.Unlock()
		return nil, err
	}
	if uid == "" && p.noUID != nil {
		p.said.Do(p.noUID)
	}
	return o, nil
}

// Put returns a lent session.
func (p *SessionPool) Put(o *OwnSession) {
	if p == nil || o == nil {
		return
	}
	// WithTransaction aborts on every error it returns, but a transaction a
	// caller started some other way and left open would make the next
	// writer's start fail. Aborted here, so a lent session is always idle.
	if o.S.TransactionRunning() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = o.S.AbortTransaction(ctx)
		cancel()
	}
	p.mu.Lock()
	p.free = append(p.free, o)
	p.mu.Unlock()
}
