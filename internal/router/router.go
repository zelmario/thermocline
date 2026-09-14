// Package router puts one address in front of both tiers.
//
// It speaks the MongoDB wire protocol and presents itself as a mongos, so
// drivers connect to it exactly as they connect to a database today and no
// application changes are needed.
//
// Almost everything is forwarded to the fast tier untouched. The router only
// acts on a miss: a read that found nothing, or a write that matched nothing,
// where the document may still be in the archive. Those are the two cases where
// the fast tier's answer is wrong rather than merely incomplete.
package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/authn"
	"github.com/zelmario/thermocline/internal/authz"
	"github.com/zelmario/thermocline/internal/cache"
	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/promote"
	"github.com/zelmario/thermocline/internal/watermark"
	"github.com/zelmario/thermocline/internal/wire"
)

// Router accepts client connections and routes them between the tiers.
type Router struct {
	cfg   config.RouterConfig
	hot   *mongo.Client
	cold  *mongo.Client
	marks *watermark.Store
	log   *slog.Logger

	// olderArchive holds the two releases when the archive is behind the fast
	// tier, which only a deployment running with allow_version_skew reaches. It
	// is read on an error path to explain what the archive refused. See
	// archiveDialectNote.
	olderArchive atomic.Pointer[releaseGap]

	// primary is the fast tier address raw connections are proxied to. It is
	// re-resolved after a failure, so a failover is survivable.
	primaryMu sync.RWMutex
	primary   string
	// members is every node of the fast tier, so a read asking for a secondary
	// can be sent to one.
	members []string
	// primaryProbeAt throttles re-resolving after a refusal. Every command in
	// flight when a primary steps down comes back refused the same way, and one
	// lookup answers all of them. primaryProbing is non-nil while one is
	// running and is closed when it finishes, so a refusal can wait for the
	// answer instead of racing the client's reconnect to it.
	primaryProbeMu sync.Mutex
	primaryProbeAt time.Time
	primaryProbing chan struct{}

	// pools hold authenticated connections per member, which only became
	// possible once authentication stopped being the client's.
	poolMu sync.Mutex
	pools  map[string]*upstreamPool
	rr     uint64

	// hello is the fast tier's own handshake reply, kept so a client handshake
	// can be answered here rather than forwarded on a shared connection.
	hello atomic.Value

	// promotedAt throttles the heartbeat that tells the feeder copy-ups are
	// happening.
	promoteMu  sync.Mutex
	promotedAt time.Time

	// creds and grants are how the router decides who a client is and what it
	// may do, once it terminates authentication rather than proxying it. Both
	// read from the fast tier, which is where the users are.
	creds  *authn.Source
	grants *authz.Source

	// checkpoints reads how far mirroring has got, so a read served from the
	// archive can wait for it rather than answer from behind.
	checkpoints *checkpoint.Store
	positions   *checkpoint.Positions
	ckptCache   checkpointCache
	// posLive watches the published position move, which is how this decides
	// whether the feeder is still running. It is a watcher rather than a
	// reading because the alternative compares this host's clock against the
	// feeder's, and getting that wrong turns the freshness barrier off.
	posLive checkpoint.Liveness
	// clock is how far this host's clock is ahead of the archive's, as last
	// measured. The feeder publishes the same reading about itself, and the two
	// together are what make the freshness barrier's wall-clock comparison mean
	// anything across hosts. See caughtUp.
	clock struct {
		mu        sync.Mutex
		skew      time.Duration
		at        time.Time // when the last reading was attempted
		known     bool
		measuring bool
	}
	// seenOpTime is the newest cluster time the fast tier has reported, packed
	// so it fits an atomic. A read falling through to the archive waits for
	// mirroring to reach it. See freshness.go for why a cluster time and not a
	// clock.
	seenOpTime atomic.Uint64

	wmCache  watermarkCache
	idxCache indexCache
	brackets bracketCache

	// warnedKillCursor keeps the privilege warning in superseded.go to one per
	// router, since the condition does not change while it runs.
	warnedKillCursor atomic.Bool
	// hintCache holds each namespace's sparse and partial indexes, the ones
	// whose hint decides which documents are looked at rather than how. See
	// hint.go.
	hintCache hintCache
	// indexer gives the fast tier the archive's indexes the first time a
	// document is copied up into a namespace. A copy-up is the other way the
	// fast tier gains documents, and the only way for a namespace no warmer
	// rule covers. See promote/indexes.go.
	indexer   *promote.Indexer
	collCache collationCache
	viewCache viewCache
	// viewMatch remembers whether the archive's copy of a view is defined the
	// way the fast tier's is, which decides whether a view read the fast tier
	// cannot answer can be sent there instead of refused.
	viewMatch viewMatchCache

	// changeStreams are the client cursors whose events need this system's own
	// housekeeping filtered out of them.
	changeStreams changeStreamRegistry
	cursors       cursorRegistry
	// tiered are the client cursors the fast tier was allowed to answer because
	// a completeness claim covered them, remembered with the claim itself so a
	// getMore can be refused once it no longer does. See tieredcursor.go.
	tiered tieredCursors

	// cache holds answers for as long as each query has been observed to keep
	// giving the same one. Nil unless router.cache.enabled. See querycache.go.
	cache *cache.Cache

	// advisor watches every read and works out which queries would be worth
	// caching and for how long. It runs whether or not the cache does, because
	// the recommendation is what a decision to turn caching on should be made
	// from. See package cache.
	advisor *cache.Advisor

	// nextConnID names connections, which is the identity a cached answer is
	// keyed by while authentication is proxied.
	nextConnID atomic.Uint64

	// watching is whether the cache's change stream is currently open. False
	// means the cache is back to its learned TTL alone as the bound on a write
	// this router did not make, which the dashboard says out loud.
	watching atomic.Bool
	addr     atomic.Value
	Stats    Stats

	// Metrics times commands by how they were served, which is what turns the
	// counters into a picture of what the fast tier is actually saving.
	Metrics *Metrics

	// inv is what each tier is holding, refreshed on a timer.
	inv     inventoryCache
	started time.Time
}

// Addr returns the address the router is listening on, once it is. Empty until
// then. Useful for tests that bind port zero and need to find out where they
// landed.
func (r *Router) Addr() string {
	if v, ok := r.addr.Load().(string); ok {
		return v
	}
	return ""
}

// Stats counts what the router did. Every one of these is a number an operator
// needs: copy-up rate says whether the boundary is cut too deep, and incomplete
// says whether queries are being answered from a tier that cannot answer them.
type Stats struct {
	Commands    atomic.Int64
	ReadMisses  atomic.Int64
	WriteMisses atomic.Int64
	CopiedUp    atomic.Int64
	Incomplete  atomic.Int64
	// NewestFirst counts reads for the newest N that the fast tier proved it
	// could answer from its own reply. Each one is an archive round trip that
	// did not happen. See newestfirst.go.
	NewestFirst atomic.Int64
	// SupersededCursors counts fast tier cursors closed because the archive's
	// answer replaced the one they belonged to. See superseded.go.
	SupersededCursors atomic.Int64
	// Panics counts connections closed by a recovered panic. Any value above
	// zero is a defect, and the stack is in the log beside it. See serve.
	Panics        atomic.Int64
	Refused       atomic.Int64
	Archived      atomic.Int64
	TxnReplays    atomic.Int64
	WarmedForTxn  atomic.Int64
	DuplicateKeys atomic.Int64
	ChangeStreams atomic.Int64
	Unauthorized  atomic.Int64
	// FailoverReplays counts commands re-sent after the fast tier changed
	// primary underneath them. Any value above zero is an election the clients
	// did not have to see.
	FailoverReplays atomic.Int64
	// SecondaryReads counts reads that asked for a secondary and were sent to
	// one. Together with PrimaryOnlyReads it says how much of the read load
	// actually moved off the primary, which is the only way to tell the feature
	// working from the feature configured.
	SecondaryReads atomic.Int64
	// PrimaryOnlyReads counts reads that asked for a secondary and were served
	// by the primary anyway. See readpref.go for why that cannot be honoured.
	PrimaryOnlyReads atomic.Int64
	Unreadable       atomic.Int64
	EventsHidden     atomic.Int64
	StaleReads       atomic.Int64
	// ClaimGraceReads counts reads answered from a completeness claim the
	// router could not re-read, which router.claim_grace allowed. Any value
	// above zero means the archive was unreachable and this router answered
	// anyway. See completeness.
	ClaimGraceReads atomic.Int64
	ClientsOpen     atomic.Int64
	ClientsTotal    atomic.Int64
}

// New builds a Router. It does not listen or connect.
func New(hot, cold *mongo.Client, cfg config.RouterConfig, log *slog.Logger) *Router {
	return &Router{
		cfg:         cfg,
		hot:         hot,
		cold:        cold,
		marks:       watermark.New(cold),
		checkpoints: checkpoint.New(cold),
		positions:   checkpoint.NewPositions(cold),
		indexer:     promote.NewIndexer(hot, cold, log),
		log:         log,
		Metrics:     NewMetrics(metricsWindow),
		started:     time.Now(),
		// Short on purpose. Both are on the path of authenticating and of every
		// command, so they have to be cached, and a revoked role or a changed
		// password has to take effect while somebody still cares.
		creds:  authn.New(hot, 30*time.Second),
		grants: authz.New(hot, 30*time.Second),
	}
}

// Listen serves until ctx is cancelled.
// enableCache builds the query cache when the configuration asks for one.
func (r *Router) enableCache() {
	// The advisor runs by default, whether or not anything is cached: it
	// answers "what would caching be worth here", which is the question a
	// decision to turn the cache on should be made from. It costs a hash and a
	// bounded table, and router.cache.measure turns even that off.
	if r.cfg.Cache.Measuring() {
		r.advisor = cache.NewAdvisor(r.cfg.Cache.MaxQueries, maxParamsPerShape, r.cfg.Cache.MaxTTL.D())
	}

	if !r.cfg.Cache.Enabled {
		if r.advisor == nil {
			r.log.Info("query cache off, and not measuring either",
				"note", "set router.cache.measure to have it say what caching would be worth")
		} else {
			r.log.Info("query cache off, still measuring",
				"note", "run `cline cache` to see which queries would be worth caching and for how long")
		}
		return
	}
	r.cache = cache.NewWithBytes(r.cfg.Cache.MaxQueries, r.cfg.Cache.MaxBytes, r.cfg.Cache.MaxTTL.D())
	r.log.Info("query cache on",
		"max_queries", r.cfg.Cache.MaxQueries,
		"max_ttl", r.cfg.Cache.MaxTTL.D(),
		"max_bytes", r.cfg.Cache.MaxBytes,
		"note", "each query is cached for a little less than it has been observed to stay the same")
}

// maxParamsPerShape bounds how many distinct parameter sets are remembered for
// one query shape. A shape asked with more than this many different values has
// little repetition to exploit, and saying so is the finding.
//
// Fifty rather than two hundred, because this multiplies: the bound on watched
// queries times this is the bound on rows held, and at a thousand shapes the
// larger number is a hundred thousand of them for no more insight.
const maxParamsPerShape = 50

// Survey runs everything a starting router would tell an operator about the
// pair, and writes nothing to either tier.
//
// It exists because those findings were only obtainable by starting a router,
// and a starting router is not read-only: ensureFastTierIndexes builds the
// archive's indexes on the fast tier, mirrorViewsToArchive creates views on the
// archive, and dropLeftoverScratch removes collections. Every one of those is
// right for a router that is about to serve traffic and wrong for a first look
// at a cluster somebody is still deciding about. So pointing this at a
// production pair to find out whether it is suitable meant building indexes on
// it to ask.
//
// The same checks, in the same order, minus those three and the listener. What
// it reports is therefore exactly what a real start would report, which is the
// point: an operator should not have to wonder whether the dry run and the real
// thing look at different things.
//
// The version check is the one that returns an error, because it is the one a
// start refuses on. Everything else is a warning on the logger, which is where
// a router puts them too.
func (r *Router) Survey(ctx context.Context) error {
	if err := r.refreshPrimary(ctx); err != nil {
		return err
	}
	if err := r.checkTierVersions(ctx); err != nil {
		return err
	}
	r.warnAboutMissingWatermarks(ctx)
	r.warnAboutStrandedCollections(ctx)
	r.warnAboutMissingUsers(ctx)
	r.warnAboutMissingRoles(ctx)
	r.warnAboutDefaultConcerns(ctx)
	r.warnAboutOptionDrift(ctx)
	r.warnAboutClocks(ctx)
	// Last here, first in Listen's ordering terms: it reads one index listing
	// per tiered namespace and a start defers it behind the listener for that
	// reason. Nothing is waiting on this one, so it runs inline.
	//
	// It reads the fast tier's own unique indexes, and a real start would have
	// built any the archive has and it lacks a moment earlier. So on an
	// adoption this says "nothing has claimed these" about indexes that do not
	// exist on the fast tier yet yet will the moment a router starts, which is
	// the honest answer to "what is true now" and is worth knowing beside the
	// index report above it.
	r.warnAboutUnclaimedUniqueKeys(ctx)

	// The three writes a start makes, reported rather than made. Together with
	// the checks above this answers the question the command exists for: what
	// will starting a router do to these two clusters?
	r.warnAboutWhatAStartWouldBuild(ctx)
	if dropped := r.dropLeftoverScratch(ctx, true); len(dropped) > 0 {
		r.log.Warn("a router start would remove these from the fast tier",
			"collections", dropped,
			"meaning", "scratch collections a previous router left in the application's own "+
				"database, from an aggregation writing into a tiered collection that was "+
				"interrupted before it could rename its result into place",
			"action", "nothing; they are this system's own litter and hold no application data. "+
				"They are named because a start removing a collection is worth seeing first")
	}
	if carried := r.mirrorViewsToArchive(ctx, true); len(carried) > 0 {
		r.log.Warn("a router start would give the archive these views",
			"views", carried,
			"meaning", "the fast tier has them and the archive does not, and repointing the "+
				"application at the archive is what rollback is, so a view has to exist there too",
			"action", "nothing, if the archive is meant to answer these after a rollback")
	}
	return nil
}

// warnAboutWhatAStartWouldBuild names the tiered collections a router start
// would create on the fast tier, and how many indexes it would build there.
//
// Every other check here reports a state. This one reports an action, and it is
// in a survey because the survey is the one caller that does not take it: a
// real start creates each of these from the archive's options and builds the
// archive's indexes on it before the listener opens.
//
// Without it the survey was quietest about the case it exists for. A tiered
// collection the archive has and the fast tier has never held is the adoption
// shape, and nothing above says a word about it: it is not stranded, because
// something does tier it; and warnAboutOptionDrift skips a collection only one
// tier has on purpose, because by the time a started router reaches it the
// indexer has already created the other side. In a survey the indexer has not
// run, so that reasoning does not hold and the gap was silence.
//
// The unique count is separate because it is the number that costs. A unique
// index build walks both tiers and can refuse, and until something claims those
// keys the constraint is not enforced across the pair at all.
func (r *Router) warnAboutWhatAStartWouldBuild(ctx context.Context) {
	byDB := map[string][]string{}
	for _, ns := range r.cfg.Managed {
		if db, _ := splitNS(ns); db != "" {
			byDB[db] = append(byDB[db], ns)
		}
	}

	var pending []string
	indexes, unique := 0, 0
	for db, nss := range byDB {
		cold, cerr := collectionsIn(ctx, r.cold, db)
		hot, herr := collectionsIn(ctx, r.hot, db)
		if cerr != nil || herr != nil {
			continue
		}
		for _, ns := range nss {
			_, coll := splitNS(ns)
			if cold[coll] != "collection" || hot[coll] != "" {
				continue
			}
			pending = append(pending, ns)
			names, err := uniqueIndexNames(ctx, r.cold, db, coll)
			if err == nil {
				unique += len(names)
			}
			if all, aerr := indexNamesOf(ctx, r.cold, db, coll); aerr == nil {
				indexes += all
			}
		}
	}
	if len(pending) == 0 {
		return
	}
	sort.Strings(pending)
	r.log.Warn("a router start would create these on the fast tier and build their indexes",
		"collections", pending, "indexes", indexes, "unique_indexes", unique,
		"meaning", "this is the adoption shape and it is not an error: the archive carries the "+
			"application's collections and the fast tier has never held them, so a start creates "+
			"each one the way the archive has it and builds the archive's indexes on it before "+
			"the listener opens",
		"cost", "index builds on the fast tier, and a unique one walks both tiers before it is "+
			"accepted; until cline keys --rebuild has claimed their values the constraint is not "+
			"enforced across the pair",
		"action", "nothing, if that is what is wanted; otherwise narrow feeder.namespaces first, "+
			"because this is what a start will do to the fast tier")
}

// indexNamesOf counts one collection's indexes on one tier.
func indexNamesOf(ctx context.Context, client *mongo.Client, db, coll string) (int, error) {
	cur, err := client.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	n := 0
	for cur.Next(ctx) {
		n++
	}
	return n, cur.Err()
}

func (r *Router) Listen(ctx context.Context) error {
	r.enableCache()
	if err := r.refreshPrimary(ctx); err != nil {
		return err
	}
	// Before anything else a start could get wrong: the pair itself has to be
	// one this system can serve. See internal/tierver.
	if err := r.checkTierVersions(ctx); err != nil {
		return err
	}
	r.warnAboutMissingWatermarks(ctx)
	r.warnAboutStrandedCollections(ctx)
	r.warnAboutMissingUsers(ctx)
	r.warnAboutMissingRoles(ctx)
	r.warnAboutDefaultConcerns(ctx)
	r.warnAboutOptionDrift(ctx)
	r.warnAboutClocks(ctx)
	// And the litter a router that died mid-aggregation leaves in the
	// application's own database. See dropLeftoverScratch.
	r.dropLeftoverScratch(ctx, false)
	// Before the listener opens: a unique index the fast tier is missing is one
	// nothing is enforcing, and the first client to write is what creates the
	// collection there. See ensureFastTierIndexes.
	r.ensureFastTierIndexes(ctx)
	// And the definitions that hold no documents, in the other direction: a view
	// the fast tier has and the archive does not. See mirrorViewsToArchive.
	r.mirrorViewsToArchive(ctx, false)

	// Open one pooled connection before accepting anybody, so the first client's
	// handshake can be answered from the fast tier's own reply. Without it that
	// first handshake has nothing to answer from, falls through to being
	// forwarded, and is refused for carrying a second client metadata document
	// on a connection that has already introduced itself.
	if r.cfg.TerminateAuth {
		p, err := r.poolFor(r.currentPrimary()).get(ctx)
		if err != nil {
			return fmt.Errorf("open the first connection to the fast tier: %w", err)
		}
		p.release()
	}

	ln, err := net.Listen("tcp", r.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", r.cfg.Listen, err)
	}
	// The address this takes over is one an application already dials, so
	// whatever that connection was has to keep working. See tls.go.
	if ln, err = listenerFor(ln, r.cfg.TLS); err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()
	r.addr.Store(ln.Addr().String())

	r.goSafely("closing the listener on shutdown", func() {
		<-ctx.Done()
		_ = ln.Close()
	})
	r.goSafely("reaping abandoned cursors", func() { r.reapCursors(ctx) })
	// Whether anything has ever claimed the keys the fast tier's unique indexes
	// constrain. On an adoption nothing has, and the constraint is then not
	// enforced against the archive at all: see warnAboutUnclaimedUniqueKeys.
	//
	// After the listener rather than before it, and that is not a preference.
	// It reads one index listing per tiered namespace, and the other startup
	// warnings are per *database*: with 139 tiered namespaces it took a router
	// past the thirty seconds a suite waits for one, so the first write after it
	// got ECONNREFUSED and the check that a write rebuilds a dropped
	// collection failed for want of a router. A diagnostic that delays the
	// listener costs an application its writes, which is worse than anything it
	// can tell an operator.
	//
	// Still after ensureFastTierIndexes, which is what it has to be after: on an
	// adoption the fast tier's copy of the index may only exist because the
	// Indexer has just built it, and that pass is above, before the listener.
	r.goSafely("checking whether the archive's unique keys were ever claimed", func() {
		r.warnAboutUnclaimedUniqueKeys(ctx)
	})
	// The advisor publishes one last report as it stops, and that write has to
	// finish before this function returns or the process exits out from under
	// it. Started fire-and-forget, it usually won that race on an idle machine
	// and usually lost it on a busy one, which is the worst way for it to
	// behave: what a router learned was kept or dropped depending on how much
	// traffic it had, and the load that makes the advice worth having is the
	// load that lost it.
	adviceDone := make(chan struct{})
	go func() {
		// The close is outside the recovery: whoever waits on this must be
		// released whether the report finished or panicked.
		defer close(adviceDone)
		r.safely("reporting what the cache learned", func() { r.reportAdvice(ctx) })
	}()
	r.goSafely("watching the fast tier for changes this router did not make",
		func() { r.watchFastTier(ctx) })

	r.log.Info("router listening",
		"addr", ln.Addr().String(),
		"fast_tier", r.currentPrimary(),
		"completeness", string(r.cfg.Completeness),
		// An operator who has just moved an application onto this address needs
		// to know whether it still speaks what the application expects.
		"connections", describeTLS(r.cfg.TLS, r.cfg.HotTLS))

	var wg sync.WaitGroup
	for {
		client, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			r.log.Warn("accept failed", "err", err)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.serve(ctx, client)
		}()
	}
	wg.Wait()
	r.cursors.closeAll(context.WithoutCancel(ctx))
	// Bounded, because an archive that cannot be reached must not turn a
	// shutdown into a hang. The report is worth a moment and no more.
	select {
	case <-adviceDone:
	case <-time.After(adviceShutdownWait):
		r.log.Warn("gave up waiting for the final cache advice report",
			"waited", adviceShutdownWait.String(),
			"effect", "what this router learned about query shapes is not published")
	}
	return nil
}

// cursorIdle matches the server's own default cursor timeout, so an abandoned
// archive cursor is cleaned up on the same schedule mongod would use.
const cursorIdle = 10 * time.Minute

func (r *Router) reapCursors(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.cursors.reapIdle(ctx, cursorIdle)
			// The same sweep, for the registrations that remember which claim
			// authorised a fast tier cursor. A client that walks away from one
			// leaves an entry behind, and mongod reaps its own cursors on a
			// timer too.
			r.tiered.reapIdle(cursorIdle)
		}
	}
}

// serve proxies one client connection for its lifetime.
//
// Each client gets its own connection to the fast tier rather than a shared
// pool. Authentication is a stateful conversation bound to a connection, so
// sharing one would mean re-implementing SCRAM here instead of letting the real
// server do it.
func (r *Router) serve(ctx context.Context, client net.Conn) {
	// A panic here takes one client down, not the process.
	//
	// Go kills the whole program for an unrecovered panic in any goroutine, and
	// this one parses whatever a client sends. Without this, a single malformed
	// message, or any defect in a rarely reached branch, would disconnect every
	// other application using this router and stop the proxy. net/http recovers
	// per connection for the same reason.
	//
	// It is not a way to live with panics. The log carries the stack at ERROR
	// and the dashboard counts them, because a panic is a defect that has to be
	// found; what this decides is who pays for it while it is being found.
	//
	// The state a panic could leave behind is this connection's, and it goes
	// with the connection. The shared caches are guarded by mutexes released by
	// defer, or held across nothing that can panic, so unwinding does not leave
	// one locked.
	defer func() {
		if p := recover(); p != nil {
			r.Stats.Panics.Add(1)
			r.log.Error("recovered a panic while serving a client, closing that connection",
				"panic", fmt.Sprint(p),
				"remote", remoteAddr(client),
				"stack", string(debug.Stack()),
				"effect", "this client's connection is closed and it will reconnect; every other "+
					"client is unaffected",
				"action", "this is a defect: report the stack above")
		}
	}()
	defer func() { _ = client.Close() }()
	open := r.Stats.ClientsOpen.Add(1)
	r.Stats.ClientsTotal.Add(1)
	defer r.Stats.ClientsOpen.Add(-1)
	r.warnAboutClientCount(open)

	// One dedicated upstream per client, but only while authentication is
	// proxied. Then the connection carries the client's identity and cannot be
	// anybody else's, so there is nothing to share.
	//
	// With authentication terminated it is borrowed per command from a pool
	// instead, and an idle client holds nothing upstream. See pool.go.
	var upstream net.Conn
	if !r.cfg.TerminateAuth {
		var err error
		upstream, err = r.dialPrimary(ctx)
		if err != nil {
			r.log.Error("cannot reach the fast tier", "err", err)
			return
		}
		defer func() { _ = upstream.Close() }()
	}

	c := &conn{
		r:        r,
		client:   client,
		upstream: upstream,
		log:      r.log,
		id:       r.nextConnID.Add(1),
	}
	// A borrowed connection goes back when the client goes away, and one still
	// holding an open transaction is closed rather than reused: the transaction
	// is abandoned, and mongod should hear that from the socket rather than wait
	// out its timeout with the connection handed to somebody else.
	defer func() {
		if c.txnOpen {
			c.dropPrimary()
			return
		}
		c.returnPrimary(false)
	}()
	// Connections still holding this client's cursors are closed rather than
	// returned, which is what tells the fast tier to drop the cursors.
	defer c.releasePinned()

	if err := c.pump(ctx); err != nil && !isClosed(err) {
		r.log.Debug("connection ended", "err", err)
	}
}

// conn is one client and its dedicated fast tier connection.
type conn struct {
	r        *Router
	client   net.Conn
	upstream net.Conn
	log      *slog.Logger

	// cls is how the command in flight was served. Every path that does
	// something other than hand the command to the fast tier sets it, and pump
	// files the timing under it once the reply has gone out.
	cls Class

	// pendingKeys are unique key claims made for the insert now in flight, kept
	// so they can be released if the fast tier rejects it.
	pendingKeys []string

	// txnKeys is what an update inside a transaction left to be checked once
	// the reply is back and the real post-image can be read.
	txnKeys txnKeyCheck

	// rename is a rename of a tiered collection the fast tier has been asked to
	// perform. The archive follows it once the fast tier has said yes, so its
	// own authorization decides whether it happens at all. See ddl.go.
	rename *pendingRename

	// indexDrop is a unique index the fast tier is being asked to drop, held
	// until the reply says whether it did. See finishIndexDrop.
	indexDrop *pendingIndexDrop

	// aggAuth are the completeness claims that let the fast tier run the
	// aggregation now in flight, held until the reply says whether it left a
	// cursor open. An aggregation is judged before it runs, unlike a find, so
	// there is nowhere else to carry them. See tieredcursor.go.
	aggAuth []nsClaim

	// observedUser is who the fast tier accepted on this connection while
	// authentication is proxied, and observedPending is who is mid-handshake.
	// See observeauth.go.
	observedUser    string
	observedPending string

	// id identifies this connection, which is the identity a cached answer is
	// keyed by while authentication is proxied and the user is unknown here.
	id uint64

	// cacheOf is the key the read in flight will be learned under, set when the
	// cache decided the command was one it could hold. See querycache.go.
	cacheOf cachePending

	// auth is the SCRAM conversation in progress, and grants what the finished
	// one established. grants being nil is what "not authenticated" means, and
	// every command is refused until it is not.
	auth        *authConv
	grants      *authz.Grants
	pendingUser string

	// txnOpen records that a transaction is in flight on the borrowed primary
	// connection, which is the one thing that must not move between commands.
	txnOpen bool

	// dirty means an exchange on the borrowed connection did not finish, so it
	// is closed rather than returned to the pool.
	dirty bool

	// closeAfterReply ends this connection once the reply in hand has gone out.
	// See endAfterFailover for what sets it.
	closeAfterReply bool

	// pinned maps a cursor to the pooled connection that owns it. A cursor
	// exists on one node, so its getMore has to go back there.
	pinned map[int64]*pooled

	// lease is the primary connection borrowed for the command in flight, or
	// held across an open transaction. Nil when authentication is proxied,
	// where this connection owns its upstream outright.
	lease *pooled

	// pendingIndexKeys are the unique indexes whose existing keys were claimed
	// for a createIndexes that has not been answered yet, on pendingIndexNS.
	// They go back if the fast tier turns the build down.
	pendingIndexKeys []string
	pendingIndexNS   string

	// warnedReadPref remembers which non-primary read preferences this client
	// has already been warned about, so a reporting client sending the same one
	// on every query gets one line rather than thousands.
	warnedReadPref map[string]bool
}

func (c *conn) pump(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		msg, err := wire.Read(c.client)
		if err != nil {
			return err
		}
		c.r.Stats.Commands.Add(1)

		// Nothing should arrive compressed, because the handshake never agrees
		// a compressor: the list is removed from the client's hello and from
		// the reply. A client that compresses anyway is sending something this
		// proxy cannot read, and a command it cannot read is a command with no
		// guard on it.
		//
		// It used to be logged and forwarded. Measured against a client that
		// compressed uninvited: an insert of an _id the archive still held was
		// refused with E11000 when sent plainly, and accepted when sent through
		// zlib, landing on the fast tier for the feeder to mirror over the
		// archived record. That is the compression finding still live, reached
		// by ignoring the negotiation rather than by winning it.
		//
		// So it is refused, and the connection goes with it. The client is told
		// why in an uncompressed reply, which the protocol allows, and the
		// connection ends because there is no way to know whether what follows
		// is readable either. A conforming driver never reaches this.
		if msg.Header.OpCode == wire.OpCompressed {
			c.r.Stats.Unreadable.Add(1)
			c.r.Stats.Refused.Add(1)
			c.log.Error("refused a compressed message and closed the connection",
				"why", "this proxy cannot read OP_COMPRESSED, so no guard could run for it",
				"remedy", "remove compressors from the connection string")
			out, cerr := c.r.commandError(0, msg.Header.RequestID,
				"thermocline: refusing a compressed message. This proxy cannot read OP_COMPRESSED, "+
					"so none of its tiering guards could run for it. The handshake does not agree a "+
					"compressor for exactly this reason; remove compressors from the connection string.")
			if cerr == nil {
				_ = c.send(out)
			}
			return fmt.Errorf("compressed message on a connection that never negotiated one")
		}

		// Timed around the whole exchange, so what is recorded is what the
		// client waited for and not what the router thinks it did.
		started := time.Now()
		c.cls = classOf(msg)
		err = c.handle(ctx, msg)
		c.r.Metrics.Record(c.cls, time.Since(started))
		if err != nil {
			return err
		}
		// The reply has gone out. Ending the connection here is what lets the
		// client start again somewhere useful. See endAfterFailover.
		if c.closeAfterReply {
			return nil
		}
	}
}

// handle forwards one message and decides whether the reply needs work.
// answer sends a guard's reply, unless the client asked for no reply at all.
//
// A write with moreToCome set is unacknowledged: the client is not waiting and
// there is nowhere to put a refusal. That is a reason not to perform the write,
// and it was being treated as a reason not to ask whether the write was safe.
// Every guard was skipped for those messages, which is the compression finding's
// shape: a flag the client sets for reasons of its own turning off the copy-up,
// the unique key claim and the upsert judgement all at once, invisibly.
//
// Measured on a collection holding {_id: 5, name: "alice", tier: "gold",
// balance: 100} in the archive only: an unacknowledged
// updateOne({_id:5},{$set:{balance:200}},{upsert:true}) left {_id:5,
// balance:200} on both tiers, an unacknowledged insert of an _id the archive
// still held replaced the archived document, and an unacknowledged deleteMany
// left nine of twelve documents readable after an erasure. All three are shapes
// that were fixed on the acknowledged path long ago.
//
// So the guards now run for these too, and where one would have answered, the
// command is simply not performed. A dropped write is what w:0 already means to
// an application: it asked not to be told.
func (c *conn) answer(msg, out *wire.Message) error {
	if msg.MoreToCome() {
		return nil
	}
	return c.send(out)
}

func (c *conn) handle(ctx context.Context, msg *wire.Message) error {
	name, db, nameErr := msg.CommandName()

	// Borrow the primary for this command. Everything below reaches the fast
	// tier through c.upstream, including the copy-up retry and the permission
	// probe, so it is populated once here rather than threaded through them.
	if err := c.borrowPrimary(ctx); err != nil {
		return err
	}
	defer func() { c.returnPrimary(c.txnOpen) }()
	c.noteTransaction(msg, name, nameErr)

	// Cleared per command. An in-transaction key check that was armed and then
	// never reached, because some path answered the command early, would
	// otherwise be consumed by whatever command came next, against ids from a
	// statement that has already finished.
	c.txnKeys = txnKeyCheck{}
	c.rename = nil
	c.indexDrop = nil

	// A handshake is answered here rather than forwarded, because a pooled
	// connection can only carry one. See answerHello.
	if c.r.cfg.TerminateAuth && nameErr == nil && isHello(name) && !msg.MoreToCome() {
		if out, done := c.answerHello(ctx, msg); done {
			return c.send(out)
		}
	}

	// Authentication and authorization come first, before any guard, because
	// every guard below this reads or writes data and none of them asks who is
	// calling. See gate.go.
	if c.r.cfg.TerminateAuth && nameErr == nil {
		if isSASL(name) && !msg.MoreToCome() {
			out, serr := c.handleSASL(ctx, msg, name, db)
			if serr != nil {
				return serr
			}
			return c.send(out)
		}
		if out, denied := c.gate(msg, name, db); denied {
			// An unacknowledged write carries moreToCome: the client is not
			// waiting for an answer, so there is nowhere to put the refusal.
			// That is a reason not to perform the write, and not a reason to
			// skip asking whether it was allowed. Letting it past because the
			// refusal has nowhere to go would be the quietest bypass in the
			// system: no error anywhere, and the document lands.
			if msg.MoreToCome() {
				user := "unauthenticated"
				if c.grants != nil {
					user = c.grants.User
				}
				c.log.Warn("dropped an unauthorized unacknowledged command",
					"command", name, "db", db, "user", user)
				return nil
			}
			return c.send(out)
		}
	}

	// A cached answer is the whole answer, so it is served before any guard
	// runs: deciding completeness for it would be deciding about a read that is
	// not happening. Authorization has already run above, and the key carries
	// who is asking. See querycache.go.
	if nameErr == nil && !msg.MoreToCome() {
		if out, hit := c.cacheLookup(ctx, msg, name, db); hit {
			return c.send(out)
		}
	}

	// A write drops what was learned about the collection it changed, which is
	// what makes this cache correct for the writer rather than merely fast.
	if nameErr == nil {
		if cmd, cerr := msg.CommandDoc(); cerr == nil {
			c.cacheInvalidate(name, db, cmd)
		}
	}

	// Cursors this router owns never existed on the fast tier, so their
	// follow-ups have to be served here or the driver gets "cursor not found".
	if nameErr == nil && !msg.MoreToCome() {
		if handled, err := c.serveOwnCursors(ctx, msg, name); handled {
			return err
		}
	}

	// Cleared before any guard runs, because a $merge claims unique keys as well
	// as an insert does, and its guard runs further up this function than the
	// write guards below. Left where it was, those claims were forgotten before
	// the reply could give them back.
	c.pendingKeys = nil

	// Aggregations are judged before they run, not on the way back. A pipeline
	// can end in $out or $merge, and acting after the fact would leave a
	// truncated collection already written.
	// Unacknowledged too. A client that sets moreToCome is saying it does not
	// want an answer, not that it does not want the guards: the write path
	// learned that the expensive way, and these two dispatches kept the old
	// habit. Measured, with an archived document nothing else held: an
	// applyOps sent with writeConcern {w:0} walked past the refusal that the
	// same command gets when it is acknowledged, and the archive held the
	// clobber seven seconds later. Where a guard would have answered, the
	// command is simply not performed and nothing is sent back, which is what
	// w:0 already means. See answer.
	if nameErr == nil && name == "aggregate" {
		// Cleared here rather than beside the guard that sets it, so that an
		// aggregation which never reaches that guard cannot have its reply
		// registered against the claims of the one before it. See
		// tieredcursor.go.
		c.aggAuth = nil

		// An aggregation can carry explain: true instead of being wrapped in an
		// explain command, and then it plans rather than runs. Every guard below
		// exists to make a pipeline that runs come out right, and each of them
		// does something to make that so: the merge target guard copies archived
		// documents up, and the completeness guard runs a writing pipeline
		// itself when the input is archived.
		//
		// Doing any of that for an explain is a side effect mongod does not
		// have. Measured: aggregate with $out and explain: true wrote nothing on
		// mongod, and through the router it wrote all ten documents into the
		// target and returned no plan at all. The client asked what would happen
		// and was told nothing, having had it happen.
		//
		// So an explained aggregation is forwarded untouched and its plan is
		// annotated on the way back, which is what the explain command already
		// does.
		cmd, cerr := msg.CommandDoc()
		explainOnly := cerr == nil && inlineExplain(cmd)
		if cerr == nil && !explainOnly {
			// The target of a $merge is judged first and separately from the
			// input. A pipeline reading something untiered, or nothing at all,
			// still writes into a collection that may be tiered, and the merge
			// decides what to do by looking at what it can see there. See
			// mergetarget.go.
			if out, done := c.guardMergeTarget(ctx, msg, cmd, db); done {
				return c.answer(msg, out)
			}
		}
		if !explainOnly {
			if out, done := c.guardAggregate(ctx, msg, db); done {
				return c.answer(msg, out)
			}
		}
	}

	// Renames split a tiered collection between the tiers, and mapReduce reads
	// every document on whichever tier it runs. Neither can be made whole
	// across two clusters, so both are refused rather than answered short. A
	// TTL index is the third: it would reach the fast tier, which holds the
	// recent part, and expire exactly the documents the archive was still
	// keeping while the old ones it is written for go untouched.
	if nameErr == nil &&
		(name == "renameCollection" || name == "mapReduce" || name == "mapreduce" ||
			name == "createIndexes" || name == "collMod" || name == "applyOps" ||
			name == "convertToCapped" || name == "cloneCollectionAsCapped" || name == "create") {
		if out, done := c.guardDDL(ctx, msg, db, name); done {
			return c.answer(msg, out)
		}
	}

	// Who this connection is, while authentication is proxied. Read on the way
	// past, believed only once the fast tier has accepted it. See
	// observeauth.go.
	if nameErr == nil && !c.r.cfg.TerminateAuth && !msg.MoreToCome() {
		if cmd, cerr := msg.CommandDoc(); cerr == nil {
			c.noteProxiedAuth(name, cmd)
		}
	}

	// Which unique indexes a dropIndexes would remove has to be read before the
	// fast tier removes them, because afterwards there is nothing left to ask.
	if nameErr == nil && !msg.MoreToCome() && name == "dropIndexes" {
		if cmd, cerr := msg.CommandDoc(); cerr == nil {
			if coll, ok := stringField(cmd, "dropIndexes"); ok {
				c.noteIndexDrop(ctx, cmd, db, coll)
			}
		}
	}

	// dbHash reads every document and returns one hash, which cannot be
	// corrected across two tiers. See dbhash.go.
	if nameErr == nil && !msg.MoreToCome() && (name == "dbHash" || name == "dbhash") {
		if out, done := c.guardDBHash(ctx, msg, db); done {
			return c.send(out)
		}
	}

	// A command that asks about a tiered collection rather than for its
	// documents is answered by the fast tier alone, and a fast tier that has
	// never held the collection says it does not exist. Give it one first. See
	// describe.go for the four that were told so while every read worked.
	if nameErr == nil && !msg.MoreToCome() {
		c.ensureDescribable(ctx, msg, name, db)
	}

	// A cursor the fast tier was allowed to answer because a claim covered it,
	// asked for its next batch after that claim has narrowed. See
	// tieredcursor.go for what the alternative was measured to be.
	if nameErr == nil && name == "getMore" && !msg.MoreToCome() {
		if out, done := c.guardTieredGetMore(ctx, msg); done {
			return c.send(out)
		}
	}

	// count and distinct read documents the same way find does, so they need
	// the same completeness check.
	if nameErr == nil && (name == "count" || name == "distinct") && !msg.MoreToCome() {
		if out, done := c.guardSimpleRead(ctx, msg, db, name); done {
			return c.send(out)
		}
	}

	// A write naming many documents only reaches the ones the fast tier holds,
	// so the rest have to come up before it runs. Before, not after: repeating
	// the write after a copy-up would apply $inc twice.
	//
	// A write naming one document is left to the reply, where interceptWrite
	// resolves it against the archive and runs it again. Unless there is no
	// reply: an unacknowledged single-document write is judged here too,
	// because nothing downstream will judge it. See guardMultiWrite.
	if nameErr == nil && (name == "update" || name == "delete") {
		if out, done := c.guardMultiWrite(ctx, msg, db, name); done {
			return c.answer(msg, out)
		}
	}

	// A sorted findAndModify picks one of several matches, and the fast tier
	// can only pick from the part it holds. Also before the write: by the time
	// a reply could be inspected the update has been applied to the wrong
	// document.
	//
	// Not gated on the reply. An unacknowledged findAndModify has no reply at
	// all, so the sort, the bare filter and the _id are all this guard's to
	// settle. See guardSortedFindAndModify.
	if nameErr == nil &&
		(name == "findAndModify" || name == "findandmodify") {
		if out, done := c.guardSortedFindAndModify(ctx, msg, db); done {
			return c.answer(msg, out)
		}
	}

	// An upsert that matches nothing does not wait to be put right on the way
	// back: it creates a document in place of the archived one and the feeder
	// then mirrors that over the archive. So it too is judged before it runs.
	if nameErr == nil &&
		(name == "update" || name == "findAndModify" || name == "findandmodify") {
		if out, done := c.guardUpsert(ctx, msg, db, name); done {
			return c.answer(msg, out)
		}
	}

	// bulkWrite is guarded before the switch below, because it is not shaped
	// like the commands there: it names its collections in nsInfo and its
	// operations point at them by position, so none of the per-command guards
	// can find a collection to act on.
	if nameErr == nil && name == "bulkWrite" {
		if cmd, cerr := msg.CommandDoc(); cerr == nil {
			if out, done := c.guardBulkWrite(ctx, msg, cmd); done {
				return c.answer(msg, out)
			}
		}
	}

	// A unique key has to be claimed before the write lands, or two writes
	// racing both see a free slot.
	if nameErr == nil {
		switch name {
		case "insert":
			if out, done := c.guardInsert(ctx, msg, db); done {
				return c.answer(msg, out)
			}
		case "update", "findAndModify", "findandmodify":
			// An update can move a document onto a value an archived document
			// holds, which the fast tier's unique index cannot see any more than
			// it can for an insert. It runs after guardMultiWrite so the matched
			// documents are all present to compute keys from.
			//
			// Once a transaction is open, the documents this update will touch
			// can include ones the transaction has written and not committed,
			// which are visible only from inside its own session. There the
			// guard runs around the write instead of before it, and refuses by
			// aborting. The statement that opens the transaction is not a
			// special case for correctness, because a transaction cannot have
			// written anything before its first command.
			cmd, cmdErr := msg.CommandDoc()
			t, inTxn := txnFields{}, false
			if cmdErr == nil {
				t, inTxn = txnOf(cmd)
			}
			if inTxn && t.open {
				// Only notes what the statement is about to touch. The check
				// itself happens after the reply, below, so that everything
				// else this command needs, above all the copy-up an update
				// that matched nothing depends on, still runs untouched.
				c.prepareTxnKeyCheck(ctx, msg, cmd, db, name, t)
			} else if out, done := c.guardUpdateKeys(ctx, msg, db, name); done {
				return c.answer(msg, out)
			}
		}
	}

	// Every read is served by the fast tier's primary. Say so, because this
	// router answers as a mongos and a mongos would have routed it.
	if nameErr == nil && !msg.MoreToCome() {
		c.noteReadPreference(msg, name)
	}

	// A client that asked for exhaust replies is answered once instead.
	//
	// The server sends several replies to one request when this bit is set, and
	// this proxy reads one. The others stay in the socket and answer whatever
	// command comes next. Measured: one getMore with the bit set produced five
	// replies from mongod, and through the router the answer to the following
	// ping was a batch of that cursor's documents. See ClearExhaustAllowed.
	if msg.ExhaustAllowed() {
		c.log.Debug("clearing the exhaust bit; this proxy answers one reply per request",
			"command", name)
		msg = msg.ClearExhaustAllowed()
	}

	// A handshake that agrees a compressor would make every later command
	// unreadable here, and an unreadable command is an unguarded one.
	if nameErr == nil && isHello(name) {
		msg = c.unnegotiateCompression(msg)
		// A driver embeds a saslStart in hello to save a round trip. Forwarded
		// on the router's own connection it would try to authenticate that
		// connection as the client, which is not what anybody wants. See
		// stripSpeculativeAuth.
		if c.r.cfg.TerminateAuth {
			msg = c.dropSpeculativeAuth(msg)
			msg = c.dropClientMetadata(msg)
		}
	}

	// A read that asked for a secondary goes to one, when there is one to use
	// and nothing about the command ties it to the primary. Everything else
	// travels on this client's own primary connection exactly as before.
	up := c.upstream
	borrowed, fromPool := c.borrowUpstream(ctx, msg, name)
	if fromPool {
		up = borrowed.conn
	}

	// A failover shows itself two ways, and both end here rather than at the
	// client. The connection dies, because a primary stepping down closes them;
	// or the connection survives and the command comes back refused. See
	// failover.go for which commands may be sent again and which must not.
	var reply *wire.Message
	var err error

	if werr := wire.Write(up, msg); werr != nil {
		if fromPool {
			// Half an exchange happened on it at most, so it cannot be reused.
			borrowed.discard()
			c.unpinExhausted(borrowed)
			return fmt.Errorf("forward to fast tier: %w", werr)
		}
		c.upstreamBroke()
		out, replayed := c.replayAfterFailover(ctx, msg, name)
		if !replayed {
			return fmt.Errorf("forward to fast tier: %w", werr)
		}
		reply = out
	} else {
		// Fire-and-forget writes expect no reply. Sending one would leave the
		// connection a message out of step for everything that follows.
		//
		// Any unique key this write claimed stays claimed. Whether the write
		// landed is exactly what an unacknowledged write does not say, and of
		// the two ways to be wrong, holding a value that turned out to be free
		// costs a later collision to clear, while releasing one the write did
		// take would hand the same value to somebody else.
		if msg.MoreToCome() {
			return nil
		}

		reply, err = wire.Read(up)
		// A reply that says more is coming means the server is mid-stream and
		// this connection has messages on it that belong to the request just
		// made. The exhaust bit is cleared on the way out so this should be
		// unreachable, and if it is ever reached the connection cannot be
		// handed to anybody else: the next borrower would be answered with
		// these documents. Throw it away instead.
		if err == nil && reply.MoreToCome() {
			c.log.Error("the fast tier answered with more replies to come; discarding the connection",
				"command", name)
			if fromPool {
				borrowed.discard()
				c.unpinExhausted(borrowed)
			} else {
				c.upstreamBroke()
			}
			return fmt.Errorf("unexpected exhaust reply from the fast tier for %q", name)
		}
		if fromPool {
			if err != nil {
				borrowed.discard()
				c.unpinExhausted(borrowed)
				return fmt.Errorf("read from fast tier: %w", err)
			}
			c.keepOrRelease(borrowed, reply)
		} else if err != nil {
			c.upstreamBroke()
			out, replayed := c.replayAfterFailover(ctx, msg, name)
			if !replayed {
				return fmt.Errorf("read from fast tier: %w", err)
			}
			reply = out
		} else if !fromPool {
			if rdoc, derr := reply.ReplyDoc(); derr == nil && isFailoverReply(rdoc) {
				// Whether or not this command can be sent again, the address
				// this router is pinned to is wrong from here on.
				c.r.primaryMoved()
				if out, replayed := c.replayAfterFailover(ctx, msg, name); replayed {
					reply = out
				} else {
					c.endAfterFailover(ctx)
				}
			}
		}
	}
	// Every reply from the fast tier says what time the cluster thinks it is,
	// so this costs a lookup on a document already in hand. A read that later
	// falls through to the archive waits for mirroring to reach that instant.
	// See freshness.go.
	c.r.noteOpTime(reply)

	if nameErr != nil {
		return c.send(reply)
	}

	// A unique index the fast tier turned down never happened either, and the
	// keys for it were claimed before the command was forwarded. See
	// newuniqueindex.go for why that order.
	c.releaseIndexKeysIfBuildFailed(ctx, reply)

	// An insert the fast tier rejected never happened, so its key claims have
	// to go back, or that value is blocked until something else collides.
	if len(c.pendingKeys) > 0 {
		if rdoc, derr := reply.ReplyDoc(); derr != nil || !replyOK(rdoc) || writeFailed(rdoc) {
			c.r.releaseKeys(ctx, c.pendingKeys)
		}
		c.pendingKeys = nil
	}

	// An index change makes what this router remembers about the collection
	// wrong, and the window that matters most is right after somebody declares
	// a field unique.
	if cmd, cerr := msg.CommandDoc(); cerr == nil {
		if coll, ok := indexDDL(cmd, name); ok {
			// Stale rather than forgotten, and then read again on purpose.
			// Forgetting leaves nothing for the next listing to be compared
			// against, and that comparison is what notices a unique index has
			// gone: it is what says so in the log and what releases the claims
			// the index made. Reading it here rather than leaving it to the
			// next write, because a dropIndexes is often the last thing an
			// operator does to a collection for a while.
			c.r.staleIndexes(db + "." + coll)
			// Before BumpIndexGen below, which is not a detail: bumping the
			// generation makes the next completeness read drop the cached
			// listing, and this comparison needs it.
			c.r.refreshIndexes(ctx, db+"."+coll)
			c.r.forgetHint(db + "." + coll)
			c.r.forgetView(db + "." + coll)
			// And tell the other routers. They read the claim on the hot path
			// anyway, so the number rides there. Best effort, because the DDL
			// has already been applied and failing the client's command over a
			// bookkeeping write would be worse than the disagreement this
			// exists to shorten.
			//
			// This said "no change stream carries index DDL", which stopped
			// being true when the fast tier watch started asking for
			// showExpandedEvents: on 6.0 and up another router hears the
			// createIndexes itself. See watchFastTier and
			// noteFastTierIndexDDL. Two mechanisms rather than one on purpose,
			// and a comment asserting the opposite of what another file does is
			// how a reader ends up trusting the wrong one:
			//
			//   - showExpandedEvents is 6.0 and up, and this supports 4.2,
			//     where passing it makes the whole stream fail to open. Below
			//     6.0 this is the only way another router hears anything.
			//   - a router whose watch is down, for a privilege it lacks or a
			//     tier it cannot reach, still reads claims.
			//
			// So the generation is the floor and the stream is the fast path.
			if c.r.cfg.Manages(db + "." + coll) {
				if berr := c.r.marks.BumpIndexGen(ctx, db+"."+coll); berr != nil {
					c.log.Warn("could not tell the other routers the unique indexes changed",
						"ns", db+"."+coll, "err", berr,
						"effect", "another router keeps its old listing for up to a minute, and "+
							"does not claim keys for an index it has not seen",
						"action", "run cline keys --rebuild, or restart the routers to have it "+
							"enforced everywhere at once")
				}
			}
			// drop and create are the two that change which collection lives
			// under a name, and either half can arrive here without the other:
			// a collection dropped out of band and created through this router
			// leaves it remembering the one before.
			if name == "drop" || name == "create" {
				// The fast tier holds none of it either way: a drop removed it
				// and a create makes it empty. A cached claim that it holds all
				// of it is wrong from this instant, and this is the cache whose
				// staleness decides whether a read skips the archive, so it is
				// the one that should be cleared soonest rather than last.
				//
				// create was missing while its two neighbours below had it, on
				// the reasoning written above them. The window is the claim
				// cache TTL, five seconds, because the feeder sees the drop on
				// the fast tier's change stream and withdraws the recorded
				// claim whichever way the drop arrived. Five seconds of reads
				// answered from an empty collection is still worth not having.
				//
				// No reproduction to point at, and that is stated rather than
				// dressed up: the check written for it passes either way,
				// because a create does enough work of its own, mirroring the
				// collection and starting an index build, that the cache has
				// expired before the next read. It is here because holding a
				// claim that the fast tier holds everything, moments after
				// making it empty, is wrong on its face.
				//
				// One list, shared with the drop this router hears about on
				// the fast tier's change stream rather than performs. They
				// were written separately and had already drifted: this one
				// left the remembered hint and the remembered view standing,
				// so a collection dropped and recreated here went on having a
				// dropped partial index's filter reproduced against the
				// archive for up to a minute, which asks a narrower question
				// than the client asked and answers short.
				//
				// The indexer's half of that list is the one worth naming
				// here: it records a namespace as done once the fast tier has
				// the collection and its indexes, and never asks again. A drop
				// makes that untrue, and without clearing it the collection
				// comes back with nothing but _id_, every query on it scans,
				// and a unique index the archive still carries is not enforced
				// on the fast tier at all.
				c.r.forgetDropped(db + "." + coll)
				// And the archive's copy, when what it holds under that name is
				// a view. A view has no documents, so the rule that a drop is
				// never replayed does not reach it. See dropArchiveView.
				c.r.dropArchiveView(ctx, db+"."+coll)
			}
			if name == "create" {
				ns := db + "." + coll
				// The archive's copy is created by the feeder's first mirrored
				// insert, which carries no options at all, so a collection
				// created here with a collation is a different collection down
				// there. Synchronously, and before the index build below,
				// because the write that follows a create can be immediate and
				// the feeder is mirroring it within the second.
				//
				// A view arrives as a create too, carrying viewOn and a
				// pipeline, and it is mirrored whether or not anything tiers
				// it. A view holds no documents, so nothing ever mirrors one:
				// the archive would not have it, and repointing the
				// application at the archive is what rollback is. The runbook
				// already says an adoption's views have to exist on both
				// sides, and this keeps that true for the ones created
				// afterwards.
				_, viewErr := cmd.LookupErr("viewOn")
				isView := viewErr == nil
				if isView || c.r.cfg.Manages(ns) {
					promote.MirrorCollection(ctx, c.r.hot, c.r.cold, db, coll, c.log)
				}

				// Forgetting only clears the record that it was done; the
				// build happens on the next Ensure, and without this that
				// waited for a client to list the catalog. The collection
				// exists on the fast tier from this moment and has nothing but
				// _id_, so this is exactly when there is something to do.
				//
				// In the background and rate limited inside the indexer, the
				// same as the catalog path.
				c.r.goSafely("building the archive's indexes on the fast tier", func() {
					c.r.indexer.Ensure(context.WithoutCancel(ctx), ns)
				})
			}
			// A collection's collation is fixed at creation, so the only way it
			// changes is a drop and a recreate, which comes through here.
			c.r.forgetCollation(db + "." + coll)
		}
		// dropDatabase names no collection, so nothing above sees it, and every
		// collection it removed is one this router still remembers something
		// about. The query cache already had its own case for this; the rest
		// did not. See forgetEverythingIn.
		if name == "dropDatabase" {
			c.r.forgetEverythingIn(db)
		}
	}

	// And the fast tier's answer to it, before replyHello answers a handshake
	// and returns.
	if !c.r.cfg.TerminateAuth && c.observedPending != "" {
		if rdoc, derr := reply.ReplyDoc(); derr == nil {
			c.confirmProxiedAuth(name, rdoc)
		}
	}

	if isHello(name) {
		return c.replyHello(msg, reply)
	}

	// A change stream on a tiered collection reports the evictor's removals as
	// deletes and copy-ups as inserts. Both are this system's own work, and
	// neither happened as far as the application is concerned.
	if name == "aggregate" {
		if cmd, cerr := msg.CommandDoc(); cerr == nil {
			// The reply is rewritten when this batch carried tiering events,
			// which is what a consumer resuming from a stored token reads.
			//
			// Carried on rather than sent from here, so that everything an
			// aggregate reply is still put through -- registering the claims a
			// getMore will be judged against, above all -- sees exactly what the
			// client will. Returning early skipped all of it.
			if out, filtered := c.noteChangeStream(ctx, cmd, db, reply); filtered {
				reply = out
			}
		}
	}
	if name == "getMore" {
		// A cursor that has just been drained has no more getMores to guard, so
		// the claim it was authorised by is forgotten here rather than left for
		// the idle reaper. Without this every tiered cursor a BI tool opens sits
		// in that map for the ten minutes mongod would keep the cursor itself.
		c.forgetExhaustedTieredCursor(msg, reply)
		if out, filtered := c.filterChangeStream(ctx, msg, reply); filtered {
			return c.send(out)
		}
		// A later batch of a listCollections cursor still carries this
		// router's own scratch collections, because only the first batch is
		// filtered where the archive's half is added. See listcollections.go.
		if out, filtered := c.filterListCollectionsGetMore(msg, reply); filtered {
			return c.send(out)
		}
		// A cursor neither this router nor the fast tier knows. See
		// cursorlost.go for the two things it can be and why the id cannot say
		// which.
		if out, said := c.explainCursorNotFound(msg, reply); said {
			return c.send(out)
		}
	}
	if name == "killCursors" {
		if cmd, cerr := msg.CommandDoc(); cerr == nil {
			for _, id := range cursorIDsOf(cmd) {
				c.r.changeStreams.drop(id)
			}
		}
	}

	// An explain describes the fast tier, which may not be where the query it
	// explains would run. Say so beside the plan rather than instead of it.
	// A rename the fast tier accepted has an archive to move behind it.
	if c.rename != nil && name == "renameCollection" {
		rdoc, derr := reply.ReplyDoc()
		c.finishRename(ctx, derr == nil && replyOK(rdoc))
	}
	// And a unique index the fast tier dropped has one to drop behind it, or
	// the archive goes on refusing values the router now accepts.
	if c.indexDrop != nil && name == "dropIndexes" {
		rdoc, derr := reply.ReplyDoc()
		if cmd, cerr := msg.CommandDoc(); cerr == nil {
			c.finishIndexDrop(ctx, derr == nil && replyOK(rdoc), cmd)
		}
	}

	if name == "explain" || (name == "aggregate" && inlineExplainOf(msg)) {
		if out, done := c.annotateExplain(ctx, msg, db, reply); done {
			return c.send(out)
		}
	}

	rewritten, err := c.intercept(ctx, msg, name, db, reply)
	if err != nil {
		c.log.Warn("routing failed, returning the fast tier's answer",
			"command", name, "err", err)
		return c.send(reply)
	}
	// Again, on what the interception produced, because it may have done the
	// write the client asked for.
	//
	// A deleteOne naming a document the fast tier does not hold matches nothing
	// there, and that first reply is the one noted above. The interception then
	// copies the document up and runs the delete again, and *that* is when the
	// document goes. Noting only the first reply leaves the barrier waiting for
	// an instant before the delete, so a read that falls through a moment later
	// is told mirroring has caught up when the tombstone is still in flight,
	// and the archive answers with the document the client has just deleted.
	//
	// Measured with the feeder given a backlog to fall behind on: one round in
	// thirty deleted a document, was told deletedCount 1, and counted it again
	// immediately afterwards. stale_reads stayed at zero throughout, because
	// the barrier believed it had caught up. Taking the maximum, so a second
	// call can only move it forward.
	c.r.noteOpTime(rewritten)

	// Inside a transaction the unique key guard runs here, on what the write
	// actually produced, rather than on a guess made before it ran. Nothing is
	// visible outside the transaction yet, so a duplicate can still be refused
	// by aborting it.
	if out, done := c.verifyTxnKeys(ctx, msg, rewritten, name); done {
		return c.send(out)
	}
	// What the client is actually given is what is learned from, whichever tier
	// it came from and whatever was rewritten on the way.
	c.cacheStore(rewritten)
	return c.send(rewritten)
}

// replyHello rewrites the handshake so the driver stays with this proxy.
func (c *conn) replyHello(msg, reply *wire.Message) error {
	doc, err := reply.ReplyDoc()
	if err != nil {
		return c.send(reply)
	}
	out, err := mongosIdentity(doc)
	if err != nil {
		return c.send(reply)
	}
	// A driver that embedded a saslStart in hello gets no speculative result
	// back, so it falls back to an explicit conversation, which is the one this
	// router terminates. The legacy OP_QUERY handshake cannot be rewritten on
	// the way out to the fast tier, so this is where that case is caught: the
	// saslStart reached the fast tier, nothing ever continues it there, and the
	// upstream connection keeps the identity the router gave it.
	if c.r.cfg.TerminateAuth {
		out = dropSpeculativeResult(out)
	}

	var rewritten *wire.Message
	if msg.Header.OpCode == wire.OpQuery {
		rewritten, err = wire.NewReply(reply.Header.RequestID, msg.Header.RequestID, out)
	} else {
		rewritten, err = wire.NewMsg(reply.Header.RequestID, msg.Header.RequestID, out)
	}
	if err != nil {
		return c.send(reply)
	}
	return c.send(rewritten)
}

// currentPrimary returns the fast tier address raw connections use.
func (r *Router) currentPrimary() string {
	r.primaryMu.RLock()
	defer r.primaryMu.RUnlock()
	return r.primary
}

// refreshPrimary asks the fast tier which member is currently primary.
//
// Raw byte proxying needs one address, but a replica set moves. Re-resolving
// through the driver, which already tracks topology, keeps a failover from
// being permanent.
func (r *Router) refreshPrimary(ctx context.Context) error {
	var hello struct {
		Primary string   `bson:"primary"`
		Me      string   `bson:"me"`
		Hosts   []string `bson:"hosts"`
	}
	err := r.hot.Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
	if err != nil {
		return fmt.Errorf("resolve fast tier primary: %w", err)
	}

	addr := hello.Primary
	if addr == "" {
		addr = hello.Me
	}
	if addr == "" {
		return errors.New("fast tier reported no primary address")
	}

	r.primaryMu.Lock()
	changed := r.primary != addr
	old := r.primary
	r.primary = addr
	r.members = hello.Hosts
	r.primaryMu.Unlock()

	if changed {
		// Idle connections to the member that is no longer primary are useless
		// and worse than useless: handing one out means a write refused with
		// "not primary" for a reason the client cannot act on. The pool for the
		// new address fills itself on demand.
		if old != "" {
			r.closePool(old)
		}
		r.log.Info("fast tier primary resolved", "addr", addr)
	}
	return nil
}

// primaryProbeEvery is the shortest interval between two re-resolutions
// prompted by a refusal.
const primaryProbeEvery = time.Second

// primaryProbeWait bounds one such lookup. It is generous enough to cross an
// ordinary election and short enough that a fast tier with no primary at all
// does not accumulate probes.
const primaryProbeWait = 5 * time.Second

// primaryMoved re-resolves the pinned fast tier address after a member has said
// it is no longer the primary.
//
// dialPrimary already re-resolves when a dial fails, which covers a member that
// died. A stepDown is the other half and it looks nothing like that: the member
// stays up, keeps accepting connections and answers every command with
// NotPrimaryNoSecondaryOk, so nothing ever fails to dial and nothing ever asks
// again. Measured before this existed, with `terminate_auth` off so no command
// could be replayed: one stepDown on the fast tier, and seven minutes later
// every command through the router still failed, including from clients that
// connected after the election was over. The pinned address never moved, and
// the router said nothing about it.
//
// The command that prompted this still fails, which is honest: whether it was
// applied is exactly what a lost acknowledgement does not say, and replaying it
// is decided in failover.go on much narrower grounds. What this buys is that
// the next command reaches the new primary rather than the same secondary.
//
// It runs in the background because the caller is holding a client's reply. The
// driver tracks the topology continuously but its own view may still be mid
// election, and waiting for that here would stall a connection that has an
// answer to deliver.
func (r *Router) primaryMoved() {
	r.primaryProbeMu.Lock()
	if r.primaryProbing != nil || time.Since(r.primaryProbeAt) < primaryProbeEvery {
		r.primaryProbeMu.Unlock()
		return
	}
	done := make(chan struct{})
	r.primaryProbing = done
	r.primaryProbeAt = time.Now()
	r.primaryProbeMu.Unlock()

	go func() {
		// Releasing the waiters is outside the recovery and in a defer, so it
		// happens whether the probe finished or panicked. Inside, a panic would
		// leave primaryProbing set and every caller of awaitPrimaryProbe
		// waiting on a channel nothing will ever close: the process used to die
		// instead, which at least did not hang.
		defer func() {
			r.primaryProbeMu.Lock()
			r.primaryProbing = nil
			r.primaryProbeMu.Unlock()
			close(done)
		}()
		r.safely("finding the fast tier's primary", func() {
			ctx, cancel := context.WithTimeout(context.Background(), primaryProbeWait)
			defer cancel()
			if err := r.refreshPrimary(ctx); err != nil {
				r.log.Warn("the fast tier refused a command as not primary, and which member took over could not be resolved",
					"pinned", r.currentPrimary(), "err", err)
			}
		})
	}()
}

// awaitPrimaryProbe waits for a re-resolution already under way, if there is
// one.
//
// Without this the fix above is a race the client can win. A driver retries as
// soon as it sees the refusal, and the connection it opens dials whatever
// address this router is pinned to, so a retry that arrives before the lookup
// finishes lands on the same member that just refused it. Measured: the retry
// succeeded three times out of four and failed the fourth, which is the worst
// kind of working.
//
// The wait costs the refused command about a second, on a command that has
// already failed, and it is bounded twice over: by the probe's own timeout and
// by the caller's context. A driver talking straight to the replica set would
// be blocking in server selection for longer.
func (r *Router) awaitPrimaryProbe(ctx context.Context) {
	r.primaryProbeMu.Lock()
	done := r.primaryProbing
	r.primaryProbeMu.Unlock()
	if done == nil {
		return
	}
	timer := time.NewTimer(primaryProbeWait)
	defer timer.Stop()
	select {
	case <-done:
	case <-ctx.Done():
	case <-timer.C:
	}
}

// primaryWait is how long a new connection waits for the fast tier to have a
// primary, mirroring a driver's serverSelectionTimeoutMS.
//
// This matters because of an asymmetry the proxy creates. Talking to a replica
// set directly, a driver that loses the primary blocks in server selection
// until a new one is elected, and the application sees a pause. Talking to the
// router, the client only knows about one server and that server stays up, so
// there is no server selection to do: a failed dial upstream came straight back
// as an error and the write failed outright. Measured during an election, six
// writes a second apart failed rather than pausing. Waiting here restores the
// behaviour the application would have had without the proxy.
const primaryWait = 20 * time.Second

func (r *Router) dialPrimary(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	deadline := time.Now().Add(primaryWait)

	var lastErr error
	for attempt := 0; ; attempt++ {
		addr := r.currentPrimary()
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			secured, terr := r.upstreamTLS(conn, addr)
			if terr != nil {
				return nil, terr
			}
			if attempt > 0 {
				r.log.Info("fast tier reachable again", "addr", addr, "waited", time.Since(deadline.Add(-primaryWait)).Round(time.Second))
			}
			return secured, nil
		}
		lastErr = err

		// The address is stale after a failover, and during the election there
		// may be no primary at all to re-resolve to.
		_ = r.refreshPrimary(ctx)

		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("dial %s: %w", addr, lastErr)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func isClosed(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

// forgetEverythingIn drops what this router remembers about every namespace it
// manages in one database.
//
// For dropDatabase, which removes them all at once and names none of them. The
// per collection path is driven by indexDDL, and a command naming no collection
// never reaches it, so a database dropped through the router left every cached
// claim and collation in it standing.
//
// The claim recorded in the archive is a separate matter and outlives this: the
// warmer notices a fast tier that has lost its contents and withdraws it. This
// closes the router's own window, which is the one that opens immediately.
func (r *Router) forgetEverythingIn(db string) {
	prefix := db + "."
	for _, ns := range r.cfg.Managed {
		if !strings.HasPrefix(ns, prefix) {
			continue
		}
		// Deliberately the same list as forgetDropped, which is what a drop of
		// one collection runs, and deliberately without forgetIndexes for the
		// reason written there: the unique index listing is the one memory whose
		// staleness is a refusal rather than a wrong answer, and re-reading it
		// now would replace a listing that is merely old with an empty one, off
		// a database that has just gone. Empty is the direction that lets a
		// duplicate of an archived document through, and the archive still holds
		// every document the constraint covered, because a drop is never
		// replayed there.
		//
		// This list had it and forgetDropped did not, which is the drift a
		// single list exists to stop. No reproduction: the listing expires on
		// indexRefresh anyway, so the window is a minute rather than for ever.
		// It is here because two lists disagreeing about which way a cache is
		// allowed to be stale is how the next reader gets it wrong.
		r.forgetDropped(ns)
	}
}

// remoteAddr names a client for a log line, without assuming there is one: a
// connection can be closed by the time a panic is being reported.
func remoteAddr(c net.Conn) string {
	if c == nil {
		return "unknown"
	}
	a := c.RemoteAddr()
	if a == nil {
		return "unknown"
	}
	return a.String()
}
