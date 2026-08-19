// Package config loads and validates the shared Thermocline configuration.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/zelmario/thermocline/internal/meta"
)

// UnannouncedDeletes decides what the feeder does with a delete on the fast
// tier that the evictor did not announce. See Feeder.Tombstone for why this
// matters.
type UnannouncedDeletes string

const (
	// Tombstone treats it as an application delete and marks the cold copy.
	// This is correct whenever the evictor is the only thing removing data.
	Tombstone UnannouncedDeletes = "tombstone"
	// Ignore leaves the cold copy readable. Safe against accidental data loss,
	// at the cost of resurrecting documents the application deleted.
	Ignore UnannouncedDeletes = "ignore"
	// Halt stops the feeder and alerts. For operators who would rather have
	// mirroring stop than guess.
	Halt UnannouncedDeletes = "halt"
)

// Config is the whole of cline.yaml.
type Config struct {
	Hot     Tier          `yaml:"hot"`
	Cold    Tier          `yaml:"cold"`
	Log     LogConfig     `yaml:"log"`
	Meta    MetaConfig    `yaml:"meta"`
	Feeder  FeederConfig  `yaml:"feeder"`
	Evictor EvictorConfig `yaml:"evictor"`
	Warmer  WarmerConfig  `yaml:"warmer"`
	Router  RouterConfig  `yaml:"router"`
}

// MetaConfig names the database Thermocline keeps its own bookkeeping in.
type MetaConfig struct {
	// DB is the bookkeeping database, on the cold tier for everything except
	// the unique key registry, which has to live beside the data it constrains
	// on the fast tier. Empty means the default, which is what every single
	// deployment wants.
	//
	// It exists because one pair of tiers can carry two Thermocline
	// deployments, and two of them sharing this database would each read the
	// other's watermarks, residency holds and singleton markers as their own.
	// A feeder id already separates checkpoints; this separates the rest.
	DB string `yaml:"db"`
}

// LogConfig says where the daemons write their logs.
type LogConfig struct {
	// File is appended to, and created if missing. Empty means stderr, which
	// is right under systemd because journald captures the stream.
	//
	// Setting it behaves like mongod's systemLog.path: everything goes to the
	// file and nothing to the terminal. Rotation is logrotate's job; send
	// SIGHUP after rotating and the daemon reopens the file.
	File string `yaml:"file"`
}

// Completeness decides what the router does with a query whose answer the fast
// tier cannot guarantee is whole.
type Completeness string

const (
	// Archive answers the query from the cold tier instead. The archive holds
	// every document, so it can answer in full what the fast tier cannot, and
	// the caller gets a correct result rather than an error.
	//
	// This is the default because the point of the system is that applications
	// do not change. Refusing a countDocuments is safe and still a broken
	// promise. The cost is that such a query runs at archive speed, and the
	// router counts them so an operator can see how often it happens.
	Archive Completeness = "archive"
	// Strict refuses the query. The fast tier returning some of the matching
	// documents looks exactly like it returning all of them, and a short answer
	// reported as success is the worst failure this system has. Use this when
	// you would rather an application break loudly than run slowly.
	Strict Completeness = "strict"
	// Permissive answers from the fast tier anyway and counts it. This returns
	// short results and calls them success, so it exists only for deployments
	// that have decided approximate reads are acceptable.
	Permissive Completeness = "permissive"
)

// RouterConfig tunes the proxy that fronts both tiers.
type RouterConfig struct {
	// Listen is the address clients connect to, taking over the one the
	// application already dials.
	Listen string `yaml:"listen"`
	// Completeness handles queries reaching past what the fast tier holds.
	Completeness Completeness `yaml:"completeness"`
	// Residency is how long a copied-up document is protected from eviction.
	Residency Duration `yaml:"residency"`
	// ArchiveWait is how long a read that falls through to the archive will
	// wait for mirroring to reach the moment the read started.
	//
	// The archive trails the fast tier by the feeder's lag, so without this a
	// fallback read can miss a write that already returned, or return a
	// document that was already deleted. Waiting costs nothing while the feeder
	// keeps up, which is the normal case. Set it to zero to answer immediately
	// and accept secondary-read freshness.
	ArchiveWait Duration `yaml:"archive_wait"`
	// Dashboard is the address the operator dashboard listens on, empty for
	// off. It is deliberately a separate address from Listen: the page names
	// collections and reports traffic, so it belongs on an operator network
	// rather than wherever the application already connects.
	Dashboard string `yaml:"dashboard"`
	// TLS is the certificate clients connect with, empty for a plaintext
	// listener.
	TLS TLSConfig `yaml:"tls"`
	// Cache holds query results for as long as they have been observed to stay
	// the same. Off unless enabled.
	Cache CacheConfig `yaml:"cache"`
	// HotTLS says the fast tier requires TLS, taken from hot.uri at load time.
	// The driver reads that option for its own connections; the router also
	// proxies raw bytes over a connection it opens itself, and that one has to
	// be wrapped the same way or it speaks plaintext at a server that will not
	// answer.
	HotTLS bool `yaml:"-"`
	// HotTLSInsecure says the fast tier's certificate is not to be verified,
	// taken from tlsInsecure or tlsAllowInvalidCertificates in hot.uri. Self
	// signed certificates are ordinary inside a private network and refusing
	// them here would only mean the option is set in one place and not the
	// other.
	HotTLSInsecure bool `yaml:"-"`
	// HotTLSCAFile is the authority the fast tier's certificate is checked
	// against, from tlsCAFile in hot.uri.
	HotTLSCAFile string `yaml:"-"`
	// ServiceUser, ServicePass and ServiceAuthDB are the router's own
	// credentials, copied from hot.uri at load time rather than configured
	// twice. They are only used when TerminateAuth is on.
	ServiceUser   string `yaml:"-"`
	ServicePass   string `yaml:"-"`
	ServiceAuthDB string `yaml:"-"`
	// TerminateAuth makes the router authenticate clients itself instead of
	// passing their SCRAM conversation through to the fast tier.
	//
	// Off by default, and it should stay off until the deployment has been
	// tested with it on, because it moves the decision about who may read what
	// from the database into this proxy. With it on the router verifies the
	// client's password against the credentials mongod stores, loads that
	// user's privileges, and refuses anything they do not cover. With it off
	// the fast tier decides, as it always has, and read preference, failover
	// writes and connection pooling remain limited by the single authenticated
	// upstream connection.
	//
	// The router's own account then needs read on admin.system.users, to verify
	// passwords, and it should have nothing else it does not already need.
	TerminateAuth bool `yaml:"terminate_auth"`
	// FeederID names the checkpoint ArchiveWait reads, copied from feeder.id at
	// load time rather than configured twice.
	FeederID string `yaml:"-"`
	// Managed lists the namespaces Thermocline mirrors, copied from
	// feeder.namespaces at load time rather than configured twice.
	//
	// The router needs it to tell a collection it tiers from one that only ever
	// lived on the fast tier. An untiered collection is whole by definition, so
	// demanding a completeness claim for it would refuse perfectly good queries.
	// Empty means every namespace is managed, matching the feeder.
	Managed []string `yaml:"-"`
}

// Manages reports whether ns is one of the namespaces Thermocline tiers.
func (r RouterConfig) Manages(ns string) bool {
	if len(r.Managed) == 0 {
		return true
	}
	for _, m := range r.Managed {
		if m == ns {
			return true
		}
	}
	return false
}

// WarmerConfig tunes filling the fast tier from the archive.
type WarmerConfig struct {
	// Rules say how far back to fill, one per namespace.
	Rules []WarmRule `yaml:"rules"`
	// Rate caps documents copied up per second.
	Rate int `yaml:"rate"`
	// BatchSize is documents per pass. These carry whole documents, unlike the
	// evictor's batches, so keep it smaller.
	BatchSize int `yaml:"batch_size"`
	// Residency is how long a warmed document is protected from eviction.
	Residency Duration `yaml:"residency"`
	// Backoff holds the fast tier health limits. The warmer writes to the fast
	// tier, so it backs off on the same signals the evictor does.
	Backoff BackoffConfig `yaml:"backoff"`
}

// WarmRule says how far back one namespace should be filled.
type WarmRule struct {
	// NS is the namespace as db.collection.
	NS string `yaml:"ns"`
	// Back is how far into the past to fill. Everything newer than this ends
	// up in the fast tier, which is what lets the completeness watermark move.
	Back Duration `yaml:"back"`
	// DateField holds the document's age. Empty uses the timestamp in a default
	// ObjectId _id, same convention as the evictor.
	DateField string `yaml:"date_field"`
}

// UsesObjectIDTime reports whether the rule reads age from the _id itself.
func (r WarmRule) UsesObjectIDTime() bool { return r.DateField == "" }

// EvictorConfig tunes removal from the fast tier.
type EvictorConfig struct {
	// Rules select what is eligible, one per namespace.
	Rules []EvictionRule `yaml:"rules"`
	// Rate caps documents removed per second. Start low. This is the knob that
	// decides whether eviction is invisible or an incident.
	Rate int `yaml:"rate"`
	// BatchSize is documents per pass. Each pass is five round trips.
	BatchSize int `yaml:"batch_size"`
	// MaxFeederLag stops eviction when mirroring falls behind. Removing a
	// document the archive has not received yet loses it permanently.
	MaxFeederLag Duration `yaml:"max_feeder_lag"`
	// FeederID names the checkpoint to read lag from. Must match feeder.id.
	FeederID string `yaml:"feeder_id"`
	// Backoff holds the fast tier health limits.
	Backoff BackoffConfig `yaml:"backoff"`
}

// EvictionRule decides which documents in one namespace may leave the fast tier.
type EvictionRule struct {
	// NS is the namespace as db.collection.
	NS string `yaml:"ns"`
	// OlderThan is the age past which a document is a candidate.
	OlderThan Duration `yaml:"older_than"`
	// DateField is the field holding the document's age. Leave empty to use the
	// timestamp embedded in a default ObjectId _id, which needs no extra index
	// and lets the scan follow the _id index in order.
	DateField string `yaml:"date_field"`
}

// UsesObjectIDTime reports whether the rule reads age from the _id itself.
func (r EvictionRule) UsesObjectIDTime() bool { return r.DateField == "" }

// BackoffConfig holds the live signals that pause eviction. Every one of these
// is a way for a bulk delete campaign to damage the cluster it runs against.
type BackoffConfig struct {
	MaxReplicationLag Duration `yaml:"max_replication_lag"`
	MaxCacheDirty     float64  `yaml:"max_cache_dirty"`
	// MinWriteTicketsFree is a fraction of the pool, not a count. MongoDB 7.0
	// made the pool dynamic and host-sized, so a container can report a total
	// of 8 tickets and any useful absolute floor becomes unreachable.
	MinWriteTicketsFree float64  `yaml:"min_write_tickets_free"`
	MinOplogWindow      Duration `yaml:"min_oplog_window"`
}

// Tier is one side of the pair.
type Tier struct {
	URI string `yaml:"uri"`
}

// FeederConfig tunes the mirroring daemon.
type FeederConfig struct {
	// ID names this feeder's checkpoint. Only change it to start over.
	ID string `yaml:"id"`
	// Namespaces to mirror, as "db.collection". Empty mirrors everything
	// except system and Thermocline's own databases.
	Namespaces []string `yaml:"namespaces"`
	// CheckpointEvery bounds how much work a restart repeats. Mirroring is
	// idempotent, so this trades duplicate writes against checkpoint writes.
	CheckpointEvery time.Duration `yaml:"checkpoint_every"`
	// PositionEvery is how often the feeder publishes how far it has mirrored.
	//
	// There is no point setting this slower than MaxAwait: the position can
	// only advance when a poll finds the stream empty, so pacing publication
	// below that rate just adds to what every fallback read waits for.
	//
	// This is what a read falling through to the archive waits on, so it sets
	// that read's floor latency. It is deliberately separate from
	// CheckpointEvery: the two trade against different things. A stale
	// checkpoint costs re-mirrored work after a restart; a stale position costs
	// every fallback read its wait. Zero publishes nothing and falls back to
	// the checkpoint, which is what a feeder from before this did.
	PositionEvery time.Duration `yaml:"position_every"`
	// BatchSize caps documents per bulk write to the cold tier.
	BatchSize int `yaml:"batch_size"`
	// MaxAwait is how long a change stream poll blocks before returning empty.
	//
	// It is also the floor on every fallback read, which is not obvious and is
	// the reason this default is as low as it is. The feeder only knows it has
	// caught up when a poll comes back with nothing, and that is the only
	// moment it can honestly publish "everything written up to now is in the
	// archive". A read that falls through to the archive waits for a position
	// at least as new as itself, so it waits for the next such poll.
	//
	// Measured on a shop-shaped workload with this at 500ms, every archive read
	// cost about 570ms while the archive answered the same query in 11ms and
	// the same read with the barrier disabled took 54ms. Almost all of it was
	// waiting for the next poll. Ten polls a second on an idle stream is one
	// small getMore each, which is what a change stream does anyway.
	MaxAwait time.Duration `yaml:"max_await"`
	// UnannouncedDeletes handles deletes the evictor did not announce.
	UnannouncedDeletes UnannouncedDeletes `yaml:"unannounced_deletes"`
}

// Default returns a configuration with every optional field filled in.
//
// The evictor defaults are deliberately timid. A too-slow eviction campaign
// finishes late; a too-fast one blows out the oplog and takes the secondaries
// with it.
func Default() Config {
	return Config{
		Meta: MetaConfig{DB: meta.DefaultDB},
		Feeder: FeederConfig{
			ID:                 "default",
			CheckpointEvery:    5 * time.Second,
			PositionEvery:      100 * time.Millisecond,
			BatchSize:          500,
			MaxAwait:           100 * time.Millisecond,
			UnannouncedDeletes: Tombstone,
		},
		Evictor: EvictorConfig{
			Rate:         1000,
			BatchSize:    500,
			MaxFeederLag: Duration(30 * time.Second),
			FeederID:     "default",
			Backoff: BackoffConfig{
				MaxReplicationLag:   Duration(10 * time.Second),
				MaxCacheDirty:       0.05,
				MinWriteTicketsFree: 0.25,
				MinOplogWindow:      Duration(24 * time.Hour),
			},
		},
		Warmer: WarmerConfig{
			Rate:      2000,
			BatchSize: 200,
			Residency: Duration(30 * 24 * time.Hour),
			Backoff: BackoffConfig{
				MaxReplicationLag:   Duration(10 * time.Second),
				MaxCacheDirty:       0.05,
				MinWriteTicketsFree: 0.25,
				MinOplogWindow:      Duration(24 * time.Hour),
			},
		},
		Router: RouterConfig{
			Listen:       "127.0.0.1:27017",
			Completeness: Archive,
			Residency:    Duration(30 * 24 * time.Hour),
			ArchiveWait:  Duration(5 * time.Second),
			FeederID:     "default",
			Cache: CacheConfig{
				// Off. The bounds apply when it is turned on: a thousand
				// queries is a large working set for one router, and a minute
				// is short enough that a writer this router cannot see is
				// never far ahead of it.
				MaxQueries: 1000,
				MaxBytes:   64 * 1024 * 1024,
				MaxTTL:     Duration(time.Minute),
			},
		},
	}
}

// Load reads a YAML file over the defaults and validates the result.
func Load(path string) (Config, error) {
	cfg := Default()

	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	cfg.Router.Managed = cfg.Feeder.Namespaces
	cfg.Router.FeederID = cfg.Feeder.ID
	// The router's own account, taken from the fast tier connection string
	// rather than configured a second time. With terminate_auth on, the raw
	// upstream connection is the router's rather than the client's, so the
	// router has to authenticate it itself.
	cfg.Router.ServiceUser, cfg.Router.ServicePass, cfg.Router.ServiceAuthDB = credentialsFromURI(cfg.Hot.URI)
	cfg.Router.HotTLS, cfg.Router.HotTLSInsecure, cfg.Router.HotTLSCAFile = tlsFromURI(cfg.Hot.URI)
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	// Every command reaches this line before it opens a connection, which is
	// what makes a package-level name safe: there is one place that sets it,
	// and it is past validation, so a rejected name never takes effect.
	meta.SetDB(cfg.Meta.DB)
	return cfg, nil
}

// Validate reports the first problem that would stop a daemon from running.
func (c Config) Validate() error {
	if c.Hot.URI == "" {
		return errors.New("hot.uri is required")
	}
	if c.Cold.URI == "" {
		return errors.New("cold.uri is required")
	}
	if c.Hot.URI == c.Cold.URI {
		return errors.New("hot.uri and cold.uri point at the same cluster")
	}
	if c.Feeder.ID == "" {
		return errors.New("feeder.id is required")
	}
	if c.Feeder.BatchSize < 1 {
		return errors.New("feeder.batch_size must be at least 1")
	}
	switch c.Feeder.UnannouncedDeletes {
	case Tombstone, Ignore, Halt:
	default:
		return fmt.Errorf("feeder.unannounced_deletes must be tombstone, ignore or halt, got %q",
			c.Feeder.UnannouncedDeletes)
	}
	if err := c.validateMetaDB(); err != nil {
		return err
	}
	for _, ns := range c.Feeder.Namespaces {
		db, coll, ok := strings.Cut(ns, ".")
		if !ok || db == "" || coll == "" {
			return fmt.Errorf("feeder.namespaces: %q is not db.collection", ns)
		}
	}
	if err := c.Evictor.Validate(); err != nil {
		return err
	}
	if err := c.Warmer.Validate(); err != nil {
		return err
	}
	return c.Router.Validate()
}

// validateMetaDB rejects a bookkeeping database that MongoDB would refuse, or
// that would quietly stop the feeder mirroring.
//
// The last check is the one that matters. The feeder skips its own bookkeeping
// database wholesale, so naming it after a database that holds tiered
// collections means those collections are never mirrored, and nothing says so:
// the fast tier looks healthy, the archive stays empty, and the first eviction
// has nowhere to evict to.
func (c Config) validateMetaDB() error {
	db := c.Meta.DB
	if db == "" {
		return errors.New("meta.db must not be empty")
	}
	if len(db) > 63 {
		return fmt.Errorf("meta.db %q is longer than the 63 bytes MongoDB allows", db)
	}
	// MongoDB's own rejected set for a database name, plus the dot, which would
	// make every collection reference ambiguous.
	if i := strings.IndexAny(db, "/\\. \"$*<>:|?\x00"); i >= 0 {
		return fmt.Errorf("meta.db %q contains %q, which MongoDB does not allow in a database name",
			db, db[i:i+1])
	}
	switch db {
	case "admin", "local", "config":
		return fmt.Errorf("meta.db must not be %q, which is MongoDB's own", db)
	}
	for _, ns := range c.Feeder.Namespaces {
		if nsDB, _, ok := strings.Cut(ns, "."); ok && nsDB == db {
			return fmt.Errorf("meta.db %q also holds the tiered namespace %q, so the feeder would skip mirroring it",
				db, ns)
		}
	}
	return nil
}

// Validate reports the first problem that would stop the router from running.
func (r RouterConfig) Validate() error {
	if r.Listen == "" {
		return errors.New("router.listen is required")
	}
	switch r.Completeness {
	case Archive, Strict, Permissive:
	default:
		return fmt.Errorf("router.completeness must be archive, strict or permissive, got %q", r.Completeness)
	}
	if r.Residency <= 0 {
		return errors.New("router.residency must be positive, or the evictor removes what was just copied up")
	}
	if r.Dashboard != "" && samePort(r.Dashboard, r.Listen) {
		// Both would bind it and one would lose, leaving either no dashboard or
		// no router, with nothing but a bind error to say which.
		return fmt.Errorf("router.dashboard %q is the address clients connect to; give the dashboard its own port",
			r.Dashboard)
	}
	return nil
}

// Validate reports the first problem that would stop the warmer from running.
func (w WarmerConfig) Validate() error {
	if w.Rate < 1 {
		return errors.New("warmer.rate must be at least 1")
	}
	if w.BatchSize < 1 {
		return errors.New("warmer.batch_size must be at least 1")
	}
	if w.Residency <= 0 {
		return errors.New("warmer.residency must be positive, or the evictor removes what was just warmed")
	}
	if w.Backoff.MaxCacheDirty <= 0 || w.Backoff.MaxCacheDirty > 1 {
		return fmt.Errorf("warmer.backoff.max_cache_dirty must be a fraction between 0 and 1, got %v",
			w.Backoff.MaxCacheDirty)
	}
	seen := make(map[string]bool, len(w.Rules))
	for _, r := range w.Rules {
		db, coll, ok := strings.Cut(r.NS, ".")
		if !ok || db == "" || coll == "" {
			return fmt.Errorf("warmer.rules: %q is not db.collection", r.NS)
		}
		if seen[r.NS] {
			return fmt.Errorf("warmer.rules: %q appears twice", r.NS)
		}
		seen[r.NS] = true
		if r.Back <= 0 {
			return fmt.Errorf("warmer.rules %q: back must be positive", r.NS)
		}
	}
	return nil
}

// Validate reports the first problem that would stop the evictor from running.
func (e EvictorConfig) Validate() error {
	if e.Rate < 1 {
		return errors.New("evictor.rate must be at least 1")
	}
	if e.BatchSize < 1 {
		return errors.New("evictor.batch_size must be at least 1")
	}
	if e.MaxFeederLag <= 0 {
		return errors.New("evictor.max_feeder_lag must be positive: eviction without a mirroring check loses data")
	}
	if e.FeederID == "" {
		return errors.New("evictor.feeder_id is required so lag can be read")
	}
	if e.Backoff.MinWriteTicketsFree < 0 || e.Backoff.MinWriteTicketsFree > 1 {
		return fmt.Errorf("evictor.backoff.min_write_tickets_free must be a fraction between 0 and 1, got %v",
			e.Backoff.MinWriteTicketsFree)
	}
	if e.Backoff.MaxCacheDirty <= 0 || e.Backoff.MaxCacheDirty > 1 {
		return fmt.Errorf("evictor.backoff.max_cache_dirty must be a fraction between 0 and 1, got %v",
			e.Backoff.MaxCacheDirty)
	}
	seen := make(map[string]bool, len(e.Rules))
	for _, r := range e.Rules {
		db, coll, ok := strings.Cut(r.NS, ".")
		if !ok || db == "" || coll == "" {
			return fmt.Errorf("evictor.rules: %q is not db.collection", r.NS)
		}
		if seen[r.NS] {
			return fmt.Errorf("evictor.rules: %q appears twice", r.NS)
		}
		seen[r.NS] = true
		if r.OlderThan <= 0 {
			return fmt.Errorf("evictor.rules %q: older_than must be positive", r.NS)
		}
	}
	return nil
}

// samePort reports whether two listen addresses would fight for the same port.
//
// Only the port is compared. "0.0.0.0:27017" and ":27017" and "127.0.0.1:27017"
// are different strings that collide in practice, and a wildcard bind takes the
// port on every interface anyway.
func samePort(a, b string) bool {
	_, ap, err := net.SplitHostPort(a)
	if err != nil {
		return false
	}
	_, bp, err := net.SplitHostPort(b)
	if err != nil {
		return false
	}
	return ap == bp
}

// credentialsFromURI pulls the router's own account out of the fast tier
// connection string.
//
// Taken from there rather than configured separately because they are the same
// account: whatever the router already uses to mirror, evict and warm is what it
// will use to hold the upstream connection when it terminates authentication.
// Configuring them twice is a way for the two to drift apart, and the failure
// when they do is a login that works everywhere except through the proxy.
// CacheConfig is the learned-TTL query cache.
//
// Nothing here is a TTL. The point of the feature is that the router works out
// how long each query's result stays the same and caches it for a little less
// than that, so the only numbers configured are the bounds: how many queries to
// watch, and the longest any answer may be held however stable it has looked.
type CacheConfig struct {
	// Enabled turns the cache on. Off by default, because a cache is the one
	// thing in this system that can answer without asking.
	Enabled bool `yaml:"enabled"`
	// MaxQueries bounds how many distinct queries are watched at once.
	MaxQueries int `yaml:"max_queries"`
	// MaxTTL is the ceiling on a learned TTL. A query that has not changed
	// since the router started is not proof that it never will.
	MaxTTL Duration `yaml:"max_ttl"`
	// MaxBytes is how much memory the held answers may occupy in total.
	// Counting queries is not a memory bound: a thousand replies of a megabyte
	// each is a gigabyte, and nothing in a count says which of those it is.
	MaxBytes int `yaml:"max_bytes"`
	// Measure keeps watching queries even when nothing is being cached, which
	// is what makes `cline cache` and the dashboard able to say whether caching
	// would be worth turning on. On by default.
	//
	// It is a switch of its own because it is not free: a hash of every reply
	// and a bounded table of shapes. An operator who has decided against
	// caching, or who is counting every megabyte, can have neither half.
	Measure *bool `yaml:"measure"`
}

// Measuring reports whether the advisor should run. Absent means yes, which is
// why this is a pointer: "off" has to be distinguishable from "not mentioned".
func (c CacheConfig) Measuring() bool {
	if c.Measure == nil {
		return true
	}
	return *c.Measure
}

// TLSConfig is the certificate a router presents to its clients, and what it
// asks of theirs.
//
// The router takes over the address an application already dials, so whatever
// that application expected of the connection it has to keep getting. A
// deployment that required TLS to the replica set cannot be handed a plaintext
// proxy and be told it is a drop-in replacement.
type TLSConfig struct {
	// Cert and Key are the router's own certificate and private key, in PEM.
	// Both or neither.
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
	// CA is the authority client certificates are checked against. Setting it
	// is what makes client certificates possible at all.
	CA string `yaml:"ca"`
	// RequireClientCert rejects a client that presents no certificate. It needs
	// CA, because there is nothing to check against otherwise.
	RequireClientCert bool `yaml:"require_client_cert"`
}

// Enabled reports whether the listener should speak TLS.
func (t TLSConfig) Enabled() bool { return t.Cert != "" || t.Key != "" }

// tlsFromURI reads what a connection string says about TLS.
//
// The driver parses this for its own connections and gives no way to ask it
// afterwards, so it is read here for the raw connection the router opens
// itself. Both spellings, because tls and ssl are the same option and drivers
// have accepted either for years.
func tlsFromURI(uri string) (enabled, insecure bool, caFile string) {
	u, err := url.Parse(uri)
	if err != nil {
		return false, false, ""
	}
	q := u.Query()
	yes := func(keys ...string) bool {
		for _, k := range keys {
			if v := q.Get(k); v != "" {
				return strings.EqualFold(v, "true")
			}
		}
		return false
	}
	enabled = yes("tls", "ssl")
	insecure = yes("tlsInsecure", "tlsAllowInvalidCertificates", "sslInsecure")
	caFile = q.Get("tlsCAFile")
	if caFile == "" {
		caFile = q.Get("sslCAFile")
	}
	return enabled, insecure, caFile
}

func credentialsFromURI(uri string) (user, pass, authDB string) {
	u, err := url.Parse(uri)
	if err != nil || u.User == nil {
		return "", "", ""
	}
	user = u.User.Username()
	pass, _ = u.User.Password()
	authDB = u.Query().Get("authSource")
	if authDB == "" {
		// The driver's own default: the database named in the path, or admin.
		authDB = strings.TrimPrefix(u.Path, "/")
		if authDB == "" {
			authDB = "admin"
		}
	}
	return user, pass, authDB
}
