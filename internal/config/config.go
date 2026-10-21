// Package config loads and validates the shared Thermocline configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
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

	// AllowVersionSkew starts the daemons against tiers running different
	// MongoDB releases. Off, because the archive runs the client's own queries
	// and has to understand everything the application says: see
	// internal/tierver. Turning it on accepts that a query can work for months
	// and then fail on the day its documents age out of the fast tier.
	AllowVersionSkew bool `yaml:"allow_version_skew"`
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

// Freshness decides what the router does when the archive cannot be shown to
// have caught up with the fast tier: a read that falls through, or a copy-up,
// has waited router.archive_wait for mirroring and it has not arrived, or the
// feeder has stopped publishing altogether.
type Freshness string

const (
	// FreshnessStrict refuses the read or the write that needed the archive.
	// The default since 2026-10-12. With the feeder stopped for two minutes on
	// the staging pair, answering anyway gave a shop running through the
	// router 2,759 answers that contradicted its own acknowledged writes:
	// counts that did not move after an order, and cancelled orders coming
	// back. An error is what the application can see and retry; a wrong
	// answer reported as success is not.
	FreshnessStrict Freshness = "strict"
	// FreshnessPermissive answers from the archive anyway, counts it in
	// thermocline_stale_reads_total and logs it. An answer may miss a very
	// recent write or include a very recently deleted document, and a copy-up
	// may bring back a document deleted a moment ago. For deployments that
	// would rather read slightly behind than fail during a feeder outage.
	FreshnessPermissive Freshness = "permissive"
)

// CopyUpReads decides whether a point read the fast tier misses copies the
// document up. Writes and transactions copy up regardless: they need the
// document on the fast tier to be applied there.
type CopyUpReads string

const (
	// CopyUpAlways copies every document a point read finds only in the
	// archive. The behaviour before this setting existed, and the default.
	CopyUpAlways CopyUpReads = "always"
	// CopyUpRepeated copies a document up on its second miss inside
	// copy_up_window, and answers the first from the archive alone. A read of
	// an old record that nobody asks for again is the common case in a long
	// tail, and copying it costs a write on each tier and the bookkeeping that
	// keeps it from looking like an application insert, for a document the next
	// sweep evicts again.
	CopyUpRepeated CopyUpReads = "repeated"
	// CopyUpNever answers every such read from the archive and copies nothing.
	CopyUpNever CopyUpReads = "never"
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
	// Freshness is what happens when that wait runs out. See Freshness.
	Freshness Freshness `yaml:"freshness"`
	// CopyUpReads is whether a point read the fast tier misses copies the
	// document up. See CopyUpReads.
	CopyUpReads CopyUpReads `yaml:"copy_up_reads"`
	// CopyUpWindow is how close together two misses of one document have to
	// be for copy_up_reads: repeated to copy it up.
	CopyUpWindow Duration `yaml:"copy_up_window"`
	// ClaimGrace is how much longer than the ordinary claim cache TTL a router
	// may keep answering from a completeness claim it can no longer re-read,
	// because the archive is unreachable. Zero, the default, is no grace at all.
	//
	// It buys availability during an archive outage and it is the one knob here
	// that trades a guarantee for it, so the reasoning is worth stating.
	//
	// The claim is a value with two copies and they disagree for
	// watermark.ClaimCacheTTL, which is safe because the disagreement only ever
	// runs one way: the evictor raises a claim before it removes anything, and
	// waits out that TTL in between. Both of those writes go to the archive. So
	// an archive nobody can reach is an archive where no claim can be narrowed
	// and nothing can be evicted, and a claim held past its TTL is still true.
	//
	// That argument holds for an outage and fails for a partition that isolates
	// only this router: the evictor still reaches the archive, still narrows
	// claims, still removes documents, and this router answers from a claim
	// that covers documents the fast tier no longer holds. The answer is short
	// with no error on it, for as long as this grace lasts.
	//
	// Set it if a few minutes of short answers during a network fault is a
	// better outcome for the application than a few minutes of refusals. Leave
	// it at zero if it is not. There is no value that is safe in both faults,
	// which is why there is no default other than off.
	ClaimGrace Duration `yaml:"claim_grace"`
	// AllowUntieredJoin downgrades to a warning the refusal of a pipeline that
	// the archive would answer and that also reads a collection nothing tiers.
	//
	// Refusing is the default, because the archive holds only what such a
	// collection contained on the day it was adopted and answering would be
	// short by everything written to it since, with no error on it. An operator
	// who knows the untiered collections a pipeline reads are reference data
	// that has not changed since the cutover can set this and get the answer
	// instead. The warning names them every time.
	AllowUntieredJoin bool `yaml:"allow_untiered_join"`
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
	// LiveCache keeps the answers to equality reads in memory, updated from the
	// fast tier's change stream. On a tiered collection it holds only what the
	// fast tier proves, newest first by the age field or by _id. See package
	// livecache.
	LiveCache LiveCacheConfig `yaml:"live_cache"`
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
	// managedSet is Managed as a set, built once at load time. See Manages.
	managedSet map[string]bool `yaml:"-"`

	// Managed lists the namespaces Thermocline mirrors, copied from
	// feeder.namespaces at load time rather than configured twice.
	//
	// The router needs it to tell a collection it tiers from one that only ever
	// lived on the fast tier. An untiered collection is whole by definition, so
	// demanding a completeness claim for it would refuse perfectly good queries.
	// Empty means every namespace is managed, matching the feeder.
	Managed []string `yaml:"-"`

	// TieredBy names, for each managed namespace, the field its age is read
	// from. An entry present and empty means the timestamp inside a default
	// ObjectId, which is what a rule with no date_field uses. A namespace with
	// no entry has no rule saying how it ages.
	//
	// The router needs this to read a query's lower bound honestly. A
	// completeness claim says the fast tier holds every document newer than an
	// instant, and "newer" is measured in the dimension the rules tier by. A
	// bound on any other date field is a bound in a different dimension and
	// proves nothing: see lowerTimeBound.
	TieredBy map[string]string `yaml:"-"`

	// AllowVersionSkew is the top-level setting of the same name, copied at
	// load time rather than configured twice. It belongs to the whole
	// deployment rather than the router: every daemon that touches both tiers
	// checks the pair.
	AllowVersionSkew bool `yaml:"-"`
}

// AgeField reports the field a namespace's age is read from, and whether
// anything says. An empty name with ok true means the ObjectId timestamp.
func (r RouterConfig) AgeField(ns string) (string, bool) {
	f, ok := r.TieredBy[ns]
	return f, ok
}

// Manages reports whether ns is one of the namespaces Thermocline tiers.
//
// A map because this is on the hot path and the list is not short. It was a
// linear scan over Managed, and a deployment tiering a hundred namespaces paid
// a hundred string comparisons on every call, from twenty nine call sites,
// several of them per write. The test configuration here has 110 entries.
func (r RouterConfig) Manages(ns string) bool {
	if len(r.Managed) == 0 {
		return true
	}
	if r.managedSet != nil {
		return r.managedSet[ns]
	}
	// A config built in code rather than loaded still has to answer correctly.
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
	// MinFreeDisk is the fraction of the fast tier's filesystem that must stay
	// free, as a number between 0 and 1. Zero turns the check off.
	//
	// It lives here rather than in Backoff because only the warmer may act on
	// it. The warmer fills the fast tier and a warm window is bounded by time
	// rather than by size, so a window chosen from the age of the data instead
	// of the size of the tier fills the disk. Holding the evictor back on the
	// same signal would stop the one thing that makes room.
	MinFreeDisk float64 `yaml:"min_free_disk"`
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
	// There is no point setting this slower than MaxAwait: the position
	// advances when a poll finds the stream empty, or on a stream that is
	// never empty when the buffer is flushed, at most 100ms apart, so pacing
	// publication below that rate just adds to what every fallback read waits
	// for. See Feeder.dueToFlush for the stream that is never empty, which is
	// the one this used to miss.
	//
	// This is what a read falling through to the archive waits on, so it sets
	// that read's floor latency. It is deliberately separate from
	// CheckpointEvery: the two trade against different things. A stale
	// checkpoint costs re-mirrored work after a restart; a stale position costs
	// every fallback read its wait. Zero publishes nothing and falls back to
	// the checkpoint, which is what a feeder from before this did.
	//
	// 5ms by default since 2026-10-09. On the Hetzner staging pair under the
	// shop it gave 260 operations a second against 194 at 20ms, and a mean
	// barrier wait of 16.5ms against 28.1ms. See docs/router.md.
	PositionEvery time.Duration `yaml:"position_every"`
	// BatchSize caps documents per bulk write to the cold tier.
	BatchSize int `yaml:"batch_size"`
	// TombstoneRetention is how long the archive keeps a document the
	// application deleted. Zero, the default, keeps it for ever.
	//
	// A delete through the router does not erase the archive's copy, it marks
	// it with meta.TombstoneField so reads stop returning it. That is an audit
	// trail on purpose, and it is also two problems on a deployment that runs
	// for years: the archive grows with every delete it has ever seen, and a
	// delete the application issued to erase data does not erase it.
	//
	// Setting this gives the archive a TTL index on the mark, so mongod removes
	// the copy that long after the delete. Reads are unaffected either way: a
	// tombstoned copy is already invisible to them.
	TombstoneRetention Duration `yaml:"tombstone_retention"`
	// EvictionRecord is how long the fast tier keeps the record of each
	// eviction the feeder confirmed, in meta.Evicted. Zero keeps none.
	//
	// It is what lets the archive be restored from a backup this old. The
	// feeder replays the fast tier's oplog from the backup's checkpoint, and
	// an eviction in that replay is recognised only by this record: without
	// it every document evicted since the backup is read as deleted and
	// tombstoned. So it should cover the archive's backup interval, and the
	// fast tier's oplog window has to as well. About 54 bytes of the fast
	// tier's oplog per eviction, measured; see meta.RecordEvictions.
	EvictionRecord Duration `yaml:"eviction_record"`
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
			PositionEvery:      5 * time.Millisecond,
			BatchSize:          500,
			MaxAwait:           20 * time.Millisecond,
			UnannouncedDeletes: Tombstone,
			// A daily backup with a week of slack.
			EvictionRecord: Duration(8 * 24 * time.Hour),
		},
		Evictor: EvictorConfig{
			Rate:         1000,
			BatchSize:    500,
			MaxFeederLag: Duration(30 * time.Second),
			// No default id here on purpose: Load takes it from feeder.id.
			// A literal "default" was the default until it was measured, and a
			// file that renamed its feeder without renaming this one pointed
			// the mirroring-lag gate at a checkpoint nothing writes.
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
			Freshness:    FreshnessStrict,
			CopyUpReads:  CopyUpAlways,
			LiveCache:    LiveCacheConfig{MaxBytes: 256 << 20},
			CopyUpWindow: Duration(10 * time.Minute),
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
	// Strict: a key this does not know is an error rather than a silence.
	//
	// It was a silence, and that is the worst shape a configuration mistake can
	// take. Measured on one file: `terminate-auth: true` with a hyphen loaded
	// as false, so the router proxied authentication while the operator
	// believed it was terminating it and checking privileges itself. Nothing
	// was logged, nothing failed, and the setting simply did not exist. The
	// same goes for a misspelt retention, which keeps everything for ever, and
	// for a misspelt claim_grace, which gives no grace.
	//
	// Refusing costs an operator a restart with an error naming the line. The
	// silence costs them a setting they think they have.
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		// io.EOF is an empty file, which is a config that changes nothing
		// rather than a broken one: every daemon then fails validation on the
		// missing URIs, which is the honest error.
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	cfg.Router.Managed = cfg.Feeder.Namespaces
	cfg.Router.managedSet = make(map[string]bool, len(cfg.Feeder.Namespaces))
	for _, ns := range cfg.Feeder.Namespaces {
		cfg.Router.managedSet[ns] = true
	}
	cfg.Router.FeederID = cfg.Feeder.ID
	// evictor.feeder_id names the checkpoint the mirroring-lag gate reads, and
	// it has to be the id of a feeder that is actually running. Taken from
	// feeder.id when it is not spelled out, the way the router's already is.
	//
	// It defaulted to the literal "default" instead. A file that set
	// feeder.id and left this alone pointed the gate at a checkpoint nothing
	// writes: the lag reads as unknown, which is the safe direction, so the
	// evictor never removed anything and said "mirroring lag is unknown" every
	// minute for ever. Nothing was lost and nothing was aged, and the message
	// blamed the feeder, which was running perfectly.
	if cfg.Evictor.FeederID == "" {
		cfg.Evictor.FeederID = cfg.Feeder.ID
	}
	cfg.Router.TieredBy = tieringFields(cfg)
	cfg.Router.AllowVersionSkew = cfg.AllowVersionSkew
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
	// mongod's TTL monitor runs once a minute, so anything shorter than that is
	// a number that cannot be honoured. Refusing it is how a misplaced unit is
	// caught before a deployment believes its deletes erase in seconds.
	if c.Feeder.TombstoneRetention < 0 {
		return errors.New("feeder.tombstone_retention cannot be negative: leave it out to keep deleted documents for ever")
	}
	if c.Feeder.TombstoneRetention > 0 && c.Feeder.TombstoneRetention.D() < time.Minute {
		return fmt.Errorf("feeder.tombstone_retention is %s, and mongod expires TTL indexes once a minute: "+
			"give at least 1m", c.Feeder.TombstoneRetention.String())
	}
	// expireAfterSeconds is a 32 bit integer at the server, so a longer
	// retention than this cannot be expressed. Caught here rather than sent, or
	// it arrives as a negative number of seconds.
	if secs := c.Feeder.TombstoneRetention.D().Seconds(); secs > math.MaxInt32 {
		return fmt.Errorf("feeder.tombstone_retention is %s, and the longest a TTL index can express "+
			"is about 68 years: leave it out to keep deleted documents for ever",
			c.Feeder.TombstoneRetention.String())
	}
	if r := c.Feeder.EvictionRecord; r < 0 || (r > 0 && r.D() < time.Hour) {
		return fmt.Errorf("feeder.eviction_record is %s; give 0 to keep no record, or at least 1h, "+
			"the age of the oldest archive backup you would restore from", r.String())
	}
	if secs := c.Feeder.EvictionRecord.D().Seconds(); secs > math.MaxInt32 {
		return fmt.Errorf("feeder.eviction_record is %s, longer than an index can expire after",
			c.Feeder.EvictionRecord.String())
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
	if err := c.validateRulesAreTiered(); err != nil {
		return err
	}
	return c.Router.Validate()
}

// FeederIDWarnings names an evictor pointed at a feeder id this file does not
// describe.
//
// Load fills evictor.feeder_id from feeder.id when it is not spelled out, so a
// difference here means somebody wrote both and wrote them differently. That is
// legal, because the daemons can be run from separate files, and it is far more
// often a mistake: the gate reads a checkpoint under that id, finds none, and
// pauses eviction for ever while reporting a mirroring problem.
//
// A warning rather than a refusal, because a deployment that really does run
// its feeder from another file is not wrong, and refusing would break it on an
// upgrade. The verdict names the id too, so an operator who misses this at
// startup meets it again in the pause.
func (c Config) FeederIDWarnings() []string {
	if c.Evictor.FeederID == "" || c.Evictor.FeederID == c.Feeder.ID {
		return nil
	}
	return []string{fmt.Sprintf(
		"evictor.feeder_id is %q and feeder.id is %q. The evictor reads mirroring lag from the "+
			"checkpoint the feeder writes under its own id, so unless a feeder with id %q is "+
			"running from another config, nothing will be evicted",
		c.Evictor.FeederID, c.Feeder.ID, c.Evictor.FeederID)}
}

// WindowWarnings names each namespace whose warm window reaches further back
// than its eviction cutoff.
//
// Those two rules are instructions about the same documents and the components
// obey them both. "Fill the fast tier back 120 days" and "remove anything older
// than 90 days" means the warmer copies the 90 to 120 day band up, the evictor
// takes it away, and it repeats for as long as both run. That is real waste:
// the same documents read out of the archive, written to the fast tier and
// deleted again, indefinitely.
//
// It is waste and not wrong answers, which is worth being exact about, because
// this started life as a refusal on the strength of a measurement it did not
// own. Eighteen answers disagreed on the pilot under a fifteen year warm
// against a one day cutoff, and the cause was the eviction announcement bug
// that was tombstoning documents at the same time. Measured again with that
// fixed, under the same fifteen year warm and one day cutoff, with the fast
// tier emptied by the fight while the questions ran: five rounds of 199
// questions, no disagreements, and no tombstones.
//
// The claim stays honest because the evictor narrows the completeness watermark
// before it removes anything, and because the warmer counts both tiers before
// claiming a window rather than assuming it still holds what it copied. That
// second guard is the one that closes this, and it works whatever the
// configuration says.
//
// So it warns. A refusal here would also have been thin protection: the evictor
// and the warmer are separate daemons and can be given separate files, each
// legal alone and contradictory together, which is how they are usually
// deployed and is not visible from any one file.
// AgeFieldWarnings names every namespace whose two rules age it differently.
//
// This is worse than it looks, and none of it is visible from either file
// alone. The evictor and the warmer then select different documents, which is
// the obvious half. The quiet half is the router: it will not read a lower
// bound out of a query on such a namespace, because it cannot tell which
// dimension a claim about it is measured in, so *every* bounded query on it is
// answered by the archive and nothing says why. One word of configuration turns
// the whole optimisation off for that collection. See tieringFields, which
// drops the namespace, and lowerTimeBound, which then has nothing to read.
//
// A warning rather than a refusal, for the same reason as WindowWarnings: the
// two daemons are usually given separate files, each legal alone.
func (c Config) AgeFieldWarnings() []string {
	field := make(map[string]string, len(c.Evictor.Rules))
	for _, r := range c.Evictor.Rules {
		field[r.NS] = r.DateField
	}
	var out []string
	for _, w := range c.Warmer.Rules {
		ef, ok := field[w.NS]
		if !ok || ef == w.DateField {
			continue
		}
		out = append(out, fmt.Sprintf(
			"%s: the evictor ages it by %s and the warmer by %s. Set date_field to the same field "+
				"on both rules", w.NS, fieldName(ef), fieldName(w.DateField)))
	}
	return out
}

// fieldName says which field a rule ages by, in the words the message needs:
// an empty date_field means the timestamp inside the ObjectId _id.
func fieldName(f string) string {
	if f == "" {
		return "the _id timestamp"
	}
	return f
}

func (c Config) WindowWarnings() []string {
	evict := make(map[string]Duration, len(c.Evictor.Rules))
	for _, r := range c.Evictor.Rules {
		evict[r.NS] = r.OlderThan
	}
	var out []string
	for _, w := range c.Warmer.Rules {
		older, ok := evict[w.NS]
		if !ok {
			continue
		}
		if w.Back.D() > older.D() {
			out = append(out, fmt.Sprintf(
				"%s: warmer.back is %s but evictor.older_than is %s, so the warmer keeps copying "+
					"up what the evictor keeps removing; set warmer.back to %s or less, or "+
					"evictor.older_than to %s or more",
				w.NS, w.Back.String(), older.String(), older.String(), w.Back.String()))
		}
	}
	return out
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

// validateRulesAreTiered refuses a warmer or evictor rule for a namespace the
// feeder does not mirror.
//
// The two lists name the same collections in the same file and nothing checked
// that they agreed, so leaving a collection out of feeder.namespaces while
// naming it in evictor.rules was an accepted configuration. It is the obvious
// mistake to make, because the adoption runbook has an operator tier some
// collections and copy others up whole.
//
// What it does is hide documents. An untiered collection belongs to the fast
// tier alone as far as the router is concerned: reads of it are answered there
// and never fall through, because a collection Thermocline does not tier is
// whole by definition. The evictor, which consults its own rules and nothing
// else, removes the old documents from the fast tier as soon as it can see a
// copy in the archive. On an adoption it always can: the archive *is* the old
// cluster and carries everything it ever held. So the copy that satisfies the
// guard is a snapshot from the cutover rather than a mirror, the documents go
// from the only tier that answers for them, and no error is raised anywhere.
//
// The warmer's version of the same mistake is the mirror image: it copies the
// archive's cutover snapshot up, so a document the application deleted after
// the cutover comes back.
//
// Refused rather than warned. Both outcomes are silent wrong answers, the file
// alone is enough to see the mistake, and no correct deployment needs either
// rule: eviction is only safe because the feeder put the copy there.
func (c Config) validateRulesAreTiered() error {
	// Empty means the feeder mirrors everything, so every rule has its copy.
	if len(c.Feeder.Namespaces) == 0 {
		return nil
	}
	tiered := make(map[string]bool, len(c.Feeder.Namespaces))
	for _, ns := range c.Feeder.Namespaces {
		tiered[ns] = true
	}
	for _, r := range c.Evictor.Rules {
		if !tiered[r.NS] {
			return fmt.Errorf("evictor.rules names %q, which feeder.namespaces does not tier: "+
				"nothing mirrors it, so the router answers it from the fast tier alone and never "+
				"reads the archive for it, and evicting it would remove the documents from the "+
				"only tier that answers. Add it to feeder.namespaces, or drop the rule", r.NS)
		}
	}
	for _, r := range c.Warmer.Rules {
		if !tiered[r.NS] {
			return fmt.Errorf("warmer.rules names %q, which feeder.namespaces does not tier: "+
				"nothing mirrors it, so the archive holds whatever it held when it was adopted "+
				"and warming would copy that back up, returning documents the application has "+
				"since deleted. Add it to feeder.namespaces, or drop the rule", r.NS)
		}
	}
	return nil
}

// Validate reports the first problem that would stop the router from running.
// maxAdvisorShapes is the largest router.cache.max_queries this accepts. See
// RouterConfig.Validate for the measurement behind the number.
const maxAdvisorShapes = 100000

func (r RouterConfig) Validate() error {
	if r.Listen == "" {
		return errors.New("router.listen is required")
	}
	switch r.Completeness {
	case Archive, Strict, Permissive:
	default:
		return fmt.Errorf("router.completeness must be archive, strict or permissive, got %q", r.Completeness)
	}
	switch r.Freshness {
	case FreshnessStrict, FreshnessPermissive:
	default:
		return fmt.Errorf("router.freshness must be strict or permissive, got %q", r.Freshness)
	}
	switch r.CopyUpReads {
	case CopyUpAlways, CopyUpRepeated, CopyUpNever:
	default:
		return fmt.Errorf("router.copy_up_reads must be always, repeated or never, got %q", r.CopyUpReads)
	}
	if r.LiveCache.Enabled && r.LiveCache.MaxBytes <= 0 {
		return errors.New("router.live_cache.max_bytes must be positive")
	}
	if r.CopyUpReads == CopyUpRepeated && r.CopyUpWindow <= 0 {
		return errors.New("router.copy_up_window must be positive when copy_up_reads is repeated")
	}
	if r.Residency <= 0 {
		return errors.New("router.residency must be positive, or the evictor removes what was just copied up")
	}
	if r.ClaimGrace < 0 {
		return errors.New("router.claim_grace cannot be negative")
	}
	if r.ClaimGrace > 0 && r.Completeness != Archive {
		// Nothing reads a claim it cannot re-read in the other two modes: they
		// refuse rather than fall through, so the grace would silently do
		// nothing and look like protection that is in place.
		return fmt.Errorf("router.claim_grace only applies to completeness: archive, not %q", r.Completeness)
	}
	// The advisor watches up to max_queries distinct shapes and, once full,
	// evicts by building a slice of every one of them and sorting it, holding
	// its own lock while every cacheable read waits behind it. Measured on this
	// machine, per eviction:
	//
	//	max_queries      1,000       89us
	//	max_queries     10,000      823us
	//	max_queries    100,000     14.2ms
	//	max_queries  1,000,000      154ms
	//
	// Batching means one eviction covers the next max_queries/10 inserts, so
	// the amortised cost is fine at any of these. The stall is not: 154ms on a
	// proxy whose whole overhead is 130us is an outage for whoever is behind
	// it, and "cache more" is the obvious thing to type. Refused above a
	// hundred thousand, which is the last row that stays in milliseconds.
	//
	// Bounding the work instead of the table would lift this, by sampling a
	// fixed number of shapes rather than sorting all of them, at the cost of
	// evicting approximately rather than exactly. Not done: the advisor's
	// recommendations are the thing being protected here, and nothing has asked
	// for a cap this large.
	if r.Cache.MaxQueries > maxAdvisorShapes {
		return fmt.Errorf("router.cache.max_queries is %d, and above %d one eviction holds the "+
			"advisor's lock for over ten milliseconds while every cacheable read waits: "+
			"measured at 14.2ms for %d and 154ms for a million",
			r.Cache.MaxQueries, maxAdvisorShapes, maxAdvisorShapes)
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
	// A fraction next to a number that reads like a percentage, which is the
	// mistake worth refusing: min_free_disk: 20 meant as "20%" compares 0.85
	// against 20 on every pass, so the warmer backs off for ever and the fast
	// tier is never filled. It says so in the log, as "the fast tier's
	// filesystem is 85.0% free, limit 2000.0%", and a limit of 2000% is not a
	// line anybody goes looking for. Refused at load instead, with the units in
	// the message, the same way max_cache_dirty above is.
	//
	// One is refused as well as more: keeping the whole filesystem free is a
	// warmer that never warms, which is the same outcome by a different route,
	// and turning it off is what zero is for.
	if w.MinFreeDisk < 0 || w.MinFreeDisk >= 1 {
		return fmt.Errorf("warmer.min_free_disk must be a fraction between 0 and 1, got %v: "+
			"0.2 is twenty per cent, and 0 turns the check off", w.MinFreeDisk)
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
	// The announcement that says a delete was an eviction expires on a TTL, and
	// the feeder only sees that delete once it has caught up to it. Letting
	// mirroring fall behind further than the notice lives means the feeder
	// reads an eviction as the application's own delete, tombstones the
	// archive's copy, and the document is gone from both tiers with nothing
	// said. Refused rather than warned, because this one destroys data and the
	// evictor's own file is enough to see it.
	if e.MaxFeederLag.D() >= meta.NoticeTTL {
		return fmt.Errorf("evictor.max_feeder_lag is %s, which reaches the %s an eviction "+
			"announcement is kept: a delete the feeder sees after the announcement has expired "+
			"is read as the application's and the archive tombstones its copy, losing the "+
			"document from both tiers. Set it well below %s",
			e.MaxFeederLag.String(), meta.NoticeTTL, meta.NoticeTTL)
	}
	// Empty is not a problem here: Load fills it from feeder.id, which is
	// itself required. A config built in code and never loaded can still be
	// empty, and an empty id finds no checkpoint, which pauses eviction rather
	// than allowing it.
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
// LiveCacheConfig turns on the live cache and bounds it.
type LiveCacheConfig struct {
	// Enabled turns it on. Off by default.
	Enabled bool `yaml:"enabled"`
	// MaxBytes bounds the documents it holds, least recently used first.
	MaxBytes int `yaml:"max_bytes"`
}

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
	//
	// Bounded above by maxAdvisorShapes, because the eviction that keeps it at
	// this size is a sort of the whole table under a lock every read waits on.
	// See RouterConfig.Validate for the measurements.
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

// tieringFields reads, for each namespace with a rule, the field its age comes
// from.
//
// The evictor and the warmer each name one, and they are meant to agree: they
// are two halves of the same decision about which documents belong on the fast
// tier. When they do not, the namespace is left out rather than guessed at. A
// claim about "everything newer than T" measured one way and a query bounded
// the other way have no relation, and the honest answer is that nothing here
// knows which dimension the claim is in.
func tieringFields(c Config) map[string]string {
	out := map[string]string{}
	disagreed := map[string]bool{}
	note := func(ns, field string) {
		if disagreed[ns] {
			return
		}
		if seen, ok := out[ns]; ok && seen != field {
			delete(out, ns)
			disagreed[ns] = true
			return
		}
		out[ns] = field
	}
	for _, r := range c.Evictor.Rules {
		note(r.NS, r.DateField)
	}
	for _, r := range c.Warmer.Rules {
		note(r.NS, r.DateField)
	}
	return out
}
