// Package config loads and validates the shared Thermocline configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
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
	Feeder  FeederConfig  `yaml:"feeder"`
	Evictor EvictorConfig `yaml:"evictor"`
	Warmer  WarmerConfig  `yaml:"warmer"`
	Router  RouterConfig  `yaml:"router"`
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
	// Strict refuses the query. The fast tier returning some of the matching
	// documents looks exactly like it returning all of them, and a short answer
	// reported as success is the worst failure this system has.
	Strict Completeness = "strict"
	// Permissive answers anyway and counts it. For deployments that would
	// rather have fast approximate reads than an error.
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
	// BatchSize caps documents per bulk write to the cold tier.
	BatchSize int `yaml:"batch_size"`
	// MaxAwait is how long a change stream poll blocks before returning empty.
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
		Feeder: FeederConfig{
			ID:                 "default",
			CheckpointEvery:    5 * time.Second,
			BatchSize:          500,
			MaxAwait:           time.Second,
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
			Completeness: Strict,
			Residency:    Duration(30 * 24 * time.Hour),
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
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
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

// Validate reports the first problem that would stop the router from running.
func (r RouterConfig) Validate() error {
	if r.Listen == "" {
		return errors.New("router.listen is required")
	}
	switch r.Completeness {
	case Strict, Permissive:
	default:
		return fmt.Errorf("router.completeness must be strict or permissive, got %q", r.Completeness)
	}
	if r.Residency <= 0 {
		return errors.New("router.residency must be positive, or the evictor removes what was just copied up")
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
