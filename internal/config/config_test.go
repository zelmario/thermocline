package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zelmario/thermocline/internal/meta"
)

// valid is the smallest config that passes, so each case below changes exactly
// one thing and the failure names that thing.
func valid() Config {
	c := Default()
	c.Hot.URI = "mongodb://127.0.0.1:28017/?replicaSet=rsHot"
	c.Cold.URI = "mongodb://127.0.0.1:28018/?replicaSet=rsCold"
	c.Feeder.ID = "test"
	c.Feeder.Namespaces = []string{"shop.orders"}
	c.Router.Listen = "127.0.0.1:27100"
	return c
}

func TestMetaDBDefaults(t *testing.T) {
	if got := valid().Meta.DB; got != meta.DefaultDB {
		t.Fatalf("a config that names no bookkeeping database got %q, want %q", got, meta.DefaultDB)
	}
	if err := valid().Validate(); err != nil {
		t.Fatalf("the default config does not validate: %v", err)
	}
}

func TestMetaDBAccepted(t *testing.T) {
	for _, name := range []string{"_thermocline", "_thermocline_w3", "bookkeeping"} {
		c := valid()
		c.Meta.DB = name
		if err := c.Validate(); err != nil {
			t.Errorf("meta.db %q was refused: %v", name, err)
		}
	}
}

func TestMetaDBRefused(t *testing.T) {
	cases := []struct {
		name string
		db   string
		want string
	}{
		{"empty", "", "must not be empty"},
		{"too long", strings.Repeat("x", 64), "63 bytes"},
		{"a dot", "meta.db", "does not allow"},
		{"a space", "meta db", "does not allow"},
		{"a dollar", "meta$db", "does not allow"},
		{"a slash", "meta/db", "does not allow"},
		{"mongodb's own", "admin", "MongoDB's own"},
		{"mongodb's own local", "local", "MongoDB's own"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := valid()
			c.Meta.DB = tc.db
			err := c.Validate()
			if err == nil {
				t.Fatalf("meta.db %q was accepted", tc.db)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("meta.db %q: got %q, want it to mention %q", tc.db, err, tc.want)
			}
		})
	}
}

// TestMetaDBOverTieredNamespace is the check worth having. Naming the
// bookkeeping database after one that holds tiered collections stops the feeder
// mirroring them, and nothing else would say so: the fast tier looks healthy
// and the archive stays empty.
func TestMetaDBOverTieredNamespace(t *testing.T) {
	c := valid()
	c.Feeder.Namespaces = []string{"shop.orders", "wh.parts"}
	c.Meta.DB = "wh"
	err := c.Validate()
	if err == nil {
		t.Fatal("a bookkeeping database holding a tiered namespace was accepted")
	}
	if !strings.Contains(err.Error(), "wh.parts") {
		t.Fatalf("the error does not name the namespace that would stop being mirrored: %v", err)
	}
}

// TestExampleConfigLoads keeps the documented config honest. It is the file
// operators copy, and a new option documented there with a name the loader does
// not accept, or a value it refuses, is found here rather than by them.
// Every configuration this repository ships, not only the example. The loader
// refuses a key it does not know, so a setting renamed in the code and left
// behind in one of these files stops that file from loading at all, and the
// suite or the pilot it belongs to fails in a way that reads like the product.
func TestEveryShippedConfigLoads(t *testing.T) {
	for _, path := range []string{
		"../../cline.example.yaml",
		"../../cline.test.yaml",
		"../../test/cline.yaml",
		"../../test/pilot/pilot.yaml",
		"../../test/store/two.yaml",
	} {
		cfg, err := Load(path)
		if err != nil {
			t.Errorf("%s does not load: %v", path, err)
			continue
		}
		// And none of them may produce a startup warning. Every daemon prints
		// these, so one that fires on the repository's own configs is noise in
		// every suite log and teaches an operator to skip the block.
		for _, w := range cfg.FeederIDWarnings() {
			t.Errorf("%s warns at startup: %s", path, w)
		}
		for _, w := range cfg.AgeFieldWarnings() {
			t.Errorf("%s warns at startup: %s", path, w)
		}
	}
}

func TestExampleConfigLoads(t *testing.T) {
	for _, path := range []string{"../../cline.example.yaml", "../../test/cline.yaml"} {
		cfg, err := Load(path)
		if err != nil {
			t.Errorf("%s does not load: %v", path, err)
			continue
		}
		if cfg.Meta.DB == "" {
			t.Errorf("%s left the bookkeeping database empty", path)
		}
	}
}

// allow_version_skew belongs to the deployment, not to the router, but the
// router is where it is read: every daemon that touches both tiers checks the
// pair, and the router receives its half of the config as RouterConfig alone. A
// setting that parses and never reaches the code that reads it is silent, so
// this is the wiring, not the parsing.
func TestAllowVersionSkewReachesTheRouter(t *testing.T) {
	dir := t.TempDir()
	base := `hot:
  uri: "mongodb://hot1/?replicaSet=rsHot"
cold:
  uri: "mongodb://cold1/?replicaSet=rsCold"
feeder:
  id: "vercheck"
  namespaces:
    - "app.orders"
`
	for _, c := range []struct {
		name string
		yaml string
		want bool
	}{
		{"absent, which is the default", base, false},
		{"off", base + "allow_version_skew: false\n", false},
		{"on", base + "allow_version_skew: true\n", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(dir, "cline.yaml")
			if err := os.WriteFile(path, []byte(c.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("does not load: %v", err)
			}
			if cfg.AllowVersionSkew != c.want {
				t.Errorf("AllowVersionSkew = %v, want %v", cfg.AllowVersionSkew, c.want)
			}
			if cfg.Router.AllowVersionSkew != c.want {
				t.Errorf("the router was given %v, want %v", cfg.Router.AllowVersionSkew, c.want)
			}
		})
	}
}

// A warm window reaching further back than the eviction cutoff tells the two
// components to do opposite things to the same documents. They both obey, so
// the same documents are copied out of the archive, written to the fast tier
// and deleted again for as long as both run.
//
// Named rather than refused, and the distinction is the point. This began as a
// refusal, on the strength of eighteen disagreeing answers measured on the
// pilot under a fifteen year warm against a one day cutoff. That measurement
// belonged to the eviction announcement bug that was tombstoning documents at
// the same time. Measured again with that fixed, under the same two windows and
// with the fast tier emptied by the fight while the questions ran: five rounds
// of 199 questions, no disagreements. It costs work, not correctness.
func TestContradictoryWindowsAreNamed(t *testing.T) {
	withWindows := func(back, older string) Config {
		c := valid()
		// Both rules name a namespace the feeder tiers, because a rule for one
		// it does not is refused outright now: see validateRulesAreTiered.
		c.Feeder.Namespaces = []string{"app.orders", "app.audit"}
		c.Warmer.Rules = []WarmRule{{NS: "app.orders", DateField: "at", Back: mustDur(t, back)}}
		c.Evictor.Rules = []EvictionRule{{NS: "app.orders", DateField: "at", OlderThan: mustDur(t, older)}}
		return c
	}

	c := withWindows("120d", "90d")
	if err := c.Validate(); err != nil {
		t.Errorf("a wide warm must load, because it is waste rather than a wrong answer: %v", err)
	}
	warnings := c.WindowWarnings()
	if len(warnings) != 1 {
		t.Fatalf("a warm reaching 120d against an eviction cutoff of 90d produced %d warnings, want 1", len(warnings))
	}
	// An operator reading this has to be able to find the namespace and both
	// numbers in it, or it names a problem without saying where.
	for _, want := range []string{"app.orders", "120d", "90d"} {
		if !strings.Contains(warnings[0], want) {
			t.Errorf("the warning does not carry %q: %s", want, warnings[0])
		}
	}

	// The ordinary configuration: they meet at the same boundary and never
	// address the same document.
	if w := withWindows("90d", "90d").WindowWarnings(); len(w) != 0 {
		t.Errorf("a warm and an eviction cutoff at the same boundary must be quiet: %v", w)
	}
	// And the safe direction, a warm narrower than the cutoff.
	if w := withWindows("30d", "90d").WindowWarnings(); len(w) != 0 {
		t.Errorf("a warm narrower than the eviction cutoff must be quiet: %v", w)
	}
	// A namespace the evictor never touches is not a contradiction whatever the
	// warm window says.
	other := valid()
	other.Feeder.Namespaces = []string{"app.orders", "app.audit"}
	other.Warmer.Rules = []WarmRule{{NS: "app.audit", DateField: "at", Back: mustDur(t, "3650d")}}
	other.Evictor.Rules = []EvictionRule{{NS: "app.orders", DateField: "at", OlderThan: mustDur(t, "90d")}}
	if w := other.WindowWarnings(); len(w) != 0 {
		t.Errorf("a warm rule with no eviction rule beside it was named: %v", w)
	}
}

func mustDur(t *testing.T, s string) Duration {
	t.Helper()
	d, err := ParseDuration(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return d
}

// A namespace aged one way by the evictor and another by the warmer costs more
// than the two daemons disagreeing: the router stops reading bounds out of
// queries on it, so every one of them is answered by the archive.
func TestAgeFieldWarnings(t *testing.T) {
	withFields := func(evictField, warmField string) Config {
		return Config{
			Evictor: EvictorConfig{Rules: []EvictionRule{
				{NS: "shop.orders", OlderThan: Duration(24 * time.Hour), DateField: evictField},
			}},
			Warmer: WarmerConfig{Rules: []WarmRule{
				{NS: "shop.orders", Back: Duration(24 * time.Hour), DateField: warmField},
			}},
		}
	}

	if w := withFields("createdAt", "createdAt").AgeFieldWarnings(); len(w) != 0 {
		t.Errorf("agreeing rules warned: %v", w)
	}
	if w := withFields("", "").AgeFieldWarnings(); len(w) != 0 {
		t.Errorf("both ageing by the _id warned: %v", w)
	}

	w := withFields("createdAt", "").AgeFieldWarnings()
	if len(w) != 1 {
		t.Fatalf("a disagreement gave %d warnings, want 1: %v", len(w), w)
	}
	if !strings.Contains(w[0], "createdAt") || !strings.Contains(w[0], "_id timestamp") {
		t.Errorf("the warning names neither field: %s", w[0])
	}

	// A namespace only one of them tiers is not a disagreement.
	only := Config{Warmer: WarmerConfig{Rules: []WarmRule{
		{NS: "shop.orders", Back: Duration(24 * time.Hour), DateField: "createdAt"},
	}}}
	if w := only.AgeFieldWarnings(); len(w) != 0 {
		t.Errorf("a namespace with no eviction rule warned: %v", w)
	}
}

// The announcement that says a delete was an eviction expires on a TTL, and the
// feeder only sees that delete once it has caught up to it. A max_feeder_lag
// that reaches the TTL means evictions are read as application deletes and the
// documents are lost from both tiers, so the config refuses it rather than
// leaving it to be discovered.
func TestMaxFeederLagMustStayUnderTheNoticeTTL(t *testing.T) {
	withLag := func(d time.Duration) EvictorConfig {
		return EvictorConfig{
			Rate:         100,
			BatchSize:    100,
			MaxFeederLag: Duration(d),
			FeederID:     "test",
			Backoff: BackoffConfig{
				MaxReplicationLag:   Duration(10 * time.Second),
				MaxCacheDirty:       0.05,
				MinWriteTicketsFree: 0.25,
				MinOplogWindow:      Duration(time.Minute),
			},
		}
	}

	if err := withLag(30 * time.Second).Validate(); err != nil {
		t.Errorf("an ordinary lag was refused: %v", err)
	}
	if err := withLag(meta.NoticeTTL - time.Minute).Validate(); err != nil {
		t.Errorf("a lag just under the notice TTL was refused: %v", err)
	}

	for _, d := range []time.Duration{meta.NoticeTTL, meta.NoticeTTL + time.Hour, 24 * time.Hour} {
		err := withLag(d).Validate()
		if err == nil {
			t.Errorf("max_feeder_lag %s was accepted, and it loses documents", d)
			continue
		}
		if !strings.Contains(err.Error(), "announcement") {
			t.Errorf("the refusal does not say why: %v", err)
		}
	}
}

// A retention shorter than mongod's TTL sweep is a number that cannot be
// honoured, and the likeliest way to write one is a misplaced unit. Refused
// rather than rounded up, so a deployment does not believe its deletes erase in
// seconds.
func TestTombstoneRetentionBounds(t *testing.T) {
	dir := t.TempDir()
	base := `hot:
  uri: "mongodb://hot1/?replicaSet=rsHot"
cold:
  uri: "mongodb://cold1/?replicaSet=rsCold"
feeder:
  id: "ttl"
  namespaces:
    - "app.orders"
`
	for _, c := range []struct {
		name string
		yaml string
		want time.Duration
		err  string
	}{
		{name: "absent keeps deleted documents for ever", yaml: base, want: 0},
		{name: "a day is fine", yaml: base + "  tombstone_retention: 1d\n", want: 24 * time.Hour},
		{name: "a minute is the floor and is accepted", yaml: base + "  tombstone_retention: 1m\n", want: time.Minute},
		{name: "under a minute is refused", yaml: base + "  tombstone_retention: 30s\n",
			err: "once a minute"},
		{name: "negative is refused", yaml: base + "  tombstone_retention: -1h\n",
			err: "cannot be negative"},
		{name: "longer than a TTL index can express is refused",
			yaml: base + "  tombstone_retention: 40000d\n", err: "about 68 years"},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(dir, "cline.yaml")
			if err := os.WriteFile(path, []byte(c.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if c.err != "" {
				if err == nil {
					t.Fatalf("loaded, want an error naming %q", c.err)
				}
				if !strings.Contains(err.Error(), c.err) {
					t.Errorf("error is %q, want it to name %q", err, c.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("does not load: %v", err)
			}
			if got := cfg.Feeder.TombstoneRetention.D(); got != c.want {
				t.Errorf("TombstoneRetention = %v, want %v", got, c.want)
			}
		})
	}
}

// router.claim_grace only means anything where reads fall through to the
// archive, and it is off unless it is asked for. A knob that silently does
// nothing is worse than one that is absent: it reads as protection in place.
func TestClaimGraceBounds(t *testing.T) {
	if got := valid().Router.ClaimGrace; got != 0 {
		t.Fatalf("claim_grace defaults to %v, want off", got)
	}

	cases := []struct {
		name  string
		grace Duration
		mode  Completeness
		want  string
	}{
		{"off, in every mode", 0, Strict, ""},
		{"a minute, falling through to the archive", Duration(time.Minute), Archive, ""},
		{"negative", Duration(-time.Second), Archive, "cannot be negative"},
		{"set, but nothing falls through", Duration(time.Minute), Strict, "only applies to completeness: archive"},
		{"set, but permissive", Duration(time.Minute), Permissive, "only applies to completeness: archive"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := valid()
			cfg.Router.ClaimGrace = c.grace
			cfg.Router.Completeness = c.mode
			err := cfg.Validate()
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case c.want != "" && err == nil:
				t.Fatalf("accepted, want a refusal naming %q", c.want)
			case c.want != "" && !strings.Contains(err.Error(), c.want):
				t.Fatalf("refused with %q, which does not name %q", err, c.want)
			}
		})
	}
}

// A key the loader does not know is refused rather than ignored.
//
// It used to be ignored, which is the worst shape a configuration mistake can
// take: measured on one file, `terminate-auth: true` with a hyphen loaded as
// false, so the router proxied authentication while the operator believed it
// was terminating it and checking privileges itself. Nothing failed and nothing
// was logged. The setting simply did not exist.
func TestAMistypedKeyIsRefused(t *testing.T) {
	base := `hot:
  uri: "mongodb://hot1/?replicaSet=rsHot"
cold:
  uri: "mongodb://cold1/?replicaSet=rsCold"
feeder:
  id: "test"
  namespaces: ["shop.orders"]
router:
  listen: "127.0.0.1:27100"
`
	cases := []struct {
		name string
		add  string
		want string
	}{
		{"nothing added", "", ""},
		{"a hyphen where the key has an underscore", "  terminate-auth: true\n", "terminate-auth"},
		{"a misspelt claim grace", "  claim_grase: 5m\n", "claim_grase"},
		{"a setting under the wrong daemon", "  rate: 2000\n", "rate"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, "cline.yaml")
			if err := os.WriteFile(p, []byte(base+c.add), 0o644); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(p)
			switch {
			case c.want == "":
				if err != nil {
					t.Fatalf("refused a config with nothing wrong with it: %v", err)
				}
			case err == nil:
				t.Fatalf("accepted %q, and terminate_auth came out %v", strings.TrimSpace(c.add), cfg.Router.TerminateAuth)
			case !strings.Contains(err.Error(), c.want):
				t.Fatalf("refused with %q, which does not name %q", err, c.want)
			}
		})
	}
}

// A warmer or evictor rule for a namespace the feeder does not mirror.
//
// The two lists name the same collections in the same file and nothing checked
// that they agreed, which made this an accepted configuration. It hides
// documents: an untiered collection is the fast tier's alone as far as the
// router is concerned, reads of it never fall through, and the evictor removes
// its old documents as soon as it can see a copy in the archive. On an adoption
// it always can, because the archive is the old cluster and holds everything it
// ever held, so the copy satisfying the guard is a cutover snapshot rather than
// a mirror. The warmer's version copies that snapshot back up, returning
// documents the application deleted.
func TestRulesMustNameATieredNamespace(t *testing.T) {
	rule := func(ns string) Config {
		c := valid() // feeder.namespaces is shop.orders
		c.Evictor.Rules = []EvictionRule{{NS: ns, OlderThan: Duration(90 * 24 * time.Hour)}}
		return c
	}
	warm := func(ns string) Config {
		c := valid()
		c.Warmer.Rules = []WarmRule{{NS: ns, Back: Duration(90 * 24 * time.Hour)}}
		return c
	}

	if err := rule("shop.orders").Validate(); err != nil {
		t.Errorf("an eviction rule for a tiered namespace was refused: %v", err)
	}
	if err := warm("shop.orders").Validate(); err != nil {
		t.Errorf("a warm rule for a tiered namespace was refused: %v", err)
	}

	err := rule("shop.invoices").Validate()
	if err == nil {
		t.Fatal("an eviction rule for a namespace nothing tiers was accepted, and it removes documents from the only tier that answers for them")
	}
	for _, want := range []string{"evictor.rules", "shop.invoices", "feeder.namespaces"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q: %v", want, err)
		}
	}

	err = warm("shop.invoices").Validate()
	if err == nil {
		t.Fatal("a warm rule for a namespace nothing tiers was accepted, and it copies the archive's cutover snapshot back up")
	}
	for _, want := range []string{"warmer.rules", "shop.invoices"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q: %v", want, err)
		}
	}

	// An empty list means the feeder mirrors everything, so every rule has its
	// copy and nothing here applies.
	c := rule("shop.invoices")
	c.Feeder.Namespaces = nil
	if err := c.Validate(); err != nil {
		t.Errorf("a feeder that mirrors everything refused a rule: %v", err)
	}
}

// evictor.feeder_id names the checkpoint the mirroring-lag gate reads, and it
// has to be an id some feeder is actually writing.
//
// It defaulted to the literal "default" rather than to feeder.id, so a file
// that named its feeder anything else pointed the gate at a checkpoint nothing
// writes. The lag then reads as unknown, which is the safe direction: the
// evictor pauses instead of removing. So nothing was lost and nothing was aged,
// for ever, and the message said mirroring lag was unknown while the feeder ran
// perfectly. Two settings that must agree, one of which silently disabled a
// daemon when they drifted.
func TestEvictorFeederIDFollowsTheFeeder(t *testing.T) {
	write := func(t *testing.T, extra string) Config {
		dir := t.TempDir()
		p := filepath.Join(dir, "cline.yaml")
		base := `hot:
  uri: "mongodb://hot1/?replicaSet=rsHot"
cold:
  uri: "mongodb://cold1/?replicaSet=rsCold"
feeder:
  id: "prod"
  namespaces: ["shop.orders"]
router:
  listen: "127.0.0.1:27100"
`
		if err := os.WriteFile(p, []byte(base+extra), 0o644); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(p)
		if err != nil {
			t.Fatalf("does not load: %v", err)
		}
		return cfg
	}

	// Not spelled out: it follows the feeder, and the router's already did.
	cfg := write(t, "")
	if cfg.Evictor.FeederID != "prod" {
		t.Errorf("evictor.feeder_id came out %q, want the feeder's own %q", cfg.Evictor.FeederID, "prod")
	}
	if cfg.Router.FeederID != "prod" {
		t.Errorf("router.feeder_id came out %q, want %q", cfg.Router.FeederID, "prod")
	}
	if w := cfg.FeederIDWarnings(); len(w) != 0 {
		t.Errorf("an inherited id was warned about: %v", w)
	}

	// Spelled out and the same: no warning either.
	if w := write(t, "evictor:\n  feeder_id: \"prod\"\n").FeederIDWarnings(); len(w) != 0 {
		t.Errorf("an id that agrees was warned about: %v", w)
	}

	// Spelled out and different: legal, because the daemons can be run from
	// separate files, and named because it is usually a mistake.
	cfg = write(t, "evictor:\n  feeder_id: \"stale\"\n")
	if cfg.Evictor.FeederID != "stale" {
		t.Fatalf("an explicit id was overridden: %q", cfg.Evictor.FeederID)
	}
	w := cfg.FeederIDWarnings()
	if len(w) != 1 {
		t.Fatalf("a mismatched id produced %d warnings, want 1", len(w))
	}
	for _, want := range []string{"stale", "prod", "evicted"} {
		if !strings.Contains(w[0], want) {
			t.Errorf("the warning does not carry %q: %s", want, w[0])
		}
	}
}

// Manages is on the hot path and answers from a set built at load time, with a
// linear scan left in for a config built in code and never loaded. Both have to
// give the same answer, or a deployment behaves differently from a unit test.
func TestManagesAgreesWhicheverWay(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "cline.yaml")
	if err := os.WriteFile(p, []byte(`hot:
  uri: "mongodb://hot1/?replicaSet=rsHot"
cold:
  uri: "mongodb://cold1/?replicaSet=rsCold"
feeder:
  id: "test"
  namespaces: ["shop.orders", "shop.events", "wh.stock"]
router:
  listen: "127.0.0.1:27100"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	// The same thing assembled by hand, which never goes through Load and so
	// has no set to answer from.
	built := RouterConfig{Managed: []string{"shop.orders", "shop.events", "wh.stock"}}

	for _, ns := range []string{"shop.orders", "shop.events", "wh.stock", "shop.other", "", "shop", "orders"} {
		l, b := loaded.Router.Manages(ns), built.Manages(ns)
		if l != b {
			t.Errorf("Manages(%q): loaded %v, built by hand %v", ns, l, b)
		}
	}
	for _, ns := range []string{"shop.orders", "wh.stock"} {
		if !loaded.Router.Manages(ns) {
			t.Errorf("Manages(%q) is false for a namespace the feeder tiers", ns)
		}
	}
	if loaded.Router.Manages("shop.untiered") {
		t.Error("Manages is true for a namespace nothing tiers")
	}
	// Empty still means everything, through both paths.
	if !(RouterConfig{}).Manages("anything.at.all") {
		t.Error("an empty list stopped meaning every namespace")
	}
}

// The advisor's table is bounded, because the eviction that keeps it at size is
// a sort of the whole table under a lock every cacheable read waits behind.
//
// Measured with BenchmarkAdvisorEvictionPause, per eviction: 89us at the
// default 1,000, 823us at 10,000, 14.2ms at 100,000 and 154ms at a million.
// The amortised cost is fine at every one of those, because a batch covers the
// next tenth of a table's worth of inserts. The stall is not, and "cache more"
// is the obvious thing for an operator to type.
func TestAdvisorTableIsBounded(t *testing.T) {
	cases := []struct {
		name string
		max  int
		want string
	}{
		{"the default", 1000, ""},
		{"ten thousand", 10000, ""},
		{"the last row that stays in milliseconds", maxAdvisorShapes, ""},
		{"one past it", maxAdvisorShapes + 1, "max_queries"},
		{"a million, which is the 154ms one", 1000000, "154ms"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := valid()
			cfg.Router.Cache.MaxQueries = c.max
			err := cfg.Validate()
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("refused %d: %v", c.max, err)
			case c.want != "" && err == nil:
				t.Fatalf("accepted %d, want a refusal naming %q", c.max, c.want)
			case c.want != "" && !strings.Contains(err.Error(), c.want):
				t.Fatalf("refused with %q, which does not name %q", err, c.want)
			}
		})
	}
}

// min_free_disk is a fraction sitting next to a number that reads like a
// percentage, and nothing refused the percentage.
//
// Written as 20, meaning twenty per cent, the warmer compares the filesystem's
// 0.85 against 20 on every pass and backs off for ever: the fast tier is never
// filled and every read of an aged range falls through to the archive. It is
// not silent, it logs "limit 2000.0%", and a limit of 2000% is not a line
// anybody goes looking for.
func TestMinFreeDiskMustBeAFraction(t *testing.T) {
	cases := []struct {
		name string
		v    float64
		want string
	}{
		{"off", 0, ""},
		{"a tenth", 0.1, ""},
		{"most of the disk", 0.9, ""},
		{"the whole disk, which never warms", 1, "fraction between 0 and 1"},
		{"twenty, meant as a percentage", 20, "0.2 is twenty per cent"},
		{"negative", -0.1, "fraction between 0 and 1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := valid()
			cfg.Warmer.MinFreeDisk = c.v
			err := cfg.Validate()
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("refused %v: %v", c.v, err)
			case c.want != "" && err == nil:
				t.Fatalf("accepted %v, want a refusal naming %q", c.v, c.want)
			case c.want != "" && !strings.Contains(err.Error(), c.want):
				t.Fatalf("refused with %q, which does not name %q", err, c.want)
			}
		})
	}
}

// archive_wait: 0 turns off the wait strict refuses on, so under strict it
// answered every fall-through from the archive as it stood, nothing refused and
// nothing counted.
func TestArchiveWaitZeroNeedsPermissive(t *testing.T) {
	c := valid()
	c.Router.ArchiveWait = 0
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "archive_wait") {
		t.Errorf("archive_wait 0 under strict was accepted: %v", err)
	}
	c.Router.Freshness = FreshnessPermissive
	if err := c.Validate(); err != nil {
		t.Errorf("archive_wait 0 under permissive was refused: %v", err)
	}
	c.Router.ArchiveWait = Duration(-time.Second)
	if err := c.Validate(); err == nil {
		t.Error("a negative archive_wait was accepted")
	}
}
