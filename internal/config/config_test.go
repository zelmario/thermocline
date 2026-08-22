package config

import (
	"strings"
	"testing"

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
