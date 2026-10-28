package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func loadString(t *testing.T, body string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cline.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

// One line per collection becomes the feeder's namespace, the evictor's rule
// and the warmer's rule, with the same age in both rules.
func TestShortFormExpands(t *testing.T) {
	cfg, err := loadString(t, `
hot: mongodb://h1,h2,h3/?replicaSet=rsHot
cold: mongodb://c1,c2,c3/?replicaSet=rsCold
listen: 0.0.0.0:27017
collections:
  app.orders: 90d
  app.events:
    older_than: 30d
    date_field: createdAt
`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Hot.URI != "mongodb://h1,h2,h3/?replicaSet=rsHot" || cfg.Cold.URI != "mongodb://c1,c2,c3/?replicaSet=rsCold" {
		t.Errorf("uris: hot %q cold %q", cfg.Hot.URI, cfg.Cold.URI)
	}
	if cfg.Router.Listen != "0.0.0.0:27017" {
		t.Errorf("router.listen = %q", cfg.Router.Listen)
	}
	if got := strings.Join(cfg.Feeder.Namespaces, ","); got != "app.events,app.orders" {
		t.Errorf("feeder.namespaces = %s", got)
	}
	if !cfg.Router.Manages("app.orders") || !cfg.Router.Manages("app.events") {
		t.Errorf("the router does not manage both collections: %v", cfg.Router.Managed)
	}
	want := map[string]struct {
		age   time.Duration
		field string
	}{"app.orders": {90 * 24 * time.Hour, ""}, "app.events": {30 * 24 * time.Hour, "createdAt"}}
	if len(cfg.Evictor.Rules) != 2 || len(cfg.Warmer.Rules) != 2 {
		t.Fatalf("rules: %d evictor, %d warmer", len(cfg.Evictor.Rules), len(cfg.Warmer.Rules))
	}
	for _, r := range cfg.Evictor.Rules {
		if w := want[r.NS]; r.OlderThan.D() != w.age || r.DateField != w.field {
			t.Errorf("evictor rule %s: %s by %q, want %s by %q", r.NS, r.OlderThan, r.DateField, w.age, w.field)
		}
	}
	for _, r := range cfg.Warmer.Rules {
		if w := want[r.NS]; r.Back.D() != w.age || r.DateField != w.field {
			t.Errorf("warmer rule %s: %s by %q, want %s by %q", r.NS, r.Back, r.DateField, w.age, w.field)
		}
	}
	if f, ok := cfg.Router.AgeField("app.events"); !ok || f != "createdAt" {
		t.Errorf("router age field for app.events = %q, %v", f, ok)
	}
}

// The long form still loads, and the two forms can share a file as long as
// no collection is named twice.
func TestShortFormBesideLongForm(t *testing.T) {
	cfg, err := loadString(t, `
hot:
  uri: mongodb://h1/?replicaSet=rsHot
cold:
  uri: mongodb://c1/?replicaSet=rsCold
feeder:
  namespaces: [app.logs]
evictor:
  rate: 50
  rules:
    - {ns: app.logs, older_than: 7d}
collections:
  app.orders: 90d
`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Evictor.Rate != 50 || len(cfg.Evictor.Rules) != 2 || len(cfg.Feeder.Namespaces) != 2 {
		t.Errorf("rate %d, %d rules, namespaces %v", cfg.Evictor.Rate, len(cfg.Evictor.Rules), cfg.Feeder.Namespaces)
	}
	if cfg.Router.Listen != Default().Router.Listen {
		t.Errorf("router.listen = %q without listen", cfg.Router.Listen)
	}
}

func TestShortFormRefusals(t *testing.T) {
	const uris = "hot: mongodb://h1/?replicaSet=a\ncold: mongodb://c1/?replicaSet=b\n"
	for _, c := range []struct{ name, body, want string }{
		{"named twice", uris + "feeder:\n  namespaces: [app.orders]\ncollections:\n  app.orders: 90d\n",
			"app.orders is in collections and in feeder.namespaces"},
		{"in an evictor rule", uris + "evictor:\n  rules: [{ns: app.orders, older_than: 9d}]\ncollections:\n  app.orders: 90d\n",
			"app.orders is in collections and in evictor.rules"},
		{"two listens", uris + "listen: 0.0.0.0:27017\nrouter:\n  listen: 127.0.0.1:27017\n",
			"listen and router.listen are both set"},
		{"no age", uris + "collections:\n  app.orders: {date_field: createdAt}\n",
			"app.orders needs an age"},
		{"misspelt field", uris + "collections:\n  app.orders: {older_than: 90d, datefield: createdAt}\n",
			"field datefield not found"},
		{"misspelt uri key", "hot:\n  url: mongodb://h1\ncold: mongodb://c1\n",
			"field url not found"},
		{"bad age", uris + "collections:\n  app.orders: ninety\n", "duration"},
		{"not db.collection", uris + "collections:\n  orders: 90d\n", "orders"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := loadString(t, c.body)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("got %v, want an error containing %q", err, c.want)
			}
		})
	}
}
