package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zelmario/thermocline/internal/config"
)

// log.format json writes one object a line, and log.level drops what is
// below it.
func TestLogFormatAndLevel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cline.log")
	cfg := config.Default()
	cfg.Log.Format, cfg.Log.Level = "json", "warn"
	log, closeLog, err := setupLogging(path, cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("an info line", "n", 1)
	log.Warn("a warn line", "n", 2)
	closeLog()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("not JSON: %q: %v", l, err)
		}
		lines = append(lines, m)
	}
	// setupLogging's own "logging to file" is info, so warn drops it too.
	if len(lines) != 1 || lines[0]["msg"] != "a warn line" || lines[0]["n"] != float64(2) {
		t.Errorf("got %v, want only the warn line", lines)
	}
}

// The startup line names the clusters without the credentials their
// connection strings carry.
func TestStartupLineHasNoPassword(t *testing.T) {
	got := hostsOf("mongodb://dba:s3cret@h1:27017,h2:27017/?replicaSet=rsHot&authSource=admin")
	if got != "rsHot/h1:27017,h2:27017" {
		t.Errorf("hostsOf = %q", got)
	}
	if strings.Contains(got, "s3cret") || strings.Contains(got, "dba") {
		t.Errorf("hostsOf leaked credentials: %q", got)
	}
	cfg := config.Default()
	cfg.Feeder.Namespaces = []string{"app.orders", "app.events", "app.logs"}
	cfg.Evictor.Rules = []config.EvictionRule{
		{NS: "app.orders", OlderThan: config.Duration(90 * 24 * 3600e9)},
		{NS: "app.events", OlderThan: config.Duration(30 * 24 * 3600e9), DateField: "createdAt"},
	}
	if s := tieredSummary(cfg); s != "app.orders=90d by _id app.events=30d by createdAt app.logs=not evicted" {
		t.Errorf("tieredSummary = %q", s)
	}
}
