package main

import (
	"fmt"
	"log/slog"
	"strings"

	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"

	"github.com/zelmario/thermocline/internal/config"
)

// version is what cline version prints and every daemon logs at startup.
const version = "0.1.0-dev"

// logStartup writes one line a daemon starts with, holding what support asks
// for first: which build, which file, which clusters, what is tiered and by
// what age, and how it logs. The tiers' MongoDB versions follow it, from
// tierver.Check.
func logStartup(log *slog.Logger, cfg config.Config, path, component string) {
	args := []any{"cline", version, "component", component, "config", path,
		"fast_tier", hostsOf(cfg.Hot.URI), "archive", hostsOf(cfg.Cold.URI),
		"collections", tieredSummary(cfg), "feeder_id", cfg.Feeder.ID, "meta_db", cfg.Meta.DB}
	if component == "router" || component == "run" {
		args = append(args, "listen", cfg.Router.Listen)
	}
	format, level := cfg.Log.Format, cfg.Log.Level
	if format == "" {
		format = "text"
	}
	if level == "" {
		level = "info"
	}
	log.Info("starting", append(args, "log_format", format, "log_level", level)...)
}

// hostsOf is a connection string's replica set and hosts, and nothing else:
// the string itself carries the password.
func hostsOf(uri string) string {
	cs, err := connstring.ParseAndValidate(uri)
	if err != nil {
		return "unparsed"
	}
	hosts := strings.Join(cs.Hosts, ",")
	if cs.ReplicaSet != "" {
		return cs.ReplicaSet + "/" + hosts
	}
	return hosts
}

// tieredSummary names each tiered collection with the age it leaves the fast
// tier at and the field that age is read from.
func tieredSummary(cfg config.Config) string {
	if len(cfg.Feeder.Namespaces) == 0 {
		return "every collection"
	}
	rules := map[string]config.EvictionRule{}
	for _, r := range cfg.Evictor.Rules {
		rules[r.NS] = r
	}
	parts := make([]string, 0, len(cfg.Feeder.Namespaces))
	for _, ns := range cfg.Feeder.Namespaces {
		r, ok := rules[ns]
		switch {
		case !ok:
			parts = append(parts, ns+"=not evicted")
		case r.UsesObjectIDTime():
			parts = append(parts, fmt.Sprintf("%s=%s by _id", ns, r.OlderThan))
		default:
			parts = append(parts, fmt.Sprintf("%s=%s by %s", ns, r.OlderThan, r.DateField))
		}
	}
	return strings.Join(parts, " ")
}
