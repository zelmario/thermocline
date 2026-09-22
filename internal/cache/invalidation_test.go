package cache

import (
	"testing"

	"github.com/zelmario/thermocline/internal/meta"
)

func TestNothingCanInvalidate(t *testing.T) {
	unreachable := []string{
		"local.oplog.rs",
		"admin.system.users",
		"admin.system.roles",
		"config.collections",
		"config.system.sessions",
		meta.DB + ".watermarks",
		meta.DB + ".keys",
		"shop.system.views",
		"shop.system.profile",
	}
	for _, ns := range unreachable {
		if !NothingCanInvalidate(ns) {
			t.Errorf("%s: want unreachable, got reachable", ns)
		}
	}
	// A collection whose name merely begins the same way is the application's,
	// and dropping it from the cache would be dropping real traffic.
	reachable := []string{
		"shop.orders",
		"shop.systems",
		"shop.system",
		"sample_mflix.comments",
		"configuration.things",
		"locality.places",
		"administration.staff",
	}
	for _, ns := range reachable {
		if NothingCanInvalidate(ns) {
			t.Errorf("%s: want reachable, got unreachable", ns)
		}
	}
}
