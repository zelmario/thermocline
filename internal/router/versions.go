package router

import (
	"context"

	"github.com/zelmario/thermocline/internal/tierver"
)

// The requirement, and why it is a refusal rather than a warning, is in
// internal/tierver. This is where the router applies it: before the listener
// opens, because a router that starts on a mismatched pair answers correctly
// for months and then fails on the day the data ages.

// checkTierVersions stops the router when the tiers run different MongoDB
// releases, unless the configuration says to allow it.
func (r *Router) checkTierVersions(ctx context.Context) error {
	return tierver.Check(ctx, r.hot, r.cold, r.cfg.AllowVersionSkew, r.log)
}
