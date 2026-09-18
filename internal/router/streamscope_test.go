package router

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
)

// A change stream on a whole database or the whole deployment carries events
// for tiered collections, and noteChangeStream filters them for exactly that
// reason. The counter and the warnings have to agree with the sieve about
// which streams matter, or an operator watching change_streams_total sees zero
// while the router is hiding events from a stream that is open right now.
func TestEveryStreamTheSieveFiltersIsCounted(t *testing.T) {
	watch := func(coll any, resume bool) bson.D {
		spec := bson.D{}
		if resume {
			spec = append(spec, bson.E{Key: "startAtOperationTime", Value: bson.Raw(nil)})
		}
		return bson.D{
			{Key: "aggregate", Value: coll},
			{Key: "pipeline", Value: bson.A{bson.D{{Key: "$changeStream", Value: spec}}}},
			{Key: "cursor", Value: bson.D{}},
		}
	}

	cases := []struct {
		name string
		db   string
		cmd  bson.D
	}{
		{"one tiered collection", "shop", watch("orders", false)},
		{"the whole database", "shop", watch(int32(1), false)},
		{"the whole deployment", "admin", watch(int32(1), false)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &conn{
				r: &Router{cfg: config.RouterConfig{
					Managed: []string{"shop.orders"},
				}},
				log: slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			c.guardAggregate(context.Background(), msgOf(t, tc.cmd), tc.db)
			if got := c.r.Stats.ChangeStreams.Load(); got != 1 {
				t.Errorf("change streams counted = %d, want 1", got)
			}
		})
	}
}
