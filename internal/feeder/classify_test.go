package feeder

import (
	"testing"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
)

func TestClassifyOp(t *testing.T) {
	tests := []struct {
		op      string
		want    action
		decided bool
		why     string
	}{
		{"insert", actionUpsert, true, "new documents reach the archive"},
		{"update", actionUpsert, true, "updates are mirrored as whole documents"},
		{"replace", actionUpsert, true, "replacements are mirrored as whole documents"},
		{"delete", actionSkip, false, "deletes need the eviction notice to decide"},

		// The rule the archive depends on: destructive DDL never crosses over.
		{"drop", actionSkip, true, "dropping a collection must not drop the archive"},
		{"dropDatabase", actionSkip, true, "dropping a database must not drop the archive"},
		{"rename", actionSkip, true, "renames are not replayed"},
		{"invalidate", actionSkip, true, "invalidate is a stream event, not data"},
		{"shardCollection", actionSkip, true, "topology changes are not data"},
		{"somethingAddedInMongoDB9", actionSkip, true, "unknown operations are never replayed"},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			got, decided := classifyOp(tt.op)
			if got != tt.want || decided != tt.decided {
				t.Errorf("classifyOp(%q) = (%v, %v), want (%v, %v): %s",
					tt.op, got, decided, tt.want, tt.decided, tt.why)
			}
		})
	}
}

func TestClassifyDelete(t *testing.T) {
	tests := []struct {
		name      string
		announced bool
		policy    config.UnannouncedDeletes
		want      action
		why       string
	}{
		{
			name: "announced delete is an eviction", announced: true,
			policy: config.Tombstone, want: actionSkip,
			why: "the evictor moved the document down a tier; the archive keeps it",
		},
		{
			name: "announced delete ignores policy", announced: true,
			policy: config.Halt, want: actionSkip,
			why: "policy only applies when nothing announced the delete",
		},
		{
			name: "unannounced delete tombstones by default", announced: false,
			policy: config.Tombstone, want: actionTombstone,
			why: "the evictor is the only supported way to remove data",
		},
		{
			name: "unannounced delete can be ignored", announced: false,
			policy: config.Ignore, want: actionSkip,
			why: "operators who bulk-delete outside the evictor keep their archive",
		},
		{
			name: "unannounced delete can halt", announced: false,
			policy: config.Halt, want: actionHalt,
			why: "stop mirroring rather than guess",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyDelete(tt.announced, tt.policy); got != tt.want {
				t.Errorf("classifyDelete(%v, %q) = %v, want %v: %s",
					tt.announced, tt.policy, got, tt.want, tt.why)
			}
		})
	}
}

func TestSkipNamespace(t *testing.T) {
	for _, db := range []string{"admin", "local", "config", meta.DB} {
		if !skipNamespace(db) {
			t.Errorf("skipNamespace(%q) = false, want true", db)
		}
	}
	for _, db := range []string{"app", "orders", "thermocline", "_thermocline_backup"} {
		if skipNamespace(db) {
			t.Errorf("skipNamespace(%q) = true, want false", db)
		}
	}
}

func TestSplitNS(t *testing.T) {
	tests := []struct{ ns, db, coll string }{
		{"app.orders", "app", "orders"},
		{"app.events.2026", "app", "events.2026"}, // collection names may contain dots
		{"app", "app", ""},
	}
	for _, tt := range tests {
		db, coll := splitNS(tt.ns)
		if db != tt.db || coll != tt.coll {
			t.Errorf("splitNS(%q) = (%q, %q), want (%q, %q)", tt.ns, db, coll, tt.db, tt.coll)
		}
	}
}
