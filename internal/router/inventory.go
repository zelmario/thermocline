package router

import (
	"context"
	"sort"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
)

// The dashboard's most useful single fact is the shape of the split: how little
// of the data the fast tier holds, against how much of the traffic it answers.
// A deployment where the fast tier holds three percent of the documents and
// serves ninety-nine percent of the reads is the whole argument for tiering, and
// it is not an argument anyone should have to take on faith.
//
// Both numbers come from collection metadata rather than a count, so this costs
// two cheap commands per namespace and can be refreshed on a timer without
// anybody noticing.

// inventoryEvery is how often the tier sizes are refreshed. They move slowly:
// an eviction sweep or a warm, not individual writes.
const inventoryEvery = 30 * time.Second

// inventoryWorkers bounds how many namespaces are measured at once.
//
// Two round trips each, so this is the number of extra connections the dashboard
// asks of each tier while it refreshes, and it is deliberately a small number
// rather than a fast one.
const inventoryWorkers = 8

// NamespaceSize is one collection across both tiers.
type NamespaceSize struct {
	NS           string `json:"ns"`
	FastDocs     int64  `json:"fast_docs"`
	ArchiveDocs  int64  `json:"archive_docs"`
	FastBytes    int64  `json:"fast_bytes"`
	ArchiveBytes int64  `json:"archive_bytes"`
}

// Inventory is what both tiers are holding.
type Inventory struct {
	Namespaces  []NamespaceSize `json:"namespaces"`
	UpdatedUnix int64           `json:"updated"`
	Err         string          `json:"err,omitempty"`

	// KeyRegistryRows is how many unique-key claims the registry holds, which
	// is the one piece of this system's bookkeeping that grows with history and
	// does so on the fast tier.
	//
	// A claim is permanent on purpose: evicting a document takes its index entry
	// off the fast tier and the claim is what remembers the value was taken.
	// Nothing removes one whose document has been deleted from both tiers except
	// `cline keys --prune`, and that is an operator's obligation with nothing
	// asking for it. Measured in docs/router.md: 500 inserts and 500 deletes
	// leave 500 claims, about 100 bytes each, on the expensive tier.
	//
	// Reported rather than alerted on. There is no number that is wrong on its
	// own: what matters is this climbing while the data it constrains does not,
	// and only somebody who knows the application can say that.
	KeyRegistryRows int64 `json:"keyRegistryRows"`

	// NotSuperset is how many namespaces the archive is missing documents of,
	// because it refused them and the feeder gave up. See superset.go: while
	// this is above zero, every read of those namespaces that the fast tier
	// cannot answer in full is refused.
	//
	// A state rather than an event, and that is the whole reason it is here.
	// The feeder's counter says when a document was given up, so the alert on
	// it fires once and goes quiet; this says the deployment is still in that
	// condition, which is what somebody paged at three in the morning needs to
	// know, and it is what the reads are being refused for.
	NotSuperset int64 `json:"notSuperset"`

	// NotSupersetDocuments is how many documents that is across them, which is
	// the number an operator works through.
	NotSupersetDocuments int64 `json:"notSupersetDocuments"`
}

type inventoryCache struct {
	mu   sync.RWMutex
	last Inventory
}

// InventorySnapshot returns the last reading, which may be empty until the
// first refresh has run.
func (r *Router) InventorySnapshot() Inventory {
	r.inv.mu.RLock()
	defer r.inv.mu.RUnlock()
	return r.inv.last
}

// refreshInventory keeps the tier sizes up to date until ctx is done.
func (r *Router) refreshInventory(ctx context.Context) {
	r.readInventory(ctx)
	t := time.NewTicker(inventoryEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.readInventory(ctx)
		}
	}
}

func (r *Router) readInventory(ctx context.Context) {
	// Not r.cfg.Managed directly: `feeder.namespaces` empty tiers everything,
	// and reading the list there gave an operator a dashboard saying nothing is
	// tiered on a router tiering the lot.
	managed := r.tieredNamespaces(ctx)
	sort.Strings(managed)

	out := Inventory{
		Namespaces:  make([]NamespaceSize, 0, len(managed)),
		UpdatedUnix: time.Now().Unix(),
	}
	// One read of a collection that is empty in every healthy deployment, on
	// the same timer, so the state is reported for as long as it lasts rather
	// than only when it starts.
	if byNS, err := meta.UnmirroredNamespaces(ctx, r.cold); err == nil {
		out.NotSuperset = int64(len(byNS))
		for _, n := range byNS {
			out.NotSupersetDocuments += int64(n)
		}
	}

	// An estimate, from collection metadata rather than a scan: the registry is
	// the largest collection this system writes and counting it exactly on a
	// timer would be the dashboard hurting the tier it reports on.
	if n, err := r.keys().EstimatedDocumentCount(ctx); err == nil {
		out.KeyRegistryRows = n
	}
	// Concurrently, bounded. collStats answers about one collection, so there is
	// no batched form of it to reach for; what makes this affordable at a
	// hundred namespaces is not asking one after another. Serially it was two
	// round trips per namespace with everything else waiting, so the refresh
	// took longer than the interval it runs on and the dashboard showed numbers
	// that were minutes old while appearing to be seconds old.
	//
	// The bound matters as much as the concurrency. An unbounded fan-out over a
	// few hundred namespaces opens a few hundred connections to each tier at
	// once, on a timer, which is a good way to make an operator dashboard the
	// thing that hurts the cluster.
	sizes := make([]NamespaceSize, len(managed))
	var wg sync.WaitGroup
	slots := make(chan struct{}, inventoryWorkers)
	for i, ns := range managed {
		db, coll := splitNS(ns)
		if db == "" || coll == "" {
			continue
		}
		wg.Add(1)
		go func(i int, ns, db, coll string) {
			// wg.Done outside the recovery, so a panic here still releases the
			// wait rather than hanging the caller.
			defer wg.Done()
			r.safely("measuring a namespace for the dashboard", func() {
				slots <- struct{}{}
				defer func() { <-slots }()
				size := NamespaceSize{NS: ns}
				size.FastDocs, size.FastBytes = collSize(ctx, r, true, db, coll)
				size.ArchiveDocs, size.ArchiveBytes = collSize(ctx, r, false, db, coll)
				sizes[i] = size
			})
		}(i, ns, db, coll)
	}
	wg.Wait()
	for _, size := range sizes {
		if size.NS == "" {
			continue
		}
		out.Namespaces = append(out.Namespaces, size)
	}

	r.inv.mu.Lock()
	r.inv.last = out
	r.inv.mu.Unlock()
}

// collSize reads a collection's document count and size from its metadata.
//
// collStats is O(1) against the catalog rather than a scan, which is what makes
// this affordable on a timer. A collection that does not exist yet is not an
// error, it is a zero.
func collSize(ctx context.Context, r *Router, fast bool, db, coll string) (docs, bytes int64) {
	client := r.cold
	if fast {
		client = r.hot
	}
	var reply bson.Raw
	err := client.Database(db).RunCommand(ctx, bson.D{
		{Key: "collStats", Value: coll},
		{Key: "scale", Value: 1},
	}).Decode(&reply)
	if err != nil {
		return 0, 0
	}
	if v, lerr := reply.LookupErr("count"); lerr == nil {
		if n, ok := intOf(v); ok {
			docs = int64(n)
		}
	}
	if v, lerr := reply.LookupErr("size"); lerr == nil {
		if n, ok := intOf(v); ok {
			bytes = int64(n)
		}
	}
	return docs, bytes
}
