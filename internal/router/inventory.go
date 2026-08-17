package router

import (
	"context"
	"sort"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
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
	managed := append([]string(nil), r.cfg.Managed...)
	sort.Strings(managed)

	out := Inventory{
		Namespaces:  make([]NamespaceSize, 0, len(managed)),
		UpdatedUnix: time.Now().Unix(),
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
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			size := NamespaceSize{NS: ns}
			size.FastDocs, size.FastBytes = collSize(ctx, r, true, db, coll)
			size.ArchiveDocs, size.ArchiveBytes = collSize(ctx, r, false, db, coll)
			sizes[i] = size
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
