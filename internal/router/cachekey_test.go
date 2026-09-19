package router

import (
	"io"
	"log/slog"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
)

// The cache key and the advisor's shape have to agree about what makes two
// commands the same query, or the advisor recommends caching something the
// cache can never hit.
//
// cache.Shape ignores comment and maxTimeMS and sorts the command's fields.
// The key did neither, so an application that sets a per-request comment, which
// is what every APM agent does, got a different key every time while
// `cline cache` reported the shape as highly repetitive and worth caching.
func TestCacheKeyAgreesWithTheShape(t *testing.T) {
	key := func(d bson.D) string {
		raw, err := bson.Marshal(d)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		c := &conn{
			r:   &Router{cfg: config.RouterConfig{}},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		return c.cacheKey("find", "shop.orders", raw)
	}

	base := bson.D{
		{Key: "find", Value: "orders"},
		{Key: "filter", Value: bson.D{{Key: "customer", Value: "c1"}}},
		{Key: "limit", Value: 10},
	}
	want := key(base)

	same := []struct {
		name string
		cmd  bson.D
	}{
		{"a per-request comment", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "filter", Value: bson.D{{Key: "customer", Value: "c1"}}},
			{Key: "limit", Value: 10},
			{Key: "comment", Value: "trace-id-9f3a2c"}}},
		{"a different per-request comment", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "filter", Value: bson.D{{Key: "customer", Value: "c1"}}},
			{Key: "limit", Value: 10},
			{Key: "comment", Value: "trace-id-0000001"}}},
		{"a per-call deadline", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "filter", Value: bson.D{{Key: "customer", Value: "c1"}}},
			{Key: "limit", Value: 10},
			{Key: "maxTimeMS", Value: 4871}}},
		{"the same fields in another order", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "limit", Value: 10},
			{Key: "filter", Value: bson.D{{Key: "customer", Value: "c1"}}}}},
	}
	for _, c := range same {
		if got := key(c.cmd); got != want {
			t.Errorf("%s: key differs from the plain command, so it can never hit", c.name)
		}
	}

	// And the controls. These are different questions and must not share an
	// answer, which is the direction that would serve one client another's data.
	diff := []struct {
		name string
		cmd  bson.D
	}{
		{"a different filter value", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "filter", Value: bson.D{{Key: "customer", Value: "c2"}}},
			{Key: "limit", Value: 10}}},
		{"a different limit", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "filter", Value: bson.D{{Key: "customer", Value: "c1"}}},
			{Key: "limit", Value: 11}}},
		{"a batch size, which changes the reply", bson.D{
			{Key: "find", Value: "orders"},
			{Key: "filter", Value: bson.D{{Key: "customer", Value: "c1"}}},
			{Key: "limit", Value: 10},
			{Key: "cursor", Value: bson.D{{Key: "batchSize", Value: 5}}}}},
	}
	for _, c := range diff {
		if got := key(c.cmd); got == want {
			t.Errorf("%s: shares a key with the plain command", c.name)
		}
	}
}

// cacheKey runs on every cacheable read when the cache is on, and agreeing with
// the shape cost it a slice and a map it did not have. Measured rather than
// assumed, because replacing a map with a sorted slice in cache.Shape once
// allocated 26 per cent more and ran slower, and this is that trade in reverse.
func BenchmarkCacheKey(b *testing.B) {
	raw, err := bson.Marshal(bson.D{
		{Key: "find", Value: "orders"},
		{Key: "filter", Value: bson.D{{Key: "customer", Value: "c1"}, {Key: "status", Value: "shipped"}}},
		{Key: "sort", Value: bson.D{{Key: "at", Value: -1}}},
		{Key: "limit", Value: 20},
		{Key: "comment", Value: "trace-id-9f3a2c"},
		{Key: "maxTimeMS", Value: 4871},
		{Key: "lsid", Value: bson.D{{Key: "id", Value: "x"}}},
		{Key: "$db", Value: "shop"},
	})
	if err != nil {
		b.Fatal(err)
	}
	c := &conn{r: &Router{cfg: config.RouterConfig{}}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if c.cacheKey("find", "shop.orders", raw) == "" {
			b.Fatal("empty key")
		}
	}
}
