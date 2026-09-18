package router

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// What the reserved-field guard costs per document, measured rather than
// reasoned about. It is on the write path for every document written to a
// tiered collection, and the cluster cannot resolve a few percent: the same
// binary came back at 7,342/s and 15,408/s in two consecutive runs.
func benchDoc(b *testing.B) bson.Raw {
	b.Helper()
	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: bson.NewObjectID()},
		{Key: "seq", Value: 12345},
		{Key: "customer", Value: "c000123"},
		{Key: "total", Value: 91.25},
		{Key: "status", Value: "placed"},
		{Key: "createdAt", Value: time.Now()},
		{Key: "lines", Value: bson.A{
			bson.D{{Key: "sku", Value: "s0042"}, {Key: "qty", Value: 2}, {Key: "price", Value: 19.99}},
			bson.D{{Key: "sku", Value: "s0311"}, {Key: "qty", Value: 1}, {Key: "price", Value: 51.27}},
		}},
	})
	if err != nil {
		b.Fatal(err)
	}
	return raw
}

func BenchmarkReservedTopLevel(b *testing.B) {
	doc := benchDoc(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, bad := reservedTopLevel(doc); bad {
			b.Fatal("this document carries no reserved field")
		}
	}
}

func BenchmarkReservedInUpdate(b *testing.B) {
	raw, err := bson.Marshal(bson.D{
		{Key: "$set", Value: bson.D{{Key: "status", Value: "shipped"}, {Key: "shippedAt", Value: time.Now()}}},
		{Key: "$inc", Value: bson.D{{Key: "version", Value: 1}}},
	})
	if err != nil {
		b.Fatal(err)
	}
	u := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: raw}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, bad := reservedInUpdate(u); bad {
			b.Fatal("this update carries no reserved field")
		}
	}
}

// The other side of the pre-filter: an update whose bytes do contain the names'
// common prefix pays the parse. Ordinary traffic does not look like this, which
// is why the benchmark above is the one to watch, but a change that makes this
// path expensive should be visible rather than reasoned about.
func BenchmarkReservedInUpdateWithThePrefix(b *testing.B) {
	raw, err := bson.Marshal(bson.D{
		{Key: "$set", Value: bson.D{{Key: "_tcatalog", Value: "shipped"}, {Key: "shippedAt", Value: time.Now()}}},
		{Key: "$inc", Value: bson.D{{Key: "version", Value: 1}}},
	})
	if err != nil {
		b.Fatal(err)
	}
	u := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: raw}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, bad := reservedInUpdate(u); bad {
			b.Fatal("this update carries no reserved field")
		}
	}
}
