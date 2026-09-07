package feeder

import (
	"context"
	"regexp"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
)

// A residency record says a document is on the fast tier and should stay there.
// Everything that takes a document off the fast tier removes its record: the
// evictor does it as it evicts, on the stated grounds that residency is
// meaningless once the document has left, and dropResidency does it when the
// application deletes one.
//
// A drop takes every document off at once and nothing removed the records. The
// rename path already did, for the same reason and with the same comment.
//
// What a leftover record costs is not untidiness. The router reads these records
// to tell its own copy-ups from an application's inserts, and the timestamp on
// one is written when the record is created, because a record exists for exactly
// as long as one promotion. A record that outlives its collection breaks that:
// the next copy-up of that _id finds the record already there, keeps a timestamp
// from minutes ago, and the promotion it describes then arrives on a client's
// change stream as an ordinary insert. Measured by the differential harness,
// whose own reseed drops the collection on the fast tier directly.

// clearResidency removes the holds for one namespace.
//
// Best effort, and logged rather than returned. The records carry a TTL, so
// failing here corrects itself; stopping mirroring over it would cost every
// namespace behind this one.
func (f *Feeder) clearResidency(ctx context.Context, ns string) {
	if f.cold == nil {
		// A Feeder assembled without an archive connection, which only happens
		// in tests that exercise event classification on its own. The same
		// guard withdrawCompleteness has, beside which this is called.
		return
	}
	res, err := f.cold.Database(meta.DB).Collection(meta.Residency).
		DeleteMany(ctx, bson.D{{Key: "ns", Value: ns}})
	if err != nil {
		f.log.Warn("could not clear the residency holds for a dropped collection",
			"ns", ns, "err", err,
			"effect", "a document promoted into this name again may have its promotion "+
				"delivered to a change stream as an ordinary insert",
			"note", "the holds expire on their own TTL, so this corrects itself")
		return
	}
	if res.DeletedCount > 0 {
		f.log.Info("cleared the residency holds for a dropped collection",
			"ns", ns, "holds", res.DeletedCount)
	}
}

// clearResidencyIn does the same one level up, for a database that was dropped.
//
// dropDatabase names no collection, so the per-collection path above never fires
// for it, which is the same gap the completeness claim had beside it.
func (f *Feeder) clearResidencyIn(ctx context.Context, db string) {
	if f.cold == nil {
		return
	}
	res, err := f.cold.Database(meta.DB).Collection(meta.Residency).
		DeleteMany(ctx, bson.D{{Key: "ns", Value: bson.D{
			{Key: "$regex", Value: "^" + regexp.QuoteMeta(db+".")},
		}}})
	if err != nil {
		f.log.Warn("could not clear the residency holds for a dropped database",
			"db", db, "err", err)
		return
	}
	if res.DeletedCount > 0 {
		f.log.Info("cleared the residency holds for a dropped database",
			"db", db, "holds", res.DeletedCount)
	}
}
