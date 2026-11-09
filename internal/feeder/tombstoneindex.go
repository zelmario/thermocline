package feeder

import (
	"context"
	"strings"

	"github.com/zelmario/thermocline/internal/meta"
)

// indexTombstonesAtStart asks for meta.TombstoneIndex on every tiered archive
// collection, and returns at once: the builds run behind mirroring.
//
// The index is what lets a count with no predicate stay a metadata read on the
// archive (meta.LiveCountEstimate). It holds only deleted copies, but building
// it reads the whole collection once, which on a large archive is minutes, and
// mirroring is not held up for that. Until a build finishes the router answers
// such a count from the archive's metadata alone and says so.
//
// With feeder.namespaces empty there is no list, and each collection is asked
// for the first time a deletion is mirrored into it instead (write.go).
func (f *Feeder) indexTombstonesAtStart(ctx context.Context) {
	for _, ns := range f.cfg.Namespaces {
		f.indexTombstones(ctx, ns)
	}
}

// indexTombstones starts the build for one namespace, once per run.
//
// Not on a collection the archive does not hold: createIndexes would create it
// bare, without the options the fast tier's copy carries (see
// settleTombstoneTTL). It is asked again at the next deletion mirrored there.
func (f *Feeder) indexTombstones(ctx context.Context, ns string) {
	if _, asked := f.tombIndexed.LoadOrStore(ns, true); asked {
		return
	}
	db, coll := splitNS(ns)
	if db == "" || coll == "" || strings.HasPrefix(coll, "system.") {
		return
	}
	go func() {
		if exists, err := f.archiveHas(ctx, db, coll); err != nil || !exists {
			f.tombIndexed.Delete(ns)
			return
		}
		c := f.cold.Database(db).Collection(coll)
		// An existing index of this name answers at once, so a restart costs
		// one round trip per namespace.
		if _, err := c.Indexes().CreateOne(ctx, meta.TombstoneIndexModel()); err != nil {
			if ctx.Err() == nil {
				f.log.Warn("could not build the index that counts deleted copies on the archive",
					"ns", ns, "index", meta.TombstoneIndex, "err", err,
					"effect", "a count with no predicate, which is what estimatedDocumentCount sends, "+
						"is answered from the archive's metadata and includes deleted documents")
				f.tombIndexed.Delete(ns)
			}
			return
		}
		f.log.Debug("deleted copies are indexed on the archive", "ns", ns, "index", meta.TombstoneIndex)
	}()
}
