package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// A checkpoint answers two questions that have nothing to do with each other,
// and answering both with one write made one of them expensive.
//
// The first is durability: where does the feeder resume after a restart? That
// costs re-mirrored work when it is stale, mirroring is idempotent, and so
// saving it every few seconds is exactly right.
//
// The second is freshness: how far has mirroring got, right now? A read that
// falls through to the archive waits on this before it answers, because the
// archive trails the fast tier and returning a document the caller already
// deleted is worse than being slow.
//
// Tying the second to the first made every fallback read wait for the next
// checkpoint, whether or not the feeder had anything to do. Measured on an idle
// database with the feeder perfectly caught up, a fallback read took 1.14 s
// with checkpoint_every at 1s and 4.53 s at the shipped default of 5s, while
// the same read with the wait disabled took 1.8 ms. The wait was not waiting
// for mirroring. It was waiting for the next checkpoint write.
//
// Worse, at the default the wait exceeded archive_wait, which also defaults to
// 5s, so reads gave up and answered stale anyway: slow and stale, the two
// things the barrier exists to avoid.
//
// So the position gets its own record and its own cadence. It carries no resume
// token, just an instant, which makes it small enough to write many times a
// second without anyone noticing.

// PositionRecord is how far mirroring has got.
type PositionRecord struct {
	ID string `bson:"_id"`
	// At is the wall-clock instant through which every event has been applied
	// to the archive. It is written after the batch lands, never before.
	//
	// It is kept for a reader that has nothing better, and it is the weaker of
	// the two. SkewMS below is what makes it comparable at all: without it the
	// reader is subtracting the feeder's clock from its own, and on separate
	// hosts the barrier is then only as sound as the difference between two
	// machines nothing keeps in step.
	//
	// Even with it, this is the weaker bar, and here is exactly how. The feeder
	// takes this instant from two places: its own clock when a change stream
	// poll comes back empty, which SkewMS puts in the archive's frame exactly,
	// and an event's cluster time when one arrives, which is the fast tier
	// primary's clock rounded down to the second and which SkewMS is the wrong
	// correction for. So a fast tier running ahead of the archive can still
	// publish a position slightly past what has been mirrored. That is bounded
	// by TS below, which is exact and which a fall-through read has to clear as
	// well as this one, and it is why neither bar is allowed to pass alone.
	At      time.Time `bson:"at"`
	SavedAt time.Time `bson:"savedAt"`
	// TS is the same position as a cluster time, and is the one to compare
	// against when it is there.
	//
	// A cluster time is the replica set's own logical clock, so the feeder and
	// the router read it from the same place and no clock of their own comes
	// into it. It is exact where the wall-clock reading is a claim: it comes
	// either from an event the feeder applied or from the resume token of an
	// empty poll, which is MongoDB saying "nothing exists up to here".
	//
	// Absent on a record written by a feeder from before this, which is the
	// only reason At survives.
	TS bson.Timestamp `bson:"ts,omitempty"`
	// EveryMS is the cadence the publisher promised, so a reader can judge
	// whether silence means "behind" or "gone" without being told how this
	// feeder is configured.
	EveryMS int64 `bson:"everyMS,omitempty"`
	// SkewMS is how far the publisher's clock is ahead of the archive's, in
	// milliseconds, as the publisher last measured it.
	//
	// It is what makes At usable across hosts. At is the feeder's clock and
	// whoever compares against it is reading their own, so the comparison is
	// only as sound as the skew between two machines nothing keeps in step.
	// Both sides do talk to the archive, though, so both can say where they
	// stand against that one clock, and a reader that knows its own offset can
	// put the two instants in the same frame before comparing them.
	//
	// Absent on a record written by a feeder from before this, and on one that
	// has not managed to read the archive's clock yet. A reader without it
	// compares raw instants, which is what everything did before. It is a
	// pointer for that distinction alone: a feeder that measured no skew is
	// saying something, and zero has to be tellable from silence.
	SkewMS *int64 `bson:"skewMS,omitempty"`
}

// PackTS makes a cluster time comparable as a single number.
//
// A BSON timestamp orders by seconds first and by increment second, which is
// exactly what the two halves packed into a uint64 do. Comparing the seconds
// alone would treat every event inside one second as simultaneous, and that
// rounding is what once made the published position appear to move backwards.
func PackTS(ts bson.Timestamp) uint64 { return uint64(ts.T)<<32 | uint64(ts.I) }

// UnpackTS is PackTS backwards, for logging a value that was kept packed.
func UnpackTS(v uint64) bson.Timestamp {
	return bson.Timestamp{T: uint32(v >> 32), I: uint32(v)}
}

// Positions reads and writes mirroring positions.
type Positions struct {
	coll *mongo.Collection
}

// NewPositions returns a position store backed by the cold tier.
func NewPositions(cold *mongo.Client) *Positions {
	return &Positions{coll: cold.Database(meta.DB).Collection(meta.PositionsColl)}
}

// Save publishes how far mirroring has got.
//
// The write concern is deliberately w:1. This is a hint that makes reads fast,
// not a durability record: if it is lost the feeder republishes it within a
// tick, and the durable checkpoint is what recovery actually uses. Paying for
// majority acknowledgement several times a second to speed up reads would be a
// poor trade.
func (p *Positions) Save(ctx context.Context, id string, at time.Time, ts bson.Timestamp, every, skew time.Duration, skewKnown bool) error {
	if at.IsZero() && ts.T == 0 {
		return nil
	}
	set := bson.D{
		{Key: "savedAt", Value: time.Now().UTC()},
		{Key: "everyMS", Value: every.Milliseconds()},
	}
	if skewKnown {
		// Rounded to milliseconds, which is well inside the error of measuring
		// it over a round trip, and zero is written as zero rather than left
		// out: a feeder that measured no skew is saying something, and dropping
		// the field would make it indistinguishable from one that never looked.
		set = append(set, bson.E{Key: "skewMS", Value: skew.Milliseconds()})
	}
	if !at.IsZero() {
		set = append(set, bson.E{Key: "at", Value: at})
	}
	if ts.T != 0 {
		set = append(set, bson.E{Key: "ts", Value: ts})
	}
	_, err := p.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: set}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save position %q: %w", id, err)
	}
	return nil
}

// At returns how far mirroring has got, and whether a position exists.
//
// A missing position is not an error. A feeder from before this existed, or one
// with positions turned off, publishes nothing here and the caller falls back
// to the checkpoint, which is where this information used to live.
func (p *Positions) At(ctx context.Context, id string) (PositionRecord, bool, error) {
	var rec PositionRecord
	err := p.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&rec)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return PositionRecord{}, false, nil
	}
	if err != nil {
		return PositionRecord{}, false, fmt.Errorf("read position %q: %w", id, err)
	}
	return rec, !rec.At.IsZero() || rec.TS.T != 0, nil
}

// Liveness decides whether the feeder is still publishing, by watching the
// record move rather than by comparing two machines' clocks.
//
// What this replaced read savedAt, which the feeder stamps with its own clock,
// and compared it against the reader's. That is the mistake this package already
// fixed once for the position itself: markCaughtUp's comment in the feeder says
// it plainly, a wall-clock instant "published for a router to compare against
// its own" is only as sound as the skew between two hosts, and the cluster time
// replaced it. The liveness test beside it kept the habit.
//
// It matters more than it looks, because of which way it errs. A router whose
// clock is *ahead* of the feeder's by more than the staleness window reads a
// perfectly healthy feeder as stopped, and a feeder read as stopped turns the
// freshness barrier off: every read that falls through to the archive answers
// at once without waiting for mirroring, counts a stale read and warns about a
// feeder that is doing its job. With position_every at its default of 20ms the
// window is its two second floor, so three seconds between two hosts is enough,
// and three seconds is ordinary on machines nobody is keeping in step.
//
// Movement needs one clock, not two. A record that differs from the one seen
// before is a publication that happened between the two readings, whatever
// either machine thinks the time is. So: it moved, it is alive; it has not
// moved for a whole staleness window of this reader's own elapsed time, it is
// stopped; and with no history yet, the wall clock is allowed to decide only
// when it is stale beyond any plausible skew.
type Liveness struct {
	mu    sync.Mutex
	seen  posFingerprint
	since time.Time // local instant that reading was first seen
	have  bool
}

// posFingerprint is everything about a record that changes when the feeder
// publishes. savedAt alone would do on a working feeder, since Save sets it
// every time, but a feeder whose clock is stopped still moves the position, and
// a position that moves is the feeder being alive.
type posFingerprint struct {
	savedAt int64
	at      int64
	ts      uint64
}

func fingerprint(rec PositionRecord) posFingerprint {
	return posFingerprint{
		savedAt: rec.SavedAt.UnixNano(),
		at:      rec.At.UnixNano(),
		ts:      PackTS(rec.TS),
	}
}

// beyondSkew is how stale a record has to look, on first sight, before this
// believes the wall clock over the absence of evidence.
//
// It exists so that a router started while the feeder has been down for hours
// does not make the first read that falls through wait the whole barrier to
// learn what is obvious. Five minutes is well past what two hosts drift apart
// by, and well short of how long a stopped feeder stays stopped.
const beyondSkew = 5 * time.Minute

// Observe records a reading and reports whether the feeder still looks alive.
//
// mine is how far the caller's own clock is ahead of the archive's, when it
// knows. With that and the offset the feeder publishes, the record's age is
// exact whatever either machine's clock says, and no watching is needed: both
// instants go into the archive's frame and are subtracted there. That is the
// normal path, and it is why a feeder that stopped a moment ago is known to
// have stopped on the first look rather than after a window of silence.
//
// Movement is what is left when either offset is missing, which is a feeder too
// old to publish one or a reader that has not managed a reading of its own yet.
// It also stands above the arithmetic: a record that moved between two readings
// is a publication that happened between them, whatever any clock says, so a
// feeder whose clock has stopped while it goes on mirroring is alive and is
// reported alive.
//
// now is the caller's own clock, used for elapsed time between its own readings
// and, on first sight with no offsets, for the one case below.
func (l *Liveness) Observe(rec PositionRecord, now time.Time, mine time.Duration, mineKnown bool) bool {
	alive, _ := l.ObserveQuiet(rec, now, mine, mineKnown)
	return alive
}

// ObserveQuiet is Observe, and also says how long the record has gone without
// a publication, as well as this reader can tell.
//
// Stopped is a judgement made after two seconds, and it is the right one for a
// caller deciding whether to trust the position at all. It is not the whole
// question for a read that is prepared to wait: a feeder that has published
// nothing for less than that read's own wait is behind rather than gone, and a
// read giving up on it answers from an archive that may not have the write it
// was asked about. quiet is what lets a caller tell the two apart.
func (l *Liveness) ObserveQuiet(rec PositionRecord, now time.Time, mine time.Duration, mineKnown bool) (alive bool, quiet time.Duration) {
	fp := fingerprint(rec)

	l.mu.Lock()
	defer l.mu.Unlock()

	moved := l.have && fp != l.seen
	first := !l.have
	if moved || first {
		l.seen, l.since, l.have = fp, now, true
	}
	if moved {
		return true, 0
	}

	if theirs, ok := rec.Skew(); ok && mineKnown {
		age := now.Add(-mine).Sub(rec.SavedAt.Add(-theirs))
		return age <= staleAfter(rec.EveryMS), age
	}

	if first {
		// Nothing has been watched yet and the clocks cannot be reconciled, so
		// the only evidence is the feeder's own stamp. It is believed only when
		// it is stale beyond anything skew explains, which keeps a router
		// started while the feeder has been down for hours from making the
		// first read that falls through wait the whole barrier to learn it.
		age := now.Sub(rec.SavedAt)
		return age <= beyondSkew, age
	}

	// It has not moved, and there is no way to date it. That is only evidence
	// once it has been watched for as long as a publication would have taken.
	return now.Sub(l.since) <= staleAfter(rec.EveryMS), now.Sub(l.since)
}

// staleAfter is how long without a publication means stopped. Four missed
// publications is the line; the record carries the cadence so this does not
// have to guess it, and the floor keeps a fast cadence from calling a feeder
// dead over one slow write.
func staleAfter(everyMS int64) time.Duration {
	every := time.Duration(everyMS) * time.Millisecond
	if every <= 0 {
		every = time.Second
	}
	if stale := 4 * every; stale > 2*time.Second {
		return stale
	}
	return 2 * time.Second
}

// Skew is how far the publisher's clock was ahead of the archive's when it last
// looked, and whether it ever managed to look.
func (rec PositionRecord) Skew() (time.Duration, bool) {
	if rec.SkewMS == nil {
		return 0, false
	}
	return time.Duration(*rec.SkewMS) * time.Millisecond, true
}
