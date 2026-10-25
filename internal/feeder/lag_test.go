package feeder

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Lag is measured from the last applied event, so on a database that goes quiet
// it climbs forever while the archive is in fact perfectly up to date. The
// evictor reads that number, fails closed, and stops evicting for good, which
// looks exactly like a hang. An empty change stream poll means everything has
// been applied, so it has to reset the measurement.
func TestIdleFeederReportsNoLag(t *testing.T) {
	f := testFeeder()

	f.lastEventAt = time.Now().Add(-90 * time.Minute)
	if f.Lag() < time.Hour {
		t.Fatalf("setup: wanted a large lag to start from, got %v", f.Lag())
	}

	f.markCaughtUp(nil)
	if got := f.Lag(); got > time.Second {
		t.Errorf("Lag after catching up = %v, want about zero: a quiet database must not look like a feeder hours behind", got)
	}
}

// A feeder that has never seen an event reports no lag rather than a lag
// measured from the zero time, which would be thousands of years.
func TestFreshFeederReportsNoLag(t *testing.T) {
	if got := testFeeder().Lag(); got != 0 {
		t.Errorf("Lag before any event = %v, want 0", got)
	}
}

// Catching up must publish a cluster time taken from the poll's own resume
// token, not one invented locally. A feeder that stamped its own clock claimed
// a moment it had not proved, and published it for a router on another host to
// compare against a different clock.
func TestCatchingUpTakesItsClusterTimeFromTheToken(t *testing.T) {
	f := testFeeder()

	f.markCaughtUp(tokenRaw(t, "826A85B3FE000000042B0429296E1404"))
	if f.lastEventTS.T != 1787147262 || f.lastEventTS.I != 4 {
		t.Fatalf("cluster time after catching up = %v, want the token's T=1787147262 I=4", f.lastEventTS)
	}

	// A token this cannot read leaves the claim where it was rather than
	// replacing it with a guess.
	f.markCaughtUp(tokenRaw(t, "not hex at all"))
	if f.lastEventTS.T != 1787147262 || f.lastEventTS.I != 4 {
		t.Errorf("an unreadable token moved the cluster time to %v", f.lastEventTS)
	}
}

// Mirroring reaching a moment is a claim that can only be strengthened. The
// resume token of an empty poll is taken before the batch that follows it is
// applied, and the two are published on different cadences, so an older value
// arriving later must not undo a newer one.
func TestClusterTimeNeverGoesBackwards(t *testing.T) {
	f := testFeeder()
	f.noteEventTS(bson.Timestamp{T: 100, I: 5})

	f.noteEventTS(bson.Timestamp{T: 100, I: 4})
	f.noteEventTS(bson.Timestamp{T: 99, I: 9})
	if f.lastEventTS.T != 100 || f.lastEventTS.I != 5 {
		t.Fatalf("an older cluster time moved the position back to %v", f.lastEventTS)
	}

	f.noteEventTS(bson.Timestamp{T: 100, I: 6})
	if f.lastEventTS.I != 6 {
		t.Errorf("a newer increment in the same second did not advance it: %v", f.lastEventTS)
	}
}

// A stream that is never quiet still has its buffer written, and so its
// position published, within a bound of time rather than only once a batch is
// full. Without the bound the position stood still for as long as a batch took
// to fill, which at a trickle was over three seconds, and a router reads two
// seconds without a publication as a feeder that has stopped.
func TestABusyStreamFlushesWithoutFillingABatch(t *testing.T) {
	f := testFeeder()
	f.cfg.BatchSize = 500
	now := time.Now()

	if f.dueToFlush(now) {
		t.Fatal("an empty buffer is never due")
	}

	f.pending = append(f.pending, pendingOp{})
	f.pendingSince = now
	if f.dueToFlush(now.Add(busyFlushEvery / 2)) {
		t.Error("one event, buffered for half the bound, was flushed; a busy feeder would write a batch per event")
	}
	if !f.dueToFlush(now.Add(busyFlushEvery)) {
		t.Error("one event buffered for the whole bound was not flushed; the position stands still until 500 arrive")
	}

	for len(f.pending) < f.cfg.BatchSize {
		f.pending = append(f.pending, pendingOp{})
	}
	if !f.dueToFlush(now) {
		t.Error("a full batch was not flushed at once")
	}
}

// A flush is one transaction on the archive, so it is bounded in bytes as well
// as in count and time. 500 documents could otherwise be gigabytes held in the
// archive's cache until the commit.
func TestAFlushIsBoundedInBytes(t *testing.T) {
	f := testFeeder()
	f.cfg.BatchSize = 500
	now := time.Now()
	f.pendingSince = now
	f.pending = append(f.pending, pendingOp{doc: make(bson.Raw, 8<<20)})
	if f.dueToFlush(now) {
		t.Fatal("8MB was flushed before the byte bound")
	}
	f.pending = append(f.pending, pendingOp{doc: make(bson.Raw, 8<<20)})
	if !f.dueToFlush(now) {
		t.Error("16MB waited for more; one transaction would grow past what the archive's cache can hold")
	}
}

// An event proves mirroring up to just before its own cluster time, because
// every operation of a transaction shares the commit's. Claiming the event's
// own time let a router read a transaction half mirrored: 499 of 600 events,
// five rounds of five.
func TestAnEventProvesOnlyWhatCameBeforeIt(t *testing.T) {
	f := testFeeder()
	raw := event(t, "insert", "shop", "orders", bson.D{
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: 1}}},
	})
	doc := bson.D{}
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for i := range doc {
		if doc[i].Key == "clusterTime" {
			doc[i].Value = bson.Timestamp{T: 100, I: 5}
		}
	}
	raw, _ = bson.Marshal(doc)
	if err := f.handle(context.Background(), raw); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if f.lastEventTS != (bson.Timestamp{T: 100, I: 4}) {
		t.Errorf("after an event at {100 5} the position claims %v, want {100 4}: the rest of a transaction may share its time", f.lastEventTS)
	}
	if justBefore(bson.Timestamp{T: 100, I: 0}) != (bson.Timestamp{T: 99, I: ^uint32(0)}) {
		t.Error("the time before the first increment of a second is the last increment of the one before")
	}
}

// A batch can end partway through one commit, and its token then carries the
// commit's shared time. Claiming that time said the whole commit was in the
// archive. The tokens are the two events of a two-insert transaction, as an
// 8.0 primary returned them: the same time, txnOpIndex 0 and 1.
func TestBatchEndInsideACommitClaimsNothing(t *testing.T) {
	const second = "826ADDEAD1000000012B042C01002B026E5A1004"
	ct := bson.Timestamp{T: 0x6ADDEAD1, I: 1}
	txnEvent := func(lsid bool) bson.Raw {
		doc := bson.D{
			{Key: "_id", Value: bson.D{{Key: "_data", Value: second}}},
			{Key: "operationType", Value: "insert"},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "app"}, {Key: "coll", Value: "orders"}}},
			{Key: "documentKey", Value: bson.D{{Key: "_id", Value: 1}}},
			{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: 1}}},
			{Key: "clusterTime", Value: ct},
		}
		if lsid {
			doc = append(doc, bson.E{Key: "lsid", Value: bson.D{{Key: "id", Value: "s"}}})
		}
		raw, err := bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	for _, tc := range []struct {
		name   string
		events []bson.Raw
		want   bool
	}{
		{"a transaction's event", []bson.Raw{txnEvent(true)}, false},
		{"two events at one time", []bson.Raw{txnEvent(false), txnEvent(false)}, false},
		{"one event on its own", []bson.Raw{txnEvent(false)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := testFeeder()
			f.cfg.Namespaces = []string{"app.orders"}
			for _, ev := range tc.events {
				if err := f.handle(context.Background(), ev); err != nil {
					t.Fatalf("handle: %v", err)
				}
			}
			f.markBatchEnd(context.Background(), tokenRaw(t, second))
			claimed := f.lastEventTS == ct
			if claimed != tc.want {
				t.Errorf("position after the batch = %v, claimed the commit's time: %v, want %v",
					f.lastEventTS, claimed, tc.want)
			}
		})
	}
}
