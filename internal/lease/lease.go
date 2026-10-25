// Package lease stops two of the same daemon running against one deployment.
//
// The feeder is the one that made this necessary. Two feeders with the same id
// watch the same namespaces and both see every delete, and an eviction is
// announced once: the first to reach it takes the announcement down, the second
// finds nothing and reads a real eviction as the application deleting the
// document. Its policy for that is to tombstone the archive's copy, which is
// the default, so the document is gone from both tiers with nothing said
// anywhere. Nobody has to do anything exotic to arrange it. Starting the daemon
// on a second host for redundancy does it, and so does a unit file that starts
// one before the old one has exited.
//
// The test harness has known this since it was written: mkworker.sh isolates
// every worker's namespaces "so no two feeders ever watch the same namespace",
// and says why in the same comment. The product said nothing and did nothing.
//
// A second evictor is milder and still wrong. Two of them announce the same
// eviction twice and only one delete follows, so a count is left standing, and
// the next delete of that _id -- the application's own, whenever it comes -- is
// read as the eviction that never happened. The archive keeps a live copy of a
// document its owner deleted.
//
// So: one row per daemon kind and id, owned by one process, renewed while it
// runs and released when it stops. A daemon that cannot take the lease refuses
// to start and names who holds it. A daemon that loses it while running stops,
// because carrying on is the thing this exists to prevent.
//
// Every instant here is the archive's own, taken as the row is written. The
// process that judges whether a lease is stale is never the process that wrote
// it, which is the whole point, so a clock comparison between the two would be
// exactly the mistake this guard is made of. See internal/health's note.
package lease

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// Stale is how long a lease outlives its holder.
//
// The trade is between two bad minutes. Short, and an ordinary pause -- a long
// checkpoint write, a stalled archive, a process the scheduler forgot about --
// lets a second daemon in beside the first, which is what this exists to stop.
// Long, and a host that died takes mirroring down for that long, because the
// replacement waits the lease out.
//
// Thirty seconds, renewed every five. Six missed renewals is well past any
// pause this has been measured to take, and a feeder that is down for half a
// minute has already cost more than the wait.
const Stale = 30 * time.Second

// Renew is how often the holder refreshes it.
const Renew = 5 * time.Second

// ErrHeld says another process holds the lease. It carries who and when, which
// is the whole of what an operator needs to act.
type ErrHeld struct {
	Kind  string
	ID    string
	Owner string
	Host  string
	PID   int
	Ago   time.Duration
}

func (e *ErrHeld) Error() string {
	// "last seen -1s ago" is what this printed once, on a box whose clock had
	// stepped backwards between the holder's renewal and this reading. A
	// negative age reads as a defect in the thing reporting it, which is the
	// wrong thing for an operator to be wondering about while two daemons are
	// fighting over one deployment. Both instants are the archive's own, so
	// nothing here can correct it; what it can do is say the true thing
	// plainly. The clock warnings at startup are where a moving clock belongs.
	seen := "just now"
	if e.Ago > 0 {
		seen = e.Ago.Round(time.Second).String() + " ago"
	}
	return fmt.Sprintf("another %s with id %q is running: %s (pid %d), last seen %s. "+
		"Two of them against one deployment is not redundancy: stop that one, or wait %s for its "+
		"lease to expire if the host is gone",
		e.Kind, e.ID, e.Host, e.PID, seen, Stale)
}

// ErrLost says the lease was taken by somebody else while this process held it.
var ErrLost = errors.New("another process took this daemon's lease")

// Lease is one daemon's claim on one id.
type Lease struct {
	coll  *mongo.Collection
	kind  string
	id    string
	owner string
	host  string
	pid   int
}

// New returns a lease for a daemon kind ("feeder", "evictor") and its
// configured id. The owner token is this process's alone: two processes on one
// host with the same pid cannot happen, and a restart gets a new one.
func New(cold *mongo.Client, kind, id string) *Lease {
	host, _ := os.Hostname()
	return &Lease{
		coll:  cold.Database(meta.DB).Collection(meta.Markers),
		kind:  kind,
		id:    id,
		owner: bson.NewObjectID().Hex(),
		host:  host,
		pid:   os.Getpid(),
	}
}

// docID is the row this lease lives in.
func (l *Lease) docID() string { return "lease:" + l.kind + ":" + l.id }

// record is what the row holds.
type record struct {
	ID    string    `bson:"_id"`
	Owner string    `bson:"owner"`
	Host  string    `bson:"host"`
	PID   int       `bson:"pid"`
	Seen  time.Time `bson:"seen"`
	Since time.Time `bson:"since"`
	// Now is the archive's clock at the moment the row was read back, so
	// staleness is worked out in the frame the row was written in.
	Now time.Time `bson:"now"`
}

// Take claims the lease, or reports who holds it.
//
// One update, so that two daemons starting together cannot both win. The
// condition is written into the pipeline rather than into a filter because a
// filter that does not match would upsert a second row: the document is found
// by _id and the pipeline decides whether this process gets to write its name
// into it.
func (l *Lease) Take(ctx context.Context) error {
	err := l.take(ctx)
	if mongo.IsDuplicateKeyError(err) {
		// Two processes upserting a row that does not exist yet: one insert
		// wins and the other is told the key is taken, which is this lease
		// being created rather than anything being wrong. The retry finds the
		// row and decides against it properly.
		return l.take(ctx)
	}
	return err
}

func (l *Lease) take(ctx context.Context) error {
	mine := l.takeIfFree()
	res := l.coll.FindOneAndUpdate(ctx,
		bson.D{{Key: "_id", Value: l.docID()}},
		mongo.Pipeline{
			bson.D{{Key: "$set", Value: bson.D{
				{Key: "owner", Value: bson.D{{Key: "$cond", Value: bson.A{mine, l.owner, "$owner"}}}},
				{Key: "host", Value: bson.D{{Key: "$cond", Value: bson.A{mine, l.host, "$host"}}}},
				{Key: "pid", Value: bson.D{{Key: "$cond", Value: bson.A{mine, l.pid, "$pid"}}}},
				{Key: "since", Value: bson.D{{Key: "$cond", Value: bson.A{mine, "$$NOW", "$since"}}}},
				{Key: "seen", Value: bson.D{{Key: "$cond", Value: bson.A{mine, "$$NOW", "$seen"}}}},
				// Read back with the row, so whoever is judging it is not
				// reading its own clock. See the package note.
				{Key: "now", Value: "$$NOW"},
			}}},
		},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After))

	var rec record
	if err := res.Decode(&rec); err != nil {
		return fmt.Errorf("take the %s lease for %q: %w", l.kind, l.id, err)
	}
	if rec.Owner == l.owner {
		return nil
	}
	held := &ErrHeld{
		Kind: l.kind, ID: l.id, Owner: rec.Owner, Host: rec.Host, PID: rec.PID,
		Ago: rec.Now.Sub(rec.Seen),
	}
	// A holder on this machine that no longer exists is not a holder. Waiting
	// the window out for it is pure cost: the process is gone, this host can
	// see that it is gone, and nothing it wrote is going to change.
	//
	// It is the ordinary way a lease is left behind. kill -9, an OOM kill, a
	// crash: none of them release, and all of them are followed by somebody
	// starting the daemon again straight away. Measured on the test suites,
	// which end with pkill -9 and start the next feeder a second later: two
	// suites failed because a lease renewed a moment before the kill still had
	// most of its thirty seconds to run.
	if l.canSteal(held) {
		took, err := l.steal(ctx, held.Owner)
		if err != nil {
			return err
		}
		if took {
			return nil
		}
	}
	return held
}

// canSteal reports whether the holder can be shown to be gone rather than
// merely quiet. Only ever true for a holder on this host: another machine's
// process cannot be looked up from here, and guessing is what the staleness
// window is for.
func (l *Lease) canSteal(held *ErrHeld) bool {
	return held.Host == l.host && held.PID > 0 && !processAlive(held.PID)
}

// steal takes a lease from a named owner, and reports whether it got it.
//
// Conditioned on that owner still holding it, so two processes both finding the
// same corpse cannot both conclude they won: the second one's filter matches
// nothing and it is told the lease is held, which by then it is.
func (l *Lease) steal(ctx context.Context, from string) (bool, error) {
	res, err := l.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: l.docID()}, {Key: "owner", Value: from}},
		mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{
			{Key: "owner", Value: bson.D{{Key: "$literal", Value: l.owner}}},
			{Key: "host", Value: bson.D{{Key: "$literal", Value: l.host}}},
			{Key: "pid", Value: l.pid},
			{Key: "since", Value: "$$NOW"},
			{Key: "seen", Value: "$$NOW"},
		}}}})
	if err != nil {
		return false, fmt.Errorf("take the %s lease for %q from a process that has gone: %w",
			l.kind, l.id, err)
	}
	return res.MatchedCount > 0, nil
}

// processAlive reports whether a pid on this host still belongs to a process
// running this same program.
//
// The program's name is checked and not merely the pid, because pids are reused
// and the one a lease remembers may belong to something else entirely by now.
// Comparing against this process's own name is what makes that check possible
// without storing anything extra: the holder of a lease runs the same binary as
// whoever is asking about it, an upgrade included, since a release does not
// rename cline.
//
// Being wrong in the safe direction means deciding a process is alive when it is
// not, which costs the wait this exists to avoid. The unsafe direction, calling
// a running daemon dead and taking its lease, needs a pid whose program has a
// different name, and a pid that still exists under this program's name is this
// program.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil {
		// argv is NUL separated; the first entry is the program.
		argv0, _, _ := strings.Cut(string(data), "\x00")
		if argv0 == "" {
			return true
		}
		return filepath.Base(argv0) == filepath.Base(os.Args[0])
	}
	// No /proc, so the question is only whether the pid exists at all. EPERM
	// means it does and belongs to somebody else, which is still alive.
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, os.ErrPermission)
}

// takeIfFree is the condition under which this process may write its name in:
// nobody holds it, this process already does, or whoever does has not been seen
// for Stale.
func (l *Lease) takeIfFree() bson.D {
	return bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$owner"}}, "missing"}}},
		bson.D{{Key: "$eq", Value: bson.A{"$owner", l.owner}}},
		bson.D{{Key: "$lt", Value: bson.A{"$seen",
			bson.D{{Key: "$subtract", Value: bson.A{"$$NOW", Stale.Milliseconds()}}}}}},
	}}}
}

// Keep renews the lease and reports whether this process still holds it.
//
// Cheap enough to call often and paced here rather than by every caller, so a
// daemon can put it in whatever loop it already has.
func (l *Lease) Keep(ctx context.Context) error {
	res := l.coll.FindOneAndUpdate(ctx,
		bson.D{{Key: "_id", Value: l.docID()}},
		mongo.Pipeline{
			bson.D{{Key: "$set", Value: bson.D{
				{Key: "seen", Value: bson.D{{Key: "$cond", Value: bson.A{
					bson.D{{Key: "$eq", Value: bson.A{"$owner", l.owner}}}, "$$NOW", "$seen"}}}},
			}}},
		},
		options.FindOneAndUpdate().SetReturnDocument(options.After))

	var rec record
	err := res.Decode(&rec)
	if errors.Is(err, mongo.ErrNoDocuments) {
		// Somebody deleted the row. Claiming it again is right: there is no
		// other holder to displace.
		return l.Take(ctx)
	}
	if err != nil {
		return fmt.Errorf("renew the %s lease for %q: %w", l.kind, l.id, err)
	}
	if rec.Owner != l.owner {
		return fmt.Errorf("%w: %s (pid %d) holds the %s lease for %q",
			ErrLost, rec.Host, rec.PID, l.kind, l.id)
	}
	return nil
}

// Fence marks the lease row inside the caller's transaction, and reports
// ErrLost when this process no longer holds it.
//
// A holder paused longer than Stale is replaced, and a write it had in flight
// lands after its successor's: a replacement over a tombstone the successor
// wrote leaves a deleted document live. Written in the same archive
// transaction as the writes it guards, the mark makes them conditional on the
// lease: a row that has another owner matches nothing and the transaction is
// abandoned, and a successor taking the row while it runs writes the same
// document, so one of the two transactions fails to commit.
func (l *Lease) Fence(ctx context.Context) error {
	res, err := l.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: l.docID()}, {Key: "owner", Value: l.owner}},
		bson.D{{Key: "$currentDate", Value: bson.D{{Key: "fenced", Value: true}}}})
	if err != nil {
		return fmt.Errorf("fence the %s lease for %q: %w", l.kind, l.id, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("%w: the %s lease for %q has another owner", ErrLost, l.kind, l.id)
	}
	return nil
}

// Release gives the lease up, so a restart does not have to wait it out.
//
// Best effort by design: a daemon that is killed never gets here, and the
// staleness window is what covers that. Only this process's own claim is
// released, because a row whose owner has changed belongs to somebody else.
func (l *Lease) Release(ctx context.Context) error {
	_, err := l.coll.DeleteOne(ctx, bson.D{
		{Key: "_id", Value: l.docID()},
		{Key: "owner", Value: l.owner},
	})
	return err
}
