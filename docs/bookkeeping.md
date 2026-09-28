# Bookkeeping

Part of [Thermocline](../README.md).

## Bookkeeping

Thermocline never modifies user documents to carry its own state. Everything
lives in the `_thermocline` database on the cold tier, whose name is
configurable as `meta.db`:

| Collection | Purpose |
|---|---|
| `checkpoints` | One change stream resume token per feeder, for recovery |
| `positions` | How far each feeder has mirrored, published far more often than the checkpoint because fallback reads wait on it |
| `evicting` | Eviction announcements, kept for an hour on a TTL index, not removed when applied |
| `residency` | How long a document should stay in the fast tier, and the record that this system put it there. Kept for an hour past the hold on a TTL index |
| `warming` | Warming announcements, expiring on a TTL index |
| `watermarks` | How far back the fast tier is complete, one per namespace |
| `markers` | Daemon heartbeats, the operator's hold on eviction, and one lease per daemon that must not run twice |
| `identity` | This archive's own id, a random value written the first time any daemon uses it |

And two on the fast tier, in a database of the same name: `keys`, the unique
key registry, and `pairing`, the id of the archive this fast tier has been
working with. Every daemon compares `pairing` with the archive's `identity` at
startup and refuses to start when they differ, because `cold.uri` then points at
somebody else's archive. So do the commands that write into the archive or
from it: `cline backfill`, `cline keys`, and `cline watermark` when it
declares a claim. `cline rollback` says so rather than refusing, because it
runs during an incident. `cline pairing --accept` records the archive it points
at now, for the time that change is meant.

`_tcBodyDropped` marks the second of those, and is described below.

The unique key registry is the one piece that lives on the *fast* tier instead,
in a database of the same name, because it has to be written in the same place
as the data it constrains.

**The name is `meta.db` in the config and almost nobody should set it.** One
pair of tiers can carry two Thermocline deployments, and the default would have
them share this database: each would read the other's watermarks, residency
holds and singleton markers as its own, and the markers in particular have no
namespace to tell them apart. A feeder id already separates the checkpoints;
this separates the rest. The config refuses a name that also holds a tiered
collection, because the feeder skips its own bookkeeping database wholesale and
that combination would stop those collections being mirrored with nothing
saying so.

**One feeder per id, enforced rather than asked for.** Two feeders with the
same id watch the same namespaces and both see every delete, and an eviction is
announced once: the first to reach it takes the announcement down, the second
finds nothing, reads a real eviction as the application deleting the document,
and tombstones the archive's copy. The document is then gone from both tiers
with nothing said anywhere. Starting the daemon on a second host for redundancy
is enough to arrange it, and so is a unit file that starts one before the old
one has exited.

So each feeder takes a lease in `markers`, named `lease:feeder:<id>`, and a
second one refuses to start and names the host and pid holding it. The lease is
renewed every five seconds and outlives its holder by thirty, so a host that
dies is replaced after half a minute and a clean stop releases it at once. A
feeder that loses its lease while running stops, because carrying on is the
thing the lease exists to prevent.

The evictor takes one too, `lease:evictor:<id>`, and for a milder version of the
same reason: two sweeps overlapping announce one eviction twice and delete it
once, so a count is left standing that the application's own delete of that _id
later answers for, and the archive keeps a live copy of a document its owner
deleted. That lease is per sweep rather than per process, because a sweep is
what must not overlap: two evictors taking turns are two ordinary sweeps. A dry
run takes no lease, announces nothing and removes nothing, so the one command an
operator reaches for to understand the system always answers.

Every instant in those rows is the archive's own, taken as the row is written.
The process deciding whether a lease is stale is never the process that wrote
it, which is the whole point of it, so comparing two hosts' clocks there would
be the very mistake the guard is made of.

Residency is a sidecar rather than a field on the document on purpose.
Extending a document's stay rewrites 60 bytes instead of a 5 KB document.

**A residency row is two things, and the second one is why it outlives the
first.** The hold says a document should stay in the fast tier, and `hotUntil`
is when that ends. The rest of the row -- `promotedAt`, and the oplog position
of each copy-up insert it caused -- is the only evidence that those inserts were
this system moving a document rather than an application creating one, and a
change stream consumer is judged when it reads, which it does when it likes. So
the row expires on `expiresAt`, an hour after the hold, which is the same hour
an eviction announcement gets and for the same reason.

The cost is stated rather than hidden. Rows alive at any moment are roughly the
copy-up rate times `router.residency` plus that hour, so a deployment promoting
a thousand documents a second with a five minute residency holds about four
million rows rather than three hundred thousand. They are small -- an id, a
namespace, two dates and a short array -- and they expire on their own. If that
is the wrong trade for a deployment, the thing to change is the hour, and the
consequence of shortening it is that a consumer further behind than the new
value is shown copy-ups as inserts.

The bound is worth saying plainly: a consumer more than an hour behind reads a
copy-up as an insert, exactly as one more than an hour behind reads an eviction
as a delete. Neither is silent -- both are the same limitation of keeping
evidence for a bounded time.

`positions` and `checkpoints` look redundant and are not. A checkpoint is
durable, majority-acknowledged and written every few seconds, because a stale
one only costs re-mirrored work after a restart. A position is a `w:1` hint
written several times a second, because a stale one costs every fallback read
its wait. Answering both with one record made freshness as slow as durability
was cheap.

The one exception is `_tcDeletedAt`, set on archived copies whose original the
application deleted. The router filters on it, so a tombstoned document is never
returned and never copied back up.

**A document already at the 16 MB limit cannot take that field**, and what
happens then is a decision rather than an accident. The mark does not fit, so
the archive's copy is replaced by a minimal tombstone carrying the `_id`, the
mark and `_tcBodyDropped: true`, and the feeder says so at WARN naming the
document.

Losing one document's audit copy is much cheaper than either alternative.
Leaving it unmarked means the archive holds a live copy of a document the
application deleted, and a later read hands it back: measured, a 16 MB document
deleted on the fast tier came back through the router in full. And the batch
carrying that mark never succeeds, so the feeder reopens its change stream on
the same event again and again and **all** mirroring stops behind it, which was
thirteen identical failures before the run was killed.

**A document the archive's unique index refuses is the other one that stops
everything.** The fast tier accepts a value the archive already holds, the
mirror is refused with a duplicate key, and no retry can ever succeed because
that value belongs to another live document in the archive. Measured on an
adoption: 85 identical reopens in 85 seconds, and an order written through the
router afterwards never reached the archive at all.

That document is left out of the batch and named at ERROR, with the namespace,
the `_id` and the index that refused it. The archive is then missing one
document until the duplicate is resolved, which the evictor already handles:
a document the archive does not hold is never removed from the fast tier, so
nothing is lost, it is only unarchived. Every other document, in that namespace
and every other, goes on mirroring.

The router will not let this happen through itself, because it enforces the
archive's unique indexes across both tiers. What reaches the feeder is a
duplicate something else put there: a unique index added to the archive alone,
or a writer that does not go through the router.

**`_tcDeletedAt` is therefore a reserved field name**, along with
`_tcBodyDropped`. An application that sets either on its own documents will find
them treated as deleted the moment they are served from the archive: still
stored, no longer returned, and only when a read reaches past what the fast tier
holds, which is the hardest shape of bug to notice.

The feeder now says so, once per collection, naming the field and what it costs.
It reports rather than refuses, because refusing would stop mirroring that
collection, which is a larger failure than the one being reported.

### Deleted documents can be given an end date

Keeping the copy is the default and it is deliberate: the archive is where you
look for what the application no longer has. On a deployment that runs for years
it is also two problems. The archive grows with every delete it has ever seen,
and a delete the application issued in order to erase data did not erase it.

`feeder.tombstone_retention` puts an end date on the copy:

    feeder:
      tombstone_retention: 90d

Absent or zero keeps deleted documents for ever, which is what every version
before this did. A value gives the archive a TTL index on `_tcDeletedAt`, so
mongod removes the copy that long after the delete was mirrored. Reads are
unaffected either way, because a tombstoned copy is already invisible to them:
what changes is whether the row is still on disk.

Three details worth knowing:

1. **The index is created on the first delete in a namespace,** not at startup.
   A collection nothing deletes from never gets one, and a feeder that never
   sees a delete does no extra work.
2. **Changing the value replaces the index.** `createIndexes` refuses the same
   name with different options, so a feeder that only created would leave the
   first schedule running while the config claimed the second. Removing the knob
   drops the index, which is how you get the audit trail back.
3. **mongod expires TTL indexes once a minute,** so a retention is a floor, not
   an instant. Anything under `1m` is refused rather than rounded, because a
   misplaced unit is worth catching before a deployment believes its deletes
   erase in seconds.

Retention is per feeder, not per namespace. A deployment that needs one
collection kept for ever and another purged after a month wants two feeders with
their own namespace lists, which is also how you would split the mirroring load.
