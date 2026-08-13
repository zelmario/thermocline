# Thermocline

Storage tiering for MongoDB. Run a small fast cluster holding what people
actually use, and a big cheap cluster holding everything, behind one connection
string.

**Status: early, but all four components work. Not production tested.**

## The problem

Databases grow, but access does not grow with them. Most systems read a small
fraction of their data regularly and keep the rest for compliance, history, or
the occasional support question. You pay for all of it on every query.

The cause is indexes. Index trees describe the whole collection, and entries for
busy and idle documents sit interleaved on the same pages. There is no such
thing as an idle index page, so every lookup drags data nobody wants through
memory. That is why adding RAM stops helping past a certain size and never
starts helping again.

When a document moves to the cheap tier, its index entries go with it.

## Components

One binary, `cline`, with a subcommand per component.

| Component | Status | What it does |
|---|---|---|
| `feeder`  | **works** | Tails the fast tier's change stream and mirrors writes into the archive |
| `evictor` | **works** | Removes documents the archive already holds, throttled on cluster health |
| `warmer`  | **works** | Fills contiguous ranges in the fast tier and advances the completeness watermark |
| `router`  | **works** | Speaks the wire protocol, answers from the right tier, copies documents up on demand |

## The rule everything rests on

**The feeder never deletes and never drops.** Inserts, updates and replaces are
mirrored as upserts. Nothing destructive crosses into the archive.

This is required so that removing a document from the fast tier does not remove
it from the archive, which is the entire point. It has a second effect worth
saying out loud: an accident on the fast tier cannot propagate. Run
`deleteMany({})` and then `drop()` on the fast tier, and the archive still holds
every document.

That is covered by a test you can run yourself, below.

## Telling an eviction from a deletion

Both look identical in a change stream: a document disappeared. They mean
opposite things. An eviction is a document moving down a tier, and the archive
must keep it. An application delete means somebody asked for the data to be
removed, and the archive must stop returning it.

The evictor announces every document it is about to remove by writing to
`_thermocline.evicting` on the cold tier **before** deleting from the fast tier.
The feeder resolves each batch of deletes against those announcements in one
round trip.

An unannounced delete is treated as an application delete, and the archived copy
is marked with `_tcDeletedAt` rather than removed. Set
`feeder.unannounced_deletes` to `ignore` if you run bulk deletes outside the
evictor, or to `halt` if you would rather mirroring stopped than guessed.

## What the evictor will not do

Three things stand between an old document and removal. All three are enforced
in code, not documented as guidance.

1. **Nothing is removed unless the archive is confirmed to hold that exact
   document.** The evictor queries the cold tier for the batch it is about to
   delete and drops anything that comes back missing. An empty or unreachable
   archive can never look like permission to delete.
2. **Nothing is removed while mirroring is behind.** Lag is read from the
   feeder's own checkpoint. This check fails closed: if the lag cannot be read
   at all, eviction stops, because removing a document the archive has not
   received destroys it.
3. **A residency hold outranks age.** A document read or promoted recently
   stays, however old it is. That veto is what makes the fast tier a working
   set rather than a date window.

The cluster health signals, replication lag, cache dirty ratio, write tickets
and oplog window, fail open instead. A metric that moves in a future MongoDB
release should not stop eviction forever.

```
bin/cline evictor --config cline.yaml --once --dry-run   # report, change nothing
bin/cline evictor --config cline.yaml --once             # one sweep, then exit
bin/cline evictor --config cline.yaml                    # sweep every 5 minutes
```

A dry run never blocks on cluster health. It changes nothing, so it reports what
would happen and warns separately if real eviction is currently paused.

## The router

It presents itself as a **mongos**, and that is the detail the whole proxy turns
on. A driver uses the `hello` reply for topology discovery: forward the fast
tier's real reply and the driver reads the `hosts` array, connects straight to
those replica set members, and never sends the router another byte. Everything
looks healthy until an archived document comes back missing.

Almost every command is forwarded untouched. The router acts on two cases:

**A read that found nothing.** If the filter is an exact `_id` and the archive
has the document, the router answers with it and copies it up behind the reply.
The caller waits about 30 ms once; every read after that is a normal fast tier
read.

**A write that matched nothing.** This is the dangerous one. An update matching
no document is reported to the driver as success, so an application updating an
archived record is told the write happened when nothing did. Here the copy-up is
synchronous: fetch, insert, then let the original command run again against a
fast tier that now holds it.

```
$ cline router --config cline.yaml
```

Range queries are governed by the watermark below.

### What it does not do yet

- **Aggregations are passed straight through.** They are not miss-checked and
  not completeness-checked.
- **`getMore` is passed through**, so a cursor that started on the fast tier
  stays there. Synthesised single-document replies carry cursor id zero, so no
  `getMore` follows them.
- **Bulk writes are only resolved one statement at a time.** A batch that
  matched nothing is left as the fast tier answered it.
- **Authentication is proxied, not terminated.** Each client gets its own
  connection to the fast tier, so SCRAM works, at the cost of one upstream
  connection per client.
- **TLS is not handled.** Terminate it in front, or add it before taking over a
  production address.

## The completeness watermark

Traffic alone fills the fast tier sparsely, one document at a time. That serves
a lookup by id and is useless for a range query, because holding 12 of a
customer's 40 orders looks exactly like holding all 40.

The warmer fills contiguous ranges instead, walking the archive newest first,
and records how far back it got. `completeFrom` means every live document newer
than that instant is present in the fast tier, so a query bounded inside the
range can be answered there alone.

Two components move it, and only one of them is dangerous:

- The **warmer lowers it** as it fills, extending the guarantee further back.
- The **evictor raises it before removing anything**, because a document removed
  from inside the claimed range makes the claim false.

An understated watermark costs one redundant lookup in the archive. An
overstated one makes range queries return short results and report success. The
evictor therefore raises the watermark *before* it deletes, so an interrupted
sweep still leaves an honest claim.

Two more rules the warmer follows:

- **Tombstoned documents are never copied up.** The archive keeps records the
  application deleted; copying one into the fast tier would bring it back.
- **It never overwrites.** Copies land as `$setOnInsert`, because the fast tier
  is authoritative and its copy is always the newer one.

```
bin/cline warmer --config cline.yaml --dry-run   # report, copy nothing
bin/cline warmer --config cline.yaml             # fill, then exit
```

## Bookkeeping

Thermocline never modifies user documents to carry its own state. Everything
lives in the `_thermocline` database on the cold tier:

| Collection | Purpose |
|---|---|
| `checkpoints` | One change stream resume token per feeder |
| `evicting` | Eviction announcements, expiring on a TTL index |
| `residency` | How long a document should stay in the fast tier (used by the evictor and router) |
| `warming` | Warming announcements, expiring on a TTL index |
| `watermarks` | How far back the fast tier is complete, one per namespace |
| `markers` | Daemon heartbeats, currently just the warmer's |

Residency is a sidecar rather than a field on the document on purpose.
Extending a document's stay rewrites 60 bytes instead of a 5 KB document.

The one exception is `_tcDeletedAt`, set on archived copies whose original the
application deleted. The router projects it out; until the router exists, filter
on it yourself.

## Running it

```
make build
cp cline.example.yaml cline.yaml     # then edit the two URIs
bin/cline feeder --config cline.yaml
```

Both tiers must be replica sets, since the feeder uses change streams.
MongoDB 4.2 or newer.

## Developing

```
make dev                # two single-node replica sets on 28017 and 28018
make check              # fmt, vet, unit tests
make check-integration  # adds the tests that need a live pair
make dev-down
```

Some invariants cannot be tested without a database, because they live in update
filters so that two daemons racing cannot both win. Those tests skip unless
`TC_TEST_COLD` is set, which `make check-integration` does for you.

### Proving the archive survives an accident

With `make dev` running and the feeder pointed at both:

```
mongosh --port 28017 --eval '
  db.getSiblingDB("app").orders.insertMany([{_id:1,x:1},{_id:2,x:2}])'

# wait a moment, then destroy it on the fast tier
mongosh --port 28017 --eval '
  db.getSiblingDB("app").orders.deleteMany({});
  db.getSiblingDB("app").orders.drop()'

# the archive still has both
mongosh --port 28018 --eval '
  print(db.getSiblingDB("app").orders.countDocuments({}))'
```

## What is not handled yet

- **Promotion echo from the router.** The warmer's echo is suppressed through
  `_thermocline.warming` and a heartbeat marker; the router will need the same
  treatment when it starts copying documents up on demand.
- **The first seconds of a warm are not suppressed.** The feeder caches its view
  of the warmer heartbeat for five seconds, so a little echo slips through at the
  start. Irrelevant for a warm lasting hours, worth knowing for a short one.
- **Metrics.** Lag and sweep counters are computed and logged but not exported
  to Prometheus. Mirroring lag is the first alert you should build, since
  eviction safety and recovery both hang off it.
- **Eviction does not pause automatically during an archive restore.** It stops
  on mirroring lag, which usually covers it, but the restore case deserves an
  explicit check.
- **Sharded fast tiers.** Untested.
- **Router connection pooling.** One upstream connection per client is simple
  and correct for authentication, and it will not scale to tens of thousands of
  clients.
- **Resharding and collection-level DDL** are logged and ignored, never replayed.

## Design documents

The architecture, sizing from 5 TB to 100 TB, and the alternatives worth ruling
out first are written up separately, along with a phased rollout guide for
adopting this on a database that is already serving traffic.
