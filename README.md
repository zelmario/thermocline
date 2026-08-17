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
| `watermark` | **works** | Shows, declares and withdraws what the fast tier claims to hold |
| `keys`    | **works** | Claims the unique keys that have to survive eviction |

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

- **Aggregations are completeness-checked but not miss-checked.** A pipeline is
  never resolved against the archive document by document; it is either safe to
  run on the fast tier or refused. See below.
- **`getMore` on a fast tier cursor is passed through.** Cursors the router
  creates for archive reads are served here instead, and tracked per connection.
- **Bulk writes are only miss-resolved one statement at a time.** A batch that
  matched nothing is left as the fast tier answered it. Unique keys and
  multi-document writes are handled across the whole batch.
- **Authentication is proxied, not terminated.** Each client gets its own
  connection to the fast tier, so SCRAM works, at the cost of one upstream
  connection per client. It also sets what happens during a fast tier election:
  the router never sees the credentials, so it cannot re-authenticate a fresh
  upstream and replay a command that was in flight. Writes issued while the fast
  tier has no primary fail and the client has to retry, where a driver talking
  to the replica set directly would block in server selection instead. Measured
  during a real election: six writes a second apart were refused, every
  acknowledged write was present, none was applied twice, and a retry finished
  the job. A client reconnecting mid-election now waits up to 20 seconds for a
  new primary rather than failing outright.
- **Read preference is not honoured. Every read is served by the primary.** This
  follows from proxied authentication and is the one limitation to weigh before a
  pilot. The router answers `hello` as a mongos, and a mongos routes by read
  preference, so drivers attach `$readPreference` to reads and expect it to be
  acted on. It cannot be: the client's SASL exchange is forwarded to one upstream
  connection, and that connection is the only thing carrying the client's
  identity. A second connection to a secondary would be authenticated as nobody,
  SCRAM is nonce-bound and one-shot so the exchange cannot be replayed there, and
  the router never sees a password to redo it with. Using the router's own
  credentials instead is precisely the privilege bypass that Authorisation below
  exists to close.

  Nothing returns a wrong answer: a read served by the primary is strictly
  fresher than one asked of a secondary. What changes is where the load lands. An
  application that pins reporting to secondaries moves that entire read load onto
  the fast tier's primary the day it goes behind this router. Measured on a
  three-member fast tier with the profiler recording on every node:
  `secondary`, `secondaryPreferred` and `nearest` each landed on the primary,
  while the same query sent straight to the replica set landed on a secondary.

  Each connection logs a warning the first time it asks for a non-primary read,
  and `thermocline_primary_only_reads_total` counts them. Lifting the limitation
  means terminating authentication in the router, which is an architectural
  change rather than a fix.
- **TLS is not handled.** Terminate it in front, or add it before taking over a
  production address.

### Reads the fast tier cannot answer

The rule is that no application changes. A query that returns the wrong number
breaks that, and so does a query that returns an error it never used to return.
Refusing a `countDocuments` is safe and still a broken application.

So a read the fast tier cannot answer in full **runs against the archive
instead**. That works because the archive is not a partial complement of the
fast tier, it is a complete MongoDB holding every document, so there is no
merging of two half-answers: the query runs whole, on one tier or the other.
Tombstoned records are filtered out on the way, so documents the application
deleted do not come back.

This covers the cases that used to need a decision before adoption: unbounded
`find`, `countDocuments` and other aggregations, `$lookup` into a tiered
collection, and skip-based pagination. All of them now return the same answer
they would from a single cluster.

**The one difference a caller can observe is freshness**, and the router closes
it rather than documenting it away. The archive trails the fast tier by the
feeder's mirroring lag, so before answering from the archive the router waits
for mirroring to reach the instant the read began. Every write that completed
before then is in the archive by the time it is read, which is why a fallback
read cannot miss a write that already returned or hand back a document the
caller has already deleted.

That wait is only as precise as the position it waits on, and this is where it
went wrong. It waited on the durable checkpoint, which moves once per
`feeder.checkpoint_every`, so a fallback read paid a whole checkpoint interval
even with the feeder perfectly caught up and nothing to wait for. Measured on an
idle database: **1,142 ms with `checkpoint_every: 1s`, 4,531 ms at the shipped
default of 5s, against 1.8 ms for the same query with the wait turned off.** At
the default it was also worse than slow: the wait exceeded `archive_wait`, which
also defaults to 5s, so reads gave up and answered stale anyway.

Freshness now has its own record. The feeder publishes how far it has mirrored
every `feeder.position_every` (250 ms by default) as one small document with no
resume token at `w:1`, and the durable checkpoint goes back to meaning only what
it says. The same read now takes **520 ms**, and the remaining cost is honest:
on an idle database the feeder cannot know that time has passed until its change
stream poll times out, so budget a fallback read at roughly
`max_await + position_every` and lower both if such reads are on a user's path.

A read of something written milliseconds ago is therefore seen, at the cost of
that wait. A stopped feeder is the one case the barrier cannot hold, and there
the router answers anyway and warns rather than turning a dead background daemon
into failed queries. It notices quickly: the position record carries the cadence
it promised, so four missed publications mark the publisher as gone instead of
making every read wait out the full `archive_wait`.

`router.completeness` chooses the behaviour: `archive` (the default), `strict`
to refuse instead, `permissive` to return the fast tier's short answer.

### Aggregations

Every driver's `countDocuments` is an `aggregate`, so leaving pipelines
unguarded left the most common count in the system free to return a wrong
number. Against a partly filled fast tier one returned 1 where the answer was
45,904, and reported success.

Pipelines are now judged **before they run**, unlike `find`, which is inspected
on the way back. A pipeline can end in `$out` or `$merge`, and refusing after
the fact would leave a truncated collection already written.

Two rules do the work, and both exist because a pipeline is not a filter:

- **Only a leading `$match` bounds the scan.** A `$match` further down filters
  what earlier stages already read, so it says nothing about how far back the
  scan reached. A pipeline with no leading `$match` scans the whole collection
  and is refused unless the fast tier holds all of it.
- **A joined collection has to be whole.** `$lookup`, `$graphLookup` and
  `$unionWith` read by key, not by time, so no partial watermark covers them.
  Joining a tiered collection that the fast tier holds only part of is refused,
  because the alternative is a join that silently drops rows.

Collections Thermocline does not tier are exempt. They live only on the fast
tier, so they are whole by definition, and demanding a claim for them would
refuse perfectly good queries. That is why the router reads `feeder.namespaces`.

`$out` and `$merge` are refused even under `completeness: permissive`. Returning
a short answer is a wrong number on somebody's screen; writing one makes it the
input to everything downstream.

### Writes that name many documents

`updateMany` and `deleteMany` only ever reached the documents the fast tier
happened to hold. Against a partly evicted collection one reported three
modified where thirteen matched, and a delete reported two where twelve did,
both as success. For a delete that is worse than a wrong number: records
somebody asked to have removed stay readable.

Reads solve this by going to the archive. A write cannot, because the archive is
upsert only and the feeder is its only writer, so applying the change there
directly would race mirroring and break the rule the design rests on. Instead
the matching documents are brought up to the fast tier first, and then the
client's own command runs, once, against a tier that now holds all of them.

The order matters. Running the write first and repeating it after a copy-up
would apply `$inc` twice.

A sweep over an unbounded range could drag the whole archive up to satisfy one
statement, so there is a ceiling of 10,000 documents. Past it the router refuses
and says so, rather than doing part of the job and reporting success.

### Unique indexes

A unique index only constrains the documents it can see. Evicting a document
takes its index entry with it, so the fast tier stops knowing that value was
ever used and a later insert of the same key succeeds. Nothing errors, and the
collection ends up holding the two documents the index existed to prevent.

Every insert now claims its unique keys in `_thermocline.keys` on the fast tier
first, and that claim is what fails when a value is already taken, whichever
tier the original is sitting in. The caller gets an ordinary `E11000`, which is
what the application already handles.

Claims are permanent, so a deleted document leaves one behind. That is resolved
lazily rather than on the delete path: a claim only matters when something
collides with it, and at that point the router checks whether the owning
document still exists in either tier. If it does not, the new document takes the
claim over. Deletes stay untouched and this costs nothing until there is a real
conflict.

**Before evicting anything from a database you adopted with data already in it,
claim the keys it already holds:**

```
bin/cline keys --config cline.yaml              # which collections are enforced
bin/cline keys --config cline.yaml --rebuild    # claim keys for existing documents
```

It is safe to re-run, and it is also how the registry comes back after a fast
tier is rebuilt, since it lives there. If it finds one key held by two live
documents it says so and picks no winner, because that means the constraint was
already broken.

Two limits worth knowing. **Sparse and partial unique indexes are not enforced
across tiers**, because a registry cannot evaluate the same condition; the
router says which ones it skipped. And **an update that changes a unique field
to a value held by an evicted document is not caught yet**, only inserts are.

### Transactions

A transaction cannot span two clusters, so copying a document up and re-running
the statement inside the caller's transaction does not work: the copy-up is an
ordinary write outside it, and the transaction's snapshot was taken before it
landed.

The way out is the contract drivers already implement. The router copies the
document up outside the transaction, then returns an error labelled
`TransientTransactionError`, which every official driver's `withTransaction`
helper retries from the start. The replay takes a fresh snapshot, the document
is there, and the application never learns why. No code changes.

A transaction whose read or multi-document write reaches past what the fast tier
holds is the same problem one size larger, and it gets the same answer. The
documents are copied up and the transaction is replayed. Reading the archive is
still not on offer, because that would leave the transaction reading two
clusters at two points in time; moving the documents to the tier it is already
reading needs no such thing.

The completeness watermark cannot decide this one. It records that a whole range
was warmed, and copying documents up for a single query does not make that true,
so a replay that re-checked the watermark would fail exactly as the first
attempt did. What is true after the copy-up is narrower and stronger: the
archive holds no live document matching this filter that the fast tier lacks.
That is completeness established per query, and it is what lets the replay
succeed. Past `maxCopyUpForWrite` documents the range is refused instead, with
an error that is deliberately not labelled transient so it fails rather than
retrying forever.

**Unique keys are checked differently inside a transaction, and have to be.**
Everywhere else the guard runs before the write, because once a write lands it
is visible and a duplicate cannot be taken back. Inside a transaction nothing is
visible until commit, so the guard runs after the write instead: the router
reads back what the statement actually produced, from inside the caller's own
session, and refuses by aborting the transaction if it duplicates a key an
archived document holds.

That ordering is not a preference. The router's own client cannot see a
transaction's uncommitted writes, so a guard that ran first would compute keys
for a document that is not there yet. It did, and a transaction that inserted a
document and then moved it onto an archived document's unique value committed
two live documents sharing one key, reported as success. Reading the post-image
inside the caller's session removes the guess entirely: no evaluating a `$inc`
or a pipeline update, and no dry run in a session of the router's own that
deadlocks against the caller's locks.

## The dashboard

Set `router.dashboard` to an address and the router serves one page showing what
each tier holds and how it is being used:

```yaml
router:
  dashboard: "127.0.0.1:27101"
```

It answers the three questions the design makes claims about, with measurements
rather than assertions:

- **what share of commands the fast tier answered on its own**, against the ones
  that needed the archive;
- **what that is worth**, as the measured mean latency of a fast tier hit beside
  the mean of a read that fell through, and the ratio between them;
- **how little of the data has to be hot** to buy that, from collection metadata
  on both tiers.

Underneath are commands per second by how they were served, latency per class on
a log scale, per-collection tier occupancy, and the counters an operator watches:
copy-ups, refusals, stale reads and cross-tier duplicates stopped. Everything on
the page is also available as a table, and as JSON at `/api/snapshot`.

The page has no authentication of its own. It names collections and reports
traffic, so bind it to an operator network rather than to the address the
application dials.

`test/traffic.sh` drives representative traffic down every path if you want to
see it with something on it, and `test/load.sh` keeps it running.

### Indexes are not copied, in either direction

Nothing synchronises indexes between the tiers, and nothing should. Index trees
describe the whole collection, which is the cost tiering exists to avoid, so
carrying every one of them on the archive gives most of the saving back. The
fast tier holds the application's query plans; the archive is expected to hold
far fewer.

**The consequence is that a read which falls through runs with whatever indexes
the archive happens to have.** For a filter that is slower. For a sort it can be
worse: past the server's sort memory limit a blocking sort does not degrade, it
fails. Archive reads therefore ask for `allowDiskUse`, so a sorted query that the
fast tier answered from an index becomes slow rather than broken. An operator who
would rather refuse expensive fallback reads than run them has
`router.completeness: strict`, which refuses them all.

`hint` is dropped for the same reason: it names an index the archive is not
expected to have.

To see the divergence rather than infer it:

```
$ cline indexes --config cline.yaml
shop.orders
  fast tier  6 index(es), archive  2
  missing from the archive:
    customer_1_createdAt_-1            {customer: 1, createdAt: -1}
  a read that falls through and uses these will scan the archive
```

The rule it prints is the one to follow: keep an index on the archive when a
query that falls through filters or sorts on its fields, and drop it when
nothing does, because that storage is the saving. A unique index is called out
separately, since cross-tier uniqueness is the key registry's job and its
absence on the archive is expected.

### Authorisation

Authentication is proxied rather than terminated, so the fast tier holds the
client's identity and decides what it may read. Almost everything reaches the
fast tier, and a read it refuses comes back as an error rather than a miss, so
nothing falls through.

`count`, `distinct` and aggregations are judged **before** they run, because a
pipeline can end in `$out` and refusing one afterwards would leave a truncated
collection written. That means the fast tier never sees them, so before
answering any of them from the archive the router asks the fast tier, over the
client's own authenticated connection, whether that client may read the
collection. The fast tier's own error is returned unchanged.

Without that check a client with no access to a tiered collection could obtain
its contents with a count, but only while the collection was incomplete, which
is to say only while tiering was doing its job.

The archive is reached with the router's own credentials, because the client has
no session there and cannot be given one. **It should therefore be reachable
only by the router**, which the operating rules already require for a different
reason: the cheap tier is read-only to everyone except the feeder.

### Upserts

An upsert is the one write shape that cannot be put right on the way back. Every
other write that matches nothing on the fast tier is resolved afterwards, by
copying the archived document up and running the write again. An upsert does not
wait: matching nothing is its cue to create a document there and then, built from
the filter's equalities and the update.

That document carries the archived record's `_id` and almost none of its fields,
and the feeder mirrors it up as an upsert, so **the archived record is
overwritten by it**. `copyUp` uses `$setOnInsert` precisely so a promotion never
clobbers a newer fast tier copy, which means it cannot undo this either.

So an upsert on a tiered collection is judged before it runs. If the fast tier
matches nothing, whatever the archive matches is copied up first, and the upsert
then finds it and updates it, which is what the same statement would have done
with no tiering underneath it.

The cost is one indexed lookup on the fast tier per upsert, and only on a
collection whose watermark does not already claim all of history. A fully warmed
collection has nothing archived that is not also on the fast tier, so the guard
returns immediately and costs nothing.

### Prometheus

The same address serves `/metrics` in the Prometheus text format: commands and
latency as a histogram per class, the copy-up, refusal, duplicate-key and
stale-read counters, documents and bytes per namespace and tier, and

    thermocline_mirroring_lag_seconds

which is the first alert to build. **It is exported by the router rather than by
the feeder on purpose.** The router already reads the feeder's published
position on every fallback read, so it knows the lag continuously, and a gauge
published by the feeder itself goes silent exactly when the feeder dies, which
is the moment the number matters. Scraped from the router, a dead feeder reads
as lag climbing, which is what an alert can see.

```yaml
scrape_configs:
  - job_name: thermocline
    static_configs:
      - targets: ["router-1.internal:27101"]
```

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

**No watermark means nothing is guaranteed**, not that everything is present.
That distinction is the whole point: a fast tier built empty has no claim, and
answering range queries from it would return short results and call them
success. Range queries are refused until something records a claim.

A fast tier that genuinely holds everything, because it is the original cluster
rather than a new empty one, says so explicitly:

```
bin/cline watermark --config cline.yaml                        # show every claim
bin/cline watermark --config cline.yaml --ns app.orders --complete-all
bin/cline watermark --config cline.yaml --ns app.orders --clear
```

A claim also has to be withdrawn when the fast tier loses documents by any route
the evictor did not drive, because the claim then describes data that is no
longer there. Two things handle that:

- **The feeder withdraws it on a `drop`,** since the fast tier just lost the
  whole collection.
- **The warmer checks it against reality.** A restore or a rebuild leaves no
  change stream event at all, so the claim simply outlives its data. Before
  trusting a claim, the warmer asks whether the fast tier holds anything in the
  range; if it holds nothing while the archive does, the claim is discarded and
  the window is warmed again.

A partial loss is caught by neither. Telling "half the range is missing" from
"half the range was legitimately deleted" needs a document by document
comparison, which is a repair tool rather than a warm.

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
| `checkpoints` | One change stream resume token per feeder, for recovery |
| `positions` | How far each feeder has mirrored, published far more often than the checkpoint because fallback reads wait on it |
| `evicting` | Eviction announcements, expiring on a TTL index |
| `residency` | How long a document should stay in the fast tier (used by the evictor and router) |
| `warming` | Warming announcements, expiring on a TTL index |
| `watermarks` | How far back the fast tier is complete, one per namespace |
| `markers` | Daemon heartbeats, currently just the warmer's |

Residency is a sidecar rather than a field on the document on purpose.
Extending a document's stay rewrites 60 bytes instead of a 5 KB document.

`positions` and `checkpoints` look redundant and are not. A checkpoint is
durable, majority-acknowledged and written every few seconds, because a stale
one only costs re-mirrored work after a restart. A position is a `w:1` hint
written several times a second, because a stale one costs every fallback read
its wait. Answering both with one record made freshness as slow as durability
was cheap.

The one exception is `_tcDeletedAt`, set on archived copies whose original the
application deleted. The router filters on it, so a tombstoned document is never
returned and never copied back up.

**`_tcDeletedAt` is therefore a reserved field name.** An application that sets
it on its own documents will find them treated as deleted the moment they are
served from the archive: still stored, no longer returned. Nothing detects this,
so it is a name to avoid rather than a trap the code springs.

## Running it

```
make build
cp cline.example.yaml cline.yaml     # then edit the two URIs
bin/cline feeder --config cline.yaml
```

### Logs

Every daemon logs to stderr by default. Under systemd that is what you want,
since journald captures the stream and rotates it for you.

Outside a service manager, give each one a file:

```
bin/cline feeder --config cline.yaml --log-file /var/log/cline/feeder.log
bin/cline router --config cline.yaml --log-file /var/log/cline/router.log
```

`log.file` in the config sets a default for all four, and `--log-file`
overrides it per process, which is how you keep them in separate files. A
configured file behaves like mongod's `systemLog.path`: everything goes to the
file and nothing to the terminal.

Rotation is logrotate's job, and it only works if you signal the process:

```
/var/log/cline/*.log {
    daily
    rotate 14
    compress
    missingok
    notifempty
    postrotate
        pkill -HUP -f '^/usr/local/bin/cline' || true
    endscript
}
```

Without the SIGHUP the daemon keeps writing to the renamed inode, so the file
you are tailing stays empty while the disk fills up.

Both tiers must be replica sets, since the feeder uses change streams.
MongoDB 4.2 or newer.

## Testing on a realistic timescale

Nothing assumes days. The tier boundary is entirely config, so `older_than: 3m`
behaves exactly like `older_than: 90d` and a full lifecycle runs in minutes.
`cline.test.yaml` is a minute-scale profile.

For tests that run repeatedly, fabricate the age instead of waiting for it:

```js
ObjectId.createFromTime(Math.floor(Date.now()/1000) - 86400)   // one day old
```

Age is read from the ObjectId, so a fabricated timestamp is indistinguishable
from a genuinely old document and the same code paths run. ObjectId timestamps
have one second of resolution, so minute-scale boundaries are safe and
sub-second ones are not.

One thing catches everybody: a cluster built minutes ago has minutes of oplog,
and the production default of `min_oplog_window: 24h` blocks eviction outright.
It looks like a hang. Lower it in any test profile.

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

- **Promotion echo from the router.** The router does copy documents up on
  demand, and its promotions are not suppressed. The warmer announces into
  `_thermocline.warming` and the feeder skips what it finds there; a router
  copy-up records residency instead, which the suppression does not read. So the
  feeder mirrors every promoted document straight back down into the archive it
  was just read from. The document is identical, so this costs a redundant
  archive write per promotion rather than a wrong answer, but it is write
  amplification exactly where a read-heavy workload over archived data will feel
  it. It was not always harmless: while promotion was written as `$setOnInsert`,
  this echo is what made the reordered copy permanent.
- **The first seconds of a warm are not suppressed.** The feeder caches its view
  of the warmer heartbeat for five seconds, so a little echo slips through at the
  start. Irrelevant for a warm lasting hours, worth knowing for a short one.
- **Prometheus for the background daemons.** The router exports itself and
  mirroring lag at `/metrics`, but the evictor's and warmer's own counters, such
  as documents swept per pass and why a pass backed off, still only reach a log
  line.
- **Eviction does not pause automatically during an archive restore.** It stops
  on mirroring lag, which usually covers it, but the restore case deserves an
  explicit check.
- **Sharded fast tiers.** Untested.
- **Ticket pressure is measured as a fraction of the pool.** MongoDB 7.0 made
  the write ticket pool dynamic and host-sized, so an absolute floor is
  meaningless: a container reports a total of 8 tickets where a pre-7.0 server
  reported a fixed 128.
- **Router connection pooling.** One upstream connection per client is simple
  and correct for authentication, and it will not scale to tens of thousands of
  clients.
- **Resharding and collection-level DDL** are logged and ignored, never replayed.

## Design documents

The architecture, sizing from 500 GB to 5 TB, and the alternatives worth ruling
out first are written up separately, along with a phased rollout guide for
adopting this on a database that is already serving traffic.
