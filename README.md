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

**The one difference a caller can observe is freshness.** The archive trails the
fast tier by the feeder's mirroring lag, normally well under a second, so a read
that falls through has the staleness of a secondary read. That is a timing
difference rather than an API difference, but it is real: a read of something
written milliseconds ago may not see it. Applications needing read-your-own-
writes on old data should keep that range warm.

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

A transaction whose read reaches past what the fast tier holds is a different
matter, and is refused. No copy-up fixes a whole range, and answering it from
the archive would leave the transaction reading two clusters at two points in
time. That refusal is deliberately not labelled transient, so it fails rather
than retrying forever.

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
