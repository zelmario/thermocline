# Thermocline

Storage tiering for MongoDB. Run a small fast cluster holding what people
actually use, and a big cheap cluster holding everything, behind one connection
string.

**Status: early, but all four components work. Not production tested.**

It is tested against real clusters rather than mocks: 34 suites and about a
thousand checks against two three-member replica sets, and a pilot that adopts
MongoDB's published sample datasets and compares every answer the router gives
against the archive it came from. See `test/README.md` and `test/pilot`. The CI
badge only means it compiles; a runner has no cluster to test tiering with.

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
   document, live.** The evictor queries the cold tier for the batch it is about
   to delete and drops anything that comes back missing. An empty or unreachable
   archive can never look like permission to delete.

   *Live* is the word that carries this. A tombstoned copy is a record that the
   application deleted the document, not a copy of it, and reads filter it out,
   so counting it would confirm the archive holds something no read will ever
   return. That state is ordinary rather than exotic: an application deletes a
   record and writes the same `_id` again, which is what a natural key or a
   reprocessed record looks like, and until the feeder mirrors the second write
   the fast tier holds the only live copy while the archive holds a tombstone.
   Measured without that filter: one sweep, and the document was on neither tier
   and the router answered nothing for a record the application had just
   written.
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
- **Authentication is proxied by default, and can be terminated.** Everything in
  the next two bullets describes the default. Setting `router.terminate_auth`
  changes the architecture: the router verifies the client's password itself,
  from the salt and stored keys mongod keeps in `admin.system.users`, loads that
  user's privileges with `usersInfo`, and reaches both tiers on connections it
  owns. That lifts all three limitations below at once, and it moves the
  decision about who may read what from the fast tier into this process, where
  Authorisation is then the only thing between a client and somebody else's
  data. **It is off by default and should stay off until it has run somewhere
  real.**

  Proxied, which is the default: each client gets its own
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

  A client that is *already* connected was the worse half of this, and it was
  not a pause but a permanent outage. The router re-resolved which member is
  primary only when a dial failed, which covers a member that died; a member
  that steps down stays up, keeps accepting connections and answers every
  command with `NotPrimaryNoSecondaryOk`, so nothing ever failed to dial and
  nothing ever asked again. Measured with the shop running through the router
  while the fast tier's primary stepped down: 4,226 refusals and still counting
  three minutes later, against a set that had elected a new primary in one
  second, and every client that connected afterwards was refused too. The router
  now re-resolves on the first refusal, and ends that connection once the
  refusal has been delivered, which is what mongod does to the clients of a
  member that stops being primary. The driver reconnects, that handshake
  authenticates against the member that is primary now, and the retry lands.
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
  and `thermocline_primary_only_reads_total` counts them. With
  `router.terminate_auth` on, read preference is honoured: measured with the
  profiler recording on all three fast tier members, `secondary`,
  `secondaryPreferred` and `nearest` each landed on a secondary.
- **A stats call counts the whole collection and measures one tier.** Those are
  different questions and a single reply carries both. `db.coll.stats().count`,
  the `collStats` command and `$collStats` all report the collection's document
  count, which is the same number `countDocuments` gives and comes from the same
  place. Everything else in that reply is bytes on a disk: `storageSize`,
  `totalIndexSize`, `avgObjSize` and the WiredTiger figures describe the fast
  tier, because they are per-cluster facts and adding two clusters' compressed
  sizes together would produce a number describing nothing. That is also what
  you want when sizing a fast tier. `dbStats` is the fast tier's throughout,
  including its `objects`.

  **That inconsistency is deliberate, and it is a decision rather than an
  oversight.** `collStats` corrects its count and `dbStats` does not. Correcting
  `dbStats` means a per-collection count against the archive on a command
  monitoring agents poll every few seconds, and the number it would replace is
  the one thing in that reply an operator sizing a fast tier actually wants.
  Read `dbStats` as a physical fact about the fast tier, and ask
  `countDocuments` or `collStats` for a logical one.

  The count used to be the fast tier's too, which was wrong rather than
  partial. Measured on MongoDB's sample datasets adopted as an archive:
  `sample_training.trips` reported 0 documents where the archive holds 10,000,
  because none of that collection was inside the hot window, while
  `countDocuments` on it answered 10,000 throughout. See `test/pilot`.
- **The router names the accounts and roles the fast tier is missing.** The fast
  tier is a new replica set and carries none of the application's users, and the
  router authenticates against it, so on day one every connection is refused
  with MongoDB's ordinary authentication failure and nothing connects that to
  the tier missing the account. It says which ones, once, at startup.

  It also names a cluster-wide read or write concern default the archive has and
  the fast tier does not. `setDefaultRWConcern` is a decision about durability
  and about what a read may see, made once and then never mentioned again by the
  application, and reads and writes through the router are served by the fast
  tier. So an old cluster answering at majority answers at local through the
  router, and the application asked for neither.

  Roles come first in that list because they come first in the fix. Creating an
  account with a custom role the fast tier does not have is refused with "Could
  not find role", so an application with its own role is one whose every account
  fails the first adoption step. Create the roles, then the accounts that hold
  them.
- **A drop through the router stops the claim at once.** Dropping a tiered
  collection empties the fast tier immediately, and the claim that it holds
  everything outlives it: the feeder withdraws the claim when it sees the event,
  and the router keeps a cached copy for a few seconds after that. Measured on a
  collection claimed complete for all of history: `countDocuments` straight
  after the drop answered **0 where the archive holds 3**, with no error.

  A drop the router forwarded is one it knows about at once, so it forgets the
  claim there and then. A drop applied straight to the fast tier, out of band, is
  not: that window remains, and the answer corrects itself once the feeder has
  withdrawn the claim and the cache has expired.

  **It also leaves the indexer remembering a collection that has gone**, and that
  one does not correct itself. The router creates the fast tier's copy the way the
  archive has it before any write that would otherwise create it bare, and
  remembers having done so; a drop it forwarded clears that memory, and a drop it
  never saw does not. The next write then recreates the collection with no
  collation and no validator, and a query the application has always run answers
  differently. So drop a tiered collection **through the router**, which is where
  every other consequence of a drop is handled too.
- **An eviction sweep waits before it removes what it has just stopped
  claiming.** The evictor raises the completeness watermark first, so an
  interrupted sweep never leaves a claim wider than the truth. That protects a
  reader who reads the claim fresh; the router caches one, because reading it
  per query would put an archive round trip on the hot path.

  So for a few seconds after the raise there are two copies of the claim and the
  router's is the older, wider one. Measured before this, with a fast tier
  claiming all of history and a sweep removing 905 of 2,000 documents: the
  router answered `countDocuments` with **1,095 where the archive holds 2,000**,
  and answered 2,000 again seven seconds later. No error either time.

  The sweep now waits out that window between raising the claim and acting on
  it. Once per rule per sweep, only when the boundary actually moved, and a
  sweep that scans thousands of documents does not notice it.
- **The newest N is answered by the fast tier, without a claim on all of
  history.** "The last ten orders" carries no lower time bound, so nothing short
  of `--complete-all` used to cover it, and a fast tier holding the last thirty
  days was sent to the archive to be told which ten orders are the newest. It is
  the one place that certainly holds them.

  The proof is in its own answer. The claim says it holds every live document
  newer than T, so if it returns the ten asked for, sorted newest first, and the
  tenth is itself newer than T, every document it did not return is older than
  all ten. The filter does not matter: whatever it selects, the fast tier holds
  every match newer than T. Measured on a shop of 3,000 orders with 314 on the
  fast tier, thirty such queries took 118 ms and three archive round trips
  before, and 53 ms and none after.

  Two conditions, neither a judgement call: the sort must lead with the field
  the collection ages by, descending, because that is the only order in which
  the last one returned bounds the rest; and the whole answer must be in the
  batch, because a partial one does not contain the document that carries the
  proof. Ask for more than the fast tier holds and it falls through as before.
- **A feeder that has stopped is not a feeder that is behind.** Reads wait for
  mirroring to reach what you have already written, up to `router.archive_wait`,
  and a feeder that is merely behind gets there. One that has stopped never
  will, so the router stops waiting for it: a position record that exists and
  has gone stale is the evidence, and it answers at once and says the answer may
  be behind.

  It matters more than one wait, because a read of an archived document copies
  it up, and that copy-up is itself a write the next read then waits to see
  mirrored. Measured with the feeder stopped and the fast tier emptied, so every
  read copied one up: **10 reads took 45,717ms, 4.6 seconds each**, with eleven
  stale-read warnings. The same 10 take 13ms now.

  The trade is stated rather than hidden: such an answer can miss a very recent
  write, which is what the warning says, and every document the archive holds is
  still returned. A feeder too old to publish a position leaves only a
  checkpoint, and that case still waits, because there is nothing else to go on.
- **A completeness claim only covers a query bounded in the same dimension.** The
  claim says the fast tier holds every document newer than an instant, and
  "newer" is measured the way your rules measure age: the `date_field`, or the
  timestamp inside a default ObjectId. A `$gte` on any other date is a bound in
  a different dimension and says nothing about which documents are here, so it
  is read as unbounded and the query goes to the archive.

  Measured on a collection aged by `createdAt`, claimed complete for the last
  thirty days, holding a recent order on the fast tier and a two year old one in
  the archive, both shipped yesterday. Before this,
  `{shippedAt: {$gte: two days ago}}` was read as bounded two days back, judged
  inside the claim, and answered 1 where the archive holds 2. No error, and the
  number looked reasonable.

  A namespace with no evictor or warmer rule, or with two rules that disagree
  about which field it ages by, has no dimension anything can name, so its
  bounded queries go to the archive too. `--complete-all` is unaffected: a claim
  on all of history needs no bound.
- **The feeder stops when its resume point is gone, rather than retrying.** If it
  is down long enough for the fast tier's oplog to roll past its checkpoint, the
  writes made in that window are on the fast tier and not in the archive, and
  reopening the change stream cannot fetch them: those oplog entries no longer
  exist. Measured before this: 37 reopen attempts in forty seconds, a driver
  error logged once a second, and nothing anywhere saying that the archive now
  had a hole in it.

  It exits instead, naming the checkpoint it could not resume from. A daemon
  that stops is noticed by whatever runs it; a repeating log line is noticed by
  nobody. Clearing the checkpoint restarts mirroring from now, which hides the
  gap rather than closing it, so copy the affected namespaces into the archive
  first.
- **A sorted `findAndModify` acts on the document the sort really names.** The
  sort is how you say which of several matches you mean, and the fast tier can
  only choose from the part it holds. The repair paths for a write that matched
  nothing do not apply here, because something did match: the fast tier answered
  with the first of its own share, and reported one document matched and one
  modified, which is true and is the wrong document.

  Measured on an archive holding seq 1, 2 and 3 with only 2 and 3 on the fast
  tier: `findOneAndUpdate({}, ..., {sort: {seq: 1}})` modified seq 2, and
  `findOneAndDelete` with the same sort deleted seq 2. Now the archive is asked
  which document that sort names, and it is brought up before the write runs.
  Before, not after: by the time a reply could be inspected the update has been
  applied to the wrong document and there is no taking it back.

  A `findAndModify` with no sort is untouched, because MongoDB itself promises
  only that some matching document is acted on, so the fast tier's choice is as
  good as the archive's. `update` and `delete` statements carry no sort.
- **Dropping a unique index reaches both tiers.** Uniqueness across the tiers is
  enforced from the fast tier's indexes, so once one is dropped there the router
  accepts a value the archive still refuses. The feeder cannot mirror that
  document: it says so once and leaves it on the fast tier alone, where the
  evictor will not touch it either, because a document the archive does not hold
  is never evicted. Measured before this: the index dropped through the router,
  then the duplicate the application was now entitled to write, and afterwards
  the fast tier held two documents and the archive one. The indexer also put the
  index back on the next router start, so the constraint was off now and on
  again later.

  A plain index is a different thing and stays where it is. It is a decision
  about speed, the two tiers are allowed to differ, and you may want one on the
  archive for the reads that fall through; `cline indexes` reports that, and
  flags a unique index missing from the fast tier as the hazard it is.
- **A tiered collection cannot be turned into one the feeder cannot mirror.**
  `convertToCapped`, `cloneCollectionAsCapped`, and `create` asking for `capped`
  or `timeseries` are refused on a tiered namespace. The feeder already refuses
  such a namespace at startup, because a capped collection removes its own
  oldest documents when it fills and nothing announces those, so the archive
  would record them as your application deleting; that check runs when the
  feeder starts, and nothing stopped a client doing it afterwards.

  Measured on a tiered collection of 200 documents of about a kilobyte, with
  `convertToCapped` to 4KB through the router. Three things happened and the
  command reported success: the fast tier went to 4 documents immediately, the
  feeder then refused to start at all so mirroring stopped for every namespace,
  and once anything claimed the fast tier was complete the router answered
  `countDocuments` with 4 where the archive holds 200.

  A collection nothing tiers may still be capped or a time series collection,
  and an ordinary `create` on a tiered name is untouched.
- **A collection using queryable encryption cannot be tiered.** Its documents
  would copy across the way any binary field does, so mirroring reports success.
  What does not copy is the state that makes its fields searchable: mongod keeps
  that in the `enxcol_` collections it creates beside the data, and nothing here
  tiers those. An encrypted read that fell through would be answered from an
  archive holding the documents without it. The feeder refuses such a namespace
  at startup, and refuses an `enxcol_` collection named directly, which belongs
  to mongod rather than to the application.
- **An empty `feeder.namespaces` is checked as well now.** It means every
  collection of every database, and it used to mean nothing was checked at all:
  the configuration that mirrors the most did the least checking, so a capped
  collection anywhere in the deployment was mirrored and the archive tombstoned
  its history at the first rollover. The refusal now names it and says to list
  the namespaces to mirror instead.

  Only the kinds that are actually mirrored block it. Nothing of a view or a
  time series collection is ever mirrored, so finding one while mirroring
  everything is not a mistake and does not stop the feeder; naming one in the
  config still is, because that says the namespace is being tiered and it never
  will be.
- **A TTL index on a tiered collection is refused.** It would do the opposite of
  what it says. The command reaches the fast tier, which holds only the recent
  part of the collection, so the index expires the documents that were recent
  enough to be cached; the feeder reads those expiries as your application
  deleting them and the archive records the deletions. The older documents the
  rule was written for are in the archive, where no TTL index is watching them.

  Measured on a tiered collection with five documents a minute old on both tiers
  and five over a year old in the archive alone, asking for anything older than
  thirty seconds to expire: the five recent ones were destroyed on both tiers,
  the five year-old ones survived, and it reported success. A retention rule that
  deletes this month and keeps last year.

  A collection nothing tiers is unaffected: it lives on the fast tier alone and a
  TTL index there means exactly what it says. A `collMod` that turns an existing
  index into a TTL one is the same instruction and is refused the same way. If a
  tiered collection really needs a retention rule, put the index on the archive
  yourself, and be clear that the archive is what keeps this data.
- **The router says when a tiered collection is not the same collection on both
  tiers.** The fast tier's copy is created with the archive's options before
  anything inserts into it, which is what keeps a collation and a validator
  working after an adoption. Two things get past that: your own insert can
  create the collection first, and a `collMod` through the router reaches the
  fast tier alone, because DDL is not mirrored.

  Then which tier answers decides the answer. Measured on a collection the
  archive created with `{locale: "en", strength: 2}` and a fast tier created by
  an insert: `countDocuments({name: "alice"})` is 2 on one and 1 on the other,
  and nothing failed. A collation cannot be added to a collection that already
  holds documents, so the fix is to recreate the fast tier's copy; a validator
  can be fixed with `collMod` on whichever tier is behind.
- **A validator applies to what your application writes, not to what moves
  between the tiers.** The fast tier enforces it at the moment of the write, the
  way your cluster always did. What reaches the archive afterwards is a copy of
  a document that already exists, plus the field this project adds to record a
  deletion, so document validation is bypassed on the way in. The same goes for
  a document copied up to the fast tier: it is already in the archive, and a
  validator added after it was written would otherwise keep it from ever being
  served from the fast tier.

  Measured with an ordinary `$jsonSchema` carrying `additionalProperties: false`.
  Before this, the first delete of a document in that collection made the archive
  refuse the tombstone, the batch never succeeded, and the feeder reopened its
  change stream on the same event fifteen times in a few seconds. Mirroring
  stopped for every namespace behind it, so a write to an unrelated collection
  never reached the archive at all.

  The feeder's account therefore needs `bypassDocumentValidation` on the archive.
  `dbOwner`, `readWrite` and `root` all carry it.
- **The catalog names the tiered collections the fast tier does not hold.**
  `listCollections` and `listDatabases` are answered by the fast tier, because
  that is where your collections live and where your writes go. For a tiered
  collection that is half the picture, and a tiered collection with no copy on
  the fast tier used to be reported as not existing while every read of it
  worked. Measured on the pilot with `sample_supplies.sales` held only in the
  archive: `countDocuments` answered 5,000, `findOne` returned a document, and
  `show collections` came back empty. Drop the database instead and `show dbs`
  omitted it while `use` and the collections and the documents were all still
  there.

  That is the ordinary state of an adoption, not an exotic drop: the fast tier
  starts empty and every tiered collection is in that position until something
  fills it. Nothing failed, which is the problem. Anything that finds its work
  by asking what exists finds nothing, so `mongodump` backs up an empty
  database, an ORM sees no table, and a BI tool drops the collection from its
  model.

  Both listings now include what the archive holds and the configuration tiers.
  Untiered collections and views stay out, because a read of one goes to the
  fast tier and comes back empty and listing it would be a lie; the router names
  those at startup instead. The added database entries carry `sizeOnDisk: 0`,
  for the same reason the stats above report one tier. What a client is allowed
  to see is decided by the fast tier, on that client's own connection, so a
  listing never names something the fast tier withheld. Asking is also what
  repairs it: the router creates the collection on the fast tier with the
  archive's options and indexes, which is what lets it be asked for its stats.

  `dbStats` is still the fast tier's throughout, so its `collections` counts
  what the fast tier has rather than what the catalog now lists.
- **TLS is handled, in both directions.** `router.tls.cert` and `key` make the
  listener speak it, `ca` checks client certificates, and
  `require_client_cert` insists on one. Whether the fast tier needs TLS is read
  from `hot.uri`, where it is already written for the driver, rather than
  configured a second place for the two to drift apart: the raw connection this
  proxies bytes over is wrapped the same way the driver wraps its own. The
  startup line says which halves are encrypted, because an operator who has just
  moved an application onto this address needs to know it still speaks what the
  application expects.

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
every `feeder.position_every` as one small document with no resume token at
`w:1`, and the durable checkpoint goes back to meaning only what it says.

**What is left is two quantisations, and both were larger than they needed to
be.** The feeder can only say "everything written up to here is mirrored" when a
change stream poll comes back empty, so a fallback read waits for the next poll.
And the router used to re-read the position on its own timer, every 25 ms, with
each waiting read consulting and refreshing a reading it reused for 20 ms, which
put a second interval in front of one it was already waiting out.

The router's side is gone. One goroutine polls while anything is waiting and
wakes every waiter the moment the position moves, so the cost is a fixed stream
of reads of one small document rather than one per waiting client, and the wait
is as short as the position is new. The feeder's side is a cadence to choose,
measured on a read that falls through to the archive:

| feeder publishes every | p50 | p95 |
|---|---|---|
| 100 ms | 106 ms | 115 ms |
| 50 ms | 59 ms | 77 ms |
| **20 ms**, the default | **27 ms** | 35 ms |
| 10 ms | 21 ms | 52 ms |
| 5 ms | 17 ms | 41 ms |
| barrier off, for reference | 4 ms | 12 ms |

A fallback read costs about the cadence plus seven milliseconds. Below 20 ms it
stops paying for itself: ten buys six milliseconds and makes the tail worse, so
20 ms is the default and `feeder.max_await` with `feeder.position_every` are
there for a deployment that would rather spend the round trips.

**What that is worth to an application**, measured on the shop against a
5,000,000 order archive with the fast tier empty, so nearly every read falls
through. Three runs of each, alternating, on the same data:

| | 100 ms | 20 ms |
|---|---|---|
| throughput | 56, 57, 63 /s | **73, 82, 98 /s** |
| count a customer's orders, p50 | 148, 154, 155 ms | **69, 84, 89 ms** |
| the same, p99 | 308, 313, 629 ms | **257, 310, 425 ms** |

**Report the spread, because a single pair of runs here says almost nothing.**
One machine carries six mongods, the router, the feeder and the shop, and the
same configuration has come back at 46/s and at 98/s. Alternating the arms and
repeating them is what makes the two ranges mean something: they do not overlap,
so the worst run of the faster cadence still beats the best run of the slower
one. A single pair had said 60 against 71, which understated it, and a different
single pair would have said the opposite. There is no
point setting `position_every` slower than `max_await`, because the position can
only move when a poll finds the stream empty.

**What the wait compares was the other half of getting this right, and it was
wrong for longer.** "How far mirroring has got" was the feeder's own wall clock,
and "the instant the read began" is the router's, so on separate hosts the
barrier was only as sound as the skew between two machines nothing keeps in
step. The dangerous direction is silent: a feeder whose clock runs a little
ahead publishes a position past writes it has not mirrored, and a read that
trusts it misses a write that had already returned to the client. Worse, the
feeder stamped that clock whenever a change stream poll came back empty, which
proves the server had nothing more to send when it answered and not that a write
committed a millisecond earlier had been delivered. The shop test caught the
consequence once in 358 order placements: placing an order moved that customer's
count from 18 to 18.

Both halves now use the replica set's own logical clock, which neither daemon
has an opinion about. The feeder publishes the cluster time it has mirrored
through, taken from an applied event or, when the poll is empty, from that
poll's post-batch resume token, which is MongoDB saying "nothing exists up to
here" rather than anyone's guess. The router keeps the newest `operationTime`
the fast tier has reported, which every reply carries and which is therefore
free, and any write already acknowledged to a client happened at or before it.
A fallback read has to clear both bars, never either, so requiring the cluster
time can only make the barrier stricter, and it falls back to the clock alone
against a feeder from before this.

**A caller's own deadline bounds all of this, and it is spent once.** The wait
takes at most half of `maxTimeMS` and the archive read is given what is left,
rather than the whole of it a second time. Measured before that: a client asking
for 800 ms with mirroring behind waited 300 ms at the barrier, handed the
archive 800 ms of its own, and got a successful answer after 1,117 ms. Splitting
it is what stops either half starving the other, because waiting out the entire
deadline honours it and then reaches the archive with nothing to spend, which
would turn a feeder that is merely behind into failed queries. That is the one
thing this barrier exists not to do.

A read of something written milliseconds ago is therefore seen, at the cost of
that wait. A stopped feeder is the one case the barrier cannot hold, and there
the router answers anyway and warns rather than turning a dead background daemon
into failed queries. It notices quickly: the position record carries the cadence
it promised, so four missed publications mark the publisher as gone instead of
making every read wait out the full `archive_wait`.

`router.completeness` chooses the behaviour: `archive` (the default), `strict`
to refuse instead, `permissive` to return the fast tier's short answer.

**An error the archive returns is MongoDB's answer, and it is passed back
unchanged.** It used to be replaced by a generic `CommandFailed` carrying a
sentence about completeness, which is the wrong answer twice over: an
application branches on `e.code` and not on the prose, and a client whose own
query was malformed was told this proxy had refused it. `count`, `distinct` and
aggregations are judged before they run, so those are the paths where it showed:
an unknown pipeline stage came back as 125 where MongoDB says 40324, and a bad
filter as 125 where MongoDB says 2. The codes now match, which `testconform.sh`
compares against an untiered reference for twenty error shapes.

Two kinds of failure keep the refusal, because they are not MongoDB's answer to
the client's command: a failure to reach the archive at all, and an
authorization error, which describes the router's own credentials rather than
the caller's and would otherwise send whoever investigates to the wrong cluster.

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

### Writes that ask for no acknowledgement

A write sent with `{w:0}` carries the wire protocol's moreToCome flag: the
client does not wait, and there is no reply to correct. Every guard above used
to be skipped for those, which is the same shape as a client negotiating
compression and turning them all off invisibly. It is worse in one respect, in
that `{w:0}` is chosen by ordinary applications for ordinary reasons, in metrics
writers and log ingesters, and needs no unusual driver options.

Measured on a collection holding `{_id: 5, name: "alice", tier: "gold",
balance: 100}` in the archive only: an unacknowledged
`updateOne({_id:5},{$set:{balance:200}},{upsert:true})` left `{_id:5,
balance:200}` on **both** tiers, an unacknowledged insert of an `_id` the
archive still held replaced the archived document entirely, and an
unacknowledged `deleteMany` left nine of twelve documents readable after an
erasure that reported nothing. All three are shapes that were fixed on the
acknowledged path long ago and reached again through this door.

The guards run for these writes too. The copy-up and the key claim happen the
same way, so an unacknowledged write now pays that latency, which is a real cost
on a path chosen for speed. Where a guard would have refused, the command is not
performed and nothing is sent back, because there is nowhere to send it.

`testunack.sh` pins all three, along with the control that an ordinary
unacknowledged write still lands on both tiers.

### Replies the client asked to have several of

The same flag exists in the other direction. A client that sets `exhaustAllowed`
on a `getMore` is telling the server it will accept several replies to that one
request, each but the last carrying moreToCome, and the server obliges: measured
against a real mongod, one such `getMore` produced **five** replies.

This proxy reads one reply per request. The other four stayed in the socket, and
the next command on that connection was answered by them. Measured through the
router: the reply to a `ping` sent afterwards came back as `responseTo=75`
carrying documents 4 and 5 of the cursor. With upstream connections pooled that
connection then goes back to the pool, so the next borrower is a different
client, reading somebody else's documents in answer to its own command.

The bit is cleared on the way through, which the wire protocol allows: setting
it never guaranteed more than one reply. An exhaust-capable driver loses a
round-trip optimisation and gets the same documents. This is the compression
remedy again, and for the same reason: a flag the client sets for itself must
not decide whether this proxy can still follow the conversation. As a second
line, a reply that arrives with moreToCome set anyway costs that connection its
place in the pool rather than being handed on.

No driver in the test harness can set the bit, so `test/exhaust` is a client
written to do it.

### What decides which documents a filter matches

A multi-document write on a tiered collection is resolved by asking the archive
the same filter and bringing up what it matches. Same filter is not enough: the
question has to be asked the same way.

Two things decide that and neither was carried across. Variables, because since
MongoDB 5.0 a filter refers to a `let` document as `$$name`; without them the
archive rejects the filter, the copy-up fails and the write is refused, which is
safe and is still an ordinary statement turned into an error. And collation,
because it decides what equal means. Measured on a collection holding `alice`
and `ALICE` in the archive only: a case-insensitive `updateMany` for `ALICE`
modified **one** document where an untiered reference modified two, and reported
success. The copy-up had asked the archive case-sensitively, so the other
document was never brought up.

Both travel with the copy-up now, on the multi-document path, the upsert path,
the bulkWrite path and the `$merge` target rerun, and the fast tier's own
pre-flight check for an upsert asks with the collation too. `testconform.sh`
compares each against the untiered reference.

### An insert the archive cannot be asked about

The check that stops an insert duplicating an `_id` the archive still holds has
to reach the archive. When it could not, the insert was let through.

Measured with the archive stopped: an insert of an `_id` the archive held was
accepted once the lookup timed out, landed on the fast tier, and the feeder
mirrored it over the archived record.
`{_id: 500, v: "archived, precious", tier: "gold"}` came back as
`{_id: 500, v: "clobber"}`. Nothing anywhere said so.

It is refused now, and says why. The exception is a fast tier claiming all of
history: there is nothing archived it does not also hold, so its own `_id` index
is the whole answer and the archive has nothing to add. The refusal costs the
driver's server selection timeout first, because that is how long the lookup
takes to fail, so an archive outage is felt as slow writes before it is felt as
refused ones.

The unique key registry fails the other way on purpose, and the difference is
worth stating. It lives on the fast tier, its failure means two live documents
could share a value that should be unique, and refusing every insert because a
bookkeeping collection is unreachable is worse than that. Losing an archived
document is not in the same class.

`testchaos.sh` stops every archive member and checks all three: the insert is
refused, nothing lands, and the archived document is still there afterwards.

### A change stream that asks for the document

`fullDocument: "updateLookup"` tells the server to fetch the document as it
stands when the event is read, and that lookup happens on the fast tier, which
is the one place that does not hold every document. An update to a document that
has since been evicted came back with `fullDocument: null`.

Null does not mean "somewhere else", it means gone. Measured: a change stream
reported an update with `fullDocument: null` while a read of that same document
through the router returned it. An application watching a collection to keep a
cache or a search index in step would have deleted the entry, and the document is
still there.

A null lookup is asked of the archive now, and answered from it when the archive
holds the document live. A document the application really deleted is tombstoned
there, so it stays null, which was the right answer all along, and
`testchange.sh` checks both.

The copy may be a moment behind the fast tier, because the archive trails it.
`updateLookup` never promised the post-image: it fetches whatever the document is
when the event is read, which may already be a later version or nothing at all.
A copy a moment old is within that, and "deleted" is not.

### Renaming a tiered collection

A rename moves two collections, not one, and the feeder ignores renames on
purpose: replaying collection-level changes is what would let an accident on the
fast tier reach the archive. So a rename used to be refused outright, with the
remedy spelled out for the operator to run by hand. Refusing is safe and it is
not the same as working, and rotating collections by name is ordinary.

It is done properly now, when it can be done safely:

- **Both names have to be tiered.** The namespace list is configuration, and a
  rename into a name it does not carry stops the tiering with nothing saying so.
- **Mirroring has to be caught up**, within two seconds. A write to the old name
  that has not reached the archive has nowhere to go once the archive collection
  is renamed. The window cannot be closed entirely from here, so it is made
  small and stated rather than assumed.
- **A `dropTarget` that would destroy archived documents is refused.** On the
  fast tier that flag is the client's business; in the archive it is the one
  thing no fast tier command may do.

The fast tier renames first, forwarded exactly as the client sent it, so its own
authorization decides whether it happens at all. Only once it has said yes does
the archive follow, and the completeness watermark and the unique key claims
follow with it, because both belong to the collection rather than to its name.
Residency records are dropped rather than rewritten, since they carry the
namespace inside their own id: the cost is that a document promoted moments ago
may be evicted early, which is a delay and not a loss.

If the archive rename fails after the fast tier's succeeded, the tiers are split
and the log says so at ERROR with the exact command to put it right. That is the
one case where an operator has to finish the job.

### An explain must not do the thing it explains

An aggregation can carry `explain: true` in the command itself rather than being
wrapped in an `explain` command, and then it plans rather than runs. Every guard
on that path does something on the pipeline's behalf: the merge target guard
copies archived documents up, and the completeness guard runs a writing pipeline
itself when the input is archived.

Measured: `aggregate` with `$out` and `explain: true` wrote nothing on mongod,
and through the router it wrote all ten documents into the target and returned no
plan at all. The client asked what would happen and was told nothing, having had
it happen.

An explained aggregation is forwarded untouched now and its plan is annotated on
the way back, which is what the wrapped form already got.

### A client that compresses uninvited

The compressor list is removed from the handshake in both directions, so the
negotiation never succeeds and a conforming driver never compresses. A client
that compresses anyway is a different question, and the answer used to be that
the message was logged and forwarded, which is every guard off for that command.

Measured against a collection whose documents were only in the archive: an
insert of an `_id` the archive still held was refused with E11000 when sent
plainly, and accepted when the same insert was wrapped in OP_COMPRESSED, landing
on the fast tier for the feeder to mirror over the archived record. That is the
original compression finding still live, reached by ignoring the negotiation
rather than by winning it.

A compressed message is now refused, in an uncompressed reply that names the
remedy, and the connection ends with it: there is no way to know whether what
follows is readable either. `test/compressed` is the client that compresses
uninvited, since no driver in the harness will.

### Whose cursor is it

A cursor over an archive answer is this router's, not the fast tier's, so it is
served from a registry here. That registry belongs to the router rather than to
one connection, which is right: a real MongoDB cursor is server-scoped, and a
driver's pool sends the `getMore` from whichever connection is free. Nothing else
scoped it, and the ids ran in sequence from a fixed start, so the first cursor
was always 2^62+1.

Measured with two connections to one router: the second, which had opened
nothing, sent a `getMore` for that id and was given the first client's documents
along with their real namespace. Naming a different collection in the command
changed nothing, because the batch comes from the cursor and not from the name.
That is also what made it reach past authorisation, which judges the name the
client supplied.

Three things now hold, each of them mongod's own rule:

- **Ids are random**, in the top quarter of the range so they stay clear of the
  server's own, so no id can be counted off.
- **A cursor belongs to whoever opened it.** The user when this router knows it,
  and the session whenever the client sends one, which every driver since 3.6
  does. mongod: "If authentication is enabled, you can only run getMore against
  cursors you created. Additionally, for cursors created inside a session, you
  cannot call getMore outside the session." With neither a user nor a session,
  the cursor is scoped to the connection, which is stricter than mongod and only
  reachable with no authentication at all.
- **The collection named has to be the cursor's.** mongod has refused a mismatch
  since at least 6.0, and the router now returns the same error word for word.

Refusals use mongod's code 13, so a driver sees the error it already handles.
`test/cursorowner` is the client that found this.

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
tier is rebuilt, since it lives there.

**And it has to be pruned, or it grows with history on the wrong tier.** A claim
is permanent on purpose: evicting a document takes its index entry off the fast
tier, and the claim is what remembers the value was taken. Nothing removed one
whose document had been deleted from both tiers. Measured on 500 inserts into a
collection with one unique index, then deleting all 500: nought documents on
either tier, 500 claims still there, about 100 bytes each, on the fast tier.

```
bin/cline keys --config cline.yaml --prune              # remove claims nothing holds
bin/cline keys --config cline.yaml --prune --prune-for 10m
```

It asks the question a collision asks: does any live document still hold this
value, in the index's own comparison, one query per tier for a batch of claims. A
claim whose document lives in the archive alone is kept, which is the only reason
the registry exists; a claim whose document has been deleted from both tiers goes,
and that key can be used again. Bounded by `--prune-for` and safe to interrupt: a
claim it did not reach is a claim that stays. Run it on a schedule beside
whatever else maintains the deployment. If it finds one key held by two live
documents it says so and picks no winner, because that means the constraint was
already broken.

Every path that can write a unique value goes through the registry: inserts,
updates, replacements, `findAndModify`, `bulkWrite`, and an upsert creating a
document. An upsert is checked whatever its update document appears to touch,
because the document mongod creates takes its key from the filter's equalities
too: `updateOne({email, date}, {$setOnInsert: {name}}, {upsert: true})` writes a
key the update never mentions. Measured on the pilot before that was true, on a
key a live archived comment held: the write was accepted, the archive refused to
mirror it with `E11000`, and a read of the key answered 1 where two documents
existed.

**Sparse and partial unique indexes are enforced too**, and the condition is
evaluated by the server rather than reimplemented here: deciding for itself
whether a document is indexed is exactly the kind of guess that produces a wrong
answer. A compound sparse index still covers a document carrying any one of its
fields, so only one missing all of them is exempt, and that part is decided
locally.

The filter costs one question per batch, asked of the fast tier. It used to be
one per document: measured, 300 inserts into a collection with a partial unique
index took 330 ms against 50 ms for the same insert with a plain one, and now
take 75 ms. An index whose filter cannot be evaluated is not enforced across
tiers rather than enforced on a guess, and the router says so.

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

### Indexes go up, never down

The archive is expected to hold far fewer indexes than the fast tier. Index trees
describe the whole collection, which is the cost tiering exists to avoid, so
carrying every one of them on the archive gives most of the saving back. Nothing
copies an index from the fast tier down to the archive, and nothing should.

**Upward is the opposite case, and it was wrong for the whole of adoption.** The
fast tier is filled by copying documents, and an index is not a document, so a
tier that has just been built holds everything the application asks for and no
way to find any of it except by `_id`. That is the shape of every adoption: the
existing cluster becomes the archive carrying the application's indexes, and the
new fast tier starts bare.

Measured on a 5,000,000 order shop, archive 1.32 GB against a 1 GB cache, hot
window 30 days:

| | fast tier bare | indexes built |
|---|---|---|
| throughput | 35/s | **78/s** |
| a customer's order history | 501 ms | **160 ms** |
| read an order by date | 543 ms | **159 ms** |
| a monthly revenue report | 1.23 s | **680 ms** |
| the warm itself | 672 s | **639 s** |

Day one, before any warm, was 62/s. So warming a bare fast tier *cost* 44% of
the throughput: it moved reads onto a tier that scans 411,000 documents while
the archive it was relieving was seeking an index. Building them costs nothing,
because an index built on an empty collection is free and one built on a full
one is a scan.

Three paths do it, through one implementation in `internal/promote`: the warmer
before it fills a namespace, the router the first time it copies a document up,
and the router at startup for every namespace it manages that the archive
already holds. Once built, never again.

**A view is stranded like a collection and cannot be fixed like one.** The
application's views live on the existing cluster; the fast tier has none of
them, so reads of them return nothing at all. Measured on an adopted view over a
tiered collection: five documents on the original cluster, zero through the
router, no error. The router names views separately at startup because the
advice differs, and the advice for a collection cannot be followed for a view:
tiering one is meaningless, so the feeder refuses it. Recreate them on the fast
tier with `createView` and tier the collections they read from.

**A view created through the router is given to the archive as well**, and that
buys something beyond a rollback working. A read through a view used to be
refused whenever the collection underneath it was incomplete, because the archive
held the documents and not the definition; when it holds both, the read has a
correct answer instead of an error. Measured on a view over a collection the fast
tier held one document of, with the archive holding three: refused before,
answers two now, which is what the archive's own view answers.

Only when the two definitions are identical, and that is the whole of the
condition: the collection read, the pipeline and the collation all matching. A
view whose pipeline differs between the tiers answers a different question, and
answering the archive's version of it would be exactly the silent difference this
system exists to prevent. A view the archive does not have, or has differently,
is still refused with the reason.

**The accounts are the other thing the new tier does not have.** The existing
cluster carries the application's users; the fast tier is a new replica set and
carries none of them. The router authenticates against the fast tier, so on day
one every connection is refused with MongoDB's ordinary `Authentication failed`
and nothing connects that message to the tier missing the account. The refusal
is correct; the silence is the failure. The router now names them once at
startup, and creating them on the fast tier with the same names, passwords and
roles is the whole fix.

**The collection comes before its indexes, for the same reason and with worse
consequences.** A collection is more than its documents, and everything about it
other than documents arrives on the fast tier only if something puts it there.
In an adoption nothing does: the fast tier's copy is brought into existence by
the first insert, which carries no options at all. Two of those options decide
answers rather than performance.

Measured on an adopted collection created with `{locale: "en", strength: 2}`,
with the same three documents present on both tiers:

| | the application's own cluster | through the router |
|---|---|---|
| `countDocuments({name: "alice"})` | 2 | **1** |
| a write the validator rejects | refused | **accepted** |

No error and no warning in either case. A query that has always matched
case-insensitively quietly stops, and a rule the application has relied on for
years quietly stops applying, on the day it is adopted.

So the router creates the fast tier's copy the way the archive has it, before
anything inserts into it: the default collation, the validator and its level and
action. `capped` and `timeseries` are deliberately not carried, because a capped
fast tier would remove its own oldest documents and the feeder would read those
removals as the application deleting them.

The third exists because the first two are both paths that fill the fast tier
*from the archive*, and a tiered collection the application only ever writes to
is filled by neither. Nothing warms it, no read of it falls through, so the
first thing to create it on the fast tier is the application's own insert, and
an insert is not a copy-up. Startup is both the correct moment and the cheapest:
correct, because a unique index built after a duplicate has been accepted is too
late and will not build; cheapest, because this is the collection the fast tier
has never held.

It is not only speed. The router reads the **fast tier's** unique indexes to
know which values it must protect across the tiers, so a bare fast tier means
neither mongod nor the registry is enforcing one, and a constraint the
application has relied on for years quietly stops holding on the day it is
adopted. Measured on an adoption whose archive carried a unique index: a value
the archive already held, and the same new value twice, were all three accepted.
`cline keys --rebuild`, the documented step for exactly this, does not save you
either, because it reads the constraint from the same place and claimed nothing.

What follows is worse than the duplicates. The fast tier then holds documents
the archive's index refuses, so the feeder cannot mirror them: it reopens its
change stream from the same checkpoint once a second, for ever, and mirroring
stops for **every** namespace behind it. An order written after that never
reaches the archive at all.

A TTL index is the one that does not travel. Expiring documents on the fast tier
would have the feeder read those expiries as application deletes and tombstone
the archive's copies, so an index whose job is to remove data would reach into
the tier that exists to keep it. The archive goes on expiring its own copies.

**And that last sentence used to be the whole of the warning.** An adoption
inherits the application's TTL index on what is now the archive, so the tier that
was adopted to keep history goes on deleting it, on a schedule nothing here set.
It is invisible from the fast tier: a read the fast tier answers whole never asks
the archive, and a read that falls through comes back short with no error on it.
So it is now said out loud, in both places an operator looks. The router and the
warmer warn once per namespace, naming the index, the field and the interval, and
`cline indexes` reports it as its own finding:

    shop.orders
      fast tier  4 index(es), archive  5
      the ARCHIVE expires documents of this collection:
        created_ttl                        {createdAt: 1}   (after 720h0m0s)
      that history leaves the tier that exists to keep it, and a read falling
      through is then short by whatever expired, with no error on it

Reported rather than dropped. The expiry may be exactly what the deployment
wants, an index the application created is not ours to remove, and dropping it
does not bring back what has already gone.

**Every index build this system starts commits on a majority**, not on every
voting member, which is mongod's default. The default is what lets one member
stop an index build: it never commits, `createIndexes` never returns, and
everything else on that cluster carries on working. The router builds the fast
tier's indexes before it opens its listener, so what hangs is the start of the
proxy, with nothing in the log to say why. Found on this project's own cluster
after one member came up on the wrong address: a build sat nineteen minutes and
read exactly like a hung proxy.

Measured afterwards on a three member 8.0.26 set with a thousand documents:

| commit quorum | members up | build |
|---|---|---|
| `votingMembers` (default) | 3 of 3 | 164ms |
| `votingMembers` (default) | 2 of 3 | did not return in 90s |
| `majority` | 2 of 3 | 117ms |

One detail decides whether you ever see this: a build on an **empty** collection
skips the two-phase protocol and needs no votes, so the shape that stalls is a
fast tier that already holds documents, which is every fast tier after the first
warm.

A majority is what the rest of this system already waits for, and a member behind
builds the index when it catches up, the same as it applies everything else. The
startup build is bounded as well, at five minutes per namespace, after which the
router says which namespace it gave up on and what is not being enforced, and
goes on starting: an application in front of a proxy that will not start is down.

The warmer deliberately does not have that bound. It builds a namespace's indexes
before it fills it, and a unique index built after the duplicates have been
written will not build at all, so a warmer that gave up and filled anyway would
trade a stalled pass for a constraint that never comes back. A stalled pass is
visible: `thermocline_daemon_seconds_since_pass{daemon="warmer"}` climbs, which
is alert 5.

**The consequence is that a read which falls through runs with whatever indexes
the archive happens to have.** For a filter that is slower. For a sort it can be
worse: past the server's sort memory limit a blocking sort does not degrade, it
fails. Archive reads therefore ask for `allowDiskUse`, so a sorted query that the
fast tier answered from an index becomes slow rather than broken. An operator who
would rather refuse expensive fallback reads than run them has
`router.completeness: strict`, which refuses them all.

`hint` is dropped for the same reason: it names an index the archive is not
expected to have. **Except when the hint is not only about the plan**, and there
are two such cases, both of which used to produce a wrong answer quietly.

A sparse index holds only the documents carrying its fields, and a partial index
only the ones its filter matches, so hinting either narrows which documents are
looked at. Measured on PSMDB 8.0, twenty documents of which six carry a field,
with a sparse index on it: `find({})` returns twenty and
`find({}).hint(thatIndex)` returns six. Dropping that hint on the way to the
archive therefore answered a different question and reported it as success. What
the hint removed is put back as a filter instead: `$exists` on the index's
fields, or the index's own `partialFilterExpression`. Both are predicates the
archive can evaluate whether or not it carries the index, which is what keeps
this from requiring the tiers to agree about indexes.

That translation is only written down where it is exact. "Holds every document
carrying at least one of its fields" is the rule for a sparse index whose key is
all directions, and it is not the rule for one carrying a `2dsphere` or `text`
component: measured with `{loc: "2dsphere", n: 1}` sparse over three documents,
hinting it returned the two carrying `loc` and left out the one with `n` and no
`loc`, which an `$or` over both fields would have put back. For those the hint
is forwarded instead, and MongoDB is the authority: the archive either carries
the index and answers exactly, or names the index it is missing.

`min` and `max` are bounds on an index key, so they mean nothing without the
index that defines it, and MongoDB has required a hint alongside them since 4.2.
Dropping it there did not cost a plan, it turned a working query into
`NoQueryExecutionPlans`. The hint travels with them now, and if the archive does
not carry that index the caller gets MongoDB's own complaint naming it, which is
a true statement about a real divergence and one the report below already
covers.

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

**With `router.terminate_auth` on, none of the above applies and this section is
the whole of the protection.** The upstream connections belong to the router, so
the fast tier will do whatever they ask, and the router decides instead. It
denies by default: a command whose required privilege is not in the mapping is
refused by name rather than guessed at, which is why an unusual client meets
refusals rather than a hole.

Judging a command by the collection it was sent to is not enough, because a
command can name more collections than its own. `$lookup`, `$unionWith` and
`$graphLookup` read another one, `$out` and `$merge` write one, an `explain`
carries a whole command inside it, and a `bulkWrite` names its collections in
`nsInfo` and refers to them by position. Every one of those is judged
separately, the way mongod judges it: find on each collection read, insert and
remove on an `$out` target, insert and update on a `$merge` target, and for an
explain exactly what the command inside it would need.

Measured against the same user sending the same command straight to the fast
tier, with a user holding find on one collection and nothing else: before those
namespaces were judged, a `$lookup` returned the other collection's documents,
a `$unionWith` returned its rows, and `$merge` and `$out` **overwrote it**,
leaving `1:-1,2:-1` where mongod had left `1:100,2:200`.

**A write that asks for no acknowledgement is still a write.** An unacknowledged
write, `{w:0}`, carries the wire protocol's moreToCome flag and the client never
waits for a reply. There is nowhere to put a refusal, and that is a reason not
to perform the write rather than a reason to skip asking. It is judged like any
other, and where the answer is no the command is dropped, which is what `{w:0}`
already means to an application: it asked not to be told.

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

### What to alert on

Nine signals, and the reason each one is worth waking somebody for. Everything
here is already published; what was missing was saying which numbers mean
something.

**1. `thermocline_panics_total` above zero. Page.** A panic is a defect, with a
stack in the log, and the connection it happened on was closed. Nothing about
the tiering is wrong at that moment, but something the router was asked to do it
could not, and the next client to ask will hit it too.

**2. `thermocline_stale_reads_total` above zero. Page.** A read fell through to
the archive and was answered before mirroring reached the moment the read
started, so it may miss a write that already returned. Every other counter here
is about work; this one is about a wrong answer.

**3. `thermocline_mirroring_lag_seconds` above the eviction notice's lifetime.
Page.** Past that point the evictor stops, which is the safe behaviour, and a
delete the feeder reaches after the notice has expired is read as the
application's own and the document is lost from both tiers. The config refuses
the combination that would allow it, so this alert is about the lag getting
there. Warn at a minute, page at ten.

**4. `thermocline_refused_total` rising. Investigate, do not page.** The router
would not answer a command, and every refusal names its reason in the log. A
steady trickle is an application asking for something this system does not do; a
step change means completeness claims no longer cover what is being asked, which
is usually a warm that has not run.

**5. `thermocline_daemon_seconds_since_pass` climbing. Page at ten minutes.** A
daemon that has stopped shows this climbing rather than going silent, which is
the whole reason it is a gauge from the last published report rather than a
heartbeat. The feeder stopping is the serious one: mirroring stops, the evictor
halts behind it, and every fall-through read pays the freshness barrier.

**6. `thermocline_daemon_gauge{gauge="clock_skew_seconds"}` past a minute.
Investigate; past the eviction notice's lifetime, page.** An eviction
announcement is stamped by the evictor and expired by the archive's TTL monitor,
so an evictor whose clock runs behind has its announcements expiring early. Far
enough behind and they are gone before the feeder reads the delete they explain:
the feeder takes that delete for the application's own, the archive tombstones
its copy, and the document is lost from both tiers. Negative is behind, which is
the direction that loses data. The evictor measures this against the archive on
every pass.

**7. A series that is absent, which is the failure no threshold catches.** Every
alert above compares a number, and a daemon that never started publishes no
number at all: the evictor's `seconds_since_pass` climbs when it stops and does
not exist when it was never run, so `> 600` never fires. Name the daemons you
expect and alert on their absence.

Mirroring is handled in the export instead, because it is the one that matters
most: with no position and no checkpoint, `thermocline_mirroring_lag_seconds`
reports `+Inf` rather than vanishing, so the first alert anyone builds fires for a
feeder that has never run as well as one that has stopped. The router's own
absence is the scrape target going down, which is `up == 0` on the job.

**8. `thermocline_claim_grace_reads_total` above zero, if you set
`router.claim_grace`. Page.** Every read it counts was answered from a
completeness claim the router could not check, because the archive was
unreachable. That is the trade you asked for, and it is also the only signal
that says the trade is being taken: the answers are correct if the archive is
down and short if only this router is cut off, and the counter cannot tell those
apart. Find out which it is. On a deployment that leaves the grace off this
counter stays at zero for ever, so the alert costs nothing.

**9. `thermocline_unauthorized_total` or `thermocline_unreadable_total` above
zero. Investigate.** The first is a client reading a collection it has no
permission for, refused. The second is a message the router could not inspect,
which means it was forwarded unguarded; a compressed connection is the cause, and
the remedy is removing the client's compressors.

```yaml
groups:
  - name: thermocline
    rules:
      - alert: ThermoclinePanic
        expr: increase(thermocline_panics_total[5m]) > 0
        labels: {severity: page}
        annotations:
          summary: "the router recovered a panic; there is a stack in its log"
      - alert: ThermoclineStaleRead
        expr: increase(thermocline_stale_reads_total[5m]) > 0
        labels: {severity: page}
        annotations:
          summary: "an archive read was answered before mirroring caught up"
      - alert: ThermoclineMirroringBehind
        expr: thermocline_mirroring_lag_seconds > 600
        for: 5m
        labels: {severity: page}
        annotations:
          summary: "mirroring is far enough behind that eviction has stopped"
      - alert: ThermoclineDaemonStopped
        expr: thermocline_daemon_seconds_since_pass > 600
        labels: {severity: page}
        annotations:
          summary: "{{ $labels.daemon }} has not finished a pass in ten minutes"
      - alert: ThermoclineRefusalsRising
        expr: increase(thermocline_refused_total[15m]) > 10
        labels: {severity: ticket}
        annotations:
          summary: "the router is refusing commands; the log says why"
      - alert: ThermoclineClockBehind
        expr: thermocline_daemon_gauge{gauge="clock_skew_seconds"} < -60
        for: 5m
        labels: {severity: page}
        annotations:
          summary: "{{ $labels.daemon }}'s clock is behind the archive's; eviction notices expire early"
      - alert: ThermoclineClockAhead
        expr: thermocline_daemon_gauge{gauge="clock_skew_seconds"} > 60
        for: 5m
        labels: {severity: ticket}
        annotations:
          summary: "{{ $labels.daemon }}'s clock is ahead of the archive's; the windows differ from the configuration"
      - alert: ThermoclineDaemonMissing
        # One per daemon you run: absent() needs the label spelled out, and an
        # evictor nobody configured is not a fault. This is what a daemon that
        # has never started looks like, where seconds_since_pass has nothing to
        # compare.
        expr: absent(thermocline_daemon_seconds_since_pass{daemon="evictor"})
        for: 10m
        labels: {severity: page}
        annotations:
          summary: "the evictor has never reported; check whether it is running at all"
      - alert: ThermoclineRouterDown
        expr: up{job="thermocline"} == 0
        for: 2m
        labels: {severity: page}
        annotations:
          summary: "the router is not answering its metrics endpoint"
      - alert: ThermoclineAnsweringWithoutTheArchive
        # Only ever fires where router.claim_grace is set. Reads are being
        # answered from a claim that cannot be checked: correct if the archive
        # is down, short if only this router is cut off from it.
        expr: increase(thermocline_claim_grace_reads_total[5m]) > 0
        labels: {severity: page}
        annotations:
          summary: "the router is answering from an unverifiable completeness claim; is the archive down, or is the router partitioned?"
      - alert: ThermoclineUnguardedOrUnauthorized
        expr: increase(thermocline_unauthorized_total[15m]) > 0
          or increase(thermocline_unreadable_total[15m]) > 0
        labels: {severity: ticket}
        annotations:
          summary: "a client read what it may not, or sent what could not be inspected"
```

Two things are deliberately **not** alerts. `copied_up_total` rising means the
eviction boundary is cut deeper than the application reads, which is a tuning
conversation rather than an incident. And `duplicate_keys_total` counts writes
refused for duplicating an archived key, which for an application that relies on
`E11000` is ordinary behaviour working correctly.

## Which queries are worth caching

The router sees every read and every answer, which is what a cache advisor
needs. For each read it records three things: the **shape** of the query, a hash
of its **parameters**, and a hash of the **result**. Nothing of the query's own
data is kept, only hashes.

    db.users.countDocuments({country: "UY"})
    db.users.countDocuments({country: "AR"})

is one shape asked twice, not two queries, and the split is what makes the
question answerable. From it the router works out, in this order:

1. Which shapes are asked repeatedly at all. A query asked once is not a
   candidate however stable its answer is.
2. Among those, which parameters repeat. A shape asked a thousand times with a
   thousand different values has no repetition to exploit, and saying so is a
   finding.
3. For those, how long the same result kept coming back for the same
   parameters.
4. A suggested TTL, ten per cent inside that, so the suggestion is always
   within the evidence for it.

`cline cache` prints it, most valuable first, ranked by how often a query is
asked weighted by how long each answer would survive:

    SUGGEST   STABLE    ASKED   VALUES QUERY
    4m30s     5m0s      312     3      count shop.users query={country:?}
    1.4s      1.6s      880     47     find shop.orders filter={customer:?} sort={at:?}

A count that returned 3,512,345 for five minutes, then 3,512,344 for fifteen,
then 3,512,346 for ten is stable for five and suggested at four and a half: the
shortest stretch decides, because a query that usually holds for an hour and
sometimes for a second is a query that changes every second as far as anybody
reading it is concerned.

**The measuring runs whether or not the caching does.** The advice is the reason
to turn caching on, so it cannot require caching to be on already. With
`router.cache.enabled` the same measurements drive the TTLs. The dashboard shows
the same table, so the decision can be made from the page rather than the
command line.

**All of it can be turned off.** The dashboard is off unless `router.dashboard`
names an address; the caching is off unless `router.cache.enabled`; and the
measuring, which costs a hash of every reply and a bounded table of shapes, is
off with `router.cache.measure: false`. A router with all three off does exactly
what it did before any of this existed.

### What it costs, and what bounds it

`max_queries` bounds how many queries are watched and `max_bytes` how much the
held answers may occupy, because counting entries is not a memory bound: a
thousand replies of a megabyte each is a gigabyte, and nothing in a count says
which of those it is. A single answer larger than 256 KB is learned from and
never held, since a large result is both less likely to repeat and more
expensive to be wrong about.

**Nothing is learned from an answer the cache produced itself.** The reply goes
back through the same funnel every answer does, and that funnel learns from what
it sends, so every hit was being recorded as a fresh observation of the same
value: the entry's age was reset and a query asked steadily was never re-fetched.
Measured with a two second ceiling and thirteen seconds of reads, the router
asked the fast tier four times and served forty one from memory. Now it asks
eleven times, which is what a two second TTL means. Stability is measured against
the database, never against what this already believed.

### What the cache will not do

A cache is the one thing here that can answer without asking, so what it refuses
matters more than what it serves. Never cached: anything inside a transaction,
because a transaction reads a snapshot and an answer from before it began is not
in that snapshot; a read concern naming a cluster time, or asking for snapshot or
linearizable, because those are promises about a position in an oplog or a
confirmation from a primary; and a reply that leaves a cursor open, because half
an answer is not an answer.

**A write through the router drops what was learned about that collection**,
rather than waiting out a timer. That is what makes this correct for the writer
rather than merely fast: the ordinary pattern of writing and then reading the
same collection is right immediately.

**And a write by anybody else drops it too.** The fast tier publishes every
change it makes and this system is built on reading that, so with the cache on
the router follows the same change stream and drops the answers a write made
stale, whoever made it: another application, a migration script, a person in a
shell. Measured: a client's cached answer of 31 became 32 within a second of
another connection inserting a document straight into the fast tier, without
waiting out the TTL. That is the difference between a cache that expires and one
that is kept honest, and it costs one change stream while the cache is on.

The learned TTL is what remains: a backstop for the moments before the stream
catches up, bounded again by `max_ttl`.

**The key carries who is asking**, or one client would be served another's
documents, which is a bypass this system has had once already. With
`terminate_auth` on that is the verified user, and answers are shared between
that user's connections. With authentication proxied the router does not know
who the client is, so the connection is the identity, and never one client with
another's answer.

**That costs a pooled driver almost all of it,** which is worth being plain
about. A pool spreads repeats across its connections, so the second asking of a
question usually lands somewhere the first was never stored. Measured with
`test/store`, a shop of 3,000 orders read through five connections with the
cache on: **7 shapes, 378 lookups, 0 hits.** The same run with
`terminate_auth` on, where the identity is the user and the pool shares one
cache: 5 hits in 378.

Five is not a rounding error away from zero, it is the same answer. The cache
serves a query repeated with the *same parameters*, and a shop asking about a
different customer each time never repeats one. What it is for is the workload
that does: a dashboard refreshing the same figures, a report run on a timer, a
hot lookup every request makes with the same argument. `cline cache` ranks what
your traffic actually repeats, and if the top of that list is thin then this
feature is not for you. It stays off by default for that reason.

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

**A rule with no `date_field` reads each document's age from the timestamp
inside its `_id`, and that only exists in an ObjectId.** Against any other `_id`
type the rule does not merely approximate, it addresses nothing: MongoDB
brackets range predicates by BSON type, so `_id < ObjectId(cutoff)` matches no
integer, string or UUID `_id` at all. Sorting uses the canonical type order;
queries do not.

Obeying such a rule is silent in both directions. The evictor sweeps, reports
success and tiers nothing, for ever. The warmer copies nothing, concludes the
window is done and records a completeness claim over a fast tier it never
filled. Measured on an adopted collection of ten integer-keyed documents: the
warmer reported `warm complete, scanned 0` and claimed it complete for all of
history, and the router then answered `countDocuments({})` with **0** where the
original cluster answers 10. The collection had disappeared and every component
reported success.

Both now refuse such a rule at ERROR, naming the namespace and telling you to
set `date_field`. Refusing leaves no claim, so reads fall through to the archive
and the application keeps seeing its data while the configuration is corrected.

**A claim on a boundary cannot answer a query with no lower bound**, and that is
most of what an application asks. "Complete for everything newer than T" covers
a query that says `at >= T`; it cannot cover `find({customer: X})`, because that
reaches arbitrarily far back and no boundary is far enough. Only a claim on all
of history covers it.

**And it covers `at >= T` only when `at` is the field the rule ages by.** A
boundary claim is measured in one dimension, so a bound in another is not a
bound on the same thing. `find({shippedAt: {$gte: yesterday}})` on a collection
aged by `createdAt` reaches every order ever placed that happens to have shipped
recently, and goes to the archive. Measured on a collection claimed complete for
thirty days, holding a recent order on the fast tier and a two year old one in
the archive, both shipped yesterday: before this it answered 1 where the archive
holds 2.

So a collection whose application filters on one date and whose rules age it by
another gets no benefit from a boundary claim at all. Either age it by the field
the application queries, or claim it complete for all of history.

This is the sharpest edge in an adoption, because everything looks finished. The
warm window is set to the age of the data, the warmer copies every document up
and reports success, and the fast tier holds the entire collection. Measured on
a 50,000 order shop in exactly that state:

| | boundary claim | all of history |
|---|---|---|
| throughput | 284/s | **1,339/s** |
| a customer's order history | 62.74 ms | **2.78 ms** |
| count a customer's orders | 58.54 ms | **3.18 ms** |
| read an order by date | 62.50 ms | **2.44 ms** |
| served by the archive | 40.8% | **0%** |

Nothing was wrong with the fast tier in the left hand column. It held all 50,000
orders and answered 59% of the run. The other 41% went to the archive because
the claim could not be shown to cover them.

**Read the right hand column for what it is: the fast tier holding the whole
collection, which is the state before tiering rather than after it.** As soon as
the archive holds anything older than the warm window, which is the entire point
of having two tiers, the warmer declines that claim and leaves a boundary one,
and the first eviction sweep would raise it anyway. Measured on a collection
warmed with a window narrower than its history: the claim came back as a
boundary, and the customer history query went to the archive before and after
eviction alike.

So the shape to expect in a steady state is this. A query bounded in time, a
report over last month, is answered by the fast tier. A query with no lower
bound, give me this customer's orders, falls through, and no amount of warming
changes that while the collection is genuinely tiered. If your hottest queries
are the unbounded kind, tiering will save you storage rather than latency, and
it is worth knowing that before you adopt rather than after.

**So the warmer makes the stronger claim when it can prove it.** When the
archive holds no live document older than the window just warmed, the fast tier
holds every document there is, and the watermark says all of history. When the
archive does hold something older, the boundary claim is the honest one and the
warmer names what it costs and how to change it.

A fast tier that genuinely holds everything, because it is the original cluster
rather than a new empty one, can also say so directly:

```
bin/cline watermark --config cline.yaml                        # show every claim
bin/cline watermark --config cline.yaml --ns app.orders --complete-all
bin/cline watermark --config cline.yaml --ns app.orders --clear
```

**A claim is checked against both tiers before it is recorded**, because it is
otherwise a promise typed by hand: it says the fast tier holds everything, and
recording one it does not have makes the router answer short with no error. So
this counts what each tier holds and refuses a claim the numbers contradict,
naming both. `--force` records it anyway, for the case where the operator knows
something the counts cannot show, such as a warm still running.

Counting is not free on a large collection, so it is bounded, and past the bound
the claim is recorded **unchecked** rather than refused: blocking a rollout on a
count would be the worse failure. `--count-for` sets that bound, a minute by
default. On the collection big enough for the default to run out, raising it is
usually what you want, because "recorded unchecked" is the one outcome nobody
asked for.

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
lives in the `_thermocline` database on the cold tier, whose name is
configurable as `meta.db`:

| Collection | Purpose |
|---|---|
| `checkpoints` | One change stream resume token per feeder, for recovery |
| `positions` | How far each feeder has mirrored, published far more often than the checkpoint because fallback reads wait on it |
| `evicting` | Eviction announcements, expiring on a TTL index |
| `residency` | How long a document should stay in the fast tier (used by the evictor and router) |
| `warming` | Warming announcements, expiring on a TTL index |
| `watermarks` | How far back the fast tier is complete, one per namespace |
| `markers` | Daemon heartbeats, currently just the warmer's |

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

## Adopting a cluster you already have

This is the shape every deployment starts in, and the one most of this system's
defects have come from. **The cluster you have becomes the archive, exactly as
it is. The fast tier is a new, smaller replica set with nothing in it. The
application is pointed at the router.** Nothing is loaded into the fast tier by
hand.

**Upgrade it to the fast tier's MongoDB release first.** Both tiers must run the
same release and every daemon refuses to start otherwise: the reason and the way
out are in [Both tiers run the same MongoDB release](#both-tiers-run-the-same-mongodb-release).
The cluster you are adopting is usually the older side, so this is part of the
adoption rather than something to do afterwards.

The order matters, because most of what goes wrong goes wrong quietly.

**1. Point the router at both, and read what it says at startup.** It compares
the two tiers and names, once, everything the fast tier is missing that the
application will need. Nothing here is fatal and all of it is silent afterwards.

**2. Change the connection string, do not just repoint it.** The router answers
`hello` as a mongos, so a driver told `replicaSet=` in its URI is looking for a
replica set member and will never accept one. Measured, with the same router and
the same credentials:

```
mongodb://user:pw@router:27017/?authSource=admin                  ok
mongodb://user:pw@router:27017/?replicaSet=rsOld&authSource=admin MongoServerSelectionError
```

The second one times out rather than saying what is wrong, and the router cannot
help: a driver never says it was looking for a replica set. Drop `replicaSet=`
and give it the router's address.

**3. Create the application's accounts on the fast tier.** It is a new replica
set and has none of them, nor any custom role they refer to; MongoDB refuses to
create a user whose role is missing and names it, so do the roles first. The router authenticates against the fast tier, so
until they exist every connection is refused with MongoDB's ordinary
`Authentication failed`, which says nothing about which tier is missing them.
The router names them at startup.

Give the router's own account `killAnyCursor` on the fast tier while you are
there. When the archive answers a query the fast tier had started, the fast tier
is left holding a cursor for the rest of it that nothing will ever read, and the
router closes it. It sends that on its own account, and mongod lets an account
close only its own cursors without that privilege. Without it those cursors sit
until mongod's timeout, ten minutes by default. The router says so once if it
cannot.

**4. Deal with the collections nothing tiers.** A collection the archive holds
that is not in `feeder.namespaces` lives only on the archive, and the router
correctly forwards reads of it to the fast tier, which has never held it. The
application sees it empty. Either tier it or copy it whole. Views are named
separately because neither answer works for them: recreate them with
`createView` and tier what they read from.

**5. Check the `_id` type of everything you are tiering, and what its timestamps
mean.** A warm or eviction rule with no `date_field` takes each document's age
from the timestamp inside its `_id`, which only an ObjectId has. Against
integer, string or UUID `_id`s the rule addresses nothing at all, so set
`date_field` for those collections. Both components now refuse such a rule
rather than obeying it, and say so.

The quieter half is a collection whose `_id`s *are* ObjectIds carrying the wrong
date. An ObjectId's timestamp is when the document was created, which is the
same as its business date only when the collection accumulated over time. A
cluster populated by an application level migration has every document
re-inserted, so every `_id` carries the migration's date. Measured on documents
dated a year ago and loaded a moment before: the evictor with `older_than: 40m`
scanned zero and evicted nothing, and would go on doing that until the
migration itself was older than the window. Nothing is wrong from where it
stands: the rule addresses documents and tiers by the wrong clock.

And set `date_field` to the *same* field on both rules for a namespace. Ageing
one collection two ways costs more than the two daemons picking different
documents. The router will not read a lower time bound out of a query on such a
namespace, because it cannot tell which of the two a completeness claim is
measured in, so every bounded query on it is answered by the archive. One word
of configuration turns the whole thing off for that collection, and neither file
shows it on its own, since the daemons are usually given separate ones. Each
daemon names the namespace at startup.

`mongorestore` keeps the original `_id`s and is fine. Anything that re-inserted
the documents is not. Compare the two before you trust a rule that has no
`date_field`:

```
const c = db.getSiblingDB("app").orders;
const o = c.find().sort({_id: 1}).limit(1).next();
print(o._id.getTimestamp() + "   <- what the rule reads");
print(o.at + "   <- what the application means");
```

**6. Warm, then look at the claim it left.** A boundary claim cannot answer a
query with no lower bound, which is most of what an application asks. When the
warm covers everything the archive holds, the warmer says so and the fast tier
answers everything; when it does not, the warmer names what that costs.

**7. Run `cline keys --rebuild` before you let the evictor run.** The registry
only protects values it knows about, and a cluster adopted with documents
already in it starts with none of them claimed. It reads the constraint from the
fast tier's unique indexes, which the router builds there at startup, so run it
after the router has been up once.

Run it again after upgrading past 0.1.0-dev if any of your unique keys are
numbers. A key is derived from the value, and whole numbers are now derived the
way mongod compares them, so int, long, double and decimal 7 arrive at one key
instead of four. Claims made by an older build carry the old id, and an old id
matches nothing, so the values they hold are not enforced until the rebuild
reclaims them.

**8. Start change stream consumers fresh, do not resume them.** A resume token
minted on the old cluster is accepted by the router and delivers nothing.
Measured with one token and one collection: against the cluster it came from it
delivers the write that followed it; through the router after the cutover it
delivers nothing at all and reports success. The router serves change streams
from the fast tier, which is a different replica set with its own oplog, and a
token from another one is not a position in it. There is no error to catch, so
this has to be a step rather than a surprise.

**9. Fallback reads land on the archive's primary, and you cannot move them.**
During the adoption most reads fall through, and the archive is the production
cluster you are relieving, so sending them to its secondaries is the obvious
thing to want. `readPreference` on `cold.uri` does not do it: archive reads are
issued as commands, and a driver runs a command against the primary whatever the
client's preference says. Measured with `secondaryPreferred`, the fallback read
was served by the primary anyway.

That stays as it is on purpose. The freshness barrier waits for mirroring to
reach the archive's primary, and a secondary is behind that by a lag nothing
here measures, so a read served there could miss a write that has already been
acknowledged. The router now says the setting is ignored rather than letting you
believe it took effect. Plan the archive's primary for the read load, and warm
the fast tier to shorten the period it lasts.

**10. Expect eviction to hold off at first.** The evictor will not remove
anything until the fast tier's oplog window exceeds `min_oplog_window`, and a
new replica set's window is its own age, so with the shipped default of 24h
nothing is evicted on the first day. That is the guard working, and it says so:
`eviction paused, reason="oplog window is 3h20m, limit 24h"`. Lower the limit
only if you understand what it protects.

An eviction sweep that does run before the warm is no longer a problem either,
and it was one. The evictor narrows what the fast tier claims to hold before it
removes anything, which is right; on an adoption there was nothing to narrow,
and it made a claim instead, saying the fast tier held everything since the
eviction boundary about a tier holding nothing. A sweep that scanned zero
documents and evicted zero was enough. Measured on 4,000 archived orders with
the fast tier empty: the router answered a thirty day range with 240 before the
sweep, the archive's own answer, and a router started after it answered 0 and
reported success. The already-running router kept the claim it had cached, so
it took a restart to appear.

A sweep now narrows a claim and never makes one. The claim comes from whatever
put the documents there: the warmer, which knows how far back it filled, or you,
with `cline watermark --complete-all`.

**11. Size the fast tier for the key registry as well as the documents.** Every
unique value gets a claim in `_thermocline.keys`, on the fast tier, and claims
are permanent by design: one has to outlive the document it protects. After
`cline keys --rebuild` on a cluster adopted with data already in it, that is one
claim for every document the collection has, multiplied by the number of unique
indexes on it.

Measured on 20,000 documents with one unique index: 147 bytes per claim
uncompressed, 56 on disk, and a registry of 1.1 MB beside a collection of
0.3 MB. The ratio is a function of how big your documents are and means little;
the per claim figure is the one to plan with. Fifty million documents with one
unique index is on the order of three gigabytes of registry, on the tier you
sized for a hot window.

**12. Know how to go back before you need to.** The feeder mirrors every write
into the archive, so the archive stays current and rolling back is repointing
the application at it. One thing has to happen first: a delete is mirrored as a
mark and not as a removal, so every document deleted since the cutover is still
physically there carrying `_tcDeletedAt`. The router filters those out of every
answer and a client reading the cluster directly does not.

Measured: three documents in the collection, one of them deleted through the
router, and the original cluster answering `countDocuments({})` with 3 where the
router answers 2.

`cline rollback` counts them, across every tiered namespace, and clears them
when you ask:

```
cline rollback --config /etc/cline.yaml
  app.orders                               2 marked deleted

cline rollback --config /etc/cline.yaml --clear
  app.orders                               cleared 2
```

**Stop the application and the feeder first.** A delete that reaches the archive
after the clear writes another mark, so clearing while anything is still writing
leaves you where you started, and the command refuses while a feeder is
publishing rather than letting you find that out afterwards. `--force` overrides
it if you know nothing is writing.

**13. Run more than one router by naming them all, not by hiding them.** A read
the fast tier cannot answer is served from a cursor the router owns in its own
memory, holding an open connection to the archive, so it cannot be shared with
another process. Drivers already handle this: they pin a cursor to the server
that created it, exactly as they do for a mongos. Give the connection string
both addresses and it works.

```
mongodb://user:pw@router1:27017,router2:27017/?authSource=admin      iterated 20 of 20
```

Putting them behind a single load balanced address breaks that, because the
driver then believes there is one server and its pool sends the getMore down
whichever connection is free. Measured, the same cursor asked of the second
router: `cursor id 8792354910962146833 not found`. It fails only for reads that
fall through and return more than one batch, which is most of them during an
adoption, and only sometimes, which is worse.

The caches do not need naming: each router keeps its own, and a write it did not
make reaches it through the change stream it watches on the fast tier. That is
asynchronous, so it is a race by nature, and it was measured rather than
assumed: with one router answering from its cache, nine hits deep, and the other
taking a write before each read, twenty rounds in a row gave the new answer and
none gave the old one.

One cache is not that one. Each router keeps its own listing of which unique
indexes a collection has, for a minute, and no change stream carries index DDL.
So a unique index created on a running system is enforced across tiers
immediately by the router the command went through, and within a minute by the
others. In that window another router does not claim keys for it, and lets
through a duplicate of an archived document that the fast tier's own index
cannot see. Restart the routers if you need it enforced everywhere at once, and
run `cline keys --rebuild` either way, which the rollout list already asks for.

**14. The archive is a hard dependency, and not only for its documents.** The
completeness watermarks, the feeder's checkpoint and its published position all
live on the archive, so a router that cannot reach it does not know what the
fast tier is allowed to answer or how far mirroring has got. It therefore
refuses to start rather than guess: `error: ping cold tier: server selection
error`. A router restart during an archive outage leaves the application with
nothing, including the reads the fast tier could have served alone. Plan
restarts accordingly, and remember that on day one almost every read needs the
archive anyway.

**That one stays as it is, deliberately.** A router that has never read a claim
knows which namespaces are tiered and nothing about what the fast tier holds of
them, so anything it served would be a guess dressed as an answer. Starting and
refusing every tiered namespace by name was considered and is not obviously
better than not starting: the application gets errors either way, and a process
that is up is a process monitoring reports as healthy.

**A router that is already running is a different case, and it has a knob.**
About five seconds into an archive outage it stops answering tiered reads too,
even the ones the fast tier owns, because the claim it caches expires and cannot
be re-read.

**What that cost was measured to be, and it was worse than "stops answering".**
One claim read against an address nothing answers on takes **30.002 seconds**,
the driver's default server selection timeout, which nothing here shortens. Each
tiered read paid it in full and was then refused, so an archive outage was not a
router refusing reads promptly, it was a router hanging for half a minute per
read and then refusing. Any client with a socket timeout gave up first and saw a
broken proxy. A failed claim read is now remembered for the same five seconds the
claim itself is cached, so one read per namespace per five seconds pays the
timeout and the rest are answered from what is held. That applies whether or not
a grace is configured: with none, the refusals simply arrive promptly.

`router.claim_grace` says how much longer a claim already read may be believed.
It is off by default and the trade is exact:

- **Safe against an archive outage.** Narrowing a claim and evicting what it
  stops covering are both writes to the archive, so while nobody can reach it
  neither can happen and the last claim read is still true.
- **Not safe against a partition that isolates only the router.** The archive is
  up, the evictor is working, claims are being narrowed and documents removed,
  and this router answers from a claim covering documents the fast tier no
  longer holds. Those reads are short with no error on them, for as long as the
  grace lasts.
- **Writes are never covered by it.** An insert skips the archive's own
  duplicate check when the fast tier claims all of history, so a claim that has
  become false would let through an `_id` the archive still holds and the feeder
  would mirror the new document over the archived one. Measured, while the grace
  still applied everywhere: an insert the same router refused without a grace
  was accepted and landed. Every write path refuses while the archive is
  unreachable, whatever the grace is set to. The trade on offer is a short read
  against a refused read, and never a destroyed document.
- **It invents nothing.** A namespace this router was never asked about has no
  claim to hold over and is refused, and so is every namespace if the outage
  started before the router was asked anything at all.

`thermocline_claim_grace_reads_total` counts the reads it allowed. Alert on it:
the counter cannot tell a correct answer from a short one, and you can.

An archive *failover* is a different matter and was measured rather than
assumed. With its primary stepped down under load, and both kinds of read
running throughout, nothing failed: 19 reads the fast tier owned and 13 that
fell through, all answered correctly while the set elected a new primary. The
driver's own retry absorbs it.

**The fast tier's failover costs a handful of operations, and it used to cost
everything.** This is the one an adoption is most likely to meet, because the
fast tier is the new replica set and the one being rolled and resized. Measured
three times with the shop running through the router, all four daemons working
underneath it and the warm still filling, and the fast tier's primary stepped
down about forty seconds in: 25,600 operations a run, an election decided in
about a second, the router pointing at the new primary 1.1 to 1.5 seconds later,
2 to 8 failed operations, and none of the five invariants broken. What the
client sees is a few refusals and a dropped connection, which its driver retries.
The control says what to compare that against: the same shop and the same
stepdown against plain MongoDB, with no router in the way, cost no errors at
all. Before this was fixed the same test never recovered; see the proxied
authentication bullet under "What it does not do yet".

**14a. Keep `evictor.max_feeder_lag` well under an hour, and keep the clocks
together.** The evictor announces a removal before it makes it, and the feeder
asks whether a delete was announced. That announcement is kept for an hour and
then expires. A delete the feeder only reaches after the announcement has gone
reads as the application's own, the archive tombstones its copy, and the
document is gone from both tiers with nothing said. So `max_feeder_lag` and that
hour are one setting in two places: the config refuses a lag that reaches it.

The same relationship depends on the two machines agreeing about the time. The
announcement is stamped by the evictor and expired by the archive's TTL monitor,
so an evictor whose clock runs behind the archive's has its announcements
expiring early, and far enough behind they are gone before they are read. Both
daemons check their clock against the archive at startup and say what they find.

**15. Expect the warm to cost something and the evictor not to.** With a shop
running against the router while all four daemons worked underneath it, 38,248
operations completed with no errors and no violated invariants, so the pieces do
not fight. What they cost is not equal, and the difference is worth knowing when
choosing a window to start in.

The evictor is free within the noise of this machine, measured by alternating
arms rather than trusted from one run:

| | throughput | order history p50 |
|---|---|---|
| evictor sweeping | 177/s | 83.21 ms |
| evictor paused | 181/s | 81.70 ms |
| evictor sweeping | 159/s | 86.94 ms |

The initial warm is the part that competes: the same shop ran at 84/s while it
was filling and around 170/s afterwards. It is bounded by `warmer.rate`, which
is the knob to turn if the application feels it, and it happens once.

Sizing the window too small costs latency. The same shop was run against a two
minute hot window with twenty second residency and a sweep every ten seconds, so
eviction and copy-up fought over the same documents throughout: 26,881 documents
evicted across twelve sweeps, 306 copied back up, the fast tier stripped to 975
orders of the 53,000 there are. No errors, and none of the five invariants
broken. What you get for a window that small is a fast tier holding nothing much
and an application paying archive latency for most of its reads.

**That run was also reported here as costing latency and not correctness, and
that was wrong.** The same fight, watched a different way, was losing data. A
copy-up deleted the eviction announcement the feeder had not used yet, so the
feeder read a real eviction as an application delete and tombstoned a record
nothing had deleted; the copy-up's own insert did not repair it, because the
feeder suppresses that as an echo. Measured on the sample datasets with a warm
and an evictor over the same collections: 66 batches of "deletes not announced
by the evictor" in ninety seconds, and an archive left holding 200 tombstoned
trips and 45 tombstoned sales that nothing had ever deleted. The router then
hid them from every answer, and the fast tier had already dropped them.

It is fixed, and the announcement is now a count that only the feeder spends.
The reason it was missed for so long is worth more than the bug: the earlier run
watched throughput and the shop's five invariants, and both stayed clean,
because a document quietly removed from a collection breaks no invariant about
the documents that remain. What caught it was comparing every answer against the
archive, which is what `test/pilot` does.

**16. Check the tiers agree, with `cline indexes`.** It reports both directions:
what the archive cannot answer efficiently, and what the fast tier is scanning
for.

What the router does for you, at startup, for every namespace it manages that
the archive already holds: it creates the collection on the fast tier the way
the archive has it, carrying the default collation and the validator, and builds
the archive's indexes on it. Those are the things that are not documents, and
filling a tier with documents does not bring them. A collation cannot be added
to a collection afterwards at all, which is why this happens before the first
client connects.

## Running it

```
make build
cp cline.example.yaml cline.yaml     # then edit the two URIs
bin/cline feeder --config cline.yaml
```

### A key the config does not know is an error

Every daemon refuses to start on a setting it cannot place, naming the line:

```
error: parse config: yaml: unmarshal errors:
  line 311: field terminate-auth not found in type config.RouterConfig
```

This used to be a silence, and a silently ignored setting is the worst shape a
configuration mistake can take. Measured on one file: `terminate-auth: true`,
with a hyphen where the key has an underscore, loaded as `false`. The router
proxied authentication while the operator believed it was terminating it and
checking privileges itself. Nothing failed, nothing was logged, and the setting
simply did not exist. A misspelt `tombstone_retention` keeps deleted documents
for ever; a misspelt `claim_grace` gives no grace.

The cost of refusing is a restart with an error naming the line. The cost of the
silence is a setting you think you have.

### Both tiers are replica sets

That is the supported shape, and the only one anything here is tested against. A
sharded fast tier is refused, with the reason, because every reading the daemons
hold themselves back on is a replica set's. A sharded archive is not refused and
not supported: it is not obviously broken, and nothing checks it. Both are in
[What is not handled yet](#what-is-not-handled-yet).

### Both tiers run the same MongoDB release

Every daemon reads both tiers' versions at startup and refuses to run when they
differ:

```
error: the tiers run different MongoDB releases: fast tier 8.0.26-11, archive
7.0.30-1. Both tiers must run the same major.minor release, because a read the
fast tier cannot answer in full is sent to the archive as the application wrote
it, so the archive has to understand every stage, operator and option the
application uses. Upgrade the older tier to 8.0, or set allow_version_skew: true
to start anyway
```

The archive runs the client's own queries, not a rewritten shape of them. That
is what makes a fall-through correct, and it is also what makes a mismatched
pair dangerous: a proxy cannot rewrite a pipeline into an older dialect, so a
query using anything the archive does not understand works for as long as the
documents it reads are on the fast tier, and then fails with the archive's own
error naming a stage the application has always used. Nothing about that reads
as a version problem.

Measured on 8.0.26 over 7.0.37 with the skew allowed: `$convert` with `format`,
which 8.0 accepts and 7.0 refuses, answered a point read from fast tier documents
and failed over the same collection when the archive had to answer it, with
`$convert found an unknown argument: format`.

**So the router adds a sentence to that error**, naming both releases and saying
that a read the fast tier cannot answer in full is sent to the archive as the
application wrote it. Only when the archive is the older tier, and only for an
error that is a server refusing to recognise a name: a duplicate key, a timeout
or a genuinely malformed query gets the archive's words and nothing added, or a
version note becomes something to chase on every unrelated error.

Patch releases may differ, and the log says so once. One tier is always upgraded
before the other, so demanding an identical patch would refuse to start in the
middle of every rolling upgrade, in exchange for nothing: a patch release adds
no stage, operator or option for the other tier to fail on.

`allow_version_skew: true` starts anyway. It is for a migration you are watching,
not a state to run in, and the log repeats the warning at every start.

**The same release is not the whole of it.** Two servers on one release still
refuse different commands when their `featureCompatibilityVersion` differs, and
which tier answers a query is decided by how old its documents are. So that is
checked too, and refused the same way:

```
error: the tiers are both on 7.0 but disagree on featureCompatibilityVersion:
fast tier 7.0, archive 6.0. ... Run
db.adminCommand({setFeatureCompatibilityVersion: "7.0"}) on the tier that is
behind, or set allow_version_skew: true to start anyway
```

A version that cannot be read is not a refusal. `buildInfo` failing is a
connection problem, and whatever needs that connection says so in its own words
a moment later; the log names which tier could not be asked, so silence is not
mistaken for agreement. Reading the feature compatibility version needs a
privilege the router's own account may not have, and an account that cannot read
it on both tiers skips that half of the check rather than refusing to start.

A mongos is one server that does this: it answers `buildInfo`, so releases are
still compared, and it does not answer `getParameter` for
`featureCompatibilityVersion`. Both tiers are replica sets here, so that is a
note rather than a case.

The four daemons check this: feeder, evictor, warmer and router. The one-shot
tools do not, because a mismatched pair is exactly when you need `cline
watermark`, `cline keys` and `cline rollback` to work.

**Upgrading a major.minor release, then, is a procedure rather than a restart.**
The tiers differ for as long as it takes, and the rule says they may not, so the
order matters and one direction is safer than the other:

1. **Set `allow_version_skew: true`** and restart the daemons. Nothing else in
   this list can happen while they refuse to start.
2. **Upgrade the archive first.** This is the half that is not obvious and it
   follows from how a fall-through works: the archive runs the client's own
   query, not a rewritten one, so an archive newer than the fast tier understands
   everything the application can send it. The other order gives you an archive
   that fails on a stage the fast tier has just learned, and only for the
   queries whose documents have aged out, which is the hardest shape of failure
   to attribute.
3. **Then the fast tier.**
4. **Then `setFeatureCompatibilityVersion` on the archive, then on the fast
   tier**, for the same reason in the same order.
5. **Remove `allow_version_skew`** and restart the daemons, so the guard is back
   on before the next thing goes wrong.

Between steps 1 and 5 the pair is unguarded, which is why the log repeats the
warning at every start. `cline indexes` and `cline watermark` keep working
throughout; they never checked.

### Backups

The archive is the complete copy, so it is the one to back up. That is the whole
point of the shape: the fast tier holds a recent slice, and a `mongodump` of it
captures exactly that slice however long the cluster has been running.

What to take, and where each piece lives:

- **The archive's application databases.** Every document the feeder has
  mirrored, live and tombstoned, lagging by mirroring lag. The router's
  dashboard and `thermocline_mirroring_lag_seconds` say how far behind that is at
  the moment you take it.
- **The archive's bookkeeping database**, `_thermocline` by default. Watermarks,
  mirroring checkpoints and positions, residency holds, the eviction and warming
  notices, the singleton markers. Restoring documents without these leaves a
  system whose claims describe a fast tier that no longer matches.
- **The fast tier's `_thermocline.keys`.** The unique key registry is the one
  piece of bookkeeping that lives on the fast tier, because it constrains the
  data there. It is also the one piece you can rebuild rather than restore, with
  `cline keys --rebuild`.
- **The fast tier's documents, optionally.** They are a subset of the archive
  except for anything the feeder never mirrored, which the daemons name at
  startup as documents "the archive has none of". Warming refills the rest.

Restoring the archive under a live fast tier needs eviction held first:

    cline pause --config cline.yaml --reason "restoring the archive"
    # restore, then
    cline resume --config cline.yaml

That order is not a formality. The evictor's check is per document and presence
only, so an archive holding an *older* version of a document confirms it as
present, and the fast tier's newer copy is removed: the restore would cost you
the writes made since the backup rather than recovering them.

**And the limit worth knowing before you need it.** A restored archive is a
restored past. Documents written after the backup are still on the fast tier if
they are recent enough to be there, and the feeder cannot fetch them: a change
stream reports changes from now, and mirroring only moves forward. Anything
written after the backup and already evicted is in neither tier. So the archive's
backup interval is the real exposure of this system, not the fast tier's, and
`cline rollback`, which counts rather than removes unless you pass `--clear`, is
what tells you what the two tiers disagree about afterwards.

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

- **mapReduce cannot write into a tiered collection**, and is refused when it
  tries. A replacing output is built beside the target and renamed over it, and a
  rename is not mirrored, so the result would reach the fast tier and nothing
  else; a merging one writes per document against a tier that holds only part of
  the collection. The same job expressed as an aggregation ending in `$out` or
  `$merge` is performed on both tiers, which is the remedy the refusal names.
  Reading a tiered collection with mapReduce is refused for a different reason,
  already: it totals whatever tier it runs on.

- **A pipeline the archive would answer that also reads an untiered collection is
  refused**, because answering it would be short for that half.

  The archive holds every document of what it is told to tier, which is what
  makes the fallback correct. It holds whatever it happened to hold of everything
  else: the feeder mirrors the namespaces in `feeder.namespaces` and nothing
  else, so anything written to an untiered collection since the cutover is on the
  fast tier alone. A `$lookup` or `$unionWith` of one, in a pipeline the fast tier
  cannot answer, would be read from a copy that stopped being updated.

  Measured before it was refused, on a `$unionWith` of an untiered collection with
  a view over a tiered one: the answer was 10 where the two clusters together hold
  11, the missing document being the one written to the untiered collection after
  the cutover. Neither the pilot nor the comparison harness can see that: the
  pilot's untiered collections were restored into the archive and never written to
  again, and the harness asks the archive the same question and gets the same
  short answer.

  This was a warning for one afternoon, on the reasoning that a report joining a
  reference collection unchanged since the adoption is right more often than it is
  wrong. That trades somebody else's correctness for our convenience, and the
  answer it let through carried no error, so it is a refusal: a refusal is a
  limitation, a wrong answer is a bug. The message names the collections and the
  two remedies, and the refusal lifts by itself once either is done: **tier the
  collections the pipeline reads**, or **warm the tiered ones** so the fast tier
  answers the whole pipeline and nothing goes to the archive at all.

  There is a third way out, for the case the afternoon's reasoning was actually
  about: `router.allow_untiered_join` answers the pipeline instead and names the
  untiered collections in a warning every time. It is a claim about your own
  application, not about this system, so nothing here can check it: set it only
  where those collections really are reference data nobody writes to. The pilot
  runs both ways, because it is the one place the claim is true, and with it on
  all nineteen aggregations agree with the archive: `bash test/pilot/setup.sh
  join`.

- **Cursors are not shared between routers.** A read answered from the archive
  is served from a cursor this router holds open against the archive, in its own
  memory, so another router cannot continue it. This is what a mongos does too,
  and drivers pin cursors to the server that created them, so naming every
  router in the connection string is all it takes. What does not work is one
  load balanced address in front of several routers: the driver sees one server,
  its pool sends the getMore down whichever connection is free, and it lands on
  a router that never had the cursor.

  What the application sees then is `CursorNotFound`, code 43, from the fast
  tier: the router does not recognise the id, forwards the getMore, and the fast
  tier has never heard of that cursor either. It cannot tell you more than that,
  and it is worth knowing why. A cursor id says nothing about who minted it:
  measured on PSMDB 8.0, mongod's own ids cover the whole positive int64 range,
  so an id from another router is indistinguishable from an expired one. A
  `CursorNotFound` on a read that falls through, behind a load balanced address,
  is this and not an expiry.

- **A change stream cannot be resumed across the cutover, and nothing says so.**
  A resume token is a position in one replica set's oplog. The router serves
  change streams from the fast tier, so a token minted on the cluster being
  adopted is not a position in it. MongoDB accepts it rather than refusing it:
  measured, the same token delivered the write that followed it against the
  cluster it came from, and delivered nothing at all through the router, with
  `ok: 1`. A consumer resuming across the cutover therefore loses every event
  the old cluster saw after its last token, silently.

  Refusing it would mean the router parsing resume tokens, which are an
  undocumented encoding that changes between server releases, so this is a step
  in the rollout rather than a guard: start consumers fresh.

  **What it does now is say so.** A change stream opened with `resumeAfter`,
  `startAfter` or `startAtOperationTime` on a tiered collection is named in the
  log at WARN, with which option carried it. That does not make a stale token
  work; it means the one failure whose symptom is silence has a line somewhere
  that mentions it.

- **Sharded fast tiers are refused, with the reason.** Everything here assumes
  the fast tier is a replica set. The router proxies bytes to one primary
  address and a mongos does not have one: measured, its `hello` answers
  `{msg: "isdbgrid", isWritablePrimary: true}` and nothing else. Every reading
  the evictor holds itself back on is a replica set's, and a mongos has none of
  them: `replSetGetStatus` is "not supported through mongos", the `local`
  database cannot be reached at all, and `serverStatus` carries no `wiredTiger`
  section, so cache pressure and write tickets are unknown too.

  A reading that cannot be taken does not block, so before this was refused the
  daemons ran against a mongos holding themselves back on nothing: an evictor
  dry run with five eligible documents reported `would_evict=5` and
  `backoffs=0`. The router did stop, with "fast tier reported no primary
  address", which is true and says nothing about the cause.

  All four commands now refuse a mongos fast tier and name it.
  `test/testsharded.sh` builds a config server, a shard and a mongos on the host
  and checks each one. Supporting a sharded fast tier is a feature rather than a
  fix, and the first thing it needs is health readings that mean something
  there.

- **A sharded archive is not supported either, and it is not refused.** Both
  tiers are replica sets: that is the supported shape, and it is the only one
  anything here is tested against.

  The distinction matters, so it is stated rather than left to be discovered. A
  mongos archive is *not* obviously broken. Nothing the daemons hold themselves
  back on is read from the archive, so the health readings a mongos cannot give
  are never asked of it; it is only ever sent ordinary queries, ordinary writes,
  and `hello` for its clock, and it answers all three. Pointed at one, this
  behaved: writes mirrored, fall-through reads came back, deletes became
  tombstones, and an eviction sweep removed from the fast tier and left the
  archive's copy.

  It is still not supported, because nothing checks it. There is no suite,
  nothing runs that way, and a defect there would be found by whoever is running
  it in production rather than here. If that shape is wanted, the work is a suite
  that keeps it honest, not a change to the code.
- **Ticket pressure is measured as a fraction of the pool.** MongoDB 7.0 made
  the write ticket pool dynamic and host-sized, so an absolute floor is
  meaningless: a container reports a total of 8 tickets where a pre-7.0 server
  reported a fixed 128.
- **Router connection pooling, with authentication proxied.** One upstream
  connection per client is simple and correct for SCRAM, and it will not scale to
  tens of thousands of clients. With `router.terminate_auth` on the connections
  are pooled instead: measured, eight idle clients cost zero extra connections to
  the primary, against thirty two with it off.
- **`$out` into a tiered collection is the one place the router removes archived
  data**, and it is deliberate rather than an oversight. Clearing the fast tier
  only reaches what the fast tier holds, so an evicted document lives in the
  archive alone and would come back beside the result: measured, a `$out`
  promising exactly one document answered with two. `$out` says the target holds
  the result and nothing else, so the archive's live copies are marked deleted
  too. A drop is not replayed into the archive precisely so one accident cannot
  destroy it; a `$out` is not an accident, it is an application saying what this
  collection should now contain. The archived documents are tombstoned rather
  than removed, so what they were is still there to look at, and the archive is
  cleared after the fast tier, so a failure leaves the collection reading as
  what it held rather than half replaced. It is the expensive half as well:
  clearing the fast tier reaches only the working set, while this reaches every
  live document the archive holds for that namespace, so a `$out` over a
  collection with years behind it is a large write. The router logs how many it
  marked.

  **A `$out` whose target is tiered is performed by the router whatever its
  input reads.** mongod does a `$out` by building the result beside the target
  and renaming over it, and a rename is not mirrored: a forwarded one put the
  result on the fast tier and nowhere else. Measured, two documents written and
  the archive holding none of them a minute later, with a count through the
  router answering 1 where the fast tier held 2. The result is built in a
  temporary collection here too, because clearing the target is part of the
  write and `$out` is allowed to read the collection it replaces: clearing first
  left nought documents on either tier where MongoDB leaves two. The last step
  moves the result in with ordinary inserts, which is the only difference from
  mongod, and claims its unique keys so the registry still holds them once those
  documents are evicted.
- **Collection DDL other than rename** is logged and ignored, never replayed.
  A drop is the one worth knowing: the fast tier's copy goes, the archive keeps
  its documents, and the completeness claim is withdrawn, so the collection
  still answers from the archive afterwards. That is the rule working rather
  than failing, because replaying a drop is exactly how one accident on the fast
  tier would destroy the archive. Removing archived documents is deliberately a
  separate act.
- **`terminate_auth` has never met traffic that was not a test.** Everything it
  lifts is lifted in principle only while it is off by default, and the
  security-critical half of it is an authorization gate. That gate is the only
  thing between a client and somebody else's data in that mode, it denies by
  default, and it has thirty three checks behind it in `testauthterm.sh`,
  including the ones that caught it allowing a `$lookup`, a `$merge` and an
  unacknowledged write it should have refused. It is still a judgement call
  rather than a finished feature.

## Design documents

The architecture, sizing from 500 GB to 5 TB, and the alternatives worth ruling
out first are written up separately, along with a phased rollout guide for
adopting this on a database that is already serving traffic.
