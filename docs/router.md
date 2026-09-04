# The router

What the proxy does with each kind of command, and why. The longest of these documents, because it is where the work is.

Part of [Thermocline](../README.md).

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
