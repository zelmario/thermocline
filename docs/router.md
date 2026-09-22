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
- **A change stream does not show this system's own work.** Tiering moves
  documents between the tiers, and every one of those moves is an event on the
  fast tier: a copy-up and a warm are inserts, an eviction is a delete. An
  application watching a tiered collection would be told a document it never
  wrote had appeared, or that one it still has was deleted. Those are hidden.
  What remains is the application's own writes, and nothing else is added or
  reordered. See below for how far behind a consumer may be before this stops
  holding.
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
  data. **It ships off, decided on 2026-09-16, and proxied authentication is the
  supported configuration.** What that costs is in `docs/operations.md`. Turning
  it on is a decision about your own risk: the gate has not been reviewed by
  anybody who did not write it, and the router warns at every start that you
  have taken it.

  Proxied, which is the default: each client gets its own
  connection to the fast tier, so SCRAM works, at the cost of one upstream
  connection per client. It also sets what happens during a fast tier election:
  the router never sees the credentials, so it cannot re-authenticate a fresh
  upstream and replay a command that was in flight. The command in flight when
  the primary goes away fails and the client has to retry it, where a driver
  talking to the replica set directly would block in server selection instead.
  Measured during a real election: six writes a second apart were refused, every
  acknowledged write was present, none was applied twice, and a retry finished
  the job.

  The client's retry is a new connection, and that connection waits up to 20
  seconds for the fast tier to have a primary rather than failing outright,
  which is the pause a driver would have had. That wait used to be reachable
  only when the pinned member was *down*, because it hung off a failed dial, and
  a member that steps down stays up and accepts connections perfectly well: the
  dial succeeded, the client's first write came back `not primary`, and its one
  retry was spent. Measured with every member frozen so no election could
  finish, 3 of 3 writes were refused. It now asks who is primary before dialing
  anybody whenever the last thing it heard was such a refusal, and keeps asking
  until somebody is: 4 of 4 paused and landed, applied exactly once each. A
  window longer than the wait still ends the connection rather than holding it,
  and the router logs the set's own topology as the reason.

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

  **A member in the set is not the same as a member you can read from**, and the
  router learns the difference from what the member says rather than from a
  health check of its own. It picks a secondary out of `hello`'s `hosts`, and
  `hosts` lists every configured member: one in a maintenance window, an initial
  sync or a rollback stays in it, stays up, accepts connections and answers
  reads with `NotPrimaryOrSecondary`. A driver never picks such a member.
  Measured with one secondary in maintenance and `retryReads=false`, which is
  what a `getMore` looks like too: ten reads straight at the set all answered,
  ten through the router all failed. The read is now sent again on the primary,
  which is the same fallback a secondary that cannot be dialled already got, and
  the member leaves the rotation for 30 seconds with one line saying so.
  `thermocline_secondary_reads_moved_total` counts them, and anything above zero
  means a member is up and not readable.

  Two edges are worth knowing. A `getMore` cannot be moved, because the cursor
  is on that node and nowhere else, so the client is told exactly what it would
  be told talking to the set directly. And when no member is readable the router
  answers from the primary, where a driver asking for `secondary` would block in
  server selection until it gave up: measured with both secondaries in
  maintenance, ten reads through the router answered and ten straight at the set
  timed out. Answering is the same choice this makes for a secondary it cannot
  dial.
- **A stats call counts the whole collection and measures one tier.** Those are
  different questions and a single reply carries both. `db.coll.stats().count`,
  the `collStats` command and `$collStats` all report the collection's document
  count, which is the same number `countDocuments` gives and comes from the same
  place. Everything else in that reply is bytes on a disk: `storageSize`,
  `totalIndexSize`, `avgObjSize` and the WiredTiger figures describe the fast
  tier, because they are per-cluster facts and adding two clusters' compressed
  sizes together would produce a number describing nothing. That is also what
  you want when sizing a fast tier.

  **`dbStats` follows the same rule**: its `objects` is the database's document
  count and everything else in the reply is the fast tier's. It was the fast
  tier's throughout until 2026-09-05, which made the two ways of asking the same
  question disagree. Measured on a database whose one tiered collection the
  archive holds seven documents of and the fast tier had just been created empty
  for: `collStats` answered 7, `countDocuments` answered 7, and
  `db.stats().objects` answered **0**. A database holding seven documents
  reported none, and that is the adoption's ordinary state rather than a corner,
  because the fast tier starts empty and fills.

  It is not free, and the price is worth knowing before you poll it. The
  correction costs one count against the archive per tiered collection in the
  database that is not already claimed complete: measured, 135 to 173ms per
  million documents, against 6ms for the metadata count the fast tier answers
  with. On a test database of 92 small collections the whole call went from 2-8ms
  to 64-92ms, which is round trips rather than counting. Warming a collection
  removes its share of the cost entirely, because a claim covering everything
  makes the fast tier's own count the right one. If the counts cannot all be
  taken inside ten seconds the reply is left as the fast tier's and the router
  says so, because a total corrected half way through describes neither tier.

  `avgObjSize` stays with the bytes it is derived from rather than being
  recomputed against the corrected count, for the same reason the bytes
  themselves are not corrected.

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
- **What a change stream is filtered against, and how far behind it may be.**
  Three things move documents without an application asking: a copy-up, when a
  read or a write reaches a document only the archive holds; a warm, which does
  the same in bulk ahead of time; and an eviction. The first two are inserts on
  the fast tier and the third is a delete, and all three are hidden from a
  client's stream. So are events for the router's own scratch collections, which
  a `$out` or `$merge` creates and drops, and the rows this system writes to its
  own bookkeeping, including the unique key registry, which is the one piece of
  it that lives on the fast tier.

  **And the catalog repair behind a copy-up.** The fast tier is repaired, not
  merely filled: before the first copy-up into a namespace the router creates
  the collection there with the archive's options, because a collation decides
  which documents match and cannot be added afterwards, and then builds the
  archive's indexes on it. Both land on the application's own namespace, so with
  `showExpandedEvents` a client would be told its collection had just been
  created and its indexes with it. Those two events are hidden; the
  application's own `create` and `createIndexes` on a tiered collection are not,
  and are matched apart from this system's by the oplog position of each, so a
  client still hears about its own DDL.

  `showExpandedEvents` is MongoDB 6.0 and up. Below it a change stream carries
  no `create` or `createIndexes` at all, so there was nothing to hide.

  **An event is judged when the client reads it, not when it happened**, and the
  client decides when that is. A consumer that stores its resume token and comes
  back to it an hour later is doing the correct thing, and it gets a backlog in
  which every tiering event is older than any window. So the decision is made
  against a record written when the document was moved, and against the event's
  own position in the oplog rather than a clock: the router and the feeder read
  the same field off the same event, so the two agree by construction rather
  than by being in sync.

  **The bound is the hour those records are kept.** A consumer further behind
  than that is shown a copy-up as an insert and an eviction as a delete, because
  the evidence has expired. It is the same hour, and the same trade, for both.

  This applies to `db.watch()` and to a deployment-wide stream as well as to one
  on a collection: those carry events for collections nothing tiers, so each
  event is matched against the configuration on its own. And it applies to the
  batch the stream is opened with, not only to the batches that follow it,
  which is what a consumer resuming from a stored position actually reads.

- **A drop through the router stops the claim at once.** Dropping a tiered
  collection empties the fast tier immediately, and the claim that it holds
  everything outlives it: the feeder withdraws the claim when it sees the event,
  and the router keeps a cached copy for a few seconds after that. Measured on a
  collection claimed complete for all of history: `countDocuments` straight
  after the drop answered **0 where the archive holds 3**, with no error.

  A drop the router forwarded is one it knows about at once, so it forgets the
  claim there and then. A drop applied straight to the fast tier, out of band,
  reaches it a moment later: the router follows the fast tier's change stream and
  forgets the same things when it sees the drop there, so the window is the
  stream's own latency rather than the claim cache's.

  **That also covers the indexer's memory, which used not to correct itself at
  all.** The router creates the fast tier's copy the way the archive has it
  before any write that would otherwise create it bare, and remembers having done
  so. A drop it never saw left that memory standing, and the next write recreated
  the collection implicitly: no collation, no validator, no index but `_id_`.
  Measured on an adopted collection with a case-insensitive collation and a
  unique index on `name`: after the out-of-band drop the fast tier's copy came
  back with neither, and a minute later, once the router had re-read its index
  listing from a collection that no longer had one, an insert of a second `alice`
  was **accepted** against a constraint the application had relied on since
  before the adoption. Nothing in either tier's log mentioned it, and it
  corrected itself only when something listed the catalog or the router was
  restarted.

  It costs one change stream per router, on the fast tier. That stream used to be
  opened only when the query cache was on; it is now always open, so the router's
  own account needs the privilege to open one. Without it the router still runs
  and says so at ERROR, and a collection dropped out of band goes back to being
  noticed at the next catalog listing or router start.

  Dropping a tiered collection **through the router** is still the right thing to
  do, because that is where every consequence of a drop is handled at once rather
  than a moment later.

  **All three drops clear the same list.** The forwarded path and the change
  stream path were written separately and had already drifted; `dropDatabase`
  was a third list nobody had compared against either, and it had drifted the
  other way, clearing the unique index listing. That listing is the one memory
  whose staleness is a refusal rather than a wrong answer, and re-reading it off
  a database that has just gone replaces an old listing with an empty one, which
  is the direction that lets a duplicate of an archived document through and
  stops all mirroring when the archive's own index refuses it. Writing it once
  added the remembered **hint** and the remembered **view**, which neither
  cleared: the hint is a sparse or partial index's own filter, which the router
  reproduces against the archive so that a fall-through read is narrowed the way
  the hint narrows it, and whether a name is a view decides whose completeness
  claim judges a read of it. Both are untrue after a drop.

  Neither has a reproduction behind it, and that is worth saying. The hint one
  was measured for a dropped *index*, which is why it is cleared there; for a
  dropped *collection* the check written for it answered 20 of 20 with and
  without the change, because putting the index back goes through
  `createIndexes` and that path already clears the same cache. The check was
  removed rather than kept green for nothing. What the shared list buys is that
  the two paths cannot drift again.

  One thing is deliberately not on the list: the fast tier's **unique index
  listing**. A collection that has just gone lists none, so re-reading it would
  replace a listing that is merely old with one that is empty, and empty is the
  direction that lets a duplicate of an archived document through. The archive
  still holds every document that constraint covered, because a drop is never
  replayed there, so the collection still logically contains them. That listing
  is refreshed by the index build the next write starts, which is when there is
  something true to read.
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

  **Copying them closes two of the three things that can happen in that window,
  and cannot close the third.** Measured on ten mirrored documents with the
  feeder stopped, one update, one insert and one delete made against the fast
  tier, the oplog rolled, and the remedy then followed exactly: the updated
  document came back right, the inserted one arrived, and the deleted one stayed
  live in the archive and was served to a client. The router counted eleven
  where the application had left ten.

  It is not an oversight in the remedy, it is what the remedy can see. A delete
  leaves nothing on the fast tier to copy, so copying the fast tier cannot know
  it happened. What can know is the completeness claim: inside the range the
  fast tier claims to hold in full, a document the archive holds live and the
  fast tier does not is one that was deleted. `cline backfill` does both halves
  and is the command to run here; doing it by hand does the first half only.
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

  **The router now says so at the moment the difference is made.** `cline
  indexes` is a report somebody has to remember to run, and the divergence is
  created by an ordinary application command that nobody is watching: an index
  added for a new query lands on the fast tier, where the working set is, and
  every read that falls through runs on the archive without it. On a
  multi-terabyte archive that is minutes against milliseconds, which is that
  command's own argument for existing. So a `createIndexes` on a tiered
  namespace that leaves the archive without an index over the same fields is one
  WARN naming the index and the namespace. Once per index per router, because a
  client that rebuilds one should not fill the log, and compared by key rather
  than by name, because an archive index over the same fields under another name
  serves the read exactly as well. A unique index raises it too: the constraint
  is enforced by the key registry across both tiers rather than by an index on
  the archive, so the archive has none and a read filtering on that field scans.

  **Two kinds of index are not a decision about speed**, and they get their own
  line saying so. mongod refuses a `$text` query with no text index and a
  `$geoNear` with no geospatial one, rather than scanning, so a read that falls
  through to an archive without them fails rather than slows. Measured with the
  text index on the fast tier alone: `text index required for $text query`
  through the router, and answered as soon as the archive had one too. Build it
  on the archive, or keep the namespace warm enough that these reads never fall
  through.
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

  Every option is compared, not a list of the ones anybody thought of. It used
  to be exactly two, collation and validator, while the create beside it carried
  every option there is except a deny-list of the ones a tiered collection
  cannot have. So an option travelled to the fast tier and was never looked at
  again: `validationAction` is one, and it decides whether a write is refused or
  merely logged, and `changeStreamPreAndPostImages` is another, where the tier
  that does not have it answers a client asking for a pre-image with an error
  rather than an event. Measured on this cluster, the wider comparison reports
  one warning where the narrow one reported one, and it is the same genuine
  collation drift; with a validationAction and a pre-image difference arranged
  on purpose it reports those too.
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

  **The commands that describe a collection get the same repair, and four of
  them used not to.** A read of a tiered collection the fast tier has never held
  falls through to the archive; a command that asks *about* the collection goes
  to the fast tier alone and is told it does not exist. Measured on one the
  archive holds six documents of, with nothing having listed the catalog:
  `countDocuments`, `find` and `distinct` all answered 6, and then

  ```
  db.coll.stats()   Unable to retrieve storageStats in $collStats
  getIndexes()      ns does not exist
  validate()        Collection 'catdb.tdbstats' does not exist to validate
  collMod           ns does not exist
  ```

  The `collStats` *command* answered, because mongod is lenient about that one
  form and replies `ok: 1` with a count of nought, which this router then
  corrects to 6. The `$collStats` *stage* errors, and that is what `mongosh`
  sends for `db.coll.stats()`. So two spellings of one question, and the one an
  operator types by hand was the one that failed.

  `listIndexes`, `collStats`, `validate` and `collMod`, and an aggregation led by
  `$collStats` or `$indexStats`, now give the fast tier the collection before
  they run, exactly as a write into an unheld collection already did. It cannot
  invent one: the create reads the archive's options and does nothing when the
  archive does not hold the collection either, so a name that exists nowhere
  still gets mongod's own answer. The first `getIndexes` after the repair reports
  `_id_` and the archive's indexes arrive with the build that follows, because
  the fast tier's indexes are the fast tier's and that is what `listIndexes` is
  asked for. `cline indexes` is what reports both.

  `dbStats`'s `collections` counts what the fast tier has rather than what the
  catalog now lists, and in practice those agree: the router creates the fast
  tier's copy of every namespace it manages as it starts, so a database the fast
  tier had never held still reported 1 collection where the archive holds one.
  What it will not show is a collection that appeared in the archive after this
  router started and that nothing has listed or written to since.
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

**The wall-clock bar stayed, and it had the same disease with the symptoms
reversed.** It has to stay, because it is the only one that covers a write this
router never saw: a second router's writes are not in this one's `operationTime`,
and the feeder having mirrored past the instant your read began covers them
whatever anybody wrote them through. But a router running a few seconds ahead of
its feeder never clears it. Every read that falls through then waits the entire
`archive_wait` and answers stale anyway, which is slow and stale, the pair this
barrier exists to avoid, and the log line it produces is the one a genuinely
lagging feeder produces. Nothing distinguishes them.

So the two instants are put in one frame before they are compared. Both daemons
talk to the archive, so both can say how far their own clock is from that one:
the feeder publishes its offset in the position record, the router measures its
own in the background on the same one-minute cadence, and the archive's clock is
then a reference neither has to trust the other about. With either offset
missing, which is a feeder from before this or a router that has not managed a
reading yet, the raw instants are compared exactly as they were. A correction
applied on one side only would be worse than none.

The same arithmetic is what `thermocline_mirroring_lag_seconds` is made of, and
what gates eviction, so both were wrong in the same way. Lag had been clamped at
zero, which treats the direction that produces a negative number and leaves the
other one looking like real lag: a host thirty seconds ahead of its feeder
graphed thirty seconds of mirroring lag on a deployment where nothing was
behind, and that is the number the alert rules watch.

**Liveness is decided by watching, not by subtracting.** A feeder that has
stopped leaves its last position behind, and a router that read that as
"mirroring is a little behind" would make every fallback read wait the whole
barrier for a feeder that is never going to catch up: measured with the feeder
stopped, 40 point reads took 185 seconds against 97 ms straight at the archive.
So a position that has gone quiet means "nothing is mirroring" and reads answer
at once. Deciding that by subtracting the feeder's stamp from this host's clock
put the whole freshness barrier behind a clock difference of two seconds, which
is the floor of the staleness window at the default cadence, and two seconds
between two hosts is ordinary. A record that differs from the one seen before is
a publication that happened between the two readings, whatever either machine
thinks the time is, so that is what is watched now: it moved, it is alive; it
has not moved for a staleness window of this router's own elapsed time, it is
stopped. A record first seen already stale by more than five minutes is taken at
its word, so a router started while the feeder has been down for hours does not
make the first read wait to learn what is obvious.

`cline preflight` and every daemon start now report a host clock more than
thirty seconds from either tier's, because three separate things here are
decided by comparing instants read on different machines and the third one,
the eviction announcements, destroys data rather than slowing a read down.

**A caller's own deadline bounds all of this, and it is spent once.** The wait
takes at most half of `maxTimeMS` and the archive read is given what is left,
rather than the whole of it a second time. Measured before that: a client asking
for 800 ms with mirroring behind waited 300 ms at the barrier, handed the
archive 800 ms of its own, and got a successful answer after 1,117 ms. Splitting
it is what stops either half starving the other, because waiting out the entire
deadline honours it and then reaches the archive with nothing to spend, which
would turn a feeder that is merely behind into failed queries. That is the one
thing this barrier exists not to do.

**And a write's deadline bounds the work this system adds to it, which it did
not until 2026-09-21.** A multi-document write that matches archived documents
brings them up first, and that copy-up is work the client cannot see and did not
agree to. It carried no deadline at all. Measured on 3,000 documents held by the
archive alone: an update asking for `maxTimeMS: 1` came back in **595 ms**, and
the same update against an untiered collection came back in **1 ms**. The error
code was right both times, which is why nothing noticed: what was wrong is that
the bound meant nothing, and an application sets `maxTimeMS` precisely to shed
load. The copy-up now carries half the caller's budget, and the same command returns
in 10 ms against the control's 2 ms.

Half rather than all of it, which is the same split the barrier above uses,
turned round. There, spending the whole deadline on the wait would leave the
archive read nothing to spend. Here, spending it all on the copy-up fails
nothing but overruns: the client's write is forwarded carrying its original
`maxTimeMS`, so mongod starts the clock again and the command could take twice
what was asked for. Half bounds the work this system adds to half the budget and
leaves the write the client actually sent with its own.

What that changes for a deployment: a write with a deadline too short for the
copy-up it needs now **fails** with `MaxTimeMSExpired` where it used to succeed
slowly. That is what one cluster does with a deadline it cannot meet, and it is
reported with MongoDB's own code rather than this router's generic one, so a
driver can tell a deadline from a refusal. A write with no `maxTimeMS` is
unchanged and still copies up everything it matches.

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

### A view is a query somebody else wrote down

A view is a saved pipeline over another collection. What makes it different from
every other way of naming a collection is that the pipeline is **stored on the
server and not sent with the query**, so every judgement the router makes about a
read of one is made without it.

Three things follow, and all three were wrong answers before they were rules.

**Which collection's claim decides.** A view names itself, and a view is never in
`feeder.namespaces` because it holds no documents of its own. So completeness is
judged against the collection at the end of the view chain, and a read the fast
tier cannot answer goes to the archive only when the archive holds the same view,
defined the same way. Anything else is refused: running a different pipeline
against the archive would answer a different question, which is worse than an
error.

**What the view itself joins.** A `$lookup` in a view's definition reads a second
collection that the query names nowhere. It is judged the way a `$lookup` the
client wrote is judged, and the rule is not the primary's: a join reads by key
rather than by time, so nothing less than the whole collection makes it whole.
This is asked of any view, not only one over a tiered collection. A small
reference collection nobody tiers, with the tiered history joined to it, is the
shape reporting most often takes, and it is a view whose base is untiered.

It stays a refusal rather than an archive answer. The archive's copies carry
tombstones, and the predicate that filters them goes in front of a pipeline: it
cannot reach inside a definition the router has no way to rewrite. Answering it
there would join deleted documents.

**Every path that reads.** A `find` is judged on the way back, an `aggregate`
before it runs because it can end in `$out`, `count` and `distinct` in a third
place, and a point read by `_id` skips the completeness guard on purpose because
a lookup by exact id matches at most one document. That exemption is about how
far back a query reaches and nothing else: a row found by its `_id` comes back
with the same short join array as one found by anything else, so the view
question is asked there too. `explain` publishes the same verdict, because a plan
it leaves untouched says the fast tier can answer the query whole.

A view is not a collection, either. Nothing is promoted into one, the indexer
builds no indexes on one, and a tiered `$out` reading through one is refused
rather than performed against a short input.

**And a dropped view is dropped in the archive too.** Collection DDL other than
rename is logged and never replayed, and that rule is about documents: replaying
a drop would destroy the history the archive exists to keep. A view has none.
Leaving it cost three things, and the first is the one that shows: the startup
report names it for ever, and that report is what step 1 of the runbook tells an
operator to read. A rollback repoints the application at the archive and finds a
view it dropped. And recreating the view with a different pipeline leaves the
archive holding the old one, so a read the fast tier cannot answer is refused
where it could have been answered.

The guard is the whole of it and it is one line: the archive's copy is dropped
only when what the archive holds under that name **is a view**. A collection is
never touched, whatever the fast tier had.

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

That sentence was written before it was true of every write, and the gap it
hid is worth stating, because it is the shape this door keeps taking.

**A write that reaches one document was never guarded at all; it was repaired
afterwards.** `interceptWrite` and `interceptFindAndModify` read the reply, see
that the fast tier matched nothing, resolve the filter against the archive, copy
the document up and run the statement again. That is a good design for a write
with a reply, and there is no reply here. The pre-write guards covered
multi-document statements, upserts, inserts, `bulkWrite` and aggregations, and
a single-document `update`, `delete` or `findAndModify` fell between the two.

Measured on a collection holding three documents, two of them in the archive
only:

    updateOne({ref: "a1"}, {$set: {status: "shipped"}}, {w: 0})
      before   status stayed "new" on both tiers; the write landed nowhere
      after    "shipped", and the fields the fast tier could not see survived

    deleteOne({ref: "b1"}, {w: 0})
      before   the document was still readable through the router
      after    erased

    findAndModify({status: "new"}, sort: {seq: 1}, remove: true, {w: 0})
      before   removed seq 9, the oldest document the fast tier happened to
               hold, and left seq 3, which is the one the sort names
      after    removed seq 3

Naming the document by `_id` rather than by another field is the same door:
`interceptWrite` resolves that on the reply as well.

The fix is the repair moved in front of the write, for unacknowledged writes
only. One document, not every match: a single-document statement acts on one
document however many it matches, and copying every match up would turn an
unacknowledged `updateOne({status: "new"})` into a warm of the whole archive.
The fast tier is asked first and a match there ends it, because without a sort
any match will do, which is all MongoDB promises. An acknowledged write takes
exactly the shortcuts it took before and pays nothing new.

**Not every guard could be moved in front of the write, and one command is
refused instead.** A unique index dropped through the router has one to drop
behind it on the archive, or the archive goes on refusing values the router now
accepts. Both halves of that live on the reply: which indexes are going has to
be read before the fast tier removes them, because afterwards there is nothing
left to ask, and the archive's copy is dropped only once the reply says the fast
tier accepted it. Doing it before forwarding would drop the archive's index for
a command the fast tier might turn down.

Measured in the adoption shape, the archive carrying its own unique index:

    acknowledged     both tiers end with _id_, and both duplicates mirror
    unacknowledged   the fast tier ends with _id_, the archive keeps code_1,
                     and of two documents the application was now entitled to
                     write, one reached the archive and one did not

That second document lives on the fast tier alone. Nothing tells the
application, and the evictor will not remove a document the archive does not
hold, so it is not lost; the archive is simply incomplete from then on. So an
unacknowledged `dropIndexes` on a tiered collection is refused, which is the
`applyOps` reasoning: a refusal is a limitation and a wrong answer is a bug.
Nothing is lost by refusing, because an operator dropping an index is present to
see that it is still there, and no driver sends DDL fire-and-forget.

**A `create` carrying options is the second, and the only one here that never
heals.** A collection's options are carried to the archive by
`MirrorCollection`, which also runs on the reply. Without one the archive's copy
is left to the feeder's first mirrored insert, and that carries no options at
all. Measured on a tiered namespace, `create` with
`collation: {locale: "en", strength: 2}`:

    acknowledged     both tiers en/2, and find({name: "ALICE"}) is 1 on both
    unacknowledged   the fast tier en/2 and the archive none, and the same
                     find is 1 on the fast tier and 0 on the archive

At strength 2 `ALICE` matches `alice`, so the answer depends on which tier
answers. A collation is fixed at creation and cannot be added to a collection
that already holds documents, so unlike everything else in this section that
divergence is permanent.

So an unacknowledged `create` on a tiered namespace is refused **when it carries
something the archive needs**. A bare `create` goes through, because the
collection the feeder's first insert makes is the same collection. Which options
count is asked of the same list `MirrorCollection` carries by, rather than
written down a second time here.

`testunack.sh` pins all of it, along with the controls that an ordinary
unacknowledged write still lands on both tiers, that an acknowledged
`dropIndexes` still drops on both and an acknowledged `create` still carries its
collation to both, that a bare unacknowledged `create` still goes through, and
that an untiered collection's index and collation are none of this system's
business whatever flag is set.

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

That list used to have four entries and there were five. The copy-up a
transaction does, which is how a transaction reads past what the fast tier
holds, passed none of them. It is the worst place to leave out, because of what
the answer means: matching nothing reads as the fast tier already holding
everything the query matches, so the transaction proceeds on the fast tier alone
and is short, with no error on it. Measured on a document held by the archive
alone, `{name: "Bob"}`, read inside a transaction as
`find({name: "bob"}).collation({locale: "en", strength: 2})`: the router
answered **0** and mongod answered **1** on the same data untiered. The same
call now takes the caller's `let`, collation and `maxTimeMS` as one value, so a
field added to it reaches this path too. `testtxn.sh` checks it with mongod as
the control.

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

**A field that holds an array is indexed once per element**, which is what
multikey means, so no two documents may share *any* element. The registry claims
one key per element for exactly that reason, and every path that claims a key
claims all of them: the insert batch, the update post-image, `$merge`, `$out` and
`cline keys --rebuild`.

**Creating a unique index is checked and claimed before it is built.** Two
things were wrong with a unique index on a tiered collection, and one walk of
both tiers answers both.

The documents in the archive were never claimed. They are the only ones the
constraint needs help with -- the fast tier's own index enforces itself over what
the fast tier holds -- and none of them was ever written through a router that
knew about this index. Measured with the index created after the document was
archived: the archive holds `held@x.com`, the index is created through the
router, and an insert of a second `held@x.com` was **accepted**. It was written
down, in the rollout list saying to run `cline keys --rebuild` afterwards, and a
documented step is not a guarantee.

And the build itself was allowed over data that already violated it. mongod
refuses `createIndex` with `unique` when two live documents share a key; split
the pair across the tiers and the fast tier's build sees only one of each, so it
succeeded. Measured with two documents holding `sku: "x"`, one on each tier:
mongod refuses the same pair in one collection with 11000 and the router
answered ok. The application is then told a constraint holds that does not.

So the walk runs **before** the build, not after it, and that is the only order
in which the answer can be a refusal: an index already built cannot be un-built
without taking away something the client was told it had. Duplicates come back
as 11000 with the key named, which is what mongod returns for the same command
over the same documents -- this is the one refusal here that is not announcing a
limitation but giving the server's own answer, so a driver that handles a failed
unique build handles it unchanged. Otherwise the keys are claimed and the build
goes ahead already covered, and the claims go back if the fast tier turns the
build down for its own reasons.

The cost is that `createIndexes` waits for a walk of both tiers, next to a build
that scans the fast tier anyway. It runs only on the router that took the
command, only for a spec that asks for `unique: true`, and only on a tiered
namespace. If the walk cannot be done at all the build is let through with a
warning, because refusing a command over a bookkeeping collection that could not
answer is a worse failure than the one being prevented. `cline keys --rebuild`
is still the answer for an index created out of band, for a fast tier rebuilt
from nothing, and for a cluster adopted with data already in it.

**`collMod` reaches the same state, and goes through the same walk.** An index
does not have to be created unique to become unique:

```
db.runCommand({collMod: "orders", index: {keyPattern: {email: 1}, prepareUnique: true}})
db.runCommand({collMod: "orders", index: {keyPattern: {email: 1}, unique: true}})
```

Measured on 6.0.26 before this was guarded, with one live document in the
archive duplicating a value the fast tier holds: `createIndexes` with
`unique: true` was refused with 11000, and both `collMod`s above were accepted.
They left a unique index over a value held by two live documents, with **zero
rows in the key registry**, so every later insert of that value was accepted
too.

Both spellings of the index are covered, `keyPattern` and `name`. The index is
read from the fast tier rather than from the command, because a `collMod`
carries only the change: `sparse`, `partialFilterExpression` and `collation`
belong to the index as it already exists, and claiming keys without them would
claim keys for documents the index does not index.

`prepareUnique` on its own is left alone. It stops the fast tier taking new
duplicates, which is a narrowing rather than a claim, and the conversion is the
moment the constraint starts being asserted about documents this tier cannot
see.

**Dropping the index releases the claims it made.** Claims are permanent on
purpose -- a deleted document leaves its row behind, and the row is resolved
when something collides with it -- and that reasoning is about documents. For an
index that has been dropped the row is a claim about a constraint that no longer
exists.

Leaving them is not merely untidy, because index names are derived from the key
and the options and are therefore reused. Create the same index again and its
rows are already there, so the walk that follows can insert none of them, and
the documents that really hold those keys never get a row. Their keys then read
as free: an update moved onto `{ items.sku: "s9", n: 34 }`, the row for it named
a document that had long since moved off that value, the claim was judged stale
and taken over, and the archived document that really held it was never asked
about. mongod refused the same write on an untiered copy. The fuzzer needed nine
rounds of a hundred and twenty operations to build that state.

The release is driven by the listing rather than by parsing a `dropIndexes`, so
an index dropped out of band releases its claims too, and it is the same
comparison that reports a constraint has gone.

Reading one value per document was wrong in both directions and neither showed
up as the other. A document holding `["x","y"]` mirrored and then evicted, and a
second holding `["y","z"]`: the router accepted it, the fast tier accepted it,
and the archive refused the mirrored insert with `E11000 dup key { tags: "y" }`,
leaving a document on the fast tier that the archive does not have and the
evictor will not remove. And a unique index on a path that crosses an array,
`items.sku`, could not be looked up at all (the driver answers "element not
found"), so every such document keyed as null, the first one claimed that key for
the namespace, and every insert after it was refused as a duplicate of a document
it has nothing in common with. A wrong answer and a false refusal from one
missing idea.

What this costs is proportionate and worth knowing: a document with a thousand
elements in an indexed array claims a thousand registry rows, because mongod puts
a thousand entries in the index. The registry lives in the archive, so those are
round trips to the other cluster, batched into as few as the driver will take. A
unique index on a large array is expensive to write through this router, and it
is expensive in mongod too.

The rules are mongod's and they are pinned by unit tests: one entry per element,
duplicates within a document collapse to one key, an empty array indexes as
`undefined` and two of them collide, an array inside an array is a value rather
than another level, an element missing the path indexes as null, and a compound
index takes the array from one field and the scalar from the other. Two array
fields in one compound index is what mongod calls parallel arrays and refuses, so
nothing is claimed for it: the write fails on the fast tier before any of this
matters.

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
whatever else maintains the deployment, and watch
`thermocline_key_registry_rows` to see whether you are: it is how many claims
the fast tier holds, and nothing else would have told you it was growing. If it finds one key held by two live
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
