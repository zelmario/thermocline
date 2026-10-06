# Production readiness, as a list that can reach zero

Every session before this one ended the same way: defects were found, so the
answer was "not ready". That is a treadmill. Any database product yields defects
under adversarial probing, so "a session that finds nothing" is not a condition
that can be met, and measuring by it says nothing about risk.

This file replaces that with a finite list. It is the only place that answers
"are we ready", and the answer is whether the **Open** section below is empty.

## What is already true

Measured, not asserted. Re-run these before believing them; the commands are in
`docs/testing.md`.

| Evidence | Result |
|---|---|
| In-node suites, 36 of them | 1,843 checks, 0 failed |
| Host suites, 10 of them | 195 checks, 0 failed |
| The same suites with the query cache on | 1,768 checks, 0 failed |
| Differential fuzzer, 10 rounds × 200 operations | 3,464 agreed, 0 disagreed |
| The same, three seeds, after $rename and pipeline updates were added | 0 disagreed |
| Pilot: MongoDB's sample datasets adopted as an archive, 425,367 documents | 199 questions, 0 disagreements |
| Pilot write, change stream and transaction batteries | 22, 5 and 15 checks, 0 failed |
| Pilot soak, 691 rounds | 0 disagreements, 0 stale reads, 0 panics, flat memory |
| Rolling upgrade under traffic, both orders, against an equal-version control | 25 checks, 0 failed |
| Router startup, 160 tiered namespaces with data on both tiers | 1.3 seconds |
| Two applications writing through one router for four hours | 2,830,950 operations, 0 errors, 0 violations |
| The same two, 45 minutes against the guards added since | 637,747 operations, 0 violations, 0 refusals |
| The same two again, four hours, 2026-09-17 | 2,494,225 operations, 0 violations, 0 refusals, 31 stale reads in one second |
| The fuzzer's change streams, 20 rounds under three other suites | 365 minutes, 0 disagreements |
| In-node suites again, 2026-09-21, after a month of guards | 1,920 checks, 0 failed, 34 minutes |
| The four daemons run as systemd units, against a real systemd | 30 checks, 0 failed |
| Losing the fast tier and rebuilding it from the archive | 33 checks, 0 failed |
| In-node suites after the day's second half | 1,932 checks, 0 failed |
| Host suites after it | 310 checks, 0 failed |
| Two applications through one router, 30 minutes, after the day's changes | 474,588 commands, 0 refused, 0 stale reads, 0 duplicate keys, 0 panics |
| A router restarted under both of them writing | stopped in 1s, ready in 1.0-1.1s, every error a refused connection and no invariant broken |
| In-node suites, 2026-09-23, after the collation audit and the residency fix | 1,990 checks, 0 failed, 32 minutes |
| The same with the query cache on | 1,975 checks, 0 failed |
| Host suites, 12 of them | 322 checks, 0 failed |
| Two applications through one router, 30 minutes, with a change stream consumer tailing | 8,375 commands, 1,440 copy-ups, 0 phantom events, 0 violations |
| The fuzzer with pipelines that write their answer down | 489 compared, 0 disagreed |
| In-node suites, 2026-09-28, after the day's key and pairing fixes | 2,025 checks, 0 failed |
| Host suites, the same build | 328 checks, 0 failed |
| The fuzzer with collated collections and mixed numeric types, 20 rounds of 150 | 4,777 agreed, 0 disagreed |
| The same with change streams compared | 39 events on each side, 1,743 agreed, 0 disagreed |

## What the last two days found, and what that says

Eight defects on 2026-09-22 and 23, all measured against an untiered mongod and
all fixed. **Five were silent wrong answers to an application**, and two of
those wrote or deleted the wrong record:

| | |
|---|---|
| a sorted single-document write acted on the wrong document | wrong record |
| a collated update created a duplicate unique key across tiers | wrong record |
| `$out` and `$merge` persisted a wrong `$group` | written down |
| a transaction's read answered short, with no error | short answer |
| a write could not see what a read of the same filter could | short answer |
| a copy-up reached a change stream as an order nobody placed | phantom event |
| a collated unique index refused writes mongod accepts | false refusal |
| the cache held answers nothing could tell it had changed | stale answer |

That is not a flattering number and it belongs here rather than in a commit
message nobody re-reads.

**The reading that cuts the other way.** Six of the eight came from one question
asked systematically: does a query this router builds against the other tier
reproduce the comparison mongod already made? A collation arrives two ways and
only the explicit one travels with the command, so the read path had been right
since it was written and the write paths never were. That audit is finished and
its boundaries are written down in `docs/router.md`, including the two places
that must **not** carry a collation, `_id` and `files_id`. It was not a random
sample of unknown territory.

**What they say about the tests, which is the actionable part.** Every one was
reachable only because the workload gained something an ordinary application
does and this one did not: a case-insensitive unique index on the collection
that actually gets evicted, deletes, fields that half the history predates,
paging, a transaction spanning two mirrored namespaces, and a consumer tailing a
change stream while the tiers moved underneath. Before that, every question the
shop asked had an answer that depended on a *value*, never on a comparison, an
ordering, or an event.

The eighth is the clearest case. A shop's search indexer tails its orders all
day; nothing here did, and within one run a copy-up arrived as an order nobody
placed. The gap was in what the tests **do**, not in how many checks they run,
and that is worth more to the two items still open below than another thousand
assertions would be.

## 2026-09-24: the ninth, and it was the procedure again

The rehearsal that had never been run to the end ran to the end, and the
application repointed at the archive accepted three duplicate order references
in sixty rounds.

Nothing copies an index to the archive, on purpose, and that reasoning is sound
while the router is in front of it. Rollback takes the router away. Uniqueness
across the tiers is enforced by the key registry, which lives on the fast tier
this procedure switches off, so the constraint the application declared was
enforced by nothing and MongoDB reported each duplicate as an ordinary
successful insert. `cline rollback` named the accounts, the views and the
collections the archive had never held, and said nothing about the indexes.

It names them now, constraints listed separately from optimisations, with the
`createIndex` calls and the collation carried along, and the rehearsal runs
those commands rather than reading them: `test/testrollback.sh`, 22 checks.

**Why it is here and not in the table above.** It is the same shape as the
catalogue defect of 2026-09-03 and it was found the same way, by walking the
procedure rather than testing the command. Both were reachable only once the
shop declared a constraint of its own, which is the point already made above
about what the tests *do*. This is the second thing the rollback procedure lost
that the rollback command was happy about, and that is the number worth
watching: two of the four walked procedures have now been short twice.

## 2026-09-24: the tenth, and the workload found it rather than a probe

A shop tailing its own orders was told an order had been placed that nobody
placed. The document was a seeded one, two months old, living in the archive: a
copy-up delivered to the application as its own insert. The fifth defect of this
family and the first that a standing test found on its own.

`endedAt` on a residency record has two writers. The feeder writes it when the
application deletes the document. The evictor writes it as it evicts,
deliberately, so the row outlives the hold for a change stream to read. The
sieve's first question was whether the insert happened after the record ended
and it assumed the first meaning, so a copy-up whose hold the evictor ended 73
milliseconds earlier arrived as somebody else's write.

**What is worth taking from it.** The fix is one line moved, and finding it took
a log line that did not exist: the sieve said when it hid an insert and said
nothing when it let one through, so a phantom reaching a consumer left no trace
on this side at all. The only evidence was an application noticing, and nothing
in this deployment was an application until the shop gained a consumer that
tails while the tiers move.

**And what it says about the count above.** The discovery rate has not fallen.
Nine and ten came on the same day, from the two things added that day: walking a
procedure to its end, and a workload doing one more thing a real application
does. Neither was a new kind of test. Both were the existing tests asked to
finish the job.

## 2026-09-25: three more, from a second router

The next store addition had to be chosen rather than taken, and the choice was
the shape every deployment with two app servers has: two applications writing
the same collections, each through a router of its own. `test/store/shared.sh`.
Its control against plain MongoDB was clean. Its first run through the routers
was not, and neither were the next three. Each defect below was measured before
it was fixed and measured again after.

**A feeder that is never idle published nothing.** It wrote its buffer when 500
events had arrived or the stream had been quiet for 20ms. A stream carrying two
shops, a warmer and an evictor is never quiet for 20ms, so the position stood
still for as long as a batch took to fill. Measured on a lone feeder at about 65
inserts a second: gaps of 3,626ms, 4,078ms and 6,497ms in three runs before the
fix, none over 208ms in four runs after, apart from the pauses of the whole
node described two paragraphs down. A router calls two seconds without a
publication a stopped feeder and stops waiting for mirroring, and each shop
reported three placed orders its own count did not include. The buffer is
written within 100ms now whatever the stream is doing.

**The evictor ended a hold it had never judged.** It checks holds when it
chooses a batch and ends them once the delete is done, and a router can place a
hold in between. The oplog gave the order to the millisecond: a hold at 27.754,
the eviction at 27.794, the hold ended at 27.822, the other router's copy-up at
27.833. Both routers then read the copy-up as the application writing the order
again, and both shops were shown two seeded orders as orders just placed. The
evictor ends only lapsed holds now, which are the ones its batch was chosen
under. `TestEvictionEndsOnlyTheHoldsItJudged` failed before the change.

**A read gave up on a feeder that was only paused.** This box stops every
process in node0 for about 2.6 seconds now and then, and nothing in any mongod
log says why. The router's read was prepared to wait five seconds and gave up on
the feeder at two, so a pause shorter than the read's own patience produced six
wrong answers in one second. A position quiet for less than `archive_wait` is
now a feeder that is behind, and the read waits for it. A feeder that has really
stopped costs one `archive_wait`, once. The warning says which of the two states
it found, because "published no position at all" sent the diagnosis looking for
a harness fault from the day before when the record was there.

**What it says about the count.** Three more, from one more thing a real
deployment does, and a fourth by reading, item 9 below, which measuring showed
was two. The discovery rate has not fallen, and the shape of the finds has not
changed: each needed two processes that each hold a view of the same state.

## 2026-09-25, evening: the next store addition, and a join it did not ask about

The addition was a consumer that falls further behind than the oplog, and
recovers: told history is lost, it reads every order through the router and
tails from where it noted, and what it keeps is judged against the tiers
themselves. It found nothing wrong. Nine resyncs each in a six-minute run
through two routers, the router passed history-lost every time, and both
indexes matched the tiers exactly at 12,267 orders.

The same run's activity report did find something. A `$lookup` from events to
orders showed an event whose order did not exist, once. No state of either tier
had held that: the shop writes the order first. **The feeder wrote each flush a
collection at a time**, so for the length of one flush the archive could hold a
later write to one collection without an earlier write to another, and the
freshness barrier does not cover it, because it only promises everything up to
the read's start. `test/crossns` measured it directly: four writers inserting an
order then its event, the archive polled, **5,438 of 14,132** events seen without
their order.

Writing each flush in oplog order fixed it and cost 93% of mirroring on
interleaved writes, 222 documents a second, because batching and a global order
are at odds. A transaction per flush keeps the batching and removes the partial
state. After: **0 sightings** in three runs, and mirroring faster rather than
slower, an interleaved backlog at 10,327 to 11,117 a second against 3,096 to
3,378 for the build before, because the archive's write concern is waited on
once per flush instead of once per collection. A standalone archive cannot run
transactions, so there it falls back and warns; that is written in
`docs/operations.md`.

## 2026-09-25, night: the evictor and warmer at once

Every store run had driven the warmer and one evictor sweep in turn from a
single loop, which is not a shape any deployment has: the evictor is a resident
daemon and the warmer an hourly timer beside it. Run that way, the first
eight-minute two-router run found two defects, both silent wrong answers, and
both measured before and after.

**A copy-up undid a delete.** Every copy-up reads the archive, and none waited
for mirroring the way a read does, so one landing between an application's
delete and its tombstone found the document live and put it back. The store saw
a cancelled order live on the fast tier a second later. Deleting a document
through the router and then updating it by `_id` matched the deleted document
**40 times of 40**, where MongoDB matches nothing. Copy-ups clear the read
barrier now: 0 of 40.

**The feeder claimed a transaction mirrored when half of it was.** Every
operation of a transaction carries the commit's cluster time, the feeder
claimed an event's own time as soon as it had written it, and a flush can fall
inside a transaction. The barrier cleared on the commit's time with **499 of
600** events in the archive, five rounds of five. An event now proves only the
time before it: 600 of 600.

After both, the same run: 0 violations in either application, both consumers'
indexes exact against the tiers at 13,475 orders.

**What it says.** One more thing the deployment really does, and two more
defects, both about the same claim: what the archive is allowed to be assumed
to hold. The discovery rate has not fallen.

## 2026-09-26, early: every reader of the archive, not only the reads

The copy-up defect of the night before was one reader of the archive missing
the freshness barrier. So every place the router reads application documents
from the archive was listed and asked the same question, and four more were
deciding something from an archive that had not yet been told about a delete.
Each was measured through a router against MongoDB's answer, before and after:

| decision | before | after |
|---|---|---|
| an `_id` or unique value re-used after its delete, refused | 20/20 | 0/20 |
| a case-insensitive unique value re-used, refused | 15/15 | 0/15 |
| a delete by filter, leaving the wrong document | 15/15 | 0/15 |
| an update event's document older than the update | 13/20 | 0/20 |
| a warm copying back documents deleted while mirroring lagged | 25/25 | 0/25 |

The waits are only on the paths that needed them: before a refusal, when the
fast tier matched nothing, when its lookup came back null. The warmer waits per
batch, and had skipped the check on the grounds that "it only adds to the fast
tier"; with no feeder it now stops rather than warm, and `--without-feeder` is
the explicit override for a fast tier refilled with nothing deleting. Each has a check in
`testfresh` or `testchange` that fails on the build before.

**What it says.** Six of the day's defects were one missing guard in six
places. The list of places that read the archive is now written down in the
commit, and a new one has to be added to it.

## 2026-09-26, morning: the evictor lost acknowledged writes

The same question asked of the evictor, the one daemon that removes data from
the fast tier: what does it prove before removing a document? That the archive
holds the `_id`. A document updated a moment earlier has the previous version
in the archive until the feeder mirrors the update, and evicting it removed the
only copy of the new one. The feeder's lookup for the update came back empty
and was skipped, because a delete was coming, and the delete that came was the
eviction. **Twenty updates acknowledged to the client, one sweep, and the router
answered the old value for all twenty.** The window in production is the
mirroring lag the evictor tolerates, up to `max_feeder_lag`, 30 seconds.

It evicts only a document whose archived copy is byte for byte the one on the
fast tier now, and each delete insists on that document. After: all twenty
kept. The sweep's own count said "evicted 20" for a sweep that had removed
nothing, and counts what it removed now.

**What it says.** The worst defect of the day, data loss rather than a wrong
answer, and it sat in the daemon whose one job is to be safe about removing
things. It was found by asking the question of the reader audit the other way
round: not "does this read wait for the archive", but "what does this removal
assume the archive already has".

## 2026-09-26, afternoon: large documents, and what two of today's fixes cost

Attacking the day's own fixes with large documents found one stall and two
memory problems, each measured before and after.

**Mirroring stalled on large documents** (848e324). The transactional flush of
65a0469 was bounded at 64MB, and flushes of 12MB documents were refused by a
1GB archive cache as `TransactionTooLargeForCache`. The feeder retried the same
flush for ever: 13 of 60 documents mirrored in two minutes, mirroring five
minutes behind for every namespace. The first measurement of that bound had
used 4MB documents, whose flushes never reached it. The bound is 16MB now, and
a flush the archive still cannot hold is written a collection at a time.

**The evictor and the warmer held whole batches of whole documents.** 2,701MB
to evict 60 documents of 12MB after 0be762a, 1,874MB to warm them; both work in
bounded steps now, 406MB and 345MB. The warmer's bound needed care, because it
reads a short batch as the end of its range and would have claimed documents it
never copied.

Two suite faults ran only in a certain order of suites on a worker: a claim
left behind by an earlier warm, and a claim's boundary printed to the second.
The second was a tool fault as well, and `cline watermark` prints the boundary
exactly now.

**What it says.** The largest defect of the afternoon was introduced this
morning, by a fix. Every fix today that bounded something was measured with an
input that could not reach the bound, until it was measured again with one that
could.

## 2026-09-26, evening: what a large document does to a change stream

Three more from the same question, what a large document does, each measured
before and after.

**One update stopped mirroring for every namespace.** The feeder's stream asks
for `updateLookup`, so an update event carries the whole document beside
`updateDescription`. An 8MB document given a 7.5MB field in one `$set` made an
event of 24,117,718 bytes, which mongod refuses to return. The feeder reopened
at the same token once a second, and nothing after that update reached the
archive. The stream now projects only the six fields the feeder reads; a
document ending 4KB under 16MB mirrors, and so does the write after it.

**A resumed change stream could never be read by the Go driver.** An update
event whose lookup came back null is a few hundred bytes, so mongod packs up to
a thousand into a batch, and the router fills each with the archive's copy.
200 documents of 1MB, updated, evicted, and read from a token taken before the
updates, came back as one batch of 209,826,801 bytes. The Go driver refuses a
message past 48MB, retried, was sent the same batch, and gave up after 28
seconds, on every resume. The router now cuts a filled batch at 16MB and holds
the rest for the cursor's next getMores, with the resume token following the
cut. Resuming in the middle gives 185 after 15 with none twice; a getMore from
another session or naming another collection is refused as mongod refuses it.

**An _id past about 5KB could stall mirroring for good.** The resume token
encodes the document key, so an event holds the `_id` three times. A 7MB `_id`,
which mongod accepts, made an insert event of 29,360,471 bytes, and no reader
can get past one mongod will not return. The router now refuses an `_id` past
4,096 bytes on every write path into a tiered collection, and the feeder says
once what stopped it when a write made straight at the fast tier does it. See
`docs/limitations.md`, which also has a unique value past about 12MB going
unchecked against the archive.

One failure in the sweep on ea87c71, testchange's "a lookup is answered from
the archive", was chased on 2026-09-27 and is not this system's. mongosh's
`watch()` with no start option calls `tryNext()` once to open the cursor and
discards what it returns. A wire tap between mongosh and the router showed the
update delivered to mongosh in that first getMore, 4ms before `watch()`
returned; against a plain mongod with no router, an insert timed to land there
was lost 3 times in 20. The suite's watchers that wait for another process's
write now open at an explicit start, which skips that call.

**A multi-document write held its copy-up by count.** An updateMany over 60
archived documents of 12MB peaked the router at 2,044MB; bounded at 16MB a
batch, 393MB.

## 2026-09-27: a field name mirroring could not replace

**One update to a document with a `$`-prefixed field stopped mirroring for
every namespace.** Field names that start with `$` or hold a dot are legal
since 5.0, and an insert of one mirrors like any other. The feeder mirrors an
update by replacing the whole document, and mongod refuses a replacement with
a top-level `$` field, so `{$price: 5}` inserted and mirrored, then one `$set`
on another field failed every flush, once a second, and a later insert never
reached the archive. Such a document is now written with a pipeline update
that replaces it with its own literal value, which mongod accepts and which
stores it byte for byte, so the evictor can still match it; every other
document keeps the plain replace. `cline backfill` had the same write and uses
the same helper. testlife's new section fails on the previous build and passes
on this one.

## 2026-09-27: numbers compared by their bytes

**The router admitted a second document with an `_id` the archive held.** The
insert guard asked the archive with `$in`, which matches by value, and mapped
the answer back to the batch by the raw bytes of the value, which a comment
defended on purpose. mongod's `_id` index compares numbers by value, so
NumberLong(8) and 8.0 are one `_id`. Measured with the fast tier's copies gone
as an eviction leaves them: 8.0 beside an archived NumberLong(8),
NumberDecimal("9") beside 9.0 and NumberDecimal("0.5") beside 0.5, all three
accepted, two documents with one identity.

**The key registry did the same for unique values that are not whole
numbers.** It already made one key of a whole number written four ways;
fractions and special values kept their bytes. NumberDecimal("0.5"),
NumberDecimal("2.250"), a decimal Infinity and a decimal NaN were each accepted
beside an archived double equal to it. mongod's rule, measured: a unique index
compares exact values, so double 0.5 and NumberDecimal("0.5000") are one key,
double 0.1 and NumberDecimal("0.1") are two, and every NaN is one. Both paths
now use one canonical form per exact value, and all seven duplicates are
refused. Claim rows written before this for such values are under the old
form; `cline keys --rebuild` claims them again under the new one.

The same, one level down: mongod compares what an embedded document or an
array holds by value too, and keeps field order, so `{a: 1}` and
`{a: NumberLong(1)}` are one `_id`. The canonical form now reaches inside them.
Measured with compound `_id`s: `{a: NumberLong("1")}` beside an archived
`{a: NumberInt(1)}`, `{x: NumberDecimal("0.5")}` beside `{x: 0.5}`, and
`{u: 1.0, d: [NumberLong("2")]}` beside `{u: 1, d: [NumberInt(2)]}` were all
accepted, and are all refused now.

And by collation: a collection's `_id` index takes the collection's default
collation, so under en/strength 2 `"A"` and `"a"` are one `_id`. The guard's
`$in` ran under that collation and found the archived `"A"`, then placed the
answer back by bytes and let `"a"` through, and `{k: "x"}` beside `{k: "X"}`
the same way. There is no collator here, so an answer that cannot be placed now
sends each unplaced `_id` to the archive on its own, under the collection's
collation. On the previous build the accepted duplicate also broke the next
testkeys section, on a different collection, every time; with the section left
out that build passes 230/0, which says the duplicate reached further than its
own collection.

## 2026-09-28: one insert naming a key twice

Two more from reading `archivedIDClashes` and `claimKeys` against what mongod
does with a batch, both measured before and after.

**An unordered insert naming an archived `_id` twice let the second through.**
The guard remembered only the first batch position of each `_id`, so only that
one was refused and the fast tier, holding no copy, took the second: a
duplicate of the archived document. `[{_id:5},{_id:5},{_id:6}]` inserted two
where plain mongod inserts one; every position is flagged now.

**Two new documents with one unique value in one batch left the one that
landed unclaimed.** The second found the first's fresh claim, owned by a
document no tier held yet, read it as stale and took it over, and its release
when the fast tier refused it removed the only row. Once the first was
archived, a third insert of the value was accepted beside it, ordered and
unordered alike. A later document of a batch with a key an earlier one claimed
is now refused at once, as mongod refuses it.

**A write that landed in part lost the claims of what landed.** After any
write that reported an error the router released every claim the command had
made. An updateMany that changed its first document and was refused on its
second, and an unordered insert the fast tier refused one document of, both
left the documents that had landed with no claim. The same shape is two clients
inserting one new value at once: the second takes over the first's fresh
claim, the first lands, and the claim then names the refused one. Measured, 30
rounds of two concurrent inserts: 4 left the survivor unclaimed before, 0
after. A claim is now kept when its owner holds the key, moved to the fast tier
document that holds it when the owner does not, and released only when nothing
does.

## 2026-09-28, evening: a collated key and an eviction in the same second

**The two-application shop found a reference accepted twice.** The shop's
references are unique under a case-insensitive collation, and "ref003955" was
accepted beside an archived "Ref003955", once in about 3,000 attempts to reuse a
reference in 30 minutes. The router asked the archive under the collation, found
the order, and passed it over because the fast tier held it too, leaving the
refusal to the fast tier's own index; the evictor, sweeping 200 old orders at a
time, removed that exact order from the fast tier in the same second, and
nothing was left to refuse the second spelling. The router now passes over an
archive match only when the fast tier holds that document with another value,
which is the stale copy the rule exists for, and refuses otherwise, which is
mongod's answer while the fast tier holds it and the only right one once it
does not.

The `_id` guard has the same shape and accepts it on purpose: when both tiers
hold an `_id` it hands the insert to mongod, so a driver's retry of an insert
that succeeded is deduplicated rather than refused, and its comment names the
cost, an eviction of that exact document in between letting the insert land and
the feeder mirror it over the archived copy. It calls the window a
millisecond. The shop has now met the collated form of that window in thirty
minutes, so the estimate is too kind. It is closed now; see Open item 10.

## 2026-10-05: told it failed, and it had landed

**The shop's "phantom" after a step-down was its own order.** The two
application shop with a step-down every minute reported an order placed that
nobody placed, about once in ten step-downs since 2026-10-02. On a 30-minute
run two of them came 2 and 3 seconds after a step-down, and the ledger showed
the reporting application had placed both. Its insert had returned an error,
so the shop forgot the order, and then the insert event arrived. That half was
the harness, which now keeps an order whose insert failed as uncertain rather
than forgetting it.

The other half was the router. One of the two errors was `NotWritablePrimary`
for an insert that had landed. The old primary applied the write, then the
step-down interrupted its wait for a majority, and the reply was `ok: 1` with
a write concern error. The router read that as success, so the connection
stayed on the old member, the driver's retry went out on a sibling still there,
and the old member, now a secondary, refused it. Straight to the replica set
the retry reaches the new primary, which knows the transaction number and
answers success.

`test/landed` measured it on its own, Go driver with no deadline, 32 workers,
a step-down every 20 seconds for 3 minutes:

    build                     errors   landed anyway   as NotWritablePrimary
    38545d7, through router       31              30                      25
    this fix, through router       0               0                       0
    straight to the set            0               0                       0

A step-down write concern error is now a failover reply, so the connection ends
after the reply and the retry finds the new primary. With `terminate_auth` the
router replays the write instead, which the transaction number deduplicates;
both builds had 0 errors that way. Attacking the fix found the replay would
also have resent an aggregate ending in `$merge` or `$out` that had already
run, and so would the existing replay after a broken connection. An aggregate
that writes is no longer replayed.

**The fuzzer found a unique key taken twice, on the build pushed that morning
too.** Churn round 8 of `testfuzz`, the same operation on four runs and both
builds: a pipeline update of `{ _id: 19 }` to `n: 20`, refused E11000 by the
untiered reference and accepted through the router. The router's debug line
said the guard had matched 0 documents. Both documents were archived. The
guard asks the fast tier what a statement matches, which was nothing, so it
claimed nothing; the update then matched nothing either, the miss path copied
document 19 up, and re-sent the command with no guard. Reproduced without the
fuzzer, both documents in the archive only and a unique index created through
the router:

    statement onto the archived key     before                       after
    pipeline update                     accepted, 2 holders          refused 11000
    $set                                accepted, 2 holders          refused 11000
    findOneAndUpdate                    accepted, 2 holders          refused 11000
    a move to a free key                accepted                     accepted

The miss path now runs the same guard before re-sending an update or a
findAndModify. The two other paths that copy a document up for a write, a
sorted findAndModify and an unacknowledged write, run before the guard and
were already covered. `testkeys` has the case now, with the updated document
archived as well as the key's holder; every earlier section kept the updated
document on the fast tier.

## 2026-10-06: an update that missed a document being evicted

**An `updateMany` reported success and skipped one document.** The sweep on
2026-10-05 had one disagreement in the fuzzer's churn round 2: an `updateMany`
with `$inc: { n: 2 }` modified 55 documents through the router and 56 on the
untiered reference, and both tiers kept document 1006 at the old value. It did
not come back in 16 more rounds on either build, so it stayed a lead.

The router's log placed it. The update copied one document up, and the sweep
that ended two seconds before had evicted one. Before a multi-document write
the router asks the fast tier which matches it already holds, skips those, and
then writes a hold so the evictor leaves them alone. The evictor's side is to
announce, read the holds, and delete. A sweep that read the holds before the
router wrote its own deleted a document the router had just decided not to
copy, and the statement then ran against a fast tier without it. The insert
path had the same handshake in the right order all along: write the hold, then
read the announcement, so whichever side runs second sees the other.

The window is about a millisecond, which is why rounds did not find it again.
An integration test holds it still, with the evictor's delete placed right
after the fast tier answers:

    a document announced for eviction, deleted in the window
                                          before              after
    reported held, so the copy skips it   yes                 no, copied up
    an eviction withdrawn after 100ms     held                held, 0.2s
    an announcement 5 minutes old         held                held, no wait

Both copy-up paths for a write now hold first, then read the announcements.
A document with an eviction in flight is waited for, up to two seconds, and
copied up from a fresh read of the archive once it has gone. Only an
announcement under 30 seconds old counts as in flight, judged by the archive's
own clock, because one left standing by a delete that matched nothing lasts an
hour. The warmer still asks first; there it costs a document the warm meant to
bring up and no statement depends on it.

**The same window, on three paths with no hold at all.** An upsert, an
unacknowledged write and an unsorted findAndModify each ask the fast tier
whether it matches anything, and on a yes let the statement through. Nothing
held the match. A sweep taking it before mongod ran the statement made the
upsert match nothing and create a document; with the `_id` in its filter, the
feeder then mirrors that new document over the archived record. A sorted
findAndModify skipped the hold the same way, and would take the next document
in the sort. Each now holds what it found, but only when an eviction could
reach it: the evictor raises the claim to its cutoff before removing anything
older, so a match aged at or after the claim cannot go first, and the hot path
of an upsert on a recent document costs nothing more. Measured with the same
hook: a match an eviction took was reported present before and is now reported
missing, so the write goes to the archive; a recent match writes no hold, an old
one and one with no date do.

**The warmer's claim undid the evictor's.** The warmer lowers the completeness
claim after every batch, with a plain write. The evictor raises it to its
cutoff on every sweep and then removes what fell below. A batch landing after a
raise put the claim back over documents already removed, and reads trusted it
until the next sweep. A probe with a warm reaching past the eviction cutoff,
sampling the claim against both tiers about every second:

    build      runs   samples   claim wrong   worst
    before        3        41             6   fast tier 385 of 459 claimed
    after         3        26             0

The warm's final claim already checked the fast tier first, and still had the
gap between that check and its write. Every claim the warmer writes is now a
compare and set against its own last write, so a raise makes it fail, and the
warm stops for that run with a line saying why.

**Two operator commands acted inside the evictor's wait.** The evictor raises
the claim, waits about six seconds for routers to read it, and then deletes
what fell below, without reading the claim again. Asking who else writes or
reads the claim during that wait found two more:

    command                            what landed in the wait        before         after
    watermark --complete-all           counted, then wrote the claim  3 of 3 wrong:  0 of 3, refused
                                                                      150 of 300
    backfill --apply --force           read the claim, then marked    959 and 682    0 and 0
                                       absent documents deleted       tombstoned

The declaration counted documents the sweep had not removed yet and wrote the
claim back over them. Backfill's marking pass took an evicted document for one
the application deleted, and the documents it marked were gone from both tiers.
The restore procedure in operations.md pauses eviction first and was safe; the
way round it was `--force`, or the half minute after a feeder stops before the
evictor notices. Both commands now hold the evictor's lease while they count,
write or mark. The backfill probe needed 30,000 documents to show anything: with
300 or 6,000 the marking pass finished inside the wait.

**`cline pause` returned while a sweep went on.** The restore procedure is pause
and then restore, and the evictor read the hold only as a sweep began. A sweep
already running removed 1,000 to 1,050 more documents in the ten seconds after
pause returned, three rounds of three, each confirmed against the archive about
to be replaced. The evictor now reads the hold before every batch, and pause
waits for a sweep in progress to reach that check: 0 in three rounds.

**The probe written for item 9 failed on every build.** Run to close item 11,
`test/echorace` sends 32 simultaneous reads of one archived document through two
routers after each eviction and reads the change stream late. A copy-up was
shown as an insert in 2 of 12 rounds on 38545d7, 4 of 18 on b7cc9e3 and 9 of 22
on 810a4c6. Two copy-ups both found the document absent and both opened a
promotion, and the second, landing after the first one's insert, moved the
promotion's instant past it. A promotion opened in the last ten seconds is now
joined rather than reopened: 0 of 18.

## What is accepted, and needs no work

Thirty-nine of the forty-seven entries in `docs/limitations.md` are one of these
two, and neither is a defect:

**Refused, with the reason given.** A sharded fast tier, `mapReduce` into a
tiered collection, `applyOps`, a clustered collection with a TTL, a capped
conversion, a cursor whose claim narrows under it, a pipeline that joins an
untiered collection. The system says no and says why. The boundary is the
product's shape, not an omission.

**Outside the router's path, and reported.** A write made straight at the fast
tier's mongod, a TTL index created there, a collection dropped there. The router
is not in that path by definition. Each is measured, written down, and now
counted or warned: `inserts_over_existing`, the TTL warning, the fast-tier
watch. The protection is operational: only the router holds credentials that
can write to the fast tier's application databases.

Adding to this section requires the same standard as the rest of the file: a
measurement, and what an operator sees.

## Open

This is the whole list. Nothing else blocks. **Two of eleven remain: items 4 and 5, which are not code.**

- [x] **1. Decide `terminate_auth`.** Decided on 2026-09-16: **it ships off.**
  Authentication is proxied, mongod judges every command against the account the
  client authenticated as, and this router cannot grant what mongod would
  refuse. Two of the three steady-state defects found on 2026-09-15 live only in
  the other mode, and they leave the product with it.

  What the decision costs, measured rather than asserted, and now written where
  an operator reads it in `docs/operations.md`: one upstream connection per
  client, with a line at a thousand; read preference not honoured, so
  `secondary`, `secondaryPreferred` and `nearest` all land on the primary, which
  means sizing the primary for the whole read load; and a command in flight
  during a step-down failing rather than blocking, with nothing applied twice.
  Until 2026-10-05 "failing" included a write that had been applied: see that
  day's section.

  `terminate_auth: true` stays in the product, tested and unsupported. It has 56
  checks and a clean pilot behind it, and that is not the claim that matters:
  with it on this router's own gate is the only thing between a client and
  somebody else's data, and nobody who did not write that gate has reviewed it.
  What would reopen this item is that review, not more traffic through it. The
  startup line says which mode is running, and turning it on warns every start.

- [x] **2. Refuse, or accept in writing, an archive that is not a replica set.**
  Closed by decision on 2026-09-16: accepted in writing rather than refused,
  because both shapes work and refusing would break a deployment that is running
  on one. `docs/operations.md` now opens with the supported shape and says
  plainly that "not refused" is not "supported": there is no suite for either,
  so a defect there is found by whoever runs it. The startup warning every
  daemon already prints is the notice.

- [x] **3. The one open lead, accepted as unreproduced.** Closed on 2026-09-16
  by the bar this file set before the answer was wanted: 20 contended rounds,
  0 disagreements.

  A change stream delivered 23 of 24 events in one sweep on 2026-09-16. The
  mechanism the code suggested was measured and refuted, and the same seed then
  passed three times, so the suite was given the evidence it would need instead
  of a guess: the router runs with `-v` there, for the one line that names the
  document it hid and why.

  `test/streamsoak.sh` then ran `testfuzz` twenty times with three other suites
  driving the same six mongods beside it, because the lead appeared during a
  parallel sweep and twenty quiet runs would have proved much less. **365
  minutes of contended running, every round's streams section agreeing, every
  `testfuzz` exiting zero.** N was written into this file before the runs
  started, which is what stops the bar moving to fit the result.

  It is accepted rather than explained, and that is the honest word for it. If
  it happens again the evidence is captured: read the lines the suite prints
  first, because a count cannot be worked back to a document, and the guess
  from the code has already been refuted once.

- [ ] **4. Measure off this box.** Every number above is six mongods on one
  WSL2 host. Throughput, mirroring rate and failover timing are the ones that
  will move.

  **The four-hour run of 2026-09-18 is the sharpest illustration, because it is
  the one where the interference broke an invariant and then stopped.**

      2,649,084 operations over 4h0m, 92/s, 0 errors
      2,651,911 router commands, 0 refused, 0 duplicate keys, 0 panics
      86 stale reads, 9 violations

  Every one of the nine violations is at **one instant, 15:57:24**, and all nine
  are the same shape: "placing one order moved c000784 from 29 to 29", a count
  read back after a write that did not include it. In the **3 hours 50 minutes
  after it, across 1.1 million further operations, there were none**, and the
  stale read counter moved once more and then stood still.

  What was happening at 15:57 was mutation testing and benchmarks, on this host,
  started by me. The run of 2026-09-17 recorded 31 stale reads and its commit
  said in as many words that interference from the system under test cannot be
  ruled out here. This run reproduced that and then demonstrated the converse by
  going quiet: 1,130 consecutive samples of `mirroring_lag_seconds` at five
  second intervals with the host idle, maximum **0.116s**, and the only two
  samples above 0.3s in the whole run fall in the minute after a `go test`.

  So the item is not "is there a defect here". It is that **this box cannot tell
  one apart from its own load**, and both runs now say so from opposite
  directions.

  The other number this run produced, which will move off this box and is worth
  recording before it does: 42.5% of answers came from the archive, and a read
  that falls through costs **p50 127-135ms** against **1.5ms** for one the fast
  tier answers alone. That is the shape of the tiering cost on six mongods
  sharing one disk.

- [ ] **5. Rehearse the four procedures, by a person, against a staging pair.**
  Adoption, rollback, recovery after a feeder stops with a rolled oplog, and a
  rolling upgrade. `rehearse-the-procedure` is the lesson here: testing a
  command is not testing the procedure it belongs to.

  Each is walked by a suite now, which is the half that can be automated:
  adoption by `test/pilot/setup.sh` on 425,367 real documents, the rolling
  upgrade by `verskew.sh` in both orders, the feeder gap and the **archive
  restore** by `testbackfill.sh`, rollback by `testrollback.sh`.

  **Two more were added on 2026-09-21, and the list of four was short.**
  `testservice.sh` walks installing the daemons as systemd units, which is the
  first procedure anyone performs and had no units to perform it with.
  `testrestore.sh` walks the other restore, the one where the *fast* tier is
  lost and rebuilt from the archive: it took exactly the backup list
  `docs/operations.md` gave, and the application came back without its catalogue
  and without the unique constraint it had declared. Both are fixed and both are
  now checked, and the reason they matter here is that neither was on the list
  of four. A procedure nobody has walked is not known to be short. The restore was
  the gap worth closing on 2026-09-16: the procedure said mirroring could not
  fetch the writes made after the backup, which was true when it was written and
  is not now. Measured, a client read ten of the fifteen the application had
  written, and fifteen after `cline backfill`. What remains is a person walking
  the prose, which nothing here can do for them.

- [x] **6. A second application writing continuously for hours.** Closed on
  2026-09-16 by `FOR=4h bash test/store/two.sh`: two shops, four hours, through
  one router, one feeder, one evictor and one claim cache. Each shop ran
  **1,415,475 operations at 98 to 104/s with 0 errors and 0 violations**, against
  2,834,356 router commands with **0 stale reads, 0 refusals, 0 duplicate keys
  and 0 panics**. 42.5% of reads were answered by the archive throughout, so the
  tiering was real rather than a fast tier holding everything, and p50 on those
  reads moved from 122ms to 129ms across the four hours.

  The run needed two changes to be possible at all, and one of them was a defect:
  the length is a duration now rather than a round count, and the churn loop
  underneath the shops used to kill the feeder and the router when it ended,
  which was invisible while forty churn iterations always outlasted three hundred
  rounds.

- [x] **7. The archive is not always a superset of the fast tier, and reads
  were answered from it anyway.** Found and closed on 2026-09-16, and it is on
  this list rather than in `docs/limitations.md` because it was a wrong answer in
  steady state rather than a boundary.

  The feeder gives up on a document the archive's unique index refuses, which is
  right: no retry can succeed, and retrying for ever stops mirroring for every
  namespace behind it. It said so at ERROR and counted it, and nothing else.
  A router cannot read a log line, so every read that fell through to the archive
  was short by that document and reported success. Measured on three documents
  written at the fast tier with the archive carrying a unique index the third
  breaks: `countDocuments` answered 2, `find` returned two of three, and
  `findOne({_id: 3})` returned the third, because a point read asks both tiers.
  One collection giving two different answers about the same document depending
  on how it was asked.

  The feeder records the document now, in the archive beside every other piece of
  bookkeeping, and the router refuses the reads that would be short by it. Writes
  are not refused, measured rather than reasoned: a multi-document write is
  applied at the fast tier after the archive's matches are copied up, so it
  reaches every document including this one. The refusal lifts by itself when the
  duplicate is resolved, because the write that finally reaches the archive is
  what clears the row. `testkeys.sh` pins all of it.

- [x] **8. An application writing this system's own mark made its own document
  invisible.** Found and closed on 2026-09-16, and here rather than in
  `docs/limitations.md` for the same reason item 7 is: a wrong answer in steady
  state rather than a boundary.

  The archive marks a deleted document with `_tcDeletedAt` and every read of the
  archive excludes the documents carrying it. Nothing stopped an application
  writing a field of that name, and mongod takes it like any other. Measured:
  both tiers accepted the document, `countDocuments` answered **0** where the
  same document in an untiered collection answered **1**, and `findOne({_id})`
  returned it, so one collection answered two ways about one document. A fast
  tier rebuilt from that archive, which is the documented recovery, would not
  have the document at all.

  It was not unknown: the feeder has warned once per collection when it mirrors
  such a document since an earlier commit, in the same words. What was missing
  was the refusal, and the way to find the documents already there.

  Every write path refuses it now: insert, update by any operator that names the
  field, a replacement document, `$rename`, a pipeline update, `findAndModify`,
  `bulkWrite`, and both paths a pipeline writes by. `$unset` stays allowed,
  because it is how the field is taken off a document that has one. One path is
  written down as uncovered in `docs/limitations.md`: a `$merge` of documents
  that already carry the field, from a collection where nothing tiers it.

  Three attempts were needed and each one was found by attacking the last: the
  first covered the write commands and missed the aggregation ones, the second
  read an allow-list of three operators where the rule is about a name, and the
  third returned its refusal wearing a read failure's words.

- [x] **9. A copy-up shown to a late consumer as an insert nobody made.**
  Opened on 2026-09-25 from reading the code and closed the same day by
  measuring it, with `test/echorace`: one archived document, evicted and read
  back each round through two routers, and a change stream opened seventy
  seconds late from a position before the read. It was two defects, and the
  second was the one written here first.

  **No race was needed.** The feeder looked for a copy-up's echo only while it
  believed a router was promoting, and cached a "no" for five seconds. An
  occasional copy-up lands inside a stale "no" almost every time, so its echo
  was mirrored rather than spent and its position was never written down. The
  router answers a late consumer from that list of positions, and a list holding
  only an earlier promotion's position answered "not ours", exactly and wrongly.
  One reader, one copy-up a round: shown **2 rounds of 2** after the first. The
  feeder now asks on every flush that holds an insert, and the heartbeats and
  the warmer's five-second wait that fed the old gate are gone. After: 4 of 4
  positions written down, 0 shown.

  **The race.** When two copy-ups of one `_id` run at once, the one that meets
  the duplicate key withdraws the echo, and the insert it lost to is that echo.
  32 simultaneous reads across two routers withdrew it in every round, 8 of 8
  and 5 of 5. After one uncontested promotion had put a position in the list, a
  late consumer was shown the copy-up in **5 rounds of 5**. The withdrawal
  stays, because a flag left standing would swallow an application's own
  delete-and-reinsert. It now also marks the record `echoUncertain`, and the
  router does not answer exactly from a list it knows may be short, or from one
  whose echo is still pending: the thirty-second window decides, as it did
  before the list existed. After: withdrawn 5 of 5, shown **0**.

  What is given up is exactness for that one promotion, never correctness for
  a consumer reading live: the window is what a live consumer is judged by
  anyway. `TestAPendingEchoMakesThePositionListIncomplete` and
  `TestAWithdrawnEchoMakesThePositionListIncomplete` each failed before their
  change.

- [x] **10. Decide how the `_id` guard trades a retry against an eviction.**
  Closed on 2026-09-28 without choosing either cost: see the end of this item.
  When both tiers hold an `_id`, the insert guard hands the insert to mongod,
  so a driver's retry of an insert that already succeeded is deduplicated by
  mongod rather than refused by this router. What that costs: an eviction of
  that exact document between the guard's read and mongod applying the insert
  lets it land, and the feeder mirrors it over the archived copy. The collated
  form of that window was met in a thirty-minute shop run on 2026-09-28. The
  other choice is the router refusing such inserts itself, which is mongod's
  own answer to a second insert of an `_id` the fast tier holds, and turns a
  retry that reaches a different router than the first attempt into an error
  where plain MongoDB returns success. Which of the two is rarer and which is
  worse for the application is Zelmar's call.

  Zelmar said fix it, and it is fixed without giving up either side. Before
  handing an insert back to mongod the router now places a residency hold on
  the fast tier's copy and then looks for an eviction announcement; the evictor
  announces and then looks for holds, dropping any document held since its
  batch was chosen and taking that announcement back. Both are writes and reads
  on the archive's primary, so whichever runs second sees the first, and an
  eviction and that insert cannot both go ahead. Retries still reach mongod.
  Measured: with an eviction announced, the insert is refused by the router
  rather than handed to mongod; with none, it is handed back with a hold placed.
  What it costs, on the one insert that pays it, an _id both tiers hold, which
  mongod refuses as a duplicate: 200 of them through the router, median 8ms
  before and 11ms after, p90 17ms and 16ms. An insert of a new _id does not
  reach it, 11ms and 9ms, within noise. An application that inserts and reads
  E11000 as "already there" pays about 3ms more on each such insert.

  And its first version lost data, found the same night by asking what the hold
  meant to the change stream sieve. It was written as a copy-up's residency,
  which announces an echo for the insert a copy-up makes; the hold's document
  makes none, and the next insert of that _id was taken for it and not
  mirrored. A refused duplicate, then a delete and a re-insert, left the archive
  holding the old document marked deleted (d73ddf8). The hold is now a hold
  and nothing else, and testlife checks that sequence. Pushed after the eight
  suites that reach it, ahead of a full sweep, because the defect was already
  on master: run both sweeps before relying on it.

- [x] **11. A copy-up concurrent with a delete puts the deleted document back.**
  Found on 2026-09-29 by `FOR=2h test/store/shared.sh` on 71b55d3, 1h52m in.
  Both consumers reported order 6abc4d97000c6ef93715db78 missing, and they were
  right: the application deleted it at 23:45:29.392, and at 23:45:29.621 a
  retryable batch insert from a fresh session (txnNumber 1, stmtId 4, the other
  four statements already present) put the archive's copy back on the fast
  tier. The feeder had tombstoned the archive at 23:45:29.610. The fast tier
  holds the cancelled order live, so the router answers it. The batch is the
  shape of a copy-up, most likely a transaction's warm (txnwarm.go), which
  inserts a customer's archived orders and skips the ones already there.

  60d19ae made every copy-up wait for mirroring to the router's cluster time as
  it begins, which covers a delete acknowledged before it. This delete was
  concurrent: after the copy-up read the archive and before it inserted, from
  the other router. Nothing covers that window, and re-checking before the
  insert only narrows it. The direction: after inserting, wait for mirroring to
  reach the insert's time, re-read the archive for the ids copied, and remove
  from the fast tier any that are now tombstoned, hidden from change streams as
  tiering work. Logs kept in ~/thermocline-keep/2026-09-29-shared2h on this
  box.

  **Reproduced and fixed on 2026-09-30.** A customer with 3,000 orders held
  only by the archive, a transaction through one router counting them, and a
  recent order deleted through the other router while it ran: 12 of 12 rounds
  put the order back, stale_reads 0 each time, and the window was about 1.6s
  wide. The fix is in the feeder rather than the router, because the feeder is
  the one process that sees the delete and the copy-up's insert in the fast
  tier's own order. A copy-up only reads documents the archive holds live, so
  an echo whose archived copy is tombstoned once the flush has landed is a
  resurrection by construction. The feeder removes it from the fast tier,
  announced as an eviction so change streams hide it, and counts it as
  `resurrections_undone`. It covers every copy-up, the warmer's included.
  After: 0 of 12, then 0 of 6 with the recent order and 0 of 6 with an
  archive-only one.

  Attacking that fix found a second defect beside it, older and broader. A
  copy-up recorded a promotion for every document it matched, including those
  the fast tier already held and the insert then skipped, and the change
  stream sieve hides any insert of a document promoted in the last 30s. An
  order placed through the router and passed over by its customer's warm a
  moment later was missing from a stream read from before it, 4 of 4, with no
  delete anywhere. Copy-ups now ask the fast tier first and only hold what it
  already has: 0 of 4.

  With both, the deleted order's own stream still showed only the delete, and
  once a phantom insert after it. The sieve now asks the feeder's exact list of
  echo positions before any clock, and defers to it inside its thirty second
  window where the list is complete: 12 of 12 rounds showed exactly insert,
  delete. Attacking the repair also found the evictor's own withdrawal was a
  no-op, a function named for consuming notices that only read them, which left
  an announcement standing for an hour for a document it never evicted.

  The first 2h `shared.sh` on those fixes, 801084c, found a regression in
  them: seven orders reported placed that nobody placed, both applications,
  by 1h15m. The hold a copy-up now gives a document the fast tier already has
  kept a record's hold running with no promotion behind it, and the next
  copy-up after an eviction read the running hold as a promotion in progress
  and announced nothing. A copy-up that asked the fast tier now always opens a
  promotion: 5 of 5 phantoms on 801084c, 0 of 5 on e4c78e7 and after.

  Open until both sweeps and a 2h `shared.sh` run are clean. What is left
  inside the race itself is written in `docs/limitations.md`.

  **Closed on 2026-10-06.** The probe for the two-router promotion race,
  `test/echorace`, failed on every build when run to close this: a copy-up
  shown to a late consumer as an insert in 2 of 12 rounds on 38545d7 and 9 of
  22 on 810a4c6. Fixed in 1fcb003, 0 of 18. Then, on that code, the reached
  suites 913 passed and 0 failed, and `FOR=2h shared.sh` was clean: both
  applications 0 errors and nothing wrong, each consumer saw every one of the
  other application's orders (30,206 and 30,225), both routers 0 stale reads,
  0 refused and 0 panics across about 658,000 commands and 211,000 copy-ups
  each. The full in-node sweep was 2032 and 0 on 2ffb17e the same day.

## What 2026-09-21 added that an operator has to do

None of these joins **Open**, by the rule below: none makes an application read
or write wrongly in steady state. Every one of them is a cost or a lifecycle
fault, and each is now named by the product at the moment it can be acted on.
They are collected here because they are the shortest list of things a first
production deployment gets wrong.

1. **Build `{date_field: -1, _id: -1}` on the archive** for every warm rule that
   ages by a date field. Without it a warm pass costs the square of the
   collection: 200,000 documents examined per batch at 200,003 archived, against
   100. An index on the field alone still sorts, at 19,039, so the obvious fix
   is not the fix.
2. **Build the unique index on the archive** for any tiered collection written
   by upserts or by inserts that carry no `_id`. Without it every such write
   asks the archive a question it has no index for: 38-40ms at 200,003 archived
   documents against 1-10ms.
3. **Use the units in `packaging/systemd`.** They carry the descriptor limit the
   router needs, the warmer as a timer rather than a daemon, and the drain that
   makes a stop end. Before them, a router with a client connected never
   stopped: still running 60s after SIGTERM against 1s with none, so every
   restart waited out `TimeoutStopSec` and ended in SIGKILL.
4. **Run `cline keys --rebuild` before the evictor**, which is step 7 of
   `docs/adoption.md` and is what the installer now prints.
5. **`warmer.min_free_disk` is a fraction.** Written as a percentage it is
   refused at startup now; before, the warmer backed off for ever.

- [x] **9. An eviction counter that went negative destroyed documents days
  later.** Found and closed on 2026-09-21, and it is on this list rather than in
  `docs/limitations.md` because it lost data in steady-state operation.

  The evictor announces every document it is about to remove so the feeder can
  tell that delete from an application's. The announcement is a count, and the
  feeder takes one down per delete it applies. The lease stops two feeders with
  the same id and nothing stops two with different ids, which is one edited
  config away: each spends the same notice, and `$inc` took the count to -1.

  **Nothing is lost at that moment**, because the repeat carries the same oplog
  timestamp and is recognised as the same delete. The damage is what it leaves
  behind. The next eviction of those documents announces them from -1 to 0, a
  notice counts only above zero, and the feeder reads a real eviction as the
  application deleting the document: the archive tombstones its copy, the fast
  tier has already dropped it, and the document is gone from both tiers with
  nothing said. The misconfiguration is long since corrected by then.

  Measured on 40 documents: two feeders, one eviction, no loss and the counters
  at -1; then one feeder, restore, evict again, and **40 of 40 tombstoned**. The
  same sequence with one feeder throughout left the counters at 0 and lost none,
  which is the control that says the second feeder is what did it.

  The spend floors at zero now and an announcement lifts the count to at least
  one, which is what heals a deployment whose counters are already negative;
  flooring the spend alone cannot. Re-measured on the same sequence: 40 live, 0
  tombstoned, and the same again with the rows forced to -1 by hand.
  `testevict.sh` pins it, and fails with 10 of 10 tombstoned against the old
  arithmetic.

## The rule that keeps this list finite

A new finding goes into **Open** only if it can make an application read or
write wrongly in steady-state operation, or lose data. Anything else is a
warning, a tool, or a line in `docs/limitations.md`, and is done when it is
written down. Work found this way is closed in the commit that finds it, and
does not extend this list.
