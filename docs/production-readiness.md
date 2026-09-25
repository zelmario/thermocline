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
deployment does, and a fourth by reading, in item 9 below. The discovery rate
has not fallen, and the shape of the finds has not changed: each needed two
processes that each hold a view of the same state.

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

This is the whole list. Nothing else blocks. **Three of nine remain, and one of them is code.**

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

- [ ] **9. Two copy-ups of one document, and the loser withdraws the winner's
  echo.** Found on 2026-09-25 by reading, while chasing the two-router defects
  above, and **not yet observed end to end**, so what follows is the mechanism
  and not a measurement.

  A copy-up records residency, which announces an echo the first time, then
  inserts. When the insert meets a duplicate key the fast tier already held the
  document, so no echo is coming, and the promoter withdraws the announcement.
  Two promoters racing on one archived document break that: two routers, or two
  reads on one router, because nothing deduplicates copy-ups of the same `_id`.
  The first records and announces, the second records and announces nothing
  because the promotion is already open, the first inserts, and the second meets
  the duplicate and withdraws the first one's echo.

  The feeder then mirrors the copy-up back into the archive, which costs a write
  and nothing else, and it writes down no position for it. A consumer reading
  that insert within thirty seconds is fine: the record's `promotedAt` is inside
  the window and the insert is hidden. One reading it later, which is any
  consumer resuming after a deploy, is answered from the record's `echoed` list,
  and if an earlier promotion of that document left a position there the answer
  is exact and wrong: **the copy-up is delivered as an insert nobody made.**

  A token per announcement does not close it, because the one that announced can
  be the one that loses: the second promoter's insert can land first. What the
  loser needs to know is whether the document was on the fast tier before anyone
  announced, and neither promoter can tell that cheaply. What closes this item is
  a reproduction first, then a fix measured against it.

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
