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
| In-node suites, 36 of them | 1,832 checks, 0 failed |
| Host suites, 10 of them | 195 checks, 0 failed |
| The same suites with the query cache on | 1,768 checks, 0 failed |
| Differential fuzzer, 10 rounds × 200 operations | 3,464 agreed, 0 disagreed |
| Pilot: MongoDB's sample datasets adopted as an archive, 425,367 documents | 199 questions, 0 disagreements |
| Pilot write, change stream and transaction batteries | 22, 5 and 15 checks, 0 failed |
| Pilot soak, 691 rounds | 0 disagreements, 0 stale reads, 0 panics, flat memory |
| Rolling upgrade under traffic, both orders, against an equal-version control | 25 checks, 0 failed |
| Router startup, 160 tiered namespaces with data on both tiers | 1.3 seconds |
| Two applications writing through one router for four hours | 2,830,950 operations, 0 errors, 0 violations |
| The fuzzer's change streams, 20 rounds under three other suites | 365 minutes, 0 disagreements |

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
watch. The protection is operational — only the router holds credentials that
can write to the fast tier's application databases.

Adding to this section requires the same standard as the rest of the file: a
measurement, and what an operator sees.

## Open

This is the whole list. Nothing else blocks. **Two of eight remain, and neither is code.**

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

- [ ] **5. Rehearse the four procedures, by a person, against a staging pair.**
  Adoption, rollback, recovery after a feeder stops with a rolled oplog, and a
  rolling upgrade. `rehearse-the-procedure` is the lesson here: testing a
  command is not testing the procedure it belongs to.

  Each is walked by a suite now, which is the half that can be automated:
  adoption by `test/pilot/setup.sh` on 425,367 real documents, the rolling
  upgrade by `verskew.sh` in both orders, the feeder gap and the **archive
  restore** by `testbackfill.sh`, rollback by `testrollback.sh`. The restore was
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

## The rule that keeps this list finite

A new finding goes into **Open** only if it can make an application read or
write wrongly in steady-state operation, or lose data. Anything else is a
warning, a tool, or a line in `docs/limitations.md`, and is done when it is
written down. Work found this way is closed in the commit that finds it, and
does not extend this list.
