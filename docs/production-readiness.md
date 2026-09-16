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
| In-node suites, 36 of them | 1,792 checks, 0 failed |
| Host suites, 10 of them | 195 checks, 0 failed |
| The same suites with the query cache on | 1,768 checks, 0 failed |
| Differential fuzzer, 10 rounds × 200 operations | 3,464 agreed, 0 disagreed |
| Pilot: MongoDB's sample datasets adopted as an archive, 425,367 documents | 199 questions, 0 disagreements |
| Pilot write, change stream and transaction batteries | 22, 5 and 15 checks, 0 failed |
| Pilot soak, three hours | 0 disagreements, 0 stale reads, 0 panics, flat memory |
| Rolling upgrade under traffic, both orders, against an equal-version control | 25 checks, 0 failed |
| Router startup, 160 tiered namespaces with data on both tiers | 1.3 seconds |

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

This is the whole list. Nothing else blocks. **Five of six remain.**

- [ ] **1. Decide `terminate_auth`.** It is off by default and
  `docs/limitations.md` calls it a judgement call rather than a finished
  feature. Two of the three steady-state defects found on 2026-09-15 live only
  in that mode. **Shipping with it off closes this item outright**, at the cost
  of read preference and connection pooling. Shipping with it on needs its
  authorisation gate reviewed by somebody who did not write it.

  The decision is still Zelmar's, but it is no longer a silent one: the startup
  line carries `auth=proxied` or `auth=terminated`, and turning it on prints a
  WARN every start saying that the router's own gate is then the only thing
  between a client and somebody else's data. It said nothing at all before.

- [x] **2. Refuse, or accept in writing, an archive that is not a replica set.**
  Closed by decision on 2026-09-16: accepted in writing rather than refused,
  because both shapes work and refusing would break a deployment that is running
  on one. `docs/operations.md` now opens with the supported shape and says
  plainly that "not refused" is not "supported": there is no suite for either,
  so a defect there is found by whoever runs it. The startup warning every
  daemon already prints is the notice.

- [ ] **3. Close or accept the one open lead.** A change stream delivered 23 of
  24 events in one sweep on 2026-09-16. The mechanism the code suggested was
  measured and refuted; the same seed then passed three times. The suite now
  captures the line that would name the document. Needs either a reproduction or
  a decision to accept it as unreproduced after N clean sweeps.

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

- [ ] **6. One shape never tried: a second application writing continuously**
  for hours, beside the first, through the same router.

## The rule that keeps this list finite

A new finding goes into **Open** only if it can make an application read or
write wrongly in steady-state operation, or lose data. Anything else is a
warning, a tool, or a line in `docs/limitations.md`, and is done when it is
written down. Work found this way is closed in the commit that finds it, and
does not extend this list.
