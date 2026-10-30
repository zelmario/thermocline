# The first production trial

`docs/production-readiness.md` lists eleven things that stood between this
system and production. Nine are closed, and nothing left on it can be closed on
the test box. Item 4 is measuring off six mongods on one laptop. Item 5 is a
person walking the four procedures. Both need a real deployment, so the next
step is a trial, and this file says how to run one so that it answers those two
items instead of only being survived.

A trial is not a launch. It is one application, on hardware you would use, with
a way back you have already walked, and a short list of numbers that end it.

## 1. Choose one application and one collection to tier.

Pick a collection that is large, mostly read by date, and old at the tail:
orders, events, audit records. Leave everything else untiered. A collection
`feeder.namespaces` does not name lives on the fast tier whole (step 4 of
`docs/adoption.md`), which keeps the trial's reach to the one collection
you chose.

The shape this was tested in, and the only one supported:

- Both tiers are replica sets, three members each. A sharded fast tier is
  refused, and a sharded or standalone archive starts with a warning but has no
  suite behind it (`docs/operations.md`, The supported shape).
- Both tiers run the same MongoDB release and the same feature compatibility
  version. Every daemon refuses to start otherwise.
- `router.terminate_auth` is off, which is the default. Authentication is
  proxied, so mongod judges every command against the client's own account.
- The four daemons run from the units in `packaging/systemd`, one of each.
  Name every proxy in the config if you run more than one (step 13).

Know the costs of that shape before you start, because the trial will show
them. They are in `docs/operations.md`: one upstream connection per client,
every read on the primary whatever the read preference, and a command in
flight during a step-down failing instead of waiting.

## 2. Rehearse on a staging pair first.

This is item 5, and it has to come before production because the trial's way
out is one of the procedures. Walk each one by hand, from the prose, against a
staging pair built the way production will be:

| Procedure | Where it is written | The suite that walks it here |
|---|---|---|
| Adoption | `docs/adoption.md`, steps 0 to 16 | `test/pilot/setup.sh` |
| Rollback | `docs/adoption.md`, step 12 | `test/testrollback.sh` |
| A feeder that stopped past the oplog | `docs/proxy.md`, the feeder's exit and `cline backfill --apply --clear-checkpoint` | `test/testbackfill.sh` |
| A rolling upgrade | `README.md`, Both tiers run the same MongoDB release | `test/verskew.sh` |
| Losing the fast tier | `docs/operations.md`, Backups | `test/testrestore.sh` |
| Installing the units | `packaging/systemd` | `test/testservice.sh` |

The suites prove each command does what it says. They do not prove the prose is
complete: on 2026-09-21 the rollback procedure, run as written, gave the
application back its orders without its catalogue, while every command in it
passed its tests. Note every step where you had to guess, and send it back as a
change to the document.

## 3. Bring it up in three stages, and measure each one.

Each stage changes one thing, so a number that moves has one cause.

**Stage A: the proxy in front, nothing evicted.** Run the feeder and the
proxy, point the application at the proxy, and leave the evictor and the
warmer stopped. The fast tier starts empty, so most reads fall through to the
archive at first, and the copy-ups fill it. This stage costs only latency, and
it is the cheapest one to leave: the archive still holds everything, so the
way back is step 12 alone.

Run `cline preflight` before the start and keep its output. Read the proxy's
startup lines; every warning there names its own action.

**Stage B: warm, then claim the keys.** Run the warmer over the window the
application reads, then `cline watermark` to see the claim it left, then
`cline keys --rebuild`. Build `{date_field: -1, _id: -1}` on the archive first,
and the unique indexes the fast tier carries, or every warm pass sorts the
archive (the list under "What 2026-09-21 added" in
`docs/production-readiness.md`).

**Stage C: let the evictor remove.** Start it with `--dry-run` for a day and
compare what it would remove with what you expect. Then run it for real. Expect
it to hold off at first (step 10 of `docs/adoption.md`): it removes nothing the
archive has not been seen to hold.

`cline pause` stops the evictor removing anything, at once and for as long as
you need. Use it the moment anything below fires, before you diagnose.

## 4. Measure what the test box could not.

Item 4 is open because one host cannot tell a defect from its own load. These
are the numbers measured here, so the trial has something to compare against.
Record each one in every stage:

| What | Measured on the test box | Where to read it |
|---|---|---|
| Proxy overhead, read the fast tier answers | 130µs (720µs proxied, 590µs direct) | client timings, proxy on and off |
| A read the archive answers | p50 127 to 135ms, against 1.5ms | client timings, `from_archive` on the proxy line |
| Share of reads answered by the archive | 30% to 42% in the shop runs | the proxy's minute line |
| Mirroring rate | 2,000 to 3,700 documents/s | the feeder's log, during a bulk load |
| Mirroring lag, steady state | 0.116s maximum over 1,130 samples | `thermocline_mirroring_lag_seconds` |
| A write across a failover | about 350ms | the application's own retry timings |
| Proxy start, 160 tiered namespaces | 1.3s | the proxy's ready line |

A number that is worse off this box is expected and is not a defect. A number
that is worse **and** comes with a stale read, a refusal or a violation of the
application's own rules is the thing item 4 exists to find.

## 5. Stop on any of these.

The first five are alerts in `docs/monitoring.md`, under What to alert on,
and the Prometheus rules written there fire them. Stopping means
`cline pause` first, then deciding whether to roll back.

1. `thermocline_stale_reads_total` above zero. A read may have missed a write
   that had already returned.
2. `thermocline_panics_total` above zero.
3. `thermocline_not_superset_namespaces` above zero. The archive is missing
   documents the fast tier holds.
4. `thermocline_mirroring_lag_seconds` past a minute and still rising.
5. `thermocline_daemon_seconds_since_pass` past ten minutes for the feeder.
6. The application reporting a record it cannot find, a count that does not
   add up, or a change stream event nobody caused. These are the checks the
   shop harness makes (`test/store`), and every defect found since 2026-09-24
   reached it first. Ask the application's owners which of their own checks
   would notice a lost or resurrected record, before the trial starts.

`thermocline_refused_total` rising is not on this list. Read the shape and the
log first: several ordinary things refuse on purpose (`docs/monitoring.md`,
rule 4).

## 6. Say in advance what ends the trial well.

Write the bar down before the first stage starts, so the result cannot move it.
A reasonable one: two weeks in stage C at normal traffic, with at least one
failover of each tier and one rolling restart of the daemons, and none of the
six stops above. When it is met, items 4 and 5 close in
`docs/production-readiness.md` with the trial's numbers in place of this box's.

When it is not met, the finding goes to the readiness list under its own rule:
it joins **Open** if it can make an application read or write wrongly, or lose
data, and otherwise it is written down in `docs/limitations.md`.
