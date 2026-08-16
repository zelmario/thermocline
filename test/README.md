# Live test suites

These run against a real pair of replica sets. All but one run inside the node
that has `cline` installed; the chaos suite runs on the host because it has to
stop and start mongod on other containers.

`cline.yaml` in this directory is the config they expect at `/root/cline.yaml`.
Keep the namespace list in it current: a collection missing from
`feeder.namespaces` is not tiered at all, so a test written against one proves
nothing.

    docker cp test/*.sh <node>:/root/
    docker cp test/cline.yaml <node>:/root/cline.yaml
    docker exec <node> bash /root/testall.sh       # every subcommand, flag and error path
    docker exec <node> bash /root/testevict.sh     # the evictor, including its fail-closed guard
    docker exec <node> bash /root/testrouter.sh    # router behaviour through the wire protocol
    docker exec <node> bash /root/testops.sh       # breadth of MongoDB operations, both tiers
    docker exec <node> bash /root/testedge.sh      # edge cases and config variants
    docker exec <node> bash /root/testkeys.sh      # unique keys across tiers, every write path
    docker exec <node> bash /root/testwarm.sh      # completeness claims, and withdrawing one
    docker exec <node> bash /root/testfresh.sh     # archive reads must be as fresh as fast tier ones
    docker exec <node> bash /root/testaggwrite.sh  # $out and $merge over archived input
    docker exec <node> bash /root/testlife.sh      # residency, drop, TTL, big documents, auth, concurrency
    docker exec <node> bash /root/testtxn.sh       # what a transaction is promised across two tiers
    bash test/testchaos.sh                         # kill things under load (host side, slow)

The suites take their endpoints from `HOT`, `COLD`, `HOTU` and `COLDU`, so pass
them with `docker exec -e` when the topology is not the documented one.

`testops.sh` is the one that earns its keep: it runs the same battery of reads
with the fast tier complete and again with everything falling through to the
archive, so the two paths have to agree. A third pass makes the fast tier
genuinely short, which is the only way `count` and `distinct` bugs show up at
all: with a complete fast tier they return the right answer even unguarded.

Things these encode that are easy to get wrong when testing by hand:

- **The evictor's precondition is a feeder that has caught up.** Killing the
  feeder does not make mirroring stale immediately, and restarting it does not
  make it fresh immediately. `testevict.sh` waits for each.
- **Collections must be listed in `feeder.namespaces`.** A collection Thermocline
  does not tier is whole on the fast tier by definition and the router leaves it
  alone, so testing tiering behaviour against one proves nothing.
- **A complete fast tier hides completeness bugs.** Make it short on purpose.
- **The feeder logs its change stream reopen at ERROR during a failover.** That
  is the recovery working, so the chaos suite excludes it by name.
- **Removing documents through the router is a delete, not an eviction.** The
  feeder tombstones them, which frees unique keys and hides them from archive
  reads, so a test that means "evicted" has to remove them with the feeder
  stopped and the checkpoint reset. `testedge.sh` has a helper for it.
- **Wait for things, do not sleep for them.** A fixed sleep after an insert was
  the largest single source of false failures here: when mirroring had not
  landed, an evicted document had no live archive copy, its key claim looked
  stale, and the next write was handed a key it should have been refused. That
  reads exactly like a product bug. `testkeys.sh` waits on the archive count and
  on the feeder's checkpoint instead.
- **Check your own fixture came up.** Running a suite twice in a row left the
  previous router still holding port 27100, the new one failed to bind, and
  every check through the proxy failed until a later section restarted it.
  Twenty-two failures, none of them real. `startRouter` now refuses to continue
  unless the router answers.
- **One collection per section when unique indexes are involved.** The router
  caches a namespace's unique indexes for a minute, so a name reused with a
  different index is still enforced under the old one, and checks then pass and
  fail for reasons unrelated to what they claim to test.
- **Age comes from the ObjectId unless a `date_field` says otherwise.** Seeding
  integer `_id`s makes every window filter match nothing, because integers sort
  before every ObjectId in BSON, and a count of zero looks exactly like a tier
  with nothing missing.
- **A copy-up only happens on a point lookup by `_id`.** Any other filter is
  answered from the archive without promoting anything, so a residency test that
  reads by a field proves nothing about residency.
- **Never use a bare `wait`.** These suites start the feeder and the router as
  background jobs of the same shell, so `wait` blocks on daemons that never
  exit and the suite hangs with every check already passed. Collect the client
  PIDs and wait on those.
- **Clear the bookkeeping a suite's own previous run left behind.** `testedge.sh`
  ends its `date_field` section with a warm, which pins all 30 documents in
  `residency` for five minutes and announces them in `warming`. A rerun inside
  that window then watched the evictor keep 30 documents where the age rule says
  8, because a residency hold outranks age, and could watch the feeder suppress
  the next run's inserts as warm echoes. Both are the product behaving correctly
  and both read as failures. Setup now drops `residency`, `warming` and the
  `warmerActive` marker alongside the checkpoint and the watermarks.
- **A guard that takes a command over has to do everything that command needed.**
  The in-transaction key check first owned the forwarding of the update it was
  checking, which skipped the copy-up an update that matched nothing depends on.
  The write then matched an archived document, modified nothing, and reported
  success: the guard against one silent wrong answer had introduced another.
  `testtxn.sh` covers it now, on a collection with a unique index so the guard
  is actually armed. It runs before the reply, notes what the statement will
  touch, and checks after it, without ever taking the command over.
- **Test the transaction cases with `withTransaction`, not `startTransaction`.**
  The router asks for a replay by returning `TransientTransactionError`, which
  is a contract with the driver's retry helper. A hand-rolled
  `startTransaction`/`commitTransaction` block has nothing to do the retrying,
  so every transparent case reads as a failure.
- **Never install a binary or run a second suite while a regression is running.**
  Everything here shares one cluster and one `/usr/local/bin/cline`, and the
  suites stop and start the daemons out from under each other. A run that had
  the binary swapped mid-flight reported three failures in `testrouter`,
  `testlife` and `testtxn` that no longer existed a minute later, which is worse
  than no result: it costs an investigation into a product that is fine. Let the
  regression finish, or stop it and start again.
- **A dashboard address is a port like any other.** `router.dashboard` was set
  to 27101, which `testrouter.sh` already uses for its strict-mode router; they
  raced for the bind and the strict check failed for reasons entirely unrelated
  to strictness. The config now refuses a dashboard on the same port as the
  router, and the suites' ports are 27100-27103, 27110 and 27177.
- **The first statement of a transaction takes a different code path.** It
  carries `startTransaction`, and the router guards that one *before* the write
  the way it guards ordinary traffic, because a transaction cannot have written
  anything yet. Only a later statement takes the read-back-and-abort path. A
  check whose transaction did one thing therefore tested the pre-flight guard
  while claiming to test the other one, and passed against a build with the bug
  still in it. Put a harmless write ahead of the statement under test.
- **Prove a new check fails against the bug it describes.** The check above was
  written for a real defect, passed immediately, and only turned out to be
  testing the wrong path when it was run against a deliberately broken build.
  Reverting the fix and watching the check go red takes two minutes and is the
  only thing that distinguishes a regression test from a decoration.
- **MongoDB's TTL monitor sweeps every 60 seconds, so anything racing it is
  flaky roughly one run in fifteen.** `testlife.sh` inserted an already-expired
  document into a collection that already carried the TTL index, then asserted
  the archive held it. When a sweep landed in the gap the document was gone
  before the feeder saw it and the archive correctly held nothing. Mirror the
  document first, wait for it, and create the TTL index afterwards.
