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
    docker exec <node> bash /root/sweep.sh         # all of the below, serially, one log each

Or one at a time:

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
    docker exec <node> bash /root/testauth.sh      # whether a client's permissions still apply from the archive
    docker exec <node> bash /root/testbulk.sh      # the bulkWrite command MongoDB 8.0 added
    docker exec <node> bash /root/testchange.sh    # change streams opened by the application, through the router
    docker exec <node> bash /root/testconcerns.sh  # write concern, read concern, and concurrent writers
    docker exec <node> bash /root/testddl.sh       # commands that change a collection rather than its documents
    docker exec <node> bash /root/testexplain.sh   # what explain says about a query the fast tier cannot answer
    docker exec <node> bash /root/testindexes.sh   # what happens when the tiers have different indexes
    docker exec <node> bash /root/testquery.sh     # query features that depend on something other than the documents
    docker exec <node> bash /root/testreadpref.sh  # where a read asking for a secondary actually lands
    docker exec <node> bash /root/testtypes.sh     # collection types that cannot be tiered
    docker exec <node> bash /root/testupsert.sh    # an upsert whose document lives only in the archive
    docker exec <node> bash /root/testsingle.sh    # single-document writes filtered on any other field
    docker exec <node> bash /root/testconform.sh   # differential: the same answer MongoDB would give
    docker exec <node> bash /root/testauthterm.sh  # the router authenticating clients itself
    docker exec <node> bash /root/testreadprefterm.sh # reads landing on the node that was asked for
    docker exec <node> bash /root/testecho.sh      # the router's promotions must not be mirrored back
    docker exec <node> bash /root/testmerge.sh     # $merge into a target the fast tier cannot fully see
    docker exec <node> bash /root/testdaemonmetrics.sh # what the evictor and warmer report about themselves
    docker exec <node> bash /root/testretry.sh     # $sample on a partial tier, and retryable writes
    bash test/testchaos.sh                         # kill things under load (host side, slow)

`testconform.sh` is the one to extend. It asserts almost nothing by hand: two
collections hold identical documents, one untiered and one tiered with half its
documents only in the archive, every operation runs against both, and the
answers must match. A failure is the router disagreeing with MongoDB rather than
with somebody's expectation. It found that every promoted document came back
with its fields reordered, which twenty-three suites of hand-written checks had
agreed with themselves about for months.

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
- **A multi-line command chained after a wait can be flattened into one line**
  by the shell wrapper, turning `done` followed by a newline and `echo` into
  `done echo ...`, which never runs. The suite it was supposed to launch simply
  never started, and an empty output file looks exactly like a slow one. Put
  anything with control flow in a script file and run that.
- **PSMDB 8.0 defaults hide the sort problem.** `allowDiskUseByDefault` is true
  and the blocking sort limit is 100 MB, so an unindexed sort on the archive
  passes for free at test scale and proves nothing. `testindexes.sh` tightens
  the archive to a 1 MB limit and then turns spilling off, which is a real
  configuration operators choose, and restores both afterwards.
- **A probe script that starts daemons must stop them.** Two throwaway scripts
  left routers running with `docker exec -d`, and the next suite reported "the
  router survived the concurrency" got=5, which is a real check failing for a
  reason that had nothing to do with the product. Trap EXIT and kill.
- **`mongosh` needs an optional module for snappy but ships zlib with Node.**
  A `compressors=snappy` URI fails to connect at all, so a compression test
  written with it proves nothing; `compressors=zlib` actually negotiates.
- **The test config is shared by every suite.** Adding an evictor rule for one
  suite broke `testall`'s "evictor with no rules" fixture, whose sed range
  ended at the first `older_than` and left the second rule orphaned. The check
  then tested a parse error while claiming to test an empty rules list.
- **Prove a guard is bypassed before claiming it is.** The compression bug was
  argued from the code first and the initial evidence was an artifact of a
  fixture whose namespace was missing from feeder.namespaces, so nothing
  mirrored and every insert was legitimately accepted. The real proof needed a
  binary that counted OP_COMPRESSED without preventing it: seven such messages,
  and a duplicate accepted through zlib that an uncompressed client got 11000
  for.
- **A test result is only about the binary you installed.** Twice now: once by
  copying a new binary while a sweep was running, and once by editing code and
  never rebuilding, so a sweep reported a fix failing that was never in it.
  Build, install, then run, and never during.
- **`pgrep -c` and `grep -c` print 0 and exit 1 when nothing matches**, so
  `|| echo 0` appends a second zero and the check reads `got=0\n0`. Use
  `|| true`. But not blindly: `... && echo mismatch || echo 0` is a different
  idiom where the zero is the pass value, and replacing it there breaks a
  passing check.
- **A test for "does it refuse" must not assume it refuses.** The collection
  type checks run the feeder in a subshell; without the guard the feeder simply
  starts and runs, so the subshell never returns and the whole suite wedges.
  Wrap it in `timeout`.
- **An uncaught error in `mongosh --eval` ends the script**, so everything after
  it silently never runs. Enabling the profiler with
  `setProfilingLevel(0); system.profile.drop(); setProfilingLevel(2)` throws on
  the `drop()` on a secondary, so `setProfilingLevel(2)` never ran on the two
  nodes the test was about. The suite then measured nothing and reported zero
  everywhere, which reads exactly like a real finding. Guard each step, or check
  afterwards that it took: `getProfilingStatus().was` should be 2.
- **A control that records nothing proves nothing.** Following on from the
  above: the first read-preference run showed the router serving every secondary
  read from the primary, which was true, but its control showed zero on all
  three nodes. That leaves "the profiler does not record on secondaries" as an
  unexcluded explanation for the whole result. Do not report a measurement whose
  control did not come back positive.
- **`system.profile` is capped and cannot be dropped on a secondary**, so
  entries survive between runs. A suite that filters profile output by a fixed
  `comment` finds a stale entry from the previous run, on whichever node ran it
  last, and reports the wrong node. `testreadpref.sh` failed four checks
  including its own control the first time it ran inside a full sweep, for that
  reason and no other. Tag each run's queries uniquely.
- **Seeding the archive while the feeder runs needs upserts, not `insertMany`.**
  The feeder mirrors the fast tier seed into the archive on its own schedule, so
  it can land between a suite's `drop` and its archive seed. The archive then
  already holds `_id` 1, an ordered `insertMany` abandons the whole batch on the
  first duplicate, and the collection is left half seeded. Every read then
  answers from half a collection, which reads as a product bug and is not one.
  Two suites have now been written with this mistake, `testconform.sh` and
  `testretry.sh`; state the archive's contents with unordered upserts instead of
  assuming it is empty.
- **A wait loop on `pgrep -f name` can match itself.** The bracket trick,
  `pgrep -f "[t]estretry"`, stops the pattern matching its own literal text, and
  it does not stop it matching the same name appearing anywhere else in that
  shell's command line, such as the path of the script being edited. An
  `until ! pgrep ...` loop written that way waits for itself and never returns.
  Wait on the thing itself, or match a pattern that cannot appear in the waiting
  command.
- **`make build` sets `CGO_ENABLED=0`, so use it.** A plain `go build` links
  against the host glibc, and the container's is older, so the binary dies at
  exec with `GLIBC_2.34 not found`. That surfaces as `FATAL the router never
  came up`, which looks like the router refusing to start rather than a build
  problem. This cost two runs before the Makefile was fixed.
