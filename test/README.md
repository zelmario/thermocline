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
    docker exec -e WORKERS=4 <node> bash /root/psweep.sh   # the same list, four at a time

`psweep.sh` is the one to use. `sweep.sh` takes over forty minutes, which is why
measurements were getting skipped, and a forty minute feedback loop costs more
than it saves. See "Running them in parallel" below for what a worker is.

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
    docker exec <node> bash /root/testunack.sh     # writes that ask for no acknowledgement
    docker exec <node> bash /root/testcache.sh    # the learned cache, and the advice it gives
    docker exec <node> bash /root/testconform.sh   # differential: the same answer MongoDB would give
    docker exec <node> bash /root/testauthterm.sh  # the router authenticating clients itself
    docker exec <node> bash /root/testreadprefterm.sh # reads landing on the node that was asked for
    docker exec <node> bash /root/testecho.sh      # the router's promotions must not be mirrored back
    docker exec <node> bash /root/testmerge.sh     # $merge into a target the fast tier cannot fully see
    docker exec <node> bash /root/testdaemonmetrics.sh # what the evictor and warmer report about themselves
    docker exec <node> bash /root/testretry.sh     # $sample on a partial tier, and retryable writes
    docker exec <node> bash /root/testpool.sh      # whether idle clients still cost an upstream connection
    go run ./test/exhaust 127.0.0.1:27600 router    # the OP_MSG exhaust bit (host side, see exhaust/README.md)
    go run ./test/cursorowner 127.0.0.1:27600      # whether one client can read another's cursor
    go run ./test/compressed 127.0.0.1:27600       # a client that compresses without being invited to
    bash test/testtls.sh                           # TLS to the client and to the fast tier (host side)
    bash test/testsharded.sh                       # a mongos fast tier, which is refused (host side)
    bash test/testchaos.sh                         # kill things under load (host side, slow)
    bash test/testfailover.sh                      # a write across a change of primary (host side)
    go run ./test/store -uri ... -skip-seed        # a shop, checking what a shop would notice (see store/README.md)

`test/store` answers a different question from all of the above. Every suite here
asks whether an operation returns what it should; the store asks whether an
application keeps working while the tiering moves data underneath it, by running
a shop and checking what a shop would notice: an order it just placed is
readable, placing one order moves that customer's count by exactly one, and a
report over a month that has already ended does not change its answer. Run it
straight at MongoDB first. That control is the point: a check that fails without
the router in the way is a broken check, and nothing it says afterwards counts.

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

## Running them in parallel

The obstacle was never scheduling. Every suite shared one pair of databases, one
bookkeeping database, one config and one set of ports, and the suites stop and
start the daemons out from under each other.

A worker is a slot with its own of each. `psweep.sh` gives worker N:

    shop_wN, wh_wN      its databases
    _thermocline_wN     its bookkeeping, set by the config's new meta.db
    27200 + 20N         a port block, offsets 0 to 17, clear of the 27100 a
                        suite run by hand still uses
    /root/wN            its configs and logs

Suites are claimed from a shared queue rather than dealt out in advance, because
they range from under a minute to several and a worker that finishes early
should take the next one.

**Every knob defaults to the single-worker layout**, so `sweep.sh` still works
and a suite run by hand behaves exactly as it always has. That is deliberate:
it is what makes a serial control run comparable to a parallel one, and the only
way to tell a suite this change broke from a suite parallelism broke.

Things this encodes, each of which cost a wrong result first:

- **Separate databases, not prefixed collections.** Two feeders watching one
  namespace is not merely duplicated work. The second feeder sees the first
  worker's eviction as an *unannounced* delete, because the announcement is in
  the other worker's bookkeeping database, and tombstones a document that was
  only being tiered. Renaming the database is what stops two feeders ever
  sharing a namespace.
- **The bookkeeping database has to be per worker, and that needed a product
  change.** Nearly every suite wipes `watermarks`, `residency`, `evicting` and
  `markers` wholesale in its setup. Scoping those wipes by namespace does not
  work, because `warmerActive`, `promotingActive` and `evictionPaused` are
  singletons with no namespace to scope by. `meta.db` in the config is the
  answer, and it is a real deployment option too: two Thermocline deployments
  sharing one pair of tiers would each have read the other's state as its own.
- **mongosh is node, so the suites read their database names from
  `process.env`, not from the shell.** Half the eval strings are single-quoted
  at the shell, where `$DB` never expands and the suite would silently query the
  wrong database. `process.env.TC_DB` works in both quoting styles.
- **`$WDIR` inside a single-quoted `pkill -f` pattern never expands**, so the
  pattern matches nothing, the kill is a no-op and the next suite on that worker
  inherits a running router. Every scoped pattern is double-quoted.
- **A quoted heredoc expands nothing at all.** `testedge.sh` edits its config in
  `python3 - <<'PY'`, which is right, and it therefore has to read the database
  and directory from `os.environ` rather than from the shell.
- **Do not export `CFG` from the driver.** Several suites name their own config:
  `testauthterm.sh` writes a `terminate_auth` one, `testreadprefterm.sh` its
  own, `testcache.sh` a pair. Exporting `CFG` overrode all of them, and
  testauthterm ran against the plain config and failed its first check and every
  one that depended on it. Every suite already defaults `CFG` to
  `$WDIR/cline.yaml`, so the driver only has to set `WDIR`.
- **The profiler needs nothing extra.** MongoDB's profiler level and its
  `system.profile` are per database, so the three suites that use it are
  isolated by the database rename with no further work.
- **A rewrite of the suites can rewrite its own preamble.** The port
  substitution ran after the preamble was inserted, so `PORT=${PORT:-27100}`
  became `PORT=${PORT:-$PORT}` in all thirty three suites. With `PORT` unset it
  expanded to nothing, `mongosh --port` took the next argument, and every check
  went straight to mongod instead of through the router. That reads as the
  product having no guards at all: `testtxn.sh` reported twelve failures about
  key claims not being enforced and archived documents not being found, and the
  binary was fine. **The control that caught it was running the suite from
  `git show HEAD:` against the same binary**, which passed 23/0. Do that before
  believing a suite that starts failing after the suite itself was edited.
- **A session names its own database, and `getSiblingDB` is not the only way
  to reach one.** Renaming the database caught 904 `getSiblingDB('shop')` and
  missed 16 `s.getDatabase('shop')`, which is how every transaction test
  reaches its collection. The result was that exactly the transaction checks
  failed, in four separate suites, and they failed by *succeeding* differently:
  a read answered straight from the archive rather than through a replay looks
  like the router being cleverer, not like a test pointed at the wrong
  database. Also missed: an eval string nested inside another one, where the
  quotes are escaped (`getSiblingDB(\"shop\")`), `stats().db == 'shop'`,
  `$merge`'s `into: {db: ...}`, and a `listDatabases` check naming the database
  it expected to see. **The bisect that found it** was two configs differing in
  one thing each: rename only the bookkeeping database (passed 23/0), rename
  only the user database (failed 10). Do that before suspecting the product.
- **A namespace built from a shell variable is missed by a rename that matches
  literal names.** `keys.deleteMany({ns:'shop.$1'})` survived a substitution
  that caught every `shop.tkupd`, so `fresh()` cleared key claims under a
  database no worker used and left the real ones standing. Grep for the
  variable forms separately.
- **`mkworker.sh` asserts what a worker's config should say, not what it should
  not.** The first version looked for the shared database and port instead, and
  fired on the one worker whose port legitimately equalled the default. A guard
  that reports a problem the run does not have costs as much as one that misses
  a problem it does.
- **`${VAR:?}` in a suite is a check that cannot run, reported as a check that
  failed.** `testevict.sh` reached the tiers through `${HOT:?}` and `${COLD:?}`
  with no default, and a sweep exports neither, so its two newest checks died at
  the first `mongosh` and printed `got= want=1`. That is the same line a real
  eviction bug prints, and the pair guards the evictor destroying a live record,
  so the one check that would catch the worst defect found here had never run in
  a sweep. Every other suite defaults `HOT` and `COLD` to the six-node URIs;
  this one now does too. **A suite that needs something from the environment
  should default it, not assert it**, because the assertion fires inside a
  check and wears that check's name.

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
- **No driver in this harness sets every wire protocol flag, so some findings
  need a client written for the purpose.** `mongosh` will not negotiate snappy,
  will not send OP_COMPRESSED on demand, and will not ask for exhaust replies.
  Two real defects sat behind exactly those gaps. `test/exhaust` is the client
  that found the second one; write another when a flag matters and nothing here
  can set it.
- **`make build` sets `CGO_ENABLED=0`, so use it.** A plain `go build` links
  against the host glibc, and the container's is older, so the binary dies at
  exec with `GLIBC_2.34 not found`. That surfaces as `FATAL the router never
  came up`, which looks like the router refusing to start rather than a build
  problem. This cost two runs before the Makefile was fixed.
