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
