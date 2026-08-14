# Live test suites

These run against a real pair of replica sets. Three run inside the node that
has `cline` installed; the chaos suite runs on the host because it has to stop
and start mongod on other containers.

    docker cp test/*.sh <node>:/root/
    docker exec <node> bash /root/testall.sh      # every subcommand, flag and error path
    docker exec <node> bash /root/testevict.sh    # the evictor, including its fail-closed guard
    docker exec <node> bash /root/testrouter.sh   # router behaviour through the wire protocol
    docker exec <node> bash /root/testops.sh      # breadth of MongoDB operations, both tiers
    bash test/testchaos.sh                        # kill things under load (host side, slow)

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
