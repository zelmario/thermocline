# Live test suites

These run against a real pair of replica sets, inside the node that has `cline`
installed. See the anydbver setup in the project notes.

    docker cp test/*.sh <node>:/root/
    docker exec <node> bash /root/testall.sh      # every subcommand, flag and error path
    docker exec <node> bash /root/testevict.sh    # the evictor, including its fail-closed guard
    docker exec <node> bash /root/testrouter.sh   # router behaviour through the wire protocol

Two things they encode that are easy to get wrong when testing by hand:

- **The evictor's precondition is a feeder that has caught up.** Killing the
  feeder does not make mirroring stale immediately, and restarting it does not
  make it fresh immediately. `testevict.sh` waits for each.
- **Collections must be listed in `feeder.namespaces`.** A collection Thermocline
  does not tier is whole on the fast tier by definition, and the router leaves it
  alone, so testing tiering behaviour against one proves nothing.
