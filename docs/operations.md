# Operating it

Backups, restores and logs.

Part of [Thermocline](../README.md).

## The supported shape

**Replica sets on both tiers, running the same MongoDB release and the same
feature compatibility version.** That is what every suite runs against and the
only shape these numbers describe.

A sharded or standalone archive is *not* refused, because both work: nothing
tails the archive, so it needs no oplog. Every daemon says so at startup and
that warning is the notice, not a formality. "Not refused" is not "supported":
there is no suite for either shape, so a defect there would be found by whoever
is running it rather than here. A sharded fast tier *is* refused, because the
feeder tails it.

If you need one of those shapes, the work is a suite that keeps it honest, not a
change to the code. See `docs/limitations.md`.

**Authentication is proxied, and that is the shipped configuration.** Decided on
2026-09-16: `router.terminate_auth` stays off. Each client gets its own upstream
connection to the fast tier, mongod judges every command against the account the
client authenticated as, and this router cannot grant what mongod would refuse.

Three things come with that, all measured, and an operator plans around them
rather than discovering them:

- **One upstream connection per client.** The router says so once at a thousand
  open clients. The wall is mongod's rather than this proxy's.
- **Read preference is not honoured.** `secondary`, `secondaryPreferred` and
  `nearest` all land on the primary, measured with the profiler recording on all
  three members. Each connection warns the first time it asks, and
  `thermocline_primary_only_reads_total` counts them. Size the primary for the
  whole read load.
- **A command in flight when the primary steps down fails**, and the client
  retries it, where a driver talking to the replica set directly would block in
  server selection. Nothing is applied twice; `testfailover.sh` checks that.

`router.terminate_auth: true` lifts all three and is **not supported**. It works
and has 56 checks behind it, it has carried the whole pilot over data nobody
here wrote, and that is still not the claim that matters: with it on, this
router's own authorisation gate is the only thing between a client and somebody
else's data, and that gate has not been reviewed by anybody who did not write
it. Turning it on is a decision about your own risk, and the router warns at
every start that you have taken it.

### Backups

The archive is the complete copy, so it is the one to back up. That is the whole
point of the shape: the fast tier holds a recent slice, and a `mongodump` of it
captures exactly that slice however long the cluster has been running.

What to take, and where each piece lives:

- **The archive's application databases.** Every document the feeder has
  mirrored, live and tombstoned, lagging by mirroring lag. The router's
  dashboard and `thermocline_mirroring_lag_seconds` say how far behind that is at
  the moment you take it.
- **The archive's bookkeeping database**, `_thermocline` by default. Watermarks,
  mirroring checkpoints and positions, residency holds, the eviction and warming
  notices, the singleton markers. Restoring documents without these leaves a
  system whose claims describe a fast tier that no longer matches.
- **The fast tier's `_thermocline.keys`.** The unique key registry is the one
  piece of bookkeeping that lives on the fast tier, because it constrains the
  data there. It is also the one piece you can rebuild rather than restore, with
  `cline keys --rebuild`.
- **The fast tier's accounts and roles**, `admin.system.users` and
  `admin.system.roles` on the fast tier. This is the piece with no other source,
  and the reason it is easy to miss is that it is not data: the router
  authenticates clients against the *fast* tier, and that replica set was new at
  adoption, so its accounts were created by hand at step 3 of the rollout and
  have never come from anywhere else. The archive's copies are its own and are
  not the ones being used.

  Restore everything else and leave this out and the system is correct and
  unusable: every client gets an ordinary authentication failure naming nothing.
  The daemons say so at startup — "the archive has accounts the fast tier does
  not" — and `cline preflight` reports it without starting anything, which is
  the check to run after a restore rather than the thing to rely on instead of
  taking the backup.

- **The fast tier's documents, optionally.** They are a subset of the archive
  except for anything the feeder never mirrored, which the daemons name at
  startup as documents "the archive has none of". Warming refills the rest.

Everything else on the fast tier is rebuilt rather than restored, and it is
worth knowing which is which before an outage rather than during one. Collection
options and indexes come back from the archive when a router starts, the unique
key registry from `cline keys --rebuild`, views from the archive if a router
mirrored them there, and the documents from warming. Accounts are the one thing
on that tier that nothing rebuilds.

**Restoring the fast tier is the other direction, and it has its own trap.** The
completeness claim says the fast tier holds every live document newer than some
instant, and it lives on the *archive*. Restore the fast tier from a backup and
that claim survives, describing the tier the backup replaced: every read bounded
inside the claimed range is then answered from the fast tier alone and comes
back short, with no error and nothing in any log. The claim is checked when it
is declared and never again.

Measured on a shop holding 6,187 orders, with forty taken off the fast tier the
way a restore takes them: the same bounded read answered 147 through the router
where the archive holds 187.

So `cline preflight` counts both tiers for every claim before you start
anything, and names the ones the numbers contradict:

    shopstore.orders: the fast tier holds 147 of documents whose _id timestamp
    is at or after 2026-09-14T03:32:06Z and the archive holds 187, so it is 40
    short of the claim.

Warm the namespace, or clear the claim with `cline watermark --ns <ns> --clear`
and let the warmer record it again. The counting is bounded by `--count-for`,
and a claim the budget does not reach is reported as unchecked rather than
passed over, because a claim not checked is a claim that stands.

Restoring the archive under a live fast tier needs eviction held first:

    cline pause --config cline.yaml --reason "restoring the archive"
    # restore, then
    cline resume --config cline.yaml

That order is not a formality. The evictor's check is per document and presence
only, so an archive holding an *older* version of a document confirms it as
present, and the fast tier's newer copy is removed: the restore would cost you
the writes made since the backup rather than recovering them.

That is measured now rather than reasoned, in both directions, by
`test/testevict.sh`. Five documents mirrored at one value, updated to a second,
the archive restored to the earlier backup underneath a live fast tier: paused,
the evictor removes nothing and the newer copies survive; unpaused, all five go
from the fast tier and the newer value is then in neither tier. The guard works
and the danger it guards against is real.

**And the limit worth knowing before you need it.** A restored archive is a
restored past. Documents written after the backup are still on the fast tier if
they are recent enough to be there, and mirroring will not fetch them: a change
stream reports changes from now, and it only moves forward.

**`cline backfill` is what fetches them**, and it is the step to run after a
restore, before letting the evictor go again:

```
bin/cline pause    --config cline.yaml --reason "restoring the archive"
# restore the archive
bin/cline backfill --config cline.yaml --apply
bin/cline resume   --config cline.yaml
```

Leave the feeder running while it copies. That is the order with no window in
it: the feeder carries what happens next while the copy brings back what the
restore dropped. Measured on ten documents mirrored, a backup, five more
written, then the archive dropped and restored: a client read ten of the fifteen
the application had written, and fifteen after the copy. `test/testbackfill.sh`
walks exactly those steps.

What it cannot fetch is what the fast tier no longer holds: anything written
after the backup **and already evicted** is in neither tier, and nothing can
recover it. That is why the pause comes first, and why the archive's backup
interval is the real exposure of this system rather than the fast tier's.
`cline rollback`, which counts rather than removes unless you pass `--clear`, is
what tells you what the two tiers disagree about afterwards.

### One of each daemon

The feeder and the evictor refuse to run twice against one deployment, and the
refusal names who holds the lease:

```
$ bin/cline feeder --config cline.yaml
error: another feeder with id "prod" is running: db7 (pid 2214), last seen 3s
ago. Two of them against one deployment is not redundancy: stop that one, or
wait 30s for its lease to expire if the host is gone
```

This is not tidiness. Two feeders on one namespace both see every delete and an
eviction is announced once, so the second reads a real eviction as the
application deleting the document and tombstones the archive's copy: the
document is gone from both tiers with nothing said. The way to get there is
ordinary, which is why the guard exists rather than a line in a runbook.

What that means for a failover, measured on the test cluster rather than
reasoned: a feeder stopped cleanly releases its lease and the replacement is
mirroring **122 ms** later; one killed with `kill -9` releases nothing and the
replacement is mirroring **129 ms** later, because this host can see the pid is
gone; and a replacement started beside a feeder that is still running is refused
in **126 ms** with the message above. Only a holder on another host that stopped
without releasing costs the thirty seconds. There is
nothing to clear by hand, and clearing the row by hand is safe if you are sure
the other process is gone.

**The lease protects you only once both daemons have it.** A version from before
it takes none and respects none, so during an upgrade the old feeder and the new
one run beside each other exactly as two new ones cannot. Stop the old one and
wait for it to exit before starting the new one, which is the order you would
use anyway.

The evictor's lease is per sweep rather than per process, because a sweep is
what must not overlap: two evictors taking turns are two ordinary sweeps. So
`cline evictor --once` run while the daemon is mid-sweep refuses and says so,
and the daemon skips a pass rather than exiting when it finds one running,
because a daemon that stopped because somebody swept by hand is a fast tier
that fills up. `--dry-run` takes no lease at all: it announces nothing and
removes nothing, so the one command you reach for to understand the system
always answers.

### Logs

Every daemon logs to stderr by default. Under systemd that is what you want,
since journald captures the stream and rotates it for you.

Outside a service manager, give each one a file:

```
bin/cline feeder --config cline.yaml --log-file /var/log/cline/feeder.log
bin/cline router --config cline.yaml --log-file /var/log/cline/router.log
```

`log.file` in the config sets a default for all four, and `--log-file`
overrides it per process, which is how you keep them in separate files. A
configured file behaves like mongod's `systemLog.path`: everything goes to the
file and nothing to the terminal.

Rotation is logrotate's job, and it only works if you signal the process:

```
/var/log/cline/*.log {
    daily
    rotate 14
    compress
    missingok
    notifempty
    postrotate
        pkill -HUP -f '^/usr/local/bin/cline' || true
    endscript
}
```

Without the SIGHUP the daemon keeps writing to the renamed inode, so the file
you are tailing stays empty while the disk fills up.

Both tiers must be replica sets, since the feeder uses change streams.
MongoDB 4.2 or newer.
