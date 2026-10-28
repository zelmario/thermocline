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

A standalone archive also loses one guarantee, and the feeder says so the first
time it matters. Mirroring writes each flush as one transaction on the archive,
so a reader there never sees a write to one collection without an earlier write
to another, such as an order's event without the order. A standalone mongod
cannot run transactions, so there each collection is written on its own and, for
the length of one flush, it can. A one-member replica set is enough to keep it.

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
  Take `_thermocline.checkpoints` **before** the application databases, or take
  everything in one snapshot. A restore replays from that checkpoint, and one
  older than the data only replays more, while one newer than the data skips
  what the data is missing.
- **A snapshot backup holds mirroring while it runs, and nothing is lost.**
  `db.fsyncLock()` on the archive, which is what a filesystem or volume snapshot
  needs, blocks writes rather than failing them, so the feeder simply waits.
  Measured: two writes made through the router during the lock, the feeder
  silent throughout with no WARN or ERROR of any kind, and
  `thermocline_mirroring_lag_seconds` climbing 43, 58, 72 seconds while it was
  held. On `fsyncUnlock` the lag was back to 0.015s within seconds and both
  writes were in the archive.

  So the lag gauge is the thing that shows a held lock, and the alert rule on it
  will fire if the lock outlives the alert's window. That is the right signal
  and the feeder's own silence is not a defect: a blocked write and a slow one
  look the same from inside it. What it means for an operator is to expect the
  lag alert during a snapshot, and to treat one that does not clear afterwards
  as a lock that was never released.

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
  The daemons say so at startup ("the archive has accounts the fast tier does
  not"), and `cline preflight` reports it without starting anything, which is
  the check to run after a restore rather than the thing to rely on instead of
  taking the backup.

- **Every collection on the fast tier that nothing tiers, and this one is not
  optional.** "The archive is the complete copy" is true of what the feeder
  mirrors and false of everything else: a collection missing from
  `feeder.namespaces` has lived on the fast tier alone since the cutover and the
  archive has never held a copy. Take those with `mongodump` of the fast tier.

  Measured by `test/testrestore.sh`, which takes exactly the list above and
  nothing else, loses the fast tier and puts it back: the orders came back, the
  tickets came back, and the shop had no customers, no products and no marker,
  because none of them was ever in the archive to come back from. The daemons
  name them at every start, as "the archive holds collections the fast tier does
  not" in the other direction and as collections "nothing tiers" in this one.

- **The fast tier's index definitions.** Not the indexes themselves, the
  statements that create them: `db.coll.getIndexes()` per tiered collection, or
  the output of `cline indexes`. Nothing copies an index to the archive on
  purpose, because an index describes the whole collection and carrying every
  one of them on the archive gives back most of what tiering buys. The
  consequence is only visible on the way back: a fast tier rebuilt from the
  archive comes back with the collections, their options and their documents,
  and none of the indexes the application declared.

  For a plain index that is a slow query. **For a unique index it is a lost
  constraint**, because uniqueness across the tiers is enforced from the fast
  tier's own listing, and an index no tier declares is not enforced by anything.
  Measured: with the key registry restored and 200 rows in it, an insert
  duplicating a value an archived document held was accepted. A router says so
  now, at every start, as "the key registry holds claims for a unique index no
  tier has", which is the one trace left that the constraint existed.

- **The fast tier's documents, optionally.** The ones the feeder mirrored are a
  subset of the archive, so warming refills them. What it cannot refill is the
  two entries above.

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

Pausing is still the procedure, and the reason for it changed on 2026-09-26.
The evictor's check used to be presence only, so an archive holding an *older*
version of a document confirmed it as present and the fast tier's newer copy
was removed: measured by `test/testevict.sh`, five documents updated after the
backup went from the fast tier and the newer value was in neither tier.

The evictor now evicts only a document whose archived copy is byte for byte the
one on the fast tier, so a restored archive no longer costs the writes the fast
tier still holds; the same suite measures all five kept with eviction running.
The pause stays in the procedure because a restore is not the moment for any
daemon to be moving documents, and because the replay below is what makes
those documents evictable again.

**And the limit worth knowing before you need it.** A restored archive is a
restored past: every insert, update and delete the application made after the
backup is missing from it. The fast tier's oplog still holds them, so the
feeder replays them, from the checkpoint taken with the backup:

```
bin/cline pause --config cline.yaml --reason "restoring the archive"
systemctl stop cline-feeder
# restore the application's databases on the archive, then the feeder's
# checkpoint from the same backup:
mongorestore --uri "$ARCHIVE" --drop --nsInclude '_thermocline.checkpoints' backup/
systemctl start cline-feeder     # logs "resuming from checkpoint"
bin/cline resume --config cline.yaml
```

Restore only the checkpoints from the bookkeeping backup. The rest of
`_thermocline` describes the archive as it was a moment ago, and that is the
more accurate description.

**A replay also meets every eviction since the backup**, and an eviction is
a delete on the fast tier like any other. The feeder tells them apart by an
announcement it spends as it mirrors, so a replay finds them spent and, until
2026-10-14, read them all as the application deleting the document and marked
the archive's only copy deleted. Measured on the staging pair: ten documents
evicted after the backup, the checkpoint restored with it, and a client read
none of them. The fast tier now keeps a record of every eviction the feeder
has confirmed, in `_thermocline.evicted`, for `feeder.eviction_record` (8 days
by default), and a replay checks it first. The same run with the record: a
client read all ten, and the one document deleted after the backup stayed
deleted. `test/testbackfill.sh` walks exactly those steps.

So two numbers bound the backup you can restore from, and both have to be
older than it: the fast tier's oplog window and `feeder.eviction_record`.

- **Past the oplog window**, the feeder stops with "the fast tier's oplog no
  longer reaches where mirroring got to". `cline backfill --apply
  --clear-checkpoint` is the remedy, with the feeder stopped: it copies what
  the fast tier holds and marks deletes only inside the range the fast tier
  claims to hold in full. A document deleted after the backup and outside
  that range comes back, and deleting it again through the router removes it.
- **Past `feeder.eviction_record`**, the feeder refuses before it reads
  anything ("the checkpoint is older than the eviction record reaches"),
  because evictions in that window can no longer be told from deletes. The
  remedy is the same command.

Leaving the feeder running and copying with `cline backfill --apply` instead,
which is what this section said until 2026-10-14, brings back every
document the application deleted after the backup: ten mirrored, a backup,
one deleted, and a client read ten where the application had left nine.

What no replay can fetch is a version only the lost archive held. A document
changed after the backup and evicted after that comes back as it was before
the change: the newer version reached the archive, the evictor confirmed it
there and removed the fast tier's copy, and the restore took the archive's.
The feeder names each one as it replays ("a document evicted during the
replayed window comes back older than the application left it") and counts
them as `replayed_stale`, so the list of documents to rewrite from the
application's own records is in the log rather than in somebody's bug report.
That is why the pause comes first, and why the archive's backup interval is
the real exposure of this system rather than the fast tier's. `cline pause`
returns only once no sweep is in progress: a sweep that was already running
stops before its next batch, and the command waits for it.
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

**The same goes for the newer protections, and in the same direction.** A feeder
records the documents the archive refuses, and a router reads those records to
refuse the reads that would be short by them. During an upgrade the halves are
apart: an old feeder records nothing, so a new router has nothing to read and
answers as everything did before; a new feeder records them and an old router
ignores the records and answers short, which is what every version did until
this one. Neither is worse than the version it replaces, and the protection is
whole once both ends are upgraded. Upgrade the routers last, so the feeder has
already recorded what the routers are about to start acting on.

The evictor's lease is per sweep rather than per process, because a sweep is
what must not overlap: two evictors taking turns are two ordinary sweeps. So
`cline evictor --once` run while the daemon is mid-sweep refuses and says so,
and the daemon skips a pass rather than exiting when it finds one running,
because a daemon that stopped because somebody swept by hand is a fast tier
that fills up. `--dry-run` takes no lease at all: it announces nothing and
removes nothing, so the one command you reach for to understand the system
always answers.

### Running them as services

`packaging/install.sh` installs `cline.service`, which runs `cline run`: every
daemon in one process, in the order below, with the feeder, evictor and warmer
restarted inside it after ten seconds and the whole unit restarted by systemd
when the router stops.

```
sudo packaging/install.sh bin/cline
sudo install -m 0640 -o cline -g cline cline.yaml /etc/cline/cline.yaml
cline preflight --config /etc/cline/cline.yaml
sudo systemctl enable --now cline
```

It installs a unit for each daemon as well, for running them apart, such as
routers on more hosts. Enable `cline` or those on a host, not both. By hand,
the order `cline run` keeps is:

```
sudo packaging/install.sh bin/cline
sudo install -m 0640 -o cline -g cline cline.yaml /etc/cline/cline.yaml
cline preflight --config /etc/cline/cline.yaml
sudo systemctl enable --now cline-feeder          # and let it catch up first
sudo systemctl enable --now cline-router          # builds the archive's indexes on the fast tier
cline keys --rebuild --config /etc/cline/cline.yaml
sudo systemctl enable --now cline-warmer.timer cline-evictor
```

The evictor is last and follows the rebuild, which is step 7 of
[docs/adoption.md](adoption.md): the key registry only protects values it knows
about, and an adopted cluster starts with none of its keys claimed.

The installer creates a `cline` system account that owns neither tier, the
directories, the units and the logrotate file, and it is safe to run again: the
one thing it never writes is `/etc/cline/cline.yaml`, because the two URIs in it
are the only thing here nobody can guess and overwriting a working one during an
upgrade is the kind of help nobody wants. Run over a deployment that is already
up, it says so and gives the restart order rather than the install one.

The config holds the credentials for both tiers, which is why it is `0640` and
owned by the account rather than by root.

**Three of them are daemons and the fourth is not.** The feeder, the router and
the evictor run until they are stopped. `cline warmer` makes one pass, prints
`warm complete` and exits, so it is a `oneshot` unit driven by
`cline-warmer.timer` rather than a service with `Restart=`. Written the other
way it warms the fast tier every ten seconds for ever. The timer ships at
hourly, which is a starting point and not a recommendation: a warm exists to put
back what reads are about to ask for, so the cycle belongs to the application's
reading pattern, and a warm with nothing to do copies nothing.

**The warmer needs the feeder running.** Before each batch it waits, up to 30
seconds, for mirroring to reach what the fast tier has written, because a
document the application deleted a moment ago is still live in the archive
until its tombstone lands, and copying it up would undo the delete. With the
feeder stopped the wait never ends, and the warm stops with `not warming while
mirroring is behind the fast tier` and resumes on the next run.
`cline warmer --without-feeder` skips the wait. Use it only when nothing is
deleting documents, such as a fast tier being refilled with the application
stopped; it says what it gives up every time it runs.

**Restart=always on the feeder is what makes a standby.** Install the feeder
unit on two hosts and enable it on both. One mirrors; the other is refused every
ten seconds by the lease, which says who holds it, and takes over when the
holder stops: 122 ms after a clean stop, 129 ms after `kill -9`, thirty seconds
when the host itself is gone. That is why the unit also sets
`StartLimitIntervalSec=0`. systemd's default gives up after five failures in
ten seconds, and a feeder waiting out a lease is a feeder doing its job, not one
failing.

**A feeder that was stopped for hours can take minutes to start mirroring.** It
resumes from its checkpoint, and mongod reads the fast tier's oplog forward to
that point before the stream returns its first event. Measured on this
project's test box: a checkpoint 44 hours and 1,433,277 oplog entries (1.0GB)
behind took 2m58s to open, and 5m45s from the same point an hour later with the
oplog's pages no longer in cache, against 0.08s for one a few minutes behind. Nothing
is mirrored in between, so routers report mirroring as stopped and reads past
`archive_wait` that need the archive are refused, or answered stale under
`router.freshness: permissive`. The startup line says
`resuming from checkpoint` with `behind=` when the checkpoint is a minute or
more old, and `mirroring` when the stream is open. Silence between those two is
the oplog scan, not a hang. The takeover times above are for a standby, whose
checkpoint the previous holder kept current.

**The router's restart loop is the archive outage procedure.** A router refuses
to start while the archive is unreachable, on purpose. `Restart=always` turns
that into "it serves again as soon as the archive is back" rather than "somebody
has to notice". Nothing orders the router after the feeder: that order belongs
to the day you adopt a cluster, not to a reboot.

**The router's unit raises `LimitNOFILE`, and it has to.** While authentication
is proxied, which is the default, each client costs two file descriptors on the
router: its own socket and the upstream connection carrying its SCRAM identity.
systemd's default soft limit is 1024, so a router left with it meets "too many
open files" at about five hundred clients, before the thousand-client warning
the router itself prints, and well before mongod's connection limit, which is
where this document says the wall is. The unit sets 65535 so that stays true.

**`ProtectSystem=strict` is why `log.file` has to live under `/var/log/cline`.**
The units make the whole filesystem read-only and name that one directory as
writable. Point `log.file` somewhere else and the daemon starts and writes
nothing, so add the directory to `ReadWritePaths=` in the unit, or leave the
logs in the journal where the units put them by default.

**A stop closes idle client connections and waits for the ones mid-command.**
A router holds one goroutine per client, parked in a read, and an application's
pool holds those sockets open indefinitely, so a stop that waited for them
waited for ever: measured, a router with one idle client was still running 60
seconds after SIGTERM against one second with none, and systemd then waited out
`TimeoutStopSec` and sent SIGKILL. A stop now closes the idle ones at once,
which is the reconnect the router already warns about, and gives a command in
flight five seconds before cutting it. Keep `TimeoutStopSec` above that grace,
which the default 90 seconds comfortably is.

**What a router restart costs the application, measured.** With two shops
writing through one router at about 115 operations a second each, a `SIGTERM`
stopped it in **1 second**: the connections parked on a read are closed at once
and the clients reconnect. Starting again and answering `ping` took **1.0 to
1.1 seconds** with 176 tiered namespaces, because the listener opens after the
archive's indexes are built on the fast tier.

So a restart is roughly **two seconds during which that router refuses
connections**, and an application pointed only at it sees exactly that: every
error in the run was `connection refused`, none was a wrong answer, and both
shops' invariants held throughout. Whoever needs no such gap runs more than one
router and restarts them one at a time, which is step 13 of
[docs/adoption.md](adoption.md).

**A restart does not resume eviction.** `cline pause` writes the pause into the
archive's bookkeeping, not into the process, so an evictor restarted while the
archive is being restored comes back paused. `cline resume` is the only thing
that ends it.

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

Rotation is logrotate's job, and `packaging/logrotate/cline` is the file. It
only works if you signal the process:

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

**A daemon with no `log.file` ignores SIGHUP rather than dying of it**, which it
did not until 2026-09-21. Go terminates a process for an unhandled SIGHUP and
the handler that reopens a file was only installed when there was a file, so a
daemon logging to the journal was killed by the signal above. That matters
because the snippet signals every `cline` process by name: a deployment that
gives one daemon a file and leaves the others on the journal killed the others
at the first rotation, and systemd restarted them, so the symptom was daemons
restarting nightly for no reason anybody could see.

Both tiers must be replica sets, since the feeder uses change streams.
MongoDB 4.2 or newer.
