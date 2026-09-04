# Operating it

Backups, restores and logs.

Part of [Thermocline](../README.md).

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
- **The fast tier's documents, optionally.** They are a subset of the archive
  except for anything the feeder never mirrored, which the daemons name at
  startup as documents "the archive has none of". Warming refills the rest.

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
they are recent enough to be there, and the feeder cannot fetch them: a change
stream reports changes from now, and mirroring only moves forward. Anything
written after the backup and already evicted is in neither tier. So the archive's
backup interval is the real exposure of this system, not the fast tier's, and
`cline rollback`, which counts rather than removes unless you pass `--clear`, is
what tells you what the two tiers disagree about afterwards.

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
