# How tiering decides what to keep

The two questions everything else rests on: telling an eviction from a deletion, and what the evictor refuses to do.

Part of [Thermocline](../README.md).

## Telling an eviction from a deletion

Both look identical in a change stream: a document disappeared. They mean
opposite things. An eviction is a document moving down a tier, and the archive
must keep it. An application delete means somebody asked for the data to be
removed, and the archive must stop returning it.

The evictor announces every document it is about to remove by writing to
`_thermocline.evicting` on the cold tier **before** deleting from the fast tier.
The feeder resolves each batch of deletes against those announcements in one
round trip.

An unannounced delete is treated as an application delete, and the archived copy
is marked with `_tcDeletedAt` rather than removed. Set
`feeder.unannounced_deletes` to `ignore` if you run bulk deletes outside the
evictor, or to `halt` if you would rather mirroring stopped than guessed.

## What the evictor will not do

Three things stand between an old document and removal. All three are enforced
in code, not documented as guidance.

1. **Nothing is removed unless the archive is confirmed to hold that exact
   document, live.** The evictor queries the cold tier for the batch it is about
   to delete and drops anything that comes back missing. An empty or unreachable
   archive can never look like permission to delete.

   *Live* is the word that carries this. A tombstoned copy is a record that the
   application deleted the document, not a copy of it, and reads filter it out,
   so counting it would confirm the archive holds something no read will ever
   return. That state is ordinary rather than exotic: an application deletes a
   record and writes the same `_id` again, which is what a natural key or a
   reprocessed record looks like, and until the feeder mirrors the second write
   the fast tier holds the only live copy while the archive holds a tombstone.
   Measured without that filter: one sweep, and the document was on neither tier
   and the router answered nothing for a record the application had just
   written.
2. **Nothing is removed while mirroring is behind.** Lag is read from the
   feeder's own checkpoint. This check fails closed: if the lag cannot be read
   at all, eviction stops, because removing a document the archive has not
   received destroys it.
3. **A residency hold outranks age.** A document read or promoted recently
   stays, however old it is. That veto is what makes the fast tier a working
   set rather than a date window.

The cluster health signals, replication lag, cache dirty ratio, write tickets
and oplog window, fail open instead. A metric that moves in a future MongoDB
release should not stop eviction forever.

```
bin/cline evictor --config cline.yaml --once --dry-run   # report, change nothing
bin/cline evictor --config cline.yaml --once             # one sweep, then exit
bin/cline evictor --config cline.yaml                    # sweep every 5 minutes
```

A dry run never blocks on cluster health. It changes nothing, so it reports what
would happen and warns separately if real eviction is currently paused.
