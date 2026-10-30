# Testing and developing

Part of [Thermocline](../README.md).


### Wire shapes a shell cannot send

`test/seqwrite` writes through the official Go driver, which batches an
`insertMany` into an OP_MSG **document sequence** rather than an inline array in
the command document. Every guard here reads its statements through
`wire.AllOf`, which takes both, and the parsing has a unit test; what that does
not say is whether a guard fires for the sequenced form end to end.

It earns its place by distinguishing: refused with a duplicate key through the
proxy, accepted straight at the fast tier. A check that cannot tell those apart
would pass against a proxy that had stopped guarding sequenced writes
altogether.

`make install` builds it and copies it beside `cline`, and `testkeys.sh` skips
the section with a line when it is not there rather than passing quietly.

`test/unackread` and `test/exhaust` are the same idea one level lower: raw
sockets, for flags a driver will not set.

## Testing on a realistic timescale

Nothing assumes days. The tier boundary is entirely config, so `older_than: 3m`
behaves exactly like `older_than: 90d` and a full lifecycle runs in minutes.
`cline.test.yaml` is a minute-scale profile.

For tests that run repeatedly, fabricate the age instead of waiting for it:

```js
ObjectId.createFromTime(Math.floor(Date.now()/1000) - 86400)   // one day old
```

Age is read from the ObjectId, so a fabricated timestamp is indistinguishable
from a genuinely old document and the same code paths run. ObjectId timestamps
have one second of resolution, so minute-scale boundaries are safe and
sub-second ones are not.

One thing catches everybody: a cluster built minutes ago has minutes of oplog,
and the production default of `min_oplog_window: 24h` blocks eviction outright.
It looks like a hang. Lower it in any test profile.

## Developing

```
make dev                # two single-node replica sets on 28017 and 28018
make check              # fmt, vet, unit tests
make race               # the same unit tests under the race detector
make check-integration  # adds the tests that need a live pair
make dev-down
```

`make race` is separate because it is several times slower. Run it before a
release and after anything that adds shared state to the proxy, which holds a
claim cache, an index cache, a bracket cache and the set of namespaces the
archive is not a superset of: every one of those is read by every connection and
written by one.

What it does not cover is worth knowing. It sees the races the unit tests reach,
and the unit tests do not open two hundred client connections. The suites do
that, against a binary, where the detector is not looking. A clean run is a
floor rather than a guarantee.

Some invariants cannot be tested without a database, because they live in update
filters so that two daemons racing cannot both win. Those tests skip unless
`TC_TEST_COLD` is set, which `make check-integration` does for you.

### Proving the archive survives an accident

With `make dev` running and the feeder pointed at both:

```
mongosh --port 28017 --eval '
  db.getSiblingDB("app").orders.insertMany([{_id:1,x:1},{_id:2,x:2}])'

# wait a moment, then destroy it on the fast tier
mongosh --port 28017 --eval '
  db.getSiblingDB("app").orders.deleteMany({});
  db.getSiblingDB("app").orders.drop()'

# the archive still has both
mongosh --port 28018 --eval '
  print(db.getSiblingDB("app").orders.countDocuments({}))'
```

## Every guard, broken on purpose

A suite that passes says the system works. It does not say the suite would
notice if a guard stopped working, and that is a different question: a check
that cannot fail is worse than no check, because it is counted.

So on 2026-09-14 each of the proxy's guards was disabled in turn -- `return
nil, false` as the first statement of its body -- and the suite that ought to
notice was run against that binary. A guard whose suite still passes is a guard
nothing tests.

    guardUpsert                testupsert     caught   (5 checks failed)
    guardBulkWrite             testbulk       caught   (6)
    guardMergeTarget           testmerge      caught   (7)
    guardSortedFindAndModify   testsingle     caught   (6)
    guardTTL                   testddl        caught   (8)
    guardRename                testddl        caught   (7)
    guardUnmirrorable          testddl        caught   (9)
    guardDBHash                testops        caught   (2)
    guardTieredGetMore         testevict      caught   (2)
    guardSimpleRead            testquery      caught   (1)
    guardAggregate             testaggwrite   caught   (15)
    guardMultiWrite            testunack      caught   (5)
    guardInsert                testunack      caught   (1)
    guardUpdateKeys            testkeys       caught   (19)
    guardNewUniqueIndex        testkeys       caught   (7)

Fifteen disabled, fifteen caught, none survived. Each by the first suite tried,
which is the stronger result: the check that notices is the one a reader would
expect to.

Two of them are caught by a single check in the suite named here, and that is
worth knowing rather than worrying about: `guardSimpleRead` and `guardInsert`
both have heavier coverage elsewhere, in `testconform` and `testkeys`, and the
suite above was chosen for speed rather than for depth.

**What this cannot find is a guard that is missing**, and that is where every
defect of 2026-09-14 actually was: the unacknowledged single-document write, the
unacknowledged `dropIndexes` and `create`, the TTL index that arrives by another
door. Mutation says the guards that exist are watched. It says nothing about the
door nobody has looked at yet.

The mutants are built and run by a pair of throwaway scripts rather than
anything kept here; the method is the part worth keeping. See
[[mutation-to-find-untested-guards]].
