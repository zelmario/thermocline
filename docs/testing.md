# Testing and developing

Part of [Thermocline](../README.md).

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
make check-integration  # adds the tests that need a live pair
make dev-down
```

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
