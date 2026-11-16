# Try it in ten minutes

You want to see what Thermocline does before you point it at anything real.
This walks through it on Docker with throwaway data: an existing replica set
with two years of orders becomes the archive, an empty one becomes the fast
tier, and you compare every answer through the proxy with the archive's own.

Thermocline is a proof of concept. It has never run in production, so a
laptop or a copy of your data is the right place for it.

What you need: Docker, `curl`, and about 2 GB of memory. Every container is
named `tctry-*`, so the last step removes exactly what this one creates.

## 1. Start two replica sets.

One member each is enough to try it. Both run the same MongoDB release,
because Thermocline refuses a pair that does not:

```
docker network create tctry
docker run -d --name tctry-old  --network tctry mongo:8.0 --replSet rs-old  --bind_ip_all
docker run -d --name tctry-fast --network tctry mongo:8.0 --replSet rs-fast --bind_ip_all
sleep 5
docker exec tctry-old  mongosh --quiet --eval 'rs.initiate({_id: "rs-old",  members: [{_id: 0, host: "tctry-old:27017"}]}).ok'
docker exec tctry-fast mongosh --quiet --eval 'rs.initiate({_id: "rs-fast", members: [{_id: 0, host: "tctry-fast:27017"}]}).ok'
```

`tctry-old` plays the cluster you already have. `tctry-fast` is the new, empty
set that will hold what people actually read.

## 2. Load two years of orders into the old one.

Each order's `_id` is an ObjectId carrying its own creation time, which is
what Thermocline ages it by:

```
sleep 5
docker exec tctry-old mongosh --quiet shop --eval '
const day = 86400 * 1000, now = Date.now();
const hex = (n, w) => n.toString(16).padStart(w, "0");
for (let b = 0; b < 100; b++) {
  const docs = [];
  for (let i = 0; i < 1000; i++) {
    const t = new Date(now - Math.random() * 730 * day);
    docs.push({ _id: ObjectId(hex(Math.floor(t / 1000), 8) + hex(b * 1000 + i, 16)),
                createdAt: t, customer: Math.floor(Math.random() * 5000),
                total: Math.round(Math.random() * 50000) / 100 });
  }
  db.orders.insertMany(docs);
}
print(db.orders.countDocuments());'
```

Output:

```
100000
```

## 3. Download the release.

Pick the tarball for the architecture Docker runs, which is `aarch64` on an
Apple Silicon Mac:

```
docker run --rm mongo:8.0 uname -m        # x86_64 -> amd64, aarch64 -> arm64
ARCH=amd64
mkdir tctry && cd tctry
curl -LO https://github.com/zelmario/thermocline/releases/download/v0.2.0/cline-0.2.0-linux-$ARCH.tar.gz
curl -LO https://github.com/zelmario/thermocline/releases/download/v0.2.0/SHA256SUMS
sha256sum -c SHA256SUMS --ignore-missing   # shasum -a 256 -c on a Mac
tar xzf cline-0.2.0-linux-$ARCH.tar.gz
```

`cline` runs inside a third container on the same network, so it reaches both
sets by the names they were initiated with. That container also has `mongosh`:

```
docker run -d --name tctry-cline --network tctry -v "$PWD":/work -w /work \
  --entrypoint sleep mongo:8.0 infinity
docker exec tctry-cline ./cline-0.2.0-linux-$ARCH/cline version
```

Output:

```
cline 0.2.0
```

## 4. Let `cline adopt` write the configuration.

It reads the old cluster, writes to neither tier, and explains each decision.
`--min-tier 0` tiers a collection this small, which by default (1 GB) would
be kept whole on the fast tier:

```
docker exec tctry-cline ./cline-0.2.0-linux-$ARCH/cline adopt \
  --hot  "mongodb://tctry-fast:27017/?replicaSet=rs-fast" \
  --cold "mongodb://tctry-old:27017/?replicaSet=rs-old" \
  --keep 90d --min-tier 0
```

Output (shortened):

```
  shop.orders        100,000 docs     7.5 MB  tier by _id           12% within 90d, counted
                 _id is an ObjectId whose time agrees with createdAt within a day in 100%
                 of 1000 sampled documents, so it ages by _id and needs no index
...
Wrote cline.yaml: 1 collection(s) tiered, kept 90d on the fast tier.
...
Nothing above refuses a start.
```

The size on disk can read a few KB instead of 7.5 MB, along with the window
estimates under it: right after the load, WiredTiger has not written a
checkpoint yet. The counted 12% does not depend on it.

Read `cline.yaml`. It is two connection strings and one line for the
collection, with the evidence for the decision written beside it.

## 5. Start it.

```
docker exec -d tctry-cline sh -c "./cline-0.2.0-linux-$ARCH/cline run --config cline.yaml > run.log 2>&1"
sleep 10
docker exec tctry-cline grep -E "caught up|router listening|paused" run.log
```

The feeder catches up in a few seconds and the proxy opens on port 27017
inside the container. Two lines say warming and eviction are paused, with
`oplog window is ..., limit 1d`. That is the default, and it is on purpose:
neither copies nor removes anything until the fast tier's oplog covers a day,
so a restore always has room to replay. On a new set that is its first day.

Until then every old document lives only on the archive, which is exactly the
state right after an adoption, and the proxy has to read across both tiers to
answer correctly.

## 6. Compare every answer with the archive's.

The proxy should give the same answer MongoDB gives directly. Save this as
`compare.js` in the `tctry` directory:

```js
const proxy = connect("mongodb://localhost:27017/shop");
const archive = connect("mongodb://tctry-old:27017/shop?replicaSet=rs-old");
const day = 86400 * 1000;
const checks = {
  "count": d => d.orders.countDocuments({}),
  "last 7 days": d => d.orders.find({ createdAt: { $gte: new Date(Date.now() - 7 * day) } }).sort({ _id: 1 }).toArray(),
  "a year ago": d => d.orders.find({ createdAt: { $lt: new Date(Date.now() - 365 * day) } }).sort({ _id: 1 }).limit(5).toArray(),
  "one customer": d => d.orders.find({ customer: 42 }).sort({ _id: 1 }).toArray(),
  "orders by month": d => d.orders.aggregate([
    { $group: { _id: { $dateToString: { format: "%Y-%m", date: "$createdAt" } }, n: { $sum: 1 } } },
    { $sort: { _id: 1 } }]).toArray(),
};
for (const [name, q] of Object.entries(checks)) {
  const a = q(proxy), b = q(archive);
  print((JSON.stringify(a) === JSON.stringify(b) ? "same     " : "DIFFERENT") + "  " + name);
}
```

Run it:

```
docker exec tctry-cline mongosh --quiet --nodb compare.js
```

Output:

```
same       count
same       last 7 days
same       a year ago
same       one customer
same       orders by month
```

Now write through the proxy, the way the application would, and see where the
order lands:

```
docker exec tctry-cline mongosh --quiet --nodb --eval '
const id = connect("mongodb://localhost:27017/shop").orders.insertOne({ createdAt: new Date(), customer: 42, total: 19.99 }).insertedId;
sleep(2000);
print("fast tier:", connect("mongodb://tctry-fast:27017/shop?replicaSet=rs-fast").orders.countDocuments({ _id: id }),
      " archive:", connect("mongodb://tctry-old:27017/shop?replicaSet=rs-old").orders.countDocuments({ _id: id }));'
```

Output:

```
fast tier: 1  archive: 1
```

The write went to the fast tier and the feeder mirrored it into the archive,
so the archive stays a complete copy. Run `compare.js` again: still the same.

Add your own queries to `checks`. Any line that says `DIFFERENT` is the most
valuable report you can send: open an issue with the query and both answers.

## 7. Clean up.

```
docker rm -f tctry-cline tctry-fast tctry-old
docker network rm tctry
```

The `tctry` directory holds the tarball, `cline.yaml` and `run.log`; files
`cline` wrote are owned by root, so remove it with `sudo rm -rf tctry`.

## Where to go next

- [README](../README.md): what each component does and the configuration's rules.
- [docs/adoption.md](adoption.md): the numbered order for a cluster that is
  already serving traffic, with accounts, indexes and the cutover.
- [docs/limitations.md](limitations.md): what it does not do.
- [docs/production-trial.md](production-trial.md): how to run a first trial
  on hardware you would use.
