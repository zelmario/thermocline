# A workload with a realistic shape

Every other suite here proves correctness: it reads ten documents and checks the
answer. None of them says what happens when an application with a realistic
access pattern runs against two tiers, and that is the only question a decision
to deploy this actually turns on.

The shape that matters is **age**. Real reads are not spread evenly over a
collection's history: most touch the last few days, some reach back months, and
almost none touch the oldest records, which is exactly why keeping those on
expensive storage is waste. Everything else about tiering follows from how
strong that bias is.

So the bias is the parameter, and it is stated the way somebody can state it
about their own traffic:

    -newest10 0.7    # 70% of reads touch the newest tenth of the collection

That is the honest boundary of this tool. The data is synthetic; the access
pattern is whatever you tell it, and the numbers follow from that. Point it at a
real figure and the answer is about your workload.

## Running it

Load once, then read as many times as you like:

    go run ./test/workload -uri "mongodb://127.0.0.1:27017/" \
      -ns bench.orders -load 200000 -days 365

    go run ./test/workload -uri "mongodb://127.0.0.1:27100/" -skip-load \
      -reads 50000 -conc 16 -newest10 0.8 \
      -snapshot http://127.0.0.1:27101/api/snapshot

Run it against MongoDB directly for a baseline, then against the router with the
same arguments. The difference is what the proxy costs and what tiering saves.

`-snapshot` is what makes it a test of tiering rather than of MongoDB: from
outside, every read looks the same, and only the router knows which tier
answered. It reports the class counters before and after, so the run's own reads
are the only ones counted.

## What it prints

- **Latency**, split between point lookups and range reads, at p50, p95 and p99.
  Read them as ratios against the same workload run straight at MongoDB, not as
  absolutes: both tiers are usually on one machine while testing.
- **The curve that decides the deployment.** For each tenth of the collection
  held on fast storage, the share of reads it would answer. This is the number
  to take to whoever is paying for the storage.
- **What actually answered**, from the router: the fast tier, the cache, the
  archive, or the archive followed by a copy-up.

## Doing this against real traffic

The gap between this and the real thing is the access pattern, and there is a
way to close it without guessing. Turn MongoDB's profiler on in a copy of
production for a while:

    db.setProfilingLevel(2)

`system.profile` then holds the real commands, with their namespaces and
timestamps. Two things fall out of it: the real value for `-newest10`, by
looking at how old the documents each query touched were, and eventually a
replay, which is the honest end of this road.
