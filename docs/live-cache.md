# The live cache

Part of [Thermocline](../README.md).

## The problem it solves

The query cache in [caching.md](caching.md) stores whole replies and drops them
when a write could have changed them. On a store that is the whole problem:
every checkout changes a product's stock, so every product listing is dropped
many times a second. Measured on a store workload over 40 million orders, it
answered 3.1% of reads, and throughput was lower with it on than off.

The live cache keeps an answer and changes it as the data changes, instead of
throwing it away. It follows the idea ReadySet is built on: a cached answer is a
view, keyed by the values the query was asked with, and the database's own
change stream is applied to it as it happens. A product whose stock changed is
replaced in every listing that holds it, in its sorted place.

It is off by default:

```yaml
router:
  live_cache:
    enabled: true
    max_bytes: 268435456   # 256 MB, the default
    max_staleness: 0s      # read-your-writes, the default
    stream_from: primary   # or secondary
```

## How an answer is kept

An entry is one query's answer for one set of values: a category's listing in
price order, a product by `_id`, the number of products in a category, sales per
hour. Only what ReadySet calls partial views are kept. An entry exists once
somebody asked for it, and it is filled on a miss by asking MongoDB. Nothing
holds a copy of a whole collection.

One goroutine, the applier, reads the fast tier's change stream and applies each
change to every entry in order. A changed document is taken out of every list it
was in and put back where its new values place it. Each change sets a
document's state rather than applying a difference, so a fill that races the
stream converges on the same answer.

A sorted list keeps the head of the answer plus 50 documents, so the next page
and a few deletions are still answerable. It reaches at most 500 deep. A head
that can no longer prove its window, because deletions emptied the extra 50, is
answered by MongoDB until it is filled again.

Memory is bounded by `max_bytes`, least recently used first. An entry keeps only
the fields its query's projection returns, plus `_id` and the fields it filters
and sorts by, which applying a change needs.

## What it answers

It is deliberately narrow, and says no to anything whose answer it could get
wrong. It answers:

- `find` with equality and range (`$gt`, `$gte`, `$lt`, `$lte`) filters on
  top-level fields, a sort of up to three top-level fields, skip and limit within
  500, and a projection of top-level fields.
- `count` and `countDocuments` over the same filters.
- Reports of this shape, which is what a dashboard asks again and again:

  ```
  [$match] [$unwind "$field"] $group [$sort] [$limit]
  ```

  The group `_id` is a constant, a field, or `$hour`, `$dayOfMonth`, `$month` or
  `$year` of a date field. The accumulators are `{$sum: 1}` and
  `{$sum: "$field"}`. Every document the `$match` takes is remembered with what
  it adds to which group, so a change takes its old part away and adds its new
  one. On the Docker rig the store's sales by hour took 19.8ms from mongod and
  its top sellers 58ms.

Everything else goes to mongod, as before:

- Arrays, because `{tags: "a"}` matches a document whose `tags` holds `"a"`.
- `null`, other operators and dotted paths.
- Collections with a default collation, and views.
- A sort over values of mixed types.
- In a report, anything that makes mongod's own answer depend on the order it
  reads documents in: a Decimal128, NaN, an integer near overflow, one group
  `_id` seen as two numeric types, a `$sort` tie across the `$limit`, and more
  groups than the first batch holds.
- A read with a read preference other than `primary` or `primaryPreferred`.
- `admin`, `local`, `config`, `system.*` collections and Thermocline's own
  bookkeeping database.

When a document shows that a shape cannot be answered exactly, for example an
array arrives in a field the shape filters on, the shape is refused for as long as the router
runs and counted in `live_refused_shapes`.

## Reading your own writes

ReadySet's answers lag its replication stream by however long that takes, and it
does not promise that a client sees its own write. Thermocline does.

With `max_staleness: 0s`, the default, a write through this router to a
collection sends every read of that collection to mongod until the change
stream has applied the write. On a collection written all the time, that is
nearly every read. Measured on a store at 32 shoppers: 1,681,919 reads were held
for the stream against 196,274 answered, and throughput was lower with the cache
than without it.

`max_staleness` is a bound in place of that. The cache answers while no write
this router acknowledged has gone unapplied for longer than the bound. An answer
may miss a write, but never one acknowledged longer ago than the bound. That is
ReadySet's model with a promise ReadySet does not make.

Writes inside a transaction, and on 8.0 the documents of one batched write,
share one cluster time. The cache never answers with part of such a group
applied: a namespace the open group has reached is sent to mongod until the
group closes.

## Who may read what

With authentication proxied, mongod decides who may read what, and an answer
from memory skips mongod. So a user is answered from memory only for a
namespace mongod let them read within the last minute. Anybody else goes to
mongod, and its yes is what lets them in next time. Revoking access takes
effect here within that minute.

## Tiered collections

A tiered collection is held as far as the fast tier can prove its answer, and
it differs in three ways:

1. A sorted list is filled with only what the router's newest-first proof says
   the fast tier's answer settles. What it does not prove is merged with the
   archive's answer, read at the same position.
2. An eviction is a delete on the fast tier's change stream, of a document that
   still exists. So a document leaving the fast tier drops every entry that held
   it, instead of being taken out of it. A copy-up needs nothing: it inserts the
   document's real state, and state is what is applied.
3. A fill that reads the archive competes with the client reads that must go
   there. So a tiered entry is filled on its second miss within two minutes,
   which is TinyLFU's doorkeeper. Without it, on the 200 million order adoption,
   the fills took the archive's volume to 98% busy and "my orders" went from a
   p95 of 14ms on the old cluster alone to 213ms.

A tiered entry is asked again every 10 seconds, so it is never kept longer than
the router would trust the proof it rested on.

## Shapes that never repeat

Every miss asks for a fill. That pays when the entry is asked for again, and a
shape whose every query names a different key never is. On staging, YCSB's read
by `_id` made 6.4 million lookups in five minutes with 36,000 hits (0.56%) and
206,000 fills, each one a read of the fast tier and some a read of the archive.

So each shape is judged every 20,000 lookups. A shape answered for under 1% of
them asks for no new fills for 10 minutes, then is judged again with fills on.
No answer changes: its existing entries still answer and are still kept exact.
It is counted in `live_shapes_rested` and `live_fills_rested`.

## Reading the stream from a secondary

`stream_from: secondary` reads the change stream from a secondary when one is
up. The stream's events are majority committed, which a secondary holds, and
everything the cache reads directly still goes to the primary, so reads stay at
least as new as the stream. What moves is the oplog scan and the lookup of
every updated document, off the node every client is waiting on.

## What to watch

Every counter is on the dashboard and in Prometheus with a `_total` suffix (see
[monitoring.md](monitoring.md)):

| Counter | What it counts |
|---|---|
| `live_hits` | Reads answered from memory |
| `live_misses` | Reads it could take but did not hold, each asking for a fill |
| `live_waited_for_stream` | Reads it held but sent to mongod, because a write was not yet applied within `max_staleness` |
| `live_fills` | Entries filled from MongoDB |
| `live_unproven` | Tiered fills the fast tier could not prove, so not held |
| `live_shapes_rested`, `live_fills_rested` | Shapes put to rest, and the fills they did not ask for |
| `live_dropped` | Entries evicted for space, or invalidated by a change it could not apply |
| `live_refused_shapes` | Shapes refused because a document showed they could not be answered exactly |
| `live_entries`, `live_bytes` | What it holds now |

A high `live_waited_for_stream` against `live_hits` means read-your-writes is
holding most reads back. That is the case for `max_staleness`.

## How it is proved

A cache that answers almost nothing passes every test. The suites ask most
questions once, and the cache answers the second time, so a passing sweep with
it on proves little. Three things prove it instead:

1. **Verify mode.** `verify_log` makes the cache answer nothing. Every read it
   could answer is answered by mongod and compared with what the cache holds,
   read with `readConcern: snapshot` at the cache's own position. Any difference
   is a defect, not a race. `LIVE=1 test/psweep.sh` runs the sweep this way and
   fails on a mismatch or on 0 answers compared.
2. **A workload aimed at the fragile paths.** `test/cachestress` drives writers
   and readers at the same documents, with transactions, batched writes and
   large documents.
3. **A mutant for every rule.** Each check is run against a build with that rule
   broken, and must fail. Changes not applied gave 1,896 wrong answers out of
   1,965; the read-your-writes gate removed gave 636 stale writer reads; a report
   that does not take a changed document's old part away gave 699 wrong out of
   1,096.

A check that passes its mutant is not a check. The latest gate was the cache
suites in verify mode at 876 checks, 0 failed.
