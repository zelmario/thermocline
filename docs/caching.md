# Which queries are worth caching

Part of [Thermocline](../README.md).


## What the cache will not answer

A cached answer is served before any guard runs, deliberately: deciding
completeness for it would be deciding about a read that is not happening. There
is one exception, and it is about the namespace rather than about the read.

While the archive is missing documents the fast tier holds, because its unique
index refused them and mirroring gave up, every fall-through read of that
namespace is refused rather than answered short. An answer stored before that
began is exactly the short answer the refusal exists to prevent, so the cache is
not consulted for such a namespace at all.

It costs nothing in a healthy deployment: the set is read from the router's own
cached copy with no round trip, and it is empty. A write to the collection would
drop the stored answers anyway, and the state does arise from a write, so this
closes a window rather than a gap that was standing open. It was found by
reading the order the two run in rather than by seeing it happen: attempts to
serve a stale answer past the refusal ran into the learned TTL expiring first,
every time.

The other thing it will not answer is a namespace nothing can tell it about.

Two things drop a stored answer, and both have a reach. A write through the
router drops what it could have changed in that collection, and the fast tier's
change stream covers the writers that go around the router. That stream is a
client-level one, which MongoDB does not open on the `admin`, `local` or
`config` databases and does not deliver system collections through, and the
archive's own bookkeeping database sits on the other tier, so no stream of the
fast tier's carries it either.

For those the learned TTL is not a bound on how stale an answer can be, it is
the whole of the answer's life. So they are not stored at all:

- `admin`, `local` and `config`, whole databases
- any collection whose name begins `system.`, in any database
- Thermocline's own bookkeeping database, `_thermocline` by default

A collection merely named `systems`, or a database named `configuration` or
`locality`, is the application's and is cached as usual.

Measured before this rule existed, on a router with the cache on: eighteen reads
of `local.oplog.rs`, `admin.system.users` and the archive's watermarks were
looked up 11 times and left 2 entries behind. Whether one of those is then
served back is a race against the learned TTL, so what the suite checks is that
nothing goes in. Nothing an application asks lives in these namespaces, so the
cost of the rule is a slower answer to a question the cache was never there for.

## Which queries are worth caching

The router sees every read and every answer, which is what a cache advisor
needs. For each read it records three things: the **shape** of the query, a hash
of its **parameters**, and a hash of the **result**. Nothing of the query's own
data is kept, only hashes.

    db.users.countDocuments({country: "UY"})
    db.users.countDocuments({country: "AR"})

is one shape asked twice, not two queries, and the split is what makes the
question answerable. From it the router works out, in this order:

1. Which shapes are asked repeatedly at all. A query asked once is not a
   candidate however stable its answer is.
2. Among those, which parameters repeat. A shape asked a thousand times with a
   thousand different values has no repetition to exploit, and saying so is a
   finding.
3. For those, how long the same result kept coming back for the same
   parameters.
4. A suggested TTL, ten per cent inside that, so the suggestion is always
   within the evidence for it.

## The fourth ask is the first one answered

A query has to be asked **four times** before the cache answers one of them, and
that is the number to hold a workload against.

The first ask has no evidence of anything and stores nothing. The second derives
a TTL from the single interval it has watched, ten per cent inside it, so the
entry expires just before the third arrives. The third stores again from the
longer span it has now seen, and that one reaches the fourth. After that a
stable query settles at roughly three hits in four: the single miss in each run
is the entry being re-validated against the database, because a hit deliberately
does not refresh its age. Measured over twenty asks at a steady interval, and
the same at one second, ten seconds and a minute apart:

| the answer | asks 1-20 | hits |
|---|---|---|
| never changes | `...H.HHH.HHHHHHH.HHH` | 14 |
| changes every fourth ask | `...H.HHH.HHH.HHH.HHH` | 13 |
| changes every second ask | `...H.H.H.H.H.H.H.H.H` | 9 |
| changes every time it is looked at | `....................` | 0 |

Two things follow. A query whose answer holds still for even a few looks is
nearly as cacheable as one that never changes, because the TTL is drawn from how
long the answer actually held rather than from a guess. And a query whose answer
is different every time costs nothing but the observation: it is never stored
and never served.

The warm-up is also why a hit rate measured over a short run understates the
feature. A pilot that asked each of its questions about three and a half times
recorded 7%, and the conclusion drawn at the time was that the workload did not
repeat itself. It did; it stopped one ask short.

## What the report says

`cline cache` prints it, most valuable first, ranked by how often a query is
asked weighted by how long each answer would survive:

    SUGGEST   STABLE    ASKED   VALUES QUERY
    4m30s     5m0s      312     3      count shop.users query={country:?}
    1.4s      1.6s      880     47     find shop.orders filter={customer:?} sort={at:?}

A count that returned 3,512,345 for five minutes, then 3,512,344 for fifteen,
then 3,512,346 for ten is stable for five and suggested at four and a half: the
shortest stretch decides, because a query that usually holds for an hour and
sometimes for a second is a query that changes every second as far as anybody
reading it is concerned.

**The measuring runs whether or not the caching does.** The advice is the reason
to turn caching on, so it cannot require caching to be on already. With
`router.cache.enabled` the same measurements drive the TTLs. The dashboard shows
the same table, so the decision can be made from the page rather than the
command line.

**All of it can be turned off.** The dashboard is off unless `router.dashboard`
names an address; the caching is off unless `router.cache.enabled`; and the
measuring, which costs a hash of every reply and a bounded table of shapes, is
off with `router.cache.measure: false`. A router with all three off does exactly
what it did before any of this existed.

### What it costs, and what bounds it

`max_queries` bounds how many queries are watched and `max_bytes` how much the
held answers may occupy, because counting entries is not a memory bound: a
thousand replies of a megabyte each is a gigabyte, and nothing in a count says
which of those it is. A single answer larger than 256 KB is learned from and
never held, since a large result is both less likely to repeat and more
expensive to be wrong about.

**`max_queries` is capped at 100,000, and the reason is a pause rather than
memory.** Once the advisor's table is full, every insert past it evicts, and an
eviction builds a slice of every shape and sorts it while holding the lock each
cacheable read waits on. Measured per eviction:

| `max_queries` | one eviction |
|---|---|
| 1,000, the default | 89us |
| 10,000 | 823us |
| 100,000 | 14.2ms |
| 1,000,000 | 154ms |

A batch clears a tenth of the table, so the amortised cost is small at every one
of those rows. The stall is what matters: 154ms on a proxy whose whole overhead
is 130us is an outage for whoever is behind it, and "cache more" is the obvious
thing to type. Anything above the last row that stays in milliseconds is refused
at startup, with those numbers in the message.

**Nothing is learned from an answer the cache produced itself.** The reply goes
back through the same funnel every answer does, and that funnel learns from what
it sends, so every hit was being recorded as a fresh observation of the same
value: the entry's age was reset and a query asked steadily was never re-fetched.
Measured with a two second ceiling and thirteen seconds of reads, the router
asked the fast tier four times and served forty one from memory. Now it asks
eleven times, which is what a two second TTL means. Stability is measured against
the database, never against what this already believed.

### What the cache will not do

A cache is the one thing here that can answer without asking, so what it refuses
matters more than what it serves. Never cached: anything inside a transaction,
because a transaction reads a snapshot and an answer from before it began is not
in that snapshot; a read concern naming a cluster time, or asking for snapshot or
linearizable, because those are promises about a position in an oplog or a
confirmation from a primary; and a reply that leaves a cursor open, because half
an answer is not an answer.

**A write through the router drops what was learned about that collection**,
rather than waiting out a timer. That is what makes this correct for the writer
rather than merely fast: the ordinary pattern of writing and then reading the
same collection is right immediately.

**Only what the write could have changed, since 2026-10-09.** Dropping every
answer on every write was correct, and on a collection anything writes to it was
also why the cache served nothing. The shop's monthly report counts a closed
month, and it was dropped by every order placed today: at 8.6 orders a second a
held report lived about 0.12s, and 1.3% of reports were served from the cache.
Now each held answer remembers what it read, and a write is judged against that
first:

- **An insert** drops an answer only if one of its documents could pass the
  answer's filter. A document is ruled out only on what the router fully
  understands: a top-level field holding a number, a date or an ObjectId,
  compared with `$eq`, `$gt`, `$gte`, `$lt` or `$lte` against a value of the
  same kind, under `$and`, `$or` or plain conjunction. Strings are left to the
  collation, arrays to their elements, and anything else counts as passing.
- **An update** drops an answer only if it changes a field the answer read. That
  covers the filter, the sort, the projection, and every field a pipeline
  stage names. A `find` without an inclusion projection returns whole
  documents, so every field counts as read.
- **Everything else** drops every answer on the collection, as before: deletes,
  replacements, upserts, pipeline updates, `findAndModify`, an aggregation with
  a stage outside `$match`, `$group`, `$count`, `$sortByCount`, `$sort`,
  `$limit`, `$skip`, `$project`, `$addFields`, `$set` and `$unset`, and an
  answer read through a view or a `$lookup`.

Every rule errs the same way. Keeping an answer that changed is a wrong answer,
and dropping one that did not is a lookup. `cache_spared_total` counts the
answers kept, beside `cache_invalidated_total`.

**And a write by anybody else drops it too, by the same rules.** The fast tier
publishes every change it makes and this system is built on reading that, so the
router follows the same change stream and drops the answers a write made stale,
whoever made it: another application, a migration script, a person in a shell.
On 6.0 and later the stream brings an insert's document, up to 1MB, and an
update's field names, so those writes are judged as above. A larger document,
or any write on an older release, drops everything that read its collection.
Measured: a client's cached answer of 31 became 32 within a second of another
connection inserting a document straight into the fast tier, without waiting
out the TTL.
That is the difference between a cache that expires and one that is kept honest.

That stream is no longer the cache's own. The router follows the fast tier
whether or not the cache is on, because it is also how it hears that a collection
was dropped out of band, and the cache is one of two things it corrects. With the
cache off the stream asks the server for the DDL events alone, so it costs
nothing per write; with it on every event is wanted and none are filtered. Either
way it is one change stream per router, and the dashboard's `watching` says
whether it is open.

The learned TTL is what remains: a backstop for the moments before the stream
catches up, bounded again by `max_ttl`.

**The key carries who is asking**, or one client would be served another's
documents, which is a bypass this system has had once already. With
`terminate_auth` on that is the verified user, and answers are shared between
that user's connections. With authentication proxied the router does not know
who the client is, so the connection is the identity, and never one client with
another's answer.

**That costs a pooled driver almost all of it,** which is worth being plain
about. A pool spreads repeats across its connections, so the second asking of a
question usually lands somewhere the first was never stored. Measured with
`test/store`, a shop of 3,000 orders read through five connections with the
cache on: **7 shapes, 378 lookups, 0 hits.** The same run with
`terminate_auth` on, where the identity is the user and the pool shares one
cache: 5 hits in 378.

Five is not a rounding error away from zero, it is the same answer. The cache
serves a query repeated with the *same parameters*, and a shop asking about a
different customer each time never repeats one. What it is for is the workload
that does: a dashboard refreshing the same figures, a report run on a timer, a
hot lookup every request makes with the same argument. `cline cache` ranks what
your traffic actually repeats, and if the top of that list is thin then this
feature is not for you. It stays off by default for that reason.
