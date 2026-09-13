# Which queries are worth caching

Part of [Thermocline](../README.md).

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

**And a write by anybody else drops it too.** The fast tier publishes every
change it makes and this system is built on reading that, so the router follows
the same change stream and drops the answers a write made stale, whoever made
it: another application, a migration script, a person in a shell. Measured: a
client's cached answer of 31 became 32 within a second of another connection
inserting a document straight into the fast tier, without waiting out the TTL.
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
