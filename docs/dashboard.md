# The dashboard, and authorisation

The operator page, and what terminate_auth changes about who may read what.

Part of [Thermocline](../README.md).

## The dashboard

Set `router.dashboard` to an address and the router serves one page showing what
each tier holds and how it is being used:

```yaml
router:
  dashboard: "127.0.0.1:27101"
```

It answers the three questions the design makes claims about, with measurements
rather than assertions:

- **what share of commands the fast tier answered on its own**, against the ones
  that needed the archive;
- **what that is worth**, as the measured mean latency of a fast tier hit beside
  the mean of a read that fell through, and the ratio between them;
- **how little of the data has to be hot** to buy that, from collection metadata
  on both tiers.

Underneath are commands per second by how they were served, latency per class on
a log scale, per-collection tier occupancy, and the counters an operator watches:
copy-ups, refusals, stale reads and cross-tier duplicates stopped. Everything on
the page is also available as a table, and as JSON at `/api/snapshot`.

The page has no authentication of its own. It names collections and reports
traffic, so bind it to an operator network rather than to the address the
application dials.

`test/traffic.sh` drives representative traffic down every path if you want to
see it with something on it, and `test/load.sh` keeps it running.

### Indexes go up, never down

The archive is expected to hold far fewer indexes than the fast tier. Index trees
describe the whole collection, which is the cost tiering exists to avoid, so
carrying every one of them on the archive gives most of the saving back. Nothing
copies an index from the fast tier down to the archive, and nothing should.

**Upward is the opposite case, and it was wrong for the whole of adoption.** The
fast tier is filled by copying documents, and an index is not a document, so a
tier that has just been built holds everything the application asks for and no
way to find any of it except by `_id`. That is the shape of every adoption: the
existing cluster becomes the archive carrying the application's indexes, and the
new fast tier starts bare.

Measured on a 5,000,000 order shop, archive 1.32 GB against a 1 GB cache, hot
window 30 days:

| | fast tier bare | indexes built |
|---|---|---|
| throughput | 35/s | **78/s** |
| a customer's order history | 501 ms | **160 ms** |
| read an order by date | 543 ms | **159 ms** |
| a monthly revenue report | 1.23 s | **680 ms** |
| the warm itself | 672 s | **639 s** |

Day one, before any warm, was 62/s. So warming a bare fast tier *cost* 44% of
the throughput: it moved reads onto a tier that scans 411,000 documents while
the archive it was relieving was seeking an index. Building them costs nothing,
because an index built on an empty collection is free and one built on a full
one is a scan.

Three paths do it, through one implementation in `internal/promote`: the warmer
before it fills a namespace, the router the first time it copies a document up,
and the router at startup for every namespace it manages that the archive
already holds. Once built, never again.

**A view is stranded like a collection and cannot be fixed like one.** The
application's views live on the existing cluster; the fast tier has none of
them, so reads of them return nothing at all. Measured on an adopted view over a
tiered collection: five documents on the original cluster, zero through the
router, no error. The router names views separately at startup because the
advice differs, and the advice for a collection cannot be followed for a view:
tiering one is meaningless, so the feeder refuses it. Recreate them on the fast
tier with `createView` and tier the collections they read from.

**A view created through the router is given to the archive as well**, and that
buys something beyond a rollback working. A read through a view used to be
refused whenever the collection underneath it was incomplete, because the archive
held the documents and not the definition; when it holds both, the read has a
correct answer instead of an error. Measured on a view over a collection the fast
tier held one document of, with the archive holding three: refused before,
answers two now, which is what the archive's own view answers.

Only when the two definitions are identical, and that is the whole of the
condition: the collection read, the pipeline and the collation all matching. A
view whose pipeline differs between the tiers answers a different question, and
answering the archive's version of it would be exactly the silent difference this
system exists to prevent. A view the archive does not have, or has differently,
is still refused with the reason.

**The accounts are the other thing the new tier does not have.** The existing
cluster carries the application's users; the fast tier is a new replica set and
carries none of them. The router authenticates against the fast tier, so on day
one every connection is refused with MongoDB's ordinary `Authentication failed`
and nothing connects that message to the tier missing the account. The refusal
is correct; the silence is the failure. The router now names them once at
startup, and creating them on the fast tier with the same names, passwords and
roles is the whole fix.

**The collection comes before its indexes, for the same reason and with worse
consequences.** A collection is more than its documents, and everything about it
other than documents arrives on the fast tier only if something puts it there.
In an adoption nothing does: the fast tier's copy is brought into existence by
the first insert, which carries no options at all. Two of those options decide
answers rather than performance.

Measured on an adopted collection created with `{locale: "en", strength: 2}`,
with the same three documents present on both tiers:

| | the application's own cluster | through the router |
|---|---|---|
| `countDocuments({name: "alice"})` | 2 | **1** |
| a write the validator rejects | refused | **accepted** |

No error and no warning in either case. A query that has always matched
case-insensitively quietly stops, and a rule the application has relied on for
years quietly stops applying, on the day it is adopted.

So the router creates the fast tier's copy the way the archive has it, before
anything inserts into it: the default collation, the validator and its level and
action. `capped` and `timeseries` are deliberately not carried, because a capped
fast tier would remove its own oldest documents and the feeder would read those
removals as the application deleting them.

The third exists because the first two are both paths that fill the fast tier
*from the archive*, and a tiered collection the application only ever writes to
is filled by neither. Nothing warms it, no read of it falls through, so the
first thing to create it on the fast tier is the application's own insert, and
an insert is not a copy-up. Startup is both the correct moment and the cheapest:
correct, because a unique index built after a duplicate has been accepted is too
late and will not build; cheapest, because this is the collection the fast tier
has never held.

It is not only speed. The router reads the **fast tier's** unique indexes to
know which values it must protect across the tiers, so a bare fast tier means
neither mongod nor the registry is enforcing one, and a constraint the
application has relied on for years quietly stops holding on the day it is
adopted. Measured on an adoption whose archive carried a unique index: a value
the archive already held, and the same new value twice, were all three accepted.
`cline keys --rebuild`, the documented step for exactly this, does not save you
either, because it reads the constraint from the same place and claimed nothing.

What follows is worse than the duplicates. The fast tier then holds documents
the archive's index refuses, so the feeder cannot mirror them: it reopens its
change stream from the same checkpoint once a second, for ever, and mirroring
stops for **every** namespace behind it. An order written after that never
reaches the archive at all.

A TTL index is the one that does not travel. Expiring documents on the fast tier
would have the feeder read those expiries as application deletes and tombstone
the archive's copies, so an index whose job is to remove data would reach into
the tier that exists to keep it. The archive goes on expiring its own copies.

**And that last sentence used to be the whole of the warning.** An adoption
inherits the application's TTL index on what is now the archive, so the tier that
was adopted to keep history goes on deleting it, on a schedule nothing here set.
It is invisible from the fast tier: a read the fast tier answers whole never asks
the archive, and a read that falls through comes back short with no error on it.
So it is now said out loud, in both places an operator looks. The router and the
warmer warn once per namespace, naming the index, the field and the interval, and
`cline indexes` reports it as its own finding:

    shop.orders
      fast tier  4 index(es), archive  5
      the ARCHIVE expires documents of this collection:
        created_ttl                        {createdAt: 1}   (after 720h0m0s)
      that history leaves the tier that exists to keep it, and a read falling
      through is then short by whatever expired, with no error on it

Reported rather than dropped. The expiry may be exactly what the deployment
wants, an index the application created is not ours to remove, and dropping it
does not bring back what has already gone.

**Every index build this system starts commits on a majority**, not on every
voting member, which is mongod's default. The default is what lets one member
stop an index build: it never commits, `createIndexes` never returns, and
everything else on that cluster carries on working. The router builds the fast
tier's indexes before it opens its listener, so what hangs is the start of the
proxy, with nothing in the log to say why. Found on this project's own cluster
after one member came up on the wrong address: a build sat nineteen minutes and
read exactly like a hung proxy.

Measured afterwards on a three member 8.0.26 set with a thousand documents:

| commit quorum | members up | build |
|---|---|---|
| `votingMembers` (default) | 3 of 3 | 164ms |
| `votingMembers` (default) | 2 of 3 | did not return in 90s |
| `majority` | 2 of 3 | 117ms |

One detail decides whether you ever see this: a build on an **empty** collection
skips the two-phase protocol and needs no votes, so the shape that stalls is a
fast tier that already holds documents, which is every fast tier after the first
warm.

A majority is what the rest of this system already waits for, and a member behind
builds the index when it catches up, the same as it applies everything else. The
startup build is bounded as well, at five minutes per namespace, after which the
router says which namespace it gave up on and what is not being enforced, and
goes on starting: an application in front of a proxy that will not start is down.

The warmer deliberately does not have that bound. It builds a namespace's indexes
before it fills it, and a unique index built after the duplicates have been
written will not build at all, so a warmer that gave up and filled anyway would
trade a stalled pass for a constraint that never comes back. A stalled pass is
visible: `thermocline_daemon_seconds_since_pass{daemon="warmer"}` climbs, which
is alert 5.

**The consequence is that a read which falls through runs with whatever indexes
the archive happens to have.** For a filter that is slower. For a sort it can be
worse: past the server's sort memory limit a blocking sort does not degrade, it
fails. Archive reads therefore ask for `allowDiskUse`, so a sorted query that the
fast tier answered from an index becomes slow rather than broken. An operator who
would rather refuse expensive fallback reads than run them has
`router.completeness: strict`, which refuses them all.

`hint` is dropped for the same reason: it names an index the archive is not
expected to have. **Except when the hint is not only about the plan**, and there
are two such cases, both of which used to produce a wrong answer quietly.

A sparse index holds only the documents carrying its fields, and a partial index
only the ones its filter matches, so hinting either narrows which documents are
looked at. Measured on PSMDB 8.0, twenty documents of which six carry a field,
with a sparse index on it: `find({})` returns twenty and
`find({}).hint(thatIndex)` returns six. Dropping that hint on the way to the
archive therefore answered a different question and reported it as success. What
the hint removed is put back as a filter instead: `$exists` on the index's
fields, or the index's own `partialFilterExpression`. Both are predicates the
archive can evaluate whether or not it carries the index, which is what keeps
this from requiring the tiers to agree about indexes.

That translation is only written down where it is exact. "Holds every document
carrying at least one of its fields" is the rule for a sparse index whose key is
all directions, and it is not the rule for one carrying a `2dsphere` or `text`
component: measured with `{loc: "2dsphere", n: 1}` sparse over three documents,
hinting it returned the two carrying `loc` and left out the one with `n` and no
`loc`, which an `$or` over both fields would have put back. For those the hint
is forwarded instead, and MongoDB is the authority: the archive either carries
the index and answers exactly, or names the index it is missing.

`min` and `max` are bounds on an index key, so they mean nothing without the
index that defines it, and MongoDB has required a hint alongside them since 4.2.
Dropping it there did not cost a plan, it turned a working query into
`NoQueryExecutionPlans`. The hint travels with them now, and if the archive does
not carry that index the caller gets MongoDB's own complaint naming it, which is
a true statement about a real divergence and one the report below already
covers.

To see the divergence rather than infer it:

```
$ cline indexes --config cline.yaml
shop.orders
  fast tier  6 index(es), archive  2
  missing from the archive:
    customer_1_createdAt_-1            {customer: 1, createdAt: -1}
  a read that falls through and uses these will scan the archive
```

The rule it prints is the one to follow: keep an index on the archive when a
query that falls through filters or sorts on its fields, and drop it when
nothing does, because that storage is the saving. A unique index is called out
separately, since cross-tier uniqueness is the key registry's job and its
absence on the archive is expected.

### Authorisation

Authentication is proxied rather than terminated, so the fast tier holds the
client's identity and decides what it may read. Almost everything reaches the
fast tier, and a read it refuses comes back as an error rather than a miss, so
nothing falls through.

`count`, `distinct` and aggregations are judged **before** they run, because a
pipeline can end in `$out` and refusing one afterwards would leave a truncated
collection written. That means the fast tier never sees them, so before
answering any of them from the archive the router asks the fast tier, over the
client's own authenticated connection, whether that client may read the
collection. The fast tier's own error is returned unchanged.

Without that check a client with no access to a tiered collection could obtain
its contents with a count, but only while the collection was incomplete, which
is to say only while tiering was doing its job.

The archive is reached with the router's own credentials, because the client has
no session there and cannot be given one. **It should therefore be reachable
only by the router**, which the operating rules already require for a different
reason: the cheap tier is read-only to everyone except the feeder.

**With `router.terminate_auth` on, none of the above applies and this section is
the whole of the protection.** The upstream connections belong to the router, so
the fast tier will do whatever they ask, and the router decides instead. It
denies by default: a command whose required privilege is not in the mapping is
refused by name rather than guessed at, which is why an unusual client meets
refusals rather than a hole.

Judging a command by the collection it was sent to is not enough, because a
command can name more collections than its own. `$lookup`, `$unionWith` and
`$graphLookup` read another one, `$out` and `$merge` write one, an `explain`
carries a whole command inside it, and a `bulkWrite` names its collections in
`nsInfo` and refers to them by position. Every one of those is judged
separately, the way mongod judges it: find on each collection read, insert and
remove on an `$out` target, insert and update on a `$merge` target, and for an
explain exactly what the command inside it would need.

Measured against the same user sending the same command straight to the fast
tier, with a user holding find on one collection and nothing else: before those
namespaces were judged, a `$lookup` returned the other collection's documents,
a `$unionWith` returned its rows, and `$merge` and `$out` **overwrote it**,
leaving `1:-1,2:-1` where mongod had left `1:100,2:200`.

**A write that asks for no acknowledgement is still a write.** An unacknowledged
write, `{w:0}`, carries the wire protocol's moreToCome flag and the client never
waits for a reply. There is nowhere to put a refusal, and that is a reason not
to perform the write rather than a reason to skip asking. It is judged like any
other, and where the answer is no the command is dropped, which is what `{w:0}`
already means to an application: it asked not to be told.

### Upserts

An upsert is the one write shape that cannot be put right on the way back. Every
other write that matches nothing on the fast tier is resolved afterwards, by
copying the archived document up and running the write again. An upsert does not
wait: matching nothing is its cue to create a document there and then, built from
the filter's equalities and the update.

That document carries the archived record's `_id` and almost none of its fields,
and the feeder mirrors it up as an upsert, so **the archived record is
overwritten by it**. `copyUp` uses `$setOnInsert` precisely so a promotion never
clobbers a newer fast tier copy, which means it cannot undo this either.

So an upsert on a tiered collection is judged before it runs. If the fast tier
matches nothing, whatever the archive matches is copied up first, and the upsert
then finds it and updates it, which is what the same statement would have done
with no tiering underneath it.

The cost is one indexed lookup on the fast tier per upsert, and only on a
collection whose watermark does not already claim all of history. A fully warmed
collection has nothing archived that is not also on the fast tier, so the guard
returns immediately and costs nothing.
