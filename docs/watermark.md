# The completeness watermark

Part of [Thermocline](../README.md).

## The completeness watermark

Traffic alone fills the fast tier sparsely, one document at a time. That serves
a lookup by id and is useless for a range query, because holding 12 of a
customer's 40 orders looks exactly like holding all 40.

The warmer fills contiguous ranges instead, walking the archive newest first,
and records how far back it got. `completeFrom` means every live document newer
than that instant is present in the fast tier, so a query bounded inside the
range can be answered there alone.

Two components move it, and only one of them is dangerous:

- The **warmer lowers it** as it fills, extending the guarantee further back.
- The **evictor raises it before removing anything**, because a document removed
  from inside the claimed range makes the claim false.

An understated watermark costs one redundant lookup in the archive. An
overstated one makes range queries return short results and report success. The
evictor therefore raises the watermark *before* it deletes, so an interrupted
sweep still leaves an honest claim.

**No watermark means nothing is guaranteed**, not that everything is present.
That distinction is the whole point: a fast tier built empty has no claim, and
answering range queries from it would return short results and call them
success. Range queries are refused until something records a claim.

**And the claim is the `completeFrom` field, not the row it sits in.** A row in
the watermarks collection carrying nothing but its `_id` used to read as
complete from the zero time, which is the most permissive claim this system can
hold: every range query on that namespace answered from the fast tier alone,
however far back it reached. Measured through `cline watermark` on such a row:
`complete for all of history`. Nothing here writes one, which is why it was
never seen, and that is what made it worth closing: an `$unset`, a hand-edited
row, a backup restored from an older schema, or the next feature to `$set` some
other field on that collection with `upsert: true` would all have made one. A
row without the field now claims nothing. A `completeFrom` that is present but
is not a date is refused rather than guessed at, because "the claim cannot be
read" and "nothing has claimed this" need different words and have different
remedies.

**A rule with no `date_field` reads each document's age from the timestamp
inside its `_id`, and that only exists in an ObjectId.** Against any other `_id`
type the rule does not merely approximate, it addresses nothing: MongoDB
brackets range predicates by BSON type, so `_id < ObjectId(cutoff)` matches no
integer, string or UUID `_id` at all. Sorting uses the canonical type order;
queries do not.

Obeying such a rule is silent in both directions. The evictor sweeps, reports
success and tiers nothing, for ever. The warmer copies nothing, concludes the
window is done and records a completeness claim over a fast tier it never
filled. Measured on an adopted collection of ten integer-keyed documents: the
warmer reported `warm complete, scanned 0` and claimed it complete for all of
history, and the router then answered `countDocuments({})` with **0** where the
original cluster answers 10. The collection had disappeared and every component
reported success.

Both now refuse such a rule at ERROR, naming the namespace and telling you to
set `date_field`. Refusing leaves no claim, so reads fall through to the archive
and the application keeps seeing its data while the configuration is corrected.

**A claim on a boundary cannot answer a query with no lower bound**, and that is
most of what an application asks. "Complete for everything newer than T" covers
a query that says `at >= T`; it cannot cover `find({customer: X})`, because that
reaches arbitrarily far back and no boundary is far enough. Only a claim on all
of history covers it.

**And it covers `at >= T` only when `at` is the field the rule ages by.** A
boundary claim is measured in one dimension, so a bound in another is not a
bound on the same thing. `find({shippedAt: {$gte: yesterday}})` on a collection
aged by `createdAt` reaches every order ever placed that happens to have shipped
recently, and goes to the archive. Measured on a collection claimed complete for
thirty days, holding a recent order on the fast tier and a two year old one in
the archive, both shipped yesterday: before this it answered 1 where the archive
holds 2.

So a collection whose application filters on one date and whose rules age it by
another gets no benefit from a boundary claim at all. Either age it by the field
the application queries, or claim it complete for all of history.

This is the sharpest edge in an adoption, because everything looks finished. The
warm window is set to the age of the data, the warmer copies every document up
and reports success, and the fast tier holds the entire collection. Measured on
a 50,000 order shop in exactly that state:

| | boundary claim | all of history |
|---|---|---|
| throughput | 284/s | **1,339/s** |
| a customer's order history | 62.74 ms | **2.78 ms** |
| count a customer's orders | 58.54 ms | **3.18 ms** |
| read an order by date | 62.50 ms | **2.44 ms** |
| served by the archive | 40.8% | **0%** |

Nothing was wrong with the fast tier in the left hand column. It held all 50,000
orders and answered 59% of the run. The other 41% went to the archive because
the claim could not be shown to cover them.

**Read the right hand column for what it is: the fast tier holding the whole
collection, which is the state before tiering rather than after it.** As soon as
the archive holds anything older than the warm window, which is the entire point
of having two tiers, the warmer declines that claim and leaves a boundary one,
and the first eviction sweep would raise it anyway. Measured on a collection
warmed with a window narrower than its history: the claim came back as a
boundary, and the customer history query went to the archive before and after
eviction alike.

So the shape to expect in a steady state is this. A query bounded in time, a
report over last month, is answered by the fast tier. A query with no lower
bound, give me this customer's orders, falls through, and no amount of warming
changes that while the collection is genuinely tiered. If your hottest queries
are the unbounded kind, tiering will save you storage rather than latency, and
it is worth knowing that before you adopt rather than after.

**So the warmer makes the stronger claim when it can prove it.** When the
archive holds no live document older than the window just warmed, the fast tier
holds every document there is, and the watermark says all of history. When the
archive does hold something older, the boundary claim is the honest one and the
warmer names what it costs and how to change it.

A fast tier that genuinely holds everything, because it is the original cluster
rather than a new empty one, can also say so directly:

```
bin/cline watermark --config cline.yaml                        # show every claim
bin/cline watermark --config cline.yaml --ns app.orders --complete-all
bin/cline watermark --config cline.yaml --ns app.orders --clear
```

**A claim is checked against both tiers before it is recorded**, because it is
otherwise a promise typed by hand: it says the fast tier holds everything, and
recording one it does not have makes the router answer short with no error. So
this counts what each tier holds and refuses a claim the numbers contradict,
naming both. `--force` records it anyway, for the case where the operator knows
something the counts cannot show, such as a warm still running.

**A declaration waits for an eviction sweep in progress.** A sweep raises the
claim to its cutoff, waits for routers to read it, and only then deletes what is
older. A declaration counted and written inside that wait describes documents
the sweep is about to remove, and the counts cannot see it because the deletes
have not happened yet. So declaring takes the evictor's lease for as long as it
counts and writes, waiting up to two minutes for a sweep to finish, and the
evictor skips one sweep if it comes round meanwhile.

Counting is not free on a large collection, so it is bounded, and past the bound
the claim is recorded **unchecked** rather than refused: blocking a rollout on a
count would be the worse failure. `--count-for` sets that bound, a minute by
default. On the collection big enough for the default to run out, raising it is
usually what you want, because "recorded unchecked" is the one outcome nobody
asked for.

A claim also has to be withdrawn when the fast tier loses documents by any route
the evictor did not drive, because the claim then describes data that is no
longer there. Two things handle that:

- **The feeder withdraws it on a `drop`,** since the fast tier just lost the
  whole collection.
- **The warmer checks it against reality.** A restore or a rebuild leaves no
  change stream event at all, so the claim simply outlives its data. Before
  trusting a claim, the warmer asks whether the fast tier holds anything in the
  range; if it holds nothing while the archive does, the claim is discarded and
  the window is warmed again.

A partial loss is caught by neither. Telling "half the range is missing" from
"half the range was legitimately deleted" needs a document by document
comparison, which is a repair tool rather than a warm.

Two more rules the warmer follows:

- **Tombstoned documents are never copied up.** The archive keeps records the
  application deleted; copying one into the fast tier would bring it back.
- **It never overwrites.** Copies land as `$setOnInsert`, because the fast tier
  is authoritative and its copy is always the newer one.

```
bin/cline warmer --config cline.yaml --dry-run   # report, copy nothing
bin/cline warmer --config cline.yaml             # fill, then exit
```
