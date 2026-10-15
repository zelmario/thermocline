# Adopting a cluster you already have

The numbered rollout, and what an adoption carries that is not a document.

Part of [Thermocline](../README.md).

## Adopting a cluster you already have

This is the shape every deployment starts in, and the one most of this system's
defects have come from. **The cluster you have becomes the archive, exactly as
it is. The fast tier is a new, smaller replica set with nothing in it. The
application is pointed at the router.** Nothing is loaded into the fast tier by
hand.

**Upgrade it to the fast tier's MongoDB release first.** Both tiers must run the
same release and every daemon refuses to start otherwise: the reason and the way
out are in [Both tiers run the same MongoDB release](#both-tiers-run-the-same-mongodb-release).
The cluster you are adopting is usually the older side, so this is part of the
adoption rather than something to do afterwards.

The order matters, because most of what goes wrong goes wrong quietly.

**0. Run `cline preflight` first, which writes nothing.** It makes the same
comparison a router makes at startup -- versions and feature compatibility,
collections and databases the fast tier has never held, collation and validator
drift, unique indexes the fast tier is missing, accounts and roles the archive
has and the fast tier does not, and whether anything has claimed the unique keys an adopted archive
carries -- plus the feeder's own check that every namespace it is told to mirror
is a kind that can be mirrored at all, and it also names what a start *would*
build on the fast tier, with the index counts.

That last check was missing here for longer than it should have been, and the
shape of the mistake is worth keeping: preflight ran the router's checks, said
"Nothing above refuses a start", and the feeder then refused to start on a
capped collection named in `feeder.namespaces`. A report that runs one daemon's
checks and speaks for all of them is worse than no report.

That last line is the one to read before step 1, because step 1 is not a reader.
A router start creates each tiered collection on the fast tier the way the
archive has it and builds the archive's indexes on it, unique ones included,
before it opens the listener. That is right for a router about to serve traffic
and it is a surprise on a cluster somebody is still deciding about. Preflight
exits non-zero only when the pair is one a router would refuse outright, so a
rollout script can gate on it.

**1. Point the router at both, and read what it says at startup.** It compares
the two tiers and names, once, everything the fast tier is missing that the
application will need. Nothing here is fatal and all of it is silent afterwards.

**2. Change the connection string, do not just repoint it.** The router answers
`hello` as a mongos, so a driver told `replicaSet=` in its URI is looking for a
replica set member and will never accept one. Measured, with the same router and
the same credentials:

```
mongodb://user:pw@router:27017/?authSource=admin                  ok
mongodb://user:pw@router:27017/?replicaSet=rsOld&authSource=admin MongoServerSelectionError
```

The second one times out rather than saying what is wrong, and the router cannot
help: a driver never says it was looking for a replica set. Drop `replicaSet=`
and give it the router's address.

**3. Create the application's accounts on the fast tier.** It is a new replica
set and has none of them, nor any custom role they refer to; MongoDB refuses to
create a user whose role is missing and names it, so do the roles first. The router authenticates against the fast tier, so
until they exist every connection is refused with MongoDB's ordinary
`Authentication failed`, which says nothing about which tier is missing them.

The list is in the output of step 0: `cline preflight` prints "the archive has
roles the fast tier does not" and "the archive has accounts the fast tier does
not", each naming them. Create each one with the same name, the same roles and
the password the application already uses; MongoDB stores only a hash, so a
password cannot be read back from the archive. If preflight names neither, check
that its account can read `admin.system.users` on both tiers: when it cannot, the
comparison is skipped and nothing is printed, so silence is not "none missing".

Give the router's own account `killAnyCursor` on the fast tier while you are
there. When the archive answers a query the fast tier had started, the fast tier
is left holding a cursor for the rest of it that nothing will ever read, and the
router closes it. It sends that on its own account, and mongod lets an account
close only its own cursors without that privilege. Without it those cursors sit
until mongod's timeout, ten minutes by default. The router says so once if it
cannot.

**4. Deal with the collections nothing tiers.** A collection the archive holds
that is not in `feeder.namespaces` lives only on the archive, and the router
correctly forwards reads of it to the fast tier, which has never held it. The
application sees it empty. Either tier it or copy it whole. Views are named
separately because neither answer works for them: recreate them with
`createView` and tier what they read from.

**5. Check the `_id` type of everything you are tiering, and what its timestamps
mean.** A warm or eviction rule with no `date_field` takes each document's age
from the timestamp inside its `_id`, which only an ObjectId has. Against
integer, string or UUID `_id`s the rule addresses nothing at all, so set
`date_field` for those collections. Both components now refuse such a rule
rather than obeying it, and say so.

The quieter half is a collection whose `_id`s *are* ObjectIds carrying the wrong
date. An ObjectId's timestamp is when the document was created, which is the
same as its business date only when the collection accumulated over time. A
cluster populated by an application level migration has every document
re-inserted, so every `_id` carries the migration's date. Measured on documents
dated a year ago and loaded a moment before: the evictor with `older_than: 40m`
scanned zero and evicted nothing, and would go on doing that until the
migration itself was older than the window. Nothing is wrong from where it
stands: the rule addresses documents and tiers by the wrong clock.

And set `date_field` to the *same* field on both rules for a namespace. Ageing
one collection two ways costs more than the two daemons picking different
documents. The router will not read a lower time bound out of a query on such a
namespace, because it cannot tell which of the two a completeness claim is
measured in, so every bounded query on it is answered by the archive. One word
of configuration turns the whole thing off for that collection, and neither file
shows it on its own, since the daemons are usually given separate ones. Each
daemon names the namespace at startup.

`mongorestore` keeps the original `_id`s and is fine. Anything that re-inserted
the documents is not. Compare the two before you trust a rule that has no
`date_field`:

```
const c = db.getSiblingDB("app").orders;
const o = c.find().sort({_id: 1}).limit(1).next();
print(o._id.getTimestamp() + "   <- what the rule reads");
print(o.at + "   <- what the application means");
```

**6. Warm, then look at the claim it left.** A boundary claim cannot answer a
query with no lower bound, which is most of what an application asks. When the
warm covers everything the archive holds, the warmer says so and the fast tier
answers everything; when it does not, the warmer names what that costs.

**7. Run `cline keys --rebuild` before you let the evictor run.** The registry
only protects values it knows about, and a cluster adopted with documents
already in it starts with none of them claimed. It reads the constraint from the
fast tier's unique indexes, which the router builds there at startup, so run it
after the router has been up once.

This step is about the indexes the adoption brought with it, which no router was
running when they were created. A unique index created *later*, through a
router, claims the existing documents' keys itself.

Run it again after upgrading past 0.1.0-dev if any of your unique keys are
numbers. A key is derived from the value, and whole numbers are now derived the
way mongod compares them, so int, long, double and decimal 7 arrive at one key
instead of four. Claims made by an older build carry the old id, and an old id
matches nothing, so the values they hold are not enforced until the rebuild
reclaims them.

**8. Start change stream consumers fresh, do not resume them.** A resume token
minted on the old cluster is accepted by the router and delivers nothing.
Measured with one token and one collection: against the cluster it came from it
delivers the write that followed it; through the router after the cutover it
delivers nothing at all and reports success. The router serves change streams
from the fast tier, which is a different replica set with its own oplog, and a
token from another one is not a position in it. There is no error to catch, so
this has to be a step rather than a surprise.

**9. Fallback reads land on the archive's primary, and you cannot move them.**
During the adoption most reads fall through, and the archive is the production
cluster you are relieving, so sending them to its secondaries is the obvious
thing to want. `readPreference` on `cold.uri` does not do it: archive reads are
issued as commands, and a driver runs a command against the primary whatever the
client's preference says. Measured with `secondaryPreferred`, the fallback read
was served by the primary anyway.

That stays as it is on purpose. The freshness barrier waits for mirroring to
reach the archive's primary, and a secondary is behind that by a lag nothing
here measures, so a read served there could miss a write that has already been
acknowledged. The router now says the setting is ignored rather than letting you
believe it took effect. Plan the archive's primary for the read load, and warm
the fast tier to shorten the period it lasts.

**10. Expect eviction to hold off at first.** The evictor will not remove
anything until the fast tier's oplog window exceeds `min_oplog_window`, and a
new replica set's window is its own age, so with the shipped default of 24h
nothing is evicted on the first day. That is the guard working, and it says so:
`eviction paused, reason="oplog window is 3h20m, limit 24h"`. Lower the limit
only if you understand what it protects.

An eviction sweep that does run before the warm is no longer a problem either,
and it was one. The evictor narrows what the fast tier claims to hold before it
removes anything, which is right; on an adoption there was nothing to narrow,
and it made a claim instead, saying the fast tier held everything since the
eviction boundary about a tier holding nothing. A sweep that scanned zero
documents and evicted zero was enough. Measured on 4,000 archived orders with
the fast tier empty: the router answered a thirty day range with 240 before the
sweep, the archive's own answer, and a router started after it answered 0 and
reported success. The already-running router kept the claim it had cached, so
it took a restart to appear.

A sweep now narrows a claim and never makes one. The claim comes from whatever
put the documents there: the warmer, which knows how far back it filled, or you,
with `cline watermark --complete-all`.

**11. Size the fast tier for the key registry as well as the documents.** Every
unique value gets a claim in `_thermocline.keys`, on the fast tier, and claims
are permanent by design: one has to outlive the document it protects. After
`cline keys --rebuild` on a cluster adopted with data already in it, that is one
claim for every document the collection has, multiplied by the number of unique
indexes on it.

Measured on 20,000 documents with one unique index: 147 bytes per claim
uncompressed, 56 on disk, and a registry of 1.1 MB beside a collection of
0.3 MB. The ratio is a function of how big your documents are and means little;
the per claim figure is the one to plan with. Fifty million documents with one
unique index is on the order of three gigabytes of registry, on the tier you
sized for a hot window.

**12. Know how to go back before you need to.** The feeder mirrors every write
into the archive, so the archive stays current and rolling back is repointing
the application at it. One thing has to happen first: a delete is mirrored as a
mark and not as a removal, so every document deleted since the cutover is still
physically there carrying `_tcDeletedAt`. The router filters those out of every
answer and a client reading the cluster directly does not.

Measured: three documents in the collection, one of them deleted through the
router, and the original cluster answering `countDocuments({})` with 3 where the
router answers 2.

`cline rollback` counts them, across every tiered namespace, and clears them
when you ask:

```
cline rollback --config /etc/cline.yaml
  app.orders                               2 marked deleted

cline rollback --config /etc/cline.yaml --clear
  app.orders                               cleared 2
```

**Stop the application and the feeder first.** A delete that reaches the archive
after the clear writes another mark, so clearing while anything is still writing
leaves you where you started, and the command refuses while a feeder is
publishing rather than letting you find that out afterwards. `--force` overrides
it if you know nothing is writing.

**And the marks are rarely the biggest thing in the way.** The archive is the
complete copy of what is *tiered*. Every collection `feeder.namespaces` does not
name has lived on the fast tier alone since the cutover, because step 4 of the
rollout copies those up and nothing ever copies them back. Repointing without
them hands an application its orders and not its catalogue.

Measured by rehearsing the whole procedure rather than the command, which is
what `test/testrollback.sh` now does: a shop with 3,081 orders in the archive,
40 of them cancelled through the router, rolled back correctly to the 3,041 the
application had been shown, and then it would not start. The fast tier held
`customers`, `products` and `store_marker`; the archive had never heard of them.
`cline rollback` reported the accounts and finished with "the archive already
answers the way the router does".

It names them now, grouped by database, with the `mongodump` and `mongorestore`
to copy them across, and it no longer calls the archive ready while anything is
listed above. Rehearsed end to end after that: the same shop runs against the
archive with no errors and none of its five invariants broken.

**The third thing in the way is the indexes, and one of them is a constraint.**
Nothing copies an index to the archive, on purpose: an index describes the whole
collection, and carrying every one of them on the cheap tier gives back most of
what tiering buys. That reasoning holds while the router is in front of the
archive. This procedure takes the router away, and then those are the
application's own indexes and they are gone.

A plain index costs speed, and the archive is the tier with the most documents
in it. A **unique** index is not a speed matter. Uniqueness across the tiers is
enforced by the key registry, which lives on the **fast tier** that this
procedure switches off, so after the cutover the constraint the application
declared is enforced by nothing at all and the next duplicate is accepted by a
cluster that looks healthy.

Measured on 2026-09-24, the rehearsal repointed at an archive holding `_id_` and
nothing else: three duplicate order references accepted in sixty rounds, on a
collection whose fast tier copy carries `{ref: 1}` unique with a case-insensitive
collation.

`cline rollback` lists them now, constraints separately from the rest, with the
`createIndex` calls to build them and the collation and partial filter carried
along. Build the unique ones **before any application writes**. A build that
fails is naming two documents that already share a value, which is a duplicate
the constraint was supposed to prevent and did not, because the registry
protects the values it knows about and an adoption's history predates it: settle
those documents before the cutover rather than after.

A TTL index is listed and deliberately not turned into a command. The archive
holds history the fast tier stopped holding, and expiring it on the schedule the
application set for a collection that was only ever recent is a decision about
retention, not a step in a rollback.

**13. Run more than one router by naming them all, not by hiding them.** A read
the fast tier cannot answer is served from a cursor the router owns in its own
memory, holding an open connection to the archive, so it cannot be shared with
another process. Drivers already handle this: they pin a cursor to the server
that created it, exactly as they do for a mongos. Give the connection string
both addresses and it works.

```
mongodb://user:pw@router1:27017,router2:27017/?authSource=admin      iterated 20 of 20
```

Putting them behind a single load balanced address breaks that, because the
driver then believes there is one server and its pool sends the getMore down
whichever connection is free. Measured, the same cursor asked of the second
router: `cursor id 8792354910962146833 not found`. It fails only for reads that
fall through and return more than one batch, which is most of them during an
adoption, and only sometimes, which is worse.

The caches do not need naming: each router keeps its own, and a write it did not
make reaches it through the change stream it watches on the fast tier. That is
asynchronous, so it is a race by nature, and it was measured rather than
assumed: with one router answering from its cache, nine hits deep, and the other
taking a write before each read, twenty rounds in a row gave the new answer and
none gave the old one.

One cache is not that one, and it used to be the sharp edge here. Each router
keeps its own listing of which unique indexes a collection has, for a minute,
and no change stream carries index DDL. So a unique index created on a running
system was enforced across tiers immediately by the router the command went
through and only within a minute by the others, and in that window another
router did not claim keys for it: it let through a duplicate of an archived
document that the fast tier's own index cannot see, because the document is not
there. Measured, with a second router that had written to the collection before
the index existed: the duplicate was accepted and landed.

**That window is now the claim's five seconds rather than the listing's minute.**
The router that takes the DDL increments a count on the completeness claim, and
every other router reads that claim on the hot path anyway, once per five
seconds per namespace, so it costs no round trip. Seeing the number move drops
the listing, and the listing is re-read before it is enforced from rather than
after: the two happen concurrently otherwise, and the insert that should have
been refused was accepted while only the next one was judged.

It is a narrowing and not a closure. Five seconds is still a window, the bump is
best effort because failing a client's DDL over a bookkeeping write would be
worse, and `cline keys --rebuild` is still what the rollout list asks for.

**14. The archive is a hard dependency, and not only for its documents.** The
completeness watermarks, the feeder's checkpoint and its published position all
live on the archive, so a router that cannot reach it does not know what the
fast tier is allowed to answer or how far mirroring has got. It therefore
refuses to start rather than guess: `error: ping cold tier: server selection
error`. A router restart during an archive outage leaves the application with
nothing, including the reads the fast tier could have served alone. Plan
restarts accordingly, and remember that on day one almost every read needs the
archive anyway.

**That one stays as it is, and it is now a decision rather than an open
question.** Asked and answered on 2026-09-05: the router goes on refusing, and
this is the runbook entry for it. Do not restart a router during an archive
outage, and bring the archive back first. A router that has never read a claim
knows which namespaces are tiered and nothing about what the fast tier holds of
them, so anything it served would be a guess dressed as an answer. Starting and
refusing every tiered namespace by name was considered and is not obviously
better than not starting: the application gets errors either way, and a process
that is up is a process monitoring reports as healthy.

**A router that is already running is a different case, and it has a knob.**
About five seconds into an archive outage it stops answering tiered reads too,
even the ones the fast tier owns, because the claim it caches expires and cannot
be re-read.

**What that cost was measured to be, and it was worse than "stops answering".**
One claim read against an address nothing answers on takes **30.002 seconds**,
the driver's default server selection timeout, which nothing here shortens. Each
tiered read paid it in full and was then refused, so an archive outage was not a
router refusing reads promptly, it was a router hanging for half a minute per
read and then refusing. Any client with a socket timeout gave up first and saw a
broken proxy. A failed claim read is now remembered for the same five seconds the
claim itself is cached, so one read per namespace per five seconds pays the
timeout and the rest are answered from what is held. That applies whether or not
a grace is configured: with none, the refusals simply arrive promptly.

`router.claim_grace` says how much longer a claim already read may be believed.
It is off by default and the trade is exact:

- **Safe against an archive outage.** Narrowing a claim and evicting what it
  stops covering are both writes to the archive, so while nobody can reach it
  neither can happen and the last claim read is still true.
- **Not safe against a partition that isolates only the router.** The archive is
  up, the evictor is working, claims are being narrowed and documents removed,
  and this router answers from a claim covering documents the fast tier no
  longer holds. Those reads are short with no error on them, for as long as the
  grace lasts.
- **Writes are never covered by it.** An insert skips the archive's own
  duplicate check when the fast tier claims all of history, so a claim that has
  become false would let through an `_id` the archive still holds and the feeder
  would mirror the new document over the archived one. Measured, while the grace
  still applied everywhere: an insert the same router refused without a grace
  was accepted and landed. Every write path refuses while the archive is
  unreachable, whatever the grace is set to. The trade on offer is a short read
  against a refused read, and never a destroyed document.
- **It invents nothing.** A namespace this router was never asked about has no
  claim to hold over and is refused, and so is every namespace if the outage
  started before the router was asked anything at all.

`thermocline_claim_grace_reads_total` counts the reads it allowed. Alert on it:
the counter cannot tell a correct answer from a short one, and you can.

An archive *failover* is a different matter and was measured rather than
assumed. With its primary stepped down under load, and both kinds of read
running throughout, nothing failed: 19 reads the fast tier owned and 13 that
fell through, all answered correctly while the set elected a new primary. The
driver's own retry absorbs it.

**The fast tier's failover costs a handful of operations, and it used to cost
everything.** This is the one an adoption is most likely to meet, because the
fast tier is the new replica set and the one being rolled and resized. Measured
three times with the shop running through the router, all four daemons working
underneath it and the warm still filling, and the fast tier's primary stepped
down about forty seconds in: 25,600 operations a run, an election decided in
about a second, the router pointing at the new primary 1.1 to 1.5 seconds later,
2 to 8 failed operations, and none of the five invariants broken. What the
client sees is a few refusals and a dropped connection, which its driver retries.
The control says what to compare that against: the same shop and the same
stepdown against plain MongoDB, with no router in the way, cost no errors at
all. Before this was fixed the same test never recovered; see the proxied
authentication bullet under "What it does not do yet".

**14a. Keep `evictor.max_feeder_lag` well under an hour, and keep the clocks
together.** The evictor announces a removal before it makes it, and the feeder
asks whether a delete was announced. That announcement is kept for an hour and
then expires. A delete the feeder only reaches after the announcement has gone
reads as the application's own, the archive tombstones its copy, and the
document is gone from both tiers with nothing said. So `max_feeder_lag` and that
hour are one setting in two places: the config refuses a lag that reaches it.

That relationship used to depend on the two machines agreeing about the time.
The announcement was stamped by the evictor and is expired by the archive's TTL
monitor, so an evictor whose clock ran behind the archive's had its
announcements expiring early, and far enough behind they were gone before they
were read. The archive stamps them now, as they are written, so the clock that
starts the hour is the clock that ends it. Every daemon still checks its clock
against the archive at startup and says what it finds, and `cline preflight`
reports it without starting anything.

Two more things are decided by comparing instants read on different machines,
and unlike the announcements they are corrected rather than only reported: the
freshness barrier, which weighs the feeder's published position against the
instant a read began, and `thermocline_mirroring_lag_seconds`, which is the
number the alert rules watch and the one eviction is gated on. The feeder
publishes how far its own clock is from the archive's, every reader measures its
own, and the two instants are subtracted in the archive's frame. The correction
is measured over a round trip on a one-minute cadence, so it is a good answer
rather than a perfect one: run NTP on every host that runs a cline daemon and
none of this has to work hard.

**15. Expect the warm to cost something and the evictor not to.** With a shop
running against the router while all four daemons worked underneath it, 38,248
operations completed with no errors and no violated invariants, so the pieces do
not fight. What they cost is not equal, and the difference is worth knowing when
choosing a window to start in.

The evictor is free within the noise of this machine, measured by alternating
arms rather than trusted from one run:

| | throughput | order history p50 |
|---|---|---|
| evictor sweeping | 177/s | 83.21 ms |
| evictor paused | 181/s | 81.70 ms |
| evictor sweeping | 159/s | 86.94 ms |

The initial warm is the part that competes: the same shop ran at 84/s while it
was filling and around 170/s afterwards. It is bounded by `warmer.rate`, which
is the knob to turn if the application feels it, and it happens once.

Sizing the window too small costs latency. The same shop was run against a two
minute hot window with twenty second residency and a sweep every ten seconds, so
eviction and copy-up fought over the same documents throughout: 26,881 documents
evicted across twelve sweeps, 306 copied back up, the fast tier stripped to 975
orders of the 53,000 there are. No errors, and none of the five invariants
broken. What you get for a window that small is a fast tier holding nothing much
and an application paying archive latency for most of its reads.

**That run was also reported here as costing latency and not correctness, and
that was wrong.** The same fight, watched a different way, was losing data. A
copy-up deleted the eviction announcement the feeder had not used yet, so the
feeder read a real eviction as an application delete and tombstoned a record
nothing had deleted; the copy-up's own insert did not repair it, because the
feeder suppresses that as an echo. Measured on the sample datasets with a warm
and an evictor over the same collections: 66 batches of "deletes not announced
by the evictor" in ninety seconds, and an archive left holding 200 tombstoned
trips and 45 tombstoned sales that nothing had ever deleted. The router then
hid them from every answer, and the fast tier had already dropped them.

It is fixed, and the announcement is now a count that only the feeder spends.
The reason it was missed for so long is worth more than the bug: the earlier run
watched throughput and the shop's five invariants, and both stayed clean,
because a document quietly removed from a collection breaks no invariant about
the documents that remain. What caught it was comparing every answer against the
archive, which is what `test/pilot` does.

**15b. Tiering another collection later needs `cline backfill`.** Adding a
namespace to `feeder.namespaces` tells the feeder to mirror what happens next.
What is already on the fast tier it never sees, and a tiered namespace with no
completeness claim is read from the archive, so those documents stop being
returned. Measured on twenty: the fast tier held twenty one, the archive held
the one written after the edit, and a client read the one.

Add it, start the feeder, then `cline backfill --ns db.coll --apply`. That order
has no window in it: the feeder carries the new writes while the copy brings the
old ones. The router also names the collection at its next start, under "the
fast tier holds tiered collections the archive does not".

**16. Check the tiers agree, with `cline indexes`.** It reports both directions:
what the archive cannot answer efficiently, and what the fast tier is scanning
for.

You are told about the ones your application makes, without having to remember
to run it. A `createIndexes` through the router on a tiered namespace that
leaves the archive without an index over the same fields is one WARN naming the
index, because the divergence is made by an ordinary application command and
felt much later by a read that falls through. A text or geospatial index gets a
different line, because those two the archive needs in order to answer at all
rather than to answer quickly.

That line is said once, to whoever reads the log that minute, and only for an
index made through the router. So every router start, and `cline preflight`,
also names each tiered collection whose fast tier has an index the archive
cannot serve, however it got there:

    WARN the archive has no index for some of the fast tier's indexes on a tiered collection
         ns=shopstore.orders indexes=[at_-1 customer_1_at_-1 customer_1_status_1]

Unique and TTL indexes are left out of it. A unique one has its own warning,
below, and a TTL one must never be built on the archive. Leaving an index off
the archive is a fair choice for a query that never reaches past the fast tier,
so the line asks you to decide, not to build everything. Missing it is not
small, though. On the staging pair on 2026-10-12, a shop with only `_id` and
`ref` indexed on the archive slowed from 269 to 172 operations a second in
twenty minutes, with the archive's primary at load 5.6 on two cores. The next
five minutes after building the four indexes it lacked ran at 404.

**Two of those divergences cost more than a slow read, and both were measured
on 2026-09-21.**

A **unique** index the archive lacks costs writes. Uniqueness is enforced across
both tiers, so a write has to establish that no live document holds its key, and
when the fast tier holds no such document the archive is asked. With no index
there that question is a collection scan: 38-40ms per write at 200,003 archived
documents, against 1-10ms with one. It is reached by the writes that leave a
claim with no owner, which are the ordinary ones, an upsert whose filter carries
no `_id` and an insert that lets mongod name the document. Build it on the
archive for any collection written that way.

A **warm rule that ages by a `date_field`** needs `{field: -1, _id: -1}` on the
archive, and the obvious index is not it. The warm reads newest first, so
without a matching index mongod answers every batch by reading the whole
collection and sorting it, and a pass then costs the square of the collection
rather than the size of the window: 200,000 documents examined per batch at
200,003 archived. An index on the field alone still sorts, at 19,039 per batch.
With the compound one there is no sort stage and 100 are examined. A rule with
no `date_field` ages by the ObjectId and sorts on `_id`, which is always
indexed, so nothing is needed.

Both are named by the router: the first at every start and in `cline preflight`,
the second by the warmer once per rule per pass.

What the router does for you, at startup, for every namespace it manages that
the archive already holds: it creates the collection on the fast tier the way
the archive has it, carrying the default collation and the validator, and builds
the archive's indexes on it. Those are the things that are not documents, and
filling a tier with documents does not bring them. A collation cannot be added
to a collection afterwards at all, which is why this happens before the first
client connects.
