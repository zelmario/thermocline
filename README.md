# Thermocline

Storage tiering for MongoDB. Run a small fast cluster holding what people
actually use, and a big cheap cluster holding everything, behind one connection
string.

**Status: early, but all four components work. Not production tested.**

## The problem

Databases grow, but access does not grow with them. Most systems read a small
fraction of their data regularly and keep the rest for compliance, history, or
the occasional support question. You pay for all of it on every query.

The cause is indexes. Index trees describe the whole collection, and entries for
busy and idle documents sit interleaved on the same pages. There is no such
thing as an idle index page, so every lookup drags data nobody wants through
memory. That is why adding RAM stops helping past a certain size and never
starts helping again.

When a document moves to the cheap tier, its index entries go with it.

## Components

One binary, `cline`, with a subcommand per component.

| Component | Status | What it does |
|---|---|---|
| `feeder`  | **works** | Tails the fast tier's change stream and mirrors writes into the archive |
| `evictor` | **works** | Removes documents the archive already holds, throttled on cluster health |
| `warmer`  | **works** | Fills contiguous ranges in the fast tier and advances the completeness watermark |
| `router`  | **works** | Speaks the wire protocol, answers from the right tier, copies documents up on demand |
| `watermark` | **works** | Shows, declares and withdraws what the fast tier claims to hold |
| `keys`    | **works** | Claims the unique keys that have to survive eviction |

## The rule everything rests on

**The feeder never deletes and never drops.** Inserts, updates and replaces are
mirrored as upserts. Nothing destructive crosses into the archive.

This is required so that removing a document from the fast tier does not remove
it from the archive, which is the entire point. It has a second effect worth
saying out loud: an accident on the fast tier cannot propagate. Run
`deleteMany({})` and then `drop()` on the fast tier, and the archive still holds
every document.

That is covered by a test you can run yourself, below.

## Telling an eviction from a deletion

Both look identical in a change stream: a document disappeared. They mean
opposite things. An eviction is a document moving down a tier, and the archive
must keep it. An application delete means somebody asked for the data to be
removed, and the archive must stop returning it.

The evictor announces every document it is about to remove by writing to
`_thermocline.evicting` on the cold tier **before** deleting from the fast tier.
The feeder resolves each batch of deletes against those announcements in one
round trip.

An unannounced delete is treated as an application delete, and the archived copy
is marked with `_tcDeletedAt` rather than removed. Set
`feeder.unannounced_deletes` to `ignore` if you run bulk deletes outside the
evictor, or to `halt` if you would rather mirroring stopped than guessed.

## What the evictor will not do

Three things stand between an old document and removal. All three are enforced
in code, not documented as guidance.

1. **Nothing is removed unless the archive is confirmed to hold that exact
   document.** The evictor queries the cold tier for the batch it is about to
   delete and drops anything that comes back missing. An empty or unreachable
   archive can never look like permission to delete.
2. **Nothing is removed while mirroring is behind.** Lag is read from the
   feeder's own checkpoint. This check fails closed: if the lag cannot be read
   at all, eviction stops, because removing a document the archive has not
   received destroys it.
3. **A residency hold outranks age.** A document read or promoted recently
   stays, however old it is. That veto is what makes the fast tier a working
   set rather than a date window.

The cluster health signals, replication lag, cache dirty ratio, write tickets
and oplog window, fail open instead. A metric that moves in a future MongoDB
release should not stop eviction forever.

```
bin/cline evictor --config cline.yaml --once --dry-run   # report, change nothing
bin/cline evictor --config cline.yaml --once             # one sweep, then exit
bin/cline evictor --config cline.yaml                    # sweep every 5 minutes
```

A dry run never blocks on cluster health. It changes nothing, so it reports what
would happen and warns separately if real eviction is currently paused.

## The router

It presents itself as a **mongos**, and that is the detail the whole proxy turns
on. A driver uses the `hello` reply for topology discovery: forward the fast
tier's real reply and the driver reads the `hosts` array, connects straight to
those replica set members, and never sends the router another byte. Everything
looks healthy until an archived document comes back missing.

Almost every command is forwarded untouched. The router acts on two cases:

**A read that found nothing.** If the filter is an exact `_id` and the archive
has the document, the router answers with it and copies it up behind the reply.
The caller waits about 30 ms once; every read after that is a normal fast tier
read.

**A write that matched nothing.** This is the dangerous one. An update matching
no document is reported to the driver as success, so an application updating an
archived record is told the write happened when nothing did. Here the copy-up is
synchronous: fetch, insert, then let the original command run again against a
fast tier that now holds it.

```
$ cline router --config cline.yaml
```

Range queries are governed by the watermark below.

### What it does not do yet

- **Aggregations are completeness-checked but not miss-checked.** A pipeline is
  never resolved against the archive document by document; it is either safe to
  run on the fast tier or refused. See below.
- **`getMore` on a fast tier cursor is passed through.** Cursors the router
  creates for archive reads are served here instead, and tracked per connection.
- **Bulk writes are only miss-resolved one statement at a time.** A batch that
  matched nothing is left as the fast tier answered it. Unique keys and
  multi-document writes are handled across the whole batch.
- **Authentication is proxied by default, and can be terminated.** Everything in
  the next two bullets describes the default. Setting `router.terminate_auth`
  changes the architecture: the router verifies the client's password itself,
  from the salt and stored keys mongod keeps in `admin.system.users`, loads that
  user's privileges with `usersInfo`, and reaches both tiers on connections it
  owns. That lifts all three limitations below at once, and it moves the
  decision about who may read what from the fast tier into this process, where
  Authorisation is then the only thing between a client and somebody else's
  data. **It is off by default and should stay off until it has run somewhere
  real.**

  Proxied, which is the default: each client gets its own
  connection to the fast tier, so SCRAM works, at the cost of one upstream
  connection per client. It also sets what happens during a fast tier election:
  the router never sees the credentials, so it cannot re-authenticate a fresh
  upstream and replay a command that was in flight. Writes issued while the fast
  tier has no primary fail and the client has to retry, where a driver talking
  to the replica set directly would block in server selection instead. Measured
  during a real election: six writes a second apart were refused, every
  acknowledged write was present, none was applied twice, and a retry finished
  the job. A client reconnecting mid-election now waits up to 20 seconds for a
  new primary rather than failing outright.
- **Read preference is not honoured. Every read is served by the primary.** This
  follows from proxied authentication and is the one limitation to weigh before a
  pilot. The router answers `hello` as a mongos, and a mongos routes by read
  preference, so drivers attach `$readPreference` to reads and expect it to be
  acted on. It cannot be: the client's SASL exchange is forwarded to one upstream
  connection, and that connection is the only thing carrying the client's
  identity. A second connection to a secondary would be authenticated as nobody,
  SCRAM is nonce-bound and one-shot so the exchange cannot be replayed there, and
  the router never sees a password to redo it with. Using the router's own
  credentials instead is precisely the privilege bypass that Authorisation below
  exists to close.

  Nothing returns a wrong answer: a read served by the primary is strictly
  fresher than one asked of a secondary. What changes is where the load lands. An
  application that pins reporting to secondaries moves that entire read load onto
  the fast tier's primary the day it goes behind this router. Measured on a
  three-member fast tier with the profiler recording on every node:
  `secondary`, `secondaryPreferred` and `nearest` each landed on the primary,
  while the same query sent straight to the replica set landed on a secondary.

  Each connection logs a warning the first time it asks for a non-primary read,
  and `thermocline_primary_only_reads_total` counts them. With
  `router.terminate_auth` on, read preference is honoured: measured with the
  profiler recording on all three fast tier members, `secondary`,
  `secondaryPreferred` and `nearest` each landed on a secondary.
- **TLS is handled, in both directions.** `router.tls.cert` and `key` make the
  listener speak it, `ca` checks client certificates, and
  `require_client_cert` insists on one. Whether the fast tier needs TLS is read
  from `hot.uri`, where it is already written for the driver, rather than
  configured a second place for the two to drift apart: the raw connection this
  proxies bytes over is wrapped the same way the driver wraps its own. The
  startup line says which halves are encrypted, because an operator who has just
  moved an application onto this address needs to know it still speaks what the
  application expects.

### Reads the fast tier cannot answer

The rule is that no application changes. A query that returns the wrong number
breaks that, and so does a query that returns an error it never used to return.
Refusing a `countDocuments` is safe and still a broken application.

So a read the fast tier cannot answer in full **runs against the archive
instead**. That works because the archive is not a partial complement of the
fast tier, it is a complete MongoDB holding every document, so there is no
merging of two half-answers: the query runs whole, on one tier or the other.
Tombstoned records are filtered out on the way, so documents the application
deleted do not come back.

This covers the cases that used to need a decision before adoption: unbounded
`find`, `countDocuments` and other aggregations, `$lookup` into a tiered
collection, and skip-based pagination. All of them now return the same answer
they would from a single cluster.

**The one difference a caller can observe is freshness**, and the router closes
it rather than documenting it away. The archive trails the fast tier by the
feeder's mirroring lag, so before answering from the archive the router waits
for mirroring to reach the instant the read began. Every write that completed
before then is in the archive by the time it is read, which is why a fallback
read cannot miss a write that already returned or hand back a document the
caller has already deleted.

That wait is only as precise as the position it waits on, and this is where it
went wrong. It waited on the durable checkpoint, which moves once per
`feeder.checkpoint_every`, so a fallback read paid a whole checkpoint interval
even with the feeder perfectly caught up and nothing to wait for. Measured on an
idle database: **1,142 ms with `checkpoint_every: 1s`, 4,531 ms at the shipped
default of 5s, against 1.8 ms for the same query with the wait turned off.** At
the default it was also worse than slow: the wait exceeded `archive_wait`, which
also defaults to 5s, so reads gave up and answered stale anyway.

Freshness now has its own record. The feeder publishes how far it has mirrored
every `feeder.position_every` (250 ms by default) as one small document with no
resume token at `w:1`, and the durable checkpoint goes back to meaning only what
it says. The same read now takes **520 ms**, and the remaining cost is honest:
on an idle database the feeder cannot know that time has passed until its change
stream poll times out, so budget a fallback read at roughly
`max_await + position_every` and lower both if such reads are on a user's path.

**What the wait compares was the other half of getting this right, and it was
wrong for longer.** "How far mirroring has got" was the feeder's own wall clock,
and "the instant the read began" is the router's, so on separate hosts the
barrier was only as sound as the skew between two machines nothing keeps in
step. The dangerous direction is silent: a feeder whose clock runs a little
ahead publishes a position past writes it has not mirrored, and a read that
trusts it misses a write that had already returned to the client. Worse, the
feeder stamped that clock whenever a change stream poll came back empty, which
proves the server had nothing more to send when it answered and not that a write
committed a millisecond earlier had been delivered. The shop test caught the
consequence once in 358 order placements: placing an order moved that customer's
count from 18 to 18.

Both halves now use the replica set's own logical clock, which neither daemon
has an opinion about. The feeder publishes the cluster time it has mirrored
through, taken from an applied event or, when the poll is empty, from that
poll's post-batch resume token, which is MongoDB saying "nothing exists up to
here" rather than anyone's guess. The router keeps the newest `operationTime`
the fast tier has reported, which every reply carries and which is therefore
free, and any write already acknowledged to a client happened at or before it.
A fallback read has to clear both bars, never either, so requiring the cluster
time can only make the barrier stricter, and it falls back to the clock alone
against a feeder from before this.

**A caller's own deadline bounds all of this, and it is spent once.** The wait
takes at most half of `maxTimeMS` and the archive read is given what is left,
rather than the whole of it a second time. Measured before that: a client asking
for 800 ms with mirroring behind waited 300 ms at the barrier, handed the
archive 800 ms of its own, and got a successful answer after 1,117 ms. Splitting
it is what stops either half starving the other, because waiting out the entire
deadline honours it and then reaches the archive with nothing to spend, which
would turn a feeder that is merely behind into failed queries. That is the one
thing this barrier exists not to do.

A read of something written milliseconds ago is therefore seen, at the cost of
that wait. A stopped feeder is the one case the barrier cannot hold, and there
the router answers anyway and warns rather than turning a dead background daemon
into failed queries. It notices quickly: the position record carries the cadence
it promised, so four missed publications mark the publisher as gone instead of
making every read wait out the full `archive_wait`.

`router.completeness` chooses the behaviour: `archive` (the default), `strict`
to refuse instead, `permissive` to return the fast tier's short answer.

**An error the archive returns is MongoDB's answer, and it is passed back
unchanged.** It used to be replaced by a generic `CommandFailed` carrying a
sentence about completeness, which is the wrong answer twice over: an
application branches on `e.code` and not on the prose, and a client whose own
query was malformed was told this proxy had refused it. `count`, `distinct` and
aggregations are judged before they run, so those are the paths where it showed:
an unknown pipeline stage came back as 125 where MongoDB says 40324, and a bad
filter as 125 where MongoDB says 2. The codes now match, which `testconform.sh`
compares against an untiered reference for twenty error shapes.

Two kinds of failure keep the refusal, because they are not MongoDB's answer to
the client's command: a failure to reach the archive at all, and an
authorization error, which describes the router's own credentials rather than
the caller's and would otherwise send whoever investigates to the wrong cluster.

### Aggregations

Every driver's `countDocuments` is an `aggregate`, so leaving pipelines
unguarded left the most common count in the system free to return a wrong
number. Against a partly filled fast tier one returned 1 where the answer was
45,904, and reported success.

Pipelines are now judged **before they run**, unlike `find`, which is inspected
on the way back. A pipeline can end in `$out` or `$merge`, and refusing after
the fact would leave a truncated collection already written.

Two rules do the work, and both exist because a pipeline is not a filter:

- **Only a leading `$match` bounds the scan.** A `$match` further down filters
  what earlier stages already read, so it says nothing about how far back the
  scan reached. A pipeline with no leading `$match` scans the whole collection
  and is refused unless the fast tier holds all of it.
- **A joined collection has to be whole.** `$lookup`, `$graphLookup` and
  `$unionWith` read by key, not by time, so no partial watermark covers them.
  Joining a tiered collection that the fast tier holds only part of is refused,
  because the alternative is a join that silently drops rows.

Collections Thermocline does not tier are exempt. They live only on the fast
tier, so they are whole by definition, and demanding a claim for them would
refuse perfectly good queries. That is why the router reads `feeder.namespaces`.

`$out` and `$merge` are refused even under `completeness: permissive`. Returning
a short answer is a wrong number on somebody's screen; writing one makes it the
input to everything downstream.

### Writes that name many documents

`updateMany` and `deleteMany` only ever reached the documents the fast tier
happened to hold. Against a partly evicted collection one reported three
modified where thirteen matched, and a delete reported two where twelve did,
both as success. For a delete that is worse than a wrong number: records
somebody asked to have removed stay readable.

Reads solve this by going to the archive. A write cannot, because the archive is
upsert only and the feeder is its only writer, so applying the change there
directly would race mirroring and break the rule the design rests on. Instead
the matching documents are brought up to the fast tier first, and then the
client's own command runs, once, against a tier that now holds all of them.

The order matters. Running the write first and repeating it after a copy-up
would apply `$inc` twice.

A sweep over an unbounded range could drag the whole archive up to satisfy one
statement, so there is a ceiling of 10,000 documents. Past it the router refuses
and says so, rather than doing part of the job and reporting success.

### Writes that ask for no acknowledgement

A write sent with `{w:0}` carries the wire protocol's moreToCome flag: the
client does not wait, and there is no reply to correct. Every guard above used
to be skipped for those, which is the same shape as a client negotiating
compression and turning them all off invisibly. It is worse in one respect, in
that `{w:0}` is chosen by ordinary applications for ordinary reasons, in metrics
writers and log ingesters, and needs no unusual driver options.

Measured on a collection holding `{_id: 5, name: "alice", tier: "gold",
balance: 100}` in the archive only: an unacknowledged
`updateOne({_id:5},{$set:{balance:200}},{upsert:true})` left `{_id:5,
balance:200}` on **both** tiers, an unacknowledged insert of an `_id` the
archive still held replaced the archived document entirely, and an
unacknowledged `deleteMany` left nine of twelve documents readable after an
erasure that reported nothing. All three are shapes that were fixed on the
acknowledged path long ago and reached again through this door.

The guards run for these writes too. The copy-up and the key claim happen the
same way, so an unacknowledged write now pays that latency, which is a real cost
on a path chosen for speed. Where a guard would have refused, the command is not
performed and nothing is sent back, because there is nowhere to send it.

`testunack.sh` pins all three, along with the control that an ordinary
unacknowledged write still lands on both tiers.

### Replies the client asked to have several of

The same flag exists in the other direction. A client that sets `exhaustAllowed`
on a `getMore` is telling the server it will accept several replies to that one
request, each but the last carrying moreToCome, and the server obliges: measured
against a real mongod, one such `getMore` produced **five** replies.

This proxy reads one reply per request. The other four stayed in the socket, and
the next command on that connection was answered by them. Measured through the
router: the reply to a `ping` sent afterwards came back as `responseTo=75`
carrying documents 4 and 5 of the cursor. With upstream connections pooled that
connection then goes back to the pool, so the next borrower is a different
client, reading somebody else's documents in answer to its own command.

The bit is cleared on the way through, which the wire protocol allows: setting
it never guaranteed more than one reply. An exhaust-capable driver loses a
round-trip optimisation and gets the same documents. This is the compression
remedy again, and for the same reason: a flag the client sets for itself must
not decide whether this proxy can still follow the conversation. As a second
line, a reply that arrives with moreToCome set anyway costs that connection its
place in the pool rather than being handed on.

No driver in the test harness can set the bit, so `test/exhaust` is a client
written to do it.

### What decides which documents a filter matches

A multi-document write on a tiered collection is resolved by asking the archive
the same filter and bringing up what it matches. Same filter is not enough: the
question has to be asked the same way.

Two things decide that and neither was carried across. Variables, because since
MongoDB 5.0 a filter refers to a `let` document as `$$name`; without them the
archive rejects the filter, the copy-up fails and the write is refused, which is
safe and is still an ordinary statement turned into an error. And collation,
because it decides what equal means. Measured on a collection holding `alice`
and `ALICE` in the archive only: a case-insensitive `updateMany` for `ALICE`
modified **one** document where an untiered reference modified two, and reported
success. The copy-up had asked the archive case-sensitively, so the other
document was never brought up.

Both travel with the copy-up now, on the multi-document path, the upsert path,
the bulkWrite path and the `$merge` target rerun, and the fast tier's own
pre-flight check for an upsert asks with the collation too. `testconform.sh`
compares each against the untiered reference.

### An insert the archive cannot be asked about

The check that stops an insert duplicating an `_id` the archive still holds has
to reach the archive. When it could not, the insert was let through.

Measured with the archive stopped: an insert of an `_id` the archive held was
accepted once the lookup timed out, landed on the fast tier, and the feeder
mirrored it over the archived record.
`{_id: 500, v: "archived, precious", tier: "gold"}` came back as
`{_id: 500, v: "clobber"}`. Nothing anywhere said so.

It is refused now, and says why. The exception is a fast tier claiming all of
history: there is nothing archived it does not also hold, so its own `_id` index
is the whole answer and the archive has nothing to add. The refusal costs the
driver's server selection timeout first, because that is how long the lookup
takes to fail, so an archive outage is felt as slow writes before it is felt as
refused ones.

The unique key registry fails the other way on purpose, and the difference is
worth stating. It lives on the fast tier, its failure means two live documents
could share a value that should be unique, and refusing every insert because a
bookkeeping collection is unreachable is worse than that. Losing an archived
document is not in the same class.

`testchaos.sh` stops every archive member and checks all three: the insert is
refused, nothing lands, and the archived document is still there afterwards.

### A change stream that asks for the document

`fullDocument: "updateLookup"` tells the server to fetch the document as it
stands when the event is read, and that lookup happens on the fast tier, which
is the one place that does not hold every document. An update to a document that
has since been evicted came back with `fullDocument: null`.

Null does not mean "somewhere else", it means gone. Measured: a change stream
reported an update with `fullDocument: null` while a read of that same document
through the router returned it. An application watching a collection to keep a
cache or a search index in step would have deleted the entry, and the document is
still there.

A null lookup is asked of the archive now, and answered from it when the archive
holds the document live. A document the application really deleted is tombstoned
there, so it stays null, which was the right answer all along, and
`testchange.sh` checks both.

The copy may be a moment behind the fast tier, because the archive trails it.
`updateLookup` never promised the post-image: it fetches whatever the document is
when the event is read, which may already be a later version or nothing at all.
A copy a moment old is within that, and "deleted" is not.

### Renaming a tiered collection

A rename moves two collections, not one, and the feeder ignores renames on
purpose: replaying collection-level changes is what would let an accident on the
fast tier reach the archive. So a rename used to be refused outright, with the
remedy spelled out for the operator to run by hand. Refusing is safe and it is
not the same as working, and rotating collections by name is ordinary.

It is done properly now, when it can be done safely:

- **Both names have to be tiered.** The namespace list is configuration, and a
  rename into a name it does not carry stops the tiering with nothing saying so.
- **Mirroring has to be caught up**, within two seconds. A write to the old name
  that has not reached the archive has nowhere to go once the archive collection
  is renamed. The window cannot be closed entirely from here, so it is made
  small and stated rather than assumed.
- **A `dropTarget` that would destroy archived documents is refused.** On the
  fast tier that flag is the client's business; in the archive it is the one
  thing no fast tier command may do.

The fast tier renames first, forwarded exactly as the client sent it, so its own
authorization decides whether it happens at all. Only once it has said yes does
the archive follow, and the completeness watermark and the unique key claims
follow with it, because both belong to the collection rather than to its name.
Residency records are dropped rather than rewritten, since they carry the
namespace inside their own id: the cost is that a document promoted moments ago
may be evicted early, which is a delay and not a loss.

If the archive rename fails after the fast tier's succeeded, the tiers are split
and the log says so at ERROR with the exact command to put it right. That is the
one case where an operator has to finish the job.

### An explain must not do the thing it explains

An aggregation can carry `explain: true` in the command itself rather than being
wrapped in an `explain` command, and then it plans rather than runs. Every guard
on that path does something on the pipeline's behalf: the merge target guard
copies archived documents up, and the completeness guard runs a writing pipeline
itself when the input is archived.

Measured: `aggregate` with `$out` and `explain: true` wrote nothing on mongod,
and through the router it wrote all ten documents into the target and returned no
plan at all. The client asked what would happen and was told nothing, having had
it happen.

An explained aggregation is forwarded untouched now and its plan is annotated on
the way back, which is what the wrapped form already got.

### A client that compresses uninvited

The compressor list is removed from the handshake in both directions, so the
negotiation never succeeds and a conforming driver never compresses. A client
that compresses anyway is a different question, and the answer used to be that
the message was logged and forwarded, which is every guard off for that command.

Measured against a collection whose documents were only in the archive: an
insert of an `_id` the archive still held was refused with E11000 when sent
plainly, and accepted when the same insert was wrapped in OP_COMPRESSED, landing
on the fast tier for the feeder to mirror over the archived record. That is the
original compression finding still live, reached by ignoring the negotiation
rather than by winning it.

A compressed message is now refused, in an uncompressed reply that names the
remedy, and the connection ends with it: there is no way to know whether what
follows is readable either. `test/compressed` is the client that compresses
uninvited, since no driver in the harness will.

### Whose cursor is it

A cursor over an archive answer is this router's, not the fast tier's, so it is
served from a registry here. That registry belongs to the router rather than to
one connection, which is right: a real MongoDB cursor is server-scoped, and a
driver's pool sends the `getMore` from whichever connection is free. Nothing else
scoped it, and the ids ran in sequence from a fixed start, so the first cursor
was always 2^62+1.

Measured with two connections to one router: the second, which had opened
nothing, sent a `getMore` for that id and was given the first client's documents
along with their real namespace. Naming a different collection in the command
changed nothing, because the batch comes from the cursor and not from the name.
That is also what made it reach past authorisation, which judges the name the
client supplied.

Three things now hold, each of them mongod's own rule:

- **Ids are random**, in the top quarter of the range so they stay clear of the
  server's own, so no id can be counted off.
- **A cursor belongs to whoever opened it.** The user when this router knows it,
  and the session whenever the client sends one, which every driver since 3.6
  does. mongod: "If authentication is enabled, you can only run getMore against
  cursors you created. Additionally, for cursors created inside a session, you
  cannot call getMore outside the session." With neither a user nor a session,
  the cursor is scoped to the connection, which is stricter than mongod and only
  reachable with no authentication at all.
- **The collection named has to be the cursor's.** mongod has refused a mismatch
  since at least 6.0, and the router now returns the same error word for word.

Refusals use mongod's code 13, so a driver sees the error it already handles.
`test/cursorowner` is the client that found this.

### Unique indexes

A unique index only constrains the documents it can see. Evicting a document
takes its index entry with it, so the fast tier stops knowing that value was
ever used and a later insert of the same key succeeds. Nothing errors, and the
collection ends up holding the two documents the index existed to prevent.

Every insert now claims its unique keys in `_thermocline.keys` on the fast tier
first, and that claim is what fails when a value is already taken, whichever
tier the original is sitting in. The caller gets an ordinary `E11000`, which is
what the application already handles.

Claims are permanent, so a deleted document leaves one behind. That is resolved
lazily rather than on the delete path: a claim only matters when something
collides with it, and at that point the router checks whether the owning
document still exists in either tier. If it does not, the new document takes the
claim over. Deletes stay untouched and this costs nothing until there is a real
conflict.

**Before evicting anything from a database you adopted with data already in it,
claim the keys it already holds:**

```
bin/cline keys --config cline.yaml              # which collections are enforced
bin/cline keys --config cline.yaml --rebuild    # claim keys for existing documents
```

It is safe to re-run, and it is also how the registry comes back after a fast
tier is rebuilt, since it lives there. If it finds one key held by two live
documents it says so and picks no winner, because that means the constraint was
already broken.

Two limits worth knowing. **Sparse and partial unique indexes are not enforced
across tiers**, because a registry cannot evaluate the same condition; the
router says which ones it skipped. And **an update that changes a unique field
to a value held by an evicted document is not caught yet**, only inserts are.

### Transactions

A transaction cannot span two clusters, so copying a document up and re-running
the statement inside the caller's transaction does not work: the copy-up is an
ordinary write outside it, and the transaction's snapshot was taken before it
landed.

The way out is the contract drivers already implement. The router copies the
document up outside the transaction, then returns an error labelled
`TransientTransactionError`, which every official driver's `withTransaction`
helper retries from the start. The replay takes a fresh snapshot, the document
is there, and the application never learns why. No code changes.

A transaction whose read or multi-document write reaches past what the fast tier
holds is the same problem one size larger, and it gets the same answer. The
documents are copied up and the transaction is replayed. Reading the archive is
still not on offer, because that would leave the transaction reading two
clusters at two points in time; moving the documents to the tier it is already
reading needs no such thing.

The completeness watermark cannot decide this one. It records that a whole range
was warmed, and copying documents up for a single query does not make that true,
so a replay that re-checked the watermark would fail exactly as the first
attempt did. What is true after the copy-up is narrower and stronger: the
archive holds no live document matching this filter that the fast tier lacks.
That is completeness established per query, and it is what lets the replay
succeed. Past `maxCopyUpForWrite` documents the range is refused instead, with
an error that is deliberately not labelled transient so it fails rather than
retrying forever.

**Unique keys are checked differently inside a transaction, and have to be.**
Everywhere else the guard runs before the write, because once a write lands it
is visible and a duplicate cannot be taken back. Inside a transaction nothing is
visible until commit, so the guard runs after the write instead: the router
reads back what the statement actually produced, from inside the caller's own
session, and refuses by aborting the transaction if it duplicates a key an
archived document holds.

That ordering is not a preference. The router's own client cannot see a
transaction's uncommitted writes, so a guard that ran first would compute keys
for a document that is not there yet. It did, and a transaction that inserted a
document and then moved it onto an archived document's unique value committed
two live documents sharing one key, reported as success. Reading the post-image
inside the caller's session removes the guess entirely: no evaluating a `$inc`
or a pipeline update, and no dry run in a session of the router's own that
deadlocks against the caller's locks.

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

### Indexes are not copied, in either direction

Nothing synchronises indexes between the tiers, and nothing should. Index trees
describe the whole collection, which is the cost tiering exists to avoid, so
carrying every one of them on the archive gives most of the saving back. The
fast tier holds the application's query plans; the archive is expected to hold
far fewer.

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

### Prometheus

The same address serves `/metrics` in the Prometheus text format: commands and
latency as a histogram per class, the copy-up, refusal, duplicate-key and
stale-read counters, documents and bytes per namespace and tier, and

    thermocline_mirroring_lag_seconds

which is the first alert to build. **It is exported by the router rather than by
the feeder on purpose.** The router already reads the feeder's published
position on every fallback read, so it knows the lag continuously, and a gauge
published by the feeder itself goes silent exactly when the feeder dies, which
is the moment the number matters. Scraped from the router, a dead feeder reads
as lag climbing, which is what an alert can see.

```yaml
scrape_configs:
  - job_name: thermocline
    static_configs:
      - targets: ["router-1.internal:27101"]
```

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
change it makes and this system is built on reading that, so with the cache on
the router follows the same change stream and drops the answers a write made
stale, whoever made it: another application, a migration script, a person in a
shell. Measured: a client's cached answer of 31 became 32 within a second of
another connection inserting a document straight into the fast tier, without
waiting out the TTL. That is the difference between a cache that expires and one
that is kept honest, and it costs one change stream while the cache is on.

The learned TTL is what remains: a backstop for the moments before the stream
catches up, bounded again by `max_ttl`.

**The key carries who is asking**, or one client would be served another's
documents, which is a bypass this system has had once already. With
`terminate_auth` on that is the verified user, and answers are shared between
that user's connections. With authentication proxied the router does not know
who the client is, so the connection is the identity: the cache then helps an
application that repeats itself on a connection, which is what a pooled driver
does, and never one client with another's answer.

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

A fast tier that genuinely holds everything, because it is the original cluster
rather than a new empty one, says so explicitly:

```
bin/cline watermark --config cline.yaml                        # show every claim
bin/cline watermark --config cline.yaml --ns app.orders --complete-all
bin/cline watermark --config cline.yaml --ns app.orders --clear
```

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

## Bookkeeping

Thermocline never modifies user documents to carry its own state. Everything
lives in the `_thermocline` database on the cold tier, whose name is
configurable as `meta.db`:

| Collection | Purpose |
|---|---|
| `checkpoints` | One change stream resume token per feeder, for recovery |
| `positions` | How far each feeder has mirrored, published far more often than the checkpoint because fallback reads wait on it |
| `evicting` | Eviction announcements, expiring on a TTL index |
| `residency` | How long a document should stay in the fast tier (used by the evictor and router) |
| `warming` | Warming announcements, expiring on a TTL index |
| `watermarks` | How far back the fast tier is complete, one per namespace |
| `markers` | Daemon heartbeats, currently just the warmer's |

The unique key registry is the one piece that lives on the *fast* tier instead,
in a database of the same name, because it has to be written in the same place
as the data it constrains.

**The name is `meta.db` in the config and almost nobody should set it.** One
pair of tiers can carry two Thermocline deployments, and the default would have
them share this database: each would read the other's watermarks, residency
holds and singleton markers as its own, and the markers in particular have no
namespace to tell them apart. A feeder id already separates the checkpoints;
this separates the rest. The config refuses a name that also holds a tiered
collection, because the feeder skips its own bookkeeping database wholesale and
that combination would stop those collections being mirrored with nothing
saying so.

Residency is a sidecar rather than a field on the document on purpose.
Extending a document's stay rewrites 60 bytes instead of a 5 KB document.

`positions` and `checkpoints` look redundant and are not. A checkpoint is
durable, majority-acknowledged and written every few seconds, because a stale
one only costs re-mirrored work after a restart. A position is a `w:1` hint
written several times a second, because a stale one costs every fallback read
its wait. Answering both with one record made freshness as slow as durability
was cheap.

The one exception is `_tcDeletedAt`, set on archived copies whose original the
application deleted. The router filters on it, so a tombstoned document is never
returned and never copied back up.

**`_tcDeletedAt` is therefore a reserved field name.** An application that sets
it on its own documents will find them treated as deleted the moment they are
served from the archive: still stored, no longer returned. Nothing detects this,
so it is a name to avoid rather than a trap the code springs.

## Running it

```
make build
cp cline.example.yaml cline.yaml     # then edit the two URIs
bin/cline feeder --config cline.yaml
```

### Logs

Every daemon logs to stderr by default. Under systemd that is what you want,
since journald captures the stream and rotates it for you.

Outside a service manager, give each one a file:

```
bin/cline feeder --config cline.yaml --log-file /var/log/cline/feeder.log
bin/cline router --config cline.yaml --log-file /var/log/cline/router.log
```

`log.file` in the config sets a default for all four, and `--log-file`
overrides it per process, which is how you keep them in separate files. A
configured file behaves like mongod's `systemLog.path`: everything goes to the
file and nothing to the terminal.

Rotation is logrotate's job, and it only works if you signal the process:

```
/var/log/cline/*.log {
    daily
    rotate 14
    compress
    missingok
    notifempty
    postrotate
        pkill -HUP -f '^/usr/local/bin/cline' || true
    endscript
}
```

Without the SIGHUP the daemon keeps writing to the renamed inode, so the file
you are tailing stays empty while the disk fills up.

Both tiers must be replica sets, since the feeder uses change streams.
MongoDB 4.2 or newer.

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

## What is not handled yet

- **Sharded fast tiers are refused, with the reason.** Everything here assumes
  the fast tier is a replica set. The router proxies bytes to one primary
  address and a mongos does not have one: measured, its `hello` answers
  `{msg: "isdbgrid", isWritablePrimary: true}` and nothing else. Every reading
  the evictor holds itself back on is a replica set's, and a mongos has none of
  them: `replSetGetStatus` is "not supported through mongos", the `local`
  database cannot be reached at all, and `serverStatus` carries no `wiredTiger`
  section, so cache pressure and write tickets are unknown too.

  A reading that cannot be taken does not block, so before this was refused the
  daemons ran against a mongos holding themselves back on nothing: an evictor
  dry run with five eligible documents reported `would_evict=5` and
  `backoffs=0`. The router did stop, with "fast tier reported no primary
  address", which is true and says nothing about the cause.

  All four commands now refuse a mongos fast tier and name it.
  `test/testsharded.sh` builds a config server, a shard and a mongos on the host
  and checks each one. Supporting a sharded fast tier is a feature rather than a
  fix, and the first thing it needs is health readings that mean something
  there.
- **Ticket pressure is measured as a fraction of the pool.** MongoDB 7.0 made
  the write ticket pool dynamic and host-sized, so an absolute floor is
  meaningless: a container reports a total of 8 tickets where a pre-7.0 server
  reported a fixed 128.
- **Router connection pooling, with authentication proxied.** One upstream
  connection per client is simple and correct for SCRAM, and it will not scale to
  tens of thousands of clients. With `router.terminate_auth` on the connections
  are pooled instead: measured, eight idle clients cost zero extra connections to
  the primary, against thirty two with it off.
- **Collection DDL other than rename** is logged and ignored, never replayed.
  A drop is the one worth knowing: the fast tier's copy goes, the archive keeps
  its documents, and the completeness claim is withdrawn, so the collection
  still answers from the archive afterwards. That is the rule working rather
  than failing, because replaying a drop is exactly how one accident on the fast
  tier would destroy the archive. Removing archived documents is deliberately a
  separate act.
- **`terminate_auth` has never met traffic that was not a test.** Everything it
  lifts is lifted in principle only while it is off by default, and the
  security-critical half of it is an authorization gate. That gate is the only
  thing between a client and somebody else's data in that mode, it denies by
  default, and it has thirty three checks behind it in `testauthterm.sh`,
  including the ones that caught it allowing a `$lookup`, a `$merge` and an
  unacknowledged write it should have refused. It is still a judgement call
  rather than a finished feature.

## Design documents

The architecture, sizing from 500 GB to 5 TB, and the alternatives worth ruling
out first are written up separately, along with a phased rollout guide for
adopting this on a database that is already serving traffic.
