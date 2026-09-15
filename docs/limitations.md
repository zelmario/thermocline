# What is not handled yet

Every limitation known, with what each one costs and what closes it.

Part of [Thermocline](../README.md).

## What is not handled yet

- **`dropIndexes` on a tiered collection is refused when it asks for no
  acknowledgement**, and performed normally otherwise. The archive's copy of the
  index is dropped behind the fast tier's, and only the reply says whether the
  fast tier dropped its own; dropping it before forwarding would remove the
  archive's index for a command the fast tier might turn down. Measured before
  the refusal, in the adoption shape where the archive carries its own unique
  index: the fast tier ended without the index and the archive kept it, and of
  two documents the application was then entitled to write, one reached the
  archive and one did not, with nothing said anywhere. The remedy the refusal
  names is to send it without `writeConcern {w: 0}`, which no driver sets for
  DDL on its own.

- **`create` on a tiered collection is refused when it carries options the
  archive needs and asks for no acknowledgement**, and performed normally
  otherwise. A bare `create` is not refused. The archive's copy is created with
  those options on the reply to the command, and without a reply it would be
  created instead by the first mirrored insert, carrying none of them. Measured
  with `collation: {locale: "en", strength: 2}`: the fast tier ended with it and
  the archive without, and `find({name: "ALICE"})` then answered 1 or 0
  depending on which tier answered. A collation cannot be added to a collection
  that already holds documents, so that one does not heal. The remedy the
  refusal names is to send it without `writeConcern {w: 0}`.

- **A write made straight at the fast tier destroys archived documents, and
  nothing says so.** The application goes through the router; that is the whole
  contract, and this is outside it. It is also one connection string away: a
  migration script, a monitoring tool that writes, a leftover config, somebody
  in a shell. Measured on an `_id` the archive held and the fast tier did not:
  through the router the insert is refused, which is what one cluster would do
  with a duplicate key; straight at the fast tier's mongod it is accepted, and
  six seconds later the archive holds the new document with the old one's fields
  gone. No line in the feeder's log, none in the router's.

  It cannot be refused from here — the router is not in that path by
  definition — and it cannot be told apart from an application reusing an `_id`
  either. It is now counted rather than silent. The feeder publishes
  `inserts_over_existing`: inserts it mirrored onto an `_id` the archive already
  held, which through the router is refused as a duplicate key.

  What that number is worth, measured rather than reasoned. A shop run of 348
  mirrored batches with copy-ups and eviction sweeps underneath: zero. Three
  unclean feeder kills mid-batch, which is the replay this entry used to say
  would confuse it: also zero. One write made straight at the fast tier's
  mongod: one. And a delete and re-insert of the same `_id` through the router,
  which is legitimate: also one. So it is a lead rather than a verdict, and it
  carries no alert rule for that reason, but a deployment where it climbs and
  nobody reuses `_id`s has a writer that is not going through the router.

  The protection is still operational: only the router should hold credentials
  that can write to the fast tier's application databases.

- **A `$text` query fails on the archive rather than running slowly, and so does
  `$geoNear`.** Every other index is a decision about speed and the two tiers are
  allowed to differ. These two are not: mongod refuses a `$text` query with no
  text index and a `$geoNear` with no geospatial one, so a read of a tiered
  namespace that falls through to an archive without them does not get slow, it
  fails, and the message names an index the application can see on the fast tier.
  Measured with the text index on the fast tier alone: `text index required for
  $text query` through the router, answered straight at the fast tier, and
  answered through the router as soon as the archive had one too.

  The router says so where the difference is made rather than where it is felt:
  a `createIndexes` building a text or geospatial index on a tiered namespace
  the archive has no equivalent for gets its own line, saying the archive needs
  it to answer at all rather than to answer quickly. Building it on the archive
  is one answer and keeping the namespace warm enough that these reads never
  fall through is the other.

- **A tiered collection dropped on the archive reads as empty, while the fast
  tier still holds it.** The archive is the superset, which is what makes it
  right to answer a read the fast tier cannot claim from the archive alone. Take
  the archive's copy away and that read is answered from nothing.

  Measured on thirty mirrored documents with the archive's copy dropped straight
  at the archive's mongod: the fast tier still held all thirty, the client was
  told zero, and neither tier's log mentioned it. One piece of good news in it,
  also measured: an eviction sweep over the same namespace evicted nothing, so
  the fast tier's copy is not at further risk while this stands.

  The same state arrives three ways and the advice differs, so the router names
  all three rather than guessing: the collection was dropped on the archive, the
  archive was restored from a backup older than the collection, or nothing has
  ever mirrored the namespace because the feeder is not running. The last is why
  the check counts documents first: a namespace nothing has written to yet has
  no copy on either tier and is not worth a word.

  It is said at startup and by `cline survey`, which is where the mirror image
  of it is already said, and not on the read path: the read that goes short is
  the cheapest possible one and putting a catalog lookup on it would cost every
  deployment for a state almost none of them are in. So a drop that happens
  while a router runs is named at its next start rather than at once, and the
  first run of this named two namespaces left behind by the test suites that
  nobody had noticed.

- **A collection converted to capped while it is being mirrored stops the
  feeder.** The kind check runs at startup and again the first time an event for
  a namespace arrives, and it remembers its answer until the collection is
  dropped. That covers a collection created capped; it left the case of one
  already being mirrored and converted afterwards. `convertToCapped` on a tiered
  collection is refused by the router, so it takes running it straight at the
  fast tier's mongod. Measured before this: twenty mirrored documents, then the
  conversion, then filling past the cap, and the archive held sixty tombstones
  with two of the original twenty still live. The question is asked again when
  deletes arrive that nothing announced, which is the only way this does damage
  and its only symptom, at most once a minute per namespace. The feeder then
  stops with the namespace and the kind named, which is what the startup check
  already does for the same reason.

- **A TTL index created straight against the fast tier cannot be refused, and is
  named rather than prevented.** The router turns down a TTL index on a tiered
  collection, because the fast tier holds the recent part: the index expires the
  documents recent enough to be cached, the feeder mirrors those expiries as the
  application deleting them, and the older documents the rule was written for sit
  in the archive where no TTL index is watching. That guard is on the router's
  door. An operator, a migration script or a restored fixture can create the same
  index directly on the fast tier's mongod, and nothing here can stop it.
  Measured with five recent documents on both tiers and five a year old in the
  archive alone: the recent five were gone from the fast tier and tombstoned in
  the archive, the year-old five untouched, and the only trace was the feeder's
  ordinary "deletes not announced by the evictor", which is what a real
  application delete looks like too. The router now says so when it reads the
  collection's index listing, once per namespace and again whenever the answer
  changes, naming the index and which way it goes wrong. The archive's own side
  of this already had such a line; this is the fast tier's.

- **mapReduce cannot write into a tiered collection**, and is refused when it
  tries. A replacing output is built beside the target and renamed over it, and a
  rename is not mirrored, so the result would reach the fast tier and nothing
  else; a merging one writes per document against a tier that holds only part of
  the collection. The same job expressed as an aggregation ending in `$out` or
  `$merge` is performed on both tiers, which is the remedy the refusal names.
  Reading a tiered collection with mapReduce is refused for a different reason,
  already: it totals whatever tier it runs on.

- **A pipeline nesting sub-pipelines deeper than mongod's own limit is
  refused.** The collections a pipeline reads are found by walking its
  sub-pipelines: `$lookup` and `$unionWith` each take one, and `$facet` holds
  several. That walk had a depth limit of 10 and returned nothing at it, so
  anything below was invisible to the completeness check and the pipeline ran on
  the fast tier unjudged.

  Measured on 8.0.26: a `$lookup` nested 14 deep runs, and 20 is where mongod
  itself answers `MaxSubPipelineDepthExceeded`. So the limit was under mongod's,
  and a tiered collection with ten documents in the archive and three on the
  fast tier, read by a `$lookup` nested 11 deep, answered **3 with no error**
  where the same pipeline at depth 9 was refused. The limit is mongod's now and
  reaching it refuses rather than returning a list known to be short.

- **`dbHash` is refused for a collection the fast tier does not hold in full,
  and `validate` describes one tier.** Both read every document, and neither can
  be corrected the way a count can.

  `dbHash` returns one hash per collection, and there is no way to combine the
  fast tier's with the archive's short of reading both in full, which is not
  what a diagnostic should do. Measured on a collection with ten documents in
  the archive and three on the fast tier: it answered a different hash with
  nothing to say it had seen a third of the collection. That is the worst
  possible answer for the command people use to compare two copies of a
  dataset, so it is refused with the collections named, and allowed when the
  claim covers everything — where it is the collection's hash and equals the
  archive's exactly.

  `validate` is not corrected, deliberately. It is a physical check of one
  server's storage, so its `nrecords` is the fast tier's the way `storageSize`
  is, and correcting one field would leave it inconsistent with the index key
  counts in the same reply that mongod compares it against. Read it as a check
  of the fast tier, and run it against the archive separately.

- **A tiered `$out` is assembled in a scratch collection inside the
  application's own database.** `$out` into a tiered collection cannot
  end in a rename, so the result is built in
  `tmp.thermocline.out.<nanoseconds>` and renamed over the target, and that has
  to sit in the application's own database for the rename to be possible.

  Four things know they are not the application's data. The feeder does not
  mirror them, the catalog does not merge one in from the archive, a starting
  router drops any a previous router left behind after dying mid-aggregation,
  and `listCollections` through the router does not show them: measured, the
  fast tier lists one and the router lists none, with the real collections
  untouched.

  A long listing is filtered too, which it was not at first. The router filters
  the first batch where it appends the archive's half, and the batches after
  that used to come straight from the fast tier: measured with `batchSize: 1`
  over 100 `getMore`s, one scratch collection appeared in a later batch of a
  listing whose first batch was clean. Both are filtered now.

  What is still true of a long listing is the archive's half: the collections
  the archive holds and the fast tier does not are appended to the first batch
  only, because the router does not answer the `getMore` behind a cursor the
  fast tier opened. A client that pages the catalog sees them if they fit in the
  first batch and not otherwise.

- **A pipeline the archive would answer that also reads an untiered collection is
  refused**, because answering it would be short for that half.

  The archive holds every document of what it is told to tier, which is what
  makes the fallback correct. It holds whatever it happened to hold of everything
  else: the feeder mirrors the namespaces in `feeder.namespaces` and nothing
  else, so anything written to an untiered collection since the cutover is on the
  fast tier alone. A `$lookup` or `$unionWith` of one, in a pipeline the fast tier
  cannot answer, would be read from a copy that stopped being updated.

  Measured before it was refused, on a `$unionWith` of an untiered collection with
  a view over a tiered one: the answer was 10 where the two clusters together hold
  11, the missing document being the one written to the untiered collection after
  the cutover. Neither the pilot nor the comparison harness can see that: the
  pilot's untiered collections were restored into the archive and never written to
  again, and the harness asks the archive the same question and gets the same
  short answer.

  This was a warning for one afternoon, on the reasoning that a report joining a
  reference collection unchanged since the adoption is right more often than it is
  wrong. That trades somebody else's correctness for our convenience, and the
  answer it let through carried no error, so it is a refusal: a refusal is a
  limitation, a wrong answer is a bug. The message names the collections and the
  two remedies, and the refusal lifts by itself once either is done: **tier the
  collections the pipeline reads**, or **warm the tiered ones** so the fast tier
  answers the whole pipeline and nothing goes to the archive at all.

  There is a third way out, for the case the afternoon's reasoning was actually
  about: `router.allow_untiered_join` answers the pipeline instead and names the
  untiered collections in a warning every time. It is a claim about your own
  application, not about this system, so nothing here can check it: set it only
  where those collections really are reference data nobody writes to. The pilot
  runs both ways, because it is the one place the claim is true, and with it on
  all nineteen aggregations agree with the archive: `bash test/pilot/setup.sh
  join`.

- **A view whose own pipeline joins a tiered collection is refused unless the
  fast tier holds that collection in full.** A view's `$lookup` is in the view's
  definition, not in the query, so nothing about the query says the join is
  there. The completeness check reads the client's pipeline.

  The reading was already half-right: a joining view has always been refused
  when the collection it is *defined on* is incomplete, because the archive
  cannot be asked. What nobody had asked is the same question of the other path,
  the ordinary one. When the collection the view is defined on **is** complete,
  the fast tier answers, and the collection the view joins was never looked at.

  Measured on two rows whose partners the archive holds both of and the fast
  tier one: the archive joins both, and the router answered one row joined and
  one with an empty join array, with `ok: 1`. That is a reporting view over
  history quietly dropping the rows whose partner has aged out, which is every
  row the tiering exists to keep.

  Both collections are judged now, and the joined one has to be whole, for the
  reason a `$lookup` written in the query does: a join reads by key rather than
  by time, so no partial window makes it whole. The remedy is in the message and
  it is the usual one: warm the joined collection, or aggregate against the
  archive.

  It is a refusal rather than an archive answer, and that is not laziness. The
  archive's copies carry tombstones, and a `$lookup` reads a collection of its
  own, so the tombstone predicate the router puts in front of a pipeline never
  reaches inside a view's definition — which the router cannot rewrite.
  Answering it there would join deleted documents, which is the defect in
  *Joined collections are read elsewhere* with the sign flipped.

  **A `$out` into a tiered collection that reads through a view is refused for
  the same reason**, and that refusal is the sharper end of it: the reading half
  of a tiered `$out` is performed by the router rather than forwarded, so a short
  answer there is not something a client reads and re-runs, it is something on
  disk. Measured before it was refused: a pipeline reading a view over a tiered
  collection the fast tier was short of wrote one document where the archive
  holds two. A `$lookup` *naming* a view is not refused — that one resolves to
  the collection underneath and is read from the archive, whole.

- **Cursors are not shared between routers.** A read answered from the archive
  is served from a cursor this router holds open against the archive, in its own
  memory, so another router cannot continue it. This is what a mongos does too,
  and drivers pin cursors to the server that created them, so naming every
  router in the connection string is all it takes. What does not work is one
  load balanced address in front of several routers: the driver sees one server,
  its pool sends the getMore down whichever connection is free, and it lands on
  a router that never had the cursor.

  What the application sees then is `CursorNotFound`, code 43, and the router
  now says what that means rather than leaving it bare. It cannot say *which* of
  two things it is, because a cursor id says nothing about who minted it:
  measured on PSMDB 8.0, mongod's own ids cover the whole positive int64 range,
  so an id from another router is indistinguishable from an expired one. So it
  names both, and how to rule one out — name every router in the connection
  string and the driver pins each cursor to the one that made it. The code stays
  43, because a driver decides what to retry from the code rather than from the
  words.

- **A change stream cannot be resumed across the cutover, and the router says
  so rather than letting it be discovered.**
  A resume token is a position in one replica set's oplog. The router serves
  change streams from the fast tier, so a token minted on the cluster being
  adopted is not a position in it. MongoDB accepts it rather than refusing it:
  measured, the same token delivered the write that followed it against the
  cluster it came from, and delivered nothing at all through the router, with
  `ok: 1`. A consumer resuming across the cutover therefore loses every event
  the old cluster saw after its last token.

  It is not silent any more. The router logs a warning naming the namespace and
  the option the client used, with the effect and the remedy, every time a
  stream is opened with `resumeAfter`, `startAfter` or `startAtOperationTime` on
  a tiered collection. Refusing outright would mean parsing resume tokens, whose
  encoding is undocumented and changes between releases, so this names it
  instead: start consumers fresh after a cutover, and check the stream is
  delivering rather than merely open. Pinned by `testchange.sh`.

  Refusing it would mean the router parsing resume tokens, which are an
  undocumented encoding that changes between server releases, so this is a step
  in the rollout rather than a guard: start consumers fresh.

  **What it does now is say so.** A change stream opened with `resumeAfter`,
  `startAfter` or `startAtOperationTime` on a tiered collection is named in the
  log at WARN, with which option carried it. That does not make a stale token
  work; it means the one failure whose symptom is silence has a line somewhere
  that mentions it.

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

- **A sharded archive is not supported either, and it is not refused. Every
  daemon says so at startup.** Both tiers are replica sets: that is the
  supported shape, and it is the only one anything here is tested against. A
  standalone archive gets the same warning, for the same reason: nothing tails
  the archive so it needs no oplog, which is why neither is refused, and neither
  is tested. "Not refused" used to be indistinguishable from "supported" from
  the outside.

  The distinction matters, so it is stated rather than left to be discovered. A
  mongos archive is *not* obviously broken. Nothing the daemons hold themselves
  back on is read from the archive, so the health readings a mongos cannot give
  are never asked of it; it is only ever sent ordinary queries, ordinary writes,
  and `hello` for its clock, and it answers all three. Pointed at one, this
  behaved: writes mirrored, fall-through reads came back, deletes became
  tombstones, and an eviction sweep removed from the fast tier and left the
  archive's copy.

  It is still not supported, because nothing checks it. There is no suite,
  nothing runs that way, and a defect there would be found by whoever is running
  it in production rather than here. If that shape is wanted, the work is a suite
  that keeps it honest, not a change to the code.
- **A cursor whose completeness claim narrows under it is refused, not answered
  short.** The claim is read once, when the query arrives. A cursor outlives
  that: the first batch comes from the fast tier because the claim covered the
  read, and every `getMore` after it is forwarded to the fast tier, which by then
  may hold none of it.

  Both kinds of cursor. A `find` and an `aggregate` are judged at opposite ends
  of the command — one on the way back, one before it runs, because a pipeline
  can end in `$out` — and the aggregation is the easier of the two to reach:
  mongod puts 101 documents in an aggregation's first batch and a cursor holds
  the rest, so a pipeline over a collection larger than that leaves one without
  the client asking for a `batchSize` at all. An aggregation is also refused for
  a joined collection whose claim has narrowed, because `$lookup` needs that one
  whole.

  Measured on forty documents held by both tiers with a claim over all of
  history. A client took a first batch of three, an eviction sweep took the fast
  tier from forty documents to nought, and the cursor delivered **3 of 40**.
  Nothing had been deleted: the archive held all forty throughout, and a query
  issued after the sweep answered forty. Only the cursor was short, and it was
  short in silence.

  It is refused now, with the namespace named and the remedy in the message:
  re-run the query, and it is answered across both tiers and is whole. That
  costs an application one error on a read it can repeat, which is the trade this
  project makes on purpose — a refusal is a limitation and a wrong answer is a
  bug.

  It is not corrected, and the reason is worth stating. Continuing the read from
  the archive would mean knowing where the fast tier's cursor had got to, which
  needs the sort key of every document already handed over, and which is not
  defined at all for a query with no sort. An application paging a large tiered
  collection through a long-running sweep should expect this and retry; a
  `batchSize` large enough to finish the read in one batch avoids it entirely.

  **The guard is one router's, and a `getMore` that reaches a different router
  is not guarded.** What a cursor was authorised by is held in the memory of the
  router that opened it. A fast tier cursor works from any router, because they
  all speak to the same fast tier, so nothing fails: the second router forwards
  the `getMore` and the answer comes back short, which is the behaviour this
  entry exists to remove. The remedy is the one already required for cursors the
  router owns against the archive, a few entries above: **name every router in
  the connection string** rather than putting one load balanced address in front
  of them, and the driver pins each cursor to the router that made it. Recording
  the authorisation in the shared bookkeeping database instead would cost a round
  trip on every tiered query that leaves a cursor, which is most of them.

- **`distinct` over a field some documents do not carry can omit the `null`
  MongoDB would report.** This is the one command whose answer depends on the
  query plan, and the router has to change the plan.

  MongoDB reports a missing field as `null` only when it can answer from a
  non-sparse index, because an index stores a missing field as a null key.
  Measured on PSMDB 8.0 over three documents, one of which has no `meta.score`:

  ```
  no index at all       distinct("meta.score", {"meta.score":{$ne:11}})  ->  [5,7]
  a plain index         ->  ["NULL",5,7]   plan DISTINCT_SCAN
  a sparse index        ->  [5,7]
  ```

  Every archive read carries a predicate excluding the copies of documents the
  application deleted, because returning one would resurrect it. That predicate
  is what costs the covered scan:

  ```
  the client's filter alone             ["NULL",5,7]   plan DISTINCT_SCAN
  with the tombstone predicate added    [5,7]          plan IXSCAN
  ```

  So a `distinct` that falls through to the archive answers without the `null`,
  where a single MongoDB carrying the same index answers with it. Found by the
  differential harness, on a generated `distinct` after a round of generated
  index DDL.

  It is not corrected, and the reason is that a partial correction would be
  worse. Answering from the fast tier as well and merging the two value sets
  closes the case where the fast tier holds one of the documents missing the
  field, and leaves the case where they are all in the archive, so the rule
  would become "sometimes". Reproducing the behaviour outright means the router
  deciding, from the fast tier's own indexes, whether mongod would have used a
  covered scan, which is reproducing a plan choice rather than an answer.

  What to rely on instead: `null` in a `distinct` result means "some document
  has no value here" only when an index makes it visible, in MongoDB as much as
  here. Ask `countDocuments({field: {$exists: false}})` for that, which is a
  logical fact and is correct through the router.

- **A change stream consumer more than an hour behind is shown the tiering
  itself.** What makes a copy-up an insert the application never made, an
  eviction a delete it never issued, and a catalog repair a `create` it never
  ran, is a record this system writes beside the event: a residency hold, an
  eviction notice, an `ownddl` span. A client's stream is judged against those
  when it reads the event, not when the event happened, which is what lets a
  consumer resume from a stored token and still be told the truth.

  Every one of those records is kept on a TTL and then goes. An eviction notice
  and an `ownddl` span last an hour from being written (`meta.NoticeTTL`); a
  residency row lasts an hour past the end of the hold it records, so its bound
  is `router.residency` plus that hour. Past whichever applies, the evidence is
  gone and the event is handed over as the application's own. A consumer that
  keeps up never reaches it; one that is stopped for an afternoon, or resumes
  from a token it stored overnight, does.

  The hour is a choice about how long to keep evidence nothing else needs, and
  raising it costs storage in the bookkeeping database rather than correctness.
  What is *not* on the same footing is the feeder: `evictor.max_feeder_lag` is
  refused when it reaches `NoticeTTL`, because there an expired notice means a
  document destroyed on both tiers rather than an event described oddly. A
  client's stream has no such bound to enforce, so this is written down instead.

  Keep consumers of a tiered collection's change stream inside the hour, or
  treat `create`, `createIndexes` and unexplained inserts on a tiered namespace
  as possible tiering activity rather than the application's.

- **A filter written with `$expr` never bounds a query, so it is always answered
  by the archive.** The router decides whether the fast tier can answer a read
  alone by reading a lower bound out of the filter, in the dimension the
  namespace ages by. It reads that bound from the age field's own predicate:
  `{createdAt: {$gte: T}}` and the `$and` and `$or` forms around it.
  `{$expr: {$gte: ["$createdAt", T]}}` says the same thing to mongod and nothing
  to this, because the field name is an argument inside an expression rather
  than a key in the filter.

  Nothing wrong is returned. The query reaches the archive and comes back whole,
  which is the safe direction, and the cost is the whole optimisation for that
  query: on a large archive that is the difference between seeking and scanning.
  Measured against `lowerTimeBound` directly, plain and nested inside `$and`:
  neither is bounded.

  Write the bound as an ordinary predicate where the query allows it. `$expr` is
  needed to compare two fields of the same document, and a comparison against a
  constant is not that. Closing it properly means reading `$expr` arguments of
  the shape `["$field", constant]`, which is a narrow case of an expression
  language this does not otherwise parse, and it has not been worth it against
  rewriting the query.

- **A claim declared by hand past the counting budget is recorded unchecked.**
  `cline watermark --complete-all` says the fast tier holds every document there
  is, and recording one it does not have makes every unbounded read answer short
  with no error. So both tiers are counted first and a claim the numbers
  contradict is refused. Counting is not free on a large collection, so it is
  bounded by `--count-for`, a minute by default, and past that bound the claim is
  recorded **unchecked** rather than refused: blocking a rollout on a count is
  the worse failure, and the operator is left exactly where they were before the
  check existed.

  That leaves one outcome nobody asked for, and it is the collection big enough
  to need the check that reaches it. Raise `--count-for` on those, and read
  "recorded unchecked" as a claim nothing has stood behind.

- **The router will not start while the archive is unreachable**, so a restart
  during an archive outage leaves the application with nothing, including the
  reads the fast tier could have served alone. Decided on 2026-09-05 to stay that
  way: a router that has never read a claim knows which namespaces are tiered and
  nothing about what the fast tier holds of them, so anything it served would be
  a guess dressed as an answer, and starting-and-refusing gives the application
  errors either way while looking healthy to monitoring. The runbook entry is in
  [adoption.md](adoption.md): bring the archive back before restarting a router.

  A router that is already **running** is a different case and has a knob:
  `router.claim_grace` keeps tiered reads alive through an outage.

- **`applyOps` is refused for a tiered collection.** It applies oplog entries
  straight to the collection, so it passes none of the checks a write command
  passes: an `_id` or a unique value the archive still holds is never asked
  about, no archived document is brought up first, and no key is claimed.
  Measured before it was refused, with `{_id: 800, v: "archived, precious",
  tier: "gold"}` in the archive and nothing on the fast tier: an `applyOps`
  insert of that `_id` was applied, and six seconds later the archive held
  `{_id: 800, v: "clobber via applyOps"}`. The same insert sent as an insert is
  refused.

  Guarding it properly would mean reimplementing every one of those checks
  against an internal command whose entries may be inserts, updates, deletes or
  commands in any mixture. MongoDB documents it as internal and no application
  sends it, so it is refused with the reason instead, including when the
  collection is hidden one level down in a command entry such as `{op: "c", ns:
  "shop.$cmd", o: {drop: "orders"}}`. An `applyOps` that touches nothing tiered
  is forwarded whole. `mongorestore --oplogReplay` uses this command: point it
  at the archive directly, which is the restore path the runbook already
  describes.

- **Two daemons on hosts whose clocks differ are corrected, not required to
  agree.** Several things here are decided by comparing instants read on
  different machines, and none of them can be got from a logical clock.

  Where the reader is a daemon, the instants are put in the archive's frame
  before they are compared: mirroring lag, the freshness barrier's wall-clock
  bar, how old a published position is, and how long ago a daemon last
  reported. Both sides talk to the archive, so each publishes how far its own
  clock is from it. That reading is taken over a round trip once a minute, so it
  is good to a fraction of a millisecond and stale by up to a minute of drift,
  which is nothing beside the seconds any of these weigh.

  Where the reader is the archive itself, there is nothing to correct and no
  reading to take: the row is stamped by the archive as it is written, with
  `$currentDate` or `$$NOW`. That covers everything the archive's TTL monitor
  decides the lifetime of, which is the eviction and warming announcements, this
  system's record of its own DDL, and the residency rows, plus the residency
  hold itself, which a different daemon judges. An eviction announcement stamped
  by a clock an hour behind used to be born expired: the feeder then read every
  eviction as the application's own delete, the archive tombstoned its copy, and
  the document was lost from both tiers with nothing said. That one is gone
  rather than corrected.

  What is left is the windows measured against the application's own document
  dates, an eviction cutoff and a warm window, which shift by the difference
  rather than breaking. Every daemon's reading against the archive is published
  as `thermocline_daemon_gauge{gauge="clock_skew_seconds"}` and two alert rules
  watch it. Run NTP on every host that runs a cline daemon: none of the above is
  a reason not to, it is what stops a clock nobody noticed becoming a wrong
  answer.

- **Ticket pressure is measured as a fraction of the pool.** MongoDB 7.0 made
  the write ticket pool dynamic and host-sized, so an absolute floor is
  meaningless: a container reports a total of 8 tickets where a pre-7.0 server
  reported a fixed 128.
- **Router connection pooling, with authentication proxied.** One upstream
  connection per client is simple and correct for SCRAM, and it will not scale to
  tens of thousands of clients. With `router.terminate_auth` on the connections
  are pooled instead: measured, eight idle clients cost zero extra connections to
  the primary, against thirty two with it off.

  The wall is mongod's rather than the router's, so a deployment used to meet it
  as connection errors on the fast tier with nothing here having mentioned it.
  The router says so once, at a thousand open clients, naming what each one costs
  and what turns it off. A thousand is well short of the wall and well past any
  ordinary application's pool. Nothing is said with `terminate_auth` on, where an
  idle client holds no upstream connection at all.
- **`$out` into a tiered collection is the one place the router removes archived
  data**, and it is deliberate rather than an oversight. Clearing the fast tier
  only reaches what the fast tier holds, so an evicted document lives in the
  archive alone and would come back beside the result: measured, a `$out`
  promising exactly one document answered with two. `$out` says the target holds
  the result and nothing else, so the archive's live copies are marked deleted
  too. A drop is not replayed into the archive precisely so one accident cannot
  destroy it; a `$out` is not an accident, it is an application saying what this
  collection should now contain. The archived documents are tombstoned rather
  than removed, so what they were is still there to look at, and the archive is
  cleared after the fast tier, so a failure leaves the collection reading as
  what it held rather than half replaced. It is the expensive half as well:
  clearing the fast tier reaches only the working set, while this reaches every
  live document the archive holds for that namespace, so a `$out` over a
  collection with years behind it is a large write. The router logs how many it
  marked.

  **A `$out` whose target is tiered is performed by the router whatever its
  input reads.** mongod does a `$out` by building the result beside the target
  and renaming over it, and a rename is not mirrored: a forwarded one put the
  result on the fast tier and nowhere else. Measured, two documents written and
  the archive holding none of them a minute later, with a count through the
  router answering 1 where the fast tier held 2. The result is built in a
  temporary collection here too, because clearing the target is part of the
  write and `$out` is allowed to read the collection it replaces: clearing first
  left nought documents on either tier where MongoDB leaves two. The last step
  moves the result in with ordinary inserts, which is the only difference from
  mongod, and claims its unique keys so the registry still holds them once those
  documents are evicted.
- **A unique index build waits for a walk of both tiers.** mongod refuses
  `createIndex` with `unique` when two live documents already share a key, and
  the fast tier's own build cannot see the half of a duplicate that is in the
  archive. The router walks both tiers before forwarding the command, so the
  refusal is the same 11000 mongod would have given, and the keys of the
  documents already there are claimed on the way. On a large collection that
  walk is the dominant cost of creating the index. It happens only for a spec
  asking for `unique: true` on a tiered collection.

- **An adopted archive's unique constraints are not enforced until something
  claims their keys, and the router now says so at startup.** The registry is
  what remembers that a value is taken once its document is only in the archive,
  and it is filled by whatever claimed the key: a write through the router, or
  the walk of both tiers that a `createIndexes` for `unique: true` performs
  before the build is forwarded.

  An adoption has had neither. The archive arrives with its documents and its
  indexes already on it, so no `createIndexes` ever went through the router and
  nothing has walked it. Until something does, an insert duplicating a value
  only the archive holds is **accepted**: the fast tier's own index cannot see
  the other half, and a key whose registry row inserts cleanly is settled there
  and then, with nothing asked of the archive. Measured on an archive holding
  two documents and a unique index on `sku` with an empty registry: a third
  document repeating one of those values went in, and after `cline keys
  --rebuild` claimed the two the same insert was refused with 11000.

  So a uniqueness guarantee an application has relied on for years quietly stops
  holding on the day it is adopted, until one command is run. `cline keys
  --rebuild` was documented as the answer to "an index created out of band, a
  fast tier rebuilt from nothing, a cluster adopted with data already in it" and
  never as a consequence of skipping it, and a documented step is not a
  guarantee. The router now warns at startup for each tiered collection whose
  unique index has no claims at all while the archive holds documents that index
  would index, naming the namespace, the index and the command. It stays quiet
  for a fast tier built from nothing, which has nothing to claim, and for an
  index created through the router, which claimed on the way.

- **Dropping a tiered collection on the fast tier ends its unique constraints,
  and the router says so rather than pretending.** The router enforces a unique
  index across the tiers by reading the fast tier's listing, so a collection
  dropped out of band takes the index with it: the next write recreates the
  collection bare, and neither tier carries the constraint any more. The archive
  still holds every document it covered, and nothing stops a write duplicating
  one of their keys.

  It used to hold on to the listing for a minute after a drop, which read like
  protection and was not: the listing expires on its own, so the constraint was
  going to be lost a minute later either way. What that minute did produce was a
  false refusal. A collection dropped and rebuilt out of band — resetting an
  environment, restoring a fixture — does not have the old collection's unique
  index, and the router refused writes on its behalf with a duplicate key error
  naming an index that exists on neither tier. The fuzzer found it after four
  rounds, because its own reseed is that shape.

  So the constraint is dropped as soon as the fast tier says it is gone, and the
  router logs which index it stopped enforcing, what that allows and what to do
  about it. If the drop was not deliberate, recreate the index and run
  `cline keys --rebuild`, which reports any duplicates that got in.

- **`create` with `capped` written as a number was not refused, and mongod
  accepts it.** MongoDB declares that option `safeBool` in its IDL, which takes a
  number as well as a boolean and calls anything non-zero true. The router read
  it as a boolean, found none, and forwarded the command, so the capped refusal
  could be stepped around by spelling the flag differently.

  Measured on PSMDB 8.0.26: `capped` as an int32 `1`, as a long `1` and as a
  double `1.0` each created a capped collection. The same latitude is not offered
  elsewhere, which is why the fix is narrow: `renameCollection`'s `dropTarget`
  and an update's `upsert` are plain bools and mongod refuses a number for either
  with "the wrong type 'int', expected type 'bool'". A guard that coerced those
  would be turning down commands the server would never have run.

- **A clustered collection cannot carry a TTL while it is tiered, and its
  clustering does not reach the archive.** MongoDB 5.3+ lets a collection be
  created with `clusteredIndex`, and such a collection can expire its own
  documents through a top-level `expireAfterSeconds` rather than through a TTL
  index. mongod then removes its own oldest documents exactly as a capped
  collection does, nothing announces those removals, and the feeder reads them
  as the application deleting.

  Measured on PSMDB 8.0.26 before this was refused: twenty documents whose `_id`
  values were dates an hour old, in a tiered collection created with
  `expireAfterSeconds: 1`, mirrored to the archive as twenty live documents and
  were gone from the fast tier 27 seconds later. The archive was then holding
  **twenty tombstones and nothing live**, and the router answered `0`. That is
  the capped collection's failure through a door neither check was looking at:
  the router's TTL refusal reads a `createIndexes` command's `indexes` array and
  a `collMod`'s `index` document, and this is neither.

  Both spellings are refused now, on `create` and on `collMod`, and the feeder
  refuses to start against a collection that already has one, and stops the same
  way if one appears while it is running.
  `collMod: {expireAfterSeconds: "off"}` is deliberately allowed through, because
  it is what takes the expiry away.

  **The clustering itself is not refused**, and that is a decision rather than an
  oversight: the documents are ordinary documents, mirroring them is ordinary
  work, and the archive's copy is simply not clustered. So `listIndexes` through
  the router reports the fast tier's `_id_` index while the archive's copy has an
  ordinary one, and a `hint` on the clustered index is not answerable on the
  archive. That is the latitude a plain index already gets here.

- **A collection of a kind that cannot be mirrored stops the feeder, whenever it
  appears.** Capped, time series and queryable-encryption collections are
  refused at startup, and the reasons are in `internal/feeder/collkind.go`: a
  capped one removes its own oldest documents with nothing to announce them, so
  the archive tombstones its copies and discards the history it exists to keep.

  That check could only ever see collections that already existed. One created
  afterwards — which is every collection, in a deployment that leaves
  `feeder.namespaces` empty to mirror everything — was mirrored. Measured on a
  4KB capped collection filled with 400 documents: the fast tier kept the 18 it
  had room for and the archive held tombstones over the rest, marked deleted by
  a collection doing exactly what capped means.

  So the kind is checked again the first time an event for a namespace arrives,
  and the feeder stops the same way it would have refused to start, with the
  namespace named and the action in the log. **It stops rather than skipping the
  namespace**, and that is the same choice the startup check makes: a feeder that
  carried on would be mirroring less than its configuration says, and the router
  would answer reads of that namespace from an archive holding none of it. The
  cost is that one such collection, created anywhere a mirror-everything
  deployment can see, takes mirroring down until it is removed from scope.

  Views and time series collections do not reach this, and do not need to: what
  a change stream carries for a time series collection is its bucket collection,
  which nothing here mirrors, and a view has no documents of its own.

- **Neither tier's version is looked at again after the daemons start.** Both
  tiers must run the same MongoDB release and the same feature compatibility
  version, and every daemon refuses to start otherwise. That check runs once.

  So an upgrade of one tier while the daemons are running is not noticed. The
  pair they refused to start against is the pair they will happily go on serving
  until something restarts them. The exposure is the one that made the rule:
  everything the router needs of a tier goes to the fast one, and the archive
  only ever runs the client's own pipeline, so what breaks is a stage or operator
  newer than the archive, and it breaks with the archive's own error naming a
  stage the application has always used.

  It is deliberate rather than missed. Re-checking on a timer and halting would
  take the system down in the middle of a rolling upgrade, which is the one
  moment the versions are legitimately unequal. **Upgrade both tiers, then
  restart the daemons**, and they will tell you at once if the pair is wrong.

  **Upgrade the archive first.** The window where the two are unequal is
  unavoidable, and which way round it runs is not. The exposure above is one
  direction only: the archive runs the client's own pipeline, so what breaks is
  a stage or operator *newer than the archive*, and it breaks with the archive's
  own error naming a stage the application has always used. An archive ahead of
  the fast tier has no equivalent, because everything the router needs of a tier
  it asks of the fast one. The feeder points the same way: it reads the fast
  tier's change stream and writes what it finds into the archive, so a fast tier
  ahead of the archive is the order in which mirroring can meet something the
  other side will not take.

  So: archive, then fast tier, then restart the daemons, then raise the feature
  compatibility version on both.

  The ordering is still reasoned from the exposure: `testversion.sh` measures
  that exposure on a static pair, where a pipeline the archive cannot parse fails
  with the archive's own error, and it is one-directional. What had never been
  run was the window itself, and `verskew.sh` runs it now — a deployment serving
  traffic while the two tiers are a release apart, in both orders, against an
  equal pair as the control. MongoDB 8.0.26 and 7.0.37, 200 inserts, 100
  updates, 20 deletes and a read whose answer is on both sides of the boundary:
  every arm mirrored all 180 surviving documents, refused none, kept its feeder
  running and answered 182. So the window is survivable in either direction for
  mirroring and for reads, and which way round to run it is still decided by the
  exposure above rather than by anything measured here.

  The control is what makes that readable. The first run of this reported that
  an index created through the router never reached the archive and read as a
  release-gap finding; the equal pair did exactly the same, because a plain index
  is a decision about speed and the two tiers are allowed to differ. A check that
  fails on an equal pair is measuring something else.

- **The query cache drops an answer when a collection it depends on changes, and
  working out what it depends on is a best effort.** An entry is hung from every
  collection the answer was made of: the one the command named, every hop of a
  view chain under it, whatever those views join, and the collections a
  `$lookup`, `$unionWith` or `$graphLookup` in the pipeline reads.

  It used to be hung from the name in the command alone, which is not what a
  read is made of. Every invalidation this cache gets arrives under a
  collection: the router's own write path names it, and so does the fast tier's
  change stream. A read of a view names the view, so the two never met, and a
  cached read of a view outlived every write to the collection it reads.
  Measured with the cache on: a count through a view stayed at its old value
  after an insert into the collection underneath, until the learned TTL ran out.
  A `$lookup` was the same, hung from the collection the pipeline ran against
  and not from the one it joined.

  What is still best effort is a pipeline whose sub-pipelines nest past mongod's
  own depth limit, where the walk stops. That direction is the same one the
  completeness guard takes there, and the learned TTL is the bound on it. The
  older and larger exposure has not changed and is stated at the top of
  `querycache.go`: a writer this router cannot see is not seen at all, and the
  TTL is the whole of that.

- **A document whose age field holds an array is never evicted, and the fast
  tier grows by that much.** Everything about ageing a document assumes there is
  one date to read. MongoDB does not promise that: a range predicate on an array
  field matches when *any* element matches, so a document holding
  `[old, new]` is older than the eviction cutoff and newer than it at the same
  time.

  Measured before it was excluded, on four documents with one holding
  `[four hours ago, thirty seconds ago]`: the evictor removed it for the old
  element, and a read bounded at the completeness claim still matched it for the
  new one. The archive answered `1,2,3,9` and the router answered `1,2,3` — a
  short answer inside the range the claim promises, with no error on it.

  So the eviction filter excludes an array outright, which leaves those
  documents on the fast tier for ever. That is the same answer this rule already
  gives every other shape it cannot address: type bracketing means a cutoff on a
  date selects only documents whose field holds a date, so a missing, null or
  string age is invisible to every sweep already. An array of dates is the one
  thing bracketing lets through, because `$type` matches an element. The evictor
  counts them and says so at startup, separately from the others, because the
  reason is the opposite one: those cannot be selected at all, and these could be
  and must not be.

  The remedy is in the message and it is the application's: give each document
  one date in that field, or age the namespace by a field that holds one.

- **Collection DDL other than rename** is logged and ignored, never replayed.
  A drop is the one worth knowing: the fast tier's copy goes, the archive keeps
  its documents, and the completeness claim is withdrawn, so the collection
  still answers from the archive afterwards. That is the rule working rather
  than failing, because replaying a drop is exactly how one accident on the fast
  tier would destroy the archive. Removing archived documents is deliberately a
  separate act.
- **`terminate_auth` is a judgement call rather than a finished feature**, and it
  is off by default.

  What it has met, in order. Harness traffic first: a three hour soak of 490
  rounds through mongosh, and two shops writing at once for 19,253 commands.
  That run is what found the gate refusing fourteen out of fourteen ordinary
  diagnostic commands, because the authorisation table only ever judged commands
  that touch data while drivers and shells spend most of their commands
  elsewhere.

  And on 2026-09-06, **data nobody here wrote**: the whole pilot with it on, over
  MongoDB's published sample datasets adopted as an archive. 199 comparison
  questions with no disagreement, the nineteen cross-collection aggregations with
  the two documented refusals and no disagreement, and the write, change stream
  and transaction batteries clean. That closes the gap this entry used to name,
  which was that it had never met an application nobody wrote for this.

  The write battery reports one fewer section than the run beside it with
  proxied auth, and the reason is in the run rather than in the mode: the
  straddling-range section skips itself when no year has documents on both sides
  of the boundary, and the run before it had moved the boundary. Three checks,
  and they are reported as skipped rather than quietly absent.

  The security-critical half of it is an authorization gate. That gate is the
  only thing between a client and somebody else's data in that mode, it denies by
  default, and it has fifty six checks behind it in `testauthterm.sh`, including
  the ones that caught it allowing a `$lookup`, a `$merge` and an unacknowledged
  write it should have refused. A clean pilot says the mode serves an
  application correctly; it does not say the gate cannot be got round, and those
  are different claims.
