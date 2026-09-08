# What is not handled yet

Every limitation known, with what each one costs and what closes it.

Part of [Thermocline](../README.md).

## What is not handled yet

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
