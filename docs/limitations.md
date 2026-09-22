# What is not handled yet

Every limitation known, with what each one costs and what closes it.

Part of [Thermocline](../README.md).

## What is not handled yet

- **Two feeders with different ids are not refused, and they double the work.**
  The lease stops two of the same daemon with the same id; two with different
  ids watching the same namespaces are not detected by anything, because nothing
  reads another feeder's position. Since 2026-09-21 the harm is bounded: the
  eviction counter floors at zero, so the state that used to destroy documents
  later cannot be reached. What is left is doubled mirroring, doubled oplog
  reading and two checkpoints, which is waste rather than loss. Detection would
  mean recording the namespace set on the lease and comparing across ids, and
  nothing does that yet.

- **A warm rule that ages by a date field needs an index on the archive, and
  the obvious one makes it worse.** The warmer reads the archive newest first,
  and for a `date_field` rule that sort is `{field: -1, _id: -1}`. Nothing
  copies an index to the archive, so unless somebody built one there mongod
  answers *every batch* by reading the whole collection and sorting it. Per
  batch, not per pass, which is what makes a warm cost the square of the
  collection rather than the size of the window.

  Measured on 200,003 archived documents with `batch_size: 200`, one pass, from
  the archive's own profiler:

  | indexes on the archive | plan | documents examined per batch |
  |---|---|---|
  | `_id` only | COLLSCAN with a sorting stage | 200,000 |
  | `{createdAt: 1}` | IXSCAN with a sorting stage | 19,039 |
  | `{createdAt: -1, _id: -1}` | IXSCAN, no sort | 100 |

  The middle row is the one worth knowing: an index on the field alone is the
  obvious fix and it is not enough, because the tiebreak on `_id` is not in it
  and the sort remains. With the compound index the archive spent 36ms across
  the profiled finds of a whole pass, against 16,750ms with none.

  A rule with no `date_field` ages by the ObjectId's own timestamp, so it sorts
  on `_id` and every collection indexes that. The warmer names the index to
  build, once per rule per pass.

- **A unique index the archive does not have costs writes, not just reads, and
  the cost is a collection scan of the archive.** Uniqueness is enforced across
  both tiers, so a write has to establish that no live document already holds
  its key. When the fast tier holds no such document the archive is asked, and
  nothing copies an index there, so that question is answered by scanning the
  tier built to be the big one.

  Measured on an archive copy of 200,003 documents, asking for a key nobody
  holds, which is the ordinary case for a new value: **38-40ms per write**
  against **1-10ms** with an index over the same field, and it grows with the
  archive rather than with the fast tier.

  It is reached by the writes that leave a claim with no owner, and those are
  ordinary ones: an upsert whose filter carries no `_id`, and an insert that
  lets mongod name the document. A claim with an owner is re-read by `_id`
  instead, which is an indexed lookup on both tiers.

  **Who is exposed is narrower than it sounds.** An adopted cluster becomes the
  archive, so it arrives carrying every index the application had, and those
  keep answering. The gap is an index created *after* adoption: that one is
  built on the fast tier and the archive never gets it, which is the moment the
  router warns.

  Every router start and `cline preflight` name the namespaces in this state,
  not only the moment the index is created, because that line is said once to
  whoever was reading the log that minute.

  The answer is correct either way, and `test/testkeys.sh` pins that: the
  duplicate is refused with E11000 exactly as one cluster would refuse it. What
  is missing is only the index. Build the same unique index on the archive for
  any tiered collection written by upserts or by inserts without an `_id`; the
  router says so when the index is created, naming the measurement.

- **A fast tier rebuilt from the archive comes back without the indexes the
  application declared, and for a unique index that is a constraint nobody is
  enforcing.** Nothing copies an index to the archive, on purpose: an index
  describes the whole collection and carrying every one of them there gives back
  most of what tiering buys. So the archive can rebuild a collection, its
  options and its documents, and not its indexes. Uniqueness across the tiers is
  enforced from the fast tier's own index listing, so once the listing is gone
  the constraint is enforced by neither tier.

  Measured by `test/testrestore.sh`: a unique index created through the router,
  200 keys claimed in the registry, the fast tier lost and rebuilt the way
  `docs/operations.md` prescribes, and the registry restored from its own dump.
  An insert duplicating a value an archived document held was then accepted, and
  the archive ended with two documents carrying one key.

  Two things narrow it. A router says "the key registry holds claims for a
  unique index no tier has" at every start and in `cline preflight`, naming the
  index, because those rows are the only thing that remembers the constraint.
  And the backup list in `docs/operations.md` now names the index definitions as
  something to take, which is what makes the recreation a step rather than an
  archaeology exercise. Recreating the index through the router walks both tiers
  and reports the duplicates that got in.

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

  It cannot be refused from here (the router is not in that path by
  definition), and it cannot be told apart from an application reusing an `_id`
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

- **A document the archive refuses stays on the fast tier, and reads of that
  namespace are refused until it is resolved.** The feeder gives up on a document
  the archive's unique index refuses, because no retry of it can ever succeed,
  and says so at ERROR. The evictor then never removes it, which is the half
  that works: the document is not lost.

  What reads do afterwards used to be the easy half to miss. The archive is the
  superset by assumption, so a read the fast tier cannot claim is answered
  there, and there is the one place this document is not. Measured with a unique
  index on the archive alone and two documents sharing its value written at the
  fast tier: the archive refused one, an eviction sweep correctly kept it and
  removed the other two, and a client then counted two where three documents
  exist. A lookup by `_id` still found it, because a point read asks both tiers,
  which makes the collection answer differently depending on how it is asked.

  That was a wrong answer rather than a limitation, so it is refused now. The
  feeder records the document in `unmirrored` in the bookkeeping database, and a
  read that would be answered from the archive is refused instead, naming the
  namespace, what is wrong and what to do. A log line cannot be read by a router
  and a counter cannot name a namespace, which is why recording it was the whole
  of the work. Measured on the same three documents: `countDocuments`, `find`,
  `distinct` and an aggregation are refused, `findOne({_id: ...})` still returns
  the document, and `updateMany` still reaches all three, because a write is
  applied at the fast tier after the archive's matches are copied up.

  A transaction is not affected, and the reason is worth following: a
  transactional read never goes to the archive at all, because reading one tier
  inside the caller's snapshot and the other outside it is not a transaction any
  more. The documents are moved to the tier the transaction is already reading
  and it is replayed, so the answer comes from the fast tier, which is precisely
  the tier holding the document the archive lacks. Measured with the state
  standing: a fall-through read refused outside a transaction answered inside
  one, and the answer was the right one.

  There are two ways out and the refusal names both. Resolving the duplicate is
  the real one. The other is warming: a fast tier that holds the namespace in
  full never falls through, so the archive is never asked and the refusal never
  applies. Measured with the state standing and a complete claim recorded, a
  bounded read answered 16 and an unbounded one 3,472, neither refused. That is
  also the proof the guard does not over-refuse, since it sits only on the path
  that asks the archive.

  `cline backfill` meets this state too, and until 2026-09-17 it lied about it.
  The copy is unordered, so the archive applies what it can and refuses the
  rest; the driver reports that as an error, and the command turned the whole
  namespace into "could not be copied", counted nothing, and then printed its
  closing line, which says what was on the fast tier is in the archive now.
  Measured on three documents with one refused: `Copied 0`, and that sentence.
  It now copies what it can, names the documents the archive refused with the
  index and the key, says the namespace is still short by them and that the
  router refuses those reads, and keeps the old closing line only for the runs
  where it is true.

  **It lifts by itself.** Nobody runs a command here after resolving a duplicate,
  so the write that finally reaches the archive is what clears the row: measured,
  reads answered in full again within seconds of the value being changed, and the
  feeder said so. Pinned by `testkeys.sh`.

  What is still true is that there is no repair for the duplicate itself: the
  value belongs to another live document in the archive and nothing here can
  choose which should keep it.
  `thermocline_daemon_documents_total{outcome="unmirrorable"}` counts them and
  has a page rule, and `cline preflight` lists them with the index and the key.
  `cline keys --rebuild` does not: it reads the fast tier's unique indexes, and in
  the shape this state arrives in, an adoption whose archive carries an index the
  fast tier's copy does not, it answers that there is nothing to enforce.

- **Two field names are reserved in a tiered collection, and a write that uses
  one is refused.** `_tcDeletedAt` is how an archived document is marked as one
  the application deleted, and every read of the archive carries a predicate
  excluding the documents that hold it. `_tcBodyDropped` marks a tombstone that
  could not keep the document it stood for. Both are ordinary field names that
  mongod accepts from anybody.

  Measured before they were refused, on an insert of `{_id: "x", n: 5000,
  _tcDeletedAt: <a date>}` into a tiered collection: both tiers took it, and
  `countDocuments({n: 5000})` answered **0** through the router where the same
  document in an untiered collection answered **1**, with `ok` on both. A lookup
  by `_id` still returned it, because a point read asks the fast tier too, so one
  collection answered two different ways about one document. The read is the
  smaller half of it: a fast tier rebuilt from that archive, which is the
  documented recovery, does not have the document at all, because its copy reads
  as deleted.

  Every write command that can leave one is refused: an insert, a replacement
  document, `findAndModify`, a pipeline update, and an update by **any**
  operator that names the field. Any, rather than a list of the likely ones,
  because the predicate that hides a document is `{$exists: false}` and a field
  of any type hides it: measured against a first version of this guard that read
  `$set`, `$setOnInsert` and `$addFields`, both `$currentDate` and `$min` wrote
  the mark and were accepted. `$rename` is read through its value, where its
  target is named, and `$unset` is deliberately allowed, because it is how the
  field is taken off a document that has one. A pipeline writes documents too, and both of its paths
  refuse as well, by two different readings: a tiered `$out` is performed by the
  router, so each document it writes is read as it passes, and a `$merge` whose
  input the fast tier can answer alone is forwarded whole for mongod to perform,
  so what can be read there is the pipeline's stages. The refusal names the
  field and says to rename it. Three things are deliberately not refused, and
  they are checked: an ordinary write, a *nested* field of the same name, which
  no predicate here reads, and the same field in a collection nothing tiers,
  where it means nothing at all.

  The hazard was known before the guard: the feeder warns once per collection
  when it mirrors a document carrying one of these names, and the warning says
  what it costs in the same words this entry does. A warning is not a refusal,
  which is what was missing, but it is why an operator may have seen this
  already. It still has work to do, and it is now exactly the right work: it
  fires for the documents the guard cannot reach, which are the ones written
  straight at the fast tier and the ones an adoption arrived with.

  The guard is preventive, and an adoption is the shape this product exists for:
  the cluster arrives with years of documents in it, written by an application
  that never heard of this system, and refusing new writes does nothing for a
  document already there. `cline preflight` counts them, per tiered namespace,
  on the fast tier: nothing here writes either field there, so a fast tier
  document carrying one is the application's own. It is bounded by `--count-for`
  like the claim check, because the field is not indexed and this is a scan,
  which is also why it is not on a router's startup path.

  **A batch carrying one is refused whole**, where a batch carrying a duplicate
  key is not. Measured on both, through the router, with `ordered: false`: an
  insert of three documents where one duplicates an archived `_id` wrote the
  other two and reported the one failure, the way mongod does; the same batch
  with one document carrying `_tcDeletedAt` wrote none of the three.

  The difference is deliberate now that it has been measured, and the reason is
  what the two conditions are. A duplicate key is an ordinary outcome of
  ordinary data, two people choosing one email address, and an application that
  loads a thousand documents expects the other nine hundred and ninety-nine to
  land. A reserved field name is not a data condition at all: it is the
  application's schema colliding with this system's, it will recur on every
  batch until the field is renamed, and a partial success would let a bulk load
  report progress while quietly dropping the same document every time.

  **One path is not covered**, and it follows from that split: a `$merge` into a
  tiered collection, of documents that already carry the field, taken from a
  collection where carrying it is allowed because nothing tiers it. No stage
  mentions the field, mongod performs the write, and nothing here sees the
  documents. Closing it would mean the router performing every `$merge` into a
  tiered collection rather than forwarding the ones the fast tier can answer,
  which is a large cost for a narrow case.

  **A second one has the same shape, one level down.** A replacing stage says
  what the whole document becomes, as an expression rather than as a list of
  field names:

  ```
  {$replaceWith: {$mergeObjects: ["$$ROOT", {_tcDeletedAt: "$$NOW"}]}}   read
  {$replaceRoot: {newRoot: {_id: "$_id", _tcDeletedAt: "$$NOW"}}}        read
  {$replaceWith: {$setField: {field: "_tcDeletedAt", input: "$$ROOT", ...}}}  read
  {$replaceWith: "$sub"}                                                 not read
  {$replaceWith: {$arrayToObject: "$pairs"}}                             not read
  ```

  A literal document shows its keys, `$mergeObjects` shows the keys of whichever
  arguments are literal, and `$setField` names its field in a value. An
  expression that builds keys from data does not show them to anything but
  mongod, so a pipeline promoting a subdocument that happens to hold the field,
  or building one from an array of pairs, goes through. It is the same trade as
  the `$merge` above: reading the stages is what makes the guard cheap, and
  refusing every expression that can evaluate to a document would refuse most of
  the pipelines that legitimately write one.

  It is not silent, though, and that is the difference between a path nothing
  covers and a path nothing reports. Measured on exactly that shape, two
  documents carrying the field merged in from an untiered collection: the router
  accepted it and the feeder named it as it mirrored, once for the collection,
  with the effect in the line. The guard catches what it can see and the feeder
  catches what reaches the archive.

  What is not closed is the door every out-of-band write uses. A document
  carrying one of these names, written straight at the fast tier's mongod, is
  mirrored and the archive's copy reads as deleted, the same way a write made
  there can already destroy archived documents. That is the limitation two
  entries down, and the protection is the same operational one.

- **A completeness claim records which field it was measured in, because
  changing that field re-reads every claim in a dimension it was never made in.**
  "The fast tier holds every live document newer than this instant" is only a
  statement once you know what newer means, and it means the field the rules
  tier by. The row carried the instant alone.

  Measured on twenty orders whose `shippedAt` is thirty days after their
  `createdAt`, claimed complete from 15 January in `createdAt`: with the rule's
  `date_field` changed to `shippedAt`, the router answered 6 where the archive
  holds 20, and reported success. One line in a config file.

  A claim measured in a field the namespace no longer ages by is withdrawn
  rather than reinterpreted, so reads fall through to the archive, which is the
  answer they would have had before anybody claimed anything. The router says so
  once per namespace, naming both fields. Rows written before the field was
  recorded carry none, and are used exactly as they were: refusing every claim on
  an upgrade would be a worse failure than the one this prevents, and the next
  write stamps them.

- **Taking a namespace out of `feeder.namespaces` hides everything only the
  archive holds.** The mirror image of tiering one, and the archive is the side
  with the years of it. A collection this system does not tier is the fast
  tier's alone by definition, so the archive's copy stops being read the moment
  the line goes.

  Measured on fifty documents with the fast tier holding the most recent ten:
  tiered, a client counted fifty and read the oldest one; untiered and the router
  restarted, it counted ten and the oldest was missing. Nothing was said
  anywhere, because the stranded report is the all-or-nothing form of this
  question and skips a collection the fast tier has: having some of a collection
  and having it are not the same thing.

  The router now names it with both counts, as `shop.orders (10 of 50)`. It
  cannot refuse the configuration, because a collection the fast tier holds in
  full and the archive also has is a perfectly ordinary state to untier from.

- **Tiering a collection that already has data hides it until the archive has a
  copy.** Adding a namespace to `feeder.namespaces` is an ordinary operation and
  this is what it does: the feeder is a change stream, so it mirrors what
  happens next, and what was already on the fast tier it never sees. A tiered
  namespace with no completeness claim is read from the archive, so the
  documents that were there before the edit stop being returned.

  Measured on twenty documents living on the fast tier untiered, then tiered
  with one more written: the fast tier held twenty one, the archive held the
  one, and a client read the one. The other twenty were invisible from the
  moment somebody edited the config.

  The router names the collection at its next start, and `cline backfill --ns`
  is the fix. Do it with the feeder running, which is the order with no window
  in it: the feeder carries what happens next while the copy brings what was
  already there. Marking deletes is held while a feeder publishes, and nothing
  needs marking in this case anyway.

- **A delete made while nothing was mirroring cannot be recovered by copying the
  fast tier, and `cline backfill` is what closes it.** The feeder exits when the
  fast tier's oplog has rolled past its checkpoint, and the remedy named there
  is to copy the affected namespaces into the archive. Measured on ten mirrored
  documents with the feeder stopped, one update, one insert and one delete, and
  the remedy then followed by hand: the update came back right, the insert
  arrived, and the deleted document stayed live in the archive and was served to
  a client. The router counted eleven where the application had left ten.

  A delete leaves nothing on the fast tier to copy, so copying cannot know it
  happened. The completeness claim can: inside the range the fast tier claims to
  hold in full, a document the archive holds live and the fast tier does not is
  one that was deleted. `cline backfill` does both halves, reports before it
  writes, and refuses twice on the way: while a feeder is still publishing,
  because it would be closing a window something is still writing into; and when
  the claim is one the fast tier cannot support, because marking on the strength
  of a wrong claim tombstones the archive rather than recovering it.

  What it cannot do is invent the claim. A namespace nothing has claimed gets
  the copy and no marking, and says so, because without a claim there is no
  range in which absence means anything.

- **Nothing records which archive a deployment has been using.** `cold.uri` can
  be repointed at a different cluster by a typo, a config copied from another
  environment, or a DNS name that now resolves somewhere else, and the router
  will serve whatever that cluster happens to hold. Measured on two archives
  with a collection of the same name in each: ten documents mirrored into the
  first, then the URI changed to the second. A client counted four and read
  "another environment" out of a collection its own archive holds ten real
  documents in.

  The checkpoint is the nearest thing to a record of the pairing: it is written
  by this deployment, into this archive, under this feeder's id. A fast tier
  holding tiered data against an archive with no checkpoint for that id is a
  pair that has not been working together, and the router says so at startup,
  naming the id and the collections. Three things put a deployment in that state
  and the line names all three: the feeder has never run, `feeder.id` changed, or
  `cold.uri` points somewhere else.

  It is a warning and not a refusal, because the first of those three is an
  ordinary first start.

- **`feeder.namespaces` empty tiers everything, and several checks read the list
  rather than asking what that means.** The configuration says so plainly and
  `RouterConfig.Manages` honours it by answering true for every namespace. Nine
  other places iterate the list directly, and for them empty meant nothing
  rather than everything, so the deployment that tiers the most was the one
  those checks did not run for at all.

  Two of them cost more than a missing report. The fast tier was left without
  the archive's unique indexes, because the startup build iterates the list; and
  `dbHash` over such a database **answered**
  `d41d8cd98f00b204e9800998ecf8427e`, the hash of nothing, for a database the
  archive holds four documents in. The guard that exists to refuse exactly that
  found no tiered collection to be careful about. `testeverything.sh` is the
  suite for this shape, which had none before.

  The archive is what "everything" means, because it is the superset, and that
  is what the shared answer reads now.

  The commands an operator runs had it too, and the worst of them was
  `cline preflight`: its header said "Checking 0 tiered namespace(s)" and
  `verifyClaims` looked at no claim at all, which is the one failure that
  function exists to catch, since a claim not checked is a claim that stands. It
  now verifies every recorded claim, because a claim exists only for a namespace
  something has tiered. `cline indexes` and `cline keys` refused outright, which
  left the two commands the adoption guide sends you to unusable on the
  deployment that tiers the most; both ask the archive now, as `cline rollback`
  and `cline backfill` already did.

- **With authentication proxied, a client's other connections are each refused
  once when the primary moves.** Every connection has an upstream of its own in
  that mode, and the refusal that ends one says nothing to the rest: they stay
  pointed at the member that stepped down until each is refused in turn. A
  driver retries a write once, so that retry can be spent on a sibling that was
  always going to fail. Measured with two connections made by one client before
  a forced stepDown, with the driver's own retries on: five runs out of five
  left one write applied where two were sent.

  Fixing it was tried and cost more than it bought, which is why this is written
  down rather than closed. Ending a connection whose upstream is provably no
  longer the primary, before its command is sent, does work: three runs out of
  three. It works because it also ends the driver's *monitoring* connection and
  forces a topology rescan. Restrict it to the commands a demoted member would
  actually refuse, so monitoring keeps working, and it goes back to zero runs
  out of ten. Leave it unrestricted and the monitor goes with it: a write that
  should pause for an election comes back `MongoServerSelectionError: connection
  <monitor> closed`, which undoes the wait in the entry above.

  Nothing is lost either way, and `testfailover.sh` checks the part that
  matters: whichever way the sibling goes, no write is applied twice.
  `router.terminate_auth` does not have this at all, because the connections are
  the router's and a re-resolution empties the pool for the old member.

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

  It is said at startup and by `cline preflight`, which is where the mirror image
  of it is already said, and not on the read path: the read that goes short is
  the cheapest possible one and putting a catalog lookup on it would cost every
  deployment for a state almost none of them are in. So a drop that happens
  while a router runs is named at its next start rather than at once, and the
  first run of this named two namespaces left behind by the test suites that
  nobody had noticed.

- **A capped archive collection throws away the history, and the advice for it
  used to be the worst possible advice.** `convertToCapped` run straight at the
  archive is out of band the way a write there is, and the feeder refuses to
  start against such a pair, which is the half that works while it restarts.

  What a running router said was that the two tiers differ in an option, with
  the action "make the two copies the same, or know why they differ". Making
  them the same here means capping the fast tier too, which would destroy the
  recent half as well.

  Measured on eighty mirrored documents: the conversion itself left sixteen and
  the rest were gone, a later insert through the router mirrored into the capped
  collection and rolled another away, and an update that grew a document was
  applied on both tiers without complaint, because mongod stopped refusing those
  in 4.2. Mirroring never errored, and nothing but that one line named it. It
  has its own line now, saying that the archive is discarding what it exists to
  keep and that the remedy is to un-cap the archive's copy rather than to match
  the fast tier to it.

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

- **`filemd5` is refused for a GridFS file the fast tier does not hold whole.**
  A bucket stores a file as a row in `<root>.files` and a row per chunk in
  `<root>.chunks`, and a collection of file chunks that only grows and is read
  rarely is exactly what this system tiers. `filemd5` hashes one file's chunks
  in order, on whichever tier runs it.

  Measured on a four chunk file with two chunks evicted and both still live in
  the archive: `3b5d3c7d...` over 4 chunks while it was all resident, and
  `70bc8f4b...` over 2 chunks afterwards, `ok` on both. An application verifying
  a file it had stored would read that as the file being corrupt, which is the
  worst answer a verification command can give and the same one `dbHash` is
  refused for.

  So it is refused on the same terms, by asking the question the tiers can
  answer: whether the archive holds chunks of this file the fast tier does not.
  It is refused when that question cannot be asked at all, too: with the archive
  unreachable, forwarding would hash whatever the fast tier holds and call it the
  file's, and unlike a `find` there is no archive read afterwards to fail and say
  so.
  Two counts bounded by `files_id`, which every bucket indexes, on a command an
  application sends rarely. A file whose chunks are all resident still answers,
  and a bucket nothing tiers is untouched.

  Nothing else about GridFS is special here, and that is worth saying because
  nothing had looked: the chunks are ordinary documents, mirroring them is
  ordinary work, and a read of a file whose chunks span both tiers falls through
  to the archive and gets all of them.

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
  claim covers everything, where it is the collection's hash and equals the
  archive's exactly.

  `validate` is not corrected, deliberately. It is a physical check of one
  server's storage, so its `nrecords` is the fast tier's the way `storageSize`
  is, and correcting one field would leave it inconsistent with the index key
  counts in the same reply that mongod compares it against. Read it as a check
  of the fast tier, and run it against the archive separately.

  `dbCheck` and `validateDBMetadata` are the same kind of thing and get the same
  answer: each is a statement about the storage and catalogue of the server it
  runs on, they are forwarded untouched, and what they describe is the fast
  tier. Run them against the archive separately too. They are named here because
  the difference between a command that is wrong across two tiers and one that
  is simply about one of them is not obvious from the outside, and `filemd5`
  above shows which side of that line the answer can fall on.

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
  reaches inside a view's definition, which the router cannot rewrite.
  Answering it there would join deleted documents, which is the defect in
  *Joined collections are read elsewhere* with the sign flipped.

  **A `$out` into a tiered collection that reads through a view is refused for
  the same reason**, and that refusal is the sharper end of it: the reading half
  of a tiered `$out` is performed by the router rather than forwarded, so a short
  answer there is not something a client reads and re-runs, it is something on
  disk. Measured before it was refused: a pipeline reading a view over a tiered
  collection the fast tier was short of wrote one document where the archive
  holds two. A `$lookup` *naming* a view is not refused, that one resolves to
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
  names both, and how to rule one out: name every router in the connection
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

- **The router answers as a mongos and tells the truth about the replica set
  behind it, so anything that branches on the handshake is looking at a
  deployment that does not exist.** The handshake is deliberate: a driver uses
  `hello` for topology discovery, and forwarding the fast tier's real reply lets
  it read the `hosts` array, connect straight to those members and never send
  the router another byte. So `hello` answers `{msg: "isdbgrid",
  isWritablePrimary: true}` with every membership field dropped. Nothing else
  was changed to match, and the two halves disagree in both directions.

  Measured through a router in front of rsHot, authenticated as a user with
  `clusterMonitor` and `clusterManager`:

  ```
  hello                 msg=isdbgrid, isWritablePrimary=true
  isdbgrid              CommandNotFound
  listShards            CommandNotFound
  balancerStatus        CommandNotFound
  getShardMap           ShardingStateNotInitialized
  flushRouterConfig     ShardingStateNotInitialized
  config.shards         0 documents
  replSetGetStatus      ok:1, set=rsHot, 3 members, first 172.24.0.5:27017
  replSetGetConfig      ok:1, set=rsHot, 3 members
  serverStatus          wiredTiger present, repl.setName=rsHot, 3 hosts
  local.oplog.rs        readable
  replSetStepDown       ok:1, and the primary moved
  ```

  A real mongos answers the first block and refuses the second. The router does
  the opposite of both.

  What it costs. A monitoring agent that reads `msg: "isdbgrid"` and then
  collects a mongos will collect nothing: `sh.status()` against the router fails
  outright. An agent that ignores the handshake and asks `replSetGetStatus` gets
  a true answer about the fast tier, and with it the member addresses the
  handshake exists to withhold, which is the bypass the handshake is for. And a
  client with `clusterManager` can move the fast tier's primary through the one
  address the application uses. That is a privilege an application should not
  have, but nothing here refuses it.

  What it does not cost. No application read or write goes through any of these.
  A driver never sends them, and the one thing a driver does branch on for a
  mongos, the `recoveryToken` a sharded commit carries, is absent from a replica
  set's commit reply in both directions, so a commit retry has nothing to carry
  and goes back to the same address.

  Making the second block refuse the way a mongos does would take away an
  operator's only view of the tier behind the one address they were given, so
  the fix is not obviously the honest one. Written down rather than changed.

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
  of the command (one on the way back, one before it runs, because a pipeline
  can end in `$out`), and the aggregation is the easier of the two to reach:
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
  project makes on purpose: a refusal is a limitation and a wrong answer is a
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

- **Write blocking on the archive stops mirroring, and it used to read as a
  change stream problem.** MongoDB 6.0 added `setUserWriteBlockMode`, and
  migration tooling turns it on. Pointed at the archive it refuses every write
  this system makes, with code 371.

  The behaviour is right and is unchanged: the feeder keeps retrying, because
  unlike a document the archive will never take, this one succeeds the moment
  the block is lifted. Measured: twenty reopens in twenty seconds while it was
  held, mirroring catching up on its own afterwards, and the write made during
  the block reaching the archive.

  What was wrong was what it said. Every one of those lines read "change stream
  ended, reopening", which is a fast tier framing of an archive problem, and an
  operator following it goes to the wrong cluster. It is named now, once until a
  write lands rather than once a second, with the command that turns it off, and
  the resume is announced too. A feeder *starting* against a blocked archive was
  already honest: it exits naming `UserWritesBlocked` and the operation, because
  taking its lease is itself a write.

- **A read waits for mirroring to catch up, and answers anyway when it does
  not.** This is the one place the system knowingly returns an answer it cannot
  vouch for, and it is a deliberate trade rather than an oversight: the
  alternative is to fail reads whenever the feeder is behind, which turns a
  feeder that is merely slow into an outage.

  A read that falls through to the archive waits for the feeder to reach the
  instant the read began, bounded by `router.archive_wait`, and by half the
  caller's own `maxTimeMS` when it set one, so that neither half can starve the
  other. Past that bound the archive is asked as it stands.

  Measured with the feeder stopped and a document written through the router a
  moment before: `countDocuments` on that `_id` answered **0** after 208ms for a
  document the application had just written successfully. The effect is stated
  in the warning it logs: the answer may miss a very recent write, or include a
  very recently deleted document.

  It is counted and it pages. `thermocline_stale_reads_total` above zero is a
  page-level rule in `docs/monitoring.md`. A deployment seeing them has a feeder
  that cannot keep up, and the answer is to fix that rather than to raise
  `archive_wait`, which only trades the stale answer for a slow one.

  **What the runs here actually recorded**, because this paragraph claimed they
  recorded none and one of them did not. The 691-round pilot soak: none. The
  four-hour two-application run of 2026-09-17: **31**, all inside one second,
  with 0 violations, written up in the commit that recorded that run. The
  four-hour run of 2026-09-18, complete: **86**, and **nine** violations of
  read-your-own-writes, every one of them at a single instant twenty minutes in.
  Across the remaining three hours and fifty minutes, 1.1 million further
  operations produced none of either. Both bursts land in windows when this host
  was also running builds, benchmarks and mutation tests, which is item 4 of
  `docs/production-readiness.md` and not a property of the product.

  **And the bar a fall-through read waits on is raised by traffic that has
  nothing to do with the tiered collections.** A read clears two bars, and the
  binding one compares the feeder's published cluster time against the newest
  `operationTime` this router has handed a client. `operationTime` on a reply is
  the node's own latest logical time, not that command's position, so it
  advances with *any* write anywhere on the fast tier cluster, including to
  databases nothing tiers. The feeder's position advances only when it processes
  an event for a namespace it mirrors.

  That property is measured, not assumed. A `find` against a database nothing
  was writing, on the fast tier of the 2026-09-18 run, reported `operationTime`
  `t=1789753961` and three seconds later `t=1789753964`: three seconds of
  cluster time advanced by two other databases' writes, on a collection that had
  not changed. On a busy cluster this bar is therefore always close to "now",
  and the feeder has to be at the head of the oplog to clear it.

  So a fast tier shared with unrelated write traffic pushes that bar ahead
  continuously, and a fall-through read then waits for mirrored traffic to carry
  the feeder past it. With steady mirrored writes that takes milliseconds. With
  sparse mirrored writes beside a busy neighbour it can reach `archive_wait`,
  and the read is answered stale while the feeder is perfectly healthy.

  What is measured and what is inferred, kept apart: the eleven stale reads of
  2026-09-18 are measured, `mirroring_lag_seconds` reading 24ms three seconds
  before them and 14ms five seconds after is measured, and `operationTime`
  advancing on an untouched database is measured. What remains inferred is only
  that the cluster-time bar is the one that failed on that occasion, which
  follows from the code rather than from a reading, because nothing exported it
  at the time.
  `thermocline_mirroring_cluster_time_behind_seconds` and the router's
  "which bar the freshness wait gave up on" line exist to settle it the next
  time it happens, and until one of them has caught an event this paragraph is a
  mechanism and not a diagnosis.

  **What could be done about it, and why it is not done here.** The bar could be
  narrowed to the newest `operationTime` seen on replies for *tiered*
  namespaces, which is all a fall-through read of a tiered collection actually
  has to cover. That would stop a busy neighbour holding the barrier up.

  It costs something real, though, and the cost is only visible once you ask
  what the present bar covers by accident. Because `operationTime` is
  cluster-wide, today's bar also covers a write made **straight to mongod**,
  bypassing the router: the feeder mirrors it, and a fall-through read waits for
  that too. Narrowing to tiered replies gives that up, and a cutover with both
  kinds of writer in flight is exactly when somebody would notice. So this is a
  decision about which of two guarantees matters more on a shared cluster, not a
  defect to be fixed quietly, and it is written here rather than made.

  Neither figure was measured on a quiet machine. Both runs shared this host
  with builds, benchmarks and test loops, and the 2026-09-17 commit says in as
  many words that interference from the system under test cannot be ruled out
  here. So the honest statement is the narrow one: **zero is the target and the
  alert threshold, and it has not yet been demonstrated over four hours on a
  machine doing nothing else.** Item 4 of `docs/production-readiness.md` is that
  measurement, and until it is made this limitation is bounded by
  `archive_wait` and by the counter, not by evidence that it stays at zero.

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
  false refusal. A collection dropped and rebuilt out of band (resetting an
  environment, restoring a fixture) does not have the old collection's unique
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
  appears**, and the router refuses every way of creating one through it. Seven
  shapes: capped, time series, a view, a clustered collection with a TTL, one
  with `encryptedFields`, a name of the form `enxcol_`, and `convertToCapped` or
  `cloneCollectionAsCapped` onto a tiered name.

  The `enxcol_` name was the seventh and was not refused until 2026-09-16, found
  by tabulating what the feeder refuses to mirror against what the router
  refuses to create. Measured: `create` on a tiered `enxcol_` name was accepted,
  one insert into it stopped the feeder with every namespace behind it, and a
  document written to a different tiered collection afterwards never reached the
  archive. A rename is the other door to it, and the one that matters more,
  because a rename is the single piece of DDL this system replays into the
  archive. Both are refused now.
 Capped, time series and queryable-encryption collections are
  refused at startup, and the reasons are in `internal/feeder/collkind.go`: a
  capped one removes its own oldest documents with nothing to announce them, so
  the archive tombstones its copies and discards the history it exists to keep.

  That check could only ever see collections that already existed. One created
  afterwards (which is every collection, in a deployment that leaves
  `feeder.namespaces` empty to mirror everything) was mirrored. Measured on a
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
  run was the window itself, and `verskew.sh` runs it now: a deployment serving
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
  new one. The archive answered `1,2,3,9` and the router answered `1,2,3`, a
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
- **`terminate_auth` is off, and that is the shipped configuration.** Decided on
  2026-09-16 rather than left as a default nobody had ruled on. What proxied
  authentication costs is in `docs/operations.md` under the supported shape: one
  upstream connection per client, read preference not honoured, and a command in
  flight during a step-down failing rather than blocking. Those are the terms of
  the decision, not defects.

  The mode stays in the product, tested and **not supported**. What would make it
  supported is a review of its authorisation gate by somebody who did not write
  it, and not more traffic through it. The rest of this entry is what it has met,
  which is considerable and is still not that review.

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
