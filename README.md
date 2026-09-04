# Thermocline

Storage tiering for MongoDB. Run a small fast cluster holding what people
actually use, and a big cheap cluster holding everything, behind one connection
string.

**Status: early, but all four components work. Not production tested.**

It is tested against real clusters rather than mocks: 34 suites and about a
thousand checks against two three-member replica sets, and a pilot that adopts
MongoDB's published sample datasets and compares every answer the router gives
against the archive it came from. See `test/README.md` and `test/pilot`. The CI
badge only means it compiles; a runner has no cluster to test tiering with.

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

**Applications delete normally. The feeder never issues a delete against the
archive.** Those are two different statements and only the second one is a
restriction, so it is worth being exact about which.

Your application deletes a document and it is gone: gone from the fast tier,
gone from every answer the router gives, immediately. Nothing about deleting
changes. What changes is what happens *behind* the router. The feeder mirrors
inserts, updates and replaces into the archive as upserts, and mirrors a delete
as a **mark** on the archived copy, `_tcDeletedAt`, rather than as a removal.
The router filters marked documents out of every read, which is why the client
cannot tell.

Measured, on a shop with 3,081 orders in the archive of which the application
had deleted 40: the router answered 3,041, the number the application expected,
while the archive still physically held all 3,081.

**Why not just delete it there too.** Because then one accident on the fast tier
destroys the only complete copy. Run `deleteMany({})` and then `drop()` on the
fast tier and the archive still holds every document, which is the property that
makes the fast tier disposable and the whole shape worth having. There is a
test you can run yourself in [docs/testing.md](docs/testing.md).

Two things follow, and neither is a surprise if you know about them. Storage in
the archive grows with deletes as well as with writes, so
`feeder.tombstone_retention` gives a marked copy an end date and mongod's TTL
monitor erases it. And a client reading the archive **directly**, bypassing the
router, does see the marked documents: that is what `cline rollback` counts and
what `--clear` removes before you repoint anything at it.

## Installing it

One binary, `cline`, which is every component:

```
make build
cp cline.example.yaml cline.yaml     # then edit the two URIs and nothing else yet
```

`cline.example.yaml` documents every setting beside the setting itself, and the
two URIs at the top are the only ones you must change. Both tiers have to be
replica sets on the same MongoDB release; the daemons refuse otherwise, and say
why.

## Using it

Four long-running components. Start the feeder first and let it catch up, then
the router; the warmer and the evictor are safe to leave until the fast tier has
something in it. [docs/adoption.md](docs/adoption.md) is the numbered order for
a cluster that is already serving traffic, and it matters.

```
cline feeder  --config cline.yaml     # mirror writes from the fast tier into the archive
cline router  --config cline.yaml     # serve both tiers on one address
cline warmer  --config cline.yaml     # fill the fast tier from the archive
cline evictor --config cline.yaml     # remove documents the archive already holds
```

Point the application's connection string at `router.listen` and nothing in the
application changes.

Then the commands you reach for while it runs:

```
cline watermark --config cline.yaml               what the fast tier claims to hold
cline indexes   --config cline.yaml               where the two tiers' indexes differ
cline keys      --config cline.yaml [--rebuild]   unique keys that must survive eviction
cline cache     --config cline.yaml               which queries would be worth caching
cline pause     --config cline.yaml               stop the evictor, while restoring the archive
cline resume    --config cline.yaml               let it remove again
cline rollback  --config cline.yaml [--clear]     what stands between you and reading the archive directly
```

`cline --help` has the flags. Every one of them takes `--config`, because every
one of them needs both tiers.

## What the configuration refuses, and why

Four rules a deployment has to satisfy. Each is refused at startup with the
reason, rather than left to be discovered.

### A key the config does not know is an error

Every daemon refuses to start on a setting it cannot place, naming the line:

```
error: parse config: yaml: unmarshal errors:
  line 311: field terminate-auth not found in type config.RouterConfig
```

This used to be a silence, and a silently ignored setting is the worst shape a
configuration mistake can take. Measured on one file: `terminate-auth: true`,
with a hyphen where the key has an underscore, loaded as `false`. The router
proxied authentication while the operator believed it was terminating it and
checking privileges itself. Nothing failed, nothing was logged, and the setting
simply did not exist. A misspelt `tombstone_retention` keeps deleted documents
for ever; a misspelt `claim_grace` gives no grace.

The cost of refusing is a restart with an error naming the line. The cost of the
silence is a setting you think you have.

### Two settings that have to agree

`evictor.feeder_id` names the feeder whose mirroring lag the evictor reads, and
the evictor will not remove anything while that feeder is behind. Leave it out
and it follows `feeder.id`, which is what you want when every daemon reads the
one file.

It used to default to the literal `default` instead, so a deployment that named
its feeder anything else pointed the gate at a checkpoint nothing writes. The
lag then reads as unknown, which is the safe direction — the evictor pauses
rather than removing — so nothing was lost and nothing was ever aged, and the
log said `mirroring lag is unknown` once a minute while the feeder ran
perfectly. The pause now names the id it went looking for, because a feeder that
is not running and an id nothing writes look identical from a log line.

Spelling the key out and disagreeing with `feeder.id` is still allowed, for a
feeder run from a separate config, and every daemon says so at startup.

### Both tiers are replica sets

That is the supported shape, and the only one anything here is tested against. A
sharded fast tier is refused, with the reason, because every reading the daemons
hold themselves back on is a replica set's. A sharded archive is not refused and
not supported: it is not obviously broken, and nothing checks it. Both are in
[What is not handled yet](#what-is-not-handled-yet).

### Both tiers run the same MongoDB release

Every daemon reads both tiers' versions at startup and refuses to run when they
differ:

```
error: the tiers run different MongoDB releases: fast tier 8.0.26-11, archive
7.0.30-1. Both tiers must run the same major.minor release, because a read the
fast tier cannot answer in full is sent to the archive as the application wrote
it, so the archive has to understand every stage, operator and option the
application uses. Upgrade the older tier to 8.0, or set allow_version_skew: true
to start anyway
```

The archive runs the client's own queries, not a rewritten shape of them. That
is what makes a fall-through correct, and it is also what makes a mismatched
pair dangerous: a proxy cannot rewrite a pipeline into an older dialect, so a
query using anything the archive does not understand works for as long as the
documents it reads are on the fast tier, and then fails with the archive's own
error naming a stage the application has always used. Nothing about that reads
as a version problem.

Measured on 8.0.26 over 7.0.37 with the skew allowed: `$convert` with `format`,
which 8.0 accepts and 7.0 refuses, answered a point read from fast tier documents
and failed over the same collection when the archive had to answer it, with
`$convert found an unknown argument: format`.

**So the router adds a sentence to that error**, naming both releases and saying
that a read the fast tier cannot answer in full is sent to the archive as the
application wrote it. Only when the archive is the older tier, and only for an
error that is a server refusing to recognise a name: a duplicate key, a timeout
or a genuinely malformed query gets the archive's words and nothing added, or a
version note becomes something to chase on every unrelated error.

Patch releases may differ, and the log says so once. One tier is always upgraded
before the other, so demanding an identical patch would refuse to start in the
middle of every rolling upgrade, in exchange for nothing: a patch release adds
no stage, operator or option for the other tier to fail on.

`allow_version_skew: true` starts anyway. It is for a migration you are watching,
not a state to run in, and the log repeats the warning at every start.

**The same release is not the whole of it.** Two servers on one release still
refuse different commands when their `featureCompatibilityVersion` differs, and
which tier answers a query is decided by how old its documents are. So that is
checked too, and refused the same way:

```
error: the tiers are both on 7.0 but disagree on featureCompatibilityVersion:
fast tier 7.0, archive 6.0. ... Run
db.adminCommand({setFeatureCompatibilityVersion: "7.0"}) on the tier that is
behind, or set allow_version_skew: true to start anyway
```

A version that cannot be read is not a refusal. `buildInfo` failing is a
connection problem, and whatever needs that connection says so in its own words
a moment later; the log names which tier could not be asked, so silence is not
mistaken for agreement. Reading the feature compatibility version needs a
privilege the router's own account may not have, and an account that cannot read
it on both tiers skips that half of the check rather than refusing to start.

A mongos is one server that does this: it answers `buildInfo`, so releases are
still compared, and it does not answer `getParameter` for
`featureCompatibilityVersion`. Both tiers are replica sets here, so that is a
note rather than a case.

The four daemons check this: feeder, evictor, warmer and router. The one-shot
tools do not, because a mismatched pair is exactly when you need `cline
watermark`, `cline keys` and `cline rollback` to work.

**Upgrading a major.minor release, then, is a procedure rather than a restart.**
The tiers differ for as long as it takes, and the rule says they may not, so the
order matters and one direction is safer than the other:

1. **Set `allow_version_skew: true`** and restart the daemons. Nothing else in
   this list can happen while they refuse to start.
2. **Upgrade the archive first.** This is the half that is not obvious and it
   follows from how a fall-through works: the archive runs the client's own
   query, not a rewritten one, so an archive newer than the fast tier understands
   everything the application can send it. The other order gives you an archive
   that fails on a stage the fast tier has just learned, and only for the
   queries whose documents have aged out, which is the hardest shape of failure
   to attribute.
3. **Then the fast tier.**
4. **Then `setFeatureCompatibilityVersion` on the archive, then on the fast
   tier**, for the same reason in the same order.
5. **Remove `allow_version_skew`** and restart the daemons, so the guard is back
   on before the next thing goes wrong.

Between steps 1 and 5 the pair is unguarded, which is why the log repeats the
warning at every start. `cline indexes` and `cline watermark` keep working
throughout; they never checked.

## Where the rest is written down

This file is what Thermocline is and how to run it. Everything else moved out of
it, because a README nobody reaches the end of is a README that does not work:

| | |
|---|---|
| [docs/router.md](docs/router.md) | What the proxy does with each kind of command, and why. The longest one, because it is where the work is |
| [docs/adoption.md](docs/adoption.md) | The numbered rollout for a cluster already serving traffic, and what an adoption carries that is not a document |
| [docs/limitations.md](docs/limitations.md) | Every limitation known, what each costs, and what would close it. **Read this before a pilot** |
| [docs/monitoring.md](docs/monitoring.md) | Every metric published, the nine signals worth paging on, and the Prometheus rules |
| [docs/operations.md](docs/operations.md) | Backups, restores and logs |
| [docs/watermark.md](docs/watermark.md) | The completeness claim, which is what makes a range query answerable from one tier |
| [docs/tiering.md](docs/tiering.md) | Telling an eviction from a deletion, and what the evictor refuses to do |
| [docs/bookkeeping.md](docs/bookkeeping.md) | What each tier holds that is not the application's data, and how much of it |
| [docs/caching.md](docs/caching.md) | Which queries are worth caching, and what the cache will not do |
| [docs/dashboard.md](docs/dashboard.md) | The operator page, and what `terminate_auth` changes about who may read what |
| [docs/testing.md](docs/testing.md) | Running the suites, and developing against a real pair of clusters |

## Design documents

The architecture, sizing from 500 GB to 5 TB, and the alternatives worth ruling
out first are written up separately, along with a phased rollout guide for
adopting this on a database that is already serving traffic.
