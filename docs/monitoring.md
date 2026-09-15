# Prometheus, and what to alert on

Every metric the daemons publish, the nine signals worth waking somebody for, and the rules themselves.

Part of [Thermocline](../README.md).

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

#### The query cache

Six series appear only when `router.cache.enabled` is true, because a hit rate
of zero published for a cache nobody turned on reads as a cache that is not
working:

    thermocline_cache_hits_total
    thermocline_cache_misses_total
    thermocline_cache_learned_total
    thermocline_cache_rejected_total
    thermocline_cache_entries
    thermocline_cache_bytes

The hit rate over the first two is what says whether enabling it was worth it.
The other four are for the case that rate cannot explain. `cache_bytes` against
your `max_bytes` says whether the cache is evicting to stay inside its budget;
`cache_entries` against `max_queries` says the same for the table of watched
queries. `cache_rejected_total` is the one that catches the surprise: a cache
handed replies over 256KB learns from them and holds none of them, so it looks
idle from the hit rate alone and the reason is visible nowhere else.

There is no alert on any of them, on purpose. A cache that is not paying is a
tuning question, not a page, and the two things it could get wrong that *are*
worth paging on are already covered: a stale answer would show as a wrong
answer, and the memory is bounded by configuration rather than by an alert.

The feeder publishes its counters the way the evictor and the warmer do, under
`thermocline_daemon_documents_total{daemon="feeder:<id>"}`:

    outcome="mirrored"               documents copied into the archive
    outcome="tombstoned"             documents marked deleted there
    outcome="unmirrorable"           documents the archive refused and this gave up on
    outcome="inserts_over_existing"  inserts mirrored onto an _id the archive already held

`unmirrorable` has a rule below and should be zero: each one is a document the
archive will never hold while the fast tier does, so the evictor will never
remove it and a read that falls through will never find it.

`inserts_over_existing` has no rule, deliberately, because it counts a shape
rather than a fault. Through the router an insert for an `_id` the archive holds
live is refused as a duplicate key, so a write straight at the fast tier's
mongod raises this and nothing else does — except an application that deletes an
`_id` and uses it again, which is legitimate and raises it too. Measured on a
shop run of 348 mirrored batches with copy-ups and evictions underneath: zero.
On one write made straight at the fast tier: one. On a delete and re-insert of
the same `_id` through the router: also one. So it is a number to go and look at
rather than one to be woken by, and a deployment where it climbs without anybody
reusing `_id`s has a writer that is not going through the router.


### What to alert on

Nine signals, and the reason each one is worth waking somebody for. Everything
here is already published; what was missing was saying which numbers mean
something.

**1. `thermocline_panics_total` above zero. Page.** A panic is a defect, with a
stack in the log, and the connection it happened on was closed. Nothing about
the tiering is wrong at that moment, but something the router was asked to do it
could not, and the next client to ask will hit it too.

**2. `thermocline_stale_reads_total` above zero. Page.** A read fell through to
the archive and was answered before mirroring reached the moment the read
started, so it may miss a write that already returned. Every other counter here
is about work; this one is about a wrong answer.

**3. `thermocline_mirroring_lag_seconds` above the eviction notice's lifetime.
Page.** Past that point the evictor stops, which is the safe behaviour, and a
delete the feeder reaches after the notice has expired is read as the
application's own and the document is lost from both tiers. The config refuses
the combination that would allow it, so this alert is about the lag getting
there. Warn at a minute, page at ten.

**4. `thermocline_refused_total` rising. Investigate, do not page.** The router
would not answer a command, and every refusal names its reason in the log. A
steady trickle is an application asking for something this system does not do; a
step change means completeness claims no longer cover what is being asked, which
is usually a warm that has not run.

Read the *shape* of the curve, not the value, and read the log before concluding
anything. Several ordinary things refuse on purpose: a pipeline reading an
untiered collection, a transaction spanning both tiers, a write the archive's
unique index already holds. A report that runs every hour and joins a reference
collection puts a step in this counter on a schedule and nothing is wrong. That
is why the pilot's own soak stopped asserting this counter is zero and started
holding it to the refusals the comparison itself reported: a number that grows
for a good reason teaches whoever is watching to stop watching it.

**5. `thermocline_daemon_seconds_since_pass` climbing. Page at ten minutes.** A
daemon that has stopped shows this climbing rather than going silent, which is
the whole reason it is a gauge from the last published report rather than a
heartbeat. The feeder stopping is the serious one: mirroring stops, the evictor
halts behind it, and every fall-through read pays the freshness barrier.

**6. `thermocline_daemon_gauge{gauge="clock_skew_seconds"}` past a minute.
Investigate.** Nothing here loses data over it any more: everything the
archive's own TTL monitor dates is now stamped by the archive as it is written,
including the eviction announcements, which used to be the way a clock could
destroy documents. What a clock still shifts is the windows measured against
your documents' own dates, an eviction cutoff and a warm window, by about this
much. Negative is behind. The evictor measures it against the archive on every
pass.

Every daemon publishes into this one family, by whatever route it has: the
evictor in its report, the feeder in the position record it writes several times
a second, and the router by measuring its own. The rules below select on the
gauge name rather than the daemon, so all of them are covered without a rule
each. The other two matter less than the evictor and are worth watching anyway:
mirroring lag and the freshness barrier are both worked out by subtracting one
host's instant from another's, and both are corrected for exactly this reading,
so a correction nobody can see is a correction nobody can check.

**7. A series that is absent, which is the failure no threshold catches.** Every
alert above compares a number, and a daemon that never started publishes no
number at all: the evictor's `seconds_since_pass` climbs when it stops and does
not exist when it was never run, so `> 600` never fires. Name the daemons you
expect and alert on their absence.

Mirroring is handled in the export instead, because it is the one that matters
most: with no position and no checkpoint, `thermocline_mirroring_lag_seconds`
reports `+Inf` rather than vanishing, so the first alert anyone builds fires for a
feeder that has never run as well as one that has stopped. The router's own
absence is the scrape target going down, which is `up == 0` on the job.

**8. `thermocline_claim_grace_reads_total` above zero, if you set
`router.claim_grace`. Page.** Every read it counts was answered from a
completeness claim the router could not check, because the archive was
unreachable. That is the trade you asked for, and it is also the only signal
that says the trade is being taken: the answers are correct if the archive is
down and short if only this router is cut off, and the counter cannot tell those
apart. Find out which it is. On a deployment that leaves the grace off this
counter stays at zero for ever, so the alert costs nothing.

**9. `thermocline_unauthorized_total` or `thermocline_unreadable_total` above
zero. Investigate.** The first is a client reading a collection it has no
permission for, refused. The second is a message the router could not inspect,
which means it was forwarded unguarded; a compressed connection is the cause, and
the remedy is removing the client's compressors.

```yaml
groups:
  - name: thermocline
    rules:
      - alert: ThermoclinePanic
        expr: increase(thermocline_panics_total[5m]) > 0
        labels: {severity: page}
        annotations:
          summary: "the router recovered a panic; there is a stack in its log"
      - alert: ThermoclineStaleRead
        expr: increase(thermocline_stale_reads_total[5m]) > 0
        labels: {severity: page}
        annotations:
          summary: "an archive read was answered before mirroring caught up"
      - alert: ThermoclineMirroringBehind
        expr: thermocline_mirroring_lag_seconds > 600
        for: 5m
        labels: {severity: page}
        annotations:
          summary: "mirroring is far enough behind that eviction has stopped"
      - alert: ThermoclineDaemonStopped
        expr: thermocline_daemon_seconds_since_pass > 600
        labels: {severity: page}
        annotations:
          summary: "{{ $labels.daemon }} has not finished a pass in ten minutes"
      - alert: ThermoclineRefusalsRising
        expr: increase(thermocline_refused_total[15m]) > 10
        labels: {severity: ticket}
        annotations:
          summary: "the router is refusing commands; the log says why"
      - alert: ThermoclineClockBehind
        expr: thermocline_daemon_gauge{gauge="clock_skew_seconds"} < -60
        for: 5m
        labels: {severity: ticket}
        annotations:
          summary: "{{ $labels.daemon }}'s clock is behind the archive's; the windows differ from the configuration"
      - alert: ThermoclineClockAhead
        expr: thermocline_daemon_gauge{gauge="clock_skew_seconds"} > 60
        for: 5m
        labels: {severity: ticket}
        annotations:
          summary: "{{ $labels.daemon }}'s clock is ahead of the archive's; the windows differ from the configuration"
      - alert: ThermoclineDaemonMissing
        # One per daemon you run: absent() needs the label spelled out, and an
        # evictor nobody configured is not a fault. This is what a daemon that
        # has never started looks like, where seconds_since_pass has nothing to
        # compare.
        expr: absent(thermocline_daemon_seconds_since_pass{daemon="evictor"})
        for: 10m
        labels: {severity: page}
        annotations:
          summary: "the evictor has never reported; check whether it is running at all"
      - alert: ThermoclineRouterDown
        expr: up{job="thermocline"} == 0
        for: 2m
        labels: {severity: page}
        annotations:
          summary: "the router is not answering its metrics endpoint"
      - alert: ThermoclineAnsweringWithoutTheArchive
        # Only ever fires where router.claim_grace is set. Reads are being
        # answered from a claim that cannot be checked: correct if the archive
        # is down, short if only this router is cut off from it.
        expr: increase(thermocline_claim_grace_reads_total[5m]) > 0
        labels: {severity: page}
        annotations:
          summary: "the router is answering from an unverifiable completeness claim; is the archive down, or is the router partitioned?"
      - alert: ThermoclineUnmirrorableDocument
        # The archive refused a document the fast tier accepted, and the feeder
        # gave it up rather than retrying it for ever, because no retry of that
        # document can succeed. Each one is a document the archive will never
        # hold while the fast tier does, so the evictor will never remove it and
        # a read that falls through will never find it. It was an ERROR in the
        # feeder's log and nothing else until the feeder started publishing its
        # counters.
        expr: increase(thermocline_daemon_documents_total{outcome="unmirrorable"}[15m]) > 0
        labels: {severity: page}
        annotations:
          summary: "the archive refused a document the fast tier holds; see the feeder's log for which and why"
      - alert: ThermoclineMemberNotReadable
        # A member of the fast tier is in the set and refusing reads, which is
        # what a resync, an initial sync or a maintenance window looks like from
        # the router. Reads are being answered by the remaining members, so
        # nothing is wrong with the answers; what is wrong is that the load is
        # not spread the way the deployment was sized for. Only ever fires where
        # router.terminate_auth is on, since that is where read preference is
        # honoured at all.
        expr: increase(thermocline_secondary_reads_moved_total[10m]) > 0
        labels: {severity: ticket}
        annotations:
          summary: "a fast tier member is in the set and not readable; reads that asked for a secondary are going to the primary"
      - alert: ThermoclineUnguardedOrUnauthorized
        expr: increase(thermocline_unauthorized_total[15m]) > 0
          or increase(thermocline_unreadable_total[15m]) > 0
        labels: {severity: ticket}
        annotations:
          summary: "a client read what it may not, or sent what could not be inspected"
```

**These rules are extracted from this file and run**, by `test/testalerts.sh`,
against a real Prometheus scraping a real router. They were written down and
never executed until 2026-09-03, which is a strange thing to leave unproved: a
rule that does not fire is indistinguishable from a system with nothing wrong.

It checks the three ways they fail quietly. `promtool` rejects a typo. A metric
this file names and the exporter does not is reported by name, which is the
drift that matters and the one `promtool` cannot see: a renamed counter is
valid PromQL that never matches anything. And with the router stopped,
`ThermoclineRouterDown` has to actually reach `firing`, while
`ThermoclinePanic` has to stay quiet, because two rules that both fire on
everything are worth as much as neither.

Two things are deliberately **not** alerts. `copied_up_total` rising means the
eviction boundary is cut deeper than the application reads, which is a tuning
conversation rather than an incident. And `duplicate_keys_total` counts writes
refused for duplicating an archived key, which for an application that relies on
`E11000` is ordinary behaviour working correctly.
