#!/bin/bash
# Run every in-node suite, several at a time, and write one log per suite.
#
# sweep.sh runs the same list serially and takes over 40 minutes, which is why
# measurements were getting skipped. The obstacle was never scheduling, it was
# isolation: every suite shared one pair of databases, one bookkeeping database,
# one config and one set of ports, and the suites stop and start the daemons out
# from under each other.
#
# A worker here is a slot with its own of each. It gets:
#
#   shop_wN, wh_wN          databases, so no two feeders watch one namespace
#   _thermocline_wN         bookkeeping, so a wholesale wipe stays local
#   27200 + 20N ...         a port block, offsets 0-17 so blocks never overlap,
#                           and clear of the 27100 a suite run by hand still uses
#   /root/wN                configs and logs, which is also what scopes the
#                           pkills: every daemon carries --config /root/wN/...
#
# The suites take all of these from the environment and default to the
# single-worker layout, so sweep.sh still works and a suite run by hand behaves
# exactly as it always has.
#
# Suites are claimed from a shared queue rather than dealt out in advance,
# because they range from under a minute to several, and a worker that finishes
# early should take the next one rather than idle.
# Stopping a sweep takes three kills, not one.
#
# pkill -f psweep.sh reaches the driver and the workers, and leaves every suite
# they started running: those are `bash /root/testX.sh` under a `timeout`, and
# they go on driving daemons, databases and ports for as long as their own logic
# takes. On 2026-09-14 a sweep stopped that way left a testconform running; the
# next sweep's testall, testlife and testconform then failed 3, 19 and dozens of
# checks with ECONNREFUSED, because the orphan kept stopping their daemons. It
# reads exactly like a regression in whatever change is being tested.
#
#   pkill -9 -f "[p]sweep.sh"
#   pkill -9 -f "bash /root/[t]est"      # the suites, which outlive the driver
#   pkill -9 -f "[c]line (feeder|router|evictor|warmer)"
#
# Then check nothing survived, because each of those can miss:
#   pgrep -af "[t]est|[c]line" ; ss -ltn | grep 272
WORKERS=${WORKERS:-4}
# How long one suite may take before it is killed and reported as a failure.
#
# This was 25m when testconform took about seven. Both it and testkeys grew past
# that under four workers: testconform was killed at the limit in every kept
# sweep from 2026-09-30 to 2026-10-07, testkeys took 1443s on 2026-09-30 and was
# killed in the two after. A killed suite reports no tally, so the sweep's total
# went on looking clean while the end of both suites never ran.
SUITE_TIMEOUT=${SUITE_TIMEOUT:-45m}
OUT=${OUT:-/root/psweep}
SHARED=${SHARED:-/root/cline.yaml}
SUITES=${SUITES:-"testconform testfuzz testauthterm testkeys testtxn testconcerns
        testall testrouter testops testedge testlife testchange testquery
        testddl testcache testfresh testaggwrite testexplain testindexes
        testmerge testecho testunack testupsert testsingle testauth testbulk
        testreadpref testreadprefterm testtypes testbson testwarm testevict
        testdaemonmetrics testretry testpool testnotice"}

# CACHE=1 runs the whole sweep with the query cache on.
#
# Worth having as a switch rather than a procedure somebody remembers. Every
# suite but testcache.sh runs with the cache off, so this is the only way to
# find a cached answer disagreeing with the tiers, and until 2026-09-11 it had
# never been run: 36 suites and 1591 checks that had never seen a cached read.
#
# Derived here rather than by editing the shared config, so a run cannot leave
# the cache on for the next one. testcache.sh still derives its own pair either
# way: it turns the block on when it is there and off for its own control.
if [ "${CACHE:-0}" = "1" ]; then
  CACHED=/root/cline_cache_sweep.yaml
  if grep -q '^  cache:' "$SHARED"; then
    sed 's/^\( *\)enabled: false/\1enabled: true/' "$SHARED" > "$CACHED"
  else
    sed 's|^  completeness:|  cache:\n    enabled: true\n    max_queries: 200\n    max_ttl: 60s\n  completeness:|' \
      "$SHARED" > "$CACHED"
  fi
  # Asserted, not assumed. A sweep that silently ran with the cache off would be
  # twenty-five minutes proving nothing, and it would look exactly like a pass.
  grep -q 'enabled: true' "$CACHED" || {
    echo "psweep: CACHE=1 but $CACHED does not turn the cache on"; exit 2; }
  SHARED=$CACHED
  echo "psweep: the query cache is on for every worker in this run"
fi

# LIVE=1 runs the whole sweep with the live cache on, and LIVE=secondary with
# its change stream read from a secondary. No suite turns it on, so its router
# side (what a write holds, the fast tier's proof, the merged fill, a client
# reading its own write through the proxy) was proved only by its own package's
# tests against a bare replica set.
if [ -n "${LIVE:-}" ] && [ "${LIVE}" != "0" ]; then
  FROM=primary
  [ "$LIVE" = "secondary" ] && FROM=secondary
  LIVECFG=/root/cline_live_sweep.yaml
  # In verify mode: the cache answers nothing and every answer it could have
  # given is compared with the real one, so a pass is a count of comparisons
  # rather than of reads that happened to miss. See liveverify.go.
  LIVELOG=/root/live_verify.log
  rm -f "$LIVELOG"
  sed "s|^  completeness:|  live_cache:\n    enabled: true\n    stream_from: $FROM\n    verify_log: $LIVELOG\n  completeness:|" \
    "$SHARED" > "$LIVECFG"
  grep -q '^  live_cache:' "$LIVECFG" || {
    echo "psweep: LIVE=$LIVE but $LIVECFG does not turn the live cache on"; exit 2; }
  SHARED=$LIVECFG
  echo "psweep: the live cache is on for every worker in this run, its stream from the $FROM"
fi

# The partition check is not run here, and that is deliberate. This sweep runs
# inside node0, where $0 is /root/psweep.sh and the suite files sit beside
# whatever anyone has ever copied into /root: five stray testX_old.sh from old
# debugging sessions made it refuse to start, naming them as suites no sweep
# runs. The partition is a property of the repository, so it is checked where
# the repository is, by hostsweep.sh and by `make check`.

rm -rf "$OUT"; mkdir -p "$OUT"
printf '%s\n' $SUITES > "$OUT/queue"
TOTAL=$(wc -l < "$OUT/queue")
: > "$OUT/summary.txt"
START=$(date +%s)

# claim prints the next suite and removes it, or nothing when the queue is
# empty. flock rather than a lock directory because two workers reaching here at
# once is the normal case, not the exceptional one.
claim() {
  local s
  exec 9>>"$OUT/.lock"
  flock 9
  s=$(head -1 "$OUT/queue" 2>/dev/null)
  [ -n "$s" ] && sed -i '1d' "$OUT/queue"
  flock -u 9
  exec 9>&-
  printf '%s' "$s"
}

worker() {
  local n=$1
  export WDIR="/root/w$n"
  export LOGDIR="$WDIR/log"
  export PORT=$((27200 + 20 * n))
  export PDASH=$((PORT + 10))
  export DB="shop_w$n"
  export DB2="wh_w$n"
  export DB3="catdb_w$n"
  export META="_thermocline_w$n"
  # CFG is deliberately not exported. Several suites name their own config
  # (testauthterm writes a terminate_auth one, testreadprefterm its own, and
  # testcache a pair), and every suite already defaults it to $WDIR/cline.yaml.
  # Exporting it here overrode those and testauthterm ran against the plain
  # config, which failed its first check and every one that depended on it.
  unset CFG

  rm -rf "$WDIR"; mkdir -p "$LOGDIR"
  bash /root/mkworker.sh "$SHARED" "$WDIR/cline.yaml" || {
    echo "worker $n: could not write its config" >> "$OUT/summary.txt"; return 1; }

  local s start secs rc line
  while :; do
    s=$(claim)
    [ -z "$s" ] && break
    [ -f "/root/$s.sh" ] || { printf '%-18s MISSING\n' "$s" >> "$OUT/summary.txt"; continue; }
    start=$(date +%s)
    # Bounded, because a suite that hangs used to stall the whole sweep and
    # there is nothing to read while it does: no failure, no output, just a
    # worker that never claims again. Twice in one day a run was lost to one
    # stuck process, once a mongosh that never reached the router on an idle
    # machine and never gave up either.
    #
    # A suite killed here is reported as a failure with the time it was given,
    # which is what makes it visible. The bound is generous next to the longest
    # suite, which is testconform at about seven minutes, so reaching it means
    # something is wrong rather than slow.
    timeout --kill-after=30s "$SUITE_TIMEOUT" bash "/root/$s.sh" > "$OUT/$s.log" 2>&1
    rc=$?
    secs=$(( $(date +%s) - start ))
    line=$(grep -E '^PASS [0-9]+ +FAIL [0-9]+' "$OUT/$s.log" | tail -1)
    if [ "$rc" = 124 ] || [ "$rc" = 137 ]; then
      line="TIMED OUT after ${SUITE_TIMEOUT}, killed. ${line:-no tally}"
      # Whatever it left running is this worker's to clear, and the cleanup
      # below only reaches daemons. A shell it was waiting on outlives it.
      pkill -f "bash /root/$s.sh" 2>/dev/null
    fi
    [ -z "$line" ] && line="(no tally, rc=$rc)"
    # A failed suite keeps its daemons' logs. They live in the worker's own
    # directory and the next suite on that worker writes over them, so the
    # evidence for a failure that does not reproduce was gone by the time
    # anybody looked: testconform failed one check in a sweep on 2026-09-26,
    # passed alone, under load and as a probe, and its router log had already
    # been replaced. Copied, not moved, and only on failure.
    if [ "$rc" != 0 ] && [ -d "$LOGDIR" ]; then
      rm -rf "$OUT/$s.daemonlogs"
      cp -r "$LOGDIR" "$OUT/$s.daemonlogs" 2>/dev/null
    fi
    # One append per suite, and every line is written by a single printf, so the
    # writes are small enough that the kernel does not interleave them.
    printf '%-18s w%-2s rc=%-3s %4ss  %s\n' "$s" "$n" "$rc" "$secs" "$line" >> "$OUT/summary.txt"
    # Leave nothing of this suite's daemons behind for the next one on this
    # worker. Scoped to this worker's config directory, so it cannot reach
    # another worker's.
    pkill -f "cline (feeder|router|evictor|warmer) --config $WDIR/" 2>/dev/null
    pkill -9 -f "cline .*--config $WDIR/" 2>/dev/null
    sleep 1
  done
}

echo "psweep: $TOTAL suites across $WORKERS workers" | tee -a "$OUT/summary.txt"
for n in $(seq 0 $((WORKERS - 1))); do worker "$n" & done
wait

MINS=$(( ($(date +%s) - START) / 60 ))
{
  echo "PSWEEP DONE in ${MINS}m"
  # Only the tally line, which starts at column one. The per-check lines are
  # indented, and counting those would double every number.
  echo "checks: $(grep -hE '^PASS [0-9]+ +FAIL [0-9]+' "$OUT"/*.log | awk '{p+=$2; f+=$4} END {print p+0" passed, "f+0" failed"}')"
  if [ -n "${LIVELOG:-}" ]; then
    LOK=$(grep -c '^OK' "$LIVELOG" 2>/dev/null); LBAD=$(grep -c '^MISMATCH' "$LIVELOG" 2>/dev/null)
    echo "live cache: ${LOK:-0} answers compared, ${LBAD:-0} differed ($LIVELOG)"
    # Kept with the run, since the next sweep empties it.
    cp "$LIVELOG" "$OUT/live_verify.log" 2>/dev/null
    [ "${LOK:-0}" = "0" ] && echo "live cache: NO ANSWERS COMPARED, the run proved nothing about it"
    [ "${LBAD:-0}" != "0" ] && echo "live cache: FAIL 1 (answers differed)"
  fi
} >> "$OUT/summary.txt"

# And a status a caller can read. Without this the script ended on whatever the
# last command happened to return -- 1 on a clean run, as it turned out -- so
# nothing could tell a passing sweep from a failing one except a person reading
# the summary. The same rule hostsweep.sh follows: a tally is for people, an
# exit status is for whatever started it.
cat "$OUT/summary.txt"
grep -qE 'FAIL [1-9]|TIMED OUT|MISSING|no tally|SETUP FAILED|NO ANSWERS COMPARED' "$OUT/summary.txt" && exit 1
exit 0
