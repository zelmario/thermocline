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
WORKERS=${WORKERS:-4}
OUT=${OUT:-/root/psweep}
SHARED=${SHARED:-/root/cline.yaml}
SUITES=${SUITES:-"testconform testauthterm testkeys testtxn testconcerns
        testall testrouter testops testedge testlife testchange testquery
        testddl testcache testfresh testaggwrite testexplain testindexes
        testmerge testecho testunack testupsert testsingle testauth testbulk
        testreadpref testreadprefterm testtypes testwarm testevict
        testdaemonmetrics testretry testpool testnotice"}

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
    bash "/root/$s.sh" > "$OUT/$s.log" 2>&1
    rc=$?
    secs=$(( $(date +%s) - start ))
    line=$(grep -E '^PASS [0-9]+ +FAIL [0-9]+' "$OUT/$s.log" | tail -1)
    [ -z "$line" ] && line="(no tally, rc=$rc)"
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
} >> "$OUT/summary.txt"
