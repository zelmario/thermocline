#!/bin/bash
# Run every in-node suite once, serially, and write one log per suite.
#
# Serial on purpose. Everything shares one cluster and one /usr/local/bin/cline,
# and the suites stop and start the daemons out from under each other, so two at
# once report failures that do not exist.
#
# testchaos.sh is not here: it runs on the host because it stops and starts
# mongod on other containers.
OUT=${OUT:-/root/sweep}
mkdir -p "$OUT"
SUITES="testall testevict testrouter testops testkeys testwarm testfresh
        testaggwrite testlife testedge testtxn testauth testbulk testchange
        testconcerns testddl testexplain testindexes testquery testreadpref
        testtypes testupsert testsingle testunack testconform testauthterm testreadprefterm testecho testmerge testdaemonmetrics testretry testpool"
: > "$OUT/summary.txt"
for s in $SUITES; do
  [ -f "/root/$s.sh" ] || { echo "$s MISSING" >> "$OUT/summary.txt"; continue; }
  start=$(date +%s)
  bash "/root/$s.sh" > "$OUT/$s.log" 2>&1
  rc=$?
  secs=$(( $(date +%s) - start ))
  line=$(grep -E '^PASS [0-9]+ +FAIL [0-9]+' "$OUT/$s.log" | tail -1)
  [ -z "$line" ] && line="(no tally, rc=$rc)"
  printf '%-14s rc=%-3s %4ss  %s\n' "$s" "$rc" "$secs" "$line" >> "$OUT/summary.txt"
done
echo "SWEEP DONE" >> "$OUT/summary.txt"
