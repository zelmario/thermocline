#!/bin/bash
# Item 3 of docs/production-readiness.md: the change stream that delivered 23 of
# 24 events once, on 2026-09-16, and has not been seen since.
#
#   docker exec -d thermocline-zelmar-node0 bash -c 'ROUNDS=20 bash /root/streamsoak.sh > /root/streamsoak.out 2>&1'
#
# Why this rather than running testfuzz twenty times in a quiet cluster. It
# appeared during a parallel sweep, with three other workers driving daemons,
# databases and ports on the same six mongods. A lone run is a different
# machine: no contention for write tickets, no cache pressure, no other feeder
# reading a change stream. Twenty quiet runs would say very little, and saying
# it loudly is worse than saying nothing.
#
# So each round runs testfuzz on worker 0 with three other suites running
# beside it, and only testfuzz's result is the subject. The other three are
# load, and their own failures are reported separately rather than mixed in: a
# suite failing under contention is a fact about this box, and it is not what
# this is asking.
ROUNDS=${ROUNDS:-20}
OUT=${OUT:-/root/streamsoak}
SHARED=${SHARED:-/root/cline.yaml}
LOAD=${LOAD:-"testconform testall testlife"}
SUITE_TIMEOUT=${SUITE_TIMEOUT:-25m}

rm -rf "$OUT"; mkdir -p "$OUT"
: > "$OUT/summary.txt"
echo "streamsoak: $ROUNDS rounds of testfuzz with [$LOAD] running beside it"

setup() {
  local n=$1
  export WDIR="/root/w$n" LOGDIR="/root/w$n/log"
  export PORT=$((27200 + 20 * n)) PDASH=$((27200 + 20 * n + 10))
  export DB="shop_w$n" DB2="wh_w$n" DB3="catdb_w$n" META="_thermocline_w$n"
  unset CFG
  rm -rf "$WDIR"; mkdir -p "$LOGDIR"
  bash /root/mkworker.sh "$SHARED" "$WDIR/cline.yaml"
}

# The evidence this exists to capture. A count cannot be worked back to a
# document; the router's own line names it, and the round's logs are kept only
# when something disagreed, so a long run does not fill the disk with proof that
# nothing happened.
keep() {
  local r=$1
  mkdir -p "$OUT/round$r"
  cp /root/w0/log/fuzz_r.log "$OUT/round$r/" 2>/dev/null
  cp /root/w0/log/fuzz_f.log "$OUT/round$r/" 2>/dev/null
  cp "$OUT/fuzz.log" "$OUT/round$r/testfuzz.log" 2>/dev/null
  grep -F 'hiding an insert as a promotion' /root/w0/log/fuzz_r.log > "$OUT/round$r/hidden.txt" 2>/dev/null
}

disagreed=0
for r in $(seq 1 "$ROUNDS"); do
  start=$(date +%s)
  for n in 1 2 3; do
    ( setup "$n" >/dev/null 2>&1
      for s in $LOAD; do
        timeout --kill-after=30s "$SUITE_TIMEOUT" bash "/root/$s.sh" > "/root/w$n/log/$s.out" 2>&1
      done ) &
  done
  ( setup 0 >/dev/null 2>&1
    timeout --kill-after=30s "$SUITE_TIMEOUT" bash /root/testfuzz.sh > "$OUT/fuzz.log" 2>&1
    echo $? > "$OUT/fuzz.rc" ) &
  wait

  rc=$(cat "$OUT/fuzz.rc" 2>/dev/null || echo 99)
  # The streams section is the subject, so it is read on its own rather than
  # from the suite's exit code: another section failing under load is a fact
  # about this box and would hide or fake this answer either way.
  streams=$(grep -E "streams: no disagreement" "$OUT/fuzz.log" | head -1)
  agreed=$(grep -m1 '^  AGREED ' "$OUT/fuzz.log")
  secs=$(( $(date +%s) - start ))
  if echo "$streams" | grep -q "^  PASS"; then
    printf 'round %-3s %4ss  streams agreed   testfuzz rc=%s  %s\n' "$r" "$secs" "$rc" "$agreed" >> "$OUT/summary.txt"
  else
    disagreed=$((disagreed+1))
    printf 'round %-3s %4ss  STREAMS DISAGREED  testfuzz rc=%s  %s\n' "$r" "$secs" "$rc" "$agreed" >> "$OUT/summary.txt"
    keep "$r"
  fi
  tail -1 "$OUT/summary.txt"
done

echo
echo "================================"
echo "rounds $ROUNDS   streams disagreed $disagreed"
[ "$disagreed" -gt 0 ] && echo "evidence kept in $OUT/round*/, read hidden.txt first"
exit "$disagreed"
