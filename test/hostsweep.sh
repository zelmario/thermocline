#!/bin/bash
# Every suite that runs on the host, one after another, with one log each.
#
#   bash test/hostsweep.sh                 # from the repository root
#
# psweep.sh runs the suites that live inside the node. These do not, and each
# has a reason: testchaos and testfailover stop and start mongod on other
# containers, testtls and testsharded and testversion build clusters of their
# own, testalerts needs a Prometheus and the router's metrics endpoint at the
# same time, and testrollback drives the shop binary and the daemons together.
#
# The README listed all seven and nothing ran them. A list is not a runner: they
# went whenever somebody remembered, one at a time, and on 2026-09-10 none of
# them had run all day while the in-node sweep had run three times. Between them
# they are 90-odd checks over the shapes nothing else covers -- a rollback
# procedure, an election, TLS on both sides, a refused mongos, a version
# mismatch, and the alert rules an operator is told to trust.
#
# Serial on purpose. Every one either drives the shared containers or binds
# ports of its own, so two at once fight over both.
#
# A skipped suite is not a passed suite. When a prerequisite is missing the
# suite is reported SKIPPED with the reason and counted on its own line, because
# a shape nobody checked is the thing this script exists to stop happening
# quietly.
set -u
OUT=${OUT:-/tmp/hostsweep}
NODE=${NODE:-thermocline-zelmar-node0}
# testchaos is the one suite worth running in both authentication modes: with
# authentication proxied a write during an election fails and the client
# retries, and with terminate_auth on the router replays it onto a fresh
# primary, which is the only thing that exercises that pool during a real
# election. Set CHAOS_BOTH=0 to run only the default.
CHAOS_BOTH=${CHAOS_BOTH:-1}
SUITES=${SUITES:-"testrollback testfailover testalerts testversion testtls testsharded testchaos"}

[ -f test/hostsweep.sh ] || { echo "run this from the repository root"; exit 2; }

# Nothing else may be driving the cluster. An in-node sweep and these suites
# fight over the same six containers and the same /usr/local/bin/cline, and the
# result is failures that belong to neither.
busy=$(docker exec "$NODE" bash -c 'pgrep -cf "bash /root/[t]est"' 2>/dev/null | tail -1)
if [ "${busy:-0}" != "0" ]; then
  echo "the node is running $busy suite(s) already; wait for the sweep to finish"
  exit 2
fi
# And on this side of the fence too, which the check above cannot see: these
# suites run here rather than in the node, so a host-side suite already going --
# another hostsweep, or one somebody started by hand -- is invisible to a pgrep
# inside the container. The bracket keeps the pattern from matching the shell
# that runs it, which is how this check killed itself the first three times it
# was written somewhere else in this repository.
hostbusy=$(pgrep -cf "bash test/[t]est" 2>/dev/null || true)
if [ "${hostbusy:-0}" != "0" ]; then
  echo "$hostbusy host-side suite(s) are already running here; wait for them"
  pgrep -af "bash test/[t]est" 2>/dev/null | sed 's/^/  /'
  exit 2
fi

rm -rf "$OUT"; mkdir -p "$OUT"
: > "$OUT/summary.txt"
START=$(date +%s)

haveImage() { docker image inspect "$1" >/dev/null 2>&1; }

# why SUITE prints the reason it cannot run, or nothing.
why() {
  case $1 in
    testrollback)
      [ -x bin/store ] || { echo "bin/store is not built: go build -o bin/store ./test/store"; return; }
      [ -x bin/cline ] || echo "bin/cline is not built: make build" ;;
    testfailover|testchaos)
      docker inspect -f '{{.State.Running}}' "$NODE" 2>/dev/null | grep -q true \
        || echo "$NODE is not running" ;;
    testalerts)
      haveImage prom/prometheus:v2.53.5 || echo "docker pull prom/prometheus:v2.53.5" ;;
    testversion)
      for i in mongo:8.0 mongo:7.0 mongo:7; do
        haveImage "$i" || { echo "docker pull mongo:8.0 mongo:7.0 mongo:7"; return; }
      done ;;
    testtls)
      command -v openssl >/dev/null || { echo "openssl is not installed"; return; }
      command -v mongod  >/dev/null || echo "a host mongod is needed and not installed" ;;
    testsharded)
      command -v mongod >/dev/null || { echo "a host mongod is needed and not installed"; return; }
      command -v mongos >/dev/null || echo "a host mongos is needed and not installed" ;;
  esac
}

# run SUITE LABEL [ENV=VALUE ...]
run() {
  local s=$1 label=$2; shift 2
  local log="$OUT/$label.log" start secs rc line
  start=$(date +%s)
  env "$@" timeout --kill-after=60s "${SUITE_TIMEOUT:-30m}" bash "test/$s.sh" > "$log" 2>&1
  rc=$?
  secs=$(( $(date +%s) - start ))
  line=$(grep -E '^PASS [0-9]+ +FAIL [0-9]+' "$log" | tail -1)
  if [ "$rc" = 124 ] || [ "$rc" = 137 ]; then
    line="TIMED OUT after ${SUITE_TIMEOUT:-30m}. ${line:-no tally}"
  elif [ "$rc" = 2 ] && [ -z "$line" ]; then
    # Several of these exit 2 from their own setup check, which is a different
    # thing from a failing check and has to read differently.
    line="SETUP FAILED: $(grep -E 'FATAL|not here|never came up' "$log" | tail -1)"
  fi
  printf '%-22s rc=%-3s %5ss  %s\n' "$label" "$rc" "$secs" "${line:-(no tally, rc=$rc)}" >> "$OUT/summary.txt"
}

echo "hostsweep: $(echo $SUITES | wc -w) suites, serial, logs in $OUT" | tee -a "$OUT/summary.txt"

for s in $SUITES; do
  [ -f "test/$s.sh" ] || { printf '%-22s MISSING\n' "$s" >> "$OUT/summary.txt"; continue; }
  reason=$(why "$s")
  if [ -n "$reason" ]; then
    printf '%-22s SKIPPED  %s\n' "$s" "$reason" >> "$OUT/summary.txt"
    continue
  fi
  echo "  running $s ..."
  run "$s" "$s"
  # And again with authentication terminated, which is a different code path
  # through the same election rather than the same test twice.
  if [ "$s" = testchaos ] && [ "$CHAOS_BOTH" = 1 ]; then
    docker exec "$NODE" bash -c \
      "sed 's/^  listen:/  terminate_auth: true\n  listen:/' /root/cline.yaml > /root/cline_chaos_ta.yaml" \
      >/dev/null 2>&1
    if [ "$(docker exec "$NODE" grep -c 'terminate_auth: true' /root/cline_chaos_ta.yaml 2>/dev/null | tail -1)" = "1" ]; then
      echo "  running testchaos again with terminate_auth ..."
      run testchaos testchaos-termauth CFG=/root/cline_chaos_ta.yaml
    else
      printf '%-22s SKIPPED  could not write a terminate_auth config in %s\n' \
        "testchaos-termauth" "$NODE" >> "$OUT/summary.txt"
    fi
  fi
done

MINS=$(( ($(date +%s) - START) / 60 ))
{
  echo "HOSTSWEEP DONE in ${MINS}m"
  echo "checks: $(grep -hE '^PASS [0-9]+ +FAIL [0-9]+' "$OUT"/*.log 2>/dev/null |
    awk '{p+=$2; f+=$4} END {print p+0" passed, "f+0" failed"}')"
  n=$(grep -c SKIPPED "$OUT/summary.txt" || true)
  [ "${n:-0}" != "0" ] && echo "skipped: $n suite(s), which is $n shape(s) nobody checked"
} >> "$OUT/summary.txt"

cat "$OUT/summary.txt"
grep -qE 'FAIL [1-9]|TIMED OUT|SETUP FAILED|MISSING' "$OUT/summary.txt" && exit 1
exit 0
