#!/bin/bash
# Check one change in minutes: its packages' unit tests and the live suites
# that reach it, on the Docker test box, never on this machine.
#
# A full sweep is 36 suites and about twenty minutes, and most changes reach a
# handful of them. Which handful is measured, not guessed: test/suitepick reads
# the coverage map in test/suitemap (see mkcovmap.sh) and picks the cheapest set
# of suites that runs every changed function at least once.
#
#   test/quick.sh                    against HEAD, the uncommitted change
#   test/quick.sh -base master~2     the last two commits as well
#   test/quick.sh testkeys testtxn   these suites, whatever changed
#   ALL=1 test/quick.sh              every suite that reaches the change
#   LIVE=1 test/quick.sh             with the live cache in verify mode
#
# It installs this working tree's cline into the node, which is what a sweep
# would run anyway. It does not run the host-side suites (hostsweep.sh).
set -uo pipefail
cd "$(dirname "$0")/.."

BOX=${BOX:-zelmario@192.168.1.50}
NODE=${NODE:-thermocline-zelmario-node0}
HOT=${HOT:-"mongodb://dba:secret@172.18.0.6:27017,172.18.0.7:27017,172.18.0.5:27017/?replicaSet=rsHot&authSource=admin"}
COLD=${COLD:-"mongodb://dba:secret@172.18.0.4:27017,172.18.0.2:27017,172.18.0.3:27017/?replicaSet=rsCold&authSource=admin"}
OUT=/root/quick

BASE=HEAD
if [ "${1:-}" = "-base" ]; then BASE=$2; shift 2; fi

# The box is reached over a network that drops now and then. Every remote step
# is short and retried, and the long one runs detached inside the node so a drop
# cannot kill it.
rsh() { local i; for i in 1 2 3; do ssh -o ConnectTimeout=10 "$BOX" "$@" && return 0; sleep 3; done; return 1; }
put() { local i; for i in 1 2 3; do scp -q -o ConnectTimeout=10 "$@" && return 0; sleep 3; done; return 1; }

T0=$(date +%s)
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

# 1. Unit tests of every package the change touches, built here and run on the
# box with the tier URIs set, so the tests that need a real MongoDB run too
# instead of skipping.
PKGS=$( { git diff --name-only "$BASE"; git ls-files --others --exclude-standard; } |
  grep '\.go$' | grep -v '^test/' | xargs -r -n1 dirname | sort -u)
UNIT=ok
if [ -n "$PKGS" ]; then
  echo "quick: unit tests in $(echo $PKGS | wc -w) package(s): $(echo $PKGS)"
  go vet $(printf './%s ' $PKGS) || UNIT=FAIL
  for p in $PKGS; do
    b=$(echo "$p" | tr / _)
    CGO_ENABLED=0 go test -c -o "$STAGE/$b.test" "./$p" >/dev/null || { UNIT=FAIL; continue; }
    [ -f "$STAGE/$b.test" ] || continue   # a package with no tests
  done
  if ls "$STAGE"/*.test >/dev/null 2>&1; then
    put "$STAGE"/*.test "$BOX:/tmp/" || { echo "quick: could not copy the tests to $BOX"; exit 2; }
    # Each runs in its own package directory, as go test runs it, beside the
    # repository's config files and testdata: config_test.go loads every shipped
    # config by a path like ../../cline.example.yaml, and failed from /tmp.
    git ls-files '*.yaml' '*.yml' '*/testdata/*' | tar -cz -T - |
      rsh "rm -rf /tmp/quickrepo && mkdir -p /tmp/quickrepo && tar -xz -C /tmp/quickrepo" ||
      { echo "quick: could not copy the test files to $BOX"; exit 2; }
    for t in "$STAGE"/*.test; do
      n=$(basename "$t")
      d=$(echo "${n%.test}" | tr _ /)
      res=$(rsh "mkdir -p /tmp/quickrepo/$d && cd /tmp/quickrepo/$d && TC_TEST_HOT='$HOT' TC_TEST_COLD='$COLD' /tmp/$n -test.count=1 -test.timeout=5m 2>&1 | tail -15")
      if echo "$res" | grep -q '^PASS$'; then
        echo "  $n: PASS"
      else
        echo "  $n: FAIL"; echo "$res" | sed 's/^/    /'; UNIT=FAIL
      fi
    done
  fi
fi

# 2. The suites.
if [ $# -gt 0 ]; then
  SUITES="$*"
else
  PICK=()
  [ "${ALL:-0}" = "1" ] && PICK=(-all)
  SUITES=$(go run ./test/suitepick -base "$BASE" "${PICK[@]}") || exit 2
fi
SUITES=$(echo $SUITES)
if [ -z "$SUITES" ]; then
  echo "quick: no suite reaches this change; unit tests: $UNIT ($(( $(date +%s) - T0 ))s)"
  [ "$UNIT" = ok ]; exit
fi
N=$(echo $SUITES | wc -w)
W=${WORKERS:-$(( N < 6 ? N : 6 ))}
echo "quick: $N suite(s) on $W worker(s): $SUITES"

make -s build >/dev/null || exit 2
# cline.yaml too: a suite that adds a namespace to it otherwise runs against the
# node's old copy, where that namespace is not tiered and every check on it
# passes for the wrong reason.
put bin/cline test/psweep.sh test/mkworker.sh test/fuzz.js test/cline.yaml "$BOX:/tmp/" || exit 2
# A part, suite@n, is a run of the suite's own file.
SFILES=$(printf '%s\n' $SUITES | sed 's/@.*//' | sort -u)
for s in $SFILES; do put "test/$s.sh" "$BOX:/tmp/" || exit 2; done
FILES="psweep.sh mkworker.sh fuzz.js cline.yaml $(printf '%s.sh ' $SFILES)"
rsh "set -e
  docker exec $NODE sh -c 'pgrep -f \"[p]sweep.sh\" >/dev/null' && { echo 'quick: a sweep is already running in $NODE'; exit 3; }
  docker cp /tmp/cline $NODE:/usr/local/bin/cline
  for f in $FILES; do docker cp /tmp/\$f $NODE:/root/\$f; done
  docker exec $NODE sh -c 'chmod +x /usr/local/bin/cline; cd /root && sed -i s/172\\.24\\./172.18./g $FILES'
  docker exec -d $NODE bash -c 'OUT=$OUT WORKERS=$W LIVE=${LIVE:-} SUITES=\"$SUITES\" bash /root/psweep.sh > $OUT.out 2>&1'" || exit 2

# Waited on the summary's last line, which psweep writes only once every worker
# is done. Polled, since the ssh it would otherwise hang on can drop.
until rsh "docker exec $NODE grep -q '^PSWEEP DONE' $OUT/summary.txt 2>/dev/null"; do sleep 10; done
rsh "docker exec $NODE cat $OUT.out" | grep -vE '^psweep: [0-9]+ suites'
SUITE_OK=0
rsh "docker exec $NODE grep -qE 'FAIL [1-9]|TIMED OUT|MISSING|no tally|SETUP FAILED|NO ANSWERS COMPARED|answers differed' $OUT/summary.txt" || SUITE_OK=1

echo "quick: unit tests $UNIT, suites $([ $SUITE_OK = 1 ] && echo ok || echo FAIL), $(( $(date +%s) - T0 ))s in all; logs in $NODE:$OUT"
[ "$UNIT" = ok ] && [ $SUITE_OK = 1 ]
