#!/bin/bash
# Adopt MongoDB's published sample datasets and run the drop-in test over them.
#
#   bash test/pilot/setup.sh            everything below, in order
#   bash test/pilot/setup.sh compare    just the comparison, against what is up
#
# Run it from the repository root on the host. It drives the anydbver cluster
# through node0, the same way the suites do.
set -u
NODE=${NODE:-thermocline-zelmar-node0}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
ROUTER=${ROUTER:-'mongodb://dba:secret@127.0.0.1:27500/?authSource=admin'}
ARCHIVE_URL=${ARCHIVE_URL:-https://atlas-education.s3.amazonaws.com/sampledata.archive}
TIERED="sample_mflix.comments sample_supplies.sales sample_weatherdata.data sample_training.trips"

N() { docker exec $NODE bash -c "$1"; }

restore() {
  echo "== stocking the archive, and only the archive =="
  if ! docker exec $NODE test -f /root/sampledata.archive; then
    curl -sS -o /tmp/sampledata.archive "$ARCHIVE_URL" || { echo "could not fetch $ARCHIVE_URL"; exit 2; }
    docker cp /tmp/sampledata.archive $NODE:/root/sampledata.archive
  fi
  N "mongorestore --uri='$COLD' --archive=/root/sampledata.archive --numParallelCollections=2 2>&1 | tail -1"
}

copyUntiered() {
  # Runbook step 4. Nineteen of the twenty-three collections are not tiered, and
  # a collection nothing tiers lives on the archive alone, so the application
  # sees it empty. This is the "copy it whole" half of that advice.
  echo "== copying the collections nothing tiers =="
  N "rm -rf /root/pilotdump; mkdir -p /root/pilotdump"
  for db in $(N "mongosh '$COLD' --quiet --eval 'print(db.adminCommand({listDatabases:1}).databases.map(d=>d.name).filter(n=>/^sample_/.test(n)).join(\" \"))'"); do
    for c in $(N "mongosh '$COLD' --quiet --eval \"print(db.getSiblingDB('$db').getCollectionNames().join(' '))\""); do
      case " $TIERED " in *" $db.$c "*) continue ;; esac
      N "mongodump --uri='$COLD' --db $db --collection $c -o /root/pilotdump --quiet"
    done
  done
  N "mongorestore --uri='$HOT' /root/pilotdump 2>&1 | tail -1"
}

start() {
  echo "== the daemons, in runbook order =="
  docker cp test/pilot/pilot.yaml $NODE:/root/pilot.yaml
  N "mkdir -p /root/pilotlog"
  N "for p in \$(ss -ltnp | grep :27500 | grep -o 'pid=[0-9]*' | cut -d= -f2); do kill -9 \$p; done" >/dev/null 2>&1
  docker exec -d $NODE cline feeder --config /root/pilot.yaml --log-file /root/pilotlog/feeder.log
  sleep 3
  docker exec -d $NODE cline router --config /root/pilot.yaml --log-file /root/pilotlog/router.log
  for _ in $(seq 1 30); do
    N "mongosh '$ROUTER' --quiet --eval 'db.adminCommand({ping:1})'" >/dev/null 2>&1 && break
    sleep 1
  done
  echo "-- what the router says at startup, which is runbook step 1 --"
  N "grep level=WARN /root/pilotlog/router.log | sed -E 's/ meaning=.*//'"
  echo "== warming =="
  N "cline warmer --config /root/pilot.yaml 2>&1 | grep 'warm complete'"
  N "cline keys --config /root/pilot.yaml --rebuild 2>&1 | tail -1"
}

compare() {
  # The whole point. Nothing in either harness asserts an expected value: the
  # archive is the control, and the only claim being checked is that a client
  # cannot tell the difference. See README.md in this directory.
  #
  # agree.js asks per collection. aggs.js asks the questions that reach across
  # them, including a join in each direction between a tiered collection and an
  # untiered one, which is the case no watermark can cover.
  echo "== asking both the same questions =="
  local rc=0
  for h in agree aggs; do
    docker cp test/pilot/$h.js $NODE:/root/$h.js >/dev/null
    docker exec -e TC_TARGET=router -e TC_REPEAT="${TC_REPEAT:-1}" $NODE \
      mongosh "$ROUTER" --quiet --file /root/$h.js > /tmp/${h}_router.json 2>&1
    docker exec -e TC_TARGET=archive $NODE mongosh "$COLD"   --quiet --file /root/$h.js > /tmp/${h}_archive.json 2>&1
    python3 test/pilot/diff.py "$h" || rc=1
  done
  return $rc
}

rewarm() {
  # Put the fast tier back to what a warm leaves, which is what the boundary
  # checks in writes.js need. They cannot be re-run against a tier that their
  # own previous run pulled documents into, and eviction will not undo that for
  # half an hour because a copy-up carries a residency hold, which is the point
  # of one. Dropping and warming again is the documented way back: the warmer
  # withdraws a completeness claim it finds the fast tier cannot support.
  echo "== putting the fast tier back to a warm =="
  N "for p in \$(ss -ltnp | grep :27500 | grep -o 'pid=[0-9]*' | cut -d= -f2); do kill -9 \$p; done" >/dev/null 2>&1
  sleep 2
  N "mongosh '$HOT' --quiet --eval 'db.getSiblingDB(\"sample_mflix\").comments.drop()'" >/dev/null
  docker exec -d $NODE cline router --config /root/pilot.yaml --log-file /root/pilotlog/router.log
  for _ in $(seq 1 30); do
    N "mongosh '$ROUTER' --quiet --eval 'db.adminCommand({ping:1})'" >/dev/null 2>&1 && break
    sleep 1
  done
  N "cline warmer --config /root/pilot.yaml 2>&1 | grep 'warm complete'"
}

writes() {
  # The reads above never change anything. This writes to documents the fast
  # tier has never held, which is where a tiered write is hardest: a comment
  # from 1994 exists only in the archive, and an updateMany over 2016 straddles
  # the boundary. It checks everything against the archive rather than against
  # values chosen by hand, and clears up after itself.
  echo "== writing to what is not on the fast tier =="
  docker cp test/pilot/writes.js $NODE:/root/writes.js >/dev/null
  docker exec -e TC_COLD="$COLD" -e TC_HOT="$HOT" $NODE \
    mongosh "$ROUTER" --quiet --file /root/writes.js
}

streams() {
  # What a reactive application sees while the tiers move underneath it.
  echo "== watching a collection while documents move between tiers =="
  docker cp test/pilot/streams.js $NODE:/root/streams.js >/dev/null
  docker exec -e TC_COLD="$COLD" -e TC_HOT="$HOT" $NODE \
    mongosh "$ROUTER" --quiet --file /root/streams.js
}

txn() {
  # Transactions run on the fast tier and cannot reach into the archive from
  # inside themselves, so the router copies what they touch up and aborts with
  # TransientTransactionError for the driver to retry. This uses withTransaction,
  # which is what an application uses, and checks the label directly as well.
  echo "== transactions over documents that are not on the fast tier =="
  docker cp test/pilot/txn.js $NODE:/root/txn.js >/dev/null
  docker exec -e TC_COLD="$COLD" -e TC_HOT="$HOT" $NODE \
    mongosh "$ROUTER" --quiet --file /root/txn.js
}

cache() {
  # The query cache, over data that lives mostly in the archive. It needs its
  # own router, because the cache is off by default and should stay off until it
  # has run somewhere real.
  echo "== the query cache, on =="
  N "sed 's|^  completeness: archive|  cache:\n    enabled: true\n    max_queries: 1000\n    max_bytes: 67108864\n    max_ttl: 1m\n    measure: true\n  completeness: archive|' /root/pilot.yaml > /root/pilot_cache.yaml"
  N "for p in \$(ss -ltnp | grep :27500 | grep -o 'pid=[0-9]*' | cut -d= -f2); do kill -9 \$p; done" >/dev/null 2>&1
  sleep 2
  docker exec -d $NODE cline router --config /root/pilot_cache.yaml --log-file /root/pilotlog/router_cache.log
  for _ in $(seq 1 30); do
    N "mongosh '$ROUTER' --quiet --eval 'db.adminCommand({ping:1})'" >/dev/null 2>&1 && break
    sleep 1
  done
  docker cp test/pilot/cache.js $NODE:/root/cache.js >/dev/null
  docker exec -e TC_COLD="$COLD" -e TC_HOT="$HOT" $NODE \
    mongosh "$ROUTER" --quiet --file /root/cache.js
  # And the whole read battery again, asked three times on one connection so the
  # last pass is the one the cache answered. A held answer compared against the
  # archive is the only way to know it is still the right answer.
  echo "  -- the read battery, answered from the cache --"
  TC_REPEAT=3 compare
  N "curl -s http://127.0.0.1:27510/api/snapshot" | python3 -c "
import sys, json
c = json.load(sys.stdin).get('cache', {})
print('  cache: %s shapes, %s hits, %s misses, watching=%s'
      % (c.get('shapes'), c.get('hits'), c.get('misses'), c.get('watching')))"
}

churn() {
  # Ask the questions while the tiers move underneath them.
  #
  # A warm copying documents up and an evictor taking them away, both against
  # the collections the comparison is reading. Which tier answers is allowed to
  # change; what the answer is, is not.
  #
  # Measured: comments went from 1,443 documents on the fast tier to 5,238 and
  # trips from 0 to 10,000 while three rounds of 212 questions ran, with the
  # query cache on and serving. No disagreement in any round. The counts before
  # and after are the control: without them a quiet run proves only that the
  # churn never started.
  echo "== moving documents between the tiers while a client asks =="
  # A wide window and a residency of seconds. Without the short residency this
  # is a no-op: a copy-up holds a document against eviction for as long as
  # warmer.residency says, so a warm immediately followed by a sweep evicts
  # nothing and the counts before and after are identical. That happened, and
  # the counts are what showed it, which is why they are printed.
  N "sed -e 's|      back: 3650d|      back: 5475d|' -e 's|  residency: 30m|  residency: 5s|' /root/pilot.yaml > /root/pilot_wide.yaml"
  docker cp test/pilot/churn.sh $NODE:/root/churn.sh >/dev/null
  echo "  fast tier before:"; tierCounts
  docker exec -d $NODE bash /root/churn.sh
  sleep 3
  local rc=0
  for i in 1 2 3; do
    echo "  -- round $i --"
    TC_REPEAT=2 compare || rc=1
  done
  echo "  fast tier after:"; tierCounts
  return $rc
}

tierCounts() {
  N "mongosh '$HOT' --quiet --eval '
    [\"sample_mflix.comments\",\"sample_supplies.sales\",\"sample_training.trips\"].forEach(function(ns){
      const p = ns.split(\".\");
      print(\"    \" + ns + \"  \" + db.getSiblingDB(p[0])[p[1]].countDocuments({}));});'"
}

case "${1:-all}" in
  compare) compare ;;
  writes)  writes ;;
  streams) streams ;;
  txn)     txn ;;
  cache)   cache ;;
  churn)   churn ;;
  rewarm)  rewarm ;;
  all)     restore; copyUntiered; start; compare; writes; streams; txn ;;
  *)       echo "usage: $0 [all|compare|writes|streams|txn|cache|churn|rewarm]"; exit 2 ;;
esac
