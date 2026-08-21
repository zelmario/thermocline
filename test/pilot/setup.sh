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
    N "mongosh '$ROUTER' --quiet --file /root/$h.js > /root/${h}_router.json 2>&1"
    N "mongosh '$COLD'   --quiet --file /root/$h.js > /root/${h}_archive.json 2>&1"
    docker cp $NODE:/root/${h}_router.json /tmp/${h}_router.json >/dev/null
    docker cp $NODE:/root/${h}_archive.json /tmp/${h}_archive.json >/dev/null
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

case "${1:-all}" in
  compare) compare ;;
  writes)  writes ;;
  rewarm)  rewarm ;;
  all)     restore; copyUntiered; start; compare; writes ;;
  *)       echo "usage: $0 [all|compare|writes|rewarm]"; exit 2 ;;
esac
