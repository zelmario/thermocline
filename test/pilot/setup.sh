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
  # The whole point. Nothing here asserts an expected value: the archive is the
  # control, and the only claim being checked is that a client cannot tell the
  # difference. See README.md in this directory.
  echo "== asking both the same questions =="
  docker cp test/pilot/agree.js $NODE:/root/agree.js
  N "mongosh '$ROUTER' --quiet --file /root/agree.js > /root/agree_router.json 2>&1"
  N "mongosh '$COLD'   --quiet --file /root/agree.js > /root/agree_archive.json 2>&1"
  docker cp $NODE:/root/agree_router.json /tmp/agree_router.json
  docker cp $NODE:/root/agree_archive.json /tmp/agree_archive.json
  python3 - <<'PY'
import json, sys
try:
    r = json.load(open("/tmp/agree_router.json"))
    a = json.load(open("/tmp/agree_archive.json"))
except Exception as e:
    print("  could not read an answer set, so nothing was compared:", e); sys.exit(2)
ra = {(x["ns"], x["q"]): x["v"] for x in r}
aa = {(x["ns"], x["q"]): x["v"] for x in a}
keys = sorted(set(ra) | set(aa))
diff = [k for k in keys if ra.get(k) != aa.get(k)]
print(f"  {len(keys)} questions asked, {len(diff)} disagreements")
for k in diff:
    print(f"  {k[0]:38} {k[1]:22}\n      router  {str(ra.get(k))[:110]}\n      archive {str(aa.get(k))[:110]}")
sys.exit(1 if diff else 0)
PY
}

case "${1:-all}" in
  compare) compare ;;
  all)     restore; copyUntiered; start; compare ;;
  *)       echo "usage: $0 [all|compare]"; exit 2 ;;
esac
