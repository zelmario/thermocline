#!/bin/bash
# Item 9 of docs/production-readiness.md: two copy-ups of one document at once,
# and whether a consumer reading late is shown one of them as an insert.
#
#   bash test/echorace/echorace.sh
#   ROUNDS=20 bash test/echorace/echorace.sh
#   OP=update bash test/echorace/echorace.sh     write misses instead of reads
#
# On a box where this checkout is not, build here and run the copy there with
# BUILD=0, setting HOT and COLD to that box's addresses.
#
# One document, written to the archive only, the state an adopted archive is in.
# A feeder, two routers, and the probe in main.go driving rounds of eviction
# and simultaneous reads. Never run it during a sweep.
NODE=${NODE:-thermocline-zelmar-node0}
ROUNDS=${ROUNDS:-10}
READERS=${READERS:-32}
CFG=/root/p9.yaml
CFG2=/root/p9b.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
R1='mongodb://dba:secret@127.0.0.1:27840/?authSource=admin'
R2='mongodb://dba:secret@127.0.0.1:27860/?authSource=admin'

N() { docker exec "$NODE" bash -c "$1"; }
ND() { docker exec -d "$NODE" bash -c "$1"; }
Q() { docker exec "$NODE" mongosh "$1" --quiet --eval "$2" 2>/dev/null | tail -1; }

if [ "${BUILD:-1}" = 1 ]; then
  CGO_ENABLED=0 go build -buildvcs=false -o bin/echorace ./test/echorace || exit 2
fi
docker cp bin/echorace "$NODE":/usr/local/bin/echorace >/dev/null
docker cp test/echorace/p9.yaml "$NODE":$CFG >/dev/null
N "sed -e 's|0.0.0.0:27840|0.0.0.0:27860|' -e 's|0.0.0.0:27850|0.0.0.0:27870|' $CFG > $CFG2"
N "grep -q 27860 $CFG2" || { echo "FATAL could not derive the second router"; exit 2; }

N "for p in \$(pgrep -f 'config /root/p9'); do kill -9 \$p; done" >/dev/null 2>&1
sleep 1
for u in "$HOT" "$COLD"; do
  Q "$u" 'db.getSiblingDB("probe9").dropDatabase(); db.getSiblingDB("_thermocline_p9").dropDatabase()' >/dev/null
done

N "mkdir -p /root/p9log; rm -f /root/p9log/*"
ND "cline feeder --config $CFG --log-file /root/p9log/feeder.log"
sleep 4
# Old enough for the evictor's rule, and in the archive alone.
Q "$COLD" 'db.getSiblingDB("probe9").docs.insertOne({_id: ObjectId("5f0000000000000000000001"), v: "archived"})' >/dev/null
ND "cline router --config $CFG --log-file /root/p9log/router1.log"
ND "cline router --config $CFG2 --log-file /root/p9log/router2.log"
for r in "$R1" "$R2"; do
  for _ in $(seq 1 30); do [ "$(Q "$r" 'print(db.adminCommand({ping:1}).ok)')" = 1 ] && break; sleep 1; done
done

N "echorace -r1 '$R1' -r2 '$R2' -hot '$HOT' -cold '$COLD' -config $CFG -rounds $ROUNDS -readers $READERS -late ${LATE:-35s} -calm ${CALM:-0} -op ${OP:-read}"
rc=$?
N "grep -h 'letting an insert through' /root/p9log/router*.log | cut -c1-300 | head -5"
N "for p in \$(pgrep -f 'config /root/p9'); do kill -9 \$p; done" >/dev/null 2>&1
exit $rc
