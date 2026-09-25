#!/bin/bash
# Does the archive ever hold the second of two writes without the first, when
# the two are in different collections? See main.go.
#
#   bash test/crossns/crossns.sh
#
# Its own feeder and databases. Never run it during a sweep.
NODE=${NODE:-thermocline-zelmar-node0}
FOR=${FOR:-60s}
HOT='mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'
COLD='mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'
CGO_ENABLED=0 go build -buildvcs=false -o bin/crossns ./test/crossns || exit 2
docker cp bin/crossns "$NODE":/usr/local/bin/crossns >/dev/null
docker cp test/crossns/pc.yaml "$NODE":/root/pc.yaml >/dev/null
docker exec "$NODE" bash -c "pkill -f 'config /root/p[c].yaml'" >/dev/null 2>&1
for u in "$HOT" "$COLD"; do
  docker exec "$NODE" mongosh "$u" --quiet --eval 'db.getSiblingDB("probecross").dropDatabase(); db.getSiblingDB("_thermocline_pc").dropDatabase()' >/dev/null
done
docker exec -d "$NODE" bash -c "cline feeder --config /root/pc.yaml --log-file /root/pc_feeder.log"
sleep 4
docker exec "$NODE" crossns -hot "$HOT" -cold "$COLD" -for "$FOR"
rc=$?
docker exec "$NODE" bash -c "pkill -f 'config /root/p[c].yaml'" >/dev/null 2>&1
exit $rc
