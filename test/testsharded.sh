#!/bin/bash
# A sharded fast tier, which is not supported, and has to say so.
#
# This runs on the HOST and builds its own throwaway cluster, because the
# anydbver nodes are replica sets and a mongos is the whole point:
#
#   bash test/testsharded.sh
#
# Everything cline does to a fast tier assumes a replica set. The router proxies
# bytes to one primary address and a mongos does not have one. Every reading the
# evictor holds itself back on is a replica set's: replSetGetStatus is "not
# supported through mongos", the local database cannot be reached so there is no
# oplog window, and serverStatus carries no wiredTiger section so neither cache
# dirty nor write tickets exist.
#
# Before this was refused, the router failed to start with "fast tier reported no
# primary address", which is true and says nothing, and the daemons started
# anyway. Measured then: an evictor dry run with five eligible documents reported
# would_evict=5 and backoffs=0, holding itself back on nothing at all.
#
# The check is that all three commands refuse, and that they say why.
BIN=${BIN:-./bin/cline}
DIR=$(mktemp -d)
CFGPORT=${CFGPORT:-28030}
SHPORT=${SHPORT:-28031}
MSPORT=${MSPORT:-28032}
COLDPORT=${COLDPORT:-28033}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-46s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-46s got=%.90s want=%s\n' "$1" "$3" "$2"; fi
}

cleanup() {
  for p in $MSPORT $SHPORT $CFGPORT $COLDPORT; do
    mongosh --quiet --port $p --eval 'db.getSiblingDB("admin").shutdownServer()' >/dev/null 2>&1
  done
  sleep 1
  rm -rf "$DIR"
}
trap cleanup EXIT

echo "== setup =="
mkdir -p "$DIR/cfg" "$DIR/sh" "$DIR/cold"
# Capped, because a mongod sizes its cache from host RAM and this runs beside
# whatever else is on the machine.
mongod --configsvr --replSet shcfg --port $CFGPORT --dbpath "$DIR/cfg" --bind_ip 127.0.0.1 \
  --fork --logpath "$DIR/cfg.log" --wiredTigerCacheSizeGB 0.25 >/dev/null 2>&1
mongod --shardsvr --replSet shsh --port $SHPORT --dbpath "$DIR/sh" --bind_ip 127.0.0.1 \
  --fork --logpath "$DIR/sh.log" --wiredTigerCacheSizeGB 0.25 >/dev/null 2>&1
mongod --replSet shcold --port $COLDPORT --dbpath "$DIR/cold" --bind_ip 127.0.0.1 \
  --fork --logpath "$DIR/cold.log" --wiredTigerCacheSizeGB 0.25 >/dev/null 2>&1
sleep 3
mongosh --quiet --port $CFGPORT --eval "rs.initiate({_id:'shcfg',configsvr:true,members:[{_id:0,host:'127.0.0.1:$CFGPORT'}]})" >/dev/null 2>&1
mongosh --quiet --port $SHPORT   --eval "rs.initiate({_id:'shsh',members:[{_id:0,host:'127.0.0.1:$SHPORT'}]})" >/dev/null 2>&1
mongosh --quiet --port $COLDPORT --eval "rs.initiate({_id:'shcold',members:[{_id:0,host:'127.0.0.1:$COLDPORT'}]})" >/dev/null 2>&1
sleep 6
mongos --configdb "shcfg/127.0.0.1:$CFGPORT" --port $MSPORT --bind_ip 127.0.0.1 \
  --fork --logpath "$DIR/mongos.log" >/dev/null 2>&1
sleep 4
mongosh --quiet --port $MSPORT --eval "sh.addShard('shsh/127.0.0.1:$SHPORT')" >/dev/null 2>&1
chk "the mongos is up and has a shard" "1" \
    "$(mongosh --quiet --port $MSPORT --eval 'print(db.adminCommand({listShards:1}).shards.length)' 2>&1 | tail -1)"

echo "== what a mongos cannot answer =="
chk "hello reports no primary"          "true" \
    "$(mongosh --quiet --port $MSPORT --eval 'const h=db.adminCommand({hello:1}); print(h.msg==="isdbgrid" && !h.primary && !h.me)' 2>&1 | tail -1)"
chk "replSetGetStatus is refused"       "true" \
    "$(mongosh --quiet --port $MSPORT --eval 'try{db.adminCommand({replSetGetStatus:1}); print(false)}catch(e){print(true)}' 2>&1 | tail -1)"
chk "the local database is unreachable" "true" \
    "$(mongosh --quiet --port $MSPORT --eval 'try{db.getSiblingDB("local").oplog.rs.findOne(); print(false)}catch(e){print(true)}' 2>&1 | tail -1)"
chk "serverStatus has no wiredTiger"    "true" \
    "$(mongosh --quiet --port $MSPORT --eval 'print(!db.serverStatus().wiredTiger)' 2>&1 | tail -1)"

cat > "$DIR/cline.yaml" <<YAML
hot:
  uri: "mongodb://127.0.0.1:$MSPORT/"
cold:
  uri: "mongodb://127.0.0.1:$COLDPORT/?replicaSet=shcold"
log:
  file: ""
feeder:
  id: "shardcheck"
  namespaces:
    - "probe.tiered"
evictor:
  feeder_id: "shardcheck"
  rules:
    - ns: "probe.tiered"
      older_than: 1m
# The warmer validates its own config before it connects, so without a rule it
# stops on that and never reaches the question this suite is asking.
warmer:
  rules:
    - ns: "probe.tiered"
      back: 1h
router:
  listen: "127.0.0.1:27750"
  dashboard: "127.0.0.1:27751"
YAML

echo "== every command refuses, and says why =="
refused() { case "$1" in *"sharded fast tiers are not supported"*) echo REFUSED ;; *) echo "$1" ;; esac; }
# Only the evictor takes --once; the others exit on the refusal anyway, and a
# flag they do not know makes them print usage, which reads as a failure of the
# thing being tested rather than of the command line.
chk "router refuses a mongos fast tier" "REFUSED" \
    "$(refused "$(timeout 30 $BIN router --config "$DIR/cline.yaml" 2>&1 | tail -1)")"
chk "feeder refuses a mongos fast tier" "REFUSED" \
    "$(refused "$(timeout 30 $BIN feeder --config "$DIR/cline.yaml" 2>&1 | tail -1)")"
chk "evictor refuses a mongos fast tier" "REFUSED" \
    "$(refused "$(timeout 30 $BIN evictor --config "$DIR/cline.yaml" --once --dry-run 2>&1 | tail -1)")"
chk "warmer refuses a mongos fast tier" "REFUSED" \
    "$(refused "$(timeout 30 $BIN warmer --config "$DIR/cline.yaml" 2>&1 | tail -1)")"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
