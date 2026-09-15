#!/bin/bash
# `feeder.namespaces` empty, which tiers everything.
#
#   bash test/testeverything.sh
#
# The configuration says so plainly -- "Empty mirrors everything except system
# and Thermocline's own databases" -- and RouterConfig.Manages honours it by
# answering true for every namespace. Nine other places read the list directly,
# and for them empty means nothing rather than everything, so the deployment
# that tiers the most was the one several checks did not run for at all.
#
# It runs on the HOST, on two mongods of its own, because a router that tiers
# everything would reach every database on the shared test cluster and build
# indexes across all of them.
HOTPORT=${HOTPORT:-28080}
COLDPORT=${COLDPORT:-28081}
PORT=${PORT:-28085}
DIR=$(mktemp -d /tmp/tceverything.XXXXXX)
BIN=${BIN:-$(pwd)/bin/cline}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%.90s want=%s\n' "$1" "$3" "$2"; fi
}
cleanup() {
  pkill -9 -f "cline.*$DIR" 2>/dev/null
  docker rm -f tc-all-hot tc-all-cold >/dev/null 2>&1
  rm -rf "$DIR"
}
trap cleanup EXIT
M() { mongosh --quiet --port "$1" --eval "$2" 2>&1 | tail -1; }
R() { timeout 40 mongosh "mongodb://127.0.0.1:$PORT/?directConnection=true" --quiet --eval "$1" 2>&1 | tail -1; }

echo "== setup =="
docker image inspect mongo:8.0 >/dev/null 2>&1 || { echo "  FATAL  docker pull mongo:8.0"; exit 2; }
[ -x "$BIN" ] || { echo "  FATAL  no binary at $BIN; run make build"; exit 2; }
docker rm -f tc-all-hot tc-all-cold >/dev/null 2>&1
docker run -d --rm --name tc-all-hot  -p $HOTPORT:$HOTPORT  mongo:8.0 \
  mongod --replSet allhot  --port $HOTPORT  --bind_ip_all --wiredTigerCacheSizeGB 0.25 --oplogSize 128 >/dev/null 2>&1
docker run -d --rm --name tc-all-cold -p $COLDPORT:$COLDPORT mongo:8.0 \
  mongod --replSet allcold --port $COLDPORT --bind_ip_all --wiredTigerCacheSizeGB 0.25 --oplogSize 128 >/dev/null 2>&1
for p in $HOTPORT $COLDPORT; do
  for _ in $(seq 1 40); do mongosh --quiet --port $p --eval 'db.hello()' >/dev/null 2>&1 && break; sleep 1; done
done
M $HOTPORT  "rs.initiate({_id:'allhot',members:[{_id:0,host:'127.0.0.1:$HOTPORT'}]})"  >/dev/null
M $COLDPORT "rs.initiate({_id:'allcold',members:[{_id:0,host:'127.0.0.1:$COLDPORT'}]})" >/dev/null
sleep 6

# The archive is the existing cluster: two collections, one carrying a unique
# index the fast tier has never heard of. That is the adoption shape.
M $COLDPORT 'db.getSiblingDB("shop").orders.insertMany([{_id:1,code:"a",n:1},{_id:2,code:"b",n:2}]);
             db.getSiblingDB("shop").orders.createIndex({code:1},{name:"code_u",unique:true});
             db.getSiblingDB("shop").audit.insertMany([{_id:1,what:"x"},{_id:2,what:"y"}]);' >/dev/null
chk "the archive holds two collections"                   "2" \
    "$(M $COLDPORT 'print(db.getSiblingDB("shop").getCollectionNames().filter(n=>!n.startsWith("system.")).length)')"
chk "and a unique index the fast tier does not have"      "true" \
    "$(M $COLDPORT 'print(db.getSiblingDB("shop").orders.getIndexes().some(i=>i.name==="code_u"))')"

cat > "$DIR/all.yaml" <<YAML
hot:
  uri: "mongodb://127.0.0.1:$HOTPORT/?replicaSet=allhot"
cold:
  uri: "mongodb://127.0.0.1:$COLDPORT/?replicaSet=allcold"
log:
  file: ""
feeder:
  id: "all"
  checkpoint_every: 1s
router:
  listen: "127.0.0.1:$PORT"
  dashboard: ""
  completeness: archive
YAML
chk "the config names no namespaces at all"               "0" \
    "$(grep -c 'namespaces' "$DIR/all.yaml")"

echo "== what a router that tiers everything does at startup =="
"$BIN" router --config "$DIR/all.yaml" --log-file "$DIR/r.log" >/dev/null 2>&1 &
disown
for _ in $(seq 1 40); do grep -q 'router listening' "$DIR/r.log" 2>/dev/null && break; sleep 1; done
chk "the router starts"                                   "true" \
    "$(grep -q 'router listening' "$DIR/r.log" 2>/dev/null && echo true || echo false)"
# The one that costs data rather than a report: without the archive's unique
# index the fast tier cannot refuse a duplicate, and the registry is not
# protecting the values the archive holds either.
chk "it built the archive's unique index on the fast tier" "true" \
    "$(M $HOTPORT 'print(db.getSiblingDB("shop").orders.getIndexes().some(i=>i.name==="code_u"))')"
chk "and said the constraint has no claims yet"           "true" \
    "$(grep -q 'has no claims in the registry' "$DIR/r.log" && echo true || echo false)"
chk "naming the namespace"                                "true" \
    "$(grep 'has no claims in the registry' "$DIR/r.log" | grep -q 'shop.orders' && echo true || echo false)"

echo "== dbHash over a database it tiers =="
# The guard exists to stop a hash of the fast tier's half being passed off as
# the database's. It reads the configured list, so with everything tiered it
# found nothing to be careful about.
DBH=$(R 'try { const r = db.getSiblingDB("shop").runCommand({dbHash:1}); print("answered "+JSON.stringify(r.thermocline||r.md5||"none")) } catch(e) { print("refused: "+(e.message||e.codeName)) }')
echo "      dbHash answered: $DBH"
chk "dbHash is refused rather than answered short"        "true" \
    "$(grep -q 'refusing this dbHash' <<<"$DBH" && echo true || echo false)"
chk "and it names both tiered collections"               "true" \
    "$(grep -q 'audit' <<<"$DBH" && grep -q 'orders' <<<"$DBH" && echo true || echo false)"

echo "== a collection the fast tier has and the archive does not =="
M $HOTPORT 'db.getSiblingDB("shop").stray.insertMany([{_id:1},{_id:2},{_id:3}])' >/dev/null
pkill -9 -f "cline.*$DIR" 2>/dev/null; sleep 1
rm -f "$DIR/r2.log"
"$BIN" router --config "$DIR/all.yaml" --log-file "$DIR/r2.log" >/dev/null 2>&1 &
disown
for _ in $(seq 1 40); do grep -q 'router listening' "$DIR/r2.log" 2>/dev/null && break; sleep 1; done
chk "the router names it"                                 "true" \
    "$(grep -q 'the fast tier holds tiered collections the archive does not' "$DIR/r2.log" && echo true || echo false)"
chk "and says which"                                      "true" \
    "$(grep 'the fast tier holds tiered collections the archive does not' "$DIR/r2.log" | grep -q 'shop.stray' && echo true || echo false)"
pkill -9 -f "cline.*$DIR" 2>/dev/null

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
