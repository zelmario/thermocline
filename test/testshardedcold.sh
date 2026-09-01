#!/bin/bash
# A sharded archive, which is the shape the design is for.
#
# testsharded.sh covers the other side: a sharded fast tier, refused, because
# everything the router and the evictor need of a fast tier is a replica set's.
# The archive is the opposite case. It is the big cheap cluster holding
# everything, and big cheap clusters are sharded, so this has to work rather
# than be refused. It has never run.
#
#   bash test/testshardedcold.sh
#
# Runs on the HOST and builds its own throwaway cluster, because the anydbver
# nodes are replica sets and a mongos is the whole point: a config server, one
# shard and a mongos for the archive, and a single node replica set for the fast
# tier.
#
# What made this plausible before it was tried: every health reading the daemons
# take is of the fast tier, so a mongos answering none of them does not matter.
# The archive is only ever asked for ordinary queries, ordinary writes, and hello
# for its clock, and a mongos answers all three.
BIN=${BIN:-./bin/cline}
DIR=$(mktemp -d)
CFGPORT=${CFGPORT:-28070}
SHPORT=${SHPORT:-28071}
MSPORT=${MSPORT:-28072}
HOTPORT=${HOTPORT:-28073}
PORT=${PORT:-27840}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-48s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-48s got=%.90s want=%s\n' "$1" "$3" "$2"; fi
}
cleanup() {
  # KEEP=1 leaves the cluster and the config behind, which is the only way to
  # ask it anything after a check has failed.
  if [ -n "$KEEP" ]; then
    echo "kept: config $DIR/cline.yaml, mongos on $MSPORT, fast tier on $HOTPORT"
    return
  fi
  # By config path and in brackets, so this line does not match itself.
  pkill -9 -f "cline .*--config $DIR/clin[e].yaml" 2>/dev/null
  for p in $MSPORT $SHPORT $CFGPORT $HOTPORT; do
    mongosh --quiet --port $p --eval 'db.getSiblingDB("admin").shutdownServer()' >/dev/null 2>&1
  done
  sleep 1
  rm -rf "$DIR"
}
trap cleanup EXIT

MS() { mongosh --quiet --port $MSPORT --eval "$1" 2>&1 | tail -1; }
H()  { mongosh --quiet --port $HOTPORT --eval "$1" 2>&1 | tail -1; }
R()  { mongosh --quiet --port $PORT --eval "$1" 2>&1 | tail -1; }

echo "== setup =="
mkdir -p "$DIR/cfg" "$DIR/sh" "$DIR/hot"
mongod --configsvr --replSet cscfg --port $CFGPORT --dbpath "$DIR/cfg" --bind_ip 127.0.0.1 \
  --fork --logpath "$DIR/cfg.log" --wiredTigerCacheSizeGB 0.25 >/dev/null 2>&1
mongod --shardsvr --replSet cssh --port $SHPORT --dbpath "$DIR/sh" --bind_ip 127.0.0.1 \
  --fork --logpath "$DIR/sh.log" --wiredTigerCacheSizeGB 0.25 >/dev/null 2>&1
mongod --replSet cshot --port $HOTPORT --dbpath "$DIR/hot" --bind_ip 127.0.0.1 \
  --fork --logpath "$DIR/hot.log" --wiredTigerCacheSizeGB 0.25 --oplogSize 128 >/dev/null 2>&1
sleep 3
mongosh --quiet --port $CFGPORT --eval "rs.initiate({_id:'cscfg',configsvr:true,members:[{_id:0,host:'127.0.0.1:$CFGPORT'}]})" >/dev/null 2>&1
mongosh --quiet --port $SHPORT  --eval "rs.initiate({_id:'cssh',members:[{_id:0,host:'127.0.0.1:$SHPORT'}]})" >/dev/null 2>&1
mongosh --quiet --port $HOTPORT --eval "rs.initiate({_id:'cshot',members:[{_id:0,host:'127.0.0.1:$HOTPORT'}]})" >/dev/null 2>&1
sleep 6
mongos --configdb "cscfg/127.0.0.1:$CFGPORT" --port $MSPORT --bind_ip 127.0.0.1 \
  --fork --logpath "$DIR/mongos.log" >/dev/null 2>&1
sleep 4
MS "sh.addShard('cssh/127.0.0.1:$SHPORT')" >/dev/null
chk "the archive is a mongos with a shard"          "1" "$(MS 'print(db.adminCommand({listShards:1}).shards.length)')"
chk "and the fast tier is a replica set"            "cshot" "$(H 'print(rs.status().set)')"

# Genuinely sharded, not merely behind a mongos: the tiered collection is
# distributed, so every read and write the archive answers crosses that.
MS "sh.enableSharding('probe')" >/dev/null
MS "sh.shardCollection('probe.tiered', {_id:'hashed'})" >/dev/null
chk "the tiered collection is sharded"              "true" \
    "$(MS 'print(!!db.getSiblingDB("config").collections.findOne({_id:"probe.tiered"}))')"

cat > "$DIR/cline.yaml" <<YAML
hot:
  uri: "mongodb://127.0.0.1:$HOTPORT/?replicaSet=cshot"
cold:
  uri: "mongodb://127.0.0.1:$MSPORT/"
log:
  file: ""
feeder:
  id: "shardedcold"
  namespaces:
    - "probe.tiered"
  checkpoint_every: 1s
  max_await: 200ms
evictor:
  feeder_id: "shardedcold"
  rate: 500
  batch_size: 100
  max_feeder_lag: 30s
  rules:
    - ns: "probe.tiered"
      older_than: 1m
  backoff:
    min_oplog_window: 1m
warmer:
  rate: 500
  batch_size: 50
  residency: 1m
  rules:
    - ns: "probe.tiered"
      back: 1m
  backoff:
    min_oplog_window: 1m
router:
  listen: "127.0.0.1:$PORT"
  dashboard: ""
  completeness: archive
  residency: 1m
  archive_wait: 5s
YAML

echo "== the daemons start against it =="
$BIN feeder --config "$DIR/cline.yaml" --log-file "$DIR/feeder.log" >/dev/null 2>&1 &
sleep 3
chk "the feeder started"                            "true" \
    "$(grep -q 'connected to both tiers' "$DIR/feeder.log" && echo true || echo false)"
$BIN router --config "$DIR/cline.yaml" --log-file "$DIR/router.log" >/dev/null 2>&1 &
for _ in $(seq 1 30); do
  grep -q 'router listening' "$DIR/router.log" 2>/dev/null && break
  sleep 1
done
chk "the router started, rather than refusing"      "true" \
    "$(grep -q 'router listening' "$DIR/router.log" && echo true || echo false)"
if ! grep -q 'router listening' "$DIR/router.log"; then
  echo "  FATAL the router never came up, so nothing below means anything"
  tail -4 "$DIR/router.log"; exit 2
fi
chk "and said nothing about a sharded tier"         "true" \
    "$(grep -qi 'sharded' "$DIR/router.log" && echo false || echo true)"
# Both tiers' releases are read through the mongos, which answers buildInfo.
# What it does not answer is getParameter for featureCompatibilityVersion, so
# that half of the check is skipped rather than failing the start: a version it
# cannot read is never a refusal. See internal/tierver.
chk "it read both tiers' versions through the mongos" "true" \
    "$(grep -q 'msg="tier versions"' "$DIR/router.log" && echo true || echo false)"
chk "and did not refuse over the FCV it cannot read"  "true" \
    "$(grep -qi 'featureCompatibilityVersion' "$DIR/router.log" && echo false || echo true)"

echo "== mirroring, through the mongos =="
# ObjectId ids, because the eviction rule below has no date_field and takes each
# document's age from the timestamp inside its _id. With integer ids the evictor
# refuses the whole namespace, correctly, and every check after it measures that
# refusal rather than the archive being sharded.
NOW=$(date +%s)
A=$(printf '%08x%016x' "$NOW" 11)
B=$(printf '%08x%016x' "$NOW" 12)
C=$(printf '%08x%016x' "$NOW" 13)
R "db.getSiblingDB('probe').tiered.insertMany([{_id:ObjectId('$A'),v:'one'},{_id:ObjectId('$B'),v:'two'}])" >/dev/null
for _ in $(seq 1 40); do
  [ "$(MS 'print(db.getSiblingDB("probe").tiered.countDocuments({}))')" = "2" ] && break
  sleep 0.5
done
chk "both writes reached the sharded archive"       "2" "$(MS 'print(db.getSiblingDB("probe").tiered.countDocuments({}))')"
chk "and the fast tier has them too"                "2" "$(H 'print(db.getSiblingDB("probe").tiered.countDocuments({}))')"
chk "the checkpoint is in the sharded archive"      "true" \
    "$(MS 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"shardedcold"}))')"

echo "== a read the fast tier cannot answer =="
# Straight into the archive, so the fast tier has never held it: the shape an
# evicted document leaves.
MS "db.getSiblingDB('probe').tiered.insertOne({_id:ObjectId('$C'),v:'archive only'})" >/dev/null
chk "the archive holds three"                       "3" "$(MS 'print(db.getSiblingDB("probe").tiered.countDocuments({}))')"
chk "the fast tier still holds two"                 "2" "$(H 'print(db.getSiblingDB("probe").tiered.countDocuments({}))')"
chk "the router answers three"                      "3" "$(R 'print(db.getSiblingDB("probe").tiered.countDocuments({}))')"
chk "and finds the archived document by field"      "archive only" \
    "$(R 'const d=db.getSiblingDB("probe").tiered.findOne({v:"archive only"}); print(d?d.v:"MISSING")')"

echo "== a delete is a mark in the archive, not a removal =="
R "db.getSiblingDB('probe').tiered.deleteOne({_id:ObjectId('$A')})" >/dev/null
for _ in $(seq 1 40); do
  [ "$(MS "print(db.getSiblingDB('probe').tiered.countDocuments({_id:ObjectId('$A'),_tcDeletedAt:{\$exists:true}}))")" = "1" ] && break
  sleep 0.5
done
chk "the archive kept it as a tombstone"            "1" \
    "$(MS "print(db.getSiblingDB('probe').tiered.countDocuments({_id:ObjectId('$A'),_tcDeletedAt:{\$exists:true}}))")"
chk "and the router does not return it"             "0" \
    "$(R "print(db.getSiblingDB('probe').tiered.countDocuments({_id:ObjectId('$A')}))")"

echo "== the commands that count both tiers =="
# The claim is refused, and that is the answer being checked: this fixture put a
# document into the archive alone, so the fast tier really is one short. What it
# proves about a sharded archive is that the count came back from the mongos at
# all, and that both numbers are right.
WM=$($BIN watermark --config "$DIR/cline.yaml" --ns probe.tiered --complete-all 2>&1)
[ -n "$KEEP" ] && printf '    watermark said: %s\n' "$(head -3 <<<"$WM" | tr '\n' ' ')"
chk "the claim is refused, counting both tiers"     "true" \
    "$(grep -qE 'holds [0-9]+ of everything in probe.tiered and the archive holds [0-9]+' <<<"$WM" && echo true || echo false)"
chk "and --force records it anyway"                 "true" \
    "$($BIN watermark --config "$DIR/cline.yaml" --ns probe.tiered --complete-all --force 2>&1 | grep -qi 'all of history' && echo true || echo false)"
# Withdrawn again, because a claim over a fast tier that is genuinely short
# would make every check below read from it and answer short.
$BIN watermark --config "$DIR/cline.yaml" --ns probe.tiered --clear >/dev/null 2>&1
chk "and keys reports on it"                        "true" \
    "$($BIN keys --config "$DIR/cline.yaml" 2>&1 | grep -qiE 'nothing to enforce|unique indexes' && echo true || echo false)"

echo "== an evictor sweep against a sharded archive =="
# Documents old enough to evict: the rule is a minute, and these are dated two
# hours ago by their own ObjectIds.
OLD=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 1)
R "db.getSiblingDB('probe').tiered.insertOne({_id:ObjectId('$OLD'),v:'old enough'})" >/dev/null
for _ in $(seq 1 40); do
  [ "$(MS "print(db.getSiblingDB('probe').tiered.countDocuments({_id:ObjectId('$OLD')}))")" = "1" ] && break
  sleep 0.5
done
EV=$($BIN evictor --config "$DIR/cline.yaml" --once -v 2>&1)
[ -n "$KEEP" ] && printf '    evictor said: %s\n' "$(grep -E 'sweep complete|not evicting|unconfirmed|would_evict' <<<"$EV" | tail -3 | tr '\n' ' ')"
chk "the sweep completed"                           "true" \
    "$(grep -q 'sweep complete' <<<"$EV" && echo true || echo false)"
# A backoff is ordinary: the sweep waits while mirroring catches up. What must
# not happen is a signal the evictor cannot read at all, which is what a mongos
# would cause if any of them came from the archive. They all come from the fast
# tier, and this is the check that says so.
chk "no health signal was missing"                 "true" \
    "$(grep -qiE 'missing signal|not healthy enough|no oplog window' <<<"$EV" && echo false || echo true)"
chk "the old document left the fast tier"           "0" \
    "$(H "print(db.getSiblingDB('probe').tiered.countDocuments({_id:ObjectId('$OLD')}))")"
chk "and the sharded archive still has it"          "1" \
    "$(MS "print(db.getSiblingDB('probe').tiered.countDocuments({_id:ObjectId('$OLD'),_tcDeletedAt:{\$exists:false}}))")"
chk "so the router still answers with it"           "old enough" \
    "$(R "const d=db.getSiblingDB('probe').tiered.findOne({v:'old enough'}); print(d?d.v:'MISSING')")"

echo "== nothing was refused or panicked =="
chk "no panics in either log"                       "0" \
    "$({ grep -c -i panic "$DIR/router.log" "$DIR/feeder.log" 2>/dev/null | awk -F: '{s+=$2} END{print s+0}'; } | tail -1)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
