#!/bin/bash
# A rolling upgrade, under traffic, in both orders.
#
#   bash test/verskew.sh
#
# docs/limitations.md says to upgrade the archive first, and said so as an
# argument: the archive runs the client's own pipeline, so what breaks is a
# stage newer than the archive, and an archive ahead of the fast tier has no
# equivalent. testversion.sh measures the query half of that on a static pair
# and checks that every daemon refuses to start on one. What neither measured is
# the half the ordering is actually about: a deployment *running* while the two
# tiers are a release apart, with the feeder mirroring and clients writing.
#
# So: two arms, the same traffic through each, and the difference between them
# is which tier is ahead.
#
#   A  archive 8.0, fast tier 7.0   the window the documented order opens
#   B  archive 7.0, fast tier 8.0   the window the other order opens
#
# This runs on the HOST, because it needs two MongoDB releases and the cluster
# the other suites use is one release throughout.
HOTA=${HOTA:-28070}   # the 7.0 server
HOTB=${HOTB:-28071}   # the 8.0 server
HOTC=${HOTC:-28072}   # a second 8.0 server, so the control pair is equal
PORT=${PORT:-28075}
DIR=$(mktemp -d /tmp/verskew.XXXXXX)
BIN=${BIN:-$(pwd)/bin/cline}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%.90s want=%s\n' "$1" "$3" "$2"; fi
}
cleanup() {
  pkill -9 -f "cline.*$DIR" 2>/dev/null
  docker rm -f tc-skew-7 tc-skew-8 tc-skew-8b >/dev/null 2>&1
  rm -rf "$DIR"
}
trap cleanup EXIT

M() { mongosh --quiet --port "$1" --eval "$2" 2>&1 | tail -1; }
R() { timeout 40 mongosh "mongodb://127.0.0.1:$PORT/?directConnection=true" --quiet --eval "$1" 2>&1 | tail -1; }

echo "== setup =="
for img in mongo:8.0 mongo:7.0; do
  docker image inspect "$img" >/dev/null 2>&1 || {
    echo "  FATAL  $img is not here. docker pull mongo:8.0 mongo:7.0"; exit 2; }
done
[ -x "$BIN" ] || { echo "  FATAL  no binary at $BIN; run make build"; exit 2; }
docker rm -f tc-skew-7 tc-skew-8 >/dev/null 2>&1
docker run -d --rm --name tc-skew-7 -p $HOTA:$HOTA mongo:7.0 \
  mongod --replSet skew7 --port $HOTA --bind_ip_all --wiredTigerCacheSizeGB 0.25 --oplogSize 128 >/dev/null 2>&1
docker run -d --rm --name tc-skew-8 -p $HOTB:$HOTB mongo:8.0 \
  mongod --replSet skew8 --port $HOTB --bind_ip_all --wiredTigerCacheSizeGB 0.25 --oplogSize 128 >/dev/null 2>&1
docker run -d --rm --name tc-skew-8b -p $HOTC:$HOTC mongo:8.0 \
  mongod --replSet skew8b --port $HOTC --bind_ip_all --wiredTigerCacheSizeGB 0.25 --oplogSize 128 >/dev/null 2>&1
for p in $HOTA $HOTB $HOTC; do
  for _ in $(seq 1 40); do mongosh --quiet --port $p --eval 'db.hello()' >/dev/null 2>&1 && break; sleep 1; done
done
M $HOTA "rs.initiate({_id:'skew7',members:[{_id:0,host:'127.0.0.1:$HOTA'}]})" >/dev/null
M $HOTB "rs.initiate({_id:'skew8',members:[{_id:0,host:'127.0.0.1:$HOTB'}]})" >/dev/null
M $HOTC "rs.initiate({_id:'skew8b',members:[{_id:0,host:'127.0.0.1:$HOTC'}]})" >/dev/null
sleep 6
REL7=$(M $HOTA 'const v=db.getSiblingDB("admin").runCommand({buildInfo:1}).versionArray; print(v[0]+"."+v[1])')
REL8=$(M $HOTB 'const v=db.getSiblingDB("admin").runCommand({buildInfo:1}).versionArray; print(v[0]+"."+v[1])')
echo "      $REL7 on $HOTA, $REL8 on $HOTB"
chk "two releases, not one"                               "true" \
    "$([ -n "$REL7" ] && [ -n "$REL8" ] && [ "$REL7" != "$REL8" ] && echo true || echo false)"

writeConfig() { # file hotport hotset coldport coldset
  cat > "$1" <<YAML
allow_version_skew: true
hot:
  uri: "mongodb://127.0.0.1:$2/?replicaSet=$3"
cold:
  uri: "mongodb://127.0.0.1:$4/?replicaSet=$5"
log:
  file: ""
feeder:
  id: "skew"
  namespaces:
    - "probe.orders"
  checkpoint_every: 1s
evictor:
  feeder_id: "skew"
  rules:
    - ns: "probe.orders"
      older_than: 3m
  backoff:
    min_oplog_window: 1m
warmer:
  rules:
    - ns: "probe.orders"
      back: 60m
  backoff:
    min_oplog_window: 1m
router:
  listen: "127.0.0.1:$PORT"
  dashboard: ""
  completeness: archive
YAML
}

# One arm. Wipe both servers, start the feeder and the router, write through the
# router, and ask three questions: did the writes reach the archive, does a read
# that has to cross both tiers answer, and did the feeder stop.
arm() { # label hotport hotset coldport coldset
  local label=$1 hp=$2 hs=$3 cp=$4 cs=$5
  echo "== $label: fast tier $(M $hp 'print(db.version())'), archive $(M $cp 'print(db.version())') =="
  pkill -9 -f "cline.*$DIR" 2>/dev/null; sleep 1
  M $hp 'db.getSiblingDB("probe").dropDatabase(); db.getSiblingDB("_thermocline").dropDatabase()' >/dev/null
  M $cp 'db.getSiblingDB("probe").dropDatabase(); db.getSiblingDB("_thermocline").dropDatabase()' >/dev/null
  local cfg=$DIR/$label.yaml
  writeConfig "$cfg" "$hp" "$hs" "$cp" "$cs"

  # The archive holds documents the fast tier never had, which is what makes a
  # read below have to reach it rather than answer from the cache.
  M $cp 'db.getSiblingDB("probe").orders.insertMany([{_id:900,old:true},{_id:901,old:true}])' >/dev/null

  "$BIN" feeder --config "$cfg" --log-file "$DIR/$label.feeder.log" >/dev/null 2>&1 &
  disown
  "$BIN" router --config "$cfg" --log-file "$DIR/$label.router.log" >/dev/null 2>&1 &
  disown
  for _ in $(seq 1 40); do grep -q 'router listening' "$DIR/$label.router.log" 2>/dev/null && break; sleep 1; done
  chk "  the router starts on the unequal pair"           "true" \
      "$(grep -q 'router listening' "$DIR/$label.router.log" 2>/dev/null && echo true || echo false)"
  chk "  and the feeder does"                             "true" \
      "$(grep -q 'msg=mirroring' "$DIR/$label.feeder.log" 2>/dev/null && echo true || echo false)"

  # Traffic. Ordinary documents, an index, and a collection option, because a
  # release gap shows up in what one side will store rather than in a number.
  R 'for (let i=0;i<200;i++) db.getSiblingDB("probe").orders.insertOne({_id:i,n:i,at:new Date()}); print("written")' >/dev/null
  R 'db.getSiblingDB("probe").orders.createIndex({n:1},{name:"n_1"}); print("indexed")' >/dev/null
  R 'for (let i=0;i<200;i+=2) db.getSiblingDB("probe").orders.updateOne({_id:i},{$set:{n:i*2}}); print("updated")' >/dev/null
  R 'for (let i=1;i<40;i+=2) db.getSiblingDB("probe").orders.deleteOne({_id:i}); print("deleted")' >/dev/null

  # Mirroring is asynchronous, so wait for it rather than sampling it once.
  local want=$(( 200 - 20 ))
  for _ in $(seq 1 40); do
    [ "$(M $cp 'print(db.getSiblingDB("probe").orders.countDocuments({_tcDeletedAt:{$exists:false},_id:{$lt:900}}))')" = "$want" ] && break
    sleep 1
  done
  chk "  every write reached the archive"                 "$want" \
      "$(M $cp 'print(db.getSiblingDB("probe").orders.countDocuments({_tcDeletedAt:{$exists:false},_id:{$lt:900}}))')"
  # Not a check that it reached the archive, because it does not and that is the
  # contract: a plain index is a decision about speed, the two tiers are allowed
  # to differ, and `cline indexes` is what reports the difference. Asserted the
  # other way round so the day that changes is noticed, and so this section is
  # not read as a release-gap finding when the control does the same thing.
  chk "  the index stayed on the fast tier, as documented"  "true" \
      "$(M $hp 'print(db.getSiblingDB("probe").orders.getIndexes().some(i=>i.name==="n_1"))')"
  chk "  and the archive still has only its own"            "false" \
      "$(M $cp 'print(db.getSiblingDB("probe").orders.getIndexes().some(i=>i.name==="n_1"))')"
  # The reason this matters more than a count: one refusal stops mirroring for
  # every namespace, not just the one that caused it.
  # grep -c prints 0 and exits 1 when it matches nothing, so the obvious
  # `|| echo 0` prints it twice and the check fails against a clean run.
  chk "  the feeder never refused a write"                "0" \
      "$(grep -c 'the archive refuses' "$DIR/$label.feeder.log" 2>/dev/null | head -1)"
  chk "  and is still running"                            "true" \
      "$(pgrep -f "cline feeder --config $cfg" >/dev/null && echo true || echo false)"

  # A read whose answer is on both sides of the boundary.
  chk "  a read across both tiers answers"                "182" \
      "$(R 'print(db.getSiblingDB("probe").orders.countDocuments({}))')"
  pkill -9 -f "cline.*$cfg" 2>/dev/null; sleep 1
}

# The control, and it comes first. Every check below is about what a release gap
# costs, and a check that fails on an equal pair is measuring something else.
arm control "$HOTB" skew8 "$HOTC" skew8b
arm A "$HOTA" skew7 "$HOTB" skew8
arm B "$HOTB" skew8 "$HOTA" skew7

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
