#!/bin/bash
# Both tiers must run the same MongoDB release, and every daemon has to refuse
# a pair that does not.
#
# Runs on the HOST and builds its own three servers, because the anydbver nodes
# are all one version and a mismatch cannot be faked: the check reads buildInfo
# from the servers themselves.
#
#   bash test/testversion.sh
#
# Three of them, from images that are already here:
#
#   28060  mongo:8.0  8.0.26   a fast tier
#   28061  mongo:7.0  7.0.37   an archive a release behind it
#   28062  mongo:7    7.0.32   an archive a *patch* behind 28061
#
# So two pairs. 28060 with 28061 is the shape this refuses, and it is the
# ordinary adoption shape: the cluster being adopted is the older side. 28061
# with 28062 is a rolling upgrade half done, which has to keep working.
#
# An exactly matched pair is not built here. Every other suite runs on one.
BIN=${BIN:-./bin/cline}
DIR=$(mktemp -d)
HOTPORT=${HOTPORT:-28060}
COLDPORT=${COLDPORT:-28061}
PATCHPORT=${PATCHPORT:-28062}
PORT=${PORT:-27830}
PASS=0; FAIL=0; declare -a FAILED
# refused reads a captured exit code. 124 is timeout's, which means the daemon
# was still running and had to be killed: that is the opposite of a refusal and
# the pre-change binary passed a "refuses to start" check on it.
refused() { [ "$1" -ne 0 ] && [ "$1" -ne 124 ] && echo true || echo false; }
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%.90s want=%s\n' "$1" "$3" "$2"; fi
}
cleanup() {
  pkill -9 -f "cline.*$DIR" 2>/dev/null
  docker rm -f tc-ver-hot tc-ver-cold tc-ver-patch >/dev/null 2>&1
  rm -rf "$DIR"
}
trap cleanup EXIT

# release prints the major.minor a server reports, which is the unit the check
# is made in.
release() {
  mongosh --quiet --port "$1" --eval \
    'const v=db.getSiblingDB("admin").runCommand({buildInfo:1}).versionArray; print(v[0]+"."+v[1])' 2>&1 | tail -1
}
patchOf() {
  mongosh --quiet --port "$1" --eval \
    'print(db.getSiblingDB("admin").runCommand({buildInfo:1}).versionArray[2])' 2>&1 | tail -1
}

# Published ports rather than --network host: on Docker Desktop a host-networked
# container joins the *virtual machine's* network, not this one, so the port is
# up and nothing here can reach it.
start() { # name image port replset
  docker run -d --rm --name "$1" -p "$3:$3" "$2" \
    mongod --replSet "$4" --port "$3" --bind_ip_all \
    --wiredTigerCacheSizeGB 0.25 --oplogSize 128 >/dev/null 2>&1
}

echo "== setup =="
# Say what is missing rather than failing as three servers that never came up.
for img in mongo:8.0 mongo:7.0 mongo:7; do
  docker image inspect "$img" >/dev/null 2>&1 || {
    echo "  FATAL  $img is not here. docker pull mongo:8.0 mongo:7.0 mongo:7"; exit 2; }
done
docker rm -f tc-ver-hot tc-ver-cold tc-ver-patch >/dev/null 2>&1
start tc-ver-hot   mongo:8.0 $HOTPORT   verhot
start tc-ver-cold  mongo:7.0 $COLDPORT  vercold
start tc-ver-patch mongo:7   $PATCHPORT verpatch
for p in $HOTPORT $COLDPORT $PATCHPORT; do
  for _ in $(seq 1 40); do
    mongosh --quiet --port $p --eval 'db.hello()' >/dev/null 2>&1 && break
    sleep 1
  done
done
mongosh --quiet --port $HOTPORT   --eval "rs.initiate({_id:'verhot',members:[{_id:0,host:'127.0.0.1:$HOTPORT'}]})"     >/dev/null 2>&1
mongosh --quiet --port $COLDPORT  --eval "rs.initiate({_id:'vercold',members:[{_id:0,host:'127.0.0.1:$COLDPORT'}]})"   >/dev/null 2>&1
mongosh --quiet --port $PATCHPORT --eval "rs.initiate({_id:'verpatch',members:[{_id:0,host:'127.0.0.1:$PATCHPORT'}]})" >/dev/null 2>&1
sleep 6

HOTREL=$(release $HOTPORT); COLDREL=$(release $COLDPORT); PATCHREL=$(release $PATCHPORT)
chk "the fast tier is a release ahead of the archive" "true" \
    "$([ -n "$HOTREL" ] && [ "$HOTREL" != "$COLDREL" ] && echo true || echo false)"
chk "and the third server is the archive's release"   "true" \
    "$([ -n "$PATCHREL" ] && [ "$PATCHREL" = "$COLDREL" ] && echo true || echo false)"
chk "a patch apart, not a release apart"              "true" \
    "$([ -n "$(patchOf $PATCHPORT)" ] && [ "$(patchOf $PATCHPORT)" != "$(patchOf $COLDPORT)" ] && echo true || echo false)"

# Two documents on the fast tier and four in the archive, so a read that is
# answered at all had to reach the archive. Only the skew arm gets that far.
mongosh --quiet --port $HOTPORT  --eval 'db.getSiblingDB("probe").tiered.insertMany([{_id:1,v:"hot"},{_id:2,v:"hot"}])' >/dev/null 2>&1
mongosh --quiet --port $COLDPORT --eval 'db.getSiblingDB("probe").tiered.insertMany([{_id:1,v:"hot"},{_id:2,v:"hot"},{_id:3,v:"archived"},{_id:4,v:"archived"}])' >/dev/null 2>&1

writeConfig() { # file hotport coldport extra
  cat > "$1" <<YAML
hot:
  uri: "mongodb://127.0.0.1:$2/?replicaSet=$([ "$2" = "$HOTPORT" ] && echo verhot || echo vercold)"
cold:
  uri: "mongodb://127.0.0.1:$3/?replicaSet=$([ "$3" = "$COLDPORT" ] && echo vercold || echo verpatch)"
log:
  file: ""
$4
feeder:
  id: "vercheck"
  namespaces:
    - "probe.tiered"
  checkpoint_every: 1s
evictor:
  feeder_id: "vercheck"
  rules:
    - ns: "probe.tiered"
      older_than: 3m
  backoff:
    min_oplog_window: 1m
warmer:
  rules:
    - ns: "probe.tiered"
      back: 60m
  backoff:
    min_oplog_window: 1m
router:
  listen: "127.0.0.1:$PORT"
  dashboard: ""
  completeness: archive
YAML
}
writeConfig "$DIR/mismatch.yaml" $HOTPORT   $COLDPORT  ""
writeConfig "$DIR/skew.yaml"     $HOTPORT   $COLDPORT  "allow_version_skew: true"
writeConfig "$DIR/patch.yaml"    $COLDPORT  $PATCHPORT ""

echo "== a release apart, which nothing may start on =="
OUT=$(timeout 60 $BIN router --config "$DIR/mismatch.yaml" 2>&1); RC=$?
chk "the router refuses to start"                     "true" "$(refused $RC)"
chk "and names the fast tier's version"               "true" "$(grep -q "fast tier $HOTREL" <<<"$OUT" && echo true || echo false)"
chk "and names the archive's version"                 "true" "$(grep -q "archive $COLDREL" <<<"$OUT" && echo true || echo false)"
chk "and says which release to upgrade to"            "true" "$(grep -q "Upgrade the older tier to $HOTREL" <<<"$OUT" && echo true || echo false)"
chk "and names the way out"                           "true" "$(grep -q 'allow_version_skew' <<<"$OUT" && echo true || echo false)"
chk "and the listener never opened"                   "true" \
    "$(timeout 10 mongosh "mongodb://127.0.0.1:$PORT/?directConnection=true" --quiet --eval 'print("connected")' >/dev/null 2>&1 && echo false || echo true)"

OUT=$(timeout 60 $BIN feeder --config "$DIR/mismatch.yaml" 2>&1); RC=$?
chk "the feeder refuses too"                          "true" "$(refused $RC)"
chk "and mirrored nothing first"                      "true" "$(grep -q 'connected to both tiers' <<<"$OUT" && echo false || echo true)"

OUT=$(timeout 60 $BIN evictor --once --dry-run --config "$DIR/mismatch.yaml" 2>&1); RC=$?
chk "the evictor refuses, even for a dry run"         "true" "$(refused $RC)"
chk "and it is the versions it names"                 "true" "$(grep -q 'different MongoDB releases' <<<"$OUT" && echo true || echo false)"

OUT=$(timeout 60 $BIN warmer --dry-run --config "$DIR/mismatch.yaml" 2>&1); RC=$?
chk "the warmer refuses, even for a dry run"          "true" "$(refused $RC)"
chk "and it is the versions it names"                 "true" "$(grep -q 'different MongoDB releases' <<<"$OUT" && echo true || echo false)"

echo "== allow_version_skew, for a migration somebody is watching =="
$BIN router --config "$DIR/skew.yaml" --log-file "$DIR/skew.log" >/dev/null 2>&1 &
disown
for _ in $(seq 1 30); do grep -q 'router listening' "$DIR/skew.log" 2>/dev/null && break; sleep 1; done
chk "the router starts"                               "true" \
    "$(grep -q 'router listening' "$DIR/skew.log" 2>/dev/null && echo true || echo false)"
chk "and warns rather than saying nothing"            "true" \
    "$(grep -q 'different MongoDB releases, allowed by allow_version_skew' "$DIR/skew.log" && echo true || echo false)"
chk "and it serves a read that reaches the archive"   "4" \
    "$(timeout 30 mongosh "mongodb://127.0.0.1:$PORT/?directConnection=true" --quiet --eval 'print(db.getSiblingDB("probe").tiered.countDocuments({}))' 2>&1 | tail -1)"
pkill -9 -f "cline.*$DIR/skew.yaml" 2>/dev/null; sleep 1

echo "== a patch apart, which is a rolling upgrade half done =="
$BIN router --config "$DIR/patch.yaml" --log-file "$DIR/patch.log" >/dev/null 2>&1 &
disown
for _ in $(seq 1 30); do grep -q 'router listening' "$DIR/patch.log" 2>/dev/null && break; sleep 1; done
chk "the router starts"                               "true" \
    "$(grep -q 'router listening' "$DIR/patch.log" 2>/dev/null && echo true || echo false)"
chk "and says the patch releases differ"              "true" \
    "$(grep -q 'different patch releases of the same version' "$DIR/patch.log" && echo true || echo false)"
chk "and does not call it a mismatch"                 "true" \
    "$(grep -q 'different MongoDB releases' "$DIR/patch.log" && echo false || echo true)"
pkill -9 -f "cline.*$DIR/patch.yaml" 2>/dev/null; sleep 1

timeout 25 $BIN feeder --config "$DIR/patch.yaml" --log-file "$DIR/patchfeeder.log" >/dev/null 2>&1
chk "the feeder runs on it"                           "true" \
    "$(grep -q 'connected to both tiers' "$DIR/patchfeeder.log" 2>/dev/null && echo true || echo false)"

echo "== the same release is not the whole of it =="
# Two servers on one release still refuse different commands when their feature
# compatibility versions differ, and which tier answers is decided by how old
# the documents are. Same failure, same silence, so the same refusal.
mongosh --quiet --port $PATCHPORT --eval \
  'db.getSiblingDB("admin").runCommand({setFeatureCompatibilityVersion:"6.0",confirm:true})' >/dev/null 2>&1
sleep 2
ARCHFCV=$(mongosh --quiet --port $PATCHPORT --eval \
  'print(db.getSiblingDB("admin").runCommand({getParameter:1,featureCompatibilityVersion:1}).featureCompatibilityVersion.version)' 2>&1 | tail -1)
chk "the archive was put a feature version behind"    "6.0" "$ARCHFCV"

OUT=$(timeout 60 $BIN router --config "$DIR/patch.yaml" 2>&1); RC=$?
chk "the router refuses that too"                     "true" "$(refused $RC)"
chk "and names both feature versions"                 "true" \
    "$(grep -q 'fast tier 7.0, archive 6.0' <<<"$OUT" && echo true || echo false)"
chk "and the one command that fixes it"               "true" \
    "$(grep -q 'setFeatureCompatibilityVersion' <<<"$OUT" && echo true || echo false)"

mongosh --quiet --port $PATCHPORT --eval \
  'db.getSiblingDB("admin").runCommand({setFeatureCompatibilityVersion:"7.0",confirm:true})' >/dev/null 2>&1
sleep 2
$BIN router --config "$DIR/patch.yaml" --log-file "$DIR/fcv.log" >/dev/null 2>&1 &
disown
for _ in $(seq 1 30); do grep -q 'router listening' "$DIR/fcv.log" 2>/dev/null && break; sleep 1; done
chk "and starts once they agree again"                "true" \
    "$(grep -q 'router listening' "$DIR/fcv.log" 2>/dev/null && echo true || echo false)"
chk "saying what they agree on"                       "true" \
    "$(grep -q 'tier feature compatibility.*fcv=7.0' "$DIR/fcv.log" && echo true || echo false)"
pkill -9 -f "cline.*$DIR/patch.yaml" 2>/dev/null; sleep 1

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
