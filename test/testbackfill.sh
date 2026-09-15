#!/bin/bash
# Closing the hole a stopped feeder leaves.
#
#   bash test/testbackfill.sh
#
# The feeder exits when the fast tier's oplog has rolled past its checkpoint,
# and the operator is told to copy the affected namespaces into the archive and
# clear the checkpoint. Until `cline backfill` there was no command for the
# copying, so it was a script written during an incident, and a script written
# during an incident closes two of the three things that can have happened.
#
# Measured by hand first, which is why this suite exists: one update, one insert
# and one delete against a fast tier with the feeder down, then the remedy
# followed exactly. The update came back right, the insert arrived, and the
# deleted document stayed live in the archive and was served to a client. The
# router counted eleven where the application had left ten.
#
# It runs on the HOST, on two mongods of its own, because it stops the feeder
# and rewrites a completeness claim.
HOTPORT=${HOTPORT:-28100}
COLDPORT=${COLDPORT:-28101}
PORT=${PORT:-28105}
DIR=$(mktemp -d /tmp/tcbackfill.XXXXXX)
BIN=${BIN:-$(pwd)/bin/cline}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%.80s want=%s\n' "$1" "$3" "$2"; fi
}
cleanup() {
  pkill -9 -f "cline.*$DIR" 2>/dev/null
  docker rm -f tc-bf-hot tc-bf-cold >/dev/null 2>&1
  rm -rf "$DIR"
}
trap cleanup EXIT
M() { mongosh --quiet --port "$1" --eval "$2" 2>&1 | tail -1; }
R() { timeout 30 mongosh "mongodb://127.0.0.1:$PORT/?directConnection=true" --quiet --eval "$1" 2>&1 | tail -1; }
claim() { M $COLDPORT "db.getSiblingDB('_thermocline').watermarks.replaceOne({_id:'shop.orders'},{_id:'shop.orders',completeFrom:new Date($1)},{upsert:true})" >/dev/null; }

echo "== setup =="
docker image inspect mongo:8.0 >/dev/null 2>&1 || { echo "  FATAL  docker pull mongo:8.0"; exit 2; }
[ -x "$BIN" ] || { echo "  FATAL  no binary at $BIN; run make build"; exit 2; }
docker rm -f tc-bf-hot tc-bf-cold >/dev/null 2>&1
docker run -d --rm --name tc-bf-hot  -p $HOTPORT:$HOTPORT  mongo:8.0 \
  mongod --replSet bfhot  --port $HOTPORT  --bind_ip_all --wiredTigerCacheSizeGB 0.25 --oplogSize 64 >/dev/null 2>&1
docker run -d --rm --name tc-bf-cold -p $COLDPORT:$COLDPORT mongo:8.0 \
  mongod --replSet bfcold --port $COLDPORT --bind_ip_all --wiredTigerCacheSizeGB 0.25 --oplogSize 64 >/dev/null 2>&1
for p in $HOTPORT $COLDPORT; do
  for _ in $(seq 1 40); do mongosh --quiet --port $p --eval 'db.hello()' >/dev/null 2>&1 && break; sleep 1; done
done
M $HOTPORT  "rs.initiate({_id:'bfhot',members:[{_id:0,host:'127.0.0.1:$HOTPORT'}]})"  >/dev/null
M $COLDPORT "rs.initiate({_id:'bfcold',members:[{_id:0,host:'127.0.0.1:$COLDPORT'}]})" >/dev/null
sleep 6
cat > "$DIR/bf.yaml" <<YAML
hot:
  uri: "mongodb://127.0.0.1:$HOTPORT/?replicaSet=bfhot"
cold:
  uri: "mongodb://127.0.0.1:$COLDPORT/?replicaSet=bfcold"
log:
  file: ""
feeder:
  id: "bf"
  namespaces:
    - "shop.orders"
  checkpoint_every: 1s
evictor:
  feeder_id: "bf"
  rules:
    - ns: "shop.orders"
      older_than: 30m
      date_field: "at"
router:
  listen: "127.0.0.1:$PORT"
  dashboard: ""
  completeness: archive
YAML

"$BIN" feeder --config "$DIR/bf.yaml" --log-file "$DIR/f1.log" >/dev/null 2>&1 & disown
sleep 3
M $HOTPORT 'const c=db.getSiblingDB("shop").orders;
  for (let i=1;i<=10;i++) c.insertOne({_id:i,v:"first",at:new Date()});' >/dev/null
for _ in $(seq 1 30); do
  [ "$(M $COLDPORT 'print(db.getSiblingDB("shop").orders.countDocuments({}))')" = "10" ] && break
  sleep 1
done
chk "ten documents are mirrored"                      "10" \
    "$(M $COLDPORT 'print(db.getSiblingDB("shop").orders.countDocuments({}))')"
pkill -9 -f "cline.*$DIR" 2>/dev/null; sleep 1

echo "== the window where nothing was mirroring =="
M $HOTPORT 'const c=db.getSiblingDB("shop").orders;
  c.updateOne({_id:1},{$set:{v:"changed in the gap"}});
  c.insertOne({_id:11,v:"inserted in the gap",at:new Date()});
  c.deleteOne({_id:2});' >/dev/null
chk "the fast tier holds ten of its own"              "10" \
    "$(M $HOTPORT 'print(db.getSiblingDB("shop").orders.countDocuments({}))')"
# The feeder's published position has to age out, which in a real incident it
# has long since done: the oplog had to roll past the checkpoint to get here.
sleep 12
claim "Date.now()-1000*60*60*24"

echo "== the guard, before anything is written =="
# A claim the fast tier cannot support is a claim that is wrong, and marking on
# the strength of it would tombstone the archive rather than recover it.
M $HOTPORT 'db.getSiblingDB("shop").orders.renameCollection("orders_aside")' >/dev/null
WRONG=$("$BIN" backfill --config "$DIR/bf.yaml" 2>&1 | grep 'shop.orders')
chk "it refuses a claim the fast tier holds none of"  "true" \
    "$(grep -q 'the claim is wrong rather than the archive' <<<"$WRONG" && echo true || echo false)"
M $HOTPORT 'db.getSiblingDB("shop").orders_aside.renameCollection("orders")' >/dev/null

echo "== reporting, which is the default =="
DRY=$("$BIN" backfill --config "$DIR/bf.yaml" 2>&1)
chk "it says it would copy ten"                       "true" \
    "$(grep -q 'Would copy 10' <<<"$DRY" && echo true || echo false)"
chk "and mark the one that was deleted"               "true" \
    "$(grep -q 'mark 1' <<<"$DRY" && echo true || echo false)"
chk "and wrote nothing while saying so"               "first" \
    "$(M $COLDPORT 'const d=db.getSiblingDB("shop").orders.findOne({_id:1}); print(d?d.v:"absent")')"

echo "== and then applied =="
"$BIN" backfill --config "$DIR/bf.yaml" --apply >/dev/null 2>&1
M $COLDPORT 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"bf"})' >/dev/null
"$BIN" feeder --config "$DIR/bf.yaml" --log-file "$DIR/f3.log" >/dev/null 2>&1 & disown
"$BIN" router --config "$DIR/bf.yaml" --log-file "$DIR/r.log" >/dev/null 2>&1 & disown
for _ in $(seq 1 40); do grep -q 'router listening' "$DIR/r.log" 2>/dev/null && break; sleep 1; done
sleep 2
chk "the update made in the gap is in the archive"    "changed in the gap" \
    "$(M $COLDPORT 'const d=db.getSiblingDB("shop").orders.findOne({_id:1}); print(d?d.v:"absent")')"
chk "the insert made in the gap is too"               "inserted in the gap" \
    "$(M $COLDPORT 'const d=db.getSiblingDB("shop").orders.findOne({_id:11}); print(d?d.v:"absent")')"
# The half copying cannot do: a delete leaves nothing on the fast tier to copy,
# so only the completeness claim can say it happened.
chk "and the delete is marked rather than live"       "deleted" \
    "$(M $COLDPORT 'const d=db.getSiblingDB("shop").orders.findOne({_id:2}); print(d ? (d._tcDeletedAt ? "deleted" : d.v) : "absent")')"
chk "so a client does not read the deleted document"  "absent" \
    "$(R 'const d=db.getSiblingDB("shop").orders.findOne({_id:2}); print(d?d.v:"absent")')"
# The number an operator would check first, and the one that was wrong before.
chk "and the count is what the application left"      "10" \
    "$(R 'print(db.getSiblingDB("shop").orders.countDocuments({}))')"

echo "== and it refuses to run underneath a feeder =="
# Tested last, and for a reason worth writing down: the first version of this
# suite started a feeder in the middle of the gap to check the refusal, and that
# feeder mirrored the gap. The scenario the checks after it depended on was gone,
# and three of them failed against a command that was working.
#
# A feeder is running now, from the section above, so the position is fresh.
ALIVE=$("$BIN" backfill --config "$DIR/bf.yaml" --apply 2>&1)
chk "it refuses while a feeder is still publishing"   "true" \
    "$(grep -q 'still publishing' <<<"$ALIVE" && echo true || echo false)"
chk "and says to stop it first"                       "true" \
    "$(grep -q 'Stop it first' <<<"$ALIVE" && echo true || echo false)"
pkill -9 -f "cline.*$DIR" 2>/dev/null

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
