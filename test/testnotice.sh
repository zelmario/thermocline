#!/bin/bash
# An eviction announcement belongs to the feeder, and nobody else may take it.
#
#   bash test/testnotice.sh          inside node0
#
# The evictor announces a document before removing it from the fast tier, and
# the feeder reads that announcement to tell an eviction from an application
# delete. A copy-up used to delete the announcement, which is right about the
# next eviction and wrong about the one already in flight. With the feeder yet
# to apply that delete, the notice it needs is gone by the time it looks, so it
# reads a real eviction as an application delete and tombstones a record nothing
# deleted. The warm's own insert does not repair it, because the feeder
# suppresses that as an echo of the copy-up.
#
# Found on the pilot with a warm and an evictor fighting over the same
# collections while a client read them: 66 batches of "deletes not announced by
# the evictor" in ninety seconds, and an archive left holding 200 tombstoned
# trips and 45 tombstoned sales that nothing had ever deleted. The end to end
# version of that lives in test/pilot, under setup.sh churn.
#
# What is checked here is the rule itself, with the feeder stopped throughout:
# a copy-up leaves the announcement standing, and the feeder is the only thing
# that spends one. Written the other way, as "run everything and see whether a
# tombstone appears", it depended on winning a race with the feeder's poll and
# was unreliable in both directions: it failed three parallel sweeps on its own
# precondition, and then passed against a binary that had the bug.
WDIR=${WDIR:-/root}
DB=${DB:-shop}
META=${META:-_thermocline}
export TC_DB=$DB TC_META=$META
CFG=${CFG:-$WDIR/cline.yaml}
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chkv() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT"  --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
notices() { C "print(db.getSiblingDB(process.env.TC_META).evicting.countDocuments({_id:'$1'}))"; }

echo "== setup: the feeder stopped, so nothing races the checks =="
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
chkv "no feeder is running" "0" "$(pgrep -cf "cline feeder --config $WDIR/" || true)"

# A warm window wider than the eviction cutoff, so both address the same
# document. That is what "the hot window is too small" means, and it is the
# configuration the pilot was running when this was found.
sed '/^warmer:/,/^router:/ s|^      back: 40m|      back: 24h|' $CFG > $WDIR/notice_wide.yaml
chkv "the derived config warms past the eviction cutoff" "true" \
    "$(grep -q 'back: 24h' $WDIR/notice_wide.yaml && echo true || echo false)"

EVID=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 5150)
NOTICE="$DB.orders|07$EVID"
H "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$EVID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$EVID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).residency.deleteMany({});
   db.getSiblingDB(process.env.TC_META).evicting.deleteMany({});
   db.getSiblingDB(process.env.TC_META).warming.deleteMany({})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).orders.insertOne({_id:ObjectId('$EVID'), n:'live'})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.insertOne({_id:ObjectId('$EVID'), n:'live'})" >/dev/null
# The evictor refuses to run once mirroring looks stale, and a sweep that
# refuses would leave every check below measuring nothing. The feeder is stopped
# for this, so its position is stamped by hand immediately before the sweep.
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null

echo "== the evictor announces before it removes =="
EVOUT=$(cline evictor --config $CFG --once 2>&1)
chkv "the sweep actually ran"                    "true" \
    "$(grep -q "not healthy enough to evict" <<<"$EVOUT" && echo false || echo true)"
chkv "it removed the document from the fast tier" "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$EVID')}))")"
chkv "and left an announcement for the feeder"   "1" "$(notices "$NOTICE")"

echo "== a warm puts it back and must leave the announcement alone =="
cline warmer --config $WDIR/notice_wide.yaml >/dev/null 2>&1
chkv "the warm copied it back up"                "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$EVID')}))")"
chkv "the announcement is still there"           "1" "$(notices "$NOTICE")"

echo "== so does the router's own copy-up =="
# Evict it again, then read it through a router, which copies it up on the miss.
C "db.getSiblingDB(process.env.TC_META).residency.deleteMany({})" >/dev/null
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null
cline evictor --config $CFG --once >/dev/null 2>&1
BEFORE=$(notices "$NOTICE")
chkv "the second eviction is announced too"      "true" \
    "$(awk -v n="$BEFORE" 'BEGIN{print (n+0 > 0) ? "true" : "false"}')"
PORT=${PORT:-27100}
cline router --config $CFG >/dev/null 2>&1 & RP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
  --eval "db.getSiblingDB(process.env.TC_DB).orders.findOne({_id:ObjectId('$EVID')})" >/dev/null 2>&1
for _ in $(seq 1 20); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$EVID')}))")" = "1" ] && break
  sleep 0.5
done
chkv "the read copied it up"                     "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$EVID')}))")"
chkv "and left the announcement alone"           "$BEFORE" "$(notices "$NOTICE")"
kill $RP 2>/dev/null; wait $RP 2>/dev/null

echo "== the feeder is what spends one =="
cline feeder --config $CFG >/dev/null 2>&1 & FP=$!
for _ in $(seq 1 40); do
  [ "$(notices "$NOTICE")" = "0" ] && break
  sleep 0.5
done
chkv "the announcements are spent once it catches up" "0" "$(notices "$NOTICE")"
chkv "and the archive still holds the document live"  "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$EVID'),_tcDeletedAt:{\$exists:false}}))")"
kill $FP 2>/dev/null; wait $FP 2>/dev/null

H "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$EVID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$EVID')})" >/dev/null

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
