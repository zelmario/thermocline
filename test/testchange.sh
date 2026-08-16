#!/bin/bash
# Change streams opened by the application, through the router.
#
# A change stream is an aggregate, so it goes through the aggregation guard, and
# that guard was written for pipelines that read documents. A change stream
# reads events instead, and none of the completeness reasoning applies to it.
#
# Then there is the harder question underneath. The evictor removes documents
# from the fast tier, and a change stream watching the fast tier sees removals.
# An application watching for deletes, to invalidate a cache or emit an event or
# tombstone a row in a warehouse, would be told a document was deleted when it
# was only moved. That is not a slow answer or a refused one. It is the system
# reporting an event that never happened.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
NS=tcs
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/cs_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/cs_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}
waitMirrored() {
  local n=""
  for _ in $(seq 1 60); do
    n=$(C "print(db.getSiblingDB('shop').$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$1" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the archive never reached $1 live documents, last saw ${n:-none}"
}

echo "== setup =="
stopDaemons
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({}); db.getSiblingDB('_thermocline').residency.deleteMany({}); db.getSiblingDB('_thermocline').evicting.deleteMany({})" >/dev/null
startFeeder
startRouter

echo "== a change stream can be opened at all =="
# Two hours old, so the evictor's forty minute rule will take them later.
OPENED=$(R "
  try { const cs=db.getSiblingDB('shop').$NS.watch(); cs.close(); print('opened'); }
  catch(e) { print('ERR '+e.message.substring(0,70)); }")
chk "watch() is accepted on a tiered collection"       "opened" "$OPENED"

echo "== it reports the application's own writes =="
SEEN=$(R "
  const cs=db.getSiblingDB('shop').$NS.watch();
  db.getSiblingDB('shop').$NS.insertOne({_id:1, v:'a'});
  db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$set:{v:'b'}});
  let ops=[];
  for (let i=0;i<40 && ops.length<2;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType); else sleep(250); }
  cs.close();
  print(ops.join(','));")
chk "an insert and an update are both reported"        "insert,update" "$SEEN"

echo "== an eviction must not look like a deletion =="
# This is the one that matters. The document is not going away: the archive
# keeps it and a later read still finds it. An application told that it was
# deleted would drop its cache entry, emit a deletion event, or tombstone a row
# in a warehouse, for a document that still exists.
OID=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 771)
R "db.getSiblingDB('shop').$NS.insertOne({_id:ObjectId('$OID'), v:'old'})" >/dev/null
waitMirrored 2

# The watcher runs in its own client and writes what it saw to a file, because
# a change stream has to already be open when the eviction happens. Its pid is
# waited on by name: a bare wait here would also wait for the feeder and the
# router, which are background jobs of this shell and never exit.
rm -f /tmp/cs_events.txt
mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getSiblingDB('shop').$NS.watch();
  let ops=[];
  for (let i=0;i<40 && ops.length===0;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType+':'+(e.documentKey?JSON.stringify(e.documentKey._id):'')); else sleep(250); }
  cs.close();
  print(ops.length ? ops.join(',') : 'none');" > /tmp/cs_events.txt 2>&1 &
WATCHER=$!
sleep 3
cline evictor --config $CFG --once >/dev/null 2>&1
for _ in $(seq 1 40); do kill -0 $WATCHER 2>/dev/null || break; sleep 0.5; done
kill $WATCHER 2>/dev/null
EVENTS=$(tail -1 /tmp/cs_events.txt 2>/dev/null)
LEFT=$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")
echo "  the fast tier now holds $LEFT of 2, the watcher saw: ${EVENTS:-nothing}"
# An error is not an answer. Without this, a build that refuses change streams
# outright passes both of these: it saw no delete because it saw nothing at all.
chk "the watcher was actually watching"                "true" \
    "$(echo "${EVENTS:-none}" | grep -qi "error\|refus" && echo false || echo true)"
chk "an eviction is not reported as a delete"          "true" \
    "$(echo "${EVENTS:-none}" | grep -q delete && echo false || echo true)"

echo "== a copy-up must not look like an insert =="
# The other half of the same problem. The document has existed for two hours and
# is only being moved back; an application acting on inserts would treat it as
# new.
rm -f /tmp/cs_events2.txt
mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getSiblingDB('shop').$NS.watch();
  let ops=[];
  for (let i=0;i<40 && ops.length===0;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType); else sleep(250); }
  cs.close();
  print(ops.length ? ops.join(',') : 'none');" > /tmp/cs_events2.txt 2>&1 &
WATCHER2=$!
sleep 3
# A point lookup of the evicted document copies it back up.
R "db.getSiblingDB('shop').$NS.findOne({_id:ObjectId('$OID')})" >/dev/null
for _ in $(seq 1 40); do kill -0 $WATCHER2 2>/dev/null || break; sleep 0.5; done
kill $WATCHER2 2>/dev/null
EVENTS2=$(tail -1 /tmp/cs_events2.txt 2>/dev/null)
echo "  after the copy-up the watcher saw: ${EVENTS2:-nothing}"
chk "the second watcher was actually watching"         "true" \
    "$(echo "${EVENTS2:-none}" | grep -qi "error\|refus" && echo false || echo true)"
chk "a copy-up is not reported as an insert"           "true" \
    "$(echo "${EVENTS2:-none}" | grep -q insert && echo false || echo true)"
chk "the archive still holds the evicted document"     "true" \
    "$(C "const d=db.getSiblingDB('shop').$NS.findOne({_id:ObjectId('$OID')}); print(!!d && !d._tcDeletedAt)")"
chk "a read through the router still finds it"         "old" \
    "$(R "const d=db.getSiblingDB('shop').$NS.findOne({_id:ObjectId('$OID')}); print(d?d.v:'MISSING')")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
