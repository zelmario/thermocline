#!/bin/bash
# Commands that change a collection rather than its documents, and the one
# deprecated command that still reads them.
#
# The feeder mirrors documents and nothing else. Drops, renames and every other
# collection-level change are ignored on purpose, because replaying them is what
# would let an accident on the fast tier destroy the archive. That rule is right
# and it has a consequence: an operation the fast tier performs and the archive
# does not leaves the two describing different collections, and the router goes
# on answering as though they matched.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
NS=tddl
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
stopFeeder() {
  pkill -f 'cline feeder' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/ddl_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/ddl_r.log & RP=$!
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
    n=$(C "print(db.getSiblingDB('shop').$1.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$2" ] && return 0
    sleep 0.5
  done
  echo "  WARN  shop.$1 never reached $2 mirrored documents, last saw ${n:-none}"
}

echo "== setup =="
stopDaemons
for t in $NS ${NS}_renamed; do
  H "db.getSiblingDB('shop').$t.drop()" >/dev/null
  C "db.getSiblingDB('shop').$t.drop()" >/dev/null
done
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"}); db.getSiblingDB("_thermocline").watermarks.deleteMany({}); db.getSiblingDB("_thermocline").residency.deleteMany({})' >/dev/null
startFeeder
startRouter

R "db.getSiblingDB('shop').$NS.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i,grp:'g'})))" >/dev/null
waitMirrored $NS 10
stopFeeder
H "db.getSiblingDB('shop').$NS.deleteMany({_id:{\$lt:7}})" >/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
cline watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the fast tier holds three, the archive ten"        "3" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "and a normal read still answers in full"           "10" "$(R "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

echo "== renaming a tiered collection =="
# The feeder ignores renames on purpose, so the fast tier would move and the
# archive would not. Reads of the new name are not tiered at all and answer from
# whatever the fast tier holds; reads of the old name find an empty fast tier and
# answer from an archive nothing writes to any more. The collection is split in
# two and neither half says so.
RENAMED=$(R "
  try { db.adminCommand({renameCollection:'shop.$NS', to:'shop.${NS}_renamed'}); print('ALLOWED'); }
  catch(e) { print('REFUSED'); }")
chk "renaming a tiered collection is refused"           "REFUSED" "$RENAMED"
chk "and the collection is still where it was"          "3" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "the archive was not split"                         "10" "$(C "print(db.getSiblingDB('shop').$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== mapReduce over a tiered collection =="
# Deprecated, rare, and it reads every document it is pointed at. Run against a
# fast tier holding three of ten it produces a total of three and reports
# success, which is the short answer in a different costume.
MR=$(R "
  try {
    const r = db.getSiblingDB('shop').runCommand({
      mapReduce: '$NS',
      map: function() { emit('all', 1); },
      reduce: function(k, vals) { return Array.sum(vals); },
      out: {inline: 1}});
    print(r.results && r.results.length ? r.results[0].value : JSON.stringify(r).substring(0,40));
  } catch(e) { print('REFUSED'); }")
# Ten or a refusal. Three is the bug.
chk "mapReduce does not silently count three"           "true" \
    "$([ "$MR" = "REFUSED" ] || [ "$MR" = "10" ] && echo true || echo "false (got $MR)")"

echo "== explain does not crash, whatever it says =="
EXP=$(R "
  try { const e=db.getSiblingDB('shop').$NS.find({}).explain(); print(e.ok===1||e.queryPlanner?'ok':'odd'); }
  catch(e) { print('ERR '+e.message.substring(0,50)); }")
chk "explain is answered"                               "ok" "$EXP"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
