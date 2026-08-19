#!/bin/bash
# The bulkWrite command, which MongoDB 8.0 added and the router has never seen.
#
# Every guard in the router finds its collection the same way: the command names
# it, as `insert: "orders"` or `update: "orders"`. bulkWrite does not work like
# that. It goes to the admin database, lists its collections in nsInfo, and its
# operations refer to them by position, so a router looking for a collection
# name in the command finds nothing and lets the whole thing through.
#
# What goes through with it is every protection: the unique key registry that
# stops an insert duplicating an archived document, and the copy-up that makes a
# multi-document write reach documents the fast tier no longer holds. Both were
# bugs once already, on the ordinary write path, and both were fixed there.
# Parallel-safe knobs. Every default reproduces the single-worker layout, so a
# suite run by hand behaves exactly as it always has. psweep.sh sets them per
# worker: its own databases, its own bookkeeping database, its own port block,
# and its own directory for configs and logs. That directory is also what scopes
# the pkills below to this worker's daemons, because every one of them carries
# --config $WDIR/... on its command line.
WDIR=${WDIR:-/root}
LOGDIR=${LOGDIR:-/var/log/cline}
PORT=${PORT:-27100}
DB=${DB:-shop}
DB2=${DB2:-wh}
META=${META:-_thermocline}
# mongosh is node, so the suites reach these through process.env rather than
# shell expansion. Half the eval strings are single-quoted at the shell, where a
# $VAR would not expand and would silently query the wrong database.
export TC_DB=$DB TC_DB2=$DB2 TC_META=$META
P1=$((PORT+1)); P2=$((PORT+2)); P3=$((PORT+3)); PDASH=$((PORT+10)); PALT=$((PORT+17))
CFG=${CFG:-$WDIR/cline.yaml}
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
stopFeeder() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/bulk_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/bulk_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}
waitMirrored() {
  local n=""
  for _ in $(seq 1 60); do
    n=$(C "print(db.getSiblingDB(process.env.TC_DB).$1.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$2" ] && return 0
    sleep 0.5
  done
  echo "  WARN  $DB.$1 never reached $2 mirrored documents, last saw ${n:-none}"
}
evictBehindTheFeeder() {
  stopFeeder
  H "$1" >/dev/null
  C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder
}

echo "== setup =="
stopDaemons
for t in tbwu tbwm; do
  H "db.getSiblingDB(process.env.TC_DB).$t.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$t.drop()" >/dev/null
  H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:'$DB.$t'})" >/dev/null
done
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({})' >/dev/null
startFeeder
startRouter

SUPPORTED=$(R "try { db.adminCommand({bulkWrite:1, ops:[], nsInfo:[]}); print('yes'); } catch(e) { print(/no such command|unrecognized/i.test(e.message)?'no':'yes'); }")
echo "  the server supports the bulkWrite command: $SUPPORTED"
if [ "$SUPPORTED" != "yes" ]; then
  echo "  SKIP  this server predates bulkWrite, nothing to guard"
  stopDaemons; echo; echo "PASS $PASS   FAIL $FAIL"; exit 0
fi

echo "== a bulkWrite insert must not duplicate an archived unique key =="
H "db.getSiblingDB(process.env.TC_DB).tbwu.createIndex({email:1},{unique:true})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).tbwu.insertOne({_id:1,email:'taken@x.com'})" >/dev/null
waitMirrored tbwu 1
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).tbwu.deleteMany({})"
chk "the owner is in the archive only"                  "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).tbwu.countDocuments({}))")"
DUP=$(R "
  try {
    db.adminCommand({bulkWrite:1,
      ops:[{insert:0, document:{_id:2, email:'taken@x.com'}}],
      nsInfo:[{ns:process.env.TC_DB+'.tbwu'}]});
    print('ACCEPTED');
  } catch(e) { print(e.code===11000?'11000':('other:'+(e.code||e.message.substring(0,40)))); }")
chk "the duplicate is refused"                          "11000" "$DUP"
LH=$(H "print(db.getSiblingDB(process.env.TC_DB).tbwu.countDocuments({email:'taken@x.com'}))")
LC=$(C "print(db.getSiblingDB(process.env.TC_DB).tbwu.countDocuments({email:'taken@x.com',_tcDeletedAt:{\$exists:false}}))")
chk "one live holder of the key across both tiers"      "1" "$((LH + LC))"

echo "== a bulkWrite multi update must reach archived documents =="
R "db.getSiblingDB(process.env.TC_DB).tbwm.insertMany(Array.from({length:12},(_,i)=>({_id:i,grp:'g',n:i})))" >/dev/null
waitMirrored tbwm 12
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).tbwm.deleteMany({_id:{\$lt:9}})"
chk "the fast tier holds only three of them"            "3" "$(H "print(db.getSiblingDB(process.env.TC_DB).tbwm.countDocuments({}))")"
MOD=$(R "
  try {
    const r=db.adminCommand({bulkWrite:1,
      ops:[{update:0, filter:{grp:'g'}, updateMods:{\$set:{touched:1}}, multi:true}],
      nsInfo:[{ns:process.env.TC_DB+'.tbwm'}]});
    print(r.nModified !== undefined ? r.nModified : JSON.stringify(r).substring(0,60));
  } catch(e) { print('ERR '+(e.code||e.message.substring(0,40))); }")
chk "every matching document was modified"              "12" "$MOD"

echo "== a bulkWrite delete must reach archived documents =="
DEL=$(R "
  try {
    const r=db.adminCommand({bulkWrite:1,
      ops:[{delete:0, filter:{grp:'g'}, multi:true}],
      nsInfo:[{ns:process.env.TC_DB+'.tbwm'}]});
    print(r.nDeleted !== undefined ? r.nDeleted : JSON.stringify(r).substring(0,60));
  } catch(e) { print('ERR '+(e.code||e.message.substring(0,40))); }")
chk "every matching document was deleted"               "12" "$DEL"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
