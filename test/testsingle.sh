#!/bin/bash
# Single-document writes whose filter is not an _id.
#
# The write path resolves a miss in two places, and between them is a gap.
# interceptWrite acts on the way back, but only when the filter pins an exact
# _id. guardMultiWrite acts before the write and takes any filter, but only for
# statements that name many documents: multi:true for an update, limit:0 for a
# delete.
#
# A deleteOne or updateOne filtered on anything else falls between the two. The
# fast tier matches nothing, reports zero, and the archived document is left
# where it is. For a delete that is the worst possible answer, because an erasure
# request that reports success without erasing is the one failure nobody can
# audit their way out of afterwards.
#
# An insert is the mirror image: an _id the archive holds is not on the fast tier
# any more, so the insert succeeds where one cluster would have raised E11000,
# and the feeder then mirrors the new document over the archived one.
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
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/sing_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/sing_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}
# Archive only, which is what an evicted document looks like.
seed() { C "db.getSiblingDB(process.env.TC_DB).$1.insertOne($2)" >/dev/null; }
# Live means present in the archive and not tombstoned.
liveInCold() { C "const d=db.getSiblingDB(process.env.TC_DB).$1.findOne({_id:$2}); print(!!d && !d._tcDeletedAt)"; }

echo "== setup =="
stopDaemons
for n in tsdel tsupd tsins; do
  H "db.getSiblingDB(process.env.TC_DB).$n.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$n.drop()" >/dev/null
done
C "db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({}); db.getSiblingDB(process.env.TC_META).evicting.deleteMany({})" >/dev/null
startFeeder
startRouter

seed tsdel "{_id:1, email:'a@x', note:'by id'}"
seed tsdel "{_id:2, email:'b@x', note:'by field'}"
seed tsupd "{_id:1, email:'c@x', name:'carol', balance:100}"
chk "the fast tier holds none of them"                  "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tsdel.countDocuments({}))")"

echo "== deleteOne by _id, which is the covered path =="
chk "it deletes one"                                    "1" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tsdel.deleteOne({_id:1}).deletedCount)")"
sleep 5
chk "and the archive stops returning it"                "false" "$(liveInCold tsdel 1)"

echo "== deleteOne by a field that is not _id =="
# Same statement, same document, a filter the guards do not recognise.
chk "it deletes one"                                    "1" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tsdel.deleteOne({email:'b@x'}).deletedCount)")"
sleep 5
chk "and the archive stops returning it"                "false" "$(liveInCold tsdel 2)"
chk "the document is gone from a read as well"          "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tsdel.countDocuments({email:'b@x'}))")"

echo "== updateOne by a field that is not _id =="
UPD=$(R "print(db.getSiblingDB(process.env.TC_DB).tsupd.updateOne({email:'c@x'},{\$set:{balance:250}}).matchedCount)")
chk "it matches the archived document"                  "1" "$UPD"
chk "and the change is visible"                         "carol-250" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tsupd.findOne({email:'c@x'}); print(d?d.name+'-'+d.balance:'MISSING')")"
sleep 5
chk "and reached the archive"                           "250" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).tsupd.findOne({_id:1}); print(d?d.balance:'MISSING')")"

echo "== findOneAndDelete by a field that is not _id =="
seed tsdel "{_id:3, email:'d@x', note:'fam'}"
chk "it returns the archived document"                  "d@x" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tsdel.findOneAndDelete({email:'d@x'}); print(d?d.email:'MISSING')")"
sleep 5
chk "and the archive stops returning it"                "false" "$(liveInCold tsdel 3)"

echo "== insert of an _id the archive still holds =="
# One cluster answers E11000 here. Anything else both hides the collision and
# lets the feeder mirror the new document over the archived one.
seed tsins "{_id:9, name:'dave', tier:'gold', balance:100}"
INS=$(R "try { db.getSiblingDB(process.env.TC_DB).tsins.insertOne({_id:9, balance:5}); print('ACCEPTED'); }
         catch(e) { print(/E11000|duplicate key/i.test(e.message)?'DUPLICATE':'other: '+e.message.substring(0,50)); }")
chk "it is refused as a duplicate"                      "DUPLICATE" "$INS"
sleep 5
chk "and the archived document is intact"               "dave-gold-100" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).tsins.findOne({_id:9}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"

echo "== and an insert of an id nobody holds still works =="
chk "a fresh id is accepted"                            "1" \
    "$(R "db.getSiblingDB(process.env.TC_DB).tsins.insertOne({_id:10, name:'erin'}); print(db.getSiblingDB(process.env.TC_DB).tsins.countDocuments({_id:10}))")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
