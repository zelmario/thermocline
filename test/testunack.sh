#!/bin/bash
# Writes that ask for no acknowledgement.
#
# An unacknowledged write, writeConcern {w:0}, is sent with the wire protocol's
# moreToCome flag set and the client never waits for a reply. The router has
# nothing to answer, so every guard it runs was skipped for those messages: the
# copy-up that lets a multi-document write reach archived documents, the unique
# key claim that stops an insert duplicating one the fast tier can no longer
# see, and the upsert judgement that stops a stub overwriting an archived
# record.
#
# That is the same shape as the compression finding. A flag the client sets for
# reasons of its own turns off every protection in the system, invisibly,
# because nothing about the write looks unusual from the outside. It is worse
# here in one way: w:0 is chosen by ordinary applications for ordinary reasons,
# in metrics writers and log ingesters, and it needs no unusual driver options.
#
# There is no differential reference for these, because the whole question is
# what happens to documents an untiered collection would not have. What a single
# cluster does with each statement is not in doubt, and it is written beside each
# check.
#
# Refusing has nowhere to go, so where the guard would have refused the right
# thing is to not perform the write. A dropped write is what w:0 already means to
# an application: it asked not to be told.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-56s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-56s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() { pkill -9 -f 'cline feeder' 2>/dev/null; pkill -9 -f 'cline router' 2>/dev/null; }
trap cleanup EXIT

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/unack_f.log &
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/unack_r.log &
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}
stopFeeder() {
  pkill -f 'cline feeder' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
# Remove documents from the fast tier the way an eviction does, with the feeder
# blind to it, so the archive keeps a live copy. Doing it with the feeder running
# is a delete, which tombstones the archive's copies, and the collection then has
# nothing archived to lose: the suite would pass while testing nothing.
evictBehindTheFeeder() {
  stopFeeder
  H "$1" >/dev/null
  C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder
}
# The feeder has to have caught up before a check reads the archive, and it is
# always some way behind. Waiting on a sentinel beats sleeping for a guess.
waitFeeder() {
  H "db.getSiblingDB('shop').tunku.insertOne({_id:'__sentinel'})" >/dev/null
  for _ in $(seq 1 120); do
    [ "$(C "print(db.getSiblingDB('shop').tunku.countDocuments({_id:'__sentinel'}))")" = "1" ] && break
    sleep 0.5
  done
  H "db.getSiblingDB('shop').tunku.deleteOne({_id:'__sentinel'})" >/dev/null
  C "db.getSiblingDB('shop').tunku.deleteOne({_id:'__sentinel'})" >/dev/null
}

echo "== setup =="
stopDaemons
for t in tunku tunki tunkm; do
  H "db.getSiblingDB('shop').$t.drop()" >/dev/null
  C "db.getSiblingDB('shop').$t.drop()" >/dev/null
done
C "db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'});
   db.getSiblingDB('_thermocline').watermarks.deleteMany({});
   db.getSiblingDB('_thermocline').residency.deleteMany({});
   db.getSiblingDB('_thermocline').evicting.deleteMany({})" >/dev/null
startFeeder
startRouter

echo "== an unacknowledged upsert over a document only the archive holds =="
# On one cluster this updates the existing document and nothing is lost. Here the
# fast tier cannot see it, so an unjudged upsert creates a stub in its place and
# the feeder mirrors that over the archived record.
C "db.getSiblingDB('shop').tunku.insertOne({_id:5, name:'alice', tier:'gold', balance:100})" >/dev/null
chk "the fast tier has never held it"                      "0" \
    "$(H "print(db.getSiblingDB('shop').tunku.countDocuments({_id:5}))")"
R "db.getSiblingDB('shop').tunku.updateOne({_id:5},{\$set:{balance:200}},{upsert:true,writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "the fields it could not see survived"                 "alice-gold-200" \
    "$(R "const d=db.getSiblingDB('shop').tunku.findOne({_id:5}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"
chk "and the archive was not overwritten by a stub"        "alice-gold-200" \
    "$(C "const d=db.getSiblingDB('shop').tunku.findOne({_id:5}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"

echo "== an unacknowledged insert of an _id the archive still holds =="
# On one cluster this is a duplicate key error and the existing document is
# untouched. The client asked not to be told, but it did not ask for the
# archived document to be replaced.
C "db.getSiblingDB('shop').tunki.insertOne({_id:9, name:'carol', tier:'silver', balance:70})" >/dev/null
chk "the archive holds it and the fast tier does not"      "0" \
    "$(H "print(db.getSiblingDB('shop').tunki.countDocuments({_id:9}))")"
R "db.getSiblingDB('shop').tunki.insertOne({_id:9, v:'overwrite'},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "the archived document is still whole"                 "carol-silver-70" \
    "$(C "const d=db.getSiblingDB('shop').tunki.findOne({_id:9}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"

echo "== an unacknowledged deleteMany that reaches archived documents =="
# On one cluster this removes all twelve. Here nine of them are only in the
# archive, and without the copy-up the delete never sees them: they stay
# readable through the router afterwards, which is an erasure that did not erase.
H "db.getSiblingDB('shop').tunkm.insertMany(Array.from({length:12},(_,i)=>({_id:i+1,grp:'g',n:i})))" >/dev/null
waitFeeder
# Take nine away from the fast tier only, which is what eviction leaves behind.
evictBehindTheFeeder "db.getSiblingDB('shop').tunkm.deleteMany({_id:{\$gt:3}})"
chk "the fast tier holds three of twelve"                  "3" \
    "$(H "print(db.getSiblingDB('shop').tunkm.countDocuments({}))")"
chk "the archive still holds all twelve"                   "12" \
    "$(C "print(db.getSiblingDB('shop').tunkm.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
R "db.getSiblingDB('shop').tunkm.deleteMany({grp:'g'},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 4
waitFeeder
chk "nothing matching is left to read"                     "0" \
    "$(R "print(db.getSiblingDB('shop').tunkm.countDocuments({grp:'g'}))")"

echo "== and an ordinary unacknowledged write still lands =="
# The fix must not turn every w:0 write into a dropped one.
R "db.getSiblingDB('shop').tunku.insertOne({_id:50, v:'plain'},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 2
chk "a write with nothing to judge is performed"           "plain" \
    "$(H "const d=db.getSiblingDB('shop').tunku.findOne({_id:50}); print(d?d.v:'MISSING')")"
chk "and it reaches the archive too"                       "plain" \
    "$(C "const d=db.getSiblingDB('shop').tunku.findOne({_id:50}); print(d?d.v:'MISSING')")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
