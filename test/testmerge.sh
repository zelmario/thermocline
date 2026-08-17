#!/bin/bash
# $merge into a tiered collection, where the document it should match is in the
# archive.
#
# $merge is a write that decides what to do by looking at what is already there.
# whenMatched needs to find the existing document; whenNotMatched decides what
# happens when it does not. Both of those questions are answered by the fast
# tier, because that is where the stage runs, and the fast tier is exactly the
# thing that does not hold every document.
#
# So the shape of the danger is the one the upsert bug already had. A $merge
# whose target document lives only in the archive matches nothing, inserts a new
# document built from the pipeline's output instead, and the feeder then mirrors
# that over the archived record. The fields the archived version carried and the
# pipeline did not are gone from both tiers, and the whole thing is reported as
# success.
#
# Nothing here asserts a value by hand where it can avoid it: shop.tmref is an
# untiered reference holding the same documents, every merge runs against both,
# and the answers have to match. A failure is the router disagreeing with
# MongoDB.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
NS=tmrg
REF=tmref
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s\n          ref=%.140s\n          got=%.140s\n' "$1" "$2" "$3"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() { pkill -9 -f 'cline feeder' 2>/dev/null; pkill -9 -f 'cline router' 2>/dev/null; }
trap cleanup EXIT

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/merge_f.log &
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/merge_r.log &
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}

# The document as the application would see it, with volatile fields removed.
doc() { R "const d=db.getSiblingDB('shop').$1.findOne({_id:5}); if(!d){print('MISSING')}else{delete d._tcDeletedAt; print(JSON.stringify(d))}"; }
# And as the archive holds it, which is where the loss would be.
cold5() { C "const d=db.getSiblingDB('shop').$NS.findOne({_id:5}); if(!d){print('MISSING')}else{delete d._tcDeletedAt; print(JSON.stringify(d))}"; }

# reseed puts the reference whole on the fast tier, and the tiered one with _id 5
# living only in the archive, which is what an evicted document looks like.
reseed() {
  local doc5='{_id:5, name:"alice", tier:"gold", balance:100}'
  H "db.getSiblingDB('shop').$REF.drop(); db.getSiblingDB('shop').$REF.insertMany([$doc5,{_id:6,name:'bob',tier:'silver',balance:50}])" >/dev/null
  H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
  C "db.getSiblingDB('shop').$NS.drop()" >/dev/null
  H "db.getSiblingDB('shop').$NS.insertOne({_id:6,name:'bob',tier:'silver',balance:50})" >/dev/null
  C "db.getSiblingDB('shop').$NS.bulkWrite([
       {replaceOne:{filter:{_id:5},replacement:$doc5,upsert:true}},
       {replaceOne:{filter:{_id:6},replacement:{_id:6,name:'bob',tier:'silver',balance:50},upsert:true}}],{ordered:false})" >/dev/null
  cline watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null 2>&1
  sleep 1
}

echo "== setup =="
stopDaemons
C "db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'});
   db.getSiblingDB('_thermocline').watermarks.deleteMany({});
   db.getSiblingDB('_thermocline').residency.deleteMany({});
   db.getSiblingDB('_thermocline').markers.deleteMany({})" >/dev/null
startFeeder
startRouter
reseed
chk "the tiered fast tier does not hold it"           "0" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({_id:5}))")"
chk "but the archive does"                            "1" "$(C "print(db.getSiblingDB('shop').$NS.countDocuments({_id:5}))")"
# By a field, never by _id. A point lookup by _id is the one read that promotes,
# so checking the fixture that way brings the document to the fast tier and every
# merge below then finds it there, testing nothing. That is how the first two
# sections of this suite passed while the archived case was untested.
chk "and the router can still see it"                 "alice" \
    "$(R "const d=db.getSiblingDB('shop').$NS.findOne({name:'alice'}); print(d?d.name:'MISSING')")"

mergeInto() { # collection -> run the same $merge against it
  R "db.getSiblingDB('shop').aggregate([
       {\$documents:[{_id:5, balance:200}]},
       {\$merge:{into:'$1', whenMatched:'merge', whenNotMatched:'insert'}}
     ]).toArray(); print('done')" >/dev/null 2>&1
}

echo "== whenMatched merge, against a document only the archive holds =="
chk "the fast tier still does not hold it"            "0" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({_id:5}))")"
mergeInto $REF
mergeInto $NS
sleep 6
chk "the merged document matches the reference"       "$(doc $REF)" "$(doc $NS)"
chk "and the archive kept the fields it had"          "$(doc $REF)" "$(cold5)"

echo "== whenMatched replace =="
reseed
R "db.getSiblingDB('shop').aggregate([
     {\$documents:[{_id:5, balance:300}]},
     {\$merge:{into:'$REF', whenMatched:'replace', whenNotMatched:'insert'}}
   ]).toArray()" >/dev/null 2>&1
R "db.getSiblingDB('shop').aggregate([
     {\$documents:[{_id:5, balance:300}]},
     {\$merge:{into:'$NS', whenMatched:'replace', whenNotMatched:'insert'}}
   ]).toArray()" >/dev/null 2>&1
sleep 6
chk "a replacing merge matches the reference"         "$(doc $REF)" "$(doc $NS)"

echo "== whenMatched fail, which must fail rather than insert =="
reseed
reffail=$(R "try{db.getSiblingDB('shop').aggregate([{\$documents:[{_id:5,balance:400}]},
   {\$merge:{into:'$REF', whenMatched:'fail', whenNotMatched:'insert'}}]).toArray(); print('ACCEPTED')}catch(e){print('FAILED')}")
gotfail=$(R "try{db.getSiblingDB('shop').aggregate([{\$documents:[{_id:5,balance:400}]},
   {\$merge:{into:'$NS', whenMatched:'fail', whenNotMatched:'insert'}}]).toArray(); print('ACCEPTED')}catch(e){print('FAILED')}")
chk "whenMatched fail behaves the same on both"       "$reffail" "$gotfail"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
