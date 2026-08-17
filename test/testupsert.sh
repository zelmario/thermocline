#!/bin/bash
# An upsert whose document lives only in the archive.
#
# This is the one write shape where matching nothing on the fast tier is not the
# end of it. Every other write that matches nothing is resolved by copying the
# archived document up and running the write again. An upsert that matches
# nothing does not wait to be resolved: the fast tier creates a document there
# and then, built from the filter's equalities and the update, and reports
# success.
#
# That new document carries the same _id as the archived one and almost none of
# its fields. It is not a stale copy, it is a different document wearing the same
# name, and the feeder mirrors it up as an upsert, so the archived record is
# overwritten by it. An upsert is how every "insert or update" in an application
# is written, so this is not an exotic path.
#
# What has to happen instead: the archived document comes up first, and then the
# upsert finds it and updates it, which is what the same statement would have
# done with no tiering at all.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
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
  cline feeder --config $CFG --log-file /var/log/cline/ups_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/ups_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}

echo "== setup =="
stopDaemons
H "db.getSiblingDB('shop').tups.drop(); db.getSiblingDB('shop').tupsf.drop()" >/dev/null
C "db.getSiblingDB('shop').tups.drop(); db.getSiblingDB('shop').tupsf.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({}); db.getSiblingDB('_thermocline').residency.deleteMany({}); db.getSiblingDB('_thermocline').evicting.deleteMany({})" >/dev/null
startFeeder
startRouter

# Archive only, which is what an evicted document looks like: the fast tier has
# never held it. name and tier are the fields an upsert cannot reconstruct.
C "db.getSiblingDB('shop').tups.insertOne({_id:5, name:'alice', tier:'gold', balance:100})" >/dev/null
C "db.getSiblingDB('shop').tupsf.insertOne({_id:5, name:'alice', tier:'gold', balance:100})" >/dev/null
chk "the fast tier has never held it"                   "0" \
    "$(H "print(db.getSiblingDB('shop').tups.countDocuments({_id:5}))")"
chk "the archive holds it in full"                      "alice-gold-100" \
    "$(C "const d=db.getSiblingDB('shop').tups.findOne({_id:5}); print(d.name+'-'+d.tier+'-'+d.balance)")"

echo "== update with upsert:true on an archived document =="
UP=$(R "const r=db.getSiblingDB('shop').tups.updateOne({_id:5},{\$set:{balance:200}},{upsert:true}); print(r.matchedCount+'/'+r.upsertedCount)")
chk "it updates rather than creating a second one"      "1/0" "$UP"
chk "the other fields survived"                         "alice-gold-200" \
    "$(R "const d=db.getSiblingDB('shop').tups.findOne({_id:5}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"

# The real damage is downstream: whatever the fast tier now holds is mirrored up
# as an upsert, so a stub there overwrites the archived record permanently.
sleep 5
chk "and the archive was not overwritten by a stub"     "alice-gold-200" \
    "$(C "const d=db.getSiblingDB('shop').tups.findOne({_id:5}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"

echo "== findAndModify with upsert:true on an archived document =="
FAM=$(R "const d=db.getSiblingDB('shop').tupsf.findOneAndUpdate({_id:5},{\$set:{balance:300}},{upsert:true,returnDocument:'after'}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")
chk "findAndModify keeps the archived fields"           "alice-gold-300" "$FAM"
sleep 5
chk "and the archive kept them too"                     "alice-gold-300" \
    "$(C "const d=db.getSiblingDB('shop').tupsf.findOne({_id:5}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"

echo "== bulkWrite upserts the same way =="
# bulkWrite copies up before it runs for every filtered operation, so this should
# already hold. It is checked because it is the same class of loss and nothing
# else pins it.
C "db.getSiblingDB('shop').tups.insertOne({_id:6, name:'carol', tier:'silver', balance:100})" >/dev/null
R "db.adminCommand({bulkWrite:1, nsInfo:[{ns:'shop.tups'}], ops:[
     {update:0, filter:{_id:6}, updateMods:{\$set:{balance:400}}, upsert:true}]})" >/dev/null 2>&1
chk "a bulkWrite upsert keeps the archived fields"      "carol-silver-400" \
    "$(R "const d=db.getSiblingDB('shop').tups.findOne({_id:6}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"

echo "== a genuinely new document still upserts =="
# The guard must not turn every upsert into a lookup that refuses to create.
NEW=$(R "const r=db.getSiblingDB('shop').tups.updateOne({_id:99},{\$set:{balance:1}},{upsert:true}); print(r.upsertedCount)")
chk "an id in neither tier is created"                  "1" "$NEW"
chk "and it is there"                                   "1" \
    "$(R "print(db.getSiblingDB('shop').tups.countDocuments({_id:99}))")"

echo "== and one already on the fast tier is untouched =="
R "db.getSiblingDB('shop').tups.insertOne({_id:7,name:'bob',balance:10})" >/dev/null
chk "a normal upsert still updates in place"            "bob-50" \
    "$(R "db.getSiblingDB('shop').tups.updateOne({_id:7},{\$set:{balance:50}},{upsert:true}); const d=db.getSiblingDB('shop').tups.findOne({_id:7}); print(d.name+'-'+d.balance)")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
