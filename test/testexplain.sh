#!/bin/bash
# What explain says about a query the fast tier cannot answer.
#
# explain was not a command the router knew, so it went to the fast tier and came
# back describing the fast tier. That is misleading in a specific way: the plan
# is for a query that may not run there at all, and executionStats reports
# nReturned from a partial copy, so an explain of a query returning ten documents
# can say three.
#
# Refusing explain is not an option, because Compass runs it and so does every
# index review. So the plan is passed through intact and a thermocline field is
# added beside it. This suite checks both halves: the field appears when it
# should, stays away when it should not, and nothing that reads the plan breaks.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
NS=texpl
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
  cline feeder --config $CFG --log-file /var/log/cline/expl_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/expl_r.log & RP=$!
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
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({}); db.getSiblingDB('_thermocline').residency.deleteMany({})" >/dev/null
startFeeder
startRouter
R "db.getSiblingDB('shop').$NS.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i,grp:'g'})))" >/dev/null
waitMirrored 10

echo "== while the fast tier is whole, explain is left alone =="
cline watermark --config $CFG --ns shop.$NS --complete-all >/dev/null
sleep 7
chk "explain still returns a plan"                      "true" \
    "$(R "print(!!db.getSiblingDB('shop').$NS.find({}).explain().queryPlanner)")"
chk "and says nothing extra"                            "false" \
    "$(R "print(!!db.getSiblingDB('shop').$NS.find({}).explain().thermocline)")"

echo "== take the documents out from under it =="
stopFeeder
H "db.getSiblingDB('shop').$NS.deleteMany({_id:{\$lt:7}})" >/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
chk "the fast tier now holds three"                     "3" \
    "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
cline watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the query itself still answers in full"            "10" \
    "$(R "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

echo "== now explain has to say the plan is not the whole story =="
# Without this the plan looks healthy and nothing in the reply hints that the
# query it describes would be answered somewhere else entirely.
chk "explain still returns a plan"                      "true" \
    "$(R "print(!!db.getSiblingDB('shop').$NS.find({}).explain().queryPlanner)")"
chk "explain says the fast tier is not complete"        "false" \
    "$(R "print(db.getSiblingDB('shop').$NS.find({}).explain().thermocline.fastTierComplete)")"
chk "and says why"                                      "true" \
    "$(R "const t=db.getSiblingDB('shop').$NS.find({}).explain().thermocline; print(!!t.because && t.because.length>20)")"
chk "and warns the counts read low"                     "true" \
    "$(R "print(/partial copy/.test(db.getSiblingDB('shop').$NS.find({}).explain().thermocline.note))")"

# executionStats is the verbosity that reports nReturned, which is the number a
# reader is most likely to trust and the one most likely to be wrong.
chk "executionStats is annotated too"                   "false" \
    "$(R "print(db.getSiblingDB('shop').$NS.find({}).explain('executionStats').thermocline.fastTierComplete)")"

echo "== aggregations and the other read commands too =="
chk "an aggregation explain is annotated"               "false" \
    "$(R "print(db.getSiblingDB('shop').$NS.explain().aggregate([{\$group:{_id:null,n:{\$sum:1}}}]).thermocline.fastTierComplete)")"
chk "an explained count is annotated"                   "false" \
    "$(R "const e=db.getSiblingDB('shop').runCommand({explain:{count:'$NS'}}); print(e.thermocline.fastTierComplete)")"
chk "an explained distinct is annotated"                "false" \
    "$(R "const e=db.getSiblingDB('shop').runCommand({explain:{distinct:'$NS',key:'grp'}}); print(e.thermocline.fastTierComplete)")"

echo "== and a collection nobody tiers is never touched =="
H "db.getSiblingDB('shop').texplfree.drop(); db.getSiblingDB('shop').texplfree.insertOne({_id:1})" >/dev/null
chk "an untiered collection's explain is untouched"     "false" \
    "$(R "print(!!db.getSiblingDB('shop').texplfree.find({}).explain().thermocline)")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
