#!/bin/bash
# The warmer's completeness claim, and what it takes to withdraw one.
#
# A completeness claim is the router's licence to answer a range query from the
# fast tier alone. If the fast tier quietly loses documents inside a claimed
# range, every such query returns a short answer and reports success. Nothing
# else in the system notices, which is why the warmer has to.
#
# The hard part is telling a hole from a deletion. The archive is upsert only,
# so a document the application deleted is tombstoned there rather than removed:
# what is live in the archive is exactly what the fast tier should hold. Both
# directions are tested here, because a check that refilled deleted documents
# would be worse than no check at all.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
NS=orders
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-48s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-48s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do
    pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0
    sleep 0.5
  done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/warm_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/warm_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin \
      --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
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
# Remove documents from the fast tier without the feeder seeing it. This is what
# a restore from an old backup, or a botched repair, looks like: the archive
# still holds them live, so they are missing rather than deleted.
loseFromFastTier() {
  pkill -f 'cline feeder' 2>/dev/null
  for _ in $(seq 1 20); do pgrep -f 'cline feeder' >/dev/null 2>&1 || break; sleep 0.5; done
  H "$1" >/dev/null
  C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder
}

echo "== setup =="
stopDaemons
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({})" >/dev/null
startFeeder
startRouter

# Real ObjectId ids, because this rule reads a document's age from the timestamp
# inside its _id. Integer ids sort before every ObjectId in BSON, so a window
# filter of _id >= ObjectId(...) matches none of them and every count comes back
# zero, which looks exactly like a fast tier with nothing missing.
R "const c=db.getSiblingDB('shop').$NS; const d=[];
   for(let i=0;i<20;i++) d.push({_id:new ObjectId(),n:i,tag:'warm'});
   c.insertMany(d);" >/dev/null
waitMirrored 20
chk "seeded and mirrored" "20" "$(C "print(db.getSiblingDB('shop').$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== a full fast tier earns a claim =="
cline warmer --config $CFG >/dev/null 2>&1
chk "the watermark is claimed" "true" "$(cline watermark --config $CFG --ns shop.$NS 2>&1 | grep -qE 'newer than|all of history' && echo true || echo false)"
chk "the fast tier still holds everything" "20" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

echo "== a hole inside the claimed range must withdraw the claim =="
# Eight documents vanish from the fast tier. The archive still has them live, so
# this is loss, not deletion, and the claim is now a lie.
loseFromFastTier "db.getSiblingDB('shop').$NS.deleteMany({n:{\$lt:8}})"
chk "the fast tier is short"           "12" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "the archive still holds them all" "20" "$(C "print(db.getSiblingDB('shop').$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
rm -f /var/log/cline/warm_verify.log
cline warmer --config $CFG --log-file /var/log/cline/warm_verify.log >/dev/null 2>&1
chk "the warmer said the claim was unsupportable" "true" \
    "$(grep -qE 'discarding a completeness claim' /var/log/cline/warm_verify.log && echo true || echo false)"
chk "and reported how many were missing"          "true" \
    "$(grep -qE 'missing=8' /var/log/cline/warm_verify.log && echo true || echo false)"
chk "the hole was refilled"                       "20" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "and the claim is back"                       "true" \
    "$(cline watermark --config $CFG --ns shop.$NS 2>&1 | grep -qE 'newer than|all of history' && echo true || echo false)"

echo "== a deletion is not a hole =="
# The opposite case, and the dangerous one. Five documents are deleted through
# the router, so the feeder tombstones them in the archive. The fast tier now
# legitimately holds fewer than the archive stores, and a warmer that treated
# that as loss would bring deleted records back to life.
R "db.getSiblingDB('shop').$NS.deleteMany({n:{\$gte:15}})" >/dev/null
waitMirrored 15
chk "the fast tier holds the survivors" "15" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "the archive tombstoned the rest"   "5"  "$(C "print(db.getSiblingDB('shop').$NS.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
rm -f /var/log/cline/warm_del.log
cline warmer --config $CFG --log-file /var/log/cline/warm_del.log >/dev/null 2>&1
chk "the claim survives a deletion"     "false" \
    "$(grep -qE 'discarding a completeness claim' /var/log/cline/warm_del.log && echo true || echo false)"
chk "and nothing was resurrected"       "15" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "the deleted documents stay gone"   "0"  "$(R "print(db.getSiblingDB('shop').$NS.countDocuments({n:{\$gte:15}}))")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
