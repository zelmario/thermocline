#!/bin/bash
# A read served from the archive must be as fresh as one served from the fast
# tier.
#
# The archive trails the fast tier by the feeder's mirroring lag, so a fallback
# read used to be as fresh as a secondary read. Two things go wrong with that,
# and only one of them is merely slow:
#
#   a write that already returned to the client is not in the archive yet, so a
#   query that should match it does not;
#
#   a document the client already deleted is still live in the archive, because
#   its tombstone has not been mirrored, so a query returns it.
#
# The second is a wrong answer, not a stale one. Deleting a record and then
# finding it again is the kind of thing that ends up in an incident report.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
NS=tprobe
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s got=%s want=%s\n' "$1" "$3" "$2"; fi
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
  cline feeder --config $CFG --log-file /var/log/cline/fresh_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  CFGFILE=${1:-$CFG}
  cline router --config $CFGFILE --log-file /var/log/cline/fresh_r.log & RP=$!
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

echo "== setup =="
stopDaemons
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({})" >/dev/null
startFeeder
startRouter
R "const c=db.getSiblingDB('shop').$NS; const d=[];
   for(let i=0;i<10;i++) d.push({_id:i,n:i,grp:'a'});
   c.insertMany(d);" >/dev/null
waitMirrored 10

# Every read below falls through to the archive, which is the whole point.
cline watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "reads are being answered from the archive" "10" "$(R "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

echo "== a write that already returned must be in the answer =="
R "db.getSiblingDB('shop').$NS.insertOne({_id:100,n:100,grp:'a'})" >/dev/null
chk "the new document is counted at once"  "11" "$(R "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "and it comes back in a range query"   "1"  "$(R "print(db.getSiblingDB('shop').$NS.find({n:100}).itcount())")"

echo "== a document already deleted must not come back =="
R "db.getSiblingDB('shop').$NS.deleteOne({_id:100})" >/dev/null
chk "the deleted document is gone at once"  "10" "$(R "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "and a range query does not find it"    "0"  "$(R "print(db.getSiblingDB('shop').$NS.find({n:100}).itcount())")"
chk "a point lookup does not find it"       "null" "$(R "print(db.getSiblingDB('shop').$NS.findOne({_id:100}))")"

echo "== the same, in a tight write-then-read loop =="
# One iteration is easy to pass by luck. Ten in a row with no pause is where a
# read that does not wait for mirroring will show up.
chk "ten write-then-read pairs all agree" "ok" "$(R "
  const c = db.getSiblingDB('shop').$NS;
  for (let i = 200; i < 210; i++) {
    c.insertOne({_id:i, n:i, grp:'b'});
    if (c.countDocuments({n:i}) !== 1) { print('missing write ' + i); quit(); }
    c.deleteOne({_id:i});
    if (c.countDocuments({n:i}) !== 0) { print('resurrected ' + i); quit(); }
  }
  print('ok');")"

echo "== a stopped feeder must not become an outage =="
# With no mirroring at all the router cannot be fresh. It has to answer anyway
# and say so, because turning a stopped background daemon into failed queries
# would be a worse failure than a stale read.
pkill -f 'cline feeder' 2>/dev/null
for _ in $(seq 1 20); do pgrep -f 'cline feeder' >/dev/null 2>&1 || break; sleep 0.5; done
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
START=$(date +%s)
GOT=$(R "print(db.getSiblingDB('shop').$NS.countDocuments({}))")
ELAPSED=$(( $(date +%s) - START ))
chk "the read is still answered"           "10"   "$GOT"
chk "and it did not hang waiting"          "true" "$([ "$ELAPSED" -lt 8 ] && echo true || echo false)"
chk "the router said the answer was stale" "true" \
    "$(grep -qE 'answering from the archive before mirroring caught up' /var/log/cline/fresh_r.log && echo true || echo false)"

startFeeder
stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
