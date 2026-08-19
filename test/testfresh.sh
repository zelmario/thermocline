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
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
NS=tprobe
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do
    pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0
    sleep 0.5
  done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/fresh_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  CFGFILE=${1:-$CFG}
  cline router --config $CFGFILE --log-file $LOGDIR/fresh_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin \
      --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}
waitMirrored() {
  local n=""
  for _ in $(seq 1 60); do
    n=$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$1" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the archive never reached $1 live documents, last saw ${n:-none}"
}

echo "== setup =="
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).positions.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})" >/dev/null
startFeeder
startRouter
R "const c=db.getSiblingDB(process.env.TC_DB).$NS; const d=[];
   for(let i=0;i<10;i++) d.push({_id:i,n:i,grp:'a'});
   c.insertMany(d);" >/dev/null
waitMirrored 10

# Every read below falls through to the archive, which is the whole point.
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "reads are being answered from the archive" "10" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

echo "== a write that already returned must be in the answer =="
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:100,n:100,grp:'a'})" >/dev/null
chk "the new document is counted at once"  "11" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "and it comes back in a range query"   "1"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({n:100}).itcount())")"

echo "== a document already deleted must not come back =="
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:100})" >/dev/null
chk "the deleted document is gone at once"  "10" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "and a range query does not find it"    "0"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({n:100}).itcount())")"
chk "a point lookup does not find it"       "null" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:100}))")"

echo "== the same, in a tight write-then-read loop =="
# One iteration is easy to pass by luck. Ten in a row with no pause is where a
# read that does not wait for mirroring will show up.
chk "ten write-then-read pairs all agree" "ok" "$(R "
  const c = db.getSiblingDB(process.env.TC_DB).$NS;
  for (let i = 200; i < 210; i++) {
    c.insertOne({_id:i, n:i, grp:'b'});
    if (c.countDocuments({n:i}) !== 1) { print('missing write ' + i); quit(); }
    c.deleteOne({_id:i});
    if (c.countDocuments({n:i}) !== 0) { print('resurrected ' + i); quit(); }
  }
  print('ok');")"

echo "== a fallback read waits for mirroring, not for a checkpoint =="
# The barrier can only be as precise as the position it waits on. While that
# position was the durable checkpoint, a fallback read paid a whole
# checkpoint_every even with the feeder perfectly caught up: 1.14 s at 1s, and
# 4.53 s at the shipped default of 5s, against 1.8 ms for the query itself.
# Timed inside the shell rather than around it. Timing the whole mongosh
# invocation adds its startup and connection to the number, which is a few
# hundred milliseconds of noise on a busy machine and has nothing to do with the
# barrier being measured: it failed at 957 ms on a host that was running a full
# regression at the time, with the read itself well inside the budget.
TIMED=$(R "const t=Date.now();
           const n=db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({});
           print(n + ' ' + (Date.now()-t));")
GOT=${TIMED%% *}
READ_MS=${TIMED##* }
chk "the fallback read is still correct"      "10"   "$GOT"
echo "      the fallback read itself took ${READ_MS} ms"
chk "and it took well under a second"         "true" "$([ "${READ_MS:-9999}" -lt 700 ] && echo true || echo false)"
echo "      (fallback read took ${READ_MS} ms)"

echo "== the position moves while the checkpoint stands still =="
# Those are two different questions and they get two different records. A
# checkpoint every 30s is a perfectly reasonable durability setting; it must not
# also mean a 30 second read.
stopDaemons
sed 's/checkpoint_every: 1s/checkpoint_every: 30s/' $CFG > $WDIR/fresh_pos.yaml
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"}); db.getSiblingDB(process.env.TC_META).positions.deleteOne({_id:"test"})' >/dev/null
cline feeder --config $WDIR/fresh_pos.yaml --log-file $LOGDIR/fresh_p.log & FP=$!
for _ in $(seq 1 60); do
  [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).positions.findOne({_id:"test"}))')" = "true" ] && break
  sleep 0.5
done
POS1=$(C 'const d=db.getSiblingDB(process.env.TC_META).positions.findOne({_id:"test"}); print(d?d.at.getTime():0)')
CKP1=$(C 'const d=db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}); print(d&&d.eventAt?d.eventAt.getTime():0)')
sleep 4
POS2=$(C 'const d=db.getSiblingDB(process.env.TC_META).positions.findOne({_id:"test"}); print(d?d.at.getTime():0)')
CKP2=$(C 'const d=db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}); print(d&&d.eventAt?d.eventAt.getTime():0)')
chk "the position advanced within four seconds" "true" "$([ "$POS2" -gt "$POS1" ] && echo true || echo false)"
chk "the checkpoint did not"                    "true" "$([ "$CKP2" = "$CKP1" ] && echo true || echo false)"
chk "the position says how often it is written" "true"     "$(C 'const d=db.getSiblingDB(process.env.TC_META).positions.findOne({_id:"test"}); print(!!(d&&d.everyMS>0))')"
pkill -f "cline feeder --config $WDIR/fresh_pos" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
# stopDaemons above took the router with it, and the section below reads through
# it. Leaving it down made a fixture failure look like a refused query.
startFeeder
startRouter

echo "== a stopped feeder must not become an outage =="
# With no mirroring at all the router cannot be fresh. It has to answer anyway
# and say so, because turning a stopped background daemon into failed queries
# would be a worse failure than a stale read.
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"}); db.getSiblingDB(process.env.TC_META).positions.deleteOne({_id:"test"})' >/dev/null
START=$(date +%s)
GOT=$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")
ELAPSED=$(( $(date +%s) - START ))
chk "the read is still answered"           "10"   "$GOT"
chk "and it did not hang waiting"          "true" "$([ "$ELAPSED" -lt 8 ] && echo true || echo false)"
chk "the router said the answer was stale" "true" \
    "$(grep -qE 'answering from the archive before mirroring caught up' $LOGDIR/fresh_r.log && echo true || echo false)"

startFeeder
stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
