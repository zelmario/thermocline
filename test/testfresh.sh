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

echo "== deleting a document the fast tier does not hold =="
# The delete that the barrier could not see. A deleteOne naming a document the
# fast tier does not hold matches nothing there, and that first reply is the one
# the router noted the cluster time from. The interception then copies the
# document up and runs the delete again, and that is when the document actually
# goes. Noting only the first reply left the barrier waiting for an instant
# before the delete, so the read that followed was told mirroring had caught up
# while the tombstone was still in flight.
#
# Measured with the feeder given a backlog: four rounds in ninety deleted a
# document, were told deletedCount 1, and counted it again immediately after.
# stale_reads stayed at zero throughout, because the barrier believed it had
# caught up, which is what made it invisible.
#
# Made deterministic here by stopping the feeder outright, so mirroring cannot
# catch up at all. The barrier must then either wait or say it gave up; what it
# must not do is answer at once and say nothing.
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:{\$gte:300}})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:{\$gte:300}})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:301, n:301, grp:'c'})" >/dev/null
chk "the archive holds it and the fast tier does not" "1/0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:301}))")/$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:301}))")"
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
rm -f $LOGDIR/fresh_del.log
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:301})" >/dev/null
SAW=$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:301}))")
# Either answer is defensible with mirroring stopped. Answering 1 in silence is
# not: that is a deleted document handed back with nothing said.
chk "it does not come back, or the router says why" "true" \
    "$([ "$SAW" = "0" ] && echo true || { grep -q 'before mirroring caught up' $LOGDIR/fresh_r.log && echo true || echo false; })"
startFeeder
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:301,_tcDeletedAt:{\$exists:false}}))")" = "0" ] && break
  sleep 0.5
done
chk "and once mirroring catches up it is gone"     "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:301}))")"

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

echo "== the caller's deadline is spent once, not twice =="
# The wait above is bounded by the caller's maxTimeMS, and the command used to
# carry that same maxTimeMS onward to the archive, so a caller could be charged
# its deadline twice. Measured before the fix: a client asking for 800ms with
# mirroring behind waited 300ms here, handed the archive 800ms of its own, and
# got a successful answer after 1117ms.
#
# The wait takes at most half the deadline now and the read gets the rest, so
# neither can starve the other. Timed inside mongosh rather than around it,
# because timing the whole invocation measures node starting up: that mistake
# already cost this suite a false failure once.
#
# The feeder is still stopped from the section above, but its checkpoint is put
# back, because a missing one makes the router give up at once and there is then
# no wait to measure at all.
C 'db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:"test"},{$set:{eventAt:new Date(Date.now()-3600000),savedAt:new Date(Date.now()-3600000)}},{upsert:true})' >/dev/null
ELAPSED_MS=$(R "const t=Date.now();
   try { db.getSiblingDB(process.env.TC_DB).$NS.find({}).maxTimeMS(1000).itcount(); } catch(e) {}
   print(Date.now()-t)")
chk "a read is not charged its deadline twice" "true" \
    "$([ "${ELAPSED_MS:-99999}" -lt 900 ] && echo true || echo false)"
echo "      (the read took ${ELAPSED_MS} ms of a 1000 ms deadline)"

startFeeder
echo "== a feeder that has stopped, rather than one that is behind =="
# The barrier waits for mirroring to reach what this client has already done. A
# feeder that is merely behind gets there, and waiting is right. A feeder that
# has stopped never will, and waiting costs the whole archive_wait on every
# read.
#
# It is worse than one wait, because a read of an archived document copies it up,
# and that copy-up is itself a write the next read then waits to see mirrored.
# So the cost is the full barrier per read, for ever.
#
# Measured with the feeder stopped and the fast tier emptied so every read
# copies one up: 10 reads took 45,717ms, 4.6 seconds each, with eleven stale
# read warnings. The same 10 took 13ms once a stale position was read as "this
# feeder has stopped" rather than "mirroring is behind".
#
# A position record that exists and has gone stale is the evidence. A feeder too
# old to publish one leaves only a checkpoint, and that case still falls back to
# it, because there is nothing else to go on.
FNS=tfstopped
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$FNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$FNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$FNS.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i})))" >/dev/null
chk "the archive holds them and the fast tier does not" "10/0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({}))")/$(H "print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({}))")"
# A feeder runs long enough to publish a position, then stops, which is what
# leaves the record this reads.
startFeeder
sleep 3
stopDaemons
sleep 4
chk "a position was left behind"                  "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).positions.countDocuments({_id:'test'}))")"
rm -f $LOGDIR/fresh_r.log
startRouter
# A write first, so the barrier has something to wait for, then reads that each
# copy a document up.
START=$(date +%s%3N)
R "const d=db.getSiblingDB(process.env.TC_DB); d.$FNS.insertOne({_id:99,n:99});
   for (let i=0;i<5;i++) d.$FNS.findOne({_id:i});" >/dev/null
ELAPSED=$(( $(date +%s%3N) - START ))
echo "      five reads that each copy a document up took ${ELAPSED}ms"
# Five reads at the 5s barrier would be 25 seconds. Anything under a few seconds
# means the router stopped waiting for a feeder that is not coming back.
chk "the reads do not each wait out the barrier"  "true" \
    "$(awk -v e="$ELAPSED" 'BEGIN{print (e+0 < 5000) ? "true" : "false"}')"
# And it must still say so: answering without the barrier is a real weakening of
# what the read promises, and silence about it would be the wrong trade.
chk "and it says the answer may be behind"        "true" \
    "$(grep -q 'before mirroring caught up' $LOGDIR/fresh_r.log && echo true || echo false)"
# Every document the archive holds is still returned, which is the whole point of
# answering rather than refusing.
#
# Ten, not eleven. The insert above is on the fast tier and was never mirrored,
# because the feeder is stopped, and this read is answered by the archive. That
# is exactly the staleness the warning announces and it is the trade being made:
# an answer that may miss a very recent write, rather than 4.6 seconds of waiting
# for a feeder that is not coming back. Refusing instead would be worse; so
# would waiting.
chk "every mirrored document is still returned"   "10" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({}))")"
chk "and the archive is what that matches"        "10" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$FNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$FNS.drop()" >/dev/null

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
