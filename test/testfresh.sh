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

# This suite measures the barrier itself: how long a read waits, what it logs
# and when it gives up. Giving up answers from the archive only under
# router.freshness: permissive; the default since 2026-10-12 is strict, which
# refuses instead. So every section runs the opt-in, and the section at the end
# runs the default against the same stopped feeder.
STRICTCFG=$CFG
grep -q '^router:' "$STRICTCFG" || { echo "  FATAL  no router: block in $STRICTCFG"; exit 2; }
sed 's/^router:$/router:\n  freshness: permissive/' "$STRICTCFG" > $WDIR/fresh_permissive.yaml
CFG=$WDIR/fresh_permissive.yaml

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
#
# Judged against what the barrier actually promises, which is not "every write
# is visible". It waits up to router.archive_wait for mirroring to reach the
# archive and then answers anyway, counting a stale read and saying so at WARN.
# That is the documented trade, and under a loaded sweep the feeder can be
# behind by more than the budget: measured once in six sweeps, the router waited
# its five seconds, admitted the stale read and warned, and this check called it
# a failure. A suite that reports the design as a defect costs an investigation
# every time the box is busy.
#
# So the contract is checked instead: a short answer is allowed only if the
# router admitted it. Every missing write has to be matched by a stale read the
# router counted; one that is not is the defect this check exists for, a read
# answered early with nothing said. The loop counts rather than stopping at the
# first, because the comparison is between two totals.
staleReads() {
  curl -s "http://127.0.0.1:$PDASH/api/snapshot" 2>/dev/null |
    grep -oE '"stale_reads": *[0-9]+' | grep -oE '[0-9]+$'
}
SR0=$(staleReads)
LOOP=$(R "
  const c = db.getSiblingDB(process.env.TC_DB).$NS;
  let missed = 0;
  for (let i = 200; i < 210; i++) {
    c.insertOne({_id:i, n:i, grp:'b'});
    if (c.countDocuments({n:i}) !== 1) missed++;
    c.deleteOne({_id:i});
    if (c.countDocuments({n:i}) !== 0) missed++;
  }
  print(missed);")
SR1=$(staleReads)
ADMITTED=$(( ${SR1:-0} - ${SR0:-0} ))
echo "      ${LOOP:-?} of twenty reads were short, and the router admitted $ADMITTED stale read(s)"
chk "every short answer was one the router admitted" "true" \
    "$(awk -v m="${LOOP:-x}" -v a="$ADMITTED" 'BEGIN{print (m ~ /^[0-9]+$/ && m+0 <= a+0) ? "true" : "false"}')"

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

echo "== every deleted document that comes back is one the router admitted =="
# The section above asks the question once. This asks it eight times, through
# both of the paths a delete can take, and counts.
#
# It exists because of a fuzz disagreement reported on 2026-09-11 and written up
# as "a document deleted by a bulkWrite comes back in the next unbounded read
# with stale_reads at zero". Two things were wrong with that write-up and one of
# them was the evidence: the watch line it rested on printed `n=57` for the
# archive whether the archive held the document or its tombstone, so "still live
# in the archive" was never established. The seed then agreed six times in six.
#
# What could not be settled by re-running a seed is settled by asking the
# contract directly. With the feeder stopped the archive cannot catch up, so
# every read of a just-deleted document is offered the document; the barrier
# must wait out router.archive_wait and then count what it does. Measured at
# HEAD: 49 of 60 rounds were offered the document and the router admitted all
# 49, with the plain and the bulkWrite arms alternating.
#
# The two arms alternate rather than running in blocks, because a block of one
# and then a block of the other is two measurements of two different machine
# states. The bulkWrite is the fuzzer's shape: three operations, unordered, the
# delete in the middle.
DNS=tfdel
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
R "const c=db.getSiblingDB(process.env.TC_DB).$DNS;
   c.insertMany(Array.from({length:20},(_,i)=>({_id:i+1, n:i+1,
     when:new Date(Date.UTC(2024, i%12, 1+(i%27), 13,45,30,250))})));" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "20" ] && break
  sleep 0.5
done
cline watermark --config $CFG --ns $DB.$DNS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 3
# Five documents only the archive holds. Without them both tiers hold the same
# twenty and twenty proves nothing about which tier answered: the first version
# of this probe took a document off the fast tier instead, which the feeder
# promptly mirrored as a tombstone, and the guard failed against a router that
# was behaving perfectly.
C "db.getSiblingDB(process.env.TC_DB).$DNS.insertMany(
     Array.from({length:5},(_,i)=>({_id:41+i, n:41+i,
       when:new Date(Date.UTC(2024, i, 14, 13,45,30,250))})))" >/dev/null
chk "reads fall through to the archive"            "25" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({}))")"

DFEEDER=$(pgrep -f "cline feeder --config $WDIR/" | head -1)
# Named rather than assumed. With no pid the stops below are no-ops, mirroring
# keeps up, nothing is ever offered, and the section would report three passes
# for having asked nothing.
[ -n "$DFEEDER" ] || { echo "  FATAL  no feeder to stop, so this section would test nothing"; exit 2; }
DBACK=0; DSILENT=0; DSPLIT=0
for r in $(seq 1 8); do
  DID=$(( (r % 20) + 1 ))
  kill -STOP $DFEEDER 2>/dev/null
  DS0=$(staleReads)
  if [ $((r % 2)) -eq 1 ]; then
    DOUT=$(R "const c=db.getSiblingDB(process.env.TC_DB).$DNS;
              c.deleteOne({_id:$DID});
              const scan=c.find({}).sort({_id:1}).toArray().filter(x=>x._id===$DID).length;
              print(scan + ' ' + (c.findOne({_id:$DID}) ? 1 : 0));")
  else
    DOUT=$(R "const c=db.getSiblingDB(process.env.TC_DB).$DNS;
              c.bulkWrite([{updateOne:{filter:{_id:$(( (DID % 20) + 1 ))}, update:{\$set:{v:$r}}}},
                           {deleteOne:{filter:{_id:$DID}}},
                           {insertOne:{document:{_id:9000+$r, n:1, when:new Date(Date.UTC(2024,1,1))}}}],
                          {ordered:false});
              const scan=c.find({}).sort({_id:1}).toArray().filter(x=>x._id===$DID).length;
              print(scan + ' ' + (c.findOne({_id:$DID}) ? 1 : 0));")
  fi
  DS1=$(staleReads)
  kill -CONT $DFEEDER 2>/dev/null
  set -- $DOUT
  # The signature the write-up named: the same router, an instant apart, saying
  # the document is gone on a point read and returning it on a scan.
  [ "${1:-0}" = "1" ] && [ "${2:-0}" = "0" ] && DSPLIT=$((DSPLIT+1))
  if [ "${1:-0}" = "1" ]; then
    DBACK=$((DBACK+1))
    [ $(( ${DS1:-0} - ${DS0:-0} )) -eq 0 ] && DSILENT=$((DSILENT+1))
  fi
  # Put it back only once the tombstone has landed, or the next round starts
  # against an archive that is behind for a reason of its own.
  for _ in $(seq 1 60); do
    [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({_id:$DID,_tcDeletedAt:{\$exists:true}}))")" = "1" ] && break
    sleep 0.2
  done
  R "db.getSiblingDB(process.env.TC_DB).$DNS.insertOne({_id:$DID, n:$DID,
       when:new Date(Date.UTC(2024, $DID%12, 1+($DID%27), 13,45,30,250))})" >/dev/null 2>&1
done
kill -CONT $DFEEDER 2>/dev/null
echo "      $DBACK of 8 rounds were offered the deleted document"
# Vacuity first. A run where nothing came back tested the wait, not the thing
# the wait protects, and reporting it as a pass would be reporting silence.
chk "the archive did offer a deleted document"     "true" \
    "$([ "$DBACK" -gt 0 ] && echo true || echo false)"
chk "and none of them came back unadmitted"        "0" "$DSILENT"
chk "a scan and a point read never disagreed"      "0" "$DSPLIT"
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
# The last round's reinsert was not waited on, and the section after this one
# times a fallback read against a 700ms budget. Leaving mirroring behind here
# would spend that budget on this section's leftovers, which is a failure
# reported against the wrong code.
for _ in $(seq 1 60); do
  BEHIND=$(C 'const p=db.getSiblingDB(process.env.TC_META).positions.findOne({_id:"test"});
              print(p && p.at ? (Date.now() - p.at.getTime()) : 99999)')
  [ "${BEHIND:-99999}" -lt 1000 ] && break
  sleep 0.5
done

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
#
# The median of five, not one read. The defect cost every fallback read a
# checkpoint interval, so it moves the median; load slows some reads and not
# others. On 2026-10-07 one read took 767 ms under a four-worker sweep (load
# average 14) and 61 ms on the same build alone, and a single sample could not
# tell that from the defect.
TIMED=$(R "const ms=[]; let n=-1;
           for (let i=0; i<5; i++) {
             const t=Date.now();
             n=db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({});
             ms.push(Date.now()-t);
           }
           const s=ms.slice().sort((a,b)=>a-b);
           print(n + ' ' + s[2] + ' ' + ms.join(','));")
GOT=$(echo "$TIMED" | awk '{print $1}')
READ_MS=$(echo "$TIMED" | awk '{print $2}')
chk "the fallback read is still correct"      "10"   "$GOT"
echo "      five fallback reads took $(echo "$TIMED" | awk '{print $3}') ms, median ${READ_MS}"
chk "and they took well under a second"       "true" "$([ "${READ_MS:-9999}" -lt 700 ] 2>/dev/null && echo true || echo false)"

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
#
# The log goes with it, while nothing holds it open, the same way the sections
# further down do it. Without this the check below greps a file that has been
# accumulating since setup, so an earlier section's stale read satisfies it and
# it cannot fail on its own account: it would report PASS for a barrier that
# never fired here at all. Removing it with a router running would be worse than
# not removing it, because the router keeps writing to the unlinked file and the
# grep then finds nothing whatever happens.
rm -f $LOGDIR/fresh_r.log
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

echo "== the barrier across two clocks =="
# The wall-clock bar compares an instant the feeder wrote with one the router
# took, and the two daemons are not required to be on the same host. A router
# running a few seconds ahead of its feeder never clears that bar: every read
# that falls through waits the whole archive_wait and then answers stale anyway,
# which is slow and stale, the pair this barrier exists to avoid. The warning it
# prints is the one a genuinely lagging feeder prints, so nothing says why.
#
# Both instants are put in the archive's frame first. The feeder publishes how
# far ahead of the archive its own clock is, the router measures its own, and
# the archive is a reference neither has to trust the other about.
#
# Measuring that needs two clocks that differ, and both daemons here are on one
# machine. So the feeder is replaced by a record: a router pointed at a feeder id
# nothing is publishing under, and a loop that writes that id's position by hand.
# Moving it is what keeps it looking alive, and everything else about the two
# runs below is identical except the one field that says where the publisher's
# clock stands. The outcomes are opposite.
GHOST_NS=$NS
FID=$(sed -n '/^feeder:/,/^[a-z]/{s/^  id: *"\(.*\)"/\1/p}' $CFG | head -1)
stopDaemons
sed "/id:/s/\"$FID\"/\"${FID}ghost\"/" $CFG > $WDIR/fresh_skew.yaml
chk "the ghost config names a feeder nothing publishes for" "1" \
    "$(grep -c "^  id: \"${FID}ghost\"" $WDIR/fresh_skew.yaml)"
C "db.getSiblingDB(process.env.TC_META).positions.deleteOne({_id:'${FID}ghost'})" >/dev/null

# One mongosh for the whole loop: starting it per write costs a second each and
# the record has to move faster than the staleness window.
#
# BACK is how far behind the archive's present the published position is, and
# SKEW is what the publisher says about its own clock. A publisher two minutes
# behind the archive, whose position is a minute old by its own reckoning, has
# mirrored a minute past the present in the archive's frame.
moveGhost() {
  TC_GHOST="${FID}ghost" TC_BACK="$1" TC_SKEW="$2" \
  mongosh "$COLD" --quiet --eval '
    const d = db.getSiblingDB(process.env.TC_META);
    const back = Number(process.env.TC_BACK), skew = Number(process.env.TC_SKEW);
    for (let i = 0; i < 150; i++) {
      d.positions.updateOne({_id: process.env.TC_GHOST},
        {$set: {at: new Date(Date.now() - back), savedAt: new Date(),
                everyMS: 20, skewMS: skew, moved: i}}, {upsert: true});
      sleep(200);
    }' >/dev/null 2>&1 &
  GHOSTPID=$!
  for _ in $(seq 1 40); do
    [ "$(C "print(!!db.getSiblingDB(process.env.TC_META).positions.findOne({_id:'${FID}ghost'}))")" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the ghost position was never written"
}
stopGhost() { kill $GHOSTPID 2>/dev/null; wait $GHOSTPID 2>/dev/null; }

# A read through the router, timed inside mongosh. Timing the invocation would
# measure node starting up, which already cost this suite a false failure once.
timedRead() {
  R "const t=Date.now();
     const n=db.getSiblingDB(process.env.TC_DB).$GHOST_NS.countDocuments({});
     print(n + ' ' + (Date.now()-t));"
}

# What the archive holds now is what a fall-through read must return. Counted
# rather than assumed: the sections above have inserted and deleted in this
# namespace, and a hard-coded ten would be a check on their arithmetic.
WANT=$(C "print(db.getSiblingDB(process.env.TC_DB).$GHOST_NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")

# The uncorrected case first: a position a minute old, from a publisher that
# says its clock agrees with the archive. That is a feeder genuinely a minute
# behind, and waiting is the right answer.
moveGhost 60000 0
rm -f $LOGDIR/fresh_r.log
startRouter $WDIR/fresh_skew.yaml
# The router measures its own offset in the background and the first read that
# asks gets "not known yet" and the uncorrected comparison. That is deliberate:
# a correction applied on one side only would be worse than none. This read is
# what makes the offset known for the ones that follow.
timedRead >/dev/null
BEHIND=$(timedRead)
BEHIND_MS=${BEHIND##* }
echo "      a position a minute behind, publisher in step with the archive: ${BEHIND_MS} ms"
chk "a feeder genuinely behind is waited for"  "true" \
    "$([ "${BEHIND_MS:-0}" -ge 1000 ] && echo true || echo false)"
chk "and the read is answered anyway"          "$WANT" "${BEHIND%% *}"
stopDaemons
stopGhost

# The same record, one field different: the publisher's clock is two minutes
# behind the archive's. Its position is then a minute past the present in the
# archive's frame, and there is nothing to wait for.
moveGhost 60000 -120000
rm -f $LOGDIR/fresh_r.log
startRouter $WDIR/fresh_skew.yaml
timedRead >/dev/null
SKEWED=$(timedRead)
SKEWED_MS=${SKEWED##* }
echo "      the same position from a publisher two minutes behind the archive: ${SKEWED_MS} ms"
chk "a clock difference is not mirroring lag"  "true" \
    "$([ "${SKEWED_MS:-9999}" -lt 700 ] && echo true || echo false)"
chk "and that read is correct too"             "$WANT" "${SKEWED%% *}"
chk "nothing was answered short"               "false" \
    "$(grep -q 'before mirroring caught up' $LOGDIR/fresh_r.log && echo true || echo false)"
stopDaemons
stopGhost
C "db.getSiblingDB(process.env.TC_META).positions.deleteOne({_id:'${FID}ghost'})" >/dev/null


echo "== a copy-up waits for the delete it follows =="
export COLD_URI="$COLD"
# Every copy-up reads the archive, and none of them waited for mirroring the
# way a read does. A document the application has just deleted is gone from the
# fast tier at once and still live in the archive until its tombstone lands, so
# an update of it by _id missed on the fast tier, copied the archive's copy up
# and matched it: the delete was undone. Measured on 2026-09-25, forty of forty,
# where MongoDB matches nothing. The two-router store run found it first, as a
# transaction's warm putting back an order its own application had cancelled.
stopDaemons
startFeeder
startRouter
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:/^cu-/})" >/dev/null
SRB=$(staleReads)
OUT=$(R "const c=db.getSiblingDB(process.env.TC_DB).$NS;
  const cold=new Mongo(process.env.COLD_URI).getDB(process.env.TC_DB).$NS;
  let held=0, matched=0, back=0;
  for (let i=0;i<20;i++) {
    const id='cu-'+i;
    c.insertOne({_id:id, v:1});
    for (let k=0;k<500 && !cold.findOne({_id:id}); k++) sleep(20);
    if (cold.findOne({_id:id, _tcDeletedAt:{\$exists:false}})) held++;
    c.deleteOne({_id:id});
    if (c.updateOne({_id:id},{\$set:{v:2}}).matchedCount > 0) matched++;
    if (c.findOne({_id:id})) back++;
  }
  print(held+'/'+matched+'/'+back)")
# The control first: each document was live in the archive when it was deleted,
# which is the state a copy-up races. Without it the two zeros below prove
# nothing.
chk "the archive held each document live before its delete" "20" "${OUT%%/*}"
# Judged against what the router admitted. The barrier waits archive_wait and
# then goes ahead, counting a stale read, as a read does; under a four-worker
# sweep mirroring has run more than five seconds behind. A wrong answer the
# router counted is that contract, one it did not count is the defect.
ADMIT=$(( $(staleReads) - SRB ))
CU2=$(echo "$OUT" | cut -d/ -f2); CU3=$(echo "$OUT" | cut -d/ -f3)
chk "an update after a delete matches nothing"              "true" "$([ "${CU2:-99}" -le "$ADMIT" ] && echo true || echo "$CU2 matched, $ADMIT admitted")"
chk "and the deleted document stays deleted"                "true" "$([ "${CU3:-99}" -le "$ADMIT" ] && echo true || echo "$CU3 back, $ADMIT admitted")"
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:/^cu-/})" >/dev/null

echo "== a key freed by a delete can be written again at once =="
# The router asks the archive whether an _id or a unique value is taken, and a
# document deleted a moment ago is still live there until its tombstone lands.
# Deleting a document and inserting the same _id straight away, or re-using a
# unique value its deleted holder had, was refused as a duplicate twenty times
# of twenty on 2026-09-25, where MongoDB accepts both every time. The refusal
# now waits for mirroring and asks again. The partial index keeps the rest of
# this suite's documents out of it.
R "db.getSiblingDB(process.env.TC_DB).$NS.createIndex({email:1},{unique:true,name:'fresh_email_u',partialFilterExpression:{email:{\$exists:true}}})" >/dev/null
SRB=$(staleReads)
KOUT=$(R "const c=db.getSiblingDB(process.env.TC_DB).$NS;
  const cold=new Mongo(process.env.COLD_URI).getDB(process.env.TC_DB).$NS;
  let held=0, idRef=0, keyRef=0;
  for (let i=0;i<10;i++) {
    const id='rk-'+i, em='rk'+i+'@x';
    c.insertOne({_id:id, email:em});
    for (let k=0;k<500 && !cold.findOne({_id:id}); k++) sleep(20);
    if (cold.findOne({_id:id, _tcDeletedAt:{\$exists:false}})) held++;
    c.deleteOne({_id:id});
    try { c.insertOne({_id:id}); } catch(e) { idRef++; }
    c.deleteOne({_id:id});
    c.insertOne({_id:id+'a', email:em+'2'});
    for (let k=0;k<500 && !cold.findOne({_id:id+'a'}); k++) sleep(20);
    c.deleteOne({_id:id+'a'});
    try { c.insertOne({_id:id+'b', email:em+'2'}); } catch(e) { keyRef++; }
  }
  print(held+'/'+idRef+'/'+keyRef)")
chk "the archive held each document live before its delete" "10" "${KOUT%%/*}"
ADMIT=$(( $(staleReads) - SRB ))
K2=$(echo "$KOUT" | cut -d/ -f2); K3=$(echo "$KOUT" | cut -d/ -f3)
chk "the same _id is accepted again after its delete"       "true" "$([ "${K2:-99}" -le "$ADMIT" ] && echo true || echo "$K2 refused, $ADMIT admitted")"
chk "and so is a unique value its deleted holder had"      "true" "$([ "${K3:-99}" -le "$ADMIT" ] && echo true || echo "$K3 refused, $ADMIT admitted")"
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:/^rk-/}); db.getSiblingDB(process.env.TC_DB).$NS.dropIndex('fresh_email_u')" >/dev/null

echo "== a write that chooses from the archive chooses what is there now =="
# Two more decisions the router made from an archive that had not yet been told
# about a delete. A single-document delete by a filter chose the archive's first
# match, which could be the one the application had just deleted, so it left
# the next match alone where MongoDB deletes it: 15 of 15 on 2026-09-25. And a
# case-insensitive unique value re-used after its holder was deleted was refused,
# 15 of 15, because the collated check is its own lookup. The archive gets the
# collated index an adopted one would have, or the router cannot ask it.
C "db.getSiblingDB(process.env.TC_DB).$NS.createIndex({ref:1},{name:'fresh_ref_ci',collation:{locale:'en',strength:2}})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).$NS.createIndex({ref:1},{unique:true,name:'fresh_ref_ci',collation:{locale:'en',strength:2},partialFilterExpression:{ref:{\$exists:true}}})" >/dev/null
sleep 6
SRB=$(staleReads)
PKOUT=$(R "const c=db.getSiblingDB(process.env.TC_DB).$NS;
  const cold=new Mongo(process.env.COLD_URI).getDB(process.env.TC_DB).$NS;
  let wrong=0, refused=0;
  for (let i=0;i<8;i++) {
    const g='pk-'+i;
    cold.insertMany([{_id:g+'a', grp:g},{_id:g+'b', grp:g}]);
    c.deleteOne({_id:g+'a'});
    c.deleteOne({grp:g});
    if (c.findOne({_id:g+'a'}) || c.findOne({_id:g+'b'})) wrong++;
    const id='pr-'+i;
    c.insertOne({_id:id, ref:'Ref-'+i});
    for (let k=0;k<500 && !cold.findOne({_id:id}); k++) sleep(20);
    c.deleteOne({_id:id});
    try { c.insertOne({_id:id+'x', ref:'ref-'+i}); } catch(e) { refused++; }
  }
  print(wrong+'/'+refused)")
ADMIT=$(( $(staleReads) - SRB ))
P1=${PKOUT%%/*}; P2=${PKOUT##*/}
chk "a delete by filter takes the match that is still there" "true" "$([ "${P1:-99}" -le "$ADMIT" ] && echo true || echo "$P1 wrong, $ADMIT admitted")"
chk "a case-insensitive value is free once its holder is gone" "true" "$([ "${P2:-99}" -le "$ADMIT" ] && echo true || echo "$P2 refused, $ADMIT admitted")"
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:/^p[kr]-/}); db.getSiblingDB(process.env.TC_DB).$NS.dropIndex('fresh_ref_ci')" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:/^p[kr]-/}); db.getSiblingDB(process.env.TC_DB).$NS.dropIndex('fresh_ref_ci')" >/dev/null

echo "== a transaction bigger than one flush is claimed only when it is whole =="
# Every operation of a transaction carries the commit's cluster time, and the
# feeder used to claim an event's own time the moment it had written it. A flush
# falling inside a transaction, at the 500-event batch limit here, then claimed
# the commit with part of the transaction still unwritten, and a read waiting on
# that commit cleared the barrier and answered from half of it. Measured on
# 2026-09-25: 499 of 600 events, five rounds of five. The feeder's own position
# is read here, because it is exactly the claim the router's barrier trusts.
TXOUT=$(H "const s=db.getMongo().startSession();
  const c=s.getDatabase(process.env.TC_DB).$NS;
  const cold=new Mongo(process.env.COLD_URI);
  const pos=cold.getDB(process.env.TC_META).positions;
  let whole=0;
  for (let r=0;r<3;r++) {
    s.startTransaction();
    for (let i=0;i<600;i++) c.insertOne({_id:'tx-'+r+'-'+i});
    s.commitTransaction();
    const t=s.getOperationTime();
    for (let k=0;k<1000;k++) { const p=pos.findOne({_id:'test'}); if (p && p.ts && (p.ts.t>t.t || (p.ts.t==t.t && p.ts.i>=t.i))) break; sleep(5); }
    if (cold.getDB(process.env.TC_DB).$NS.countDocuments({_id:{\$regex:'^tx-'+r+'-'}})==600) whole++;
  }
  print(whole)")
chk "each time the position reached the commit, all 600 were in the archive" "3" "$TXOUT"
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:/^tx-/})" >/dev/null

stopDaemons
echo
echo "== the default refuses what permissive answers stale =="
# The same stopped feeder as "a stopped feeder must not become an outage", under
# the default. Found on the Hetzner staging pair on 2026-10-12: with the feeder
# stopped for two minutes, answering anyway gave a shop 2,759 answers that
# contradicted its own acknowledged writes. Zelmar's decision the same day:
# refuse by default, stale as the opt-in.
stopDaemons
rm -f $LOGDIR/fresh_r.log
startFeeder
startRouter "$STRICTCFG"
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:'strict1',n:1})" >/dev/null
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"}); db.getSiblingDB(process.env.TC_META).positions.deleteOne({_id:"test"})' >/dev/null
START=$(date +%s)
GOT=$(R "try { print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({})) } catch (e) { print(e.codeName + ': ' + e.message) }")
ELAPSED=$(( $(date +%s) - START ))
chk "a read that needs the archive is refused"   "true" \
    "$(grep -q '^CommandFailed: thermocline: refusing this because mirroring' <<<"$GOT" && echo true || echo false)"
chk "and the message names the opt-in"           "true" \
    "$(grep -q 'router.freshness to permissive' <<<"$GOT" && echo true || echo false)"
chk "and it did not hang waiting"                "true" "$([ "$ELAPSED" -lt 8 ] && echo true || echo false)"
chk "the router logged the refusal"              "true" \
    "$(grep -q 'refused a read the archive could not answer fresh' $LOGDIR/fresh_r.log && echo true || echo false)"
# With the reason, as a stale answer has. The explanation stayed on the
# permissive path when strict became the default, so a refusal said "waited 5s"
# and nothing about which bar: testaggwrite's delete was refused under load on
# 2026-10-13 and its log could not say whether the feeder was behind.
chk "and said why the wait gave up"              "true" \
    "$(grep -q 'and mirroring has published no position at all' $LOGDIR/fresh_r.log && echo true || echo false)"
# A write that has to bring a document up from the archive is refused the same
# way, rather than told it matched nothing: a copy-up with mirroring behind can
# bring back a document deleted a moment ago.
C "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:'archonly',n:7})" >/dev/null
UPD=$(R "try { const r=db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:'archonly'},{\$set:{n:8}}); print('matched='+r.matchedCount) } catch (e) { print(e.codeName) }")
chk "a write needing a copy-up is refused too"   "CommandFailed" "$UPD"
chk "and nothing was copied up"                  "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:'archonly'}))")"
stopDaemons

echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
