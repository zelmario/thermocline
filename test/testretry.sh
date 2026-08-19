#!/bin/bash
# Two things the command surface audit reasoned about and never measured.
#
# $sample on a partial tier. A sample is only a sample of what the thing sampling
# can see, so a $sample answered by a fast tier holding half the collection is
# drawn from half the collection and reported as a sample of all of it. The
# reasoning said the completeness guard already covers it, because a pipeline
# with no lower time bound cannot be answered by a tier that does not claim all
# of history, and the archive holds every document so a fall-through sample is
# unbiased. Reasoning is not measuring.
#
# Retryable writes through the copy-up path. This one is load-bearing and was
# never written down. When a write matches nothing on the fast tier, the router
# copies the document up and re-sends the client's own command. Modern drivers
# send updateOne and deleteOne as retryable writes, and MongoDB deduplicates a
# retried statement by returning the result it recorded the first time. If it did
# that here, the re-sent command would come back with the first attempt's zero
# and the copy-up would achieve nothing.
#
# It works, and the reason is worth pinning: a write that matched nothing
# produces no oplog entry, so there is no recorded result to return and the
# retry genuinely executes. That is an invariant of someone else's system, which
# is exactly the kind that should have a test watching it.
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
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
NS=tretry
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
# Retryable writes on purpose, which is the driver default and the case that
# matters. Spelled out so nobody has to know that to read the test.
RW() { mongosh "mongodb://dba:secret@127.0.0.1:$PORT/?authSource=admin&retryWrites=true" --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() { pkill -9 -f "cline feeder --config $WDIR/" 2>/dev/null; pkill -9 -f "cline router --config $WDIR/" 2>/dev/null; }
trap cleanup EXIT

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/retry_f.log &
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/retry_r.log &
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}

# Ten on both tiers, ten only in the archive, which is what eviction leaves.
reseed() {
  H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
  H "db.getSiblingDB(process.env.TC_DB).$NS.insertMany(Array.from({length:10},(_,i)=>({_id:i+1,n:i+1,v:0})))" >/dev/null
  # Upserts, not insertMany, and this is the second suite to need the lesson.
  # The feeder is running, so it mirrors the ten inserts above into the archive
  # on its own schedule. If that lands between the drop and this line, the
  # archive already holds _id 1 and an ordered insertMany abandons the whole
  # batch on the first duplicate, leaving ten documents where twenty were meant
  # to be. testconform lost a debugging round to exactly this.
  C "db.getSiblingDB(process.env.TC_DB).$NS.bulkWrite(
       Array.from({length:20},(_,i)=>({replaceOne:{filter:{_id:i+1},
         replacement:{_id:i+1,n:i+1,v:0}, upsert:true}})), {ordered:false})" >/dev/null
  cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null 2>&1
  sleep 1
}

echo "== setup =="
stopDaemons
C "db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'});
   db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({});
   db.getSiblingDB(process.env.TC_META).residency.deleteMany({});
   db.getSiblingDB(process.env.TC_META).markers.deleteMany({})" >/dev/null
startFeeder
startRouter
reseed
chk "the fast tier holds ten"                          "10" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "the archive holds twenty"                         "20" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

echo "== \$sample must sample the whole collection =="
# Asking for everything is the sharpest form of the question: a sample of twenty
# from a collection of twenty has to be all twenty, and a fast tier holding ten
# cannot produce that.
chk "sampling all of it returns all of it"             "20" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$sample:{size:20}}]).toArray().length)")"
chk "and every document is distinct"                   "20" \
    "$(R "const a=db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$sample:{size:20}}]).toArray(); print(new Set(a.map(d=>d._id)).size)")"
# A smaller sample has to be able to reach past the fast tier at all. Ten draws
# of five from twenty documents that miss all ten archived ones every time would
# be a one in a million accident, so this is a real check rather than a hopeful
# one.
chk "repeated samples reach archived documents"        "true" \
    "$(R "let seen=false;
         for (let i=0;i<10 && !seen;i++) {
           const a=db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$sample:{size:5}}]).toArray();
           if (a.some(d=>d._id>10)) seen=true;
         }
         print(seen)")"

echo "== a retryable write through the copy-up path =="
# _id 15 lives only in the archive, so this write matches nothing on the fast
# tier, is copied up, and the client's own command runs again.
chk "a retryable updateOne matches the archived document" "1" \
    "$(RW "print(db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:15},{\$inc:{v:1}}).matchedCount)")"
# The number that matters. A retry that re-ran the statement twice would leave
# two, and a retry served from a recorded result would leave zero.
chk "and applied exactly once"                         "1" \
    "$(H "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:15}); print(d?d.v:'MISSING')")"

chk "a retryable deleteOne removes the archived document" "1" \
    "$(RW "print(db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:16}).deletedCount)")"
for _ in $(seq 1 40); do
  [ "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:16}); print(!!d && !d._tcDeletedAt)")" = "false" ] && break
  sleep 0.5
done
chk "and the archive stops returning it"               "false" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:16}); print(!!d && !d._tcDeletedAt)")"

chk "a retryable findOneAndUpdate returns the document" "17" \
    "$(RW "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOneAndUpdate({_id:17},{\$inc:{v:1}},{returnDocument:'after'}); print(d?d.n:'MISSING')")"
chk "and applied exactly once too"                     "1" \
    "$(H "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:17}); print(d?d.v:'MISSING')")"

echo "== and a write that genuinely matches nothing still says so =="
chk "no document, no match"                            "0" \
    "$(RW "print(db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:9999},{\$inc:{v:1}}).matchedCount)")"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
