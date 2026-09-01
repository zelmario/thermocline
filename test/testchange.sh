#!/bin/bash
# Change streams opened by the application, through the router.
#
# A change stream is an aggregate, so it goes through the aggregation guard, and
# that guard was written for pipelines that read documents. A change stream
# reads events instead, and none of the completeness reasoning applies to it.
#
# Then there is the harder question underneath. The evictor removes documents
# from the fast tier, and a change stream watching the fast tier sees removals.
# An application watching for deletes, to invalidate a cache or emit an event or
# tombstone a row in a warehouse, would be told a document was deleted when it
# was only moved. That is not a slow answer or a refused one. It is the system
# reporting an event that never happened.
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
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
NS=tcs
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
stopFeeder() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/cs_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/cs_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
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
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({}); db.getSiblingDB(process.env.TC_META).evicting.deleteMany({})" >/dev/null
startFeeder
startRouter

echo "== a change stream can be opened at all =="
# Two hours old, so the evictor's forty minute rule will take them later.
OPENED=$(R "
  try { const cs=db.getSiblingDB(process.env.TC_DB).$NS.watch(); cs.close(); print('opened'); }
  catch(e) { print('ERR '+e.message.substring(0,70)); }")
chk "watch() is accepted on a tiered collection"       "opened" "$OPENED"

echo "== a resumed stream is named, because its failure is silence =="
# A resume token is a position in one replica set's oplog, and change streams are
# served from the fast tier, so a token minted on the cluster that was adopted
# names nothing here. MongoDB accepts it and delivers no events, with ok: 1, so a
# consumer resuming across the cutover loses everything the old cluster saw after
# its last token. Refusing it would mean parsing resume tokens, an undocumented
# encoding that changes between releases. So it is said out loud, once per stream.
RESUMED=$(R "
  const c=db.getSiblingDB(process.env.TC_DB).$NS;
  const cs=c.watch(); c.insertOne({_id:900,v:'token'});
  let tok=null;
  for (let i=0;i<40 && !tok;i++) { const e=cs.tryNext(); if (e) tok=e._id; else sleep(250); }
  cs.close();
  if (!tok) { print('no token'); } else {
    // tryNext before close, because watch() is lazy: mongosh sends no command
    // until the cursor is read, so closing it first opens nothing and the router
    // has nothing to say about it.
    const again=c.watch([], {resumeAfter: tok}); again.tryNext(); again.close(); print('resumed');
  }")
chk "a stream can be resumed from its own token"       "resumed" "$RESUMED"
chk "and the router said so"                           "true" \
    "$(grep -q 'a change stream was resumed from a token or timestamp' $LOGDIR/cs_r.log && echo true || echo false)"
chk "naming which option carried it"                   "true" \
    "$(grep -q 'from=resumeAfter' $LOGDIR/cs_r.log && echo true || echo false)"
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:900})" >/dev/null

echo "== it reports the application's own writes =="
SEEN=$(R "
  const cs=db.getSiblingDB(process.env.TC_DB).$NS.watch();
  db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:1, v:'a'});
  db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:1},{\$set:{v:'b'}});
  let ops=[];
  for (let i=0;i<40 && ops.length<2;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType); else sleep(250); }
  cs.close();
  print(ops.join(','));")
chk "an insert and an update are both reported"        "insert,update" "$SEEN"

echo "== an eviction must not look like a deletion =="
# This is the one that matters. The document is not going away: the archive
# keeps it and a later read still finds it. An application told that it was
# deleted would drop its cache entry, emit a deletion event, or tombstone a row
# in a warehouse, for a document that still exists.
OID=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 771)
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:ObjectId('$OID'), v:'old'})" >/dev/null
waitMirrored 2

# The watcher runs in its own client and writes what it saw to a file, because
# a change stream has to already be open when the eviction happens. Its pid is
# waited on by name: a bare wait here would also wait for the feeder and the
# router, which are background jobs of this shell and never exit.
# Wait for a watcher to say what it saw, not for a fixed number of seconds.
#
# Each watcher prints exactly once, at the end. Killing it on a deadline instead
# means that under load it is killed mid-flight and the file is empty, and an
# empty answer is not a wrong answer, it is no answer: the third watcher failed
# a parallel sweep that way while passing every run on its own.
awaitWatcher() { # awaitWatcher FILE PID
  for _ in $(seq 1 240); do
    [ -s "$1" ] && break
    kill -0 "$2" 2>/dev/null || break
    sleep 0.5
  done
  kill "$2" 2>/dev/null
  [ -s "$1" ] || echo "  the watcher never reported, so the check below is about the fixture" >&2
  tail -1 "$1" 2>/dev/null
}

rm -f /tmp/cs_events.txt
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getSiblingDB(process.env.TC_DB).$NS.watch();
  let ops=[];
  for (let i=0;i<40 && ops.length===0;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType+':'+(e.documentKey?JSON.stringify(e.documentKey._id):'')); else sleep(250); }
  cs.close();
  print(ops.length ? ops.join(',') : 'none');" > /tmp/cs_events.txt 2>&1 &
WATCHER=$!
sleep 3
cline evictor --config $CFG --once >/dev/null 2>&1
EVENTS=$(awaitWatcher /tmp/cs_events.txt $WATCHER)
LEFT=$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")
echo "  the fast tier now holds $LEFT of 2, the watcher saw: ${EVENTS:-nothing}"
# An error is not an answer. Without this, a build that refuses change streams
# outright passes both of these: it saw no delete because it saw nothing at all.
chk "the watcher was actually watching"                "true" \
    "$(echo "${EVENTS:-none}" | grep -qi "error\|refus" && echo false || echo true)"
chk "an eviction is not reported as a delete"          "true" \
    "$(echo "${EVENTS:-none}" | grep -q delete && echo false || echo true)"

echo "== a copy-up must not look like an insert =="
# The other half of the same problem. The document has existed for two hours and
# is only being moved back; an application acting on inserts would treat it as
# new.
rm -f /tmp/cs_events2.txt
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getSiblingDB(process.env.TC_DB).$NS.watch();
  let ops=[];
  for (let i=0;i<40 && ops.length===0;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType); else sleep(250); }
  cs.close();
  print(ops.length ? ops.join(',') : 'none');" > /tmp/cs_events2.txt 2>&1 &
WATCHER2=$!
sleep 3
# A point lookup of the evicted document copies it back up.
R "db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID')})" >/dev/null
EVENTS2=$(awaitWatcher /tmp/cs_events2.txt $WATCHER2)
echo "  after the copy-up the watcher saw: ${EVENTS2:-nothing}"
chk "the second watcher was actually watching"         "true" \
    "$(echo "${EVENTS2:-none}" | grep -qi "error\|refus" && echo false || echo true)"
chk "a copy-up is not reported as an insert"           "true" \
    "$(echo "${EVENTS2:-none}" | grep -q insert && echo false || echo true)"
chk "the archive still holds the evicted document"     "true" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID')}); print(!!d && !d._tcDeletedAt)")"
chk "a read through the router still finds it"         "old" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID')}); print(d?d.v:'MISSING')")"

echo "== a lookup the fast tier cannot answer =="
# fullDocument: "updateLookup" fetches the document as it stands when the event
# is read, and that lookup happens on the fast tier, which is the one place that
# does not hold every document. An update to a document that has since left it
# came back with fullDocument: null, and null does not mean "somewhere else", it
# means gone. An application watching a collection to keep a cache or a search
# index in step would delete the entry while the document is still there.
#
# Measured before the fix: the stream said null while a read of that same
# document through the router returned it.
rm -f /tmp/cs_events3.txt
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getSiblingDB(process.env.TC_DB).$NS.watch([], {fullDocument:'updateLookup'});
  let out='no event';
  for (let i=0;i<60;i++) {
    const e=cs.tryNext();
    if (e && e.operationType==='update') { out = e.fullDocument ? 'HAS '+ (e.fullDocument.v||'?') : 'NULL'; break; }
    sleep(250);
  }
  cs.close(); print(out);" > /tmp/cs_events3.txt 2>&1 &
WATCHER3=$!
sleep 3
# Update it, let that reach the archive, then take it off the fast tier the way
# an eviction does, with the feeder blind to it so the archive keeps it live.
R "db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:ObjectId('$OID')},{\$set:{v:'looked up'}})" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID')}); print(d?d.v:'')")" = "looked up" ] && break
  sleep 0.5
done
stopFeeder
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:ObjectId('$OID')})" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
LOOKUP=$(awaitWatcher /tmp/cs_events3.txt $WATCHER3)
echo "  the watcher saw: ${LOOKUP:-nothing}"
chk "a lookup is answered from the archive"            "HAS looked up" "$LOOKUP"
startFeeder

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
