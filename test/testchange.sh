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
#
# Everything the sections above left here has an integer _id, and this
# namespace's evictor rule takes each document's age from the timestamp inside
# its _id. One non-ObjectId document is enough for the evictor to refuse the
# whole namespace, so without this line the fixture below is inserted, mirrored
# and never evicted, and both checks under it pass on an eviction that never
# happened. That is what they had been doing: "the fast tier now holds 2 of 2,
# the watcher saw: none".
#
# Measured on 2026-09-14 with the evictor run by hand. With {_id: 1} present it
# logged "this rule cannot address" and left 2 of 2 on the fast tier; with it
# removed it evicted the old document and left 0 of 1, the archive keeping its
# copy.
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:{\$not:{\$type:'objectId'}}})" >/dev/null
OID=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 771)
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:ObjectId('$OID'), v:'old'})" >/dev/null
waitMirrored 1

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
echo "  the fast tier now holds $LEFT of 1, the watcher saw: ${EVENTS:-nothing}"
# First, that there was an eviction at all. An eviction that did not happen
# cannot be reported as a delete, so the two checks below say nothing until this
# one passes, and for a long time it was not there to fail: the evictor was
# refusing this namespace and the section was reporting that as a success.
chk "the eviction actually took the document"          "0" "$LEFT"
chk "and the archive kept the document it took"        "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
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

echo "== and the application's own insert of a document it just deleted =="
# The twin of the section above, and the direction it gets wrong. A copy-up must
# not look like an insert; an insert must not look like a copy-up.
#
# The router decides that an insert on the fast tier is one of its own by looking
# for a residency record for that _id and comparing the times. The record says
# the document was copied up and should stay hot, and nothing ended it when the
# application deleted the document, so it went on standing over a name the
# application was free to use again.
#
# Measured: a document copied up from the archive, deleted through the router,
# and inserted again two seconds later under the same _id. The stream delivered
# the delete and then nothing at all. The insert landed, the document was there,
# and the application's own write was missing from its own change stream.
#
# The record's own comment already said what should happen, that only the one
# insert the promotion causes is suppressed and a later genuine one is not.
# Its own document, and its own copy-up moments before the delete. The section
# above copied one up too, but by the time its checks had finished that
# promotion was well over thirty seconds old and outside the window that decides
# this, so a check written on top of it passed against the binary that had the
# defect. The three writes here happen within a few seconds of each other, which
# is what an application doing it looks like.
OID2=6a9da091b2018df81a140b02
C "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:ObjectId('$OID2')});
   db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:ObjectId('$OID2'), v:'archived', at:new Date(Date.now()-7200000)})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:ObjectId('$OID2')})" >/dev/null
rm -f /tmp/cs_events2b.txt
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getSiblingDB(process.env.TC_DB).$NS.watch();
  let ops=[];
  for (let i=0;i<80 && ops.length<2;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType); else sleep(250); }
  cs.close();
  print(ops.length ? ops.join(',') : 'none');" > /tmp/cs_events2b.txt 2>&1 &
WATCHER2B=$!
sleep 3
# All three inside one mongosh, because a shell round trip between them is about
# a second and a half and the window this turns on is thirty. Split across three
# invocations the same section passed against the binary that had the defect.
# The fast tier, read from inside the one mongosh below rather than through a
# shell round trip. Exported here because HOT is defined further up than the
# other TC_ variables are exported.
export TC_HOT="$HOT"
HOTAFTER=$(R "
  const c = db.getSiblingDB(process.env.TC_DB).$NS;
  c.findOne({_id:ObjectId('$OID2')});
  const hot = Mongo(process.env.TC_HOT).getDB(process.env.TC_DB);
  const held = hot.$NS.countDocuments({_id:ObjectId('$OID2')});
  c.deleteOne({_id:ObjectId('$OID2')});
  sleep(2000);
  c.insertOne({_id:ObjectId('$OID2'), v:'the application put it back'});
  print(held);")
chk "the copy-up put it on the fast tier"              "1" "$HOTAFTER"
EVENTS2B=$(awaitWatcher /tmp/cs_events2b.txt $WATCHER2B)
echo "  the watcher saw: ${EVENTS2B:-nothing}"
chk "the delete is reported"                           "true" \
    "$(echo "${EVENTS2B:-none}" | grep -q delete && echo true || echo false)"
chk "and so is the insert that followed it"            "true" \
    "$(echo "${EVENTS2B:-none}" | grep -q insert && echo true || echo false)"
chk "the document is what the application put there"   "the application put it back" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID2')}); print(d?d.v:'MISSING')")"

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

echo "== the same two events, read by a consumer that is behind =="
# Both suppressions are decided when the events are handed over, not when they
# happened. An application that stores its resume token and comes back to it,
# or that simply batches, reads events long after the fact. Storing the token
# and resuming is the correct thing for an application to do, so this is the
# ordinary case rather than an exotic one.
#
# The watcher opens its stream, waits out the window without reading, and only
# then asks. The events are already sitting on the server; what changes is when
# the router is asked to hand them over.
OID4=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 774)
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:ObjectId('$OID4'), v:'late'})" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID4')}); print(d?d.v:'')")" = "late" ] && break
  sleep 0.5
done

rm -f /tmp/cs_events5.txt
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getSiblingDB(process.env.TC_DB).$NS.watch();
  sleep(40000);
  let ops=[];
  for (let i=0;i<40 && ops.length===0;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType+':'+JSON.stringify(e.documentKey?e.documentKey._id:null)); else sleep(250); }
  cs.close();
  print(ops.length ? ops.join(',') : 'none');" > /tmp/cs_events5.txt 2>&1 &
WATCHER5=$!
sleep 3
# The eviction is made by hand rather than by running the evictor, so that it
# happens whatever the evictor's eligibility rules say at this point in the
# suite: the run before this one left the feeder restarted, and an evictor that
# refuses because mirroring looks behind evicts nothing, which makes a check
# that looks for no delete pass by seeing no events at all.
#
# This is exactly what the evictor does: announce the removal, then make it.
# The notice id is the namespace and the document id encoded the way meta.DocID
# encodes it, which for an ObjectId is the type byte 07 and its twelve bytes.
C "db.getSiblingDB(process.env.TC_META).evicting.updateOne(
     {_id:'$DB.$NS|07$OID4'},
     {\$inc:{pending:1}, \$set:{at:new Date()}},
     {upsert:true})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:ObjectId('$OID4')})" >/dev/null
LATE=$(awaitWatcher /tmp/cs_events5.txt $WATCHER5)
echo "  the late watcher saw: ${LATE:-nothing}"
# Seeing no delete proves nothing if no eviction happened. Both of these
# sections say "none" either way, so the thing they are about has to be shown
# to have taken place.
chk "the eviction actually happened"                   "MISSING" \
    "$(H "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID4')}); print(d?d.v:'MISSING')")"
chk "the late watcher was actually watching"           "true" \
    "$(echo "${LATE:-none}" | grep -qi "error\|refus" && echo false || echo true)"
# Keyed to this document, not to the word delete. A stream on a collection this
# suite has been writing to all along can carry an event from something else,
# and a check that greps for the operation alone reports that as the defect.
chk "an eviction read late is not a delete"            "true" \
    "$(echo "${LATE:-none}" | grep -q "delete:\"$OID4\"" && echo false || echo true)"

# And the copy-up half of it, on the document that was just evicted.
rm -f /tmp/cs_events6.txt
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getSiblingDB(process.env.TC_DB).$NS.watch();
  sleep(40000);
  let ops=[];
  for (let i=0;i<40 && ops.length===0;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType+':'+JSON.stringify(e.documentKey?e.documentKey._id:null)); else sleep(250); }
  cs.close();
  print(ops.length ? ops.join(',') : 'none');" > /tmp/cs_events6.txt 2>&1 &
WATCHER6=$!
sleep 3
R "db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID4')})" >/dev/null
LATE2=$(awaitWatcher /tmp/cs_events6.txt $WATCHER6)
echo "  the late watcher saw: ${LATE2:-nothing}"
chk "the copy-up actually happened"                    "late" \
    "$(H "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID4')}); print(d?d.v:'MISSING')")"
chk "the second late watcher was actually watching"    "true" \
    "$(echo "${LATE2:-none}" | grep -qi "error\|refus" && echo false || echo true)"
chk "a copy-up read late is not an insert"             "true" \
    "$(echo "${LATE2:-none}" | grep -q "insert:\"$OID4\"" && echo false || echo true)"

echo "== a change stream on the whole database =="
# db.watch() sends aggregate: 1 rather than a collection name, so it carried no
# namespace to match against the configuration, was never registered for
# filtering, and nothing it delivered was ever sieved. An application following
# several collections with one stream saw every copy-up as an insert and every
# eviction as a delete. Measured: the collection stream hid a copy-up and the
# database stream reported it.
OID5=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 775)
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:ObjectId('$OID5'), v:'wide'})" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID5')}); print(d?d.v:'')")" = "wide" ] && break
  sleep 0.5
done

# The eviction, made by hand for the reason given above.
rm -f /tmp/cs_events7.txt
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getSiblingDB(process.env.TC_DB).watch();
  let ops=[];
  for (let i=0;i<40 && ops.length===0;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType+':'+(e.ns?e.ns.coll:'?')); else sleep(250); }
  cs.close();
  print(ops.length ? ops.join(',') : 'none');" > /tmp/cs_events7.txt 2>&1 &
WATCHER7=$!
sleep 3
C "db.getSiblingDB(process.env.TC_META).evicting.updateOne(
     {_id:'$DB.$NS|07$OID5'},
     {\$inc:{pending:1}, \$set:{at:new Date()}},
     {upsert:true})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:ObjectId('$OID5')})" >/dev/null
WIDE1=$(awaitWatcher /tmp/cs_events7.txt $WATCHER7)
echo "  the database watcher saw: ${WIDE1:-nothing}"
chk "the eviction actually happened"                   "MISSING" \
    "$(H "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID5')}); print(d?d.v:'MISSING')")"
chk "the database watcher was actually watching"       "true" \
    "$(echo "${WIDE1:-none}" | grep -qi "error\|refus" && echo false || echo true)"
chk "an eviction is not a delete on a database stream" "true" \
    "$(echo "${WIDE1:-none}" | grep -q delete && echo false || echo true)"
# The database watcher records the collection rather than the id, and this
# suite writes to only one collection, so the operation alone is specific
# enough there.

# The copy-up half, and with it the control that matters most: filtering a
# stream must not silence it. A write the application really made has to arrive.
rm -f /tmp/cs_events8.txt
# Every event over a fixed window, keyed by document, rather than the first one
# that arrives. Grepping for "insert" was satisfied by the copy-up's own insert,
# so the control passed while reporting the very event it was meant to rule out.
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getSiblingDB(process.env.TC_DB).watch();
  let ops=[];
  for (let i=0;i<60;i++) { const e=cs.tryNext(); if (e) ops.push(e.operationType+':'+JSON.stringify(e.documentKey?e.documentKey._id:null)); else sleep(250); }
  cs.close();
  print(ops.length ? ops.join(',') : 'none');" > /tmp/cs_events8.txt 2>&1 &
WATCHER8=$!
sleep 3
R "db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID5')})" >/dev/null
sleep 2
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:'wide-real', v:'real'})" >/dev/null
WIDE2=$(awaitWatcher /tmp/cs_events8.txt $WATCHER8)
echo "  the database watcher saw: ${WIDE2:-nothing}"
chk "the copy-up actually happened"                    "wide" \
    "$(H "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:ObjectId('$OID5')}); print(d?d.v:'MISSING')")"
# Exactly one event, and it is the application's. Filtering a stream must not
# silence it, and must not leave the copy-up in it either.
chk "the application's insert arrives, and only it"    'insert:"wide-real"' "$WIDE2"
chk "and the document really is there"                 "true" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:'wide-real'}); print(!!d)")"

# The whole deployment is a third shape again: client.watch() sends aggregate: 1
# against admin. And a tiered $out assembles its result in a scratch collection
# inside the application's own database, which a client watching the database
# would see appear, fill and vanish. The catalog listing already hides those.
rm -f /tmp/cs_events9.txt
# Bounded by the clock and by what it is waiting for, not by a count of
# iterations. A deployment-wide watch sees every namespace on the fast tier, and
# under a parallel sweep that is three other workers' traffic: the loop spent
# its sixty turns discarding their events without ever sleeping, printed "none",
# and the check below read a watcher that had given up in milliseconds as a
# router that had swallowed an insert. Measured once in a four-worker sweep, and
# it is the only way this check has ever failed.
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const cs=db.getMongo().watch();
  const want='$NS';
  const until=Date.now()+25000;
  let ops=[];
  while (Date.now()<until && !ops.some(o => o.endsWith(':'+want))) {
    const e=cs.tryNext();
    if (e && e.ns && e.ns.db===process.env.TC_DB) ops.push(e.operationType+':'+e.ns.coll);
    else if (!e) sleep(250);
  }
  cs.close();
  print(ops.length ? ops.join(',') : 'none');" > /tmp/cs_events9.txt 2>&1 &
WATCHER9=$!
sleep 3
R "db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:'\$v',n:{\$sum:1}}},{\$out:'$NS_rollup'}]).toArray()" >/dev/null 2>&1
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:'deployment-real', v:'real'})" >/dev/null
WIDE3=$(awaitWatcher /tmp/cs_events9.txt $WATCHER9)
echo "  the deployment watcher saw: ${WIDE3:-nothing}"
# Nothing at all is not an answer either. An empty stream and a refused one are
# different states and this check used to pass on both, which is how a watcher
# that gave up early was read as a router that had swallowed an event.
chk "the deployment watcher was actually watching"     "true" \
    "$(echo "${WIDE3:-none}" | grep -qiE "error|refus|^none$" && echo false || echo true)"
chk "no scratch collection is reported"                "true" \
    "$(echo "${WIDE3:-none}" | grep -q "tmp.thermocline" && echo false || echo true)"
chk "the application's own insert still arrives"       "true" \
    "$(echo "${WIDE3:-none}" | grep -q "insert:$NS" && echo true || echo false)"

echo "== the router's own bookkeeping is not the application's data =="
# The unique key registry is the one piece of this system's state that lives on
# the *fast* tier, because it has to be written where the data it constrains is
# written. That is exactly where a client's change stream watches.
#
# A deployment-wide stream carried a claim row for every unique key every write
# touched, in a database the same client cannot even list: listDatabases has
# hidden it all along and the stream did not. Scratch collections were hidden
# one level up, and the database they sit beside was missed.
#
# The section above could never have caught it, because its watcher keeps only
# events whose database is the application's -- it discarded these before
# looking at them. This one keeps everything and names what it saw.
KNS=tcs4
# The daemons stop first, and the index is created before they start again.
#
# A router caches a namespace's unique indexes for a minute, so one already
# running when the index is created does not claim keys for it and the check
# reads zero. The sections above leave a router up -- startRouter then says the
# address is in use and the old one keeps serving -- so starting one here is not
# enough on its own. Seen exactly that way once: this section failed on its
# first run and passed on its second.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$KNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$KNS.drop()" >/dev/null 2>&1
H "db.getSiblingDB(process.env.TC_DB).createCollection('$KNS')" >/dev/null 2>&1
H "db.getSiblingDB(process.env.TC_DB).$KNS.createIndex({sku:1},{unique:true})" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$KNS.createIndex({sku:1},{unique:true})" >/dev/null 2>&1
startFeeder; startRouter
sleep 2
KTS=$(( $(date +%s) - 1 ))
sleep 1
R "db.getSiblingDB(process.env.TC_DB).$KNS.insertMany([1,2,3].map(i=>({_id:i, sku:'K'+i})))" >/dev/null
sleep 4
# Scoped to this collection. The registry is one collection per deployment and
# every suite on this worker has been claiming keys into it, so counting all of
# it reads whatever ran before: 220 once, under a sweep.
# Scoped to this collection. The registry is one collection per deployment and
# every suite on this worker claims keys into it, so counting all of it reads
# whatever ran before: 220 once under a sweep, against the 3 this section made.
chk "the writes did claim keys in the registry"      "3" \
    "$(H "print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+'.$KNS'}))")"
# Every database, counted by database, so nothing is discarded before it is seen.
#
# Drained, rather than read out of the first batch. This is a cluster-wide
# stream, so under a sweep it carries three other workers' writes as well as
# this one's, and a batch holds 101 events: the three this section made were
# past the end of the first batch and both sides counted zero. The control is
# what caught it, which is the whole reason the control is here -- "the
# application saw no bookkeeping" and "there was no bookkeeping to see" print
# the same 0.
#
# Through the driver's own cursor, not a hand-rolled getMore. The first version
# of this drain sent {getMore, collection: "$cmd.aggregate"} with runCommand,
# which mongosh issues under a fresh implicit session, and mongod refuses a
# getMore whose session is not the one that opened the cursor: "Cursor session
# id is not the same as the operation context\'s session id". tryNext with
# maxAwaitTimeMS does the same draining and leaves sessions to the driver.
#
# The deadline bounds the side that is supposed to find nothing. The other side
# stops as soon as it has its three.
KDRAIN="const cs = db.getMongo().watch([], {startAtOperationTime: Timestamp({t: $KTS, i: 1}), maxAwaitTimeMS: 1000});
   let n = 0;
   const deadline = Date.now() + 15000;
   while (Date.now() < deadline && n < 3) {
     const e = cs.tryNext();
     if (e && e.ns && e.ns.db === process.env.TC_META) { n++; }
   }
   cs.close();
   print(n);"
KGOT=$(R "$KDRAIN")
KCTRL=$(mongosh "$HOT" --quiet --eval "$KDRAIN" 2>&1 | tail -1)
echo "  the client saw $KGOT bookkeeping events, the fast tier's own stream $KCTRL"
chk "no bookkeeping reaches the application"         "0" "$KGOT"
chk "and the fast tier's own stream did carry it"    "3" "$KCTRL"
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$KNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$KNS.drop()" >/dev/null 2>&1
H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+'.'+'$KNS'})" >/dev/null 2>&1

echo "== a promotion read after the application deleted the document =="
# The residency record is two things at once: a hold saying the document should
# stay on the fast tier, and the only evidence that the insert a copy-up causes
# was a promotion rather than somebody writing a document. Ending it when the
# application deletes the document is what stops the *next* insert of that _id
# being suppressed, and the section above checks that half.
#
# This is the other half. A consumer that has not caught up has yet to read the
# promotion's own insert, and the router decides whether to hide it when it hands
# it over. Deleting the record took the evidence away before that: measured on a
# document only the archive held, read once through the router so it was copied
# up and deleted two seconds later, the client was shown **insert then delete**
# for a document it never inserted.
#
# Found by the fuzzer first, as four extra inserts in eighty operations, and it
# needed a hundred and twenty operations a round to reach: the sweep's forty had
# not.
PNS=tcs2
H "db.getSiblingDB(process.env.TC_DB).$PNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$PNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$PNS.insertOne({_id:1,v:'archived'})" >/dev/null
startFeeder; startRouter
PROMO=$(R "
  const c = db.getSiblingDB(process.env.TC_DB).$PNS;
  const cs = c.watch([]);
  cs.tryNext();
  c.findOne({_id:1});
  sleep(2000);
  c.deleteOne({_id:1});
  sleep(8000);
  const got = [];
  for (let r = 0; r < 20; r++) {
    for (;;) { const e = cs.tryNext(); if (!e) break; got.push(e.operationType + ':' + e.documentKey._id); }
    sleep(250);
  }
  try { cs.close(); } catch (e) {}
  print(got.join(' '));")
echo "  the consumer that was behind saw: ${PROMO:-nothing}"
chk "only the delete, not the copy-up's insert"        "delete:1" "$PROMO"
H "db.getSiblingDB(process.env.TC_DB).$PNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$PNS.drop()" >/dev/null 2>&1

echo "== a consumer resuming from a stored position reads the first batch =="
# Every section above opens a stream and then writes, so the aggregate that
# opens it comes back empty and every event arrives in a getMore. That is one
# way to read a change stream and it was the only one filtered: the aggregate
# reply was read for its cursor id and its own batch was passed through whole.
#
# A consumer that stores its resume token and comes back to it is the other way,
# and it is the correct thing for a consumer to do. resumeAfter, startAfter and
# startAtOperationTime all name a position in the past, so the events are there
# in the first batch, and none of them had ever been through the sieve.
#
# Measured before the fix: three documents copied up by one multi-document
# write, read back from a startAtOperationTime just before it, and all three
# arrived as inserts the application never made -- identical, event for event,
# to what the fast tier's own unfiltered stream delivered.
#
# The control matters more than usual here. A first batch that is empty for any
# other reason would pass this check while proving nothing, so the same question
# is put to the fast tier directly, where nothing is hidden and the insert has
# to be there.
RNS=tcs3
H "db.getSiblingDB(process.env.TC_DB).$RNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$RNS.drop()" >/dev/null 2>&1
# Created here rather than by the copy-up, so no drop or create lands inside the
# window. A drop delivers an invalidate, which closes the cursor and would end
# the batch before the events being asked about.
H "db.getSiblingDB(process.env.TC_DB).createCollection('$RNS')" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$RNS.insertOne({_id:1,v:'archived'})" >/dev/null
startFeeder; startRouter
sleep 3
RTS=$(( $(date +%s) - 1 ))
# One read through the router, which copies the document up.
R "db.getSiblingDB(process.env.TC_DB).$RNS.findOne({_id:1})" >/dev/null
sleep 6
DRAINFIRST="const r = db.getSiblingDB(process.env.TC_DB).runCommand({
     aggregate: '$RNS',
     pipeline: [{\$changeStream: {startAtOperationTime: Timestamp({t: $RTS, i: 1})}}],
     cursor: {}});
   print(r.ok ? r.cursor.firstBatch.filter(e => e.operationType === 'insert').length : 'aggregate failed');"
RFIRST=$(R "$DRAINFIRST")
RCTRL=$(H "$DRAINFIRST")
echo "  the resumed stream's first batch carried $RFIRST inserts, the fast tier's own $RCTRL"
chk "the copy-up is hidden from a resumed stream"      "0" "$RFIRST"
chk "and the fast tier's own stream did carry it"      "1" "$RCTRL"
H "db.getSiblingDB(process.env.TC_DB).$RNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$RNS.drop()" >/dev/null 2>&1

echo "== the catalog repair behind a copy-up is not the application's DDL =="
# The fast tier is repaired, not merely filled. Before the first copy-up into a
# namespace the router creates the collection there with the archive's options,
# because a collation decides which documents match and cannot be added
# afterwards, and then builds the archive's indexes on it. Both happen on the
# application's own namespace, so neither the scratch prefix nor the bookkeeping
# database hides them.
#
# An ordinary change stream carries neither, which is why this went unseen: it
# takes showExpandedEvents, MongoDB 6.0 and up, and then one point read of one
# archived document delivered **create then createIndexes** for a collection the
# archive had held all along. Measured on both the collection and the database
# shapes of the stream.
#
# Asked as the same question of both tiers, which is what makes the zero below
# mean anything: the fast tier's own stream has to carry those events, because a
# fixture where the router never repaired anything would pass this while
# proving nothing.
DNS=tcs5
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null 2>&1
C "const d = db.getSiblingDB(process.env.TC_DB).$DNS;
   d.insertOne({_id:1,v:'archived'}); d.createIndex({v:1}); print('seeded')" >/dev/null
# The window starts before the daemons and strictly after that drop, and both
# halves of that were learned by measurement.
#
# Before the daemons, because the router repairs the catalogs of the namespaces
# it serves as it starts: a window opened after startRouter misses the very
# events this section is about, and the check below then passes for the wrong
# reason. Measured, the create landed two seconds after startRouter and the
# copy-up's insert seven seconds after that.
#
# Strictly after the drop, because the drop is in the fast tier's oplog too and
# a drop delivers an invalidate, which closes a resumed cursor. A window that
# reaches back into the same second read "drop, invalidate" and ended there,
# carrying none of what follows: one run in three, which is the worst kind of
# check. The same trap the resumed-stream section above avoids by creating its
# collection beforehand, which this one cannot do, because a fast tier that
# already holds the collection has nothing to repair.
sleep 2
DTS=$(date +%s)
startFeeder; startRouter
sleep 3
# And a point read, so the copy-up path is covered whichever of the two did it.
R "db.getSiblingDB(process.env.TC_DB).$DNS.findOne({_id:1})" >/dev/null
# The index build is detached from the read's own path, so wait for it rather
# than racing it. _id_ plus the archive's v_1.
for _ in $(seq 1 40); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).$DNS.getIndexes().length)" 2>/dev/null)" = "2" ] && break
  sleep 0.5
done
echo "  the fast tier now holds $(H "print(db.getSiblingDB(process.env.TC_DB).$DNS.getIndexes().length)") indexes and $(H "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({}))") document"
# Read out of the aggregate's own first batch rather than from a getMore, so the
# section also covers the path a consumer resuming from a stored token takes.
DDLDRAIN="const r = db.getSiblingDB(process.env.TC_DB).runCommand({
     aggregate: '$DNS',
     pipeline: [{\$changeStream: {startAtOperationTime: Timestamp({t: $DTS, i: 1}),
                                 showExpandedEvents: true}}],
     cursor: {}});
   print(r.ok ? r.cursor.firstBatch.filter(e =>
     e.operationType === 'create' || e.operationType === 'createIndexes').length : 'aggregate failed');"
DROUTED=$(R "$DDLDRAIN")
DCTRL=$(H "$DDLDRAIN")
echo "  the routed stream carried $DROUTED catalog events, the fast tier's own $DCTRL"
chk "the fast tier's own stream did carry the repair"  "2" "$DCTRL"
chk "and the routed stream carried none of it"         "0" "$DROUTED"
# The other half, and the half the two checks above cannot tell apart from a
# router that hides every create and createIndexes it sees. Hiding the
# application's own DDL from its own stream is worse than the defect this fixes.
DMTS=$(( $(date +%s) - 1 ))
R "db.getSiblingDB(process.env.TC_DB).$DNS.createIndex({mine:1})" >/dev/null 2>&1
sleep 2
DMINE=$(R "const r = db.getSiblingDB(process.env.TC_DB).runCommand({
     aggregate: '$DNS',
     pipeline: [{\$changeStream: {startAtOperationTime: Timestamp({t: $DMTS, i: 1}),
                                 showExpandedEvents: true}}],
     cursor: {}});
   print(r.ok ? r.cursor.firstBatch.filter(e => e.operationType === 'createIndexes').length : 'aggregate failed');")
echo "  after the application built its own index the routed stream carried $DMINE"
chk "the application's own index build still arrives"  "1" "$DMINE"
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null 2>&1

echo "== a pre-image for a document only the archive ever held =="
# changeStreamPreAndPostImages appears nowhere in this system: not in the code,
# not in a suite, not in the documentation. A client that turns it on and asks
# for fullDocumentBeforeChange: "required" gets an error instead of an event
# when the pre-image is missing, so it is a shape that either works or breaks an
# application outright, and nothing here had ever tried it.
#
# The case only this system has is a document in the archive that the fast tier
# does not hold. A write to it copies it up first and then applies, so the fast
# tier's oplog carries an insert the client never made followed by the write.
# The sieve hides the insert. The pre-image of the write is then the copy-up's
# own document, which is the right content by construction -- provided the
# option travelled with the create the copy-up made, which is the half nothing
# was checking.
PINS=tcs6
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$PINS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$PINS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).createCollection('$PINS', {changeStreamPreAndPostImages:{enabled:true}});
   db.getSiblingDB(process.env.TC_DB).$PINS.insertMany([{_id:1,n:'one'},{_id:2,n:'two'},{_id:3,n:'three'}])" >/dev/null
chk "the archive holds them and the fast tier does not" "3/0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$PINS.countDocuments({}))")/$(H "print(db.getSiblingDB(process.env.TC_DB).$PINS.countDocuments({}))")"
startFeeder; startRouter
sleep 2

rm -f /tmp/cs_preimg.txt
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const c = db.getSiblingDB(process.env.TC_DB).$PINS;
  let cs;
  // A refusal to open is an answer too, and a different one from an empty
  // stream: it would mean the option never reached the fast tier at all.
  try { cs = c.watch([], {fullDocumentBeforeChange:'required'}); }
  catch (e) { print('open refused: ' + e.codeName); quit(0); }
  const out = [];
  try {
    for (let i = 0; i < 80 && out.length < 2; i++) {
      const ev = cs.tryNext();
      if (!ev) { sleep(250); continue; }
      out.push(ev.operationType + ':' + JSON.stringify(ev.documentKey._id) + ':' +
               (ev.fullDocumentBeforeChange === undefined ? 'NO-PREIMAGE'
                                                          : JSON.stringify(ev.fullDocumentBeforeChange)));
    }
  } catch (e) { print('stream error: ' + e.codeName); quit(0); }
  cs.close();
  print(out.length ? out.join(' | ') : 'none');" > /tmp/cs_preimg.txt 2>&1 &
PWATCH=$!
sleep 4
# Both writes are to documents the fast tier has never held, so both go through
# the copy-up path.
R "db.getSiblingDB(process.env.TC_DB).$PINS.updateOne({_id:1},{\$set:{n:'ONE'}})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).$PINS.deleteOne({_id:2})" >/dev/null
PEVENTS=$(awaitWatcher /tmp/cs_preimg.txt $PWATCH)
echo "  the watcher saw: ${PEVENTS:-nothing}"
chk "the option travelled to the fast tier's copy"     "true" \
    "$(H "const o=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$PINS'})[0]; print(!!(o&&o.options&&o.options.changeStreamPreAndPostImages&&o.options.changeStreamPreAndPostImages.enabled))")"
chk "the stream opened and delivered"                  "true" \
    "$(echo "${PEVENTS:-none}" | grep -qiE "refused|error|none" && echo false || echo true)"
chk "the update carries the archived document as its pre-image" "true" \
    "$(echo "$PEVENTS" | grep -q 'update:1:{"_id":1,"n":"one"}' && echo true || echo false)"
chk "the delete carries it too"                        "true" \
    "$(echo "$PEVENTS" | grep -q 'delete:2:{"_id":2,"n":"two"}' && echo true || echo false)"
# And the copy-up's own insert is still hidden. A pre-image is no excuse for
# telling an application it inserted something it did not.
chk "no copy-up arrived as an insert"                  "true" \
    "$(echo "$PEVENTS" | grep -q insert && echo false || echo true)"
H "db.getSiblingDB(process.env.TC_DB).$PINS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$PINS.drop()" >/dev/null 2>&1

# $NS is dropped in setup and was never dropped at the end, alone among this
# suite's collections. What it leaves is six documents whose _id values are
# plain integers, and the evictor rule that covers this namespace takes each
# document's age from the timestamp inside its _id. So the next suite on this
# worker that runs an evictor sweep gets "not evicting a namespace this rule
# cannot address" and a non-zero exit, a run or a week later: testall's three
# evictor checks failed exactly this way on 2026-09-14, against a fixture left
# by a testchange that had passed. See e829c75 for the same shape.
#
# After stopDaemons, so the feeder cannot mirror the drop back, and checked
# rather than assumed: a suppressed cleanup that failed looks exactly like one
# that worked.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null 2>&1
LEFT=$(( $(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")        + $(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))") ))
chk "this suite left nothing of $NS on either tier"     "0" "$LEFT"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
