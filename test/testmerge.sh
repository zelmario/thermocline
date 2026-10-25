#!/bin/bash
# $merge into a tiered collection, where the document it should match is in the
# archive.
#
# $merge is a write that decides what to do by looking at what is already there.
# whenMatched needs to find the existing document; whenNotMatched decides what
# happens when it does not. Both of those questions are answered by the fast
# tier, because that is where the stage runs, and the fast tier is exactly the
# thing that does not hold every document.
#
# So the shape of the danger is the one the upsert bug already had. A $merge
# whose target document lives only in the archive matches nothing, inserts a new
# document built from the pipeline's output instead, and the feeder then mirrors
# that over the archived record. The fields the archived version carried and the
# pipeline did not are gone from both tiers, and the whole thing is reported as
# success.
#
# Nothing here asserts a value by hand where it can avoid it: shop.tmref is an
# untiered reference holding the same documents, every merge runs against both,
# and the answers have to match. A failure is the router disagreeing with
# MongoDB.
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
NS=tmrg
REF=tmref
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s\n          ref=%.140s\n          got=%.140s\n' "$1" "$2" "$3"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() { pkill -9 -f "cline feeder --config $WDIR/" 2>/dev/null; pkill -9 -f "cline router --config $WDIR/" 2>/dev/null; }
trap cleanup EXIT

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/merge_f.log &
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/merge_r.log &
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}

# The document as the application would see it, with volatile fields removed.
doc() { R "const d=db.getSiblingDB(process.env.TC_DB).$1.findOne({_id:5}); if(!d){print('MISSING')}else{delete d._tcDeletedAt; print(JSON.stringify(d))}"; }
# And as the archive holds it, which is where the loss would be.
cold5() { C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:5}); if(!d){print('MISSING')}else{delete d._tcDeletedAt; print(JSON.stringify(d))}"; }

# reseed puts the reference whole on the fast tier, and the tiered one with _id 5
# living only in the archive, which is what an evicted document looks like.
reseed() {
  local doc5='{_id:5, name:"alice", tier:"gold", balance:100}'
  H "db.getSiblingDB(process.env.TC_DB).$REF.drop(); db.getSiblingDB(process.env.TC_DB).$REF.insertMany([$doc5,{_id:6,name:'bob',tier:'silver',balance:50}])" >/dev/null
  H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
  H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:6,name:'bob',tier:'silver',balance:50})" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$NS.bulkWrite([
       {replaceOne:{filter:{_id:5},replacement:$doc5,upsert:true}},
       {replaceOne:{filter:{_id:6},replacement:{_id:6,name:'bob',tier:'silver',balance:50},upsert:true}}],{ordered:false})" >/dev/null
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
chk "the tiered fast tier does not hold it"           "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:5}))")"
chk "but the archive does"                            "1" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:5}))")"
# By a field, never by _id. A point lookup by _id is the one read that promotes,
# so checking the fixture that way brings the document to the fast tier and every
# merge below then finds it there, testing nothing. That is how the first two
# sections of this suite passed while the archived case was untested.
chk "and the router can still see it"                 "alice" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({name:'alice'}); print(d?d.name:'MISSING')")"

mergeInto() { # collection -> run the same $merge against it
  R "db.getSiblingDB(process.env.TC_DB).aggregate([
       {\$documents:[{_id:5, balance:200}]},
       {\$merge:{into:'$1', whenMatched:'merge', whenNotMatched:'insert'}}
     ]).toArray(); print('done')" >/dev/null 2>&1
}

echo "== whenMatched merge, against a document only the archive holds =="
chk "the fast tier still does not hold it"            "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:5}))")"
mergeInto $REF
mergeInto $NS
sleep 6
chk "the merged document matches the reference"       "$(doc $REF)" "$(doc $NS)"
chk "and the archive kept the fields it had"          "$(doc $REF)" "$(cold5)"

echo "== whenMatched replace =="
reseed
R "db.getSiblingDB(process.env.TC_DB).aggregate([
     {\$documents:[{_id:5, balance:300}]},
     {\$merge:{into:'$REF', whenMatched:'replace', whenNotMatched:'insert'}}
   ]).toArray()" >/dev/null 2>&1
R "db.getSiblingDB(process.env.TC_DB).aggregate([
     {\$documents:[{_id:5, balance:300}]},
     {\$merge:{into:'$NS', whenMatched:'replace', whenNotMatched:'insert'}}
   ]).toArray()" >/dev/null 2>&1
sleep 6
chk "a replacing merge matches the reference"         "$(doc $REF)" "$(doc $NS)"

echo "== whenMatched fail, which must fail rather than insert =="
reseed
reffail=$(R "try{db.getSiblingDB(process.env.TC_DB).aggregate([{\$documents:[{_id:5,balance:400}]},
   {\$merge:{into:'$REF', whenMatched:'fail', whenNotMatched:'insert'}}]).toArray(); print('ACCEPTED')}catch(e){print('FAILED')}")
gotfail=$(R "try{db.getSiblingDB(process.env.TC_DB).aggregate([{\$documents:[{_id:5,balance:400}]},
   {\$merge:{into:'$NS', whenMatched:'fail', whenNotMatched:'insert'}}]).toArray(); print('ACCEPTED')}catch(e){print('FAILED')}")
chk "whenMatched fail behaves the same on both"       "$reffail" "$gotfail"

# A $merge names its target with `into`, and `into` can carry a database. The
# guard above resolves that, but every check so far has merged into a collection
# in the same database as the aggregation, so the resolution itself was never
# exercised: a guard that read only the collection name would have passed all of
# them and still destroyed archived data one database over.
XNS=tmrgx
XREF=tmrefx
xdoc() { R "const d=db.getSiblingDB(process.env.TC_DB2).$1.findOne({_id:5}); if(!d){print('MISSING')}else{delete d._tcDeletedAt; print(JSON.stringify(d))}"; }
xcold5() { C "const d=db.getSiblingDB(process.env.TC_DB2).$XNS.findOne({_id:5}); if(!d){print('MISSING')}else{delete d._tcDeletedAt; print(JSON.stringify(d))}"; }
reseedX() {
  local doc5='{_id:5, name:"alice", tier:"gold", balance:100}'
  H "db.getSiblingDB(process.env.TC_DB2).$XREF.drop(); db.getSiblingDB(process.env.TC_DB2).$XREF.insertMany([$doc5,{_id:6,name:'bob',tier:'silver',balance:50}])" >/dev/null
  H "db.getSiblingDB(process.env.TC_DB2).$XNS.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB2).$XNS.drop()" >/dev/null
  H "db.getSiblingDB(process.env.TC_DB2).$XNS.insertOne({_id:6,name:'bob',tier:'silver',balance:50})" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB2).$XNS.bulkWrite([
       {replaceOne:{filter:{_id:5},replacement:$doc5,upsert:true}},
       {replaceOne:{filter:{_id:6},replacement:{_id:6,name:'bob',tier:'silver',balance:50},upsert:true}}],{ordered:false})" >/dev/null
  cline watermark --config $CFG --ns $DB2.$XNS --complete-from 2030-01-01T00:00:00Z >/dev/null 2>&1
  sleep 1
}
mergeIntoX() { # collection in wh, merged into from an aggregation running in shop
  R "db.getSiblingDB(process.env.TC_DB).aggregate([
       {\$documents:[{_id:5, balance:200}]},
       {\$merge:{into:{db:'$DB2',coll:'$1'}, whenMatched:'merge', whenNotMatched:'insert'}}
     ]).toArray(); print('done')" >/dev/null 2>&1
}

# A $merge sent with {w:0}. The client is not waiting for an answer, and the
# aggregation dispatch used to read that as permission to skip the merge target
# guard: gated on !msg.MoreToCome(), the same habit the write path unlearned
# long ago. Without the guard the copy-up never runs, so the merge matches
# nothing and inserts over the archived document instead of merging into it.
#
# Measured before the fix, over an _id only the archive held: the archive ended
# {v: "merged in"} with tier gone.
#
# Both sides are sent the same way, so a difference is the router's and not the
# flag's. An unacknowledged write has no reply to wait on, which is why the
# sleep is longer here than the acknowledged sections need.
unackMerge() { # collection -> the same $merge, with nobody waiting for it
  R "db.getSiblingDB(process.env.TC_DB).runCommand({aggregate:1, pipeline:[
       {\$documents:[{_id:5, balance:250}]},
       {\$merge:{into:'$1', whenMatched:'merge', whenNotMatched:'insert'}}],
       cursor:{}, writeConcern:{w:0}}); print('sent')" >/dev/null 2>&1
}

echo "== an unacknowledged \$merge, over a document only the archive holds =="
reseed
chk "the fast tier does not hold it"                  "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:5}))")"
unackMerge $REF
unackMerge $NS
sleep 8
chk "the unacknowledged merge matches the reference"  "$(doc $REF)" "$(doc $NS)"
chk "and the archive kept the fields it had"          "$(doc $REF)" "$(cold5)"

echo "== a \$merge whose input only the archive holds =="
# The fast tier cannot answer the input, so guardAggregate reads it from the
# archive and feeds it to the $merge. The pre-pass that brings the targets up
# ran on the fast tier regardless, found nothing there, and brought nothing up,
# so the archived target was created anew and mirrored over.
reseed
H "db.getSiblingDB(process.env.TC_DB).tmrgin.drop(); db.getSiblingDB(process.env.TC_DB).tmrginref.drop();
   db.getSiblingDB(process.env.TC_DB).tmrginref.insertOne({_id:5, balance:700})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tmrgin.drop(); db.getSiblingDB(process.env.TC_DB).tmrgin.insertOne({_id:5, balance:700})" >/dev/null
cline watermark --config $CFG --ns $DB.tmrgin --complete-from 2030-01-01T00:00:00Z >/dev/null 2>&1
sleep 1
chk "the input is in the archive alone"               "0 1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tmrgin.countDocuments({}))") $(C "print(db.getSiblingDB(process.env.TC_DB).tmrgin.countDocuments({}))")"
for pair in "tmrginref $REF" "tmrgin $NS"; do
  set -- $pair
  R "db.getSiblingDB(process.env.TC_DB).$1.aggregate([{\$match:{balance:{\$gt:0}}},
       {\$merge:{into:'$2', whenMatched:'merge', whenNotMatched:'insert'}}]).toArray()" >/dev/null 2>&1
done
sleep 6
chk "the merged document matches the reference"       "$(doc $REF)" "$(doc $NS)"
chk "and the archive kept the fields it had"          "$(doc $REF)" "$(cold5)"

echo "== a \$merge on a nested field, over a document only the archive holds =="
# on: "x.y" names a nested field. The pre-pass looked the field up as a plain
# name, read every document as missing it, and brought nothing up.
# Empty first: two documents without x would collide on the unique index as
# two nulls, and mongod requires the index for on.
H "db.getSiblingDB(process.env.TC_DB).$REF.deleteMany({}); db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$REF.createIndex({'x.y':1},{unique:true}); db.getSiblingDB(process.env.TC_DB).$REF.insertOne({_id:7, x:{y:'k'}, keep:true})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).$NS.createIndex({'x.y':1},{unique:true})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:7, x:{y:'k'}, keep:true})" >/dev/null
for t in $REF $NS; do
  # Said when it fails: discarded, a merge that failed under load left only
  # "add is missing" to go on, which is a different question.
  MOUT=$(R "try { db.getSiblingDB(process.env.TC_DB).aggregate([{\$documents:[{x:{y:'k'}, add:1}]},
       {\$merge:{into:'$t', on:'x.y', whenMatched:'merge', whenNotMatched:'insert'}}]).toArray(); print('MERGED') }
     catch(e) { print('merge into $t failed: '+e.code+' '+e.message.substring(0,160)) }")
  [ "$MOUT" = "MERGED" ] || echo "  note: ${MOUT:-mongosh printed nothing}"
done
sleep 6
nested() { R "print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).$1.find({'x.y':'k'},{_tcDeletedAt:0}).toArray()))"; }
chk "the nested merge matches the reference"          "$(nested $REF)" "$(nested $NS)"
chk "and the archive kept the document it had"        "1 1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({'x.y':'k', _tcDeletedAt:{\$exists:false}})+' '+db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:7, keep:true, add:1}))")"
H "db.getSiblingDB(process.env.TC_DB).$REF.dropIndex('x.y_1')" >/dev/null 2>&1
R "db.getSiblingDB(process.env.TC_DB).$NS.dropIndex('x.y_1')" >/dev/null 2>&1

echo "== \$merge into a tiered collection in another database =="
reseedX
chk "the cross-database fast tier lacks it"           "0" "$(H "print(db.getSiblingDB(process.env.TC_DB2).$XNS.countDocuments({_id:5}))")"
chk "its archive holds it"                            "1" "$(C "print(db.getSiblingDB(process.env.TC_DB2).$XNS.countDocuments({_id:5}))")"
mergeIntoX $XREF
mergeIntoX $XNS
sleep 6
chk "the cross-database merge matches the reference"  "$(xdoc $XREF)" "$(xdoc $XNS)"
chk "and its archive kept the fields it had"          "$(xdoc $XREF)" "$(xcold5)"
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
