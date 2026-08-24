#!/bin/bash
# Every BSON type a field can hold, in the field the collection is aged by.
#
# A completeness claim is a range predicate, and MongoDB brackets range
# predicates by type: a date bound never matches a Timestamp, a string or a
# null. So a document whose date field holds one of those is honestly outside
# the claim, outside every bounded query the claim answers, and the archive
# would not return it either. The two agree.
#
# A sort is not bracketed. It orders across types, and Timestamp, Regex and
# MaxKey all sort above Date, so exactly that document belongs at the top of a
# newest-first answer. Three defects on 2026-08-24 came from a value whose type
# was not the one the code assumed, and each was found by hand. This asks the
# whole range at once.
#
# The archive is the control throughout: it holds every document, so whatever it
# answers is what a single cluster would have answered. Every question is put to
# both, and the only difference between the two is that the archive's carries
# the tombstone exclusion, because the archive keeps records the application
# deleted and the fast tier does not.
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
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/q_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/q_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}
NS=tmenag

# The archive is the original cluster and holds all of it.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteOne({_id:'$DB.$NS'})" >/dev/null
C "
  const t = db.getSiblingDB(process.env.TC_DB).$NS;
  const now = Date.now();
  const d = [];
  // Dated and recent. These are the ones a warm window reaches.
  for (let i = 0; i < 5; i++) d.push({_id: 'new' + i, createdAt: new Date(now - i*60000), k: 'a'});
  // Dated and old. Archive only, and legitimately so.
  for (let i = 0; i < 5; i++) d.push({_id: 'old' + i, createdAt: new Date(now - (i+100)*3600000), k: 'b'});
  // Everything that is not a date: no window selects it, so no warm ever copies
  // it up and it stays in the archive for good.
  d.push({_id: 'ts',      createdAt: new Timestamp({t: Math.floor(now/1000), i: 1})});
  d.push({_id: 'str',     createdAt: 'yesterday'});
  d.push({_id: 'int',     createdAt: NumberInt(5)});
  d.push({_id: 'long',    createdAt: NumberLong('5')});
  d.push({_id: 'dbl',     createdAt: 5.5});
  d.push({_id: 'dec',     createdAt: NumberDecimal('5.5')});
  d.push({_id: 'bool',    createdAt: true});
  d.push({_id: 'null',    createdAt: null});
  d.push({_id: 'missing'});
  d.push({_id: 'oid',     createdAt: new ObjectId()});
  d.push({_id: 'minkey',  createdAt: MinKey()});
  d.push({_id: 'maxkey',  createdAt: MaxKey()});
  d.push({_id: 'arr',     createdAt: [new Date(now - 3600000), new Date(now - 7200000)]});
  d.push({_id: 'doc',     createdAt: {at: new Date(now)}});
  d.push({_id: 'bin',     createdAt: BinData(0, 'AAAA')});
  // Every document also carries a recent updatedAt. Nothing above reads it:
  // it is there so a pipeline that rewrites createdAt from it would be proved
  // against a date that is always inside the claim, which is the mistake the
  // check below exists to catch.
  d.forEach(x => { x.updatedAt = new Date(now); });
  t.insertMany(d);" >/dev/null

# The fast tier holds the warm window. Copied out of the archive rather than
# built again from a second Date.now(): two documents with the same _id and
# different dates are two documents, and every comparison below would then be
# asking each tier about its own data. That mistake read as a product bug.
HOTDOCS=$(C "
  const src = db.getSiblingDB(process.env.TC_DB).$NS.find({_id:{\$regex:'^new'}}).toArray();
  print(JSON.stringify(src.map(d => ({_id: d._id, at: d.createdAt.getTime(), k: d.k, up: d.updatedAt.getTime()}))));")
H "const src = $HOTDOCS;
   db.getSiblingDB(process.env.TC_DB).$NS.insertMany(
     src.map(d => ({_id: d._id, createdAt: new Date(d.at), k: d.k, updatedAt: new Date(d.up)})));" >/dev/null

startRouter
# Complete from the oldest document the fast tier holds, which is what warming
# that window declares.
FROM=$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.find().sort({createdAt:1}).limit(1).toArray()[0].createdAt.toISOString())")
cline watermark --config $CFG --ns $DB.$NS --complete-from "$FROM" >/dev/null 2>&1
echo "  (complete from $FROM, fast tier 5 of 25)"

# ask NAME EXPRESSION: put the same question to the archive and to the router.
# LIVE is the only difference: the archive keeps deleted records and has to
# exclude them, and asking one side a question the other was never asked is how
# a harness reports a disagreement that is its own.
ask() {
  local want got
  want=$(C "const LIVE={_tcDeletedAt:{\$exists:false}}; const t=db.getSiblingDB(process.env.TC_DB).$NS; $2")
  got=$(R  "const LIVE={};                               const t=db.getSiblingDB(process.env.TC_DB).$NS; $2")
  chk "$1" "$want" "$got"
}
IDS='function(c){return c.toArray().map(function(d){return String(d._id)}).join(",")}'

echo "== the newest N, which is proved from the fast tier's own answer =="
for L in 1 2 3 5 10; do
  ask "the newest $L by createdAt" "const F=$IDS; print(F(t.find(LIVE).sort({createdAt:-1}).limit($L)))"
done
# A skip drops documents off the front, so "everything not returned is older
# than all N" stops being true of the skipped ones. The claim carries it anyway.
ask "the newest 3 after skipping 2"  "const F=$IDS; print(F(t.find(LIVE).sort({createdAt:-1}).skip(2).limit(3)))"
# The filter does not enter the proof, and must not change the answer either.
ask "the newest 3 with a filter"     "const F=$IDS; print(F(t.find(Object.assign({k:'a'},LIVE)).sort({createdAt:-1}).limit(3)))"
ask "the newest 3 of the not-dates"  "const F=$IDS; print(F(t.find(Object.assign({createdAt:{\$not:{\$type:'date'}}},LIVE)).sort({createdAt:-1}).limit(3)))"

echo "== the same questions as a pipeline, which is how a BI tool asks =="
# A read-only newest-N pipeline is sent to the fast tier and judged on the way
# back, the way a find is, rather than answered from the archive. It has to give
# the archive's answer, and on this collection that means the documents whose
# createdAt outranks a date, which the fast tier does not hold.
AGG='function(c){return c.toArray().map(function(d){return String(d._id)}).join(",")}'
for L in 1 3 5; do
  ask "the newest $L as a pipeline" "const F=$AGG; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:-1}},{\$limit:$L}])))"
done
ask "the newest 3 as a filtered pipeline" "const F=$AGG; print(F(t.aggregate([{\$match:Object.assign({k:'a'},LIVE)},{\$sort:{createdAt:-1}},{\$limit:3}])))"
# A stage after the $limit reshapes documents already chosen, so it cannot change
# which ones they are. This one keeps the field the proof reads.
ask "the newest 3 then projected"         "const F=$AGG; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:-1}},{\$limit:3},{\$project:{_id:1,createdAt:1}}])))"
ask "the newest 3 then unset"             "const F=$AGG; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:-1}},{\$limit:3},{\$unset:'k'}])))"
# And the two that would let the pipeline tell the proof what to believe. The
# first rewrites the age from a field that is always recent; the second moves a
# different document into the place the proof reads. Both must go to the archive.
ask "the newest 3 with the age rewritten" "const F=$AGG; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:-1}},{\$limit:3},{\$addFields:{createdAt:'\$updatedAt'}}])))"
ask "the newest 3 then re-sorted"         "const F=$AGG; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:-1}},{\$limit:3},{\$sort:{createdAt:1}}])))"
# Oldest first is not this shape and must go on being answered by the archive.
ask "the oldest 3 as a pipeline"          "const F=$AGG; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:1}},{\$limit:3}])))"
# Nor is a pipeline with no limit, which reads everything.
ask "sorted with no limit as a pipeline"  "const F=$AGG; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:-1}}])))"
# Nor one that computes rather than selects.
ask "a group as a pipeline"               "print(t.aggregate([{\$match:LIVE},{\$group:{_id:null,n:{\$sum:1}}}]).toArray()[0].n)"

echo "== where only the shape of the pipeline stands between right and wrong =="
# The two checks above cannot fail on tmenag, because that collection has ages
# outranking a date and the router declines to prove anything about it however
# the pipeline is shaped. A check that cannot fail is not a check.
#
# So: dates only, and a fast tier holding one document older than its own claim.
# The fast tier's own three newest are then not the collection's three newest,
# and the only thing that catches it is the proof reading the age of the last
# document the sort put there. Anything that rewrites that value or moves a
# different document into that place turns a correct fall-through into a wrong
# answer.
N2=tmenag2
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$N2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$N2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteOne({_id:'$DB.$N2'})" >/dev/null
C "
  const t = db.getSiblingDB(process.env.TC_DB).$N2;
  const now = Date.now();
  const d = [];
  for (let i = 0; i < 2; i++) d.push({_id: 'r' + i, createdAt: new Date(now - i*60000)});
  for (let i = 0; i < 8; i++) d.push({_id: 'm' + i, createdAt: new Date(now - (i+10)*3600000)});
  d.forEach(x => { x.updatedAt = new Date(now); });
  t.insertMany(d);" >/dev/null
# The fast tier holds the two recent ones and the very oldest, which is what a
# warm plus a document nothing has evicted yet leaves behind.
D2=$(C "
  const src = db.getSiblingDB(process.env.TC_DB).$N2.find({_id:{\$in:['r0','r1','m7']}}).toArray();
  print(JSON.stringify(src.map(d => ({_id: d._id, at: d.createdAt.getTime(), up: d.updatedAt.getTime()}))));")
H "const src = $D2;
   db.getSiblingDB(process.env.TC_DB).$N2.insertMany(
     src.map(d => ({_id: d._id, createdAt: new Date(d.at), updatedAt: new Date(d.up)})));" >/dev/null
startRouter
F2=$(H "print(db.getSiblingDB(process.env.TC_DB).$N2.findOne({_id:'r1'}).createdAt.toISOString())")
cline watermark --config $CFG --ns $DB.$N2 --complete-from "$F2" >/dev/null
ask2() {
  local want got
  want=$(C "const LIVE={_tcDeletedAt:{\$exists:false}}; const t=db.getSiblingDB(process.env.TC_DB).$N2; $2")
  got=$(R  "const LIVE={};                               const t=db.getSiblingDB(process.env.TC_DB).$N2; $2")
  chk "$1" "$want" "$got"
}
# The fast tier holds r0, r1 and m7; the collection's three newest are r0, r1
# and m0. So a proof that accepts the fast tier's answer here is visibly wrong.
ask2 "the fast tier's three are not the three" "const F=$IDS; print(F(t.find(LIVE).sort({createdAt:-1}).limit(3)))"
ask2 "the same as a pipeline"                  "const F=$IDS; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:-1}},{\$limit:3}])))"
ask2 "with the age rewritten after the limit"  "const F=$IDS; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:-1}},{\$limit:3},{\$addFields:{createdAt:'\$updatedAt'}}])))"
ask2 "re-sorted after the limit"               "const F=$IDS; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:-1}},{\$limit:3},{\$sort:{createdAt:1}}])))"
# And the claim still buys something here: asking for two is answered by the
# fast tier, because its second is inside the claim.
ask2 "the newest two, which it can prove"      "const F=$IDS; print(F(t.aggregate([{\$match:LIVE},{\$sort:{createdAt:-1}},{\$limit:2}])))"
# And what the fall-through leaves behind. The fast tier ran this query too, and
# with a batchSize below the limit it kept the rest back and holds a cursor for
# it. The client is given the archive's cursor instead, so nothing will ever ask
# for that rest and it sits there until mongod's own timeout.
#
# Counted for this collection alone, because the fast tier's total is a moving
# number that other workers move too.
idleHere() {
  H "print(db.getSiblingDB('admin').aggregate([{\$currentOp:{idleCursors:true,allUsers:true}}])
       .toArray().filter(o => o.type === 'idleCursor' && o.ns === process.env.TC_DB + '.$N2').length)"
}
for _ in 1 2 3 4 5; do
  R "db.getSiblingDB(process.env.TC_DB).$N2.find({}).sort({createdAt:-1}).limit(3).batchSize(1).toArray()" >/dev/null
done
sleep 1
chk "the fall-through left no cursor behind" "0" "$(idleHere)"

stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$N2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$N2.drop()" >/dev/null
startRouter

echo "== the directions and orders the proof does not cover =="
# Oldest first proves nothing: everything not returned is newer, not older.
for L in 1 3 10; do
  ask "the oldest $L by createdAt" "const F=$IDS; print(F(t.find(LIVE).sort({createdAt:1}).limit($L)))"
done
# Sorted by a field the collection is not aged by, which is a different order.
for L in 1 3 10; do
  ask "the newest $L by _id"       "const F=$IDS; print(F(t.find(LIVE).sort({_id:-1}).limit($L)))"
done
ask "every _id in _id order"       "const F=$IDS; print(F(t.find(LIVE).sort({_id:1})))"

echo "== counting and distinct, which read the same documents =="
ask "counting everything"          "print(t.countDocuments(LIVE))"
ask "counting inside the claim"    "print(t.countDocuments(Object.assign({createdAt:{\$gte:new Date('$FROM')}},LIVE)))"
ask "counting outside it"          "print(t.countDocuments(Object.assign({createdAt:{\$lt:new Date('$FROM')}},LIVE)))"
ask "counting the not-dates"       "print(t.countDocuments(Object.assign({createdAt:{\$not:{\$type:'date'}}},LIVE)))"
ask "distinct on a plain field"    "print(t.distinct('k',LIVE).join(','))"

stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
[ $FAIL -gt 0 ] && exit 1
exit 0
