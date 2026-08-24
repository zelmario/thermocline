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
  t.insertMany(d);" >/dev/null

# The fast tier holds the warm window. Copied out of the archive rather than
# built again from a second Date.now(): two documents with the same _id and
# different dates are two documents, and every comparison below would then be
# asking each tier about its own data. That mistake read as a product bug.
HOTDOCS=$(C "
  const src = db.getSiblingDB(process.env.TC_DB).$NS.find({_id:{\$regex:'^new'}}).toArray();
  print(JSON.stringify(src.map(d => ({_id: d._id, at: d.createdAt.getTime(), k: d.k}))));")
H "const src = $HOTDOCS;
   db.getSiblingDB(process.env.TC_DB).$NS.insertMany(
     src.map(d => ({_id: d._id, createdAt: new Date(d.at), k: d.k})));" >/dev/null

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
