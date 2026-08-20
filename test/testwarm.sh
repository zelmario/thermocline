#!/bin/bash
# The warmer's completeness claim, and what it takes to withdraw one.
#
# A completeness claim is the router's licence to answer a range query from the
# fast tier alone. If the fast tier quietly loses documents inside a claimed
# range, every such query returns a short answer and reports success. Nothing
# else in the system notices, which is why the warmer has to.
#
# The hard part is telling a hole from a deletion. The archive is upsert only,
# so a document the application deleted is tombstoned there rather than removed:
# what is live in the archive is exactly what the fast tier should hold. Both
# directions are tested here, because a check that refilled deleted documents
# would be worse than no check at all.
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
NS=orders
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-48s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-48s got=%s want=%s\n' "$1" "$3" "$2"; fi
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
  cline feeder --config $CFG --log-file $LOGDIR/warm_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/warm_r.log & RP=$!
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
# Remove documents from the fast tier without the feeder seeing it. This is what
# a restore from an old backup, or a botched repair, looks like: the archive
# still holds them live, so they are missing rather than deleted.
loseFromFastTier() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
  H "$1" >/dev/null
  C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder
}

echo "== setup =="
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})" >/dev/null
startFeeder
startRouter

# Real ObjectId ids, because this rule reads a document's age from the timestamp
# inside its _id. Integer ids sort before every ObjectId in BSON, so a window
# filter of _id >= ObjectId(...) matches none of them and every count comes back
# zero, which looks exactly like a fast tier with nothing missing.
R "const c=db.getSiblingDB(process.env.TC_DB).$NS; const d=[];
   for(let i=0;i<20;i++) d.push({_id:new ObjectId(),n:i,tag:'warm'});
   c.insertMany(d);" >/dev/null
waitMirrored 20
chk "seeded and mirrored" "20" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== a full fast tier earns a claim =="
cline warmer --config $CFG >/dev/null 2>&1
chk "the watermark is claimed" "true" "$(cline watermark --config $CFG --ns $DB.$NS 2>&1 | grep -qE 'newer than|all of history' && echo true || echo false)"
chk "the fast tier still holds everything" "20" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

echo "== a warm that covered everything says so, and it is worth saying =="
# A watermark says "complete for everything newer than T". A query carrying no
# lower time bound reaches arbitrarily far back, so no such watermark can cover
# it: it falls through to the archive however full the fast tier is. Only a
# claim on all of history covers it, which watermark.Mark.CoversAll has always
# said.
#
# That is every adoption. The warm copies the whole collection up and the claim
# it leaves still refuses the query an application asks most, give me this
# customer's orders. Measured on a 50,000 order shop whose orders had all just
# been warmed: 41% of the run still answered from the archive at 284 ops/s and
# 62.74ms, against 100% from the fast tier at 1,384 ops/s and 2.65ms once the
# same tier was declared complete for all of history.
#
# The claim is checked here, and then the thing the claim is for: a filter with
# no time bound in it must not reach the archive.
chk "the claim covers all of history" "true" \
    "$(cline watermark --config $CFG --ns $DB.$NS 2>&1 | grep -qE 'all of history' && echo true || echo false)"
# Read through the router's own counter of archive reads. Checked for being
# readable first, and deliberately: an unreachable dashboard makes both samples
# empty, and a delta of nothing minus nothing is zero, which is the answer this
# check wants. A check that cannot run must not look like one that passed.
# The snapshot is pretty printed, so the colon is followed by a space. A pattern
# without it matches nothing, and nothing is what made this check pass before it
# was ever right.
arch() { curl -s http://127.0.0.1:$PDASH/api/snapshot | grep -oE '"from_archive":[[:space:]]*[0-9]+' | grep -oE '[0-9]+'; }
BEFORE=$(arch)
chk "the router's archive counter is readable" "true" "$([ -n "$BEFORE" ] && echo true || echo false)"
chk "an unbounded query still finds them all"  "20" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({tag:'warm'}))")"
AFTER=$(arch)
chk "and no part of it went to the archive"    "0" "$(( ${AFTER:-0} - ${BEFORE:-1} ))"

echo "== a hole inside the claimed range must withdraw the claim =="
# Eight documents vanish from the fast tier. The archive still has them live, so
# this is loss, not deletion, and the claim is now a lie.
loseFromFastTier "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({n:{\$lt:8}})"
chk "the fast tier is short"           "12" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "the archive still holds them all" "20" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
rm -f $LOGDIR/warm_verify.log
cline warmer --config $CFG --log-file $LOGDIR/warm_verify.log >/dev/null 2>&1
chk "the warmer said the claim was unsupportable" "true" \
    "$(grep -qE 'discarding a completeness claim' $LOGDIR/warm_verify.log && echo true || echo false)"
chk "and reported how many were missing"          "true" \
    "$(grep -qE 'missing=8' $LOGDIR/warm_verify.log && echo true || echo false)"
chk "the hole was refilled"                       "20" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "and the claim is back"                       "true" \
    "$(cline watermark --config $CFG --ns $DB.$NS 2>&1 | grep -qE 'newer than|all of history' && echo true || echo false)"

echo "== a deletion is not a hole =="
# The opposite case, and the dangerous one. Five documents are deleted through
# the router, so the feeder tombstones them in the archive. The fast tier now
# legitimately holds fewer than the archive stores, and a warmer that treated
# that as loss would bring deleted records back to life.
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({n:{\$gte:15}})" >/dev/null
waitMirrored 15
chk "the fast tier holds the survivors" "15" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "the archive tombstoned the rest"   "5"  "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
rm -f $LOGDIR/warm_del.log
cline warmer --config $CFG --log-file $LOGDIR/warm_del.log >/dev/null 2>&1
chk "the claim survives a deletion"     "false" \
    "$(grep -qE 'discarding a completeness claim' $LOGDIR/warm_del.log && echo true || echo false)"
chk "and nothing was resurrected"       "15" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "the deleted documents stay gone"   "0"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({n:{\$gte:15}}))")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
