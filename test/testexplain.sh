#!/bin/bash
# What explain says about a query the fast tier cannot answer.
#
# explain was not a command the router knew, so it went to the fast tier and came
# back describing the fast tier. That is misleading in a specific way: the plan
# is for a query that may not run there at all, and executionStats reports
# nReturned from a partial copy, so an explain of a query returning ten documents
# can say three.
#
# Refusing explain is not an option, because Compass runs it and so does every
# index review. So the plan is passed through intact and a thermocline field is
# added beside it. This suite checks both halves: the field appears when it
# should, stays away when it should not, and nothing that reads the plan breaks.
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
NS=texpl
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
  cline feeder --config $CFG --log-file $LOGDIR/expl_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/expl_r.log & RP=$!
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
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({})" >/dev/null
startFeeder
startRouter
R "db.getSiblingDB(process.env.TC_DB).$NS.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i,grp:'g'})))" >/dev/null
waitMirrored 10

echo "== while the fast tier is whole, explain is left alone =="
cline watermark --config $CFG --ns $DB.$NS --complete-all >/dev/null
sleep 7
chk "explain still returns a plan"                      "true" \
    "$(R "print(!!db.getSiblingDB(process.env.TC_DB).$NS.find({}).explain().queryPlanner)")"
chk "and says nothing extra"                            "false" \
    "$(R "print(!!db.getSiblingDB(process.env.TC_DB).$NS.find({}).explain().thermocline)")"

echo "== take the documents out from under it =="
stopFeeder
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:{\$lt:7}})" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
chk "the fast tier now holds three"                     "3" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the query itself still answers in full"            "10" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

echo "== now explain has to say the plan is not the whole story =="
# Without this the plan looks healthy and nothing in the reply hints that the
# query it describes would be answered somewhere else entirely.
chk "explain still returns a plan"                      "true" \
    "$(R "print(!!db.getSiblingDB(process.env.TC_DB).$NS.find({}).explain().queryPlanner)")"
chk "explain says the fast tier is not complete"        "false" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).explain().thermocline.fastTierComplete)")"
chk "and says why"                                      "true" \
    "$(R "const t=db.getSiblingDB(process.env.TC_DB).$NS.find({}).explain().thermocline; print(!!t.because && t.because.length>20)")"
chk "and warns the counts read low"                     "true" \
    "$(R "print(/partial copy/.test(db.getSiblingDB(process.env.TC_DB).$NS.find({}).explain().thermocline.note))")"

# executionStats is the verbosity that reports nReturned, which is the number a
# reader is most likely to trust and the one most likely to be wrong.
chk "executionStats is annotated too"                   "false" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).explain('executionStats').thermocline.fastTierComplete)")"

echo "== aggregations and the other read commands too =="
chk "an aggregation explain is annotated"               "false" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.explain().aggregate([{\$group:{_id:null,n:{\$sum:1}}}]).thermocline.fastTierComplete)")"
chk "an explained count is annotated"                   "false" \
    "$(R "const e=db.getSiblingDB(process.env.TC_DB).runCommand({explain:{count:'$NS'}}); print(e.thermocline.fastTierComplete)")"
chk "an explained distinct is annotated"                "false" \
    "$(R "const e=db.getSiblingDB(process.env.TC_DB).runCommand({explain:{distinct:'$NS',key:'grp'}}); print(e.thermocline.fastTierComplete)")"

echo "== an explain must not do the thing it is explaining =="
# An aggregation can carry explain: true in the command itself instead of being
# wrapped in an explain command, and then it plans rather than runs. Every guard
# on the aggregate path does something on the pipeline's behalf: the merge target
# guard copies archived documents up, and the completeness guard runs a writing
# pipeline itself when the input is archived. Doing either for an explain is a
# side effect mongod does not have.
#
# Measured before the fix: aggregate with $out and explain: true wrote all ten
# documents into the target and returned no plan at all. mongod wrote nothing.
H "db.getSiblingDB(process.env.TC_DB).texplout.drop()" >/dev/null
EXPOUT=$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({aggregate:'$NS',
              pipeline:[{\$match:{}},{\$out:'texplout'}], explain:true});
            print((r.ok===1 && (!!r.stages || !!r.queryPlanner)) ? 'planned' : 'no plan')")
chk "an explained \$out comes back as a plan"           "planned" "$EXPOUT"
chk "and it wrote nothing"                              "0"     "$(H "print(db.getSiblingDB(process.env.TC_DB).texplout.countDocuments({}))")"
chk "the plan is annotated like any other"              "false"     "$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({aggregate:'$NS',
            pipeline:[{\$match:{}},{\$group:{_id:null,n:{\$sum:1}}}], explain:true});
          print(r.thermocline.fastTierComplete)")"
# The control. Without cursor and explain the same pipeline still runs, or the
# fix would have turned every writing aggregation into a plan.
chk "the same pipeline still runs when asked to"        "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"     "$(R "db.getSiblingDB(process.env.TC_DB).runCommand({aggregate:'$NS',pipeline:[{\$match:{}},{\$out:'texplout'}],cursor:{}});
          print(db.getSiblingDB(process.env.TC_DB).texplout.countDocuments({}))")"

echo "== and a collection nobody tiers is never touched =="
H "db.getSiblingDB(process.env.TC_DB).texplfree.drop(); db.getSiblingDB(process.env.TC_DB).texplfree.insertOne({_id:1})" >/dev/null
chk "an untiered collection's explain is untouched"     "false" \
    "$(R "print(!!db.getSiblingDB(process.env.TC_DB).texplfree.find({}).explain().thermocline)")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
