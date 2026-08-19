#!/bin/bash
# The background daemons reporting what they have done, and the dashboard's
# inventory refresh.
#
# The evictor and the warmer are separate processes with no listening socket, so
# their counters only ever reached a log line, which nothing can page on. They
# publish into the archive after each pass and the router exports them at
# /metrics, which is the same choice already made for mirroring lag and for the
# same reason: a gauge a daemon serves from itself goes silent exactly when the
# daemon dies, and that is the moment somebody needs the number.
#
# So the value worth checking is not any counter, it is seconds_since_pass. A
# daemon that has stopped, or that is failing closed on mirroring lag, looks
# exactly like a healthy one from outside until somebody notices it has not run.
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
NS=tdmet
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
M() { curl -s http://127.0.0.1:$PDASH/metrics 2>/dev/null; }

# The hold has to be lifted whatever happens here. A pause left behind stops the
# evictor for every suite that runs afterwards, and testevict would then fail for
# a reason that has nothing to do with it.
cleanup() {
  cline resume --config $CFG >/dev/null 2>&1
  pkill -9 -f "cline router --config $WDIR/" 2>/dev/null
  pkill -9 -f "cline feeder --config $WDIR/" 2>/dev/null
}
trap cleanup EXIT

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}

echo "== setup =="
stopDaemons
C "db.getSiblingDB(process.env.TC_META).daemonstats.deleteMany({})" >/dev/null
chk "no daemon has reported yet"                       "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).daemonstats.countDocuments({}))")"

H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.insertMany(Array.from({length:5},(_,i)=>({_id:i,n:i})))" >/dev/null

echo "== a daemon reports after a pass =="
# A dry run, so this suite changes nothing about what the tiers hold. What is
# being tested is the reporting, not the sweeping.
cline evictor --config $CFG --dry-run --once >/dev/null 2>&1
chk "the evictor published a report"                   "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).daemonstats.countDocuments({_id:'evictor'}))")"
chk "and said it was a dry run"                        "true" \
    "$(C "const d=db.getSiblingDB(process.env.TC_META).daemonstats.findOne({_id:'evictor'}); print(!!d && d.dryRun===true)")"
chk "and stamped when it ran"                          "true" \
    "$(C "const d=db.getSiblingDB(process.env.TC_META).daemonstats.findOne({_id:'evictor'}); print(!!d && !!d.at)")"
chk "and carried its counters"                         "true" \
    "$(C "const d=db.getSiblingDB(process.env.TC_META).daemonstats.findOne({_id:'evictor'}); print(!!d && d.counters && d.counters.scanned !== undefined)")"

cline warmer --config $CFG --dry-run >/dev/null 2>&1
chk "the warmer published one too"                     "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).daemonstats.countDocuments({_id:'warmer'}))")"

echo "== a later pass republishes =="
# Number(), because the counters are int64 and mongosh prints those as Long("0"),
# which is not a number as far as the shell is concerned.
#
# What cannot be checked from here is accumulation across passes. Each --once is
# a new process and starts its own total at zero, so two invocations cannot show
# a total growing; that property lives in the daemon and is visible only to a
# long-running one. What is checked instead is that a pass republishes, which is
# what keeps seconds_since_pass meaningful.
FIRSTAT=$(C "const d=db.getSiblingDB(process.env.TC_META).daemonstats.findOne({_id:'evictor'}); print(d.at.getTime())")
sleep 2
cline evictor --config $CFG --dry-run --once >/dev/null 2>&1
LATERAT=$(C "const d=db.getSiblingDB(process.env.TC_META).daemonstats.findOne({_id:'evictor'}); print(d.at.getTime())")
chk "the counter is a number"                          "true" \
    "$(C "const d=db.getSiblingDB(process.env.TC_META).daemonstats.findOne({_id:'evictor'}); print(Number.isFinite(Number(d.counters.scanned)))")"
chk "and a later pass moves the timestamp on"          "true" \
    "$(awk -v a="$FIRSTAT" -v b="$LATERAT" 'BEGIN{print (b+0 > a+0) ? "true" : "false"}')"

echo "== the router exports them =="
cline router --config $CFG --log-file $LOGDIR/dmet_r.log &
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
sleep 2
chk "seconds_since_pass is exported for the evictor"   "true" \
    "$(M | grep -q 'thermocline_daemon_seconds_since_pass{daemon="evictor"}' && echo true || echo false)"
chk "and for the warmer"                               "true" \
    "$(M | grep -q 'thermocline_daemon_seconds_since_pass{daemon="warmer"}' && echo true || echo false)"
chk "the counters are exported by outcome"             "true" \
    "$(M | grep -q 'thermocline_daemon_documents_total{daemon="evictor",outcome="scanned"}' && echo true || echo false)"
chk "the dry run is visible"                           "1" \
    "$(M | awk -F' ' '/^thermocline_daemon_dry_run\{daemon="evictor"\}/{print $2}')"
# The gauge has to move, or it is a timestamp pretending to be an age.
FIRST=$(M | awk -F' ' '/^thermocline_daemon_seconds_since_pass\{daemon="evictor"\}/{print $2}')
sleep 3
SECOND=$(M | awk -F' ' '/^thermocline_daemon_seconds_since_pass\{daemon="evictor"\}/{print $2}')
chk "and it climbs while the daemon is not running"    "true" \
    "$(awk -v a="$FIRST" -v b="$SECOND" 'BEGIN{print (b+0 > a+0) ? "true" : "false"}')"

echo "== an operator can hold eviction, and the hold is visible =="
# The case this exists for is a restored archive. The evictor's own check is per
# document and presence only, so an archive holding an older version of a
# document confirms as present and the fast tier's newer copy is removed.
cline pause --config $CFG --reason "restoring the archive" >/dev/null 2>&1
chk "the hold is recorded"                             "restoring the archive" \
    "$(C "const d=db.getSiblingDB(process.env.TC_META).markers.findOne({_id:'evictionPaused'}); print(d?d.reason:'MISSING')")"
cline evictor --config $CFG --once >/dev/null 2>&1
chk "the evictor removed nothing while held"           "5" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
sleep 2
chk "and the hold is exported"                         "1" \
    "$(M | awk -F' ' '/^thermocline_eviction_paused /{print $2}')"
chk "with how long it has been held"                   "true" \
    "$(M | grep -q '^thermocline_eviction_paused_seconds ' && echo true || echo false)"
cline resume --config $CFG >/dev/null 2>&1
sleep 2
chk "resuming lifts it"                                "0" \
    "$(M | awk -F' ' '/^thermocline_eviction_paused /{print $2}')"

echo "== the dashboard inventory still reports every namespace =="
# Measured concurrently now, so the risk is a lost or duplicated entry rather
# than a slow one.
sleep 3
COUNT=$(curl -s http://127.0.0.1:$PDASH/api/snapshot 2>/dev/null | grep -o '"ns":' | wc -l)
MANAGED=$(grep -c '^    - "' $CFG)
chk "every managed namespace appears exactly once"     "$MANAGED" "$COUNT"
chk "and the one this suite made is among them"        "true" \
    "$(curl -s http://127.0.0.1:$PDASH/api/snapshot 2>/dev/null | grep -q "$DB.$NS" && echo true || echo false)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
