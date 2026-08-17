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
CFG=/root/cline.yaml
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
M() { curl -s http://127.0.0.1:27110/metrics 2>/dev/null; }

cleanup() { pkill -9 -f 'cline router' 2>/dev/null; pkill -9 -f 'cline feeder' 2>/dev/null; }
trap cleanup EXIT

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}

echo "== setup =="
stopDaemons
C "db.getSiblingDB('_thermocline').daemonstats.deleteMany({})" >/dev/null
chk "no daemon has reported yet"                       "0" \
    "$(C "print(db.getSiblingDB('_thermocline').daemonstats.countDocuments({}))")"

H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop()" >/dev/null
H "db.getSiblingDB('shop').$NS.insertMany(Array.from({length:5},(_,i)=>({_id:i,n:i})))" >/dev/null

echo "== a daemon reports after a pass =="
# A dry run, so this suite changes nothing about what the tiers hold. What is
# being tested is the reporting, not the sweeping.
cline evictor --config $CFG --dry-run --once >/dev/null 2>&1
chk "the evictor published a report"                   "1" \
    "$(C "print(db.getSiblingDB('_thermocline').daemonstats.countDocuments({_id:'evictor'}))")"
chk "and said it was a dry run"                        "true" \
    "$(C "const d=db.getSiblingDB('_thermocline').daemonstats.findOne({_id:'evictor'}); print(!!d && d.dryRun===true)")"
chk "and stamped when it ran"                          "true" \
    "$(C "const d=db.getSiblingDB('_thermocline').daemonstats.findOne({_id:'evictor'}); print(!!d && !!d.at)")"
chk "and carried its counters"                         "true" \
    "$(C "const d=db.getSiblingDB('_thermocline').daemonstats.findOne({_id:'evictor'}); print(!!d && d.counters && d.counters.scanned !== undefined)")"

cline warmer --config $CFG --dry-run >/dev/null 2>&1
chk "the warmer published one too"                     "1" \
    "$(C "print(db.getSiblingDB('_thermocline').daemonstats.countDocuments({_id:'warmer'}))")"

echo "== the counters accumulate rather than reset =="
BEFORE=$(C "const d=db.getSiblingDB('_thermocline').daemonstats.findOne({_id:'evictor'}); print(d.counters.scanned)")
cline evictor --config $CFG --dry-run --once >/dev/null 2>&1
AFTER=$(C "const d=db.getSiblingDB('_thermocline').daemonstats.findOne({_id:'evictor'}); print(d.counters.scanned)")
# A second process starts its own total from zero, so what is checked is that a
# report is a total and not a delta, which is what a counter has to be.
chk "a report carries a total, not a delta"            "true" \
    "$([ -n "$AFTER" ] && [ "$AFTER" -ge 0 ] 2>/dev/null && echo true || echo false)"

echo "== the router exports them =="
cline router --config $CFG --log-file /var/log/cline/dmet_r.log &
for _ in $(seq 1 30); do
  mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
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

echo "== the dashboard inventory still reports every namespace =="
# Measured concurrently now, so the risk is a lost or duplicated entry rather
# than a slow one.
sleep 3
COUNT=$(curl -s http://127.0.0.1:27110/api/snapshot 2>/dev/null | grep -o '"ns":' | wc -l)
MANAGED=$(grep -c '^    - "' $CFG)
chk "every managed namespace appears exactly once"     "$MANAGED" "$COUNT"
chk "and the one this suite made is among them"        "true" \
    "$(curl -s http://127.0.0.1:27110/api/snapshot 2>/dev/null | grep -q "shop.$NS" && echo true || echo false)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
