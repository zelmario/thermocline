#!/bin/bash
# Where a read asking for a secondary actually lands, and whether the router
# says so.
#
# The router answers hello as a mongos, and a mongos routes by read preference,
# so a driver attaches $readPreference to reads and expects it to be honoured.
# It is not: authentication is proxied, so the one upstream connection carrying
# the client's identity is the connection to the primary, and a second
# connection to a secondary could not be authenticated as that client.
#
# Nothing here returns a wrong answer, because the primary is strictly fresher
# than the secondary that was asked for. What this suite pins down is that the
# reads still work, and that the router reports the fact rather than hiding it.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
LOG=/var/log/cline/readpref_r.log
NS=trdpref
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
# Through the router, with a read preference.
P() { mongosh "mongodb://dba:secret@127.0.0.1:27100/?authSource=admin&readPreference=$1" --quiet --eval "$2" 2>&1 | tail -1; }
# Direct to one node, to ask which one ran the query.
N() { mongosh "mongodb://dba:secret@$1:27017/?authSource=admin&directConnection=true" --quiet --eval "$2" 2>&1 | tail -1; }

NODES=$(mongosh "$HOT" --quiet --eval 'print(db.hello().hosts.map(h=>h.split(":")[0]).join(" "))' 2>/dev/null | tail -1)

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startRouter() {
  cline router --config $CFG --log-file $LOG & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}

echo "== setup =="
stopDaemons
rm -f $LOG
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
H "db.getSiblingDB('shop').$NS.insertMany(Array.from({length:20},(_,i)=>({_id:i,n:i})),{writeConcern:{w:3}})" >/dev/null
chk "the fast tier holds the documents"                 "20" \
    "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

# Profiling is per node and not replicated, so it records on whichever node ran
# the query. Enabling it needs its own command per node; do not try to drop
# system.profile first, which throws on a secondary and ends the script.
for n in $NODES; do N "$n" "db.getSiblingDB('shop').setProfilingLevel(2)" >/dev/null; done
ON=0
for n in $NODES; do
  [ "$(N "$n" "print(db.getSiblingDB('shop').getProfilingStatus().was)")" = "2" ] && ON=$((ON+1))
done
chk "the profiler is recording on every node"           "3" "$ON"

landed() { # comment -> "PRIMARY" or "secondary", whichever node recorded it
  local where="none"
  for n in $NODES; do
    if [ "$(N "$n" "print(db.getSiblingDB('shop').system.profile.countDocuments({ns:'shop.$NS','command.comment':'$1'}))")" != "0" ]; then
      where=$(N "$n" 'print(db.hello().isWritablePrimary ? "PRIMARY" : "secondary")')
    fi
  done
  echo "$where"
}

echo "== control: straight to the replica set, so the profiler is trusted =="
mongosh "$HOT&readPreference=secondary" --quiet \
  --eval "db.getSiblingDB('shop').$NS.find({}).comment('ctl-sec').itcount()" >/dev/null 2>&1
mongosh "$HOT&readPreference=primary" --quiet \
  --eval "db.getSiblingDB('shop').$NS.find({}).comment('ctl-pri').itcount()" >/dev/null 2>&1
sleep 3
chk "a secondary read lands on a secondary"             "secondary" "$(landed ctl-sec)"
chk "a primary read lands on the primary"               "PRIMARY"   "$(landed ctl-pri)"

echo "== through the router =="
startRouter
for mode in secondary secondaryPreferred nearest; do
  P "$mode" "db.getSiblingDB('shop').$NS.find({}).comment('rt-$mode').itcount()" >/dev/null 2>&1
done
sleep 3
# Correctness first: whatever node answers, the answer has to be right.
for mode in secondary secondaryPreferred nearest; do
  chk "readPreference=$mode still returns everything" "20" \
      "$(P "$mode" "print(db.getSiblingDB('shop').$NS.find({}).itcount())")"
done
# And then the documented limitation, pinned so it cannot change unnoticed.
for mode in secondary secondaryPreferred nearest; do
  chk "readPreference=$mode is served by the primary" "PRIMARY" "$(landed "rt-$mode")"
done

echo "== and the router says so rather than hiding it =="
warnings() { grep -c "served by the primary" $LOG 2>/dev/null || true; }
chk "the log names the limitation"                      "true" \
    "$([ -f $LOG ] && grep -q "served by the primary" $LOG && echo true || echo false)"

# The warning is once per connection per mode. A reporting client sends the same
# preference on every query, so one line per query would bury the fact rather
# than report it. What matters is that the count follows connections and not
# queries, so this issues thirty reads from one client and expects a handful of
# lines at most: a driver keeps a pool, so the number is not exactly one.
BEFORE=$(warnings)
P secondary "for (let i=0;i<30;i++) db.getSiblingDB('shop').$NS.find({}).itcount()" >/dev/null 2>&1
sleep 1
ADDED=$(( $(warnings) - BEFORE ))
chk "thirty reads warn a handful of times, not thirty" "true" \
    "$([ "$ADDED" -ge 1 ] && [ "$ADDED" -le 5 ] && echo true || echo "false (got $ADDED)")"

chk "the counter is exported"                           "true" \
    "$(curl -s http://127.0.0.1:27110/metrics 2>/dev/null | grep -q "thermocline_primary_only_reads_total" && echo true || echo false)"
COUNTED=$(curl -s http://127.0.0.1:27110/metrics 2>/dev/null | awk '/^thermocline_primary_only_reads_total /{print $2}')
# The counter counts reads, not warnings, so it has to be well past the warnings.
chk "the counter counts every read, not every warning"  "true" \
    "$([ "${COUNTED:-0}" -ge 30 ] 2>/dev/null && echo true || echo "false (got ${COUNTED:-none})")"

# primaryPreferred is satisfied by the primary, so there is nothing to say.
BEFORE=$(warnings)
P primaryPreferred "db.getSiblingDB('shop').$NS.find({}).itcount()" >/dev/null 2>&1
sleep 1
chk "primaryPreferred is not warned about"              "0" \
    "$(( $(warnings) - BEFORE ))"

for n in $NODES; do N "$n" "db.getSiblingDB('shop').setProfilingLevel(0)" >/dev/null; done
stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
