#!/bin/bash
# Whether a client's own permissions still apply when the answer comes from the
# archive.
#
# The router proxies authentication rather than terminating it, so the fast tier
# decides what a client may read and enforces it. The archive is different: the
# router reaches it with its own credentials, from its own connection, because
# the client has no session there at all.
#
# That is fine as long as the fast tier is asked first. A read it refuses comes
# back as an error and nothing falls through. But count, distinct and every
# aggregation are judged before they run, because a pipeline can end in $out and
# acting after the fact would leave a truncated collection written. Judged
# before they run means the fast tier never sees them, which means nothing ever
# checks whether this client was allowed to ask.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
NS=tauth
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
# The restricted client, through the router.
L() { mongosh "mongodb://limited:limited@127.0.0.1:27100/?authSource=admin&directConnection=true" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
stopFeeder() {
  pkill -f 'cline feeder' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/auth_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/auth_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}
waitMirrored() {
  local n=""
  for _ in $(seq 1 60); do
    n=$(C "print(db.getSiblingDB('shop').$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$1" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the archive never reached $1 live documents, last saw ${n:-none}"
}

echo "== setup =="
stopDaemons
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({}); db.getSiblingDB('_thermocline').residency.deleteMany({})" >/dev/null

# A user who may read one database and not the one being tiered. Created on the
# fast tier, which is where the router sends the handshake.
H "db.getSiblingDB('admin').dropUser('limited')" >/dev/null 2>&1
H "db.getSiblingDB('admin').createUser({user:'limited', pwd:'limited', roles:[{role:'read', db:'elsewhere'}]})" >/dev/null
chk "the restricted user exists"                        "1" \
    "$(H "print(db.getSiblingDB('admin').system.users.countDocuments({user:'limited'}))")"

startFeeder
startRouter
R "db.getSiblingDB('shop').$NS.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i,grp:'g'})))" >/dev/null
waitMirrored 10

echo "== the fast tier still refuses what it should =="
# The fast tier holds everything here, so nothing falls through and the client's
# own permissions decide, as they always have.
cline watermark --config $CFG --ns shop.$NS --complete-all >/dev/null
sleep 7
chk "a restricted find is refused"                      "DENIED" "$(L "
  try { print(db.getSiblingDB('shop').$NS.find({}).itcount()); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"
chk "a restricted count is refused"                     "DENIED" "$(L "
  try { print(db.getSiblingDB('shop').$NS.count()); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"

echo "== and it must still refuse when the answer would come from the archive =="
# Now the fast tier cannot answer, so these are the reads that fall through. The
# archive is reached with the router's credentials, not this client's, so unless
# something checks, a client with no access to the collection is handed its
# contents.
cline watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the fallback works for an allowed client"          "10" "$(R "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "a restricted find still refused"                   "DENIED" "$(L "
  try { print(db.getSiblingDB('shop').$NS.find({}).itcount()); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"
chk "a restricted count still refused"                  "DENIED" "$(L "
  try { print(db.getSiblingDB('shop').$NS.count()); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"
chk "a restricted countDocuments still refused"         "DENIED" "$(L "
  try { print(db.getSiblingDB('shop').$NS.countDocuments({})); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"
chk "a restricted distinct still refused"               "DENIED" "$(L "
  try { print(db.getSiblingDB('shop').$NS.distinct('grp').length); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"
chk "a restricted aggregation still refused"            "DENIED" "$(L "
  try { print(db.getSiblingDB('shop').$NS.aggregate([{\$group:{_id:null,n:{\$sum:1}}}]).toArray().length); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"

H "db.getSiblingDB('admin').dropUser('limited')" >/dev/null 2>&1
stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
