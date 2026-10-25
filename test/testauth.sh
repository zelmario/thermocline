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
NS=tauth
# Tiered, and deliberately left off the fast tier for the catalog section.
CATNS=tcatalog
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
# The restricted client, through the router.
L() { mongosh "mongodb://limited:limited@127.0.0.1:$PORT/?authSource=admin&directConnection=true" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
stopFeeder() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/auth_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/auth_r.log & RP=$!
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

# A user who may read one database and not the one being tiered. Created on the
# fast tier, which is where the router sends the handshake.
H "db.getSiblingDB('admin').dropUser('limited')" >/dev/null 2>&1
H "db.getSiblingDB('admin').createUser({user:'limited', pwd:'limited', roles:[{role:'read', db:'elsewhere'}]})" >/dev/null
chk "the restricted user exists"                        "1" \
    "$(H "print(db.getSiblingDB('admin').system.users.countDocuments({user:'limited'}))")"

startFeeder
startRouter
R "db.getSiblingDB(process.env.TC_DB).$NS.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i,grp:'g'})))" >/dev/null
waitMirrored 10

echo "== the fast tier still refuses what it should =="
# The fast tier holds everything here, so nothing falls through and the client's
# own permissions decide, as they always have.
cline watermark --config $CFG --ns $DB.$NS --complete-all >/dev/null
sleep 7
chk "a restricted find is refused"                      "DENIED" "$(L "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).itcount()); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"
chk "a restricted count is refused"                     "DENIED" "$(L "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.count()); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"

echo "== and it must still refuse when the answer would come from the archive =="
# Now the fast tier cannot answer, so these are the reads that fall through. The
# archive is reached with the router's credentials, not this client's, so unless
# something checks, a client with no access to the collection is handed its
# contents.
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the fallback works for an allowed client"          "10" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "a restricted find still refused"                   "DENIED" "$(L "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).itcount()); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"
chk "a restricted count still refused"                  "DENIED" "$(L "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.count()); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"
chk "a restricted countDocuments still refused"         "DENIED" "$(L "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({})); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"
chk "a restricted distinct still refused"               "DENIED" "$(L "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.distinct('grp').length); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"
chk "a restricted aggregation still refused"            "DENIED" "$(L "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:null,n:{\$sum:1}}}]).toArray().length); } catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,40)); }")"

echo "== and the catalog must not name what the fast tier withheld =="
# The router adds the archive's tiered collections and databases to
# listCollections and listDatabases, because a tiered collection with no copy on
# the fast tier is otherwise reported as not existing while reads of it work.
# That half of the answer is read with the router's own credentials over a
# connection this client has no session on, and the fast tier's half was
# filtered by what the client may see. So the names have to be checked the same
# way the documents are, or a listing tells a limited user what exists in places
# the fast tier deliberately did not mention.
H "db.getSiblingDB(process.env.TC_DB).$CATNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CATNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CATNS.insertMany([{_id:1},{_id:2}])" >/dev/null
chk "the fast tier does not have it"                    "false" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('$CATNS') >= 0)")"
# The control. An allowed client must see it, or the check below passes on a
# merge that is simply not happening.
chk "an allowed client is told it exists"               "true" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('$CATNS') >= 0)")"
chk "the restricted client is not"                      "false" \
    "$(L "try { print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('$CATNS') >= 0) } catch(e) { print('false') }")"
# That client is refused listCollections outright, so the merge never runs for
# it. The case that reaches the merge is a client whose listing succeeds and
# comes back filtered, which is a role scoped to one collection rather than to
# the database: MongoDB then answers listCollections with the collections the
# client may see and leaves out the rest.
H "db.getSiblingDB('admin').dropUser('onecoll')" >/dev/null 2>&1
H "db.getSiblingDB('admin').dropRole('onecollrole')" >/dev/null 2>&1
H "db.getSiblingDB('admin').createRole({role:'onecollrole', privileges:[{resource:{db:process.env.TC_DB, collection:'$NS'}, actions:['find']}], roles:[]})" >/dev/null
H "db.getSiblingDB('admin').createUser({user:'onecoll', pwd:'onecoll', roles:[{role:'onecollrole', db:'admin'}]})" >/dev/null
O() { mongosh "mongodb://onecoll:onecoll@127.0.0.1:$PORT/?authSource=admin&directConnection=true" --quiet --eval "$1" 2>&1 | tail -1; }
# authorizedCollections is how a client without the database-wide privilege asks
# for the part of the listing it is entitled to. Without it MongoDB refuses the
# command outright and the merge is never reached.
OLIST="db.getSiblingDB(process.env.TC_DB).runCommand({listCollections:1,nameOnly:true,authorizedCollections:true}).cursor.firstBatch.map(function (c) { return c.name })"
chk "the collection-scoped client can list at all"      "true" \
    "$(O "try { print($OLIST.indexOf('$NS') >= 0) } catch(e) { print('ERR ' + e.message.substring(0,50)) }")"
chk "and is not told about the one it may not read"     "false" \
    "$(O "try { print($OLIST.indexOf('$CATNS') >= 0) } catch(e) { print('ERR ' + e.message.substring(0,50)) }")"
chk "nor is the database named to it"                   "false" \
    "$(L "try { print(db.adminCommand({listDatabases:1,nameOnly:true}).databases.some(function (d) { return d.name === process.env.TC_DB })) } catch(e) { print('false') }")"
C "db.getSiblingDB(process.env.TC_DB).$CATNS.drop()" >/dev/null
H "db.getSiblingDB('admin').dropUser('onecoll')" >/dev/null 2>&1
H "db.getSiblingDB('admin').dropRole('onecollrole')" >/dev/null 2>&1

echo "== and a pipeline this router runs itself must be one the client may run =="
# A tiered $out, and an aggregation answered from the archive, run on this
# router's own clients. Nothing asked the fast tier whether the client may write
# the target or read what the pipeline joins, so a client allowed to read
# nothing replaced a tiered collection with its own documents.
H "db.getSiblingDB('admin').dropUser('outuser')" >/dev/null 2>&1
H "db.getSiblingDB('admin').dropRole('outrole')" >/dev/null 2>&1
H "db.getSiblingDB('admin').createRole({role:'outrole', privileges:[{resource:{db:process.env.TC_DB, collection:'$NS'}, actions:['find']}], roles:[]})" >/dev/null
H "db.getSiblingDB('admin').createUser({user:'outuser', pwd:'outuser', roles:[{role:'outrole', db:'admin'}]})" >/dev/null
U() { mongosh "mongodb://outuser:outuser@127.0.0.1:$PORT/?authSource=admin&directConnection=true" --quiet --eval "$1" 2>&1 | tail -1; }
C "db.getSiblingDB(process.env.TC_DB).$CATNS.drop(); db.getSiblingDB(process.env.TC_DB).$CATNS.insertMany([{_id:1, secret:true},{_id:2, secret:true}])" >/dev/null
DENY="catch(e) { print(/not authorized|Unauthorized/i.test(e.message)?'DENIED':'other: '+e.message.substring(0,60)); }"
chk "a client that may read nothing cannot \$out over it" "DENIED" "$(L "
  try { db.getSiblingDB(process.env.TC_DB).aggregate([{\$documents:[{_id:'x'}]},{\$out:'$NS'}]); print('WROTE'); } $DENY")"
chk "a reader of it cannot \$out over it either"         "DENIED" "$(U "
  try { db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$match:{}},{\$out:'$NS'}]); print('WROTE'); } $DENY")"
chk "nor \$merge into it"                                "DENIED" "$(U "
  try { db.getSiblingDB(process.env.TC_DB).aggregate([{\$documents:[{_id:'x'}]},{\$merge:{into:'$NS'}}]); print('WROTE'); } $DENY")"
chk "nor join a collection it may not read"              "DENIED" "$(U "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$unionWith:'$CATNS'},{\$match:{secret:true}}]).toArray().length); } $DENY")"
chk "the collection is untouched"                        "10 0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({})+' '+db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:'x'}))")"
# The control: the same reader still gets the aggregation it is allowed.
chk "and the reader still reads it from the archive"     "10" \
    "$(U "try { print(db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:null,n:{\$sum:1}}}]).toArray()[0].n); } $DENY")"
C "db.getSiblingDB(process.env.TC_DB).$CATNS.drop()" >/dev/null
H "db.getSiblingDB('admin').dropUser('outuser')" >/dev/null 2>&1
H "db.getSiblingDB('admin').dropRole('outrole')" >/dev/null 2>&1

echo "== an account the archive has and the fast tier does not =="
# Certain in an adoption rather than merely possible. The existing cluster
# carries the application's users; the fast tier is a new replica set and
# carries none of them. The router authenticates against the fast tier, so every
# connection is refused with MongoDB's ordinary authentication failure and
# nothing connects that failure to the tier missing the account.
#
# The refusal is correct. The silence is the failure, so this checks the router
# says which accounts and what to do, once, at startup.
C "try{db.getSiblingDB('admin').dropUser('oldapp')}catch(e){}
   db.getSiblingDB('admin').createUser({user:'oldapp',pwd:'oldpass',roles:[{role:'read',db:process.env.TC_DB}]})" >/dev/null 2>&1
chk "the archive has it and the fast tier does not"  "1/0" \
    "$(C "print(db.getSiblingDB('admin').system.users.countDocuments({user:'oldapp'}))")/$(H "print(db.getSiblingDB('admin').system.users.countDocuments({user:'oldapp'}))")"
stopDaemons
rm -f $LOGDIR/auth_users.log
startFeeder
cline router --config $CFG --log-file $LOGDIR/auth_users.log & RP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "the router names it at startup"                 "true" \
    "$(grep -q 'oldapp@admin' $LOGDIR/auth_users.log && echo true || echo false)"
chk "and says to create it on the fast tier"         "true" \
    "$(grep -qE 'create them on the fast tier' $LOGDIR/auth_users.log && echo true || echo false)"
# The behaviour itself is unchanged and correct: the account cannot log in.
chk "and the account still cannot log in"            "true" \
    "$(mongosh --port $PORT -u oldapp -p oldpass --authenticationDatabase admin --quiet --eval 'print(1)' 2>&1 | grep -qiE 'authentication failed' && echo true || echo false)"
C "try{db.getSiblingDB('admin').dropUser('oldapp')}catch(e){}" >/dev/null 2>&1

echo "== and the roles those accounts hold =="
# The action for a missing account is to create it on the fast tier with the
# same roles, and a custom role the fast tier does not have makes that step fail
# with "Could not find role". An application with its own role is one whose
# accounts all reference it, so the first adoption step refuses every one of
# them for a reason nobody was thinking about.
C "try{db.getSiblingDB('admin').dropUser('roleuser')}catch(e){}
   try{db.getSiblingDB('admin').dropRole('appreader')}catch(e){}
   db.getSiblingDB('admin').createRole({role:'appreader', privileges:[{resource:{db:process.env.TC_DB,collection:''}, actions:['find']}], roles:[]});
   db.getSiblingDB('admin').createUser({user:'roleuser',pwd:'pw',roles:[{role:'appreader',db:'admin'}]})" >/dev/null 2>&1
chk "the archive has the role and the fast tier does not" "1/0" \
    "$(C "print(db.getSiblingDB('admin').system.roles.countDocuments({role:'appreader'}))")/$(H "print(db.getSiblingDB('admin').system.roles.countDocuments({role:'appreader'}))")"
# The consequence, measured rather than assumed: the documented step refuses.
chk "creating the account on the fast tier refuses" "true" \
    "$(H "try{db.getSiblingDB('admin').createUser({user:'roleuser',pwd:'pw',roles:[{role:'appreader',db:'admin'}]}); print('false')}catch(e){print(/Could not find role/.test(e.message))}")"
stopDaemons
rm -f $LOGDIR/auth_roles.log
startFeeder
cline router --config $CFG --log-file $LOGDIR/auth_roles.log & RRP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "the router names the role at startup"       "true" \
    "$(grep -q 'appreader@admin' $LOGDIR/auth_roles.log && echo true || echo false)"
chk "and says to create it before the accounts"  "true" \
    "$(grep -qE 'then the accounts that hold them' $LOGDIR/auth_roles.log && echo true || echo false)"
# Built-in roles are on every server, so naming them would bury the one that
# matters.
chk "and does not name the built-in roles"       "false" \
    "$(grep -q 'readWrite@admin' $LOGDIR/auth_roles.log && echo true || echo false)"
kill $RRP 2>/dev/null; wait $RRP 2>/dev/null
C "try{db.getSiblingDB('admin').dropUser('roleuser')}catch(e){}
   try{db.getSiblingDB('admin').dropRole('appreader')}catch(e){}" >/dev/null 2>&1

echo "== and the cluster-wide defaults nobody names in a query =="
# setDefaultRWConcern is a decision about durability and about what a read may
# see, made once for a whole cluster and then never mentioned again: a driver
# that names no concern gets the cluster's. The fast tier is new and has only
# MongoDB's implicit defaults, and reads and writes through the router are
# served there, so an application whose old cluster answered at majority reads
# at local through the router and sees writes a rollback can still take away.
#
# Both sides are set explicitly rather than one, because a global default cannot
# be unset once it is set: "The global default write concern cannot be unset
# once it is set". A section that relies on the fast tier having none passes on
# a fresh cluster and then fails for ever afterwards on its own leftovers, which
# is what the first version of this did.
C "db.getSiblingDB('admin').runCommand({setDefaultRWConcern:1, defaultWriteConcern:{w:2}, defaultReadConcern:{level:'majority'}})" >/dev/null 2>&1
H "db.getSiblingDB('admin').runCommand({setDefaultRWConcern:1, defaultWriteConcern:{w:1}, defaultReadConcern:{level:'local'}})" >/dev/null 2>&1
chk "the archive carries an explicit default"     "global" \
    "$(C "print(db.getSiblingDB('admin').runCommand({getDefaultRWConcern:1}).defaultReadConcernSource)")"
chk "and the two tiers disagree about it"         "majority/local" \
    "$(C "print(db.getSiblingDB('admin').runCommand({getDefaultRWConcern:1}).defaultReadConcern.level)")/$(H "print(db.getSiblingDB('admin').runCommand({getDefaultRWConcern:1}).defaultReadConcern.level)")"
stopDaemons
rm -f $LOGDIR/auth_rw.log
startFeeder
cline router --config $CFG --log-file $LOGDIR/auth_rw.log & RWP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "the router names the read concern"           "true" \
    "$(grep -qE 'concern=read archive=majority fast_tier=local' $LOGDIR/auth_rw.log && echo true || echo false)"
chk "and the write concern, as it is written"     "true" \
    "$(grep -qE 'concern=write archive="?w=2' $LOGDIR/auth_rw.log && echo true || echo false)"
kill $RWP 2>/dev/null; wait $RWP 2>/dev/null
# Then do what the warning says, and check it stops. A warning that keeps firing
# after the fix is worth less than none.
H "db.getSiblingDB('admin').runCommand({setDefaultRWConcern:1, defaultWriteConcern:{w:2}, defaultReadConcern:{level:'majority'}})" >/dev/null 2>&1
rm -f $LOGDIR/auth_rw2.log
cline router --config $CFG --log-file $LOGDIR/auth_rw2.log & RWQ=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "and says nothing once they match"            "0" \
    "$(grep -c 'cluster-wide default' $LOGDIR/auth_rw2.log || true)"
kill $RWQ 2>/dev/null; wait $RWQ 2>/dev/null
# Both back to reading at local, because this cannot be undone and every other
# suite runs against the same cluster. A default of majority is not wrong, it is
# slower and it can answer with an older document, and a suite that reads back
# what it just wrote would start failing for a reason belonging to this section.
# The write concern stays matched at w:2, which is what a three member set
# resolves majority to anyway.
for U in "$COLD" "$HOT"; do
  mongosh "$U" --quiet --eval "db.getSiblingDB('admin').runCommand({setDefaultRWConcern:1, defaultReadConcern:{level:'local'}})" >/dev/null 2>&1
done
chk "the read concern is back to local on both"  "local/local" \
    "$(C "print(db.getSiblingDB('admin').runCommand({getDefaultRWConcern:1}).defaultReadConcern.level)")/$(H "print(db.getSiblingDB('admin').runCommand({getDefaultRWConcern:1}).defaultReadConcern.level)")"

H "db.getSiblingDB('admin').dropUser('limited')" >/dev/null 2>&1
stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
