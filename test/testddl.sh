#!/bin/bash
# Commands that change a collection rather than its documents, and the one
# deprecated command that still reads them.
#
# The feeder mirrors documents and nothing else. Drops, renames and every other
# collection-level change are ignored on purpose, because replaying them is what
# would let an accident on the fast tier destroy the archive. That rule is right
# and it has a consequence: an operation the fast tier performs and the archive
# does not leaves the two describing different collections, and the router goes
# on answering as though they matched.
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
NS=tddl
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
  cline feeder --config $CFG --log-file $LOGDIR/ddl_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/ddl_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}
# The rename of a tiered collection refuses while mirroring is behind, because a
# write to the old name that has not reached the archive has nowhere to go once
# the archive collection is renamed. A sentinel proves the feeder is current;
# sleeping for a guess does not.
feederCaughtUp() {
  H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:'__sentinel'})" >/dev/null
  for _ in $(seq 1 120); do
    [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:'__sentinel'}))")" = "1" ] && break
    sleep 0.5
  done
  H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:'__sentinel'})" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:'__sentinel'})" >/dev/null
  sleep 1
}
waitMirrored() {
  local n=""
  for _ in $(seq 1 60); do
    n=$(C "print(db.getSiblingDB(process.env.TC_DB).$1.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$2" ] && return 0
    sleep 0.5
  done
  echo "  WARN  $DB.$1 never reached $2 mirrored documents, last saw ${n:-none}"
}

echo "== setup =="
stopDaemons
for t in $NS ${NS}_renamed; do
  H "db.getSiblingDB(process.env.TC_DB).$t.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$t.drop()" >/dev/null
done
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({})' >/dev/null
startFeeder
startRouter

R "db.getSiblingDB(process.env.TC_DB).$NS.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i,grp:'g'})))" >/dev/null
waitMirrored $NS 10
stopFeeder
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:{\$lt:7}})" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the fast tier holds three, the archive ten"        "3" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "and a normal read still answers in full"           "10" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

echo "== renaming a tiered collection into a name nobody tiers =="
# The namespace list is configuration. A rename into a name it does not carry
# stops the tiering with nothing saying so: reads of the new name answer from
# whatever the fast tier holds, and reads of the old one from an archive nothing
# writes to any more. The collection is split in two and neither half says so.
RENAMED=$(R "
  try { db.adminCommand({renameCollection:'$DB.$NS', to:'$DB.${NS}_untiered'}); print('ALLOWED'); }
  catch(e) { print('REFUSED'); }")
chk "renaming into an untiered name is refused"         "REFUSED" "$RENAMED"
chk "and the collection is still where it was"          "3" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "the archive was not split"                         "10" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== renaming one tiered collection to another =="
# Both names tiered, so the tiering survives the move. The fast tier renames
# first, with the client's own authorization deciding, and the archive follows.
H "db.getSiblingDB(process.env.TC_DB).${NS}2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).${NS}2.drop()" >/dev/null
feederCaughtUp
MOVED=$(R "
  try { const r=db.adminCommand({renameCollection:'$DB.$NS', to:'$DB.${NS}2'}); print(r.ok===1?'MOVED':'ok='+r.ok); }
  catch(e) { print('REFUSED: '+e.message.substring(0,60)); }")
chk "renaming to another tiered name is allowed"        "MOVED" "$MOVED"
sleep 2
chk "the fast tier moved"                               "3"  "$(H "print(db.getSiblingDB(process.env.TC_DB).${NS}2.countDocuments({}))")"
chk "and the archive moved with it"                     "10" "$(C "print(db.getSiblingDB(process.env.TC_DB).${NS}2.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
chk "nothing is left under the old name"                "0"  "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
# The claim belongs to the collection, not to its name, so a read of the new
# name still answers in full rather than falling back to a watermark it lost.
chk "a read of the new name answers in full"            "10" "$(R "print(db.getSiblingDB(process.env.TC_DB).${NS}2.countDocuments({}))")"

echo "== a dropTarget that would destroy archived documents =="
# On the fast tier dropTarget is the client's business. In the archive it is the
# one thing no fast tier command is allowed to do.
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:99,v:'archived only'})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:99,v:'hot'})" >/dev/null
feederCaughtUp
DT=$(R "
  try { db.adminCommand({renameCollection:'$DB.${NS}2', to:'$DB.$NS', dropTarget:true}); print('ALLOWED'); }
  catch(e) { print('REFUSED'); }")
chk "dropTarget over archived documents is refused"     "REFUSED" "$DT"
chk "and they are still there"                          "1"     "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== mapReduce over a tiered collection =="
# Deprecated, rare, and it reads every document it is pointed at. Run against a
# fast tier holding three of ten it produces a total of three and reports
# success, which is the short answer in a different costume.
MR=$(R "
  try {
    const r = db.getSiblingDB(process.env.TC_DB).runCommand({
      mapReduce: '$NS',
      map: function() { emit('all', 1); },
      reduce: function(k, vals) { return Array.sum(vals); },
      out: {inline: 1}});
    print(r.results && r.results.length ? r.results[0].value : JSON.stringify(r).substring(0,40));
  } catch(e) { print('REFUSED'); }")
# Ten or a refusal. Three is the bug.
chk "mapReduce does not silently count three"           "true" \
    "$([ "$MR" = "REFUSED" ] || [ "$MR" = "10" ] && echo true || echo "false (got $MR)")"

# Where it writes was never asked about, only what it reads. A replacing output
# is built beside the target and renamed over it, and a rename is not mirrored,
# so the result would reach the fast tier and nothing else; a merging one writes
# per document against a tier holding part of the collection. The input here is
# untiered on purpose, so the check above cannot be what refuses it.
H "db.getSiblingDB(process.env.TC_DB).umrsrc.drop(); db.getSiblingDB(process.env.TC_DB).umrsrc.insertMany([{_id:1,n:1},{_id:2,n:2}])" >/dev/null
MRO() { # MRO OUTSPEC
  R "
  try {
    const r = db.getSiblingDB(process.env.TC_DB).runCommand({
      mapReduce: 'umrsrc',
      map: function() { emit('all', 1); },
      reduce: function(k, vals) { return Array.sum(vals); },
      out: $1});
    print(r.ok === 1 ? 'ALLOWED' : JSON.stringify(r).substring(0,60));
  } catch(e) { print('REFUSED'); }"
}
chk "mapReduce replacing a tiered collection refused"   "REFUSED" "$(MRO "{replace:'$NS'}")"
chk "mapReduce merging into one refused"                "REFUSED" "$(MRO "{merge:'$NS'}")"
chk "mapReduce reducing into one refused"               "REFUSED" "$(MRO "{reduce:'$NS'}")"
chk "  and it wrote nothing there"                      "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
chk "an untiered output is still allowed"               "ALLOWED" "$(MRO "{replace:'umrout'}")"
chk "  and it wrote the total there"                    "2" \
    "$(H "const d=db.getSiblingDB(process.env.TC_DB).umrout.findOne({_id:'all'}); print(d?d.value:'MISSING')")"
chk "an inline output is untouched"                     "true" \
    "$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({mapReduce:'umrsrc',map:function(){emit('all',1);},reduce:function(k,v){return Array.sum(v);},out:{inline:1}}); print(r.results&&r.results[0].value===2)")"

echo "== explain does not crash, whatever it says =="
EXP=$(R "
  try { const e=db.getSiblingDB(process.env.TC_DB).$NS.find({}).explain(); print(e.ok===1||e.queryPlanner?'ok':'odd'); }
  catch(e) { print('ERR '+e.message.substring(0,50)); }")
chk "explain is answered"                               "ok" "$EXP"

echo "== a TTL index on a tiered collection =="
# The fast tier holds the recent part of a tiered collection and the archive
# holds the rest, and this command reaches the fast tier. So the index expires
# the documents that were recent enough to be cached, the feeder reads those
# expiries as the application deleting and the archive tombstones its own
# copies, while the older documents the rule was written for sit in the archive
# where no TTL index is watching them.
#
# Measured before this was refused, on five documents a minute old on both tiers
# and five over a year old in the archive alone, asking for anything older than
# thirty seconds to expire: the five recent ones were destroyed on both tiers
# and the five year-old ones survived. It reported success.
TNS=tddlttl
H "db.getSiblingDB(process.env.TC_DB).$TNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$TNS.drop()" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).$TNS.insertOne({_id:1, at:new Date()})" >/dev/null
chk "a TTL index on a tiered collection is refused"     "refused" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$TNS.createIndex({at:1},{expireAfterSeconds:30});print('ACCEPTED')}catch(e){print('refused')}")"
chk "and the refusal says which way it would go wrong"  "true" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$TNS.createIndex({at:1},{expireAfterSeconds:30});print('false')}catch(e){print(/newest documents and keep the oldest/.test(e.message))}")"
chk "and no TTL index was created"                      "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$TNS.getIndexes().filter(function (i) { return i.expireAfterSeconds !== undefined }).length)")"
# The controls. An ordinary index on the same collection has to keep working, or
# this guard is refusing far more than it means to.
chk "an ordinary index on it is still accepted"         "accepted" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$TNS.createIndex({at:1},{name:'plain_at'});print('accepted')}catch(e){print('refused: '+e.message.substring(0,60))}")"
# And a collection nothing tiers lives on the fast tier alone, where a TTL index
# means exactly what it says.
H "db.getSiblingDB(process.env.TC_DB).customers.drop()" >/dev/null 2>&1
R "db.getSiblingDB(process.env.TC_DB).customers.insertOne({_id:1, at:new Date()})" >/dev/null
chk "an untiered collection may still have one"         "accepted" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).customers.createIndex({at:1},{expireAfterSeconds:3600});print('accepted')}catch(e){print('refused: '+e.message.substring(0,60))}")"
# The same instruction written differently.
# Looking for this refusal by name rather than for any refusal at all: on a
# binary without the guard, the collMod fails anyway because the index it names
# was never created, and a check that cannot tell those apart passes on the bug.
chk "a collMod that turns one into a TTL index is refused" "true" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({collMod:'$TNS', index:{name:'plain_at', expireAfterSeconds:30}});print('false')}catch(e){print(/newest documents and keep the oldest/.test(e.message))}")"
chk "and a collMod that does not is not"                "accepted" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({collMod:'$TNS', index:{name:'plain_at', hidden:true}});print('accepted')}catch(e){print('refused: '+e.message.substring(0,60))}")"
H "db.getSiblingDB(process.env.TC_DB).$TNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$TNS.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).customers.drop()" >/dev/null 2>&1

echo "== turning a tiered collection into one the feeder cannot mirror =="
# The feeder refuses a capped or time series namespace at startup and says why:
# a capped collection removes its own oldest documents when it fills, nothing
# announces those, and the archive would record them as the application
# deleting. That check runs when the feeder starts, and nothing stopped a client
# turning a collection into one afterwards.
#
# Measured on a tiered collection of 200 documents of about a kilobyte, with
# convertToCapped to 4KB through the router. Three things happened and the
# command reported success: the fast tier went to 4 documents immediately, the
# feeder then refused to start at all so mirroring stopped for every namespace,
# and once anything claimed the fast tier was complete the router answered
# countDocuments with 4 where the archive holds 200.
UNS=tddlcap
H "db.getSiblingDB(process.env.TC_DB).$UNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$UNS.drop()" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).$UNS.insertMany([{_id:1},{_id:2},{_id:3}])" >/dev/null
chk "convertToCapped on a tiered collection is refused"  "true" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({convertToCapped:'$UNS', size:4096});print('false')}catch(e){print(/cannot mirror one/.test(e.message))}")"
chk "and it still holds what it held"                    "3" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$UNS.countDocuments({}))")"
chk "and is not capped"                                  "false" \
    "$(H "const i=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$UNS'})[0]; print(!!(i&&i.options&&i.options.capped))")"
chk "creating it capped in the first place is refused"   "true" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({create:'${UNS}2', capped:true, size:4096});print('false')}catch(e){print(/cannot mirror one/.test(e.message))}")"
chk "and as a time series collection too"                "true" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({create:'${UNS}2', timeseries:{timeField:'at'}});print('false')}catch(e){print(/cannot mirror one/.test(e.message))}")"
# The controls. create is an ordinary command and most of what it does is fine,
# so a guard that refuses it wholesale would be worse than the bug.
chk "an ordinary create on a tiered name is accepted"    "accepted" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({create:'${UNS}2'});print('accepted')}catch(e){print('refused: '+e.message.substring(0,60))}")"
chk "and a capped collection nothing tiers is fine"      "accepted" \
    "$(R "db.getSiblingDB(process.env.TC_DB2).capfree.drop();
          try{db.getSiblingDB(process.env.TC_DB2).runCommand({create:'capfree', capped:true, size:4096});print('accepted')}catch(e){print('refused: '+e.message.substring(0,60))}")"
R "db.getSiblingDB(process.env.TC_DB2).capfree.drop()" >/dev/null 2>&1
H "db.getSiblingDB(process.env.TC_DB).$UNS.drop(); db.getSiblingDB(process.env.TC_DB).${UNS}2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$UNS.drop(); db.getSiblingDB(process.env.TC_DB).${UNS}2.drop()" >/dev/null

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
