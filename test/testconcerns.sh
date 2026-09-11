#!/bin/bash
# Write concern, read concern, and concurrent writers.
#
# None of this could be tested on single-node replica sets, because a lone
# member acknowledges majority the instant it acknowledges anything, and a
# local read is a majority read by definition. On three members the words start
# to mean different things, and the router sits in the middle of all of them.
#
# The question in every section is the same: does the promise the caller asked
# for survive the round trip? A weaker guarantee delivered silently is the
# failure this project treats as the worst kind, because nothing downstream can
# detect it.
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
NS=tconc2
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
  cline feeder --config $CFG --log-file $LOGDIR/conc_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/conc_r.log & RP=$!
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
    n=$(C "print(db.getSiblingDB(process.env.TC_DB).$1.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$2" ] && return 0
    sleep 0.5
  done
  echo "  WARN  $DB.$1 never reached $2 mirrored documents, last saw ${n:-none}"
}
evictBehindTheFeeder() {
  stopFeeder
  H "$1" >/dev/null
  C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder
}

echo "== setup =="
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({})" >/dev/null
startFeeder
startRouter
echo "  rsHot has $(H 'print(rs.status().members.length)') members, rsCold has $(C 'print(rs.status().members.length)')"

echo "== write concern travels to the fast tier =="
# The router proxies the raw command, so the write concern should reach mongod
# untouched. What must not happen is a write reported as majority-acknowledged
# when it was not, so the strict cases are the interesting ones.
chk "w:majority insert is acknowledged"        "1" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:1,v:'a'},{writeConcern:{w:'majority'}}).acknowledged?1:0)")"
chk "w:1 insert is acknowledged"               "1" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:2,v:'b'},{writeConcern:{w:1}}).acknowledged?1:0)")"
chk "w:3 insert is acknowledged"               "1" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:3,v:'c'},{writeConcern:{w:3}}).acknowledged?1:0)")"
chk "j:true insert is acknowledged"            "1" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:4,v:'d'},{writeConcern:{w:'majority',j:true}}).acknowledged?1:0)")"
# An impossible write concern must be reported, not swallowed. w:9 on a three
# member set can never be satisfied, so a short wtimeout has to surface.
chk "an unsatisfiable w:9 reports its timeout"  "true" "$(R "
  try { db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:5,v:'e'},{writeConcern:{w:9,wtimeout:1500}}); print('false'); }
  catch(e) { print(/timeout|not enough|unsatisfiable|wtimeout/i.test(e.message)?'true':('other: '+e.message.substring(0,50))); }")"
# The document is still there: a write concern timeout is not a rollback, and an
# application that assumes otherwise corrupts its own state.
chk "and the document is present regardless"    "1" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:5}))")"

echo "== read concern on the fast tier =="
chk "readConcern local reads"                   "a" "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.find({_id:1}).readConcern('local').toArray()[0]; print(d?d.v:'MISSING')")"
chk "readConcern majority reads"                "a" "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.find({_id:1}).readConcern('majority').toArray()[0]; print(d?d.v:'MISSING')")"
chk "readConcern linearizable reads"            "a" "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.find({_id:1}).readConcern('linearizable').maxTimeMS(5000).toArray()[0]; print(d?d.v:'MISSING')")"

echo "== read concern survives the fall through to the archive =="
# The whole readConcern document used to be stripped on the way to the archive,
# which turned a majority read into a local one without telling anybody. On a
# single-node archive that was invisible. On three members it is the difference
# between an answer that can be rolled back and one that cannot.
waitMirrored $NS 5
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "majority range read is answered in full"   "5" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).readConcern('majority').itcount())")"
chk "local range read is answered in full"      "5" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).readConcern('local').itcount())")"
chk "majority countDocuments is answered"       "5" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({},{readConcern:{level:'majority'}}))")"
# Ask the archive what it was actually sent, rather than believing a log line.
# The profiler records the command as received, readConcern included, which is
# the only way to see that the level crossed rather than being dropped.
C 'db.getSiblingDB(process.env.TC_DB).setProfilingLevel(2)' >/dev/null
R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).readConcern('majority').itcount())" >/dev/null
LEVELSEEN=$(C "const p=db.getSiblingDB(process.env.TC_DB).system.profile.find({'command.find':'$NS'}).sort({ts:-1}).limit(1).toArray()[0];
  print(p && p.command && p.command.readConcern ? p.command.readConcern.level : 'none')")
C 'db.getSiblingDB(process.env.TC_DB).setProfilingLevel(0)' >/dev/null
chk "the archive was told the level, not none"  "majority" "$LEVELSEEN"
# Linearizable is the one that cannot cross. Downgrading it silently would be
# the worst outcome, so it has to be refused out loud.
chk "linearizable is refused, not downgraded"   "REFUSED" "$(R "
  try { db.getSiblingDB(process.env.TC_DB).$NS.find({}).readConcern('linearizable').maxTimeMS(5000).itcount(); print('ANSWERED'); }
  catch(e) { print('REFUSED'); }")"

echo "== read concern on a point lookup that misses =="
cline watermark --config $CFG --ns $DB.$NS --complete-all >/dev/null
sleep 7
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({_id:{\$in:[1,2]}})"
chk "a majority point lookup finds the document" "a" "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.find({_id:1}).readConcern('majority').toArray()[0]; print(d?d.v:'MISSING')")"
chk "a linearizable point lookup is refused"     "REFUSED" "$(R "
  try { const d=db.getSiblingDB(process.env.TC_DB).$NS.find({_id:2}).readConcern('linearizable').maxTimeMS(5000).toArray()[0]; print(d?d.v:'MISSING'); }
  catch(e) { print('REFUSED'); }")"

echo "== options that may not survive the crossing =="
# The archive is expected to carry fewer indexes than the fast tier. The rollout
# guide says so in as many words, and tells operators to drop the ones the cheap
# tier does not need. A hint naming one of those indexes would then fail the
# moment the query falls through, caused by following our own advice.
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
H "db.getSiblingDB(process.env.TC_DB).$NS.createIndex({v:1},{name:'hot_only_v'})" >/dev/null
chk "a hinted read still works from the archive" "5" "$(R "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).hint('hot_only_v').itcount()); }
  catch(e) { print('ERR '+e.message.substring(0,60)); }")"
# The freshness wait used to ignore the caller's own deadline. To see that, the
# wait has to be long enough to notice: a feeder publishing its position every
# ten seconds makes a fallback read wait seconds unless the caller's own budget
# cuts it short. Without slowing it down there is usually nothing to wait for,
# and the check passes whether or not the fix is present.
stopFeeder
sed 's/position_every: .*/position_every: 10s/; s/checkpoint_every: .*/checkpoint_every: 30s/' $CFG > $WDIR/conc_slow.yaml
grep -q 'position_every' $WDIR/conc_slow.yaml || sed -i 's/  checkpoint_every: 30s/  checkpoint_every: 30s\n  position_every: 10s/' $WDIR/conc_slow.yaml
cline feeder --config $WDIR/conc_slow.yaml --log-file $LOGDIR/conc_slow.log & FP=$!
sleep 3
# Measured against the same read without a budget, rather than against a clock.
#
# An absolute threshold was the first version and it fails on a busy box for a
# reason that has nothing to do with the deadline: itcount pays for the getMores
# too, and maxTimeMS does not bound those -- it applies to the find, and a
# getMore carries its own or none. Measured under a parallel sweep, 3153ms for a
# 250ms budget, with the barrier itself behaving perfectly. The suite passed
# alone either side of it.
#
# Both arms pay whatever the box is charging, so the difference between them is
# the wait and nothing else. Bounded first, while the position is still stale,
# then a second write so the unbounded read has something of its own to wait for.
timed() { # EVAL -> milliseconds
  R "const t=Date.now(); try { $1 } catch(e) {} print(Date.now()-t);"
}
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:900,v:'z'})" >/dev/null
BOUNDED=$(timed "db.getSiblingDB(process.env.TC_DB).$NS.find({}).maxTimeMS(250).itcount();")
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:901,v:'z'})" >/dev/null
UNBOUNDED=$(timed "db.getSiblingDB(process.env.TC_DB).$NS.find({}).itcount();")
echo "      the same read took ${BOUNDED}ms with a 250ms budget and ${UNBOUNDED}ms with none"
# A fixture that produced no wait proves nothing either way, and saying so beats
# a green check over a comparison that never happened.
if [ "${UNBOUNDED:-0}" -lt 1000 ]; then
  echo "      SKIPPED  the unbounded read did not wait either, so there was no wait to bound"
else
  chk "maxTimeMS bounds the freshness wait"      "true" \
      "$(awk -v b="${BOUNDED:-99999}" -v u="${UNBOUNDED:-0}" 'BEGIN{print (b+0 < u/2) ? "true" : "false"}')"
fi
echo "      (a 250 ms budget returned in ${SLOWMS} ms, against a 10s position interval)"
pkill -f "cline feeder --config $WDIR/conc_slow" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
startFeeder

echo "== a client asking for compression must not get it =="
# Wire compression is negotiated in the handshake. If it is agreed, every later
# message is an OP_COMPRESSED blob this router cannot read, so no guard runs at
# all: no unique key claimed, no completeness checked, no document copied up.
# Nothing looks wrong, because the bytes are forwarded either way.
#
# So the negotiation has to fail. The check is that a client which asks for
# compressors still works and still gets guarded, which is what the duplicate
# below proves: an unguarded router would accept it.
H "db.getSiblingDB(process.env.TC_DB).${NS}z.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).${NS}z.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:'$DB.${NS}z'})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).${NS}z.createIndex({email:1},{unique:true})" >/dev/null
# zlib, not snappy: mongosh ships zlib with Node and needs an optional module
# for snappy, so a snappy URI fails to connect and tests nothing at all.
ZURI="mongodb://dba:secret@127.0.0.1:$PORT/?authSource=admin&compressors=zlib&zlibCompressionLevel=6&directConnection=true"
chk "a compressing client can still connect"           "1" \
    "$(mongosh "$ZURI" --quiet --eval 'print(db.adminCommand({ping:1}).ok)' 2>&1 | tail -1)"
mongosh "$ZURI" --quiet --eval "db.getSiblingDB(process.env.TC_DB).${NS}z.insertOne({_id:1,email:'zip@x.com'})" >/dev/null 2>&1
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).${NS}z.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "1" ] && break
  sleep 0.5
done
stopFeeder
H "db.getSiblingDB(process.env.TC_DB).${NS}z.deleteMany({})" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
# The owner is in the archive only. A router that could not read the command
# would let this through and leave two live documents holding one key.
chk "and its writes are still guarded"                 "11000" \
    "$(mongosh "$ZURI" --quiet --eval "
       try { db.getSiblingDB(process.env.TC_DB).${NS}z.insertOne({_id:2,email:'zip@x.com'}); print('ACCEPTED'); }
       catch(e) { print(e.code||e.message.substring(0,40)); }" 2>&1 | tail -1)"

echo "== concurrent writers on the same document =="
# Two clients updating one document is an ordinary write conflict inside mongod.
# What is not ordinary is that the router may be running a dry run of its own in
# a separate transaction to work out a unique key, which can conflict with them.
# When it does, the guard steps aside and says so; what must never happen is a
# lost update or a hang.
H "db.getSiblingDB(process.env.TC_DB).${NS}u.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).${NS}u.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:'$DB.${NS}u'})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).${NS}u.createIndex({email:1},{unique:true})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).${NS}u.insertOne({_id:1,email:'c0@x.com',n:0})" >/dev/null
# Collect the client pids and wait on those. A bare `wait` here waits for the
# feeder and the router too, which are background jobs of this same shell and
# never exit, so the suite hangs with every check above it already passed.
CLIENTS=()
for i in 1 2 3 4 5 6; do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
    for (let k=0;k<12;k++) { try { db.getSiblingDB(process.env.TC_DB).${NS}u.updateOne({_id:1},{\$inc:{n:1}}); } catch(e) {} }" >/dev/null 2>&1 &
  CLIENTS+=($!)
done
for pid in "${CLIENTS[@]}"; do wait "$pid" 2>/dev/null; done
chk "every increment landed, none lost"          "72" "$(H "const d=db.getSiblingDB(process.env.TC_DB).${NS}u.findOne({_id:1}); print(d?d.n:'MISSING')")"
chk "the unique index still holds one document"  "1"  "$(H "print(db.getSiblingDB(process.env.TC_DB).${NS}u.countDocuments({email:'c0@x.com'}))")"
chk "the router survived the concurrency"        "1"  "$(pgrep -fc "cline router --config $WDIR/" || true)"

echo "== concurrent inserts racing for one unique value =="
# Only one may win, and the loser must be told. The registry claim is an atomic
# upsert, so this is really a test that the router does not paper over a clash
# it lost the race on.
RACERS=()
for i in 1 2 3 4 5 6; do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
    try { db.getSiblingDB(process.env.TC_DB).${NS}u.insertOne({_id:'race$i',email:'race@x.com'}); print('won'); }
    catch(e) { print('lost'); }" >> /tmp/race.txt 2>&1 &
  RACERS+=($!)
done
for pid in "${RACERS[@]}"; do wait "$pid" 2>/dev/null; done
chk "exactly one insert won the race"            "1" "$(grep -c '^won$' /tmp/race.txt || true)"
chk "and only one document holds the value"      "1" "$(H "print(db.getSiblingDB(process.env.TC_DB).${NS}u.countDocuments({email:'race@x.com'}))")"
rm -f /tmp/race.txt

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
