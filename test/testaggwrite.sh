#!/bin/bash
# $out and $merge over an input the fast tier cannot answer in full.
#
# These used to be refused. The reason was sound: the input can only be read
# whole from the archive, and running the pipeline there would write the result
# to the archive, which is not where the application looks for it, and which
# only the feeder is allowed to write.
#
# Refusing is still the wrong answer. A nightly rollup that stops working the
# day tiering is switched on is exactly the surprise this project exists to
# avoid. So the pipeline is split at its last stage: the input is read from the
# archive, and the write stage is performed by the fast tier over that result.
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
NS=tprobe
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s got=%s want=%s\n' "$1" "$3" "$2"; fi
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
  cline feeder --config $CFG --log-file $LOGDIR/aggw_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
}
startRouter() {
  cline router --config ${1:-$CFG} --listen 0.0.0.0:${2:-$PORT} --log-file $LOGDIR/aggw_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port ${2:-$PORT} -u dba -p secret --authenticationDatabase admin \
      --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up"; exit 2
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
H "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_DB).rollup.drop(); db.getSiblingDB(process.env.TC_DB).merged.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_DB).rollup.drop(); db.getSiblingDB(process.env.TC_DB).merged.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})" >/dev/null
startFeeder
startRouter

# 30 documents in three groups, so a rollup has something to reduce.
R "const c=db.getSiblingDB(process.env.TC_DB).$NS; const d=[];
   for(let i=0;i<30;i++) d.push({_id:i,n:i,grp:['a','b','c'][i%3]});
   c.insertMany(d);" >/dev/null
waitMirrored 30

# Make the fast tier unable to answer, so every read below falls through.
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the fast tier cannot answer this input" "30" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

echo "== \$out runs instead of being refused =="
chk "\$out is accepted"          "ok" "$(R "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$out:'rollup'}]).toArray();print('ok')}catch(e){print(e.message.substring(0,60))}")"
chk "it wrote three groups"      "3"  "$(H "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({}))")"
chk "and the totals are right"   "30" "$(H "const r=db.getSiblingDB(process.env.TC_DB).rollup.aggregate([{\$group:{_id:null,t:{\$sum:'\$n'}}}]).toArray(); print(r.length?r[0].t:0)")"
chk "the result is on the fast tier, not only the archive" "10" "$(H "const d=db.getSiblingDB(process.env.TC_DB).rollup.findOne({_id:'a'}); print(d?d.n:'MISSING')")"

echo "== \$merge runs too =="
chk "\$merge is accepted"        "ok" "$(R "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$merge:{into:'merged'}}]).toArray();print('ok')}catch(e){print(e.message.substring(0,60))}")"
chk "it wrote three groups"      "3"  "$(H "print(db.getSiblingDB(process.env.TC_DB).merged.countDocuments({}))")"

echo "== the result mirrors down like any other write =="
sleep 6
chk "the archive received the rollup" "3" "$(C "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== rerunning replaces rather than doubles =="
R "db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$out:'rollup'}]).toArray()" >/dev/null
chk "\$out still holds three groups"  "3"  "$(H "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({}))")"
chk "and the totals did not double"   "30" "$(H "const r=db.getSiblingDB(process.env.TC_DB).rollup.aggregate([{\$group:{_id:null,t:{\$sum:'\$n'}}}]).toArray(); print(r.length?r[0].t:0)")"

echo "== a deleted document is not counted in the rollup =="
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({grp:'c'})" >/dev/null
waitMirrored 20
R "db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$out:'rollup'}]).toArray()" >/dev/null
chk "the deleted group is gone from the rollup" "2"  "$(H "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({}))")"
chk "and the total dropped"                     "20" "$(H "const r=db.getSiblingDB(process.env.TC_DB).rollup.aggregate([{\$group:{_id:null,t:{\$sum:'\$n'}}}]).toArray(); print(r.length?r[0].t:0)")"

echo "== a permissive router still refuses to persist a short answer =="
# Returning a short number is a wrong figure on a screen. Writing one with $out
# makes it the input to everything downstream, so permissive does not cover it.
kill $RP 2>/dev/null; wait $RP 2>/dev/null
sed 's/completeness: archive/completeness: permissive/' $CFG > $WDIR/aggw_perm.yaml
startRouter $WDIR/aggw_perm.yaml $P3
P() { mongosh --port $P3 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
chk "permissive refuses \$out" "refused" "$(P "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$out:'perm_out'}]).toArray();print('ALLOWED')}catch(e){print(e.message.match(/refusing/)?'refused':'other')}")"
chk "and wrote nothing"        "false"   "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes('perm_out'))")"

# Back to the archive-completeness router on the usual port: the permissive one
# above replaced it, and everything below expects the default behaviour.
kill $RP 2>/dev/null; wait $RP 2>/dev/null
startRouter

echo "== a result far larger than one command can carry =="
# The result used to be collected in memory and handed to the fast tier in a
# single command, which capped it at what a command can hold. It is streamed in
# batches now, so the only limit is what the tiers can store. 2500 documents of
# six kilobytes is about 15 MB, comfortably past the old ceiling, and several
# times the batch size so the batching itself is exercised.
H "db.getSiblingDB(process.env.TC_DB).tbulk.drop(); db.getSiblingDB(process.env.TC_DB).bulkout.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tbulk.drop(); db.getSiblingDB(process.env.TC_DB).bulkout.drop()" >/dev/null
R "const c=db.getSiblingDB(process.env.TC_DB).tbulk; const blob='y'.repeat(6*1024);
   for(let b=0;b<5;b++){const d=[];for(let i=0;i<500;i++)d.push({_id:b*500+i,n:b*500+i,blob:blob});c.insertMany(d);}" >/dev/null
for _ in $(seq 1 120); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).tbulk.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "2500" ] && break
  sleep 1
done
chk "the bulk collection mirrored" "2500" "$(C "print(db.getSiblingDB(process.env.TC_DB).tbulk.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
cline watermark --config $CFG --ns $DB.tbulk --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "\$out streams a 15 MB result"  "ok"   "$(R "try{db.getSiblingDB(process.env.TC_DB).tbulk.aggregate([{\$project:{n:1,blob:1}},{\$out:'bulkout'}]).toArray();print('ok')}catch(e){print(e.message.substring(0,60))}")"
chk "every document arrived"        "2500" "$(H "print(db.getSiblingDB(process.env.TC_DB).bulkout.countDocuments({}))")"
chk "and the payload survived"      "6144" "$(H "const d=db.getSiblingDB(process.env.TC_DB).bulkout.findOne({n:1234}); print(d?d.blob.length:'MISSING')")"

echo "== \$out into an untiered target is still atomic =="
# An untiered target has nothing to mirror, so the replacement is built beside
# it and renamed over the top, which is how mongod implements $out itself.
chk "no temporary collection was left behind" "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().filter(n=>n.startsWith('tmp.thermocline')).length)")"
# Re-running over a smaller input must replace, not merge into, the old contents.
R "db.getSiblingDB(process.env.TC_DB).tbulk.deleteMany({n:{\$gte:100}})" >/dev/null
for _ in $(seq 1 120); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).tbulk.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "100" ] && break
  sleep 1
done
R "db.getSiblingDB(process.env.TC_DB).tbulk.aggregate([{\$project:{n:1}},{\$out:'bulkout'}]).toArray()" >/dev/null
chk "the rerun replaced the collection"   "100" "$(H "print(db.getSiblingDB(process.env.TC_DB).bulkout.countDocuments({}))")"
chk "and left no temporary behind either" "0"   "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().filter(n=>n.startsWith('tmp.thermocline')).length)")"

echo "== \$out into a tiered target leaves nothing of what was there =="
# $out says the target holds the result and nothing else. Clearing the fast tier
# only reaches what the fast tier holds, so a document that has been evicted
# lives in the archive alone and nothing above deletes it. Measured before the
# fix: a $out promising exactly one document answered with two, the extra one
# being an evicted document from before it ran.
H "db.getSiblingDB(process.env.TC_DB).rollup.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).rollup.drop()" >/dev/null
# One document on both tiers, and one that has been evicted: live in the
# archive, absent from the fast tier, which is what eviction leaves behind.
H "db.getSiblingDB(process.env.TC_DB).rollup.insertOne({_id:2, v:'on both tiers'})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).rollup.insertOne({_id:2, v:'on both tiers'})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).rollup.insertOne({_id:1, v:'evicted long ago'})" >/dev/null
chk "the router sees both to begin with" "[1,2]" \
    "$(R "print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).rollup.find({}).sort({_id:1}).toArray().map(d=>d._id)))")"
R "db.getSiblingDB(process.env.TC_DB).tprobe.drop(); db.getSiblingDB(process.env.TC_DB).tprobe.insertOne({_id:99, v:'from the pipeline'})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).tprobe.aggregate([{\$match:{_id:99}},{\$out:'rollup'}]).toArray()" >/dev/null
sleep 5
chk "the target is exactly the result"   "[99]" \
    "$(R "print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).rollup.find({}).sort({_id:1}).toArray().map(d=>d._id)))")"
chk "and the archive holds only that"    "[99]" \
    "$(C "print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).rollup.find({_tcDeletedAt:{\$exists:false}}).sort({_id:1}).toArray().map(d=>d._id)))")"
# The archived document is marked deleted rather than removed, so what it was is
# still there to be looked at.
chk "the evicted one is tombstoned, not gone" "true" \
    "$(C "print(!!db.getSiblingDB(process.env.TC_DB).rollup.findOne({_id:1,_tcDeletedAt:{\$exists:true}}))")"

echo "== \$out into a tiered target that could have been forwarded =="
# Every section above makes the fast tier unable to answer the input, which is
# what sends the whole pipeline through the router. A $out whose input the fast
# tier *can* answer was forwarded, and mongod does a $out by building the result
# beside the target and renaming over it. The feeder ignores a rename on purpose,
# so the result reached the fast tier and nothing else.
#
# Measured before the fix: two documents written, the archive holding none of
# them sixty seconds later with its own earlier document still live, and a count
# through the router answering 1 where the fast tier held 2.
H "db.getSiblingDB(process.env.TC_DB).rollup.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).rollup.drop()" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).rollup.insertOne({_id:7, v:'here before the \$out'})" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({_id:7}))")" = "1" ] && break
  sleep 1
done
chk "the archive holds what was there"     "1" "$(C "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({_id:7,_tcDeletedAt:{\$exists:false}}))")"

# A pipeline that reads no collection at all: nothing about its input could send
# it to the archive, so this is the shape that was always forwarded.
chk "a \$documents pipeline is accepted"    "ok" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).aggregate([{\$documents:[{_id:8,v:'from documents'},{_id:9,v:'from documents'}]},{\$out:'rollup'}]).toArray();print('ok')}catch(e){print(e.code||e.message.substring(0,50))}")"
chk "the fast tier holds the result"        "[8,9]" \
    "$(H "print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).rollup.find({}).sort({_id:1}).toArray().map(d=>d._id)))")"
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({v:'from documents',_tcDeletedAt:{\$exists:false}}))")" = "2" ] && break
  sleep 1
done
chk "and so does the archive"               "[8,9]" \
    "$(C "print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).rollup.find({_tcDeletedAt:{\$exists:false}}).sort({_id:1}).toArray().map(d=>d._id)))")"
chk "what was there before is gone"         "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({_id:7}))")"
chk "and the router answers the result"     "2" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({}))")"

# The same with an input collection the fast tier claims in full, which is the
# other way a $out gets forwarded.
#
# On both tiers, by hand. A drop through the router is deliberately not replayed
# into the archive, so dropping it there and reinserting leaves the archive
# holding what the collection used to be, and the $out then reads that instead:
# 19 documents where the fixture meant 2.
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).$NS.insertMany([{_id:1,n:1},{_id:2,n:2}])" >/dev/null
waitMirrored 2
cline watermark --config $CFG --ns $DB.$NS --complete-all >/dev/null 2>&1
# The router caches a completeness claim, so a fresh one is not seen at once and
# the pipeline would be read from the archive as if the claim had never changed.
sleep 7
chk "a claimed input is accepted"           "ok" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$match:{n:{\$gte:1}}},{\$out:'rollup'}]).toArray();print('ok')}catch(e){print(e.code||e.message.substring(0,50))}")"
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "2" ] && break
  sleep 1
done
chk "the archive holds that result too"     "[1,2]" \
    "$(C "print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).rollup.find({_tcDeletedAt:{\$exists:false}}).sort({_id:1}).toArray().map(d=>d._id)))")"
chk "and no temporary was left behind"      "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().filter(n=>n.startsWith('tmp.thermocline')).length)")"

echo "== \$out onto the collection it reads =="
# mongod allows it, because it builds the result beside the target and renames
# over it, so the input is intact while it is read. Clearing the target first is
# what this used to do, and the input was deleted before it was read: measured on
# three documents with a $match keeping two, nought left on either tier where
# MongoDB leaves two. The result is built in a temporary collection now, exactly
# as mongod does, and moved in at the end.
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).$NS.insertMany([{_id:1,n:1},{_id:2,n:2},{_id:3,n:3}])" >/dev/null
waitMirrored 3
chk "three documents to begin with"        "3" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "\$out onto itself is accepted"        "ok" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$match:{n:{\$gte:2}}},{\$out:'$NS'}]).toArray();print('ok')}catch(e){print(e.code||e.message.substring(0,50))}")"
chk "  and it kept what the filter kept"   "[2,3]" \
    "$(R "print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).$NS.find({}).sort({_id:1}).toArray().map(d=>d._id)))")"
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "2" ] && break
  sleep 1
done
chk "  the archive agrees"                 "[2,3]" \
    "$(C "print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).$NS.find({_tcDeletedAt:{\$exists:false}}).sort({_id:1}).toArray().map(d=>d._id)))")"
chk "  and no temporary was left behind"   "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().filter(n=>n.startsWith('tmp.thermocline')).length)")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
