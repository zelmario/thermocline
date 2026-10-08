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
H "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_DB).rollup.drop(); db.getSiblingDB(process.env.TC_DB).merged.drop(); db.getSiblingDB(process.env.TC_DB).perm_out.drop()" >/dev/null
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

echo "== a filter mongod refuses gets mongod's answer, not a refusal =="
# A multi-document write brings archived matches up first, by asking the archive
# the client's filter. When mongod cannot parse that filter the archive refuses
# the question, and the router used to refuse the write with CommandFailed,
# saying it matched documents the fast tier did not hold. mongod answers ok:1
# with a write error of its own: 2, 51108, 168. Each line compares the router
# with the fast tier asked directly. All five were 125 on the build before 2026-10-08.
W() { echo "try{ const r=db.getSiblingDB(process.env.TC_DB).runCommand($1); print(r.ok?('ok we='+(r.writeErrors?r.writeErrors[0].code:'-')):('err '+r.code)) }catch(e){ print('err '+e.code) }"; }
WT() { echo "try{ const s=db.getMongo().startSession(); s.startTransaction(); let r; try{ r=s.getDatabase(process.env.TC_DB).runCommand($1) } finally { try{s.abortTransaction()}catch(e){} } print(r.ok?('ok we='+(r.writeErrors?r.writeErrors[0].code:'-')):('err '+r.code)) }catch(e){ print('err '+e.code) }"; }
for q in "{update:'$NS',updates:[{q:{n:{\$foo:1}},u:{\$set:{x:1}},multi:true}]}" \
         "{update:'$NS',updates:[{q:{grp:{\$regex:'a',\$options:'Q'}},u:{\$set:{x:1}},multi:true}]}" \
         "{update:'$NS',updates:[{q:{\$expr:{\$nosuch:1}},u:{\$set:{x:1}},multi:true}]}" \
         "{delete:'$NS',deletes:[{q:{n:{\$in:5}},limit:0}]}"; do
  chk "  ${q:0:46}" "$(H "$(W "$q")")" "$(R "$(W "$q")")"
done
q="{update:'$NS',updates:[{q:{n:{\$foo:1}},u:{\$set:{x:1}},multi:true}]}"
chk "  the same inside a transaction" "$(H "$(WT "$q")")" "$(R "$(WT "$q")")"
# A $merge is not forwarded but performed or refused here, after running the
# client's pipeline on the fast tier to see what it would match. That run fails
# with mongod's code, and the refusal carries it now instead of 125.
q="{aggregate:'$NS',pipeline:[{\$match:{n:{\$foo:1}}},{\$merge:{into:'$NS'}}],cursor:{}}"
chk "  a \$merge whose \$match mongod refuses" "$(H "$(W "$q")")" "$(R "$(W "$q")")"
chk "  and nothing changed"           "0" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({x:{\$exists:true}}))")"

echo "== \$out runs instead of being refused =="
chk "\$out is accepted"          "ok" "$(R "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$out:'rollup'}]).toArray();print('ok')}catch(e){print(e.message.substring(0,60))}")"
chk "it wrote three groups"      "3"  "$(H "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({}))")"
chk "and the totals are right"   "30" "$(H "const r=db.getSiblingDB(process.env.TC_DB).rollup.aggregate([{\$group:{_id:null,t:{\$sum:'\$n'}}}]).toArray(); print(r.length?r[0].t:0)")"
chk "the result is on the fast tier, not only the archive" "10" "$(H "const d=db.getSiblingDB(process.env.TC_DB).rollup.findOne({_id:'a'}); print(d?d.n:'MISSING')")"

echo "== \$merge runs too =="
chk "\$merge is accepted"        "ok" "$(R "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$merge:{into:'merged'}}]).toArray();print('ok')}catch(e){print(e.message.substring(0,60))}")"
chk "it wrote three groups"      "3"  "$(H "print(db.getSiblingDB(process.env.TC_DB).merged.countDocuments({}))")"
# One mongod refuses, performed here because the input is archived: a null _id
# cannot be merged on. The router failed it as its own CommandFailed, 125, where
# mongod says 51132, found by the fuzzer on 2026-10-07. Asked of the fast tier
# directly with the same pipeline rather than written down, so the check is
# about the router passing mongod's answer on and not about which code it is.
BADMERGE="[{\$group:{_id:null,n:{\$sum:1}}},{\$merge:{into:'mergedbad'}}]"
DIRECT=$(H "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate($BADMERGE).toArray();print('ACCEPTED')}catch(e){print(e.code)}")
chk "a \$merge mongod refuses keeps mongod's code" "$DIRECT" "$(R "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate($BADMERGE).toArray();print('ACCEPTED')}catch(e){print(e.code)}")"
chk "  which is a refusal, not an acceptance"      "true" "$([ -n "$DIRECT" ] && [ "$DIRECT" != ACCEPTED ] && echo true || echo false)"

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

echo "== a stage in the reading half that reads a collection of its own =="
# The $match that hides tombstones goes in front of the reading pipeline, and a
# $lookup does not read that pipeline's input, it reads a collection. So the
# join saw documents the application had deleted and $out wrote the count of
# them into a collection, where nothing downstream can tell it was wrong.
#
# Its own input collection, because this deletes documents and everything below
# counts $NS.
H "db.getSiblingDB(process.env.TC_DB).joinsrc.drop(); db.getSiblingDB(process.env.TC_DB).joinout.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).joinsrc.drop(); db.getSiblingDB(process.env.TC_DB).joinout.drop()" >/dev/null
R "const c=db.getSiblingDB(process.env.TC_DB).joinsrc; const d=[];
   for(let i=0;i<20;i++) d.push({_id:i,n:i,grp:['a','b'][i%2]});
   c.insertMany(d);" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).joinsrc.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "20" ] && break
  sleep 0.5
done
# Four documents out of group a, so the group still has live documents to join
# and tombstones to be wrong about. Deleting a whole group cannot show this: its
# tombstones only join to a group that has nothing left in it.
R "db.getSiblingDB(process.env.TC_DB).joinsrc.deleteMany({_id:{\$in:[0,2,4,6]}})" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).joinsrc.countDocuments({_tcDeletedAt:{\$exists:true}}))")" = "4" ] && break
  sleep 0.5
done
chk "the archive holds four tombstones"     "4"  "$(C "print(db.getSiblingDB(process.env.TC_DB).joinsrc.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
# And the fast tier must not be able to answer, or the pipeline never reaches
# the archive and the rewrite under test is never used.
cline watermark --config $CFG --ns $DB.joinsrc --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the fast tier cannot answer this input" "16" "$(R "print(db.getSiblingDB(process.env.TC_DB).joinsrc.countDocuments({}))")"
R "db.getSiblingDB(process.env.TC_DB).joinsrc.aggregate([{\$lookup:{from:'joinsrc',localField:'grp',foreignField:'grp',as:'sib'}},{\$group:{_id:'\$grp',s:{\$max:{\$size:'\$sib'}}}},{\$out:'joinout'}]).toArray()" >/dev/null
chk "the join wrote both groups"            "2"  "$(H "print(db.getSiblingDB(process.env.TC_DB).joinout.countDocuments({}))")"
chk "group a joined its six live documents" "6"  "$(H "const d=db.getSiblingDB(process.env.TC_DB).joinout.findOne({_id:'a'}); print(d?d.s:'MISSING')")"
chk "group b joined its ten"                "10" "$(H "const d=db.getSiblingDB(process.env.TC_DB).joinout.findOne({_id:'b'}); print(d?d.s:'MISSING')")"
# A $unionWith reads a collection too, and counts what it finds there.
chk "a union with itself counts each document once" "32" "$(R "const r=db.getSiblingDB(process.env.TC_DB).joinsrc.aggregate([{\$unionWith:'joinsrc'},{\$count:'c'}]).toArray(); print(r.length?r[0].c:0)")"

echo "== the reading half reaches a tiered collection through a view =="
# The section above is the same question asked of a collection named directly.
# A view is the other way to name one, and this path could not see it: the read
# guards call resolveJoinedViews before judging anything, and guardTieredOut
# builds its own scope, runs before them, and never did. So a $lookup naming a
# view was judged with the collection underneath it invisible, and this is the
# path that writes what it reads.
#
# Measured before the fix, on a joined collection the archive holds two of and
# the fast tier one: the target was written with one row joined and one holding
# an empty array. Not an answer somebody reads and re-runs, an answer on disk.
#
# And the other half of it, which is the pipeline reading *from* a view. That one
# is refused rather than read from the archive: the archive's copies carry
# tombstones, and the predicate that filters them goes in front of a pipeline and
# cannot reach inside a view's definition, which the router cannot rewrite.
stopDaemons; startFeeder; startRouter
for c in tovsrc tovjoin joinout; do
  H "db.getSiblingDB(process.env.TC_DB).$c.drop()" >/dev/null 2>&1
  C "db.getSiblingDB(process.env.TC_DB).$c.drop()" >/dev/null 2>&1
done
R "db.getSiblingDB(process.env.TC_DB).vovjoin.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({_id:{\$in:['$DB.tovsrc','$DB.tovjoin']}})" >/dev/null
# The collection the pipeline runs against: whole on both tiers and claimed, so
# nothing about it can refuse the pipeline and only the join can.
R "db.getSiblingDB(process.env.TC_DB).tovsrc.insertMany([{_id:1,k:'a'},{_id:2,k:'b'}])" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).tovsrc.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "2" ] && break
  sleep 1
done
cline watermark --config $CFG --ns $DB.tovsrc --complete-all --force >/dev/null 2>&1
# The collection the view reads, in the adoption shape: the archive holds both
# and the fast tier has never held the second. Written to each tier directly, so
# no tombstone is involved and the difference is only what each holds.
C "db.getSiblingDB(process.env.TC_DB).tovjoin.insertMany([{_id:1,k:'a',label:'ay'},{_id:2,k:'b',label:'bee'}])" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).tovjoin.insertOne({_id:1,k:'a',label:'ay'})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).createView('vovjoin','tovjoin',[])" >/dev/null
chk "the fast tier holds one of the joined collection" "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tovjoin.countDocuments({}))")"
chk "and the archive holds both"                       "2" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tovjoin.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
# Waited for: the inserts above went through the router, which read these claims
# to do it and remembers the answers it got then. See testquery.sh for the same
# wait and what asking once was measured to report.
OUTJ=""
for _ in $(seq 1 20); do
  R "db.getSiblingDB(process.env.TC_DB).joinout.drop()" >/dev/null 2>&1
  R "try{db.getSiblingDB(process.env.TC_DB).tovsrc.aggregate([{\$lookup:{from:'vovjoin',localField:'k',foreignField:'k',as:'j'}},{\$out:'joinout'}]).toArray()}catch(e){}" >/dev/null 2>&1
  OUTJ=$(R "try{print(db.getSiblingDB(process.env.TC_DB).joinout.find({}).sort({_id:1}).toArray().map(d=>d._id+':'+d.j.length).join(','))}catch(e){print('ERR')}")
  [ "$OUTJ" = "1:1,2:1" ] && break
  sleep 1
done
chk "the target is written whole, not short"           "1:1,2:1" "$OUTJ"
# And reading from the view itself, which cannot be answered from either tier.
R "db.getSiblingDB(process.env.TC_DB).joinout.drop()" >/dev/null 2>&1
OUTV=$(R "try{db.getSiblingDB(process.env.TC_DB).vovjoin.aggregate([{\$out:'joinout'}]).toArray();print('WROTE')}catch(e){print('REFUSED '+String(e.message).replace(/\s+/g,' '))}")
chk "a \$out reading from the view is refused"          "true" \
    "$(case "$OUTV" in "REFUSED "*"$DB.vovjoin"*) echo true;; *) echo "false (got $(echo "$OUTV"|cut -c1-70))";; esac)"
chk "  and it wrote nothing to the fast tier"          "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).joinout.countDocuments({}))")"
R "db.getSiblingDB(process.env.TC_DB).vovjoin.drop()" >/dev/null 2>&1
for c in tovsrc tovjoin joinout; do
  H "db.getSiblingDB(process.env.TC_DB).$c.drop()" >/dev/null 2>&1
  C "db.getSiblingDB(process.env.TC_DB).$c.drop()" >/dev/null 2>&1
done

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
# The pipeline's input is read from the archive, because the watermark above says
# the fast tier cannot answer for tprobe. So the document has to be in the
# archive before the $out runs, not merely written. The freshness barrier waits
# for mirroring, but only up to archive_wait, and in a three-worker sweep that
# was not always enough: the input came back empty and the $out then replaced the
# target with nothing. A wait rather than a hope.
waitMirrored 1
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

echo "== \$out creates the target the way the archive has it =="
# A collection brought into existence by an insert carries no options at all,
# and a default collation decides which documents match. Measured before this:
# the archive held the collection with {locale: "en", strength: 2} and the copy
# a $out left on the fast tier had none, so the query the application has always
# run answered differently as soon as the fast tier held the documents.
H "db.getSiblingDB(process.env.TC_DB).rollup.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).rollup.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).createCollection('rollup',{collation:{locale:'en',strength:2}})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).rollup.insertMany([{_id:1,name:'alice'},{_id:2,name:'ALICE'}])" >/dev/null
chk "the archive holds it with a collation"  "en" \
    "$(C "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'rollup'})[0]; print(c&&c.options&&c.options.collation?c.options.collation.locale:'none')")"
chk "the fast tier does not hold it at all"  "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'rollup'}).length)")"
R "db.getSiblingDB(process.env.TC_DB).aggregate([{\$documents:[{_id:3,name:'alice'},{_id:4,name:'ALICE'}]},{\$out:'rollup'}]).toArray()" >/dev/null
chk "the copy it created carries the collation" "en" \
    "$(H "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'rollup'})[0]; print(c&&c.options&&c.options.collation?c.options.collation.locale:'none')")"
chk "so the router matches the way the archive does" "2" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).rollup.countDocuments({name:'alice'}))")"

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

echo "== a scratch collection a dead router left behind =="
# Every failing path inside an aggregation drops the collection its result is
# being assembled in. A process that dies takes none of those paths, and nothing
# else ever looks at that collection again, so it sits in the application's own
# database for ever. A starting router removes the stale ones.
#
# The bar is an hour, from the nanosecond stamp in the name, because another
# router on this fast tier may be filling one right now and dropping its
# half-built result would turn litter into a wrong answer. So both arms: an old
# one goes and a new one stays.
#
# The mirror-everything arm is deliberately not here. The exclusion that keeps
# these out of the archive only matters with feeder.namespaces empty, and a
# feeder with an empty list inside a parallel sweep would watch every other
# worker's collections too, which is the one thing mkworker.sh exists to
# prevent. It was measured against the feeder's own pipeline instead.
NOWNS=$(( $(date +%s) * 1000000000 ))
OLDNS=$(( NOWNS - 7200 * 1000000000 ))
H "db.getSiblingDB(process.env.TC_DB).getCollection('tmp.thermocline.out.$OLDNS').insertOne({v:1});
   db.getSiblingDB(process.env.TC_DB).getCollection('tmp.thermocline.out.$NOWNS').insertOne({v:1})" >/dev/null
chk "two scratch collections, one stale one fresh" "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().filter(n=>n.startsWith('tmp.thermocline')).length)")"
kill $RP 2>/dev/null; wait $RP 2>/dev/null
startRouter
chk "  the stale one is gone"              "false" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('tmp.thermocline.out.$OLDNS') >= 0)")"
chk "  and the fresh one is left alone"    "true" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('tmp.thermocline.out.$NOWNS') >= 0)")"
chk "  and it said so, about that one"     "true" \
    "$(grep -q "removed a scratch collection a previous router left behind.*tmp.thermocline.out.$OLDNS" $LOGDIR/aggw_r.log && echo true || echo false)"
H "db.getSiblingDB(process.env.TC_DB).getCollection('tmp.thermocline.out.$NOWNS').drop()" >/dev/null

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
