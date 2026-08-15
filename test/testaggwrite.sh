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
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
NS=tprobe
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do
    pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0
    sleep 0.5
  done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/aggw_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
}
startRouter() {
  cline router --config ${1:-$CFG} --listen 0.0.0.0:${2:-27100} --log-file /var/log/cline/aggw_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port ${2:-27100} -u dba -p secret --authenticationDatabase admin \
      --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up"; exit 2
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
H "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('shop').rollup.drop(); db.getSiblingDB('shop').merged.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('shop').rollup.drop(); db.getSiblingDB('shop').merged.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({})" >/dev/null
startFeeder
startRouter

# 30 documents in three groups, so a rollup has something to reduce.
R "const c=db.getSiblingDB('shop').$NS; const d=[];
   for(let i=0;i<30;i++) d.push({_id:i,n:i,grp:['a','b','c'][i%3]});
   c.insertMany(d);" >/dev/null
waitMirrored 30

# Make the fast tier unable to answer, so every read below falls through.
cline watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the fast tier cannot answer this input" "30" "$(R "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

echo "== \$out runs instead of being refused =="
chk "\$out is accepted"          "ok" "$(R "try{db.getSiblingDB('shop').$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$out:'rollup'}]).toArray();print('ok')}catch(e){print(e.message.substring(0,60))}")"
chk "it wrote three groups"      "3"  "$(H "print(db.getSiblingDB('shop').rollup.countDocuments({}))")"
chk "and the totals are right"   "30" "$(H "const r=db.getSiblingDB('shop').rollup.aggregate([{\$group:{_id:null,t:{\$sum:'\$n'}}}]).toArray(); print(r.length?r[0].t:0)")"
chk "the result is on the fast tier, not only the archive" "10" "$(H "const d=db.getSiblingDB('shop').rollup.findOne({_id:'a'}); print(d?d.n:'MISSING')")"

echo "== \$merge runs too =="
chk "\$merge is accepted"        "ok" "$(R "try{db.getSiblingDB('shop').$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$merge:{into:'merged'}}]).toArray();print('ok')}catch(e){print(e.message.substring(0,60))}")"
chk "it wrote three groups"      "3"  "$(H "print(db.getSiblingDB('shop').merged.countDocuments({}))")"

echo "== the result mirrors down like any other write =="
sleep 6
chk "the archive received the rollup" "3" "$(C "print(db.getSiblingDB('shop').rollup.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== rerunning replaces rather than doubles =="
R "db.getSiblingDB('shop').$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$out:'rollup'}]).toArray()" >/dev/null
chk "\$out still holds three groups"  "3"  "$(H "print(db.getSiblingDB('shop').rollup.countDocuments({}))")"
chk "and the totals did not double"   "30" "$(H "const r=db.getSiblingDB('shop').rollup.aggregate([{\$group:{_id:null,t:{\$sum:'\$n'}}}]).toArray(); print(r.length?r[0].t:0)")"

echo "== a deleted document is not counted in the rollup =="
R "db.getSiblingDB('shop').$NS.deleteMany({grp:'c'})" >/dev/null
waitMirrored 20
R "db.getSiblingDB('shop').$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$out:'rollup'}]).toArray()" >/dev/null
chk "the deleted group is gone from the rollup" "2"  "$(H "print(db.getSiblingDB('shop').rollup.countDocuments({}))")"
chk "and the total dropped"                     "20" "$(H "const r=db.getSiblingDB('shop').rollup.aggregate([{\$group:{_id:null,t:{\$sum:'\$n'}}}]).toArray(); print(r.length?r[0].t:0)")"

echo "== a permissive router still refuses to persist a short answer =="
# Returning a short number is a wrong figure on a screen. Writing one with $out
# makes it the input to everything downstream, so permissive does not cover it.
kill $RP 2>/dev/null; wait $RP 2>/dev/null
sed 's/completeness: archive/completeness: permissive/' $CFG > /root/aggw_perm.yaml
startRouter /root/aggw_perm.yaml 27103
P() { mongosh --port 27103 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
chk "permissive refuses \$out" "refused" "$(P "try{db.getSiblingDB('shop').$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}},{\$out:'perm_out'}]).toArray();print('ALLOWED')}catch(e){print(e.message.match(/refusing/)?'refused':'other')}")"
chk "and wrote nothing"        "false"   "$(H "print(db.getSiblingDB('shop').getCollectionNames().includes('perm_out'))")"

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
H "db.getSiblingDB('shop').tbulk.drop(); db.getSiblingDB('shop').bulkout.drop()" >/dev/null
C "db.getSiblingDB('shop').tbulk.drop(); db.getSiblingDB('shop').bulkout.drop()" >/dev/null
R "const c=db.getSiblingDB('shop').tbulk; const blob='y'.repeat(6*1024);
   for(let b=0;b<5;b++){const d=[];for(let i=0;i<500;i++)d.push({_id:b*500+i,n:b*500+i,blob:blob});c.insertMany(d);}" >/dev/null
for _ in $(seq 1 120); do
  [ "$(C "print(db.getSiblingDB('shop').tbulk.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "2500" ] && break
  sleep 1
done
chk "the bulk collection mirrored" "2500" "$(C "print(db.getSiblingDB('shop').tbulk.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
cline watermark --config $CFG --ns shop.tbulk --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "\$out streams a 15 MB result"  "ok"   "$(R "try{db.getSiblingDB('shop').tbulk.aggregate([{\$project:{n:1,blob:1}},{\$out:'bulkout'}]).toArray();print('ok')}catch(e){print(e.message.substring(0,60))}")"
chk "every document arrived"        "2500" "$(H "print(db.getSiblingDB('shop').bulkout.countDocuments({}))")"
chk "and the payload survived"      "6144" "$(H "const d=db.getSiblingDB('shop').bulkout.findOne({n:1234}); print(d?d.blob.length:'MISSING')")"

echo "== \$out into an untiered target is still atomic =="
# An untiered target has nothing to mirror, so the replacement is built beside
# it and renamed over the top, which is how mongod implements $out itself.
chk "no temporary collection was left behind" "0" "$(H "print(db.getSiblingDB('shop').getCollectionNames().filter(n=>n.startsWith('tmp.thermocline')).length)")"
# Re-running over a smaller input must replace, not merge into, the old contents.
R "db.getSiblingDB('shop').tbulk.deleteMany({n:{\$gte:100}})" >/dev/null
for _ in $(seq 1 120); do
  [ "$(C "print(db.getSiblingDB('shop').tbulk.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "100" ] && break
  sleep 1
done
R "db.getSiblingDB('shop').tbulk.aggregate([{\$project:{n:1}},{\$out:'bulkout'}]).toArray()" >/dev/null
chk "the rerun replaced the collection"   "100" "$(H "print(db.getSiblingDB('shop').bulkout.countDocuments({}))")"
chk "and left no temporary behind either" "0"   "$(H "print(db.getSiblingDB('shop').getCollectionNames().filter(n=>n.startsWith('tmp.thermocline')).length)")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
