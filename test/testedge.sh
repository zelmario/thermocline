#!/bin/bash
# Edge cases and config variants: the code paths the other suites never reach.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
NS=tprobe
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-46s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-46s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
# Remove documents from the fast tier the way an eviction does, with the feeder
# blind to it, so the archive keeps a live copy.
evictBehindTheFeeder() {
  pkill -f 'cline feeder'; sleep 1
  H "$1" >/dev/null
  C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
  cline feeder --config $CFG --log-file /var/log/cline/edge_f.log & FP=$!
  sleep 4
}

echo "== setup =="
pkill -f 'cline feeder|cline router'; sleep 2
H "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('shop').dated.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('shop').dated.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({})" >/dev/null
cline feeder --config $CFG --log-file /var/log/cline/edge_f.log & FP=$!
cline router --config $CFG --log-file /var/log/cline/edge_r.log & RP=$!
sleep 6

echo "== _id types other than ObjectId =="
R "db.getSiblingDB('shop').$NS.insertMany([
  {_id:'str-key',v:'s'},{_id:12345,v:'i'},{_id:{a:1,b:2},v:'c'},{_id:new Date('2026-01-01'),v:'d'}])" >/dev/null
sleep 5
evictBehindTheFeeder "db.getSiblingDB('shop').$NS.deleteMany({})"
chk "string _id read miss"    "s" "$(R "const d=db.getSiblingDB('shop').$NS.findOne({_id:'str-key'}); print(d?d.v:'MISSING')")"
chk "int _id read miss"       "i" "$(R "const d=db.getSiblingDB('shop').$NS.findOne({_id:12345}); print(d?d.v:'MISSING')")"
chk "document _id read miss"  "c" "$(R "const d=db.getSiblingDB('shop').$NS.findOne({_id:{a:1,b:2}}); print(d?d.v:'MISSING')")"
chk "date _id read miss"      "d" "$(R "const d=db.getSiblingDB('shop').$NS.findOne({_id:new Date('2026-01-01')}); print(d?d.v:'MISSING')")"
sleep 4
chk "all four copied up"      "4" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

# A document-valued _id is a literal, not an operator expression. Reading it as
# an operator meant an update to an archived one matched nothing and reported
# success, losing the write silently.
evictBehindTheFeeder "db.getSiblingDB('shop').$NS.deleteMany({})"
chk "write miss on a document _id"  "1" "$(R "print(db.getSiblingDB('shop').$NS.updateOne({_id:{a:1,b:2}},{\$set:{touched:1}}).matchedCount)")"
sleep 4
chk "and it reached the archive"    "1" "$(C "const d=db.getSiblingDB('shop').$NS.findOne({_id:{a:1,b:2}}); print(d&&d.touched?d.touched:'no')")"
chk "an _id range is still a range" "0" "$(R "print(db.getSiblingDB('shop').$NS.updateOne({_id:{\$gt:999999}},{\$set:{x:1}}).matchedCount)")"

echo "== \$out and \$merge run on incomplete input =="
cline watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "\$out runs"              "ok"   "$(R "try{db.getSiblingDB('shop').$NS.aggregate([{\$group:{_id:null,n:{\$sum:1}}},{\$out:'e_out'}]).toArray();print('ok')}catch(e){print(e.message.substring(0,50))}")"
# Grouped by a real field, not by null: \$merge refuses a null _id in mongod
# itself, so asking for one here would be testing MongoDB's rule, not ours.
chk "\$merge runs"            "ok"   "$(R "try{db.getSiblingDB('shop').$NS.aggregate([{\$group:{_id:'\$v',n:{\$sum:1}}},{\$merge:{into:'e_merge'}}]).toArray();print('ok')}catch(e){print(e.message.substring(0,50))}")"
chk "both wrote to the fast tier" "true" "$(H "const n=db.getSiblingDB('shop').getCollectionNames(); print(n.includes('e_out')&&n.includes('e_merge'))")"
chk "collStats still allowed" "true"    "$(R "print(db.getSiblingDB('shop').$NS.stats().count >= 0)")"

echo "== dry runs write nothing =="
cline watermark --config $CFG --ns shop.$NS --complete-all >/dev/null
BEFORE_H=$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")
BEFORE_C=$(C "print(db.getSiblingDB('shop').$NS.countDocuments({}))")
cline warmer  --config $CFG --dry-run >/dev/null 2>&1
cline evictor --config $CFG --once --dry-run >/dev/null 2>&1
chk "warmer dry run changed the fast tier by"  "$BEFORE_H" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "evictor dry run changed the archive by"   "$BEFORE_C" "$(C "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

echo "== date_field rules, the non-ObjectId selector =="
sed 's|    - ns: "shop.orders"\n      older_than: 40m|X|' $CFG > /root/edge_df.yaml
python3 - <<'PY'
import re
p='/root/edge_df.yaml'; s=open(p).read()
s=s.replace('    - ns: "shop.orders"\n      older_than: 40m','    - ns: "shop.dated"\n      older_than: 40m\n      date_field: "createdAt"')
s=s.replace('    - ns: "shop.orders"\n      back: 40m','    - ns: "shop.dated"\n      back: 12h\n      date_field: "createdAt"')
s=s.replace('    - "shop.tacct"\n','    - "shop.tacct"\n    - "shop.dated"\n',1)
open(p,'w').write(s)
PY
kill $FP 2>/dev/null; wait $FP 2>/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
cline feeder --config /root/edge_df.yaml --log-file /var/log/cline/edge_df.log & FP=$!
sleep 4
H 'const c=db.getSiblingDB("shop").dated; const now=Date.now(); const d=[];
   for(let i=0;i<30;i++) d.push({_id:i, createdAt:new Date(now-i*5*60*1000), n:i});
   c.insertMany(d); c.createIndex({createdAt:1,_id:1});' >/dev/null
sleep 3
H 'db.getSiblingDB("shop").dated.updateMany({},{$set:{m:1}})' >/dev/null
sleep 6
chk "date_field collection mirrored" "30" "$(C 'print(db.getSiblingDB("shop").dated.countDocuments({}))')"
cline evictor --config /root/edge_df.yaml --once >/dev/null 2>&1
chk "date_field evictor kept the young"  "8"  "$(H 'print(db.getSiblingDB("shop").dated.countDocuments({}))')"
chk "and the archive kept everything"    "30" "$(C 'print(db.getSiblingDB("shop").dated.countDocuments({_tcDeletedAt:{$exists:false}}))')"
cline watermark --config /root/edge_df.yaml --ns shop.dated --clear >/dev/null
cline warmer --config /root/edge_df.yaml >/dev/null 2>&1
chk "date_field warmer refilled"         "30" "$(H 'print(db.getSiblingDB("shop").dated.countDocuments({}))')"

echo "== unannounced delete policies =="
kill $FP 2>/dev/null; wait $FP 2>/dev/null
sed 's/unannounced_deletes: tombstone/unannounced_deletes: ignore/' $CFG > /root/edge_ignore.yaml
sed 's/unannounced_deletes: tombstone/unannounced_deletes: halt/'   $CFG > /root/edge_halt.yaml
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'})" >/dev/null
cline feeder --config /root/edge_ignore.yaml --log-file /var/log/cline/edge_ig.log & FP=$!
sleep 4
H "db.getSiblingDB('shop').$NS.insertOne({_id:1,v:'a'})" >/dev/null
sleep 4
H "db.getSiblingDB('shop').$NS.deleteOne({_id:1})" >/dev/null
sleep 5
chk "ignore leaves the archive readable" "no" "$(C "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d?(d._tcDeletedAt?'yes':'no'):'GONE')")"
chk "and the feeder keeps running"       "1"  "$(pgrep -fc 'cline feeder --config /root/edge_ignore' || echo 0)"

kill $FP 2>/dev/null; wait $FP 2>/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
rm -f /var/log/cline/edge_halt.log
cline feeder --config /root/edge_halt.yaml --log-file /var/log/cline/edge_halt.log &
HP=$!
sleep 4
H "db.getSiblingDB('shop').$NS.insertOne({_id:2,v:'b'})" >/dev/null
sleep 3
H "db.getSiblingDB('shop').$NS.deleteOne({_id:2})" >/dev/null
sleep 6
wait $HP 2>/dev/null; HALTRC=$?
chk "halt stops the feeder"              "1"    "$HALTRC"
# The reason has to reach the log file. main prints it to stderr, which is not
# where the logs go once --log-file is set, so a halt used to end the log
# mid-sentence and explain nothing.
chk "and says why in the log file"       "true" "$(grep -qE 'level=ERROR.*feeder stopped.*unannounced' /var/log/cline/edge_halt.log && echo true || echo false)"

cline feeder --config $CFG --log-file /var/log/cline/edge_f.log & FP=$!
sleep 2
kill $RP $FP 2>/dev/null; wait $RP 2>/dev/null; wait $FP 2>/dev/null
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
