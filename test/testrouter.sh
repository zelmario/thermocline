#!/bin/bash
# Functional tests of every router behaviour, through the wire protocol.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() { # chk NAME EXPECTED ACTUAL
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-46s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-46s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

H 'db.getSiblingDB("shop").tprobe_out.drop()' >/dev/null 2>&1
echo "== setup: clean collections, feeder + router running =="
pkill -f 'cline feeder|cline router'; sleep 2
H 'db.getSiblingDB("shop").tprobe.drop(); db.getSiblingDB("shop").tacct.drop()' >/dev/null
C 'db.getSiblingDB("shop").tprobe.drop(); db.getSiblingDB("shop").tacct.drop()' >/dev/null
C 'db.getSiblingDB("_thermocline").watermarks.deleteMany({})' >/dev/null
H 'db.getSiblingDB("_thermocline").keys.deleteMany({})' >/dev/null
cline feeder --config $CFG --log-file /var/log/cline/f.log &
FP=$!
cline router --config $CFG --log-file /var/log/cline/r.log &
RP=$!
sleep 6

echo "== hello / handshake =="
chk "presents as mongos (isdbgrid)" "isdbgrid" "$(R 'print(db.hello().msg)')"
chk "hides the replica set name"    "undefined" "$(R 'print(typeof db.hello().setName)')"

echo "== writes and mirroring =="
R 'db.getSiblingDB("shop").tprobe.insertMany([{_id:1,v:"a"},{_id:2,v:"b"},{_id:3,v:"c"}])' >/dev/null
sleep 4
chk "insert reaches the fast tier"  "3" "$(H 'print(db.getSiblingDB("shop").tprobe.countDocuments({}))')"
chk "feeder mirrors to the archive" "3" "$(C 'print(db.getSiblingDB("shop").tprobe.countDocuments({}))')"

echo "== read miss and copy-up =="
H 'db.getSiblingDB("shop").tprobe.deleteOne({_id:2})' >/dev/null   # feeder will tombstone; use an archive-only doc instead
C 'db.getSiblingDB("shop").tprobe.insertOne({_id:9,v:"archived-only"})' >/dev/null
chk "point lookup finds an archived doc" "archived-only" "$(R 'const d=db.getSiblingDB("shop").tprobe.findOne({_id:9}); print(d?d.v:"MISSING")')"
sleep 2
chk "and it was copied up"          "archived-only" "$(H 'const d=db.getSiblingDB("shop").tprobe.findOne({_id:9}); print(d?d.v:"MISSING")')"

echo "== write miss and copy-up =="
C 'db.getSiblingDB("shop").tprobe.insertOne({_id:10,v:"old"})' >/dev/null
chk "update on an archived doc matches 1" "1" "$(R 'print(db.getSiblingDB("shop").tprobe.updateOne({_id:10},{$set:{v:"new"}}).matchedCount)')"
chk "and the fast tier now holds it"      "new" "$(H 'const d=db.getSiblingDB("shop").tprobe.findOne({_id:10}); print(d?d.v:"MISSING")')"

echo "== tombstones are not resurrected =="
R 'db.getSiblingDB("shop").tprobe.deleteOne({_id:1})' >/dev/null
sleep 4
chk "archive keeps a deleted doc"   "true"  "$(C 'print(!!db.getSiblingDB("shop").tprobe.findOne({_id:1}))')"
chk "but marks it deleted"          "true"  "$(C 'print(!!db.getSiblingDB("shop").tprobe.findOne({_id:1})._tcDeletedAt)')"
chk "and the router will not return it" "null" "$(R 'print(db.getSiblingDB("shop").tprobe.findOne({_id:1}))')"

echo "== completeness: archive fallback =="
cline watermark --config $CFG --ns shop.tprobe --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
LIVE=$(C 'print(db.getSiblingDB("shop").tprobe.countDocuments({_tcDeletedAt:{$exists:false}}))')
chk "countDocuments matches the archive" "$LIVE" "$(R 'print(db.getSiblingDB("shop").tprobe.countDocuments({}))')"
chk "find itcount matches"               "$LIVE" "$(R 'print(db.getSiblingDB("shop").tprobe.find({}).itcount())')"
chk "sort+skip+limit works"              "1"     "$(R 'print(db.getSiblingDB("shop").tprobe.find({}).sort({_id:1}).skip(1).limit(1).itcount())')"

echo "== cursor paging through the archive =="
chk "batched find returns every doc" "$LIVE" "$(R 'let n=0; const c=db.getSiblingDB("shop").tprobe.find({}).batchSize(2); while(c.hasNext()){c.next();n++} print(n)')"

echo "== unique index across tiers =="
H 'db.getSiblingDB("shop").tacct.drop(); db.getSiblingDB("shop").tacct.createIndex({email:1},{unique:true})' >/dev/null
H 'db.getSiblingDB("_thermocline").keys.deleteMany({ns:"shop.tacct"})' >/dev/null
R 'db.getSiblingDB("shop").tacct.insertOne({_id:1,email:"z@x.com"})' >/dev/null
sleep 4
# Simulate an eviction rather than a delete. Deleting through the router would
# make the feeder tombstone the archive copy, which frees the key correctly and
# tests the opposite thing. A real eviction leaves the archive copy live, so the
# feeder must not see this removal at all.
kill $FP 2>/dev/null; wait $FP 2>/dev/null
H 'db.getSiblingDB("shop").tacct.deleteOne({_id:1})' >/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
cline feeder --config $CFG --log-file /var/log/cline/f.log & FP=$!
sleep 5
chk "archive still holds the original"     "true"  "$(C 'const d=db.getSiblingDB("shop").tacct.findOne({_id:1}); print(!!d && !d._tcDeletedAt)')"
chk "duplicate of an evicted key rejected" "11000" "$(R 'try{db.getSiblingDB("shop").tacct.insertOne({_id:2,email:"z@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,20))}')"
chk "a fresh key is still accepted"        "ok"    "$(R 'try{db.getSiblingDB("shop").tacct.insertOne({_id:3,email:"new@x.com"});print("ok")}catch(e){print(e.code)}')"
# And the other half: once the original is genuinely deleted the key frees up.
C 'db.getSiblingDB("shop").tacct.updateOne({_id:1},{$set:{_tcDeletedAt:new Date()}})' >/dev/null
chk "a deleted original frees its key"     "ok"    "$(R 'try{db.getSiblingDB("shop").tacct.insertOne({_id:4,email:"z@x.com"});print("ok")}catch(e){print(e.code)}')"

echo "== bulk insert keeps mongod semantics =="
H 'db.getSiblingDB("shop").tacct.drop(); db.getSiblingDB("shop").tacct.createIndex({email:1},{unique:true})' >/dev/null
H 'db.getSiblingDB("_thermocline").keys.deleteMany({ns:"shop.tacct"})' >/dev/null
R 'db.getSiblingDB("shop").tacct.insertOne({_id:"seed",email:"taken@x.com"})' >/dev/null
R 'try{db.getSiblingDB("shop").tacct.insertMany([{_id:"b1",email:"n1@x.com"},{_id:"b2",email:"taken@x.com"},{_id:"b3",email:"n3@x.com"}],{ordered:false})}catch(e){}' >/dev/null
chk "unordered inserts the good documents" "true,false,true" "$(H 'print(["b1","b2","b3"].map(i=>!!db.getSiblingDB("shop").tacct.findOne({_id:i})).join(","))')"
H 'db.getSiblingDB("shop").tacct.deleteMany({_id:{$in:["b1","b3"]}})' >/dev/null
H 'db.getSiblingDB("_thermocline").keys.deleteMany({ns:"shop.tacct"})' >/dev/null
R 'db.getSiblingDB("shop").tacct.insertOne({_id:"seed2",email:"taken2@x.com"})' >/dev/null
R 'try{db.getSiblingDB("shop").tacct.insertMany([{_id:"o1",email:"p1@x.com"},{_id:"o2",email:"taken2@x.com"},{_id:"o3",email:"p3@x.com"}],{ordered:true})}catch(e){}' >/dev/null
chk "ordered stops at the first clash" "true,false,false" "$(H 'print(["o1","o2","o3"].map(i=>!!db.getSiblingDB("shop").tacct.findOne({_id:i})).join(","))')"

echo "== transactions =="
C 'db.getSiblingDB("shop").tprobe.insertOne({_id:20,v:"txn"})' >/dev/null
chk "transaction replays after a copy-up" "1" "$(R 'const s=db.getMongo().startSession(); let a=0; try{ s.withTransaction(()=>{a++; const r=s.getDatabase("shop").tprobe.updateOne({_id:20},{$set:{v:"txn2"}}); if(r.matchedCount!==1) throw new Error("miss");}); print(a>1?1:0);}catch(e){print("FAILED")} s.endSession()')"

echo "== \$out runs against the archive =="
# The input reaches past what the fast tier holds, so it is read from the
# archive and the write stage is performed on the fast tier, where the
# application will look for the result.
chk "\$out on incomplete input runs"    "ok"    "$(R 'try{db.getSiblingDB("shop").tprobe.aggregate([{$group:{_id:null,n:{$sum:1}}},{$out:"tprobe_out"}]).toArray(); print("ok")}catch(e){print(e.message.substring(0,50))}')"
chk "and the result is on the fast tier" "true"  "$(H 'print(db.getSiblingDB("shop").getCollectionNames().includes("tprobe_out"))')"
# Recomputed here, not reused from earlier: the transaction section above adds
# a document to the archive, so the count taken before it is already stale.
NOWLIVE=$(C 'print(db.getSiblingDB("shop").tprobe.countDocuments({_tcDeletedAt:{$exists:false}}))')
chk "and it counted every document"      "$NOWLIVE" "$(H 'const d=db.getSiblingDB("shop").tprobe_out.findOne({}); print(d?d.n:"MISSING")')"

kill $RP 2>/dev/null; wait $RP 2>/dev/null

echo "== completeness: strict =="
sed 's/completeness: archive/completeness: strict/' $CFG > /root/strict.yaml
cline router --config /root/strict.yaml --listen 0.0.0.0:27101 --log-file /var/log/cline/rs.log &
SP=$!; sleep 5
S() { mongosh --port 27101 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
chk "strict refuses an incomplete find" "refused" "$(S 'try{db.getSiblingDB("shop").tprobe.find({}).itcount(); print("ALLOWED")}catch(e){print(e.message.match(/refusing/)?"refused":"other")}')"
kill $SP 2>/dev/null; wait $SP 2>/dev/null

echo "== completeness: permissive =="
sed 's/completeness: archive/completeness: permissive/' $CFG > /root/perm.yaml
cline router --config /root/perm.yaml --listen 0.0.0.0:27102 --log-file /var/log/cline/rp.log &
PP=$!; sleep 5
P() { mongosh --port 27102 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
HOTN=$(H 'print(db.getSiblingDB("shop").tprobe.countDocuments({}))')
chk "permissive returns the fast tier answer" "$HOTN" "$(P 'print(db.getSiblingDB("shop").tprobe.find({}).itcount())')"
kill $PP 2>/dev/null; wait $PP 2>/dev/null

kill $FP 2>/dev/null; wait $FP 2>/dev/null
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
