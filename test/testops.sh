#!/bin/bash
# Breadth of MongoDB operations through the router.
#
# The same battery runs twice: once with the fast tier complete, so answers come
# from it, and once with the watermark narrowed so everything falls through to
# the archive. Both passes must agree, which is the whole promise in one test.
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
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-44s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$MODE/$1"); printf '  FAIL  %-44s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

echo "== setup =="
pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/"; sleep 2
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})' >/dev/null
cline feeder --config $CFG --log-file $LOGDIR/ops_f.log & FP=$!
cline router --config $CFG --log-file $LOGDIR/ops_r.log & RP=$!
sleep 6

# 40 documents with a secondary date field and a few shapes to query on.
R "
const c = db.getSiblingDB(process.env.TC_DB).$NS;
const docs = [];
for (let i = 0; i < 40; i++) docs.push({
  _id: i, n: i, grp: ['a','b','c'][i%3], name: 'item-'+i,
  tags: ['t'+(i%4), 'all'], made: new Date(Date.UTC(2026,0,1+i)), nested: {v: i*2}
});
c.insertMany(docs);
print('seeded ' + c.countDocuments({}));" >/dev/null
sleep 5
echo "  seeded, archive has $(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

battery() {
  echo "-- $MODE --"
  chk "countDocuments all"        "40"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
  chk "countDocuments filtered"   "14"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({grp:'a'}))")"
  chk "find + itcount"            "40"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).itcount())")"
  chk "find with \$in"            "3"   "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({n:{\$in:[1,2,3]}}).itcount())")"
  chk "find with \$regex"         "4"   "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({name:{\$regex:'^item-1[0-3]\$'}}).itcount())")"
  chk "find on nested field"      "1"   "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({'nested.v':10}).itcount())")"
  chk "find on array member"      "10"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({tags:'t0'}).itcount())")"
  chk "find with date range"      "10"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({made:{\$gte:new Date(Date.UTC(2026,0,31))}}).itcount())")"
  chk "projection returns fields" "39"  "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.find({n:39},{name:1}).next(); print(d.name.split('-')[1])")"
  chk "sort desc, first doc"      "39"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).sort({n:-1}).limit(1).next().n)")"
  chk "sort by secondary field"   "0"   "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).sort({made:1}).limit(1).next().n)")"
  chk "skip + limit"              "5"   "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).sort({n:1}).skip(5).limit(1).next().n)")"
  chk "distinct"                  "3"   "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.distinct('grp').length)")"
  chk "aggregate \$group"         "3"   "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}}]).toArray().length)")"
  chk "aggregate \$match+\$count" "14"  "$(R "const r=db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$match:{grp:'a'}},{\$count:'n'}]).toArray(); print(r.length?r[0].n:0)")"
  chk "aggregate \$sort+\$limit"  "39"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$sort:{n:-1}},{\$limit:1}]).toArray()[0].n)")"
  chk "aggregate \$unwind"        "80"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$unwind:'\$tags'},{\$count:'n'}]).toArray()[0].n)")"
  chk "estimatedDocumentCount"    "40"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.estimatedDocumentCount())")"
  chk "cursor paging batchSize 7" "40"  "$(R "let n=0; const c=db.getSiblingDB(process.env.TC_DB).$NS.find({}).batchSize(7); while(c.hasNext()){c.next();n++} print(n)")"
  chk "explain runs"              "true" "$(R "print(!!db.getSiblingDB(process.env.TC_DB).$NS.find({n:1}).explain())")"
}

MODE="fast tier complete"
cline watermark --config $CFG --ns $DB.$NS --complete-all >/dev/null
sleep 7
battery

MODE="answered from the archive"
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
battery

echo "== fast tier genuinely incomplete: every read command must still be whole =="
# The two passes above both had the fast tier holding everything, so count and
# distinct would have looked right even unguarded. Make it actually short.
kill $RP $FP 2>/dev/null; wait $RP 2>/dev/null; wait $FP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({n:{\$gte:10}})" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
cline feeder --config $CFG --log-file $LOGDIR/ops_f.log & FP=$!
cline router --config $CFG --log-file $LOGDIR/ops_r.log & RP=$!
sleep 6
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
MODE="incomplete fast tier"
HOTN=$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")
TRUTH=$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")
echo "  fast tier holds $HOTN, archive holds $TRUTH"
chk "find itcount"              "$TRUTH" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).itcount())")"
chk "countDocuments"            "$TRUTH" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "estimatedDocumentCount"    "$TRUTH" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.estimatedDocumentCount())")"
chk "count command"             "$TRUTH" "$(R "print(db.getSiblingDB(process.env.TC_DB).runCommand({count:'$NS'}).n)")"
chk "distinct n"                "$TRUTH" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.distinct('n').length)")"
chk "count with a query"        "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({grp:'a',_tcDeletedAt:{\$exists:false}}))")" "$(R "print(db.getSiblingDB(process.env.TC_DB).runCommand({count:'$NS',query:{grp:'a'}}).n)")"
# A stats call counts documents too, and it was the one read that did not say so.
# $collStats and collStats were let through as metadata, on the grounds that they
# describe the collection rather than read it, so there is nothing for the answer
# to be short of. True of every field but the count. Measured on MongoDB's own
# sample datasets adopted as an archive: sample_training.trips reported 0 where
# the archive holds 10,000, because none of that collection was inside the hot
# window, while countDocuments on it answered 10,000 throughout.
chk "collStats count"           "$TRUTH" "$(R "print(db.getSiblingDB(process.env.TC_DB).runCommand({collStats:'$NS'}).count)")"
chk "\$collStats count"         "$TRUTH" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$collStats:{count:{}}}]).toArray()[0].count)")"
chk "\$collStats storageStats"  "$TRUTH" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$collStats:{storageStats:{}}}]).toArray()[0].storageStats.count)")"
chk "stats().count"             "$TRUTH" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.stats().count)")"
# And the other half: bytes on disk are a per-tier fact, so they must still be
# the fast tier's. Adding two clusters' compressed sizes together would produce
# a number describing nothing, and an operator sizing a fast tier reads this one.
#
# Let it settle first. Every read above that fell through copied documents up,
# and those land asynchronously, so storageSize is still growing while this
# section runs. These are two separately timed measurements of a moving number:
# under a parallel sweep a copy-up landed between them and the check reported
# 36864 against 20480, which is the collection growing and not the router
# changing anything. Alone it passed every time, which is the worst kind of
# flake.
settledStorage() {
  local a b
  a=$(H "print(db.getSiblingDB(process.env.TC_DB).runCommand({collStats:'$NS'}).storageSize)")
  for _ in $(seq 1 20); do
    sleep 1
    b=$(H "print(db.getSiblingDB(process.env.TC_DB).runCommand({collStats:'$NS'}).storageSize)")
    [ "$a" = "$b" ] && break
    a=$b
  done
  echo "$a"
}
chk "storage still describes the fast tier" \
    "$(settledStorage)" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).runCommand({collStats:'$NS'}).storageSize)")"

echo "== write paths, with the fast tier still short =="
# Deliberately left incomplete. Declaring it complete here would be a lie the
# router correctly believes, and updateMany would then quietly touch only the
# resident documents, which is the bug this pass exists to catch.
MODE="writes"
chk "updateOne"        "1"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:1},{\$set:{w:1}}).modifiedCount)")"
chk "updateMany"       "14" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.updateMany({grp:'a'},{\$set:{w:2}}).modifiedCount)")"
chk "replaceOne"       "1"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.replaceOne({_id:2},{_id:2,n:2,grp:'b',replaced:true}).modifiedCount)")"
chk "upsert creates"   "1"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:900},{\$set:{n:900}},{upsert:true}).upsertedCount)")"
chk "findAndModify"    "1"  "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOneAndUpdate({_id:1},{\$set:{fam:1}},{returnDocument:'after'}); print(d?d.fam:'null')")"
chk "findOneAndDelete" "901" "$(R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:901,n:901}); const d=db.getSiblingDB(process.env.TC_DB).$NS.findOneAndDelete({_id:901}); print(d?d._id:'null')")"
chk "findOneAndReplace" "true" "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOneAndReplace({_id:3},{_id:3,n:3,fr:true},{returnDocument:'after'}); print(d?!!d.fr:'null')")"
chk "deleteOne"        "1"  "$(R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:902,n:902}); print(db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:902}).deletedCount)")"
chk "deleteMany"       "2"  "$(R "db.getSiblingDB(process.env.TC_DB).$NS.insertMany([{_id:903,z:1},{_id:904,z:1}]); print(db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({z:1}).deletedCount)")"
chk "bulkWrite mixed"  "3"  "$(R "const r=db.getSiblingDB(process.env.TC_DB).$NS.bulkWrite([{insertOne:{document:{_id:905,n:905}}},{updateOne:{filter:{_id:905},update:{\$set:{b:1}}}},{deleteOne:{filter:{_id:905}}}]); print(r.insertedCount+r.modifiedCount+r.deletedCount)")"
chk "\$inc operator"   "43" "$(R "db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:4},{\$inc:{n:39}}); print(db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:4}).n)")"
chk "\$push to array"  "3"  "$(R "db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:5},{\$push:{tags:'new'}}); print(db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:5}).tags.length)")"

echo "== metadata commands =="
MODE="metadata"
chk "listCollections"  "true" "$(R "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes('$NS'))")"
chk "listIndexes"      "true" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.getIndexes().length >= 1)")"
chk "createIndex"      "true" "$(R "db.getSiblingDB(process.env.TC_DB).$NS.createIndex({grp:1}); print(db.getSiblingDB(process.env.TC_DB).$NS.getIndexes().some(i=>i.name=='grp_1'))")"
chk "dropIndex"        "false" "$(R "db.getSiblingDB(process.env.TC_DB).$NS.dropIndex('grp_1'); print(db.getSiblingDB(process.env.TC_DB).$NS.getIndexes().some(i=>i.name=='grp_1'))")"
# The count itself is checked against the archive above, where what the fast
# tier is short by is known. Here it is only that the command answers at all.
chk "collStats"        "true" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.stats().ok == 1)")"
chk "dbStats"          "true" "$(R "print(db.getSiblingDB(process.env.TC_DB).stats().db == '$DB')")"
chk "ping"             "1"    "$(R "print(db.adminCommand({ping:1}).ok)")"
chk "buildInfo"        "true" "$(R "print(!!db.adminCommand({buildInfo:1}).version)")"

echo "== writes reached the archive =="
sleep 6
chk "mirrored update"  "1"  "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:1}); print(d&&d.w?d.w:'no')")"
chk "mirrored replace" "true" "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:2}); print(d?!!d.replaced:'no')")"
chk "mirrored upsert"  "900" "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:900}); print(d?d.n:'no')")"

kill $RP $FP 2>/dev/null; wait $RP 2>/dev/null; wait $FP 2>/dev/null
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
