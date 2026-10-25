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
DB3=${DB3:-catdb}
META=${META:-_thermocline}
# mongosh is node, so the suites reach these through process.env rather than
# shell expansion. Half the eval strings are single-quoted at the shell, where a
# $VAR would not expand and would silently query the wrong database.
export TC_DB=$DB TC_DB2=$DB2 TC_DB3=$DB3 TC_META=$META
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

# routerUp is the same ping R makes, without the pipe, and it exists because a
# wait loop has to test the command's own status.
#
# R ends in `| tail -1`, so a pipeline's status is tail's and is zero however
# badly mongosh failed. Every `do R 'ping' && break` loop in this file therefore
# broke on its first iteration and waited for nothing at all: the router was
# given no time to come up, and whatever ran next met a closed port. On an idle
# box the router wins that race and nothing shows; under a parallel sweep it
# does not, and the result is a write failing into /dev/null and a later check
# reporting something else entirely. Measured: testindexes reported "carol
# reached the archive got=2 want=3" for a router that was still starting.
routerUp() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
  --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

echo "== setup =="
pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/"; sleep 2
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})' >/dev/null
# From now, not from where an earlier run stopped: resuming an hour and a half
# back on this busy oplog took the feeder longer than the first archive read
# waits, and that read was refused as behind.
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
cline feeder --config $CFG --log-file $LOGDIR/ops_f.log & FP=$!
cline router --config $CFG --log-file $LOGDIR/ops_r.log & RP=$!
# Until it answers, not a fixed six seconds: on 2026-09-28, with other
# containers loading the host, this router was not listening by then and the
# seed below went nowhere, which read as 51 failures of everything after it.
for _ in $(seq 1 60); do routerUp && break; sleep 1; done

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
# Until it answers, not a fixed six seconds: on 2026-09-28, with other
# containers loading the host, this router was not listening by then and the
# seed below went nowhere, which read as 51 failures of everything after it.
for _ in $(seq 1 60); do routerUp && break; sleep 1; done
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
# The third command of that family, and it was the one left. dataSize returns
# numObjects beside its bytes, and it names its collection in full rather than
# where every other command puts it, which is how it was missed. Measured on ten
# documents in the archive and three on the fast tier: countDocuments,
# collStats.count and dbStats.objects all answered ten and this answered three.
chk "dataSize numObjects"       "$TRUTH" \
    "$(R "print(Number(db.getSiblingDB(process.env.TC_DB).runCommand({dataSize:process.env.TC_DB + '.$NS'}).numObjects))")"
# And not for a key range, which is a question about where documents sit in one
# collection's index. That is what the balancer asks and it means nothing across
# two clusters, so it stays the fast tier's: a number that means something else
# would be worse than one that is short.
chk "  but not over a key range" "true" \
    "$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({dataSize:process.env.TC_DB + '.$NS',
          keyPattern:{_id:1}, min:{_id:MinKey()}, max:{_id:MaxKey()}});
          print(r.ok === 1 && r.numObjects <= $TRUTH)")"
# And the other half: bytes on disk are a per-tier fact, so they must still be
# the fast tier's. Adding two clusters' compressed sizes together would produce
# a number describing nothing, and an operator sizing a fast tier reads this one.
#
# Not by equality, which is what this asked for twice and lost twice. Every read
# above that fell through copied documents up, those land asynchronously, and
# WiredTiger moves storageSize on its own at a checkpoint, so two separately
# timed readings of it are two measurements of a moving number. Waiting for it to
# settle was the first attempt and it still failed under a parallel sweep: 36864
# against 20480, which is the collection growing and not the router changing
# anything.
#
# What the check is actually for is one specific wrong answer: the router adding
# two clusters' compressed sizes together, which an operator sizing a fast tier
# would read as its own. That answer is not a checkpoint out, it is the archive
# out: ten thousand documents against a handful. So the assertion is that the
# router's number is the fast tier's order of magnitude and nowhere near the sum.
HOTSZ=$(H "print(db.getSiblingDB(process.env.TC_DB).runCommand({collStats:'$NS'}).storageSize)")
COLDSZ=$(C "print(db.getSiblingDB(process.env.TC_DB).runCommand({collStats:'$NS'}).storageSize)")
RSZ=$(R "print(db.getSiblingDB(process.env.TC_DB).runCommand({collStats:'$NS'}).storageSize)")
chk "storage still describes the fast tier" "true" \
    "$(awk -v r="$RSZ" -v h="$HOTSZ" -v c="$COLDSZ" 'BEGIN{
        if (r+0 <= 0 || h+0 <= 0) { print "false (router " r ", fast tier " h ")"; exit }
        if (r+0 > 4*(h+0)) { print "false (router " r " against fast tier " h ")"; exit }
        if (r+0 >= (h+0)+(c+0)) { print "false (router " r " is the sum of " h " and " c ")"; exit }
        print "true" }')"

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

echo "== a write's own maxTimeMS, on the work this system adds =="
# The copy-up is work the client cannot see and did not agree to, and it used to
# be unbounded: measured on 3,000 documents held by the archive alone, an update
# asking for 1ms came back in 595ms and the same update on an untiered
# collection came back in 1ms. The error code was right both times, which is why
# the code alone cannot tell this apart: what was wrong was that the deadline
# meant nothing. The read path had always honoured it, in withRemainingTime.
#
# The mechanism is what is checked here rather than the clock, because a timing
# bound under a parallel sweep measures the box. The router says this line only
# when the deadline is what ended the copy-up, which cannot happen unless the
# copy-up is carrying the deadline at all.
# grep -c prints 0 and exits 1 when nothing matches, so `|| echo 0` makes this
# two lines and every comparison against it an "integer expression expected".
BEFORE=$(grep -c "own maxTimeMS ran out" "$LOGDIR/ops_r.log" 2>/dev/null); BEFORE=${BEFORE:-0}
# Asserted first, because a copy-up that is already done has no deadline to
# miss and the check below would pass for the wrong reason.
chk "the fast tier is short of these to begin with" "true" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({grp:'c'}))" | awk '{print ($1+0 < 13) ? "true" : "false"}')"
# With the feeder stopped and a write after it, so the freshness barrier
# cannot clear and the copy-up is certain to meet the deadline. Left as it was,
# this measured the barrier's speed instead: once the barrier answered a caught
# up archive in a hello's round trip, the copy-up of thirteen documents fitted
# in the budget, the write went on to mongod with its own 1ms, and mongod cut
# it after 12 documents, which is what one cluster does with a multi-document
# update out of time. Three runs in four on the Docker test box. A stopped
# feeder alone is not enough: with nothing written since its last position the
# barrier rightly says the archive is current.
kill -STOP $FP
R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:'deadline-marker'})" >/dev/null
EXPIRED=$(R "try { db.getSiblingDB(process.env.TC_DB).runCommand({update: '$NS',
    updates: [{q: {grp: 'c'}, u: {\$set: {mt: 1}}, multi: true}], maxTimeMS: 1});
    print('accepted') } catch (e) { print(e.code) }")
# Every write path that brings archived documents up shares the deadline, and
# they have to answer alike: the same condition reaching a driver as
# MaxTimeMSExpired from an update and as CommandFailed from a bulkWrite is a
# difference the application would have to know about this proxy to handle. The
# first version taught only the multi-document path.
# The upsert path is a different caller of the same copy-up, so it is the one
# worth asking. A filter that matches archived documents and would otherwise
# create a stub over them.
UPS=$(R "try { db.getSiblingDB(process.env.TC_DB).runCommand({update: '$NS',
    updates: [{q: {grp: 'c', n: {\$gte: 0}}, u: {\$set: {mt3: 1}}, upsert: true}], maxTimeMS: 1});
    print('accepted') } catch (e) { print(e.code) }")
kill -CONT $FP
chk "a deadlined write says MaxTimeMSExpired"  "50" "$EXPIRED"
sleep 1
AFTER=$(grep -c "own maxTimeMS ran out" "$LOGDIR/ops_r.log" 2>/dev/null); AFTER=${AFTER:-0}
chk "and the copy-up is what the deadline cut" "true" \
    "$([ "$AFTER" -gt "$BEFORE" ] 2>/dev/null && echo true || echo false)"
# Nothing applied, because the write never ran: a deadline that stops half a
# multi-document write would be worse than one that means nothing.
chk "and nothing was half applied"             "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({mt:1}))")"
chk "the upsert path says the same thing"      "50" "$UPS"
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:'deadline-marker'})" >/dev/null
# And the ordinary case is untouched: no deadline, the copy-up runs, every
# matching document is reached.
WANT=$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({grp:'c'}))")
chk "a write with no deadline still reaches them" "$WANT" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.updateMany({grp:'c'},{\$set:{mt2:1}}).modifiedCount)")"

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

echo "== dbStats over a database the fast tier holds part of =="
# collStats counts the collection and dbStats counts the database, and only one
# of them was corrected. The principle behind the first is that a count is a
# logical fact about the data while bytes on disk are a per-tier one; objects is
# the same field one level up and it was the fast tier's.
#
# Measured before this, on a database whose one tiered collection the archive
# holds seven documents of and the fast tier had just been created empty for:
# collStats answered 7, countDocuments answered 7, and db.stats().objects
# answered **0**. A database holding seven documents reported none. That is the
# adoption's ordinary state rather than a corner, because the fast tier starts
# empty and fills.
#
# In its own database, because dbStats totals all of one and the check is only
# deterministic where nothing else writes. catdb is that database here, and the
# suites that use it are the ones that drop it whole.
SDB=$DB3
H "db.getSiblingDB(process.env.TC_DB3).dropDatabase()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB3).dropDatabase()" >/dev/null
# tdbstats: ten in the archive and three on the fast tier, which is a warmed
# window. tdbstats2: five in the archive and none copied up, which is a
# collection entirely outside it.
C "db.getSiblingDB(process.env.TC_DB3).tdbstats.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i})));
   db.getSiblingDB(process.env.TC_DB3).tdbstats2.insertMany(Array.from({length:5},(_,i)=>({_id:i,n:i})))" >/dev/null
H "db.getSiblingDB(process.env.TC_DB3).tdbstats.insertMany(Array.from({length:3},(_,i)=>({_id:i,n:i})))" >/dev/null
# The router is restarted onto the seeded archive, which is the adoption arriving
# in the right order: the archive is prepared and then this is pointed at it. It
# creates the fast tier's copy of every namespace the archive holds as it starts,
# so tdbstats2 exists there and holds nothing, which is the state being measured.
# Without that a collection the fast tier has never heard of makes collStats
# error rather than answer, which is a different subject.
kill $RP 2>/dev/null; wait $RP 2>/dev/null
cline router --config $CFG --log-file $LOGDIR/ops_r.log & RP=$!
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
# Number() because mongod answers dbStats with an int64 and mongosh prints one
# as Long('15'). The count is what this compares, not how the shell renders it.
chk "the fast tier's own dbStats counts what it holds" "3" \
    "$(H "print(Number(db.getSiblingDB(process.env.TC_DB3).stats().objects))")"
chk "dbStats through the router counts the database"   "15" \
    "$(R "print(Number(db.getSiblingDB(process.env.TC_DB3).stats().objects))")"
chk "and agrees with collStats on the same collection" "10" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB3).tdbstats.stats().count)")"
chk "and with the one nothing was copied up for"       "5" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB3).tdbstats2.stats().count)")"
# The other half, and the reason only one field moves: bytes on disk are a
# per-tier fact. The router's storage figures must still be the fast tier's, so
# an operator sizing one reads its own number and not two clusters added up.
#
# Bracketed rather than compared to one reading, because storageSize is a
# physical number and the two calls are not one instant. WiredTiger allocates
# and checkpoints on its own schedule, and under a parallel sweep the gap
# between these is long enough for it: measured on 2026-09-12, the router
# answered 8192 and the fast tier answered 12288 a moment later, and the check
# reported the router summing two clusters when it had done nothing of the kind.
#
# So the fast tier is asked on both sides of the router's call and the router's
# answer has to fall between them. That is the contract -- the number is the
# fast tier's own -- and it is what can be held still. The archive's is printed
# with a failure, because the mistake this guards against would show up as the
# sum of the two.
DBSZ_H1=$(H "print(db.getSiblingDB(process.env.TC_DB3).stats().storageSize)")
DBSZ_R=$(R "print(db.getSiblingDB(process.env.TC_DB3).stats().storageSize)")
DBSZ_H2=$(H "print(db.getSiblingDB(process.env.TC_DB3).stats().storageSize)")
DBSZ_LO=$(( DBSZ_H1 < DBSZ_H2 ? DBSZ_H1 : DBSZ_H2 ))
DBSZ_HI=$(( DBSZ_H1 > DBSZ_H2 ? DBSZ_H1 : DBSZ_H2 ))
chk "storageSize is the fast tier's, not the two added up" "true" \
    "$([ "$DBSZ_R" -ge "$DBSZ_LO" ] && [ "$DBSZ_R" -le "$DBSZ_HI" ] 2>/dev/null && echo true \
       || echo "false (router $DBSZ_R, fast tier $DBSZ_LO..$DBSZ_HI, archive $(C "print(db.getSiblingDB(process.env.TC_DB3).stats().storageSize)"))")"
H "db.getSiblingDB(process.env.TC_DB3).dropDatabase()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB3).dropDatabase()" >/dev/null

echo "== writes reached the archive =="
sleep 6
chk "mirrored update"  "1"  "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:1}); print(d&&d.w?d.w:'no')")"
chk "mirrored replace" "true" "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:2}); print(d?!!d.replaced:'no')")"
chk "mirrored upsert"  "900" "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:900}); print(d?d.n:'no')")"

kill $RP $FP 2>/dev/null; wait $RP 2>/dev/null; wait $FP 2>/dev/null
echo "== dbHash over a collection the fast tier is short of =="
# Every other command that reads documents is either checked for completeness
# and answered from the archive, or is a physical fact about one cluster and
# right to be. dbHash is neither: it reads every document and returns one hash,
# and a hash cannot be corrected across two tiers the way a count can.
#
# Found by asking eleven document-reading commands of a collection the fast tier
# held three of ten of, with the archive as the control. Nine agreed. This
# answered a different hash with nothing to say it had seen a third of the
# collection, which is the worst possible answer for the command people use to
# compare two copies of a dataset.
DHNS=tdbhash
# A router of this section's own: the sections above leave none running, and
# without one every check here reads a connection error as an answer. That is
# how the first version of this reported the product refusing nothing.
pkill -f "cline router --config $WDIR/" 2>/dev/null; sleep 1
cline router --config $CFG --log-file $LOGDIR/ops_dbhash_r.log & DHRP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "a router is up for this section"         "1" \
    "$(R 'print(db.adminCommand({ping:1}).ok)')"
C "db.getSiblingDB(process.env.TC_DB).$DHNS.drop();
   db.getSiblingDB(process.env.TC_DB).$DHNS.insertMany([{_id:1,n:1},{_id:2,n:2},{_id:3,n:3}])" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$DHNS.drop();
   db.getSiblingDB(process.env.TC_DB).$DHNS.insertOne({_id:1,n:1})" >/dev/null
cline watermark --config $CFG --ns $DB.$DHNS --clear >/dev/null 2>&1
chk "the fast tier holds one of three"        "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$DHNS.countDocuments({}))")"
chk "dbHash naming it is refused"             "REFUSED" \
    "$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({dbHash:1,collections:['$DHNS']}); print(r.ok===1?'ANSWERED':'ANSWERED')" 2>&1 | grep -q 'refusing this dbHash' && echo REFUSED || echo ANSWERED)"
chk "and the whole database form too"         "REFUSED" \
    "$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({dbHash:1}); print('ANSWERED')" 2>&1 | grep -q 'refusing this dbHash' && echo REFUSED || echo ANSWERED)"
# And allowed when it means what it says, which is the other half: a hash of a
# collection the fast tier holds in full is the collection's hash, and it has to
# equal the archive's.
H "db.getSiblingDB(process.env.TC_DB).$DHNS.drop();
   db.getSiblingDB(process.env.TC_DB).$DHNS.insertMany([{_id:1,n:1},{_id:2,n:2},{_id:3,n:3}])" >/dev/null
cline watermark --config $CFG --ns $DB.$DHNS --complete-all --force >/dev/null 2>&1
sleep 7
chk "with a complete claim it answers"        "$(C "print(db.getSiblingDB(process.env.TC_DB).runCommand({dbHash:1,collections:['$DHNS']}).collections.$DHNS)")" \
    "$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({dbHash:1,collections:['$DHNS']}); print(r.ok===1 ? r.collections.$DHNS : 'REFUSED')")"
H "db.getSiblingDB(process.env.TC_DB).$DHNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DHNS.drop()" >/dev/null
cline watermark --config $CFG --ns $DB.$DHNS --clear >/dev/null 2>&1
kill $DHRP 2>/dev/null; wait $DHRP 2>/dev/null

echo "== filemd5 over a GridFS file the fast tier is short of =="
# The same defect as dbHash with a file instead of a collection, and GridFS is
# exactly the shape this system tiers: a bucket stores a file as a row per chunk
# in <root>.chunks, that collection only grows, and old files are read rarely,
# so an old file's chunks are the ones the evictor moves.
#
# Measured on a four chunk file with two evicted and both still live in the
# archive: 3b5d3c7d... over 4 chunks while it was all resident, 70bc8f4b... over
# 2 chunks afterwards, ok on both. An application verifying a file it stored
# reads the second as the file being corrupt.
FMNS=fs.chunks
H "db.getSiblingDB(process.env.TC_DB).fs.chunks.drop(); db.getSiblingDB(process.env.TC_DB).fs.files.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).fs.chunks.drop(); db.getSiblingDB(process.env.TC_DB).fs.files.drop()" >/dev/null
# The fast tier holds two of the file's four chunks, the archive all four, which
# is a file half evicted. Written at each tier directly, so no feeder is needed
# and the state is exactly the one being asked about.
# A fixed id rather than one captured from the shell. The first version of this
# generated one and read it back with print(oid.valueOf()), which mongosh
# renders as ObjectId('...') rather than the bare hex, so every check after it
# built ObjectId('ObjectId('...')') and failed on a syntax error. A literal both
# sides can spell needs no capture at all.
FID=00000000000000000000fa11
H "const oid=ObjectId('$FID'); const d=db.getSiblingDB(process.env.TC_DB);
   d.fs.files.insertOne({_id:oid,length:1024,chunkSize:256,uploadDate:new Date(),filename:'f.bin'});
   for (let n=0;n<2;n++) d.fs.chunks.insertOne({_id:ObjectId(),files_id:oid,n:n,data:BinData(0,'AAAAAAAAAAAAAAAAAAAAAA==')});
   d.fs.chunks.createIndex({files_id:1,n:1})" >/dev/null
C "const oid=ObjectId('$FID'); const d=db.getSiblingDB(process.env.TC_DB);
   d.fs.files.insertOne({_id:oid,length:1024,chunkSize:256,uploadDate:new Date(),filename:'f.bin'});
   for (let n=0;n<4;n++) d.fs.chunks.insertOne({_id:ObjectId(),files_id:oid,n:n,data:BinData(0,'AAAAAAAAAAAAAAAAAAAAAA==')});
   d.fs.chunks.createIndex({files_id:1,n:1})" >/dev/null
cline router --config $CFG --log-file $LOGDIR/ops_filemd5_r.log & FMRP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "a router is up for this section"         "1" \
    "$(R 'print(db.adminCommand({ping:1}).ok)')"
chk "the fast tier holds two of four chunks"  "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).fs.chunks.countDocuments({files_id:ObjectId('$FID')}))")"
chk "filemd5 on the half file is refused"     "REFUSED" \
    "$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({filemd5:ObjectId('$FID'),root:'fs'}); print(r.ok===1?'ANSWERED':'ANSWERED')" 2>&1 | grep -q 'refusing this filemd5' && echo REFUSED || echo ANSWERED)"
# And the control, which decides whether the guard is worth having: a file whose
# chunks are all on the fast tier still hashes, and its hash is the file's.
FID2=00000000000000000000fa22
H "const oid=ObjectId('$FID2'); const d=db.getSiblingDB(process.env.TC_DB);
   d.fs.files.insertOne({_id:oid,length:512,chunkSize:256,uploadDate:new Date(),filename:'whole.bin'});
   for (let n=0;n<2;n++) d.fs.chunks.insertOne({_id:ObjectId(),files_id:oid,n:n,data:BinData(0,'AAAAAAAAAAAAAAAAAAAAAA==')})" >/dev/null
C "const oid=ObjectId('$FID2'); const d=db.getSiblingDB(process.env.TC_DB);
   d.fs.files.insertOne({_id:oid,length:512,chunkSize:256,uploadDate:new Date(),filename:'whole.bin'});
   for (let n=0;n<2;n++) d.fs.chunks.insertOne({_id:ObjectId(),files_id:oid,n:n,data:BinData(0,'AAAAAAAAAAAAAAAAAAAAAA==')})" >/dev/null
chk "a file with every chunk resident answers" "true" \
    "$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({filemd5:ObjectId('$FID2'),root:'fs'}); print(r.ok===1 && r.numChunks===2)")"
kill $FMRP 2>/dev/null; wait $FMRP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).fs.chunks.drop(); db.getSiblingDB(process.env.TC_DB).fs.files.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).fs.chunks.drop(); db.getSiblingDB(process.env.TC_DB).fs.files.drop()" >/dev/null

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
