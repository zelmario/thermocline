#!/bin/bash
# Differential conformance across the MongoDB surface an application uses.
#
# The promise this system makes is that no application changes. That is not a
# promise about any one feature, it is a promise that the answer is the one you
# would have had without it, so the honest way to test it is to ask for both
# answers and compare.
#
# Two collections hold identical documents. shop.tcref is ordinary and untiered,
# so the router passes it straight through and it is the reference. shop.tcon is
# tiered, half its documents sit only in the archive, and its watermark says the
# fast tier is incomplete, so every read has to be resolved across both tiers.
# Each operation runs against both and the two results must match exactly.
#
# Almost nothing here asserts a value written by hand. A check that fails is the
# router disagreeing with MongoDB, which is the only definition of a bug that
# matters for a drop-in proxy. The few checks that cannot be differential, the
# ones whose answer is random or is the current time, say so where they are.
#
# Both tiers of tcon get the same secondary indexes, because that is the
# documented deployment: see "Indexes are not copied, in either direction" in
# README, and `cline indexes` for the divergence report.
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
# reseed opens the archive from inside mongosh rather than as a second process,
# so the URI has to reach the JS. After COLD is set, not before: exported empty
# it would be Mongo(""), and the reseed would fail for a reason that reads like
# an unreachable archive.
export TC_COLD=$COLD
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-40s %.34s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-40s\n          ref=%.150s\n          got=%.150s\n' "$1" "$2" "$3"; fi
}
R()    { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
# The same connection without tail, for a script whose output has sections and
# is parsed by the caller. tail -1 is right for one answer and destroys two.
RALL() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

# The heart of the suite. The expression names its collection as COLL; it runs
# once against the reference and once against the tiered collection, both through
# the router, and the answers must agree.
# Both halves run in one mongosh, not two. mongosh is node, and each invocation
# costs about a second of process startup, SCRAM auth and topology discovery
# before it does any work at all; the database work here is twenty documents.
# The reference and the tiered collection are still two separate runs of the
# expression through the router, in the same order as before, and each answer is
# still the last line that half printed, which is what tail -1 gave.
#
# One thing does change: the halves now share a connection, so they share an
# implicit session, where before each had its own process and its own. That
# makes the tiered half causally after the reference half rather than
# independent of it, which is stricter than it was and cannot hide a stale
# answer. The transaction checks are unaffected: they call R directly and open
# their own explicit sessions.
same() {
  local label="$1" expr="$2" out ref got
  out=$(RALL "const d=db.getSiblingDB(process.env.TC_DB);
           function run(c) { try { ${expr//COLL/c} } catch(e) { print('ERR '+e.message.substring(0,70)) } }
           run(d.tcref);
           print('__TCON__');
           run(d.tcon);" )
  ref=$(sed -n '1,/^__TCON__$/p' <<<"$out" | grep -v '^__TCON__$' | tail -1)
  got=$(sed -n '/^__TCON__$/,$p'  <<<"$out" | grep -v '^__TCON__$' | tail -1)
  chk "$label" "$ref" "$got"
}
# For writes, which need both collections put back first.
samew() { reseed; same "$1" "$2"; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/conf_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/conf_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}

# Twenty documents shaped to exercise every operator in the list: numbers, a
# string, an array of scalars, an array of subdocuments, a subdocument, a date,
# a point, a sentence for text search, and a field only some of them carry.
DOCS='Array.from({length:20},(_,i)=>({
  _id: i+1,
  n: i+1,
  f: (i+1)/4,
  name: "user" + String(i+1).padStart(2,"0"),
  grp: ["alpha","beta"][i%2],
  tags: ["t"+((i%3)+1), "common"],
  items: [{sku:"s"+(i%4), qty:i+1},{sku:"s9", qty:2}],
  meta: {score: (i*7)%13, active: i%4!==0},
  words: ["red apple pie","blue berry tart","green kiwi cake"][i%3],
  loc: {type:"Point", coordinates:[(i%10)-5, (i%7)-3]},
  when: new Date(Date.UTC(2024,i%12,1+(i%27),13,45,30,250))
}))'
OPT='[3,6,9,12,15,18]'

reseed() {
  RESEED=$(mongosh "$HOT" --quiet --eval "
    const hot  = db.getSiblingDB(process.env.TC_DB);
    const cold = Mongo(process.env.TC_COLD).getDB(process.env.TC_DB);

    function mkIndexes(c) {
      c.createIndex({words:'text'});
      c.createIndex({loc:'2dsphere'});
      c.createIndex({n:1}); c.createIndex({grp:1}); c.createIndex({when:1});
      // Two selective indexes, for the hint checks below. A sparse index holds
      // only the documents carrying the field, and a partial one only those its
      // filter matches, so hinting either is a statement about which documents
      // are looked at and not merely about how.
      c.createIndex({opt:1},{sparse:true,name:'conf_sparse'});
      c.createIndex({f:1},{partialFilterExpression:{n:{\$gt:10}},name:'conf_partial'});
    }

    const all = $DOCS;
    hot.tcref.drop();
    hot.tcref.insertMany(all);
    hot.tcref.updateMany({_id:{\$in:$OPT}},{\$set:{opt:'yes'}});

    hot.tcon.drop();
    cold.tcon.drop();
    // 1 to 10 on both tiers, 11 to 20 only in the archive, which is what an
    // evicted document looks like.
    hot.tcon.insertMany(all.slice(0,10));
    // Upserts, not insertMany. A late mirror of the fast tier inserts above can
    // put documents here first, and an ordered insert then abandons the whole
    // seed on the first duplicate. This states the archive's contents rather
    // than assuming it is empty, and replacing the whole document also clears
    // any tombstone.
    cold.tcon.bulkWrite(
      all.map(d=>({replaceOne:{filter:{_id:d._id}, replacement:d, upsert:true}})),
      {ordered:false});
    hot.tcon.updateMany({_id:{\$in:$OPT}},{\$set:{opt:'yes'}});
    cold.tcon.updateMany({_id:{\$in:$OPT}},{\$set:{opt:'yes'}});

    mkIndexes(hot.tcref);
    mkIndexes(hot.tcon);
    mkIndexes(cold.tcon);

    // waitFeederPast, inlined. It blocks until the feeder has processed
    // everything written to the fast tier so far, proved by a sentinel document
    // reaching the archive. The feeder runs throughout this suite and is always
    // some way behind, and two things above depend on it having caught up. Both
    // used to be a sleep 1 that was usually but not always long enough.
    //
    // The archive seed raced the feeder's mirror of the previous reseed's fast
    // tier inserts. When that mirror landed after this reseed had dropped the
    // archive collection, the archive already held _id 1, and an ordered
    // insertMany aborts on the first duplicate without inserting any of the
    // twenty. The archive was left holding ten documents rather than twenty, so
    // half the collection was missing from every answer and checks failed with
    // nothing wrong with the product. It hit a different handful each run.
    //
    // The completeness claim raced the drop. The feeder withdraws the claim when
    // it sees a collection dropped on the fast tier, which is correct, but
    // reseed writes the claim moments later and the withdrawal can land after it.
    hot.tcon.insertOne({_id:'__sentinel'});
    let caught = false;
    for (let i = 0; i < 240; i++) {
      if (cold.tcon.countDocuments({_id:'__sentinel'}) === 1) { caught = true; break; }
      sleep(500);
    }
    hot.tcon.deleteOne({_id:'__sentinel'});
    cold.tcon.deleteOne({_id:'__sentinel'});
    print(caught ? 'RESEEDED' : 'FEEDER-NEVER-CAUGHT-UP');
  " 2>&1 | tail -1)
  # Said out loud rather than inferred. A reseed that silently half happened is
  # the shape of failure this suite is least able to tell from a product bug.
  [ "$RESEED" = "RESEEDED" ] && : || echo "  WARN  reseed: $RESEED"
  # The claim goes last, after the feeder has seen the drop and withdrawn the
  # previous one. Writing it before that is what made the claim vanish.
  cline watermark --config $CFG --ns $DB.tcon --complete-from 2030-01-01T00:00:00Z >/dev/null
}

echo "== setup =="
stopDaemons
C "db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({}); db.getSiblingDB(process.env.TC_META).evicting.deleteMany({})" >/dev/null
startFeeder
startRouter
reseed
sleep 7
chk "the reference holds twenty"                "20" "$(R "print(db.getSiblingDB(process.env.TC_DB).tcref.countDocuments({}))")"
chk "the tiered fast tier holds ten"            "10" "$(H "print(db.getSiblingDB(process.env.TC_DB).tcon.countDocuments({}))")"
chk "its archive holds twenty"                  "20" "$(C "print(db.getSiblingDB(process.env.TC_DB).tcon.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

S='.sort({_id:1}).toArray()))'
echo
echo "== CRUD reads =="
same 'find()'                 "print(JSON.stringify(COLL.find({})$S"
same 'findOne()'              'print(JSON.stringify(COLL.findOne({_id:17})))'
same 'find() projection'      "print(JSON.stringify(COLL.find({},{name:1,n:1})$S"

echo
echo "== hints, which are not only about the plan =="
# A hint on an ordinary index picks how to answer and cannot change the answer.
# A hint on a sparse or partial index also decides *which documents are looked
# at*: measured on PSMDB 8.0, a sparse index on a field six of twenty documents
# carry returns those six and nothing else. So a tier that drops the hint
# answers a different question, and answers it as though nothing happened.
#
# These are differential like everything else here, so they say nothing about
# what the right answer is, only that both collections have to give it.
# The find options, which are where this suite had a gap. Every one of these is
# a flag the router reads or has to pass through untouched, and none of them was
# asked of a collection split across two tiers. returnKey is the interesting one:
# it answers with index keys rather than documents, so a router that resolves a
# read by looking documents up by _id has nothing to look up.
same 'returnKey on an index'   "print(JSON.stringify(COLL.find({n:{\$gt:5}}).returnKey().hint({n:1}).toArray()))"
same 'returnKey with no index' "print(JSON.stringify(COLL.find({}).returnKey().toArray()))"
same 'singleBatch with a limit' "print(JSON.stringify(COLL.find({}).limit(3).batchSize(1000)$S"
# A negative limit and a negative skip are refused by mongod, and its refusal
# runs to several lines. same() compares the last line of each side, so the
# message is flattened to its first line here: two different errors would still
# differ, and a refusal against an answer certainly would.
same 'a negative limit'        "try{print(JSON.stringify(d.runCommand({find:c.getName(),filter:{},limit:-3,sort:{_id:1}}).cursor.firstBatch))}catch(e){print('ERR '+e.message.split('\n')[0].substring(0,60))}"
same 'a negative skip'         "try{print(JSON.stringify(d.runCommand({find:c.getName(),filter:{},skip:-1}).cursor.firstBatch))}catch(e){print('ERR '+e.message.split('\n')[0].substring(0,60))}"
same 'batchSize zero'          "print(JSON.stringify(d.runCommand({find:c.getName(),filter:{},batchSize:0}).cursor.firstBatch))"
same 'skip past the end'       "print(JSON.stringify(COLL.find({}).skip(1000)$S"
same 'allowDiskUse on a find'  "print(JSON.stringify(d.runCommand({find:c.getName(),filter:{},allowDiskUse:true,sort:{_id:1}}).cursor.firstBatch.length))"
same 'tailable on a normal collection' "try{print('n='+d.runCommand({find:c.getName(),filter:{},tailable:true}).cursor.firstBatch.length)}catch(e){print('ERR '+e.message.split('\n')[0].substring(0,60))}"
# showRecordId's value is a storage engine's own address, so the two collections
# cannot agree on it and must not be asked to. What has to match is that the
# field is there at all, on every document, whichever tier answered.
same 'showRecordId is present' "print(COLL.find({}).showRecordId().toArray().every(x => x.\$recordId !== undefined))"
same 'hint an ordinary index'  "print(JSON.stringify(COLL.find({n:{\$gt:5}}).hint({n:1})$S"
same 'hint a sparse index'     "print(JSON.stringify(COLL.find({}).hint('conf_sparse')$S"
same 'hint a sparse index by key' "print(JSON.stringify(COLL.find({}).hint({opt:1})$S"
same 'count through a sparse hint' "print(COLL.find({}).hint('conf_sparse').itcount())"
same 'hint a partial index'    "print(JSON.stringify(COLL.find({}).hint('conf_partial')$S"
same 'count through a partial hint' "print(COLL.find({}).hint('conf_partial').itcount())"
# An index recreated with a different filter is the case the router's own cache
# gets wrong. It remembers which indexes are selective, and reproduces their
# filter against the archive because the archive does not have the index. That
# listing is reused for a minute, so a partial index dropped and put back
# without its filter left the router asking the archive a narrower question than
# the client asked, and answering short. Both collections are changed the same
# way, so the comparison is still differential.
RECREATE="c.dropIndex('conf_partial'); c.createIndex({f:1},{name:'conf_partial'});"
RALL "const d=db.getSiblingDB(process.env.TC_DB);
      for (const c of [d.tcref, d.tcon]) { $RECREATE }" >/dev/null
same 'hint a partial index made plain' "print(JSON.stringify(COLL.find({}).hint('conf_partial')$S"
same 'count through it'                "print(COLL.find({}).hint('conf_partial').itcount())"
# And back, so nothing after this inherits the changed shape.
RALL "const d=db.getSiblingDB(process.env.TC_DB);
      for (const c of [d.tcref, d.tcon]) {
        c.dropIndex('conf_partial');
        c.createIndex({f:1},{partialFilterExpression:{n:{\$gt:10}},name:'conf_partial'});
      }" >/dev/null

# min and max are bounds on an index key, so they mean nothing without the index
# that defines the key, and MongoDB has required a hint alongside them since
# 4.2. Dropping that hint does not cost a plan, it turns a working query into
# NoQueryExecutionPlans.
same 'min and max with a hint'  "print(JSON.stringify(COLL.find({}).min({n:5}).max({n:12}).hint({n:1})$S"
same 'min alone with a hint'    "print(JSON.stringify(COLL.find({}).min({n:15}).hint({n:1})$S"

echo
echo "== query and filtering =="
same '$eq'        "print(JSON.stringify(COLL.find({n:{\$eq:17}})$S"
same '$ne'        "print(JSON.stringify(COLL.find({grp:{\$ne:'alpha'}})$S"
same '$gt'        "print(JSON.stringify(COLL.find({n:{\$gt:14}})$S"
same '$gte'       "print(JSON.stringify(COLL.find({n:{\$gte:14}})$S"
same '$lt'        "print(JSON.stringify(COLL.find({n:{\$lt:6}})$S"
same '$lte'       "print(JSON.stringify(COLL.find({n:{\$lte:6}})$S"
same '$in'        "print(JSON.stringify(COLL.find({n:{\$in:[2,12,19]}})$S"
same '$nin'       "print(JSON.stringify(COLL.find({n:{\$nin:[2,12,19]}})$S"
same '$and'       "print(JSON.stringify(COLL.find({\$and:[{n:{\$gt:5}},{grp:'beta'}]})$S"
same '$or'        "print(JSON.stringify(COLL.find({\$or:[{n:2},{n:18}]})$S"
same '$nor'       "print(JSON.stringify(COLL.find({\$nor:[{grp:'alpha'},{n:{\$lt:5}}]})$S"
same '$not'       "print(JSON.stringify(COLL.find({n:{\$not:{\$gt:10}}})$S"
same '$exists'    "print(JSON.stringify(COLL.find({opt:{\$exists:true}})$S"
same '$exists false' "print(JSON.stringify(COLL.find({opt:{\$exists:false}})$S"
same '$type'      "print(JSON.stringify(COLL.find({when:{\$type:'date'}})$S"
same '$type array' "print(JSON.stringify(COLL.find({tags:{\$type:'array'}})$S"
same '$regex'     "print(JSON.stringify(COLL.find({name:{\$regex:'^user1'}})$S"
same '$regex opts' "print(JSON.stringify(COLL.find({name:{\$regex:'USER0[1-3]',\$options:'i'}})$S"
same '$text'      "print(JSON.stringify(COLL.find({\$text:{\$search:'berry'}}).sort({_id:1}).toArray().map(d=>d._id)))"
same '$expr'      "print(JSON.stringify(COLL.find({\$expr:{\$gt:['\$n',{\$multiply:['\$f',3]}]}})$S"
same '$all'       "print(JSON.stringify(COLL.find({tags:{\$all:['t2','common']}})$S"
same '$elemMatch' "print(JSON.stringify(COLL.find({items:{\$elemMatch:{sku:'s1',qty:{\$gt:3}}}})$S"
same '$size'      "print(JSON.stringify(COLL.find({tags:{\$size:2}})$S"
same 'nested field' "print(JSON.stringify(COLL.find({'meta.active':true})$S"

echo
echo "== geospatial =="
same '$geoWithin'    "print(JSON.stringify(COLL.find({loc:{\$geoWithin:{\$centerSphere:[[0,0],0.2]}}}).sort({_id:1}).toArray().map(d=>d._id)))"
same '$geoIntersects' "print(JSON.stringify(COLL.find({loc:{\$geoIntersects:{\$geometry:{type:'Point',coordinates:[0,0]}}}}).sort({_id:1}).toArray().map(d=>d._id)))"
same '$near'         "print(JSON.stringify(COLL.find({loc:{\$near:{\$geometry:{type:'Point',coordinates:[0,0]},\$maxDistance:500000}}}).toArray().map(d=>d._id)))"
same '$nearSphere'   "print(JSON.stringify(COLL.find({loc:{\$nearSphere:{\$geometry:{type:'Point',coordinates:[0,0]},\$maxDistance:500000}}}).toArray().map(d=>d._id)))"

echo
echo "== query result operations =="
same 'sort() ascending'   'print(JSON.stringify(COLL.find({}).sort({n:1}).toArray().map(d=>d.n)))'
same 'sort() descending'  'print(JSON.stringify(COLL.find({}).sort({n:-1}).toArray().map(d=>d.n)))'
same 'sort() on a string' 'print(JSON.stringify(COLL.find({}).sort({name:-1}).toArray().map(d=>d.name)))'
same 'sort() compound'    'print(JSON.stringify(COLL.find({}).sort({grp:1,n:-1}).toArray().map(d=>d.n)))'
same 'limit()'            'print(JSON.stringify(COLL.find({}).sort({n:1}).limit(4).toArray().map(d=>d.n)))'
same 'skip()'             'print(JSON.stringify(COLL.find({}).sort({n:1}).skip(15).toArray().map(d=>d.n)))'
same 'skip() and limit()' 'print(JSON.stringify(COLL.find({}).sort({n:1}).skip(8).limit(5).toArray().map(d=>d.n)))'
same 'project()'          "print(JSON.stringify(COLL.find({}).projection({name:1,_id:0})$S"
same 'countDocuments()'   'print(COLL.countDocuments({}))'
same 'countDocuments() filtered' 'print(COLL.countDocuments({grp:"beta"}))'
same 'distinct()'         'print(JSON.stringify(COLL.distinct("grp").sort()))'
same 'distinct() on array' 'print(JSON.stringify(COLL.distinct("tags").sort()))'
same 'distinct() with query' 'print(JSON.stringify(COLL.distinct("grp",{n:{$gt:12}}).sort()))'
same 'estimatedDocumentCount()' 'print(COLL.estimatedDocumentCount())'

A='.toArray()))'
echo
echo "== aggregation stages =="
same '$match'        "print(JSON.stringify(COLL.aggregate([{\$match:{n:{\$gt:15}}},{\$sort:{_id:1}}])$A"
same '$project'      "print(JSON.stringify(COLL.aggregate([{\$project:{name:1,double:{\$multiply:['\$n',2]}}},{\$sort:{_id:1}}])$A"
same '$set'          "print(JSON.stringify(COLL.aggregate([{\$set:{flag:'y'}},{\$sort:{_id:1}}]).toArray().map(d=>d.flag)))"
same '$addFields'    "print(JSON.stringify(COLL.aggregate([{\$addFields:{plus:{\$add:['\$n',100]}}},{\$sort:{_id:1}}]).toArray().map(d=>d.plus)))"
same '$unset'        "print(JSON.stringify(COLL.aggregate([{\$unset:'tags'},{\$sort:{_id:1}}]).toArray().map(d=>d.tags===undefined)))"
same '$group'        "print(JSON.stringify(COLL.aggregate([{\$group:{_id:'\$grp',total:{\$sum:'\$n'},c:{\$sum:1}}},{\$sort:{_id:1}}])$A"
same '$count'        "print(JSON.stringify(COLL.aggregate([{\$count:'n'}])$A"
same '$sort'         "print(JSON.stringify(COLL.aggregate([{\$sort:{n:-1}}]).toArray().map(d=>d.n)))"
same '$limit'        "print(JSON.stringify(COLL.aggregate([{\$sort:{n:1}},{\$limit:5}]).toArray().map(d=>d.n)))"
same '$skip'         "print(JSON.stringify(COLL.aggregate([{\$sort:{n:1}},{\$skip:16}]).toArray().map(d=>d.n)))"
same '$unwind'       "print(JSON.stringify(COLL.aggregate([{\$unwind:'\$tags'},{\$sort:{_id:1,tags:1}}]).toArray().map(d=>d._id+':'+d.tags)))"
same '$unwind opts'  "print(JSON.stringify(COLL.aggregate([{\$unwind:{path:'\$items',includeArrayIndex:'ix'}},{\$sort:{_id:1,ix:1}}]).toArray().length))"
same '$lookup'       "print(JSON.stringify(COLL.aggregate([{\$match:{_id:3}},{\$lookup:{from:COLL.getName(),localField:'grp',foreignField:'grp',as:'p'}},{\$project:{c:{\$size:'\$p'}}}])$A"
same '$lookup pipeline' "print(JSON.stringify(COLL.aggregate([{\$match:{_id:3}},{\$lookup:{from:COLL.getName(),pipeline:[{\$match:{n:{\$lt:4}}}],as:'p'}},{\$project:{c:{\$size:'\$p'}}}])$A"
same '$graphLookup'  "print(JSON.stringify(COLL.aggregate([{\$match:{_id:1}},{\$graphLookup:{from:COLL.getName(),startWith:'\$grp',connectFromField:'grp',connectToField:'grp',as:'g'}},{\$project:{c:{\$size:'\$g'}}}])$A"
same '$replaceRoot'  "print(JSON.stringify(COLL.aggregate([{\$replaceRoot:{newRoot:'\$meta'}},{\$sort:{score:1}}])$A"
same '$replaceWith'  "print(JSON.stringify(COLL.aggregate([{\$replaceWith:'\$meta'},{\$sort:{score:1}}])$A"
same '$facet'        "print(JSON.stringify(COLL.aggregate([{\$facet:{a:[{\$count:'c'}],b:[{\$group:{_id:'\$grp',c:{\$sum:1}}},{\$sort:{_id:1}}]}}])$A"
same '$bucket'       "print(JSON.stringify(COLL.aggregate([{\$bucket:{groupBy:'\$n',boundaries:[0,5,10,15,25],default:'other',output:{c:{\$sum:1}}}}])$A"
same '$bucketAuto'   "print(JSON.stringify(COLL.aggregate([{\$bucketAuto:{groupBy:'\$n',buckets:4}}]).toArray().map(d=>d.count)))"
# Both groups hold ten documents, and $sortByCount sorts by count alone, so the
# order of a tie is MongoDB's to choose and the two tiers are free to disagree
# about it without either being wrong. Sort the tie out here, or this compares
# something undefined and fails about one run in two.
same '$sortByCount'  "print(JSON.stringify(COLL.aggregate([{\$sortByCount:'\$grp'},{\$sort:{count:-1,_id:1}}])$A"
same '$unionWith'    "print(JSON.stringify(COLL.aggregate([{\$unionWith:{coll:COLL.getName()}},{\$count:'c'}])$A"
same '$setWindowFields' "print(JSON.stringify(COLL.aggregate([{\$setWindowFields:{sortBy:{n:1},output:{run:{\$sum:'\$n',window:{documents:['unbounded','current']}}}}},{\$sort:{n:1}}]).toArray().map(d=>d.run)))"
# $sample is random by definition, so only its shape can be differential.
same '$sample size'  "print(COLL.aggregate([{\$sample:{size:6}}]).toArray().length)"
same '$sample ids are real' "const s=COLL.aggregate([{\$sample:{size:6}}]).toArray(); print(s.every(d=>d._id>=1&&d._id<=20))"

echo
echo "== aggregation accumulators =="
G() { echo "print(JSON.stringify(COLL.aggregate([{\$group:{_id:null,v:$1}}]).toArray().map(d=>d.v)))"; }
same '$sum'         "$(G '{$sum:"$n"}')"
same '$avg'         "$(G '{$avg:"$n"}')"
same '$min'         "$(G '{$min:"$n"}')"
same '$max'         "$(G '{$max:"$n"}')"
same '$stdDevPop'   "$(G '{$stdDevPop:"$n"}')"
same '$stdDevSamp'  "$(G '{$stdDevSamp:"$n"}')"
same '$push'        "print(JSON.stringify(COLL.aggregate([{\$sort:{n:1}},{\$group:{_id:null,v:{\$push:'\$n'}}}]).toArray().map(d=>d.v)))"
same '$addToSet'    "print(JSON.stringify(COLL.aggregate([{\$group:{_id:null,v:{\$addToSet:'\$grp'}}}]).toArray().map(d=>d.v.sort())))"
same '$first'       "print(JSON.stringify(COLL.aggregate([{\$sort:{n:1}},{\$group:{_id:null,v:{\$first:'\$n'}}}]).toArray().map(d=>d.v)))"
same '$last'        "print(JSON.stringify(COLL.aggregate([{\$sort:{n:1}},{\$group:{_id:null,v:{\$last:'\$n'}}}]).toArray().map(d=>d.v)))"
same '$mergeObjects' "print(JSON.stringify(COLL.aggregate([{\$sort:{n:1}},{\$group:{_id:null,v:{\$mergeObjects:'\$meta'}}}]).toArray().map(d=>d.v)))"
same '$firstN'      "print(JSON.stringify(COLL.aggregate([{\$sort:{n:1}},{\$group:{_id:null,v:{\$firstN:{input:'\$n',n:3}}}}]).toArray().map(d=>d.v)))"
same '$lastN'       "print(JSON.stringify(COLL.aggregate([{\$sort:{n:1}},{\$group:{_id:null,v:{\$lastN:{input:'\$n',n:3}}}}]).toArray().map(d=>d.v)))"
same '$top'         "print(JSON.stringify(COLL.aggregate([{\$group:{_id:null,v:{\$top:{sortBy:{n:-1},output:'\$n'}}}}]).toArray().map(d=>d.v)))"
same '$topN'        "print(JSON.stringify(COLL.aggregate([{\$group:{_id:null,v:{\$topN:{sortBy:{n:-1},output:'\$n',n:3}}}}]).toArray().map(d=>d.v)))"
same '$bottom'      "print(JSON.stringify(COLL.aggregate([{\$group:{_id:null,v:{\$bottom:{sortBy:{n:-1},output:'\$n'}}}}]).toArray().map(d=>d.v)))"
same '$bottomN'     "print(JSON.stringify(COLL.aggregate([{\$group:{_id:null,v:{\$bottomN:{sortBy:{n:-1},output:'\$n',n:3}}}}]).toArray().map(d=>d.v)))"

echo
echo "== aggregation expressions =="
E() { echo "print(JSON.stringify(COLL.aggregate([{\$sort:{_id:1}},{\$project:{v:$1}}]).toArray().map(d=>d.v)))"; }
same '$add'          "$(E '{$add:["$n",5]}')"
same '$subtract'     "$(E '{$subtract:["$n",5]}')"
same '$multiply'     "$(E '{$multiply:["$n",3]}')"
same '$divide'       "$(E '{$divide:["$n",4]}')"
same '$mod'          "$(E '{$mod:["$n",5]}')"
same '$abs'          "$(E '{$abs:{$subtract:["$n",10]}}')"
same '$ceil'         "$(E '{$ceil:"$f"}')"
same '$floor'        "$(E '{$floor:"$f"}')"
same '$round'        "$(E '{$round:["$f",1]}')"
same '$trunc'        "$(E '{$trunc:["$f",1]}')"
same '$pow'          "$(E '{$pow:["$n",2]}')"
same '$sqrt'         "$(E '{$sqrt:"$n"}')"
same '$concat'       "$(E '{$concat:["$name","-","$grp"]}')"
same '$toLower'      "$(E '{$toLower:"$grp"}')"
same '$toUpper'      "$(E '{$toUpper:"$grp"}')"
same '$trim'         "$(E '{$trim:{input:{$concat:["  ","$grp","  "]}}}')"
same '$ltrim'        "$(E '{$ltrim:{input:{$concat:["  ","$grp"]}}}')"
same '$rtrim'        "$(E '{$rtrim:{input:{$concat:["$grp","  "]}}}')"
same '$split'        "$(E '{$split:["$words"," "]}')"
same '$replaceOne'   "$(E '{$replaceOne:{input:"$words",find:"e",replacement:"E"}}')"
same '$replaceAll'   "$(E '{$replaceAll:{input:"$words",find:"e",replacement:"E"}}')"
same '$size expr'    "$(E '{$size:"$tags"}')"
same '$arrayElemAt'  "$(E '{$arrayElemAt:["$tags",0]}')"
same '$arrayToObject' "$(E '{$arrayToObject:[[{k:"a",v:"$n"}]]}')"
same '$objectToArray' "$(E '{$objectToArray:"$meta"}')"
same '$concatArrays' "$(E '{$concatArrays:["$tags",["z"]]}')"
same '$filter'       "$(E '{$filter:{input:"$items",as:"i",cond:{$gt:["$$i.qty",2]}}}')"
same '$map'          "$(E '{$map:{input:"$tags",as:"t",in:{$toUpper:"$$t"}}}')"
same '$reduce'       "$(E '{$reduce:{input:"$items",initialValue:0,in:{$add:["$$value","$$this.qty"]}}}')"
same '$range'        "$(E '{$range:[0,"$n",3]}')"
same '$reverseArray' "$(E '{$reverseArray:"$tags"}')"
same '$sortArray'    "$(E '{$sortArray:{input:"$items",sortBy:{qty:-1}}}')"
same '$cond'         "$(E '{$cond:[{$gt:["$n",10]},"big","small"]}')"
same '$ifNull'       "$(E '{$ifNull:["$opt","none"]}')"
same '$switch'       "$(E '{$switch:{branches:[{case:{$lt:["$n",7]},then:"low"},{case:{$lt:["$n",14]},then:"mid"}],default:"high"}}')"
same '$and expr'     "$(E '{$and:[{$gt:["$n",5]},{$lt:["$n",15]}]}')"
same '$or expr'      "$(E '{$or:[{$lt:["$n",5]},{$gt:["$n",15]}]}')"
same '$not expr'     "$(E '{$not:[{$gt:["$n",10]}]}')"
same '$literal'      "$(E '{$literal:"$n"}')"

echo
echo "== date operations =="
same '$year'         "$(E '{$year:"$when"}')"
same '$month'        "$(E '{$month:"$when"}')"
same '$week'         "$(E '{$week:"$when"}')"
same '$dayOfYear'    "$(E '{$dayOfYear:"$when"}')"
same '$dayOfMonth'   "$(E '{$dayOfMonth:"$when"}')"
same '$dayOfWeek'    "$(E '{$dayOfWeek:"$when"}')"
same '$hour'         "$(E '{$hour:"$when"}')"
same '$minute'       "$(E '{$minute:"$when"}')"
same '$second'       "$(E '{$second:"$when"}')"
same '$millisecond'  "$(E '{$millisecond:"$when"}')"
same '$dateToString' "$(E '{$dateToString:{date:"$when",format:"%Y-%m-%d %H:%M:%S"}}')"
same '$dateFromString' "$(E '{$dateFromString:{dateString:{$dateToString:{date:"$when"}}}}')"
same '$dateToParts'  "$(E '{$dateToParts:{date:"$when"}}')"
same '$dateFromParts' "$(E '{$dateFromParts:{year:2024,month:3,day:9}}')"
same '$dateAdd'      "$(E '{$dateAdd:{startDate:"$when",unit:"day",amount:10}}')"
same '$dateSubtract' "$(E '{$dateSubtract:{startDate:"$when",unit:"month",amount:1}}')"
same '$dateDiff'     "$(E '{$dateDiff:{startDate:"$when",endDate:new Date(Date.UTC(2025,0,1)),unit:"day"}}')"

echo
echo "== document and object operations =="
same '$getField'     "$(E '{$getField:{field:"score",input:"$meta"}}')"
same '$setField'     "$(E '{$setField:{field:"added",input:"$meta",value:1}}')"
same '$unsetField'   "$(E '{$unsetField:{field:"score",input:"$meta"}}')"
same '$mergeObjects expr' "$(E '{$mergeObjects:["$meta",{extra:true}]}')"

echo
echo "== update operators, on a document held only in the archive =="
# _id 17 is archive only in the tiered collection and ordinary in the reference,
# so every one of these has to reach across the tiers and land identically.
upd() { samew "$1" "COLL.updateOne({_id:17},$2); print(JSON.stringify(COLL.findOne({_id:17})))"; }
upd '$set'        '{$set:{name:"changed",extra:1}}'
upd '$unset'      '{$unset:{grp:""}}'
upd '$rename'     '{$rename:{name:"label"}}'
upd '$inc'        '{$inc:{n:5}}'
upd '$mul'        '{$mul:{n:3}}'
upd '$min'        '{$min:{n:2}}'
upd '$max'        '{$max:{n:99}}'
upd '$push'       '{$push:{tags:"pushed"}}'
upd '$pop'        '{$pop:{tags:1}}'
upd '$pull'       '{$pull:{tags:"common"}}'
upd '$pullAll'    '{$pullAll:{tags:["common","t3"]}}'
upd '$addToSet'   '{$addToSet:{tags:"common"}}'
upd '$each'       '{$push:{tags:{$each:["a","b"]}}}'
upd '$position'   '{$push:{tags:{$each:["front"],$position:0}}}'
upd '$slice'      '{$push:{tags:{$each:["x","y","z"],$slice:3}}}'
upd '$sort modifier' '{$push:{items:{$each:[{sku:"s0",qty:99}],$sort:{qty:-1}}}}'
# $currentDate cannot be differential, because the two runs happen at different
# instants. What has to match is that it landed at all, and as a date.
samew '$currentDate' 'COLL.updateOne({_id:17},{$currentDate:{seen:true}}); print(COLL.findOne({_id:17}).seen instanceof Date)'

echo
echo "== the write commands themselves =="
samew 'insertOne()'          'COLL.insertOne({_id:77,n:77}); print(COLL.countDocuments({}))'
samew 'insertMany()'         'COLL.insertMany([{_id:78,n:78},{_id:79,n:79}]); print(COLL.countDocuments({}))'
samew 'insertOne() duplicate' 'try { COLL.insertOne({_id:15,dup:1}); print("ACCEPTED"); } catch(e) { print(/E11000|duplicate/i.test(e.message)?"DUPLICATE":"other: "+e.message.substring(0,40)); }'
samew 'updateOne()'          'const r=COLL.updateOne({_id:14},{$set:{v:1}}); print(r.matchedCount+"/"+r.modifiedCount)'
samew 'updateOne() by field' 'const r=COLL.updateOne({name:"user18"},{$set:{v:1}}); print(r.matchedCount+"/"+r.modifiedCount+"/"+COLL.countDocuments({v:1}))'
samew 'updateMany()'         'const r=COLL.updateMany({grp:"beta"},{$set:{v:1}}); print(r.matchedCount+"/"+r.modifiedCount)'
samew 'updateMany() applied' 'COLL.updateMany({n:{$gt:12}},{$inc:{n:1}}); print(JSON.stringify(COLL.find({}).sort({_id:1}).toArray().map(d=>d.n)))'
samew 'updateOne() upsert existing' 'const r=COLL.updateOne({_id:17},{$set:{v:1}},{upsert:true}); print(r.matchedCount+"/"+r.upsertedCount+"/"+JSON.stringify(COLL.findOne({_id:17})))'
samew 'updateOne() upsert new' 'const r=COLL.updateOne({_id:88},{$set:{v:1}},{upsert:true}); print(r.upsertedCount+"/"+COLL.countDocuments({}))'
samew 'replaceOne()'         'COLL.replaceOne({_id:16},{_id:16,replaced:true}); print(JSON.stringify(COLL.findOne({_id:16})))'
samew 'deleteOne() by _id'   'const r=COLL.deleteOne({_id:19}); print(r.deletedCount+"/"+COLL.countDocuments({}))'
samew 'deleteOne() by field' 'const r=COLL.deleteOne({name:"user18"}); print(r.deletedCount+"/"+COLL.countDocuments({}))'
samew 'deleteMany()'         'const r=COLL.deleteMany({n:{$gt:15}}); print(r.deletedCount+"/"+COLL.countDocuments({}))'
samew 'findOneAndUpdate()'   'print(JSON.stringify(COLL.findOneAndUpdate({_id:13},{$set:{z:1}},{returnDocument:"after"})))'
samew 'findOneAndReplace()'  'print(JSON.stringify(COLL.findOneAndReplace({_id:13},{_id:13,fresh:1},{returnDocument:"after"})))'
samew 'findOneAndDelete()'   'print(JSON.stringify(COLL.findOneAndDelete({_id:13}))+"/"+COLL.countDocuments({}))'

echo
echo "== bulkWrite, every operation type =="
samew 'bulkWrite insertOne'  'COLL.bulkWrite([{insertOne:{document:{_id:90,n:90}}}]); print(COLL.countDocuments({}))'
samew 'bulkWrite updateOne'  'const r=COLL.bulkWrite([{updateOne:{filter:{_id:17},update:{$set:{b:1}}}}]); print(r.matchedCount+"/"+JSON.stringify(COLL.findOne({_id:17})))'
samew 'bulkWrite updateMany' 'const r=COLL.bulkWrite([{updateMany:{filter:{grp:"beta"},update:{$set:{b:1}}}}]); print(r.matchedCount+"/"+COLL.countDocuments({b:1}))'
samew 'bulkWrite replaceOne' 'COLL.bulkWrite([{replaceOne:{filter:{_id:16},replacement:{_id:16,r:1}}}]); print(JSON.stringify(COLL.findOne({_id:16})))'
samew 'bulkWrite deleteOne'  'const r=COLL.bulkWrite([{deleteOne:{filter:{_id:19}}}]); print(r.deletedCount+"/"+COLL.countDocuments({}))'
samew 'bulkWrite deleteMany' 'const r=COLL.bulkWrite([{deleteMany:{filter:{n:{$gt:15}}}}]); print(r.deletedCount+"/"+COLL.countDocuments({}))'
samew 'bulkWrite mixed'      'const r=COLL.bulkWrite([{insertOne:{document:{_id:91,n:91}}},{updateOne:{filter:{_id:18},update:{$set:{m:1}}}},{deleteOne:{filter:{_id:20}}}]); print(r.insertedCount+"/"+r.matchedCount+"/"+r.deletedCount+"/"+COLL.countDocuments({}))'

echo
echo "== indexes =="
reseed
same 'createIndex()' 'print(COLL.createIndex({f:1},{name:"conf_f"}))'
same 'getIndexes()'  'print(JSON.stringify(COLL.getIndexes().map(i=>i.name).sort()))'
same 'dropIndex()'   'COLL.dropIndex("conf_f"); print(JSON.stringify(COLL.getIndexes().map(i=>i.name).sort()))'
same 'dropIndexes()' 'COLL.dropIndexes(); print(JSON.stringify(COLL.getIndexes().map(i=>i.name).sort()))'

echo
echo "== database and collection commands =="
reseed
chk "show dbs lists the database"                       "true"  "$(R "print(db.adminCommand({listDatabases:1}).databases.some(d=>d.name==='$DB'))")"
chk "show collections lists the tiered one"     "true"  "$(R "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes('tcon'))")"
chk "db names the database"                     "$DB"  "$(R "print(db.getSiblingDB(process.env.TC_DB).getName())")"
chk "createCollection()"                        "true"  "$(R "db.getSiblingDB(process.env.TC_DB).tcnew.drop(); db.getSiblingDB(process.env.TC_DB).createCollection('tcnew'); print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes('tcnew'))")"
chk "drop()"                                    "false" "$(R "db.getSiblingDB(process.env.TC_DB).tcnew.drop(); print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes('tcnew'))")"
chk "dropDatabase()"                            "true"  "$(R "db.getSiblingDB('tcdropme').x.insertOne({a:1}); print(db.getSiblingDB('tcdropme').dropDatabase().ok===1)")"

echo
echo "== let variables, which a filter may refer to =="
# MongoDB 5.0 gave update, delete and findAndModify a let document, and a filter
# refers to it as $$name. The copy-up that lets a multi-document write reach
# archived documents asks the archive the same filter, so it has to carry the
# same variables. Without them the archive rejects the filter and the write is
# refused: safe, and an ordinary statement turned into an error.
samew 'update with a let variable'   'const r=db.getSiblingDB(process.env.TC_DB).runCommand({update:COLL.getName(),
     updates:[{q:{$expr:{$eq:["$_id","$$t"]}},u:{$set:{lethit:1}},multi:true}],let:{t:17}});
   print(r.n+"/"+r.nModified)'
samew 'delete with a let variable'   'const r=db.getSiblingDB(process.env.TC_DB).runCommand({delete:COLL.getName(),
     deletes:[{q:{$expr:{$eq:["$_id","$$t"]}},limit:0}],let:{t:18}});
   print(r.n+"/"+COLL.countDocuments({_id:18}))'
# Collation decides what equal means. The documents are named user01 to user20
# and 11 to 20 are in the archive only, so a case-insensitive filter for USER17
# matches one document the fast tier cannot see. Asking the archive the same
# filter without the collation asks a different question, and the write then
# reaches some of what it matched and reports success. Measured before the fix
# on a collection holding alice and ALICE in the archive: n=1 where an untiered
# reference gave n=2.
samew 'update with a collation'      'const r=db.getSiblingDB(process.env.TC_DB).runCommand({update:COLL.getName(),
     updates:[{q:{name:"USER17"},u:{$set:{colhit:1}},multi:true,
               collation:{locale:"en",strength:2}}]});
   print(r.n+"/"+r.nModified)'
samew 'delete with a collation'      'const r=db.getSiblingDB(process.env.TC_DB).runCommand({delete:COLL.getName(),
     deletes:[{q:{name:"USER18"},limit:0,collation:{locale:"en",strength:2}}]});
   print(r.n+"/"+COLL.countDocuments({}))'
samew 'upsert with a collation'      'const r=db.getSiblingDB(process.env.TC_DB).runCommand({update:COLL.getName(),
     updates:[{q:{name:"USER19"},u:{$set:{colups:1}},upsert:true,
               collation:{locale:"en",strength:2}}]});
   print(r.n+"/"+(r.upserted?r.upserted.length:0)+"/"+COLL.countDocuments({}))'

echo
echo "== errors the server itself would return =="
# A drop-in proxy has to fail the way MongoDB fails, not just succeed the way it
# succeeds. An application branches on e.code, and a retry helper branches on the
# error label, so an error rewritten on the way out is a wrong answer with an
# ok:0 on it. Every check here prints the code and the codeName rather than the
# message, because that is what a driver acts on.
reseed
E='catch(e){print(e.code+" "+e.codeName)}'
same 'unknown top-level operator'  "try{COLL.find({\$nosuchop:1}).toArray();print('no error')}$E"
same 'unknown field operator'      "try{COLL.find({n:{\$badop:1}}).toArray();print('no error')}$E"
same 'invalid regular expression'  "try{COLL.find({name:{\$regex:'('}}).toArray();print('no error')}$E"
same 'mixed projection'            "try{COLL.find({},{name:1,grp:0}).toArray();print('no error')}$E"
same 'bad sort specification'      "try{COLL.find({}).sort({n:'up'}).toArray();print('no error')}$E"
same 'negative skip'               "try{COLL.find({}).skip(-1).toArray();print('no error')}$E"
same 'hint naming no index'        "try{COLL.find({}).hint('tc_no_such_index').toArray();print('no error')}$E"
same 'unknown pipeline stage'      "try{COLL.aggregate([{\$nosuchstage:{}}]).toArray();print('no error')}$E"
same 'unknown accumulator'         "try{COLL.aggregate([{\$group:{_id:null,x:{\$nosuchacc:'\$n'}}}]).toArray();print('no error')}$E"
same 'unknown expression operator' "try{COLL.aggregate([{\$project:{x:{\$nosuchexpr:['\$n',1]}}}]).toArray();print('no error')}$E"
same 'divide by zero'              "try{COLL.aggregate([{\$project:{x:{\$divide:['\$n',0]}}}]).toArray();print('no error')}$E"
same 'zero $limit'                 "try{COLL.aggregate([{\$limit:0}]).toArray();print('no error')}$E"
same 'count with a bad filter'     "try{print(COLL.count({\$badop:1}))}$E"
same 'distinct with a bad filter'  "try{print(COLL.distinct('grp',{\$badop:1}).length)}$E"
same 'countDocuments bad filter'   "try{print(COLL.countDocuments({\$badop:1}))}$E"
same 'update with a bad operator'  "try{print(COLL.updateOne({_id:17},{\$nosuchup:{x:1}}).modifiedCount)}$E"
same 'update conflicting paths'    "try{print(COLL.updateOne({_id:17},{\$set:{'a.b':1,'a':2}}).modifiedCount)}$E"
same 'insert duplicate _id'        "try{print(COLL.insertOne({_id:17}).acknowledged)}$E"
same 'findAndModify bad operator'  "try{print(JSON.stringify(COLL.findAndModify({query:{_id:17},update:{\$nosuchup:{x:1}}})))}$E"
echo
echo "== a point read is still the whole query =="
# An _id equality names at most one document, which is why a point read is
# exempt from the completeness guard: no claim about how much history the fast
# tier holds is needed to answer it. The exemption never covered answering a
# different question, and that is what happened, because the miss was resolved
# by reading the document by its _id and handing it back.
#
# _id 17 lives only in the archive here, so every one of these takes that path.
# samew rather than same: answering one of them promotes the document to the
# fast tier, and the next check would then never reach the archive at all.
samew 'a predicate that excludes the document' "print(JSON.stringify(COLL.find({_id:17,n:{\$gt:100}}).toArray()))"
samew 'a predicate that includes it'           "print(JSON.stringify(COLL.find({_id:17,n:{\$gte:17}}).toArray()))"
samew 'an \$eq on _id with a predicate'        "print(JSON.stringify(COLL.find({_id:{\$eq:17},grp:'nosuchgroup'}).toArray()))"
samew 'a projection'                           "print(JSON.stringify(COLL.find({_id:17},{name:1,_id:0}).toArray()))"
samew 'an excluding projection'                "print(JSON.stringify(COLL.find({_id:17},{items:0,loc:0,words:0,when:0}).toArray()))"
samew 'skipped past'                            "print(JSON.stringify(COLL.find({_id:17}).skip(1).toArray()))"
samew 'a limit of one'                          "print(JSON.stringify(COLL.find({_id:17}).limit(1).toArray()))"
samew 'findOne with a predicate that excludes'  "print(JSON.stringify(COLL.findOne({_id:17,opt:'nosuchvalue'})))"
samew 'the plain point read still answers'      "print(JSON.stringify(COLL.findOne({_id:17})))"
# The write paths were never wrong here, because they copy the document up and
# run the client's own statement against the fast tier, which applies the whole
# filter. Locked in rather than assumed.
samew 'an update by _id a predicate excludes'   "print(COLL.updateOne({_id:17,n:{\$gt:100}},{\$set:{touched:1}}).matchedCount)"
samew 'and it changed nothing'                  "COLL.updateOne({_id:17,n:{\$gt:100}},{\$set:{touched:1}}); print(COLL.countDocuments({touched:1}))"
samew 'a delete by _id a predicate excludes'    "print(COLL.deleteOne({_id:17,n:{\$gt:100}}).deletedCount)"
samew 'findAndModify a predicate excludes'      "print(JSON.stringify(COLL.findAndModify({query:{_id:17,n:{\$gt:100}},update:{\$set:{touched:1}}})))"

echo
echo "== a stage that reads a collection of its own =="
# $lookup, $unionWith and $graphLookup do not read the pipeline's input, they
# read another collection, so the tombstone filter in front of the pipeline
# never reaches them. The archive keeps a deleted document as a tombstone, and
# the difference only shows once something has been deleted: before that, both
# tiers agree and the feature looks finished.
#
# Deletes go through the router so both tiers see them, then the archive is
# polled until the tombstones are there. Comparing before that would report the
# mirroring lag rather than the rewrite.
reseed
RALL "const d=db.getSiblingDB(process.env.TC_DB);
      for (const c of [d.tcref, d.tcon]) { c.deleteMany({_id:{\$in:[3,4,17,18]}}); }" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).tcon.countDocuments({_tcDeletedAt:{\$exists:true}}))")" = "4" ] && break
  sleep 0.5
done
chk "the archive holds four tombstones"  "4" "$(C "print(db.getSiblingDB(process.env.TC_DB).tcon.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
# print(JSON.stringify(COLL.aggregate([ ... ]).toArray())) and not one bracket
# less. aggregate returns a cursor, and JSON.stringify of a cursor throws the
# same circular-structure error on both sides, so `same` agrees and the check
# passes having asked nothing. Six of these did that on the first run.
JOINED="{\$group:{_id:'\$grp',n:{\$sum:1},sibs:{\$max:{\$size:'\$sib'}}}},{\$sort:{_id:1}}]).toArray()))"
same 'a self join by field'        "print(JSON.stringify(COLL.aggregate([{\$lookup:{from:COLL.getName(),localField:'grp',foreignField:'grp',as:'sib'}},$JOINED"
same 'a self join with a pipeline' "print(JSON.stringify(COLL.aggregate([{\$lookup:{from:COLL.getName(),let:{g:'\$grp'},pipeline:[{\$match:{\$expr:{\$eq:['\$grp','\$\$g']}}}],as:'sib'}},$JOINED"
same 'a union with itself'         "print(COLL.aggregate([{\$unionWith:COLL.getName()},{\$count:'c'}]).toArray()[0].c)"
same 'a union with a pipeline'     "print(COLL.aggregate([{\$unionWith:{coll:COLL.getName(),pipeline:[{\$match:{grp:'alpha'}}]}},{\$count:'c'}]).toArray()[0].c)"
same 'a graph lookup'              "print(JSON.stringify(COLL.aggregate([{\$graphLookup:{from:COLL.getName(),startWith:'\$grp',connectFromField:'grp',connectToField:'grp',as:'tree'}},{\$group:{_id:'\$grp',t:{\$max:{\$size:'\$tree'}}}},{\$sort:{_id:1}}]).toArray()))"
same 'a lookup inside a facet'     "print(JSON.stringify(COLL.aggregate([{\$facet:{j:[{\$lookup:{from:COLL.getName(),localField:'grp',foreignField:'grp',as:'sib'}},{\$group:{_id:null,s:{\$max:{\$size:'\$sib'}}}}]}}]).toArray()))"
same 'a lookup nested in a lookup' "print(JSON.stringify(COLL.aggregate([{\$lookup:{from:COLL.getName(),localField:'grp',foreignField:'grp',as:'sib',pipeline:[{\$lookup:{from:COLL.getName(),localField:'grp',foreignField:'grp',as:'inner'}},{\$project:{i:{\$size:'\$inner'}}}]}},{\$group:{_id:'\$grp',n:{\$max:{\$size:'\$sib'}}}},{\$sort:{_id:1}}]).toArray()))"
# And the filter must not be added where it would break the stage: $documents
# has to be the first stage of the pipeline it is in.
same 'a lookup over $documents'    "print(JSON.stringify(COLL.aggregate([{\$limit:1},{\$lookup:{as:'d',pipeline:[{\$documents:[{x:1},{x:2}]}]}},{\$project:{c:{\$size:'\$d'}}}]).toArray()))"
# The guard against the failure above: two sides that both throw agree, so at
# least one of these has to be shown answering. Hand-asserted on the untiered
# reference, which is ordinary MongoDB, and it says the answer has the shape the
# comparisons are comparing.
chk "the joins were answered, not thrown" "alpha/8/8" "$(R "const r=db.getSiblingDB(process.env.TC_DB).tcref.aggregate([{\$lookup:{from:'tcref',localField:'grp',foreignField:'grp',as:'sib'}},{\$group:{_id:'\$grp',n:{\$sum:1},sibs:{\$max:{\$size:'\$sib'}}}},{\$sort:{_id:1}}]).toArray(); print(r.length?r[0]._id+'/'+r[0].n+'/'+r[0].sibs:'NOTHING')")"

echo
echo "== transactions =="
reseed
TXN=$(R "
  const s=db.getMongo().startSession(); let out='';
  for (let attempt=0; attempt<6; attempt++) {
    try {
      s.startTransaction();
      const c=s.getDatabase(process.env.TC_DB).tcon;
      c.updateOne({_id:18},{\$set:{txn:1}});
      const seen=c.countDocuments({txn:1});
      s.commitTransaction();
      out='committed-'+seen; break;
    } catch(e) {
      try { s.abortTransaction(); } catch(x) {}
      if (e.errorLabels && e.errorLabels.includes('TransientTransactionError')) continue;
      out='error: '+e.message.substring(0,60); break;
    }
  }
  s.endSession(); print(out);")
chk "startTransaction and commitTransaction"    "committed-1" "$TXN"
chk "the commit is visible afterwards"          "1" "$(R "print(db.getSiblingDB(process.env.TC_DB).tcon.countDocuments({txn:1}))")"
ABORT=$(R "
  const s=db.getMongo().startSession(); s.startTransaction();
  try { s.getDatabase(process.env.TC_DB).tcon.updateOne({_id:2},{\$set:{gone:1}}); } catch(e) {}
  try { s.abortTransaction(); } catch(e) {}
  s.endSession();
  print(db.getSiblingDB(process.env.TC_DB).tcon.countDocuments({gone:1}));")
chk "abortTransaction leaves nothing behind"    "0" "$ABORT"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
