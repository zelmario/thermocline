#!/bin/bash
# What happens when the two tiers have different indexes, which they always do.
#
# Nothing copies indexes between the tiers, in either direction, and that is
# deliberate: the fast tier carries the application's query plans, and the
# rollout guide tells operators to drop most of the cheap tier's indexes to
# reclaim its storage. So the archive is expected to be able to *find* any
# document and not expected to be able to answer any query efficiently.
#
# The line that matters is between slow and broken. A fallback read that scans
# is fine. A fallback read that errors is an application that worked before
# adoption and does not after, which is the promise this whole system makes.
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
NS=tidx
ANS=tidxadopt
ONS=tidxopt
N=${N:-40000}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/idx_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/idx_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}

echo "== setup: $N documents with a kilobyte each =="
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({})" >/dev/null
startFeeder
startRouter

# A kilobyte of padding each, so a blocking sort over the collection is well
# past the 32 MB in-memory sort limit. That limit is the thing being probed:
# with an index the sort is free, without one it has to be spilled or refused.
H "const c=db.getSiblingDB(process.env.TC_DB).$NS; const pad='x'.repeat(1024);
   for (let b=0; b<$N/2000; b++) {
     const d=[];
     for (let i=0;i<2000;i++){ const n=b*2000+i; d.push({_id:n, sortme:($N-n), grp:'g'+(n%10), body:pad}); }
     c.insertMany(d);
   }" >/dev/null
for _ in $(seq 1 300); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "$N" ] && break
  sleep 1
done
echo "  archive holds $(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

echo "== indexes are not copied, in either direction =="
# Stated as a check rather than a comment, because somebody will eventually
# assume otherwise and this says so out loud.
H "db.getSiblingDB(process.env.TC_DB).$NS.createIndex({sortme:1},{name:'fast_sortme'})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).$NS.createIndex({grp:1},{name:'via_router_grp'})" >/dev/null
sleep 4
chk "the fast tier has the index"               "true"  "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.getIndexes().some(i=>i.name=='fast_sortme'))")"
chk "the archive did not get it"                "false" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.getIndexes().some(i=>i.name=='fast_sortme'))")"
chk "nor one created through the router"        "false" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.getIndexes().some(i=>i.name=='via_router_grp'))")"

echo "== every read below falls through to the archive =="
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "an unsorted fallback read is answered"     "$N" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

echo "== a sort the archive has no index for =="
# PSMDB 8.0 ships allowDiskUseByDefault true and a 100 MB sort limit, so the
# easy case passes for free and proves nothing. These reads are run against a
# deliberately tightened archive: a 1 MB sort limit, which $N kilobyte
# documents cannot possibly fit inside. That is also a real configuration,
# since operators lower these to protect a cluster from expensive queries.
C 'db.adminCommand({setParameter:1, internalQueryMaxBlockingSortMemoryUsageBytes: 1048576})' >/dev/null
# This is the one that matters. The fast tier answers it from an index. The
# archive has to sort $N kilobyte documents with nothing to help it, which is
# far past the 32 MB in-memory limit, so it either spills to disk or fails. A
# query that worked before adoption must not start failing after it.
SORTED=$(R "
  try {
    const c=db.getSiblingDB(process.env.TC_DB).$NS.find({}).sort({sortme:1}).limit(3).toArray();
    print(c.length===3 ? c[0].sortme : 'SHORT:'+c.length);
  } catch(e) { print('ERR '+e.message.substring(0,70)); }")
chk "a sorted fallback read is answered"        "1" "$SORTED"
# And the whole thing, not just the first page, since a limit lets the server
# use a top-k sort that fits in memory whatever the collection size.
BIGSORT=$(R "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).sort({sortme:1}).itcount()); }
  catch(e) { print('ERR '+e.message.substring(0,70)); }")
chk "a full sorted scan is answered"            "$N" "$BIGSORT"

echo "== the same, with the archive refusing to spill =="
# The configuration where this genuinely breaks: no disk use, and a limit the
# sort cannot fit in. If the router does not ask for disk use itself, a query
# the fast tier answers from an index becomes an error on the archive.
C 'db.adminCommand({setParameter:1, allowDiskUseByDefault: false})' >/dev/null
NODISK=$(R "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).sort({sortme:1}).itcount()); }
  catch(e) { print('ERR '+e.message.substring(0,60)); }")
chk "a sorted read works even with spilling off" "$N" "$NODISK"
C 'db.adminCommand({setParameter:1, allowDiskUseByDefault: true})' >/dev/null

echo "== an aggregation sort, which has its own rules =="
AGGSORT=$(R "
  try { print(db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$sort:{sortme:1}},{\$limit:1}]).toArray()[0].sortme); }
  catch(e) { print('ERR '+e.message.substring(0,70)); }")
chk "a sorted aggregation is answered"          "1" "$AGGSORT"

echo "== a big answer from the archive is split into batches =="
# readBatch counted documents and not bytes, so a client asking for a large
# batch of large documents had the router assemble one reply of arbitrary size:
# past the wire protocol's limit, and large enough to take the proxy down and
# every connection on it. mongod bounds a batch by bytes; so must this.
#
# 40,000 kilobyte documents is forty megabytes, and asking for all of them in
# one batch is what a driver does when told batchSize.
BIG=$(R "
  try {
    const c = db.getSiblingDB(process.env.TC_DB).$NS.find({}).batchSize(40000);
    let n = 0;
    while (c.hasNext() && n < $N) { c.next(); n++; }
    print(n);
  } catch(e) { print('ERR '+e.message.substring(0,70)); }")
chk "every document arrives across batches"      "$N" "$BIG"

echo "== a query whose index exists only on the fast tier =="
chk "a filter on an archive-unindexed field works" "4000" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({grp:'g3'}))")"

# Put the archive back the way it was found.
C 'db.adminCommand({setParameter:1, internalQueryMaxBlockingSortMemoryUsageBytes: 104857600})' >/dev/null
C 'db.adminCommand({setParameter:1, allowDiskUseByDefault: true})' >/dev/null

echo "== what cline indexes reports, and what it refuses to suggest =="
# The report is what an operator decides from, and --commands is what they will
# paste into an archive, so both are checked against a collection whose indexes
# are known rather than against prose.
#
# Two of the fast tier's index options must never cross. A unique index on the
# archive can refuse a write the feeder has to make, because a value deleted and
# written again collides with the tombstoned copy of the old one. A TTL index
# would delete the copies the archive exists to keep, and nothing would report
# it: the documents would simply stop being there.
IXNS=tidxrep
H "db.getSiblingDB(process.env.TC_DB).$IXNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$IXNS.drop()" >/dev/null
H "const c=db.getSiblingDB(process.env.TC_DB).$IXNS;
   c.insertOne({_id:1, customer:1, createdAt:new Date(), email:'a@x', nested:{path:1}});
   c.createIndex({customer:1, createdAt:-1});
   c.createIndex({email:1},{unique:true});
   c.createIndex({createdAt:1},{expireAfterSeconds:3600, name:'ttl_idx'});
   c.createIndex({'nested.path':1});" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$IXNS.insertOne({_id:1})" >/dev/null

REP=$(cline indexes --config $CFG --ns $DB.$IXNS 2>&1)
chk "the report names an index the archive lacks" "true" \
    "$(grep -q 'customer_1_createdAt_-1' <<<"$REP" && echo true || echo false)"
chk "and warns about the TTL one by name"        "true" \
    "$(grep -qE 'ttl_idx.*would delete' <<<"$REP" && echo true || echo false)"

CMDS=$(cline indexes --config $CFG --ns $DB.$IXNS --commands 2>&1 | grep 'createIndex(')
chk "--commands offers the missing ones"          "3" "$(grep -c 'createIndex(' <<<"$CMDS" || true)"
chk "and never the TTL one"                       "0" "$(grep -c 'ttl_idx' <<<"$CMDS" || true)"
chk "and never carries unique across"             "0" "$(grep -c 'unique' <<<"$CMDS" || true)"
# A nested path has a dot in its field name, so an unquoted key is not valid
# JavaScript and the whole block would fail to run.
chk "a nested path is quoted"                     "true" \
    "$(grep -q '"nested.path"' <<<"$CMDS" && echo true || echo false)"

# The strongest check: the output is not merely plausible, it runs.
#
# Through --eval rather than piped on stdin. Same URI, different topology:
# mongosh given a script on stdin connects direct to whichever member it lands
# on, so every createIndex is refused with NotWritablePrimary, while --eval on
# the identical URI discovers the replica set and reaches the primary. This
# passed for as long as it landed on the primary and started failing the moment
# an election moved it, which is the shape of check that hides for weeks.
mongosh "$COLD" --quiet --eval "$CMDS" >/dev/null 2>&1
chk "the commands run against the archive"        "4" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$IXNS.getIndexes().length)")"
chk "and none of them arrived unique or expiring" "true" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$IXNS.getIndexes().every(i=>!i.unique && i.expireAfterSeconds===undefined))")"
AFTER=$(cline indexes --config $CFG --ns $DB.$IXNS 2>&1)
chk "the report then shows only the TTL one left" "true" \
    "$(grep -qE 'ttl_idx' <<<"$AFTER" && ! grep -q 'customer_1_createdAt_-1' <<<"$AFTER" && echo true || echo false)"
H "db.getSiblingDB(process.env.TC_DB).$IXNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$IXNS.drop()" >/dev/null

echo "== the collection an adoption starts with, written to before it is read =="
# The shape every adoption has: the existing cluster becomes the archive
# carrying the application's indexes, the fast tier is new and empty, and the
# application is pointed at the router. This collection is tiered, nothing warms
# it, and no read of it ever falls through, so the only thing that ever creates
# it on the fast tier is the application's own first insert.
#
# The unique index is the point. The router reads the FAST TIER's unique indexes
# to know which values it has to protect across both tiers, so a fast tier that
# has never been given them means neither mongod nor the key registry is
# enforcing one. A constraint the application has relied on for years must not
# stop holding on the day it is adopted.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$ANS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ANS.drop()" >/dev/null
# The registry lives on the fast tier and outlives the collection, so a re-run
# would inherit its own last run's claims and refuse a value nothing holds any
# more. Adoption means nothing has been claimed yet, so say so.
H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+'.$ANS'})" >/dev/null
C "const c=db.getSiblingDB(process.env.TC_DB).$ANS;
   c.createIndex({code:1},{unique:true, name:'code_1'});
   c.createIndex({owner:1},{name:'owner_1'});
   c.insertMany([{_id:1,code:'A-001',owner:'ann'},{_id:2,code:'A-002',owner:'bo'}])" >/dev/null
chk "the archive carries it, the fast tier does not" "false" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes('$ANS'))")"
startFeeder
startRouter

chk "the fast tier is given the unique index"      "true" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$ANS.getIndexes().some(i=>i.name=='code_1' && i.unique===true))")"
# The documented adoption step, and it only works because of the line above.
# The registry protects what it knows about, and a database adopted with
# documents already in it starts with none of them claimed. RebuildKeys reads
# the constraint from the FAST TIER's unique indexes, so against a fast tier
# that had never been given them it walked both tiers and claimed nothing,
# reporting success. That is the check below: not that the command ran, but
# that it found a constraint to enforce.
REB=$(cline keys --config $CFG --rebuild 2>&1)
chk "keys --rebuild finds a constraint to enforce" "true" \
    "$(grep -qE "$ANS" <<<"$REB" && echo true || echo false)"
# Both halves of the constraint, because they fail for different reasons: a
# value the archive holds is the registry's job, and a value only this fast tier
# holds is mongod's.
chk "a code the archive already holds is refused"  "refused" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$ANS.insertOne({_id:10,code:'A-001',owner:'cy'});print('ACCEPTED')}catch(e){print('refused')}")"
chk "and the same new code twice is refused"       "refused" \
    "$(R "const c=db.getSiblingDB(process.env.TC_DB).$ANS;
          try{c.insertOne({_id:11,code:'B-001',owner:'cy'})}catch(e){print('first insert refused: '+e.message.substring(0,40))}
          try{c.insertOne({_id:12,code:'B-001',owner:'di'});print('ACCEPTED')}catch(e){print('refused')}")"
chk "so the fast tier holds one B-001, not two"    "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$ANS.countDocuments({code:'B-001'}))")"
# The consequence of getting the above wrong, and the reason it is not merely
# slow: a document the fast tier accepted and the archive's unique index refuses
# cannot be mirrored, and the feeder that cannot mirror it stops mirroring
# everything behind it. This check is what says the archive is still moving.
sleep 6
chk "and mirroring is still running behind it"     "true" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$ANS.countDocuments({code:'B-001'})===1)")"
H "db.getSiblingDB(process.env.TC_DB).$ANS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ANS.drop()" >/dev/null

echo "== an index the copy has to carry whole =="
# An index carries more than its keys, and the fast tier's copy is built from
# the archive's definition. A text index reports its key as {_fts, _ftsx} and
# keeps the meaning in weights; a wildcard index keeps it in wildcardProjection.
# Copied without them, one is refused by MongoDB and the other silently becomes
# an index over every field of every document.
#
# createIndexes is one command for the whole batch, so the refusal took the
# other indexes of the collection with it: measured, this collection ended up
# holding _id_ and nothing else, retried every minute, for ever. That is an
# adopted collection with a search box on it, and the only sign is a log line.
TXNS=tidxtext
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$TXNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$TXNS.drop()" >/dev/null
C "const c=db.getSiblingDB(process.env.TC_DB).$TXNS;
   c.insertMany([{_id:1,title:'red widget',body:'a plain widget',a:1,b:2},
                 {_id:2,title:'blue gadget',body:'red red red',a:3,b:4}]);
   c.createIndex({title:'text',body:'text'},{name:'txt',weights:{title:10,body:1}});
   c.createIndex({'\$**':1},{name:'wild',wildcardProjection:{a:1,b:1}});
   c.createIndex({a:1},{name:'plain_a'})" >/dev/null
chk "the archive carries all three"                "true" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$TXNS.getIndexes().length === 4)")"
startFeeder
startRouter
# A read of an archived document is what copies the collection up, and the
# indexer runs with it.
R "db.getSiblingDB(process.env.TC_DB).$TXNS.findOne({_id:1})" >/dev/null 2>&1
for _ in $(seq 1 30); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).$TXNS.getIndexes().length)")" -ge 4 ] && break
  sleep 1
done
IX() { H "print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).$TXNS.getIndexes().filter(function (i) { return i.name === '$1' })[0] || null))"; }
# The ordinary index is the one that says the batch was not lost wholesale.
chk "the ordinary index arrived"                   "true" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$TXNS.getIndexes().some(function (i) { return i.name === 'plain_a' }))")"
chk "the text index arrived"                       "true" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$TXNS.getIndexes().some(function (i) { return i.name === 'txt' }))")"
chk "and kept the weights that rank a search"      "10" \
    "$(H "const i = db.getSiblingDB(process.env.TC_DB).$TXNS.getIndexes().filter(function (x) { return x.name === 'txt' })[0]; print(i && i.weights ? i.weights.title : 'missing')")"
chk "the wildcard index kept its projection"       "true" \
    "$(H "const i = db.getSiblingDB(process.env.TC_DB).$TXNS.getIndexes().filter(function (x) { return x.name === 'wild' })[0]; print(!!(i && i.wildcardProjection && i.wildcardProjection.a === 1))")"
# And the thing an index exists for: the same search, ranked the same way. With
# title weighted ten to one, the document whose title matches comes first, and a
# copy without the weights puts the other one there.
chk "the router ranks a text search as the archive does" "$(C "print(db.getSiblingDB(process.env.TC_DB).$TXNS.find({\$text:{\$search:'red'}},{score:{\$meta:'textScore'}}).sort({score:{\$meta:'textScore'}}).toArray().map(function (d) { return d._id }).join(','))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$TXNS.find({\$text:{\$search:'red'}},{score:{\$meta:'textScore'}}).sort({score:{\$meta:'textScore'}}).toArray().map(function (d) { return d._id }).join(','))")"
H "db.getSiblingDB(process.env.TC_DB).$TXNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$TXNS.drop()" >/dev/null

echo "== the options an adopted collection carries, which documents do not =="
# A collection is more than its documents, and the fast tier is filled with
# documents. In an adoption the existing cluster becomes the archive carrying
# its collections as they are, and the fast tier's copy is brought into
# existence by the first insert, which carries no options at all.
#
# Two of them decide answers rather than speed. Measured before this was fixed,
# on a collection created with {locale:'en',strength:2}: the application's own
# cluster answered countDocuments({name:'alice'}) with 2 and the router answered
# 1 as soon as the fast tier held the documents. And a write the archive's
# validator rejects was accepted. Neither said anything.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$ONS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ONS.drop()" >/dev/null
C "const d=db.getSiblingDB(process.env.TC_DB);
   d.createCollection('$ONS', {collation:{locale:'en',strength:2},
     validator:{\$jsonSchema:{bsonType:'object',required:['email'],
                properties:{email:{bsonType:'string'}}}}});
   d.$ONS.insertMany([{_id:1,name:'Alice',email:'a@x'},{_id:2,name:'alice',email:'b@x'},
                      {_id:3,name:'BOB',email:'c@x'}]);" >/dev/null
chk "the archive answers case-insensitively"      "2" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$ONS.countDocuments({name:'alice'}))")"
chk "and the fast tier has never held it"         "false" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes('$ONS'))")"
startFeeder
startRouter

chk "the fast tier's copy carries the collation"  "en" \
    "$(H "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$ONS'})[0];
          print(c&&c.options&&c.options.collation?c.options.collation.locale:'none')")"
chk "and the validator"                           "true" \
    "$(H "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$ONS'})[0];
          print(!!(c&&c.options&&c.options.validator))")"
# The behaviour, not the metadata. The documents have to be on the fast tier
# before it can be said to hold them: a point read by _id is what copies one up,
# which is what ordinary traffic does. Claiming completeness over an empty fast
# tier is a false claim, and the zero it then answers is correct rather than the
# bug this is looking for.
R "[1,2,3].forEach(i=>db.getSiblingDB(process.env.TC_DB).$ONS.findOne({_id:i}))" >/dev/null
for _ in $(seq 1 30); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).$ONS.countDocuments({}))")" = "3" ] && break
  sleep 0.5
done
chk "the fast tier holds them after the copy-up"  "3" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$ONS.countDocuments({}))")"
# With the fast tier answering, the same query the application has always run
# must give the same answer it has always given.
cline watermark --config $CFG --ns $DB.$ONS --complete-all >/dev/null 2>&1
sleep 7
chk "the router still answers case-insensitively" "2" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$ONS.countDocuments({name:'alice'}))")"
chk "and still refuses what the validator refuses" "refused" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$ONS.insertOne({_id:9,email:12345});print('ACCEPTED')}catch(e){print('refused')}")"

echo "== the same, when a copy-up is what recreates the collection =="
# Everything above was carried by the router's startup pass, which runs once and
# sees every managed namespace as it was then. An application that drops the
# collection and keeps reading gets the fast tier's copy back from the first
# copy-up, and an insert carries no options at all. Measured before this: the
# collation carried at startup was gone from the recreated copy, and the query
# the application has always run answered 1 where its own cluster answers 2.
R "db.getSiblingDB(process.env.TC_DB).$ONS.drop()" >/dev/null
chk "the fast tier no longer holds it"            "false" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes('$ONS'))")"
chk "and the archive kept its documents"          "3" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$ONS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
R "[1,2,3].forEach(i=>db.getSiblingDB(process.env.TC_DB).$ONS.findOne({_id:i}))" >/dev/null
for _ in $(seq 1 30); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).$ONS.countDocuments({}))")" = "3" ] && break
  sleep 0.5
done
chk "the copy-up recreated it with the collation" "en" \
    "$(H "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$ONS'})[0];
          print(c&&c.options&&c.options.collation?c.options.collation.locale:'none')")"
chk "and with the validator"                      "true" \
    "$(H "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$ONS'})[0];
          print(!!(c&&c.options&&c.options.validator))")"
cline watermark --config $CFG --ns $DB.$ONS --complete-all >/dev/null 2>&1
sleep 7
chk "so the router still answers the same"        "2" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$ONS.countDocuments({name:'alice'}))")"

H "db.getSiblingDB(process.env.TC_DB).$ONS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ONS.drop()" >/dev/null

echo "== the same options going the other way, into the archive =="
# Options travel upward on their own, because an adoption starts with the
# archive holding everything. Downward nothing carried them: a collection
# created through the router exists on the fast tier with its options, and the
# archive's copy is brought into existence by the feeder's first mirrored
# insert, which carries none.
#
# Measured before this: the fast tier answered countDocuments({name:'alice'})
# with 2 and the archive with 1, so every read that fell through, which is every
# read of an evicted document, matched differently from the tier the application
# had been talking to.
R "db.getSiblingDB(process.env.TC_DB).createCollection('$ONS',{collation:{locale:'en',strength:2},
     validator:{\$jsonSchema:{bsonType:'object',required:['email'],
                properties:{email:{bsonType:'string'}}}}})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).$ONS.insertMany([{_id:1,name:'Alice',email:'a@x'},
     {_id:2,name:'alice',email:'b@x'},{_id:3,name:'BOB',email:'c@x'}])" >/dev/null
for _ in $(seq 1 40); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$ONS.countDocuments({}))")" = "3" ] && break
  sleep 0.5
done
chk "the archive's copy carries the collation"    "en" \
    "$(C "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$ONS'})[0];
          print(c&&c.options&&c.options.collation?c.options.collation.locale:'none')")"
chk "and the validator"                           "true" \
    "$(C "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$ONS'})[0];
          print(!!(c&&c.options&&c.options.validator))")"
chk "so the archive answers the same as the fast tier" "2" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$ONS.countDocuments({name:'alice'}))")"
# And what the client sees once the fast tier no longer holds them, which is
# what eviction leaves: the read falls through, and the archive is what answers.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$ONS.deleteMany({})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'})" >/dev/null
startFeeder
startRouter
chk "and the router still answers the same"       "2" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$ONS.countDocuments({name:'alice'}))")"
H "db.getSiblingDB(process.env.TC_DB).$ONS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ONS.drop()" >/dev/null

echo "== a unique index the archive has and the fast tier does not =="
# The router reads the FAST TIER's unique indexes to know what to protect across
# both tiers, so while this is true it accepts a value the archive refuses. The
# feeder cannot mirror that document, says so once in an error nobody is reading
# at the time, and leaves it on the fast tier alone; the evictor will not remove
# it either, because a document the archive does not hold is never evicted.
LNS=tacct
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$LNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$LNS.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+'.$LNS'})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).createCollection('$LNS');
   db.getSiblingDB(process.env.TC_DB).$LNS.createIndex({code:1},{unique:true,name:'code_u'})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).createCollection('$LNS')" >/dev/null
chk "the archive has the constraint, the fast tier does not" "true/false" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$LNS.getIndexes().some(function (i) { return i.name === 'code_u' }))")/$(H "print(db.getSiblingDB(process.env.TC_DB).$LNS.getIndexes().some(function (i) { return i.name === 'code_u' }))")"
# Asked before the router starts, because the router's own indexer builds the
# fast tier's indexes from the archive's and would repair this on the way up.
chk "cline indexes calls it worse than slow"       "true" \
    "$(cline indexes --config $CFG --ns $DB.$LNS 2>&1 | grep -q 'worse than slow' && echo true || echo false)"
rm -f $LOGDIR/lost.log
cline router --config $CFG --log-file $LOGDIR/lost.log >/dev/null 2>&1 & LRP=$!
for _ in $(seq 1 30); do R 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break; sleep 1; done
chk "the router names it and the namespace"        "true" \
    "$(grep -qE "unique index the fast tier does not.*ns=$DB.$LNS" $LOGDIR/lost.log && echo true || echo false)"
chk "and says the document stays on the fast tier" "true" \
    "$(grep -q 'stays on the fast tier alone' $LOGDIR/lost.log && echo true || echo false)"
# And the router repairs it on the way up, which is why the tool was asked
# first: the indexer builds the fast tier's indexes from the archive's.
for _ in $(seq 1 20); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).$LNS.getIndexes().some(function (i) { return i.name === 'code_u' }))")" = "true" ] && break
  sleep 1
done
chk "and the indexer then builds it"               "true" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$LNS.getIndexes().some(function (i) { return i.name === 'code_u' }))")"
kill $LRP 2>/dev/null; wait $LRP 2>/dev/null

echo "== a collection dropped through the router has to be built again =="
# The indexer records a namespace as done once the fast tier has the collection
# and its indexes, and never asks again. A drop makes that untrue. Without being
# told, it noticed only when a catalog listing happened to find the collection
# missing, which is a later and much weaker signal than the drop itself: until
# then the collection comes back with nothing but _id_, every query on it scans,
# and a unique index the archive still carries is not enforced on the fast tier
# at all.
#
# The same router throughout, because the point is what it does without being
# restarted. A restart repairs this and hides it.
DNS=tidxdrop
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.createIndex({code:1},{unique:true,name:'dropcode_u'})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.insertOne({_id:1, code:'a'})" >/dev/null
rm -f $LOGDIR/idxdrop.log
cline router --config $CFG --log-file $LOGDIR/idxdrop.log >/dev/null 2>&1 & DRP=$!
for _ in $(seq 1 30); do R 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break; sleep 1; done
hasIdx() {
  H "print(db.getSiblingDB(process.env.TC_DB).$DNS.getIndexes().some(function (i) { return i.name === 'dropcode_u' }))"
}
for _ in $(seq 1 20); do [ "$(hasIdx)" = "true" ] && break; sleep 1; done
chk "the indexer builds it on the way up"          "true" "$(hasIdx)"
# Now drop it through the router, which is the signal that used to be ignored,
# and let the collection come back.
R "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
# Created again through the router, which is the other half of the signal.
# Ensure creates the collection as well as its indexes, so it must not be called
# on the drop itself: that would put back what the client had just removed. The
# create is where there is something to build.
#
# An implicit create, which is what an insert into a dropped collection is,
# sends no create command and is not covered by this. It is repaired at the next
# catalog listing or router start, and until then reads fall through to the
# archive, which still holds both the documents and the indexes.
R "db.getSiblingDB(process.env.TC_DB).createCollection('$DNS')" >/dev/null 2>&1
R "db.getSiblingDB(process.env.TC_DB).$DNS.insertOne({_id:2, code:'b'})" >/dev/null
for _ in $(seq 1 30); do [ "$(hasIdx)" = "true" ] && break; sleep 1; done
chk "and builds it again after a drop and create"   "true" "$(hasIdx)"
kill $DRP 2>/dev/null; wait $DRP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null

echo "== dropping a unique index has to reach both tiers =="
# DDL is not mirrored, so the drop used to reach the fast tier alone. From then
# on the router accepted a value the archive refused and the document stayed on
# the fast tier alone: measured, the fast tier held two and the archive one.
# The indexer put the index back on the next router start, so the constraint was
# off now and on again later, which is worse than either.
#
# A plain index is a different thing and must not follow. It is a decision about
# speed, the two tiers are allowed to differ, and an operator may keep one on
# the archive for the reads that fall through; cline indexes exists to report
# exactly that.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$LNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$LNS.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+'.$LNS'})" >/dev/null
for U in "$HOT" "$COLD"; do
  mongosh "$U" --quiet --eval "const c=db.getSiblingDB('$DB').$LNS;
    db.getSiblingDB('$DB').createCollection('$LNS');
    c.createIndex({code:1},{unique:true,name:'code_u'});
    c.createIndex({owner:1},{name:'owner_1'})" >/dev/null 2>&1
done
startFeeder
startRouter
R "db.getSiblingDB(process.env.TC_DB).$LNS.insertOne({_id:1, code:'A', owner:'ann'})" >/dev/null
sleep 4
chk "the duplicate is refused while the index is there" "refused" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$LNS.insertOne({_id:2, code:'A'});print('ACCEPTED')}catch(e){print('refused')}")"
R "db.getSiblingDB(process.env.TC_DB).$LNS.dropIndex('code_u')" >/dev/null 2>&1
chk "the archive dropped it too"                   "false" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$LNS.getIndexes().some(function (i) { return i.name === 'code_u' }))")"
# The plain index beside it is untouched, on both tiers.
R "db.getSiblingDB(process.env.TC_DB).$LNS.dropIndex('owner_1')" >/dev/null 2>&1
chk "a plain index is dropped only from the fast tier" "false/true" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$LNS.getIndexes().some(function (i) { return i.name === 'owner_1' }))")/$(C "print(db.getSiblingDB(process.env.TC_DB).$LNS.getIndexes().some(function (i) { return i.name === 'owner_1' }))")"
# And the point of all of it: the write the application is now entitled to make
# reaches the archive.
R "db.getSiblingDB(process.env.TC_DB).$LNS.insertOne({_id:3, code:'A', owner:'bo'})" >/dev/null 2>&1
sleep 6
chk "the duplicate is accepted now"                "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$LNS.countDocuments({code:'A'}))")"
chk "and it reached the archive"                   "2" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$LNS.countDocuments({code:'A',_tcDeletedAt:{\$exists:false}}))")"
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$LNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$LNS.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+'.$LNS'})" >/dev/null

echo "== a tiered collection that is not the same collection on both tiers =="
# The fast tier's copy is created with the archive's options before anything
# inserts into it, which is what keeps a collation working after an adoption.
# Two things get past that: an application insert can create the collection
# first, and a collMod through the router afterwards reaches the fast tier alone
# because DDL is not mirrored. Either way the two stop being the same
# collection, and a collation cannot be added to one that already holds
# documents.
#
# Measured: the archive created with {locale:'en',strength:2} answers
# countDocuments({name:'alice'}) with 2 and a fast tier created by an insert
# answers 1, so the answer depends on which tier served it. Nothing failed.
DNS=tdrift
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "const d=db.getSiblingDB(process.env.TC_DB);
   d.createCollection('$DNS', {collation:{locale:'en',strength:2}});
   d.$DNS.insertMany([{_id:1,name:'Alice'},{_id:2,name:'alice'}])" >/dev/null
# The race, made deterministic: the application's insert creates it first.
H "db.getSiblingDB(process.env.TC_DB).$DNS.insertMany([{_id:1,name:'Alice'},{_id:2,name:'alice'}])" >/dev/null
chk "the two tiers answer the same query differently" "2/1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({name:'alice'}))")/$(H "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({name:'alice'}))")"
rm -f $LOGDIR/drift.log
cline router --config $CFG --log-file $LOGDIR/drift.log >/dev/null 2>&1 & DRP=$!
for _ in $(seq 1 30); do R 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break; sleep 1; done
chk "the router names the collection and the option" "true" \
    "$(grep -qE "ns=$DB.$DNS option=collation" $LOGDIR/drift.log && echo true || echo false)"
chk "and says which tier has what"                 "true" \
    "$(grep -qE 'archive=en/2 fast_tier=none' $LOGDIR/drift.log && echo true || echo false)"
kill $DRP 2>/dev/null; wait $DRP 2>/dev/null
# And a collection that matches must be quiet, or the warning is noise on every
# startup and nobody reads it.
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).createCollection('$DNS', {collation:{locale:'en',strength:2}})" >/dev/null
rm -f $LOGDIR/drift2.log
cline router --config $CFG --log-file $LOGDIR/drift2.log >/dev/null 2>&1 & DRQ=$!
for _ in $(seq 1 30); do R 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break; sleep 1; done
chk "and says nothing once they match"             "0" \
    "$(grep -c "ns=$DB.$DNS" $LOGDIR/drift2.log || true)"
kill $DRQ 2>/dev/null; wait $DRQ 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null

echo "== a validator must not be applied to what moves between the tiers =="
# A validator is a rule about what the application may write, and the fast tier
# applies it when the application writes. What reaches the archive afterwards is
# a copy of a document that already exists, plus the tombstone this project adds
# to record a deletion, and no schema was written with that field in mind.
#
# Measured with an ordinary $jsonSchema carrying additionalProperties: false.
# The first delete of a document in the collection made the archive refuse the
# tombstone, the batch never succeeded, and the feeder reopened its change
# stream on the same event fifteen times in a few seconds. Mirroring stopped for
# every namespace behind it: a write to another collection afterwards never
# reached the archive at all. One ordinary delete, and the archive stops
# receiving anything.
VNS=tval
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$VNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$VNS.drop()" >/dev/null
VSCHEMA="{validator:{\$jsonSchema:{bsonType:'object',required:['_id','name'],properties:{_id:{bsonType:'int'},name:{bsonType:'string'}},additionalProperties:false}}}"
H "db.getSiblingDB(process.env.TC_DB).createCollection('$VNS', $VSCHEMA)" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).createCollection('$VNS', $VSCHEMA)" >/dev/null
# The control: the archive really would refuse a tombstone on its own.
chk "the archive's validator refuses an extra field" "refused" \
    "$(C "const c=db.getSiblingDB(process.env.TC_DB).$VNS;
          c.insertOne({_id:1,name:'ann'});
          let r;
          try{c.updateOne({_id:1},{\$set:{_tcDeletedAt:new Date()}});r='ACCEPTED'}catch(e){r='refused'}
          c.deleteMany({});
          print(r)")"
startFeeder
H "db.getSiblingDB(process.env.TC_DB).$VNS.insertOne({_id:1,name:'ann'})" >/dev/null
for _ in $(seq 1 20); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$VNS.countDocuments({}))")" = "1" ] && break
  sleep 0.5
done
chk "the insert reached the archive"              "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$VNS.countDocuments({}))")"
H "db.getSiblingDB(process.env.TC_DB).$VNS.deleteOne({_id:1})" >/dev/null
sleep 6
chk "and the deletion was recorded as a tombstone" "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$VNS.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
# The consequence that makes this worth a check of its own: everything behind it.
H "db.getSiblingDB(process.env.TC_DB).orders.insertOne({_id:ObjectId(), vprobe:'after'})" >/dev/null
sleep 6
chk "and mirroring is still running behind it"    "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({vprobe:'after'}))")"
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$VNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$VNS.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).orders.deleteMany({vprobe:'after'})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.deleteMany({vprobe:'after'})" >/dev/null

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
