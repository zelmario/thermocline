#!/bin/bash
# What a transaction is actually promised when its documents live in two tiers.
#
# A transaction is a snapshot of one cluster. Thermocline puts a second cluster
# behind that one, and everything the router does to keep the tiers consistent
# happens outside the caller's session: key claims go through the router's own
# client, and an update whose result cannot be read off the command is evaluated
# in a transaction of the router's own. Neither can see the caller's uncommitted
# writes, and the caller cannot see theirs.
#
# So the question this suite asks is not "do transactions work". It is: where
# does the guarantee end, and does it end in a refusal or in a wrong answer? A
# refusal is a limitation. A wrong answer is a bug.
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
stopFeeder() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/txn_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/txn_r.log & RP=$!
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
# One collection per section: the router caches a namespace's unique indexes for
# a minute, so a name reused with a different index is still enforced under the
# old one.
fresh() {
  H "db.getSiblingDB(process.env.TC_DB).$1.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$1.drop()" >/dev/null
  H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:'$DB.$1'})" >/dev/null
  [ -n "$2" ] && H "db.getSiblingDB(process.env.TC_DB).$1.createIndex($2,{unique:true})" >/dev/null
  return 0
}
# Remove documents from the fast tier the way an eviction does, with the feeder
# blind to it, so the archive keeps a live copy.
evictBehindTheFeeder() {
  stopFeeder
  H "$1" >/dev/null
  C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder
}

echo "== setup =="
stopDaemons
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({}); db.getSiblingDB(process.env.TC_META).warming.deleteMany({}); db.getSiblingDB(process.env.TC_META).markers.deleteOne({_id:"warmerActive"})' >/dev/null
startFeeder
startRouter

echo "== the cross-tier guard still holds inside a transaction =="
# The owner of the key is in the archive only. The fast tier's own unique index
# cannot see it, so the registry is the only thing standing between this and two
# live documents sharing a key.
fresh ttxa '{email:1}'
R "db.getSiblingDB(process.env.TC_DB).ttxa.insertOne({_id:1,email:'a@x.com'})" >/dev/null
waitMirrored ttxa 1
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).ttxa.deleteMany({})"
chk "duplicate of an archived key refused inside a txn" "11000" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxa;
  s.startTransaction();
  try { c.insertOne({_id:2,email:'a@x.com'}); s.commitTransaction(); print('COMMITTED'); }
  catch(e) { try{s.abortTransaction()}catch(_){} print(e.code||e.codeName||e.message.substring(0,40)); }")"
chk "and the duplicate did not land"                    "0"     "$(H "print(db.getSiblingDB(process.env.TC_DB).ttxa.countDocuments({email:'a@x.com'}))")"

echo "== a write the router refuses inside a transaction aborts it =="
# mongod aborts a transaction on a write error, so the commit after one fails.
# A write the router refuses never reaches mongod, and the transaction stayed
# open: a debit, then an insert refused E11000 here and ignored by the
# application, and the commit succeeded with the debit in it.
R "db.getSiblingDB(process.env.TC_DB).ttxa.insertOne({_id:3, email:'c@x.com', bal:100})" >/dev/null
chk "the commit after a refused insert fails"           "COMMIT FAILED" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxa;
  s.startTransaction();
  c.updateOne({_id:3},{\$inc:{bal:-100}});
  try { c.insertOne({_id:4,email:'a@x.com'}); } catch(e) {}
  try { s.commitTransaction(); print('COMMITTED'); } catch(e) { print('COMMIT FAILED'); }")"
chk "  and the debit before it did not land"            "100" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).ttxa.findOne({_id:3}).bal)")"

echo "== a value the transaction itself freed can be taken again in it =="
# The registry read a key's owner outside the caller's transaction, so a value
# the transaction's own earlier statement freed still read as held: the second
# statement was refused E11000 and the transaction aborted, where MongoDB
# commits both.
fresh ttxa '{email:1}'
R "db.getSiblingDB(process.env.TC_DB).ttxa.insertMany([{_id:1,email:'a1'},{_id:2,email:'b1'},{_id:3,email:'c1'}])" >/dev/null
chk "an update frees a value and another takes it"      "COMMITTED" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxa;
  s.startTransaction();
  try { c.updateOne({_id:1},{\$set:{email:'a2'}}); c.updateOne({_id:2},{\$set:{email:'a1'}}); s.commitTransaction(); print('COMMITTED'); }
  catch(e) { try{s.abortTransaction()}catch(_){} print(e.code||e.message.substring(0,60)); }")"
chk "  and both changed"                                "a2 a1" \
    "$(R "const c=db.getSiblingDB(process.env.TC_DB).ttxa; print(c.findOne({_id:1}).email+' '+c.findOne({_id:2}).email)")"
chk "a delete frees a value and an insert takes it"     "COMMITTED" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxa;
  s.startTransaction();
  try { c.deleteOne({_id:3}); c.insertOne({_id:4,email:'c1'}); s.commitTransaction(); print('COMMITTED'); }
  catch(e) { try{s.abortTransaction()}catch(_){} print(e.code||e.message.substring(0,60)); }")"
chk "  while a value still held is refused"             "11000" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxa;
  s.startTransaction();
  try { c.insertOne({_id:5,email:'a2'}); s.commitTransaction(); print('COMMITTED'); }
  catch(e) { try{s.abortTransaction()}catch(_){} print(e.code||e.message.substring(0,60)); }")"

echo "== an aborted transaction leaves no false duplicate =="
# The claim is written by the router's own client, so an abort cannot roll it
# back. What must not happen is the orphaned claim rejecting the next honest
# insert of that value: nobody holds it, so nobody can be duplicating it.
fresh ttxb '{email:1}'
R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxb;
  s.startTransaction(); c.insertOne({_id:1,email:'b@x.com'}); s.abortTransaction();" >/dev/null
chk "the aborted insert is gone"                        "0"  "$(H "print(db.getSiblingDB(process.env.TC_DB).ttxb.countDocuments({}))")"
chk "an honest insert of that value still succeeds"     "ok" "$(R "
  try { db.getSiblingDB(process.env.TC_DB).ttxb.insertOne({_id:2,email:'b@x.com'}); print('ok'); }
  catch(e) { print(e.code||e.message.substring(0,60)); }")"

echo "== a transaction reads its own writes =="
fresh ttxc
R "db.getSiblingDB(process.env.TC_DB).ttxc.insertMany([{_id:1,v:'a'},{_id:2,v:'b'}])" >/dev/null
waitMirrored ttxc 2
chk "a txn sees its own uncommitted insert"             "z" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxc;
  s.startTransaction();
  c.insertOne({_id:99,v:'z'});
  const d=c.findOne({_id:99});
  const v=d?d.v:'MISSING';
  s.abortTransaction();
  print(v);")"
chk "and the abort took it away"                        "MISSING" "$(R "
  const d=db.getSiblingDB(process.env.TC_DB).ttxc.findOne({_id:99}); print(d?d.v:'MISSING')")"

echo "== a read miss inside a transaction is replayed, not lost =="
# The document is in the archive only. The router cannot copy it into the
# caller's snapshot, so it copies it up outside and asks for the whole
# transaction to be replayed. withTransaction is what makes that invisible.
fresh ttxd
R "db.getSiblingDB(process.env.TC_DB).ttxd.insertOne({_id:1,v:'deep'})" >/dev/null
waitMirrored ttxd 1
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).ttxd.deleteMany({})"
OUT=$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxd;
  let tries=0, got='none';
  s.withTransaction(() => { tries++; const d=c.findOne({_id:1}); got=d?d.v:'MISSING'; });
  print(got+' '+(tries>1));")
chk "withTransaction eventually reads the document"     "deep" "$(echo "$OUT" | awk '{print $1}')"
chk "and it took more than one attempt"                 "true" "$(echo "$OUT" | awk '{print $2}')"

echo "== a range read past the fast tier is served in full =="
# Answering this from the archive would have the transaction reading two
# clusters at two different instants, so that is not on offer. What is: move the
# documents to the tier the transaction is already reading, and replay it. A
# short answer is the one outcome that must never happen.
fresh ttxe
R "db.getSiblingDB(process.env.TC_DB).ttxe.insertMany(Array.from({length:10},(_,i)=>({_id:i,v:i})))" >/dev/null
waitMirrored ttxe 10
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).ttxe.deleteMany({})"
OUT5=$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxe;
  let tries=0, n=-1;
  try { s.withTransaction(() => { tries++; n=c.find({}).toArray().length; }); print(n+' '+(tries>1)); }
  catch(e) { print('REFUSED '+(tries>1)); }")
chk "a txn range read is answered in full"              "10"   "$(echo "$OUT5" | awk '{print $1}')"
chk "and a replay is what made it possible"            "true" "$(echo "$OUT5" | awk '{print $2}')"
chk "the documents are on the fast tier now"           "10"   "$(H 'print(db.getSiblingDB(process.env.TC_DB).ttxe.countDocuments({}))')"

echo "== countDocuments inside a txn is answered, not refused =="
# Every driver implements countDocuments as an aggregate, so it takes the
# aggregation guard rather than the find one. That guard refused inside a
# transaction while find had already learned to warm and replay, which made the
# most common read in the system after find the one shape a transaction could
# not perform.
fresh ttxj
R "db.getSiblingDB(process.env.TC_DB).ttxj.insertMany(Array.from({length:14},(_,i)=>({_id:i,grp:'g',n:i})))" >/dev/null
waitMirrored ttxj 14
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).ttxj.deleteMany({})"
OUTC=$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxj;
  let tries=0, n=-1;
  try { s.withTransaction(() => { tries++; n=c.countDocuments({}); }); print(n+' '+(tries>1)); }
  catch(e) { print('REFUSED '+(tries>1)); }")
chk "countDocuments in a txn counts them all"          "14"   "$(echo "$OUTC" | awk '{print $1}')"
chk "and a replay is what made it possible"           "true" "$(echo "$OUTC" | awk '{print $2}')"
# A grouping pipeline is the same path with a different shape.
chk "a \$group in a txn sees every document"          "14"   "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxj;
  let n=-1;
  try { s.withTransaction(() => { n=c.aggregate([{\$group:{_id:'\$grp',t:{\$sum:1}}}]).toArray()[0].t; }); print(n); }
  catch(e) { print('REFUSED'); }")"

echo "== a multi-document write inside a txn reaches every document =="
# The failure this prevents is the quiet one: updateMany reporting three
# modified when thirteen matched, because ten of them were in the archive.
fresh ttxg
R "db.getSiblingDB(process.env.TC_DB).ttxg.insertMany(Array.from({length:12},(_,i)=>({_id:i,grp:'g',n:i})))" >/dev/null
waitMirrored ttxg 12
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).ttxg.deleteMany({_id:{\$lt:9}})"
OUT6=$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxg;
  let tries=0, mod=-1;
  try { s.withTransaction(() => { tries++; mod=c.updateMany({grp:'g'},{\$set:{touched:1}}).modifiedCount; }); print(mod+' '+(tries>1)); }
  catch(e) { print('REFUSED '+(tries>1)); }")
chk "updateMany inside a txn modified all twelve"      "12"   "$(echo "$OUT6" | awk '{print $1}')"
chk "and it took a replay to reach them"               "true" "$(echo "$OUT6" | awk '{print $2}')"

echo "== a guarded write miss inside a txn still copies the document up =="
# The key guard and the copy-up both act on the same command inside a
# transaction, and an early version of the guard took the command over and
# skipped the copy-up. The update then matched nothing and reported success,
# which is the exact failure the copy-up exists to prevent. It needs a unique
# index on the collection or the guard is never armed and this proves nothing.
fresh ttxh '{email:1}'
R "db.getSiblingDB(process.env.TC_DB).ttxh.insertOne({_id:1,email:'h@x.com',v:0})" >/dev/null
waitMirrored ttxh 1
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).ttxh.deleteMany({})"
OUT7=$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxh;
  let n=-1;
  try { s.withTransaction(() => { n=c.updateOne({_id:1},{\$set:{v:9}}).matchedCount; }); print(n); }
  catch(e) { print('ERR '+e.message.substring(0,40)); }")
chk "the update matched the archived document"           "1" "$(echo "$OUT7" | awk '{print $1}')"
chk "and the new value is on the fast tier"              "9" "$(H "const d=db.getSiblingDB(process.env.TC_DB).ttxh.findOne({_id:1}); print(d?d.v:'MISSING')")"

echo "== a write inside a txn the guard cannot evaluate =="
# The sharp edge. The router works out the key an update will produce by having
# the fast tier apply it in a transaction of the router's own, which cannot see
# the caller's uncommitted writes. Here the document being updated was inserted
# earlier in the same transaction, so the evaluation runs against a fast tier
# where it does not exist, while an archived document already holds the value
# being moved onto.
fresh ttxf '{email:1}'
R "db.getSiblingDB(process.env.TC_DB).ttxf.insertOne({_id:1,email:'z@x.com'})" >/dev/null
waitMirrored ttxf 1
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).ttxf.deleteMany({})"
chk "a duplicate built inside a txn is refused"         "REFUSED" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxf;
  s.startTransaction();
  try {
    c.insertOne({_id:2,email:'tmp@x.com'});
    c.updateOne({_id:2},[{\$set:{email:'z@x.com'}}]);
    s.commitTransaction(); print('COMMITTED');
  } catch(e) { try{s.abortTransaction()}catch(_){} print('REFUSED'); }")"
# Whatever the router decided, this is the invariant that matters: one live
# document per unique value across both tiers.
LIVE_H=$(H "print(db.getSiblingDB(process.env.TC_DB).ttxf.countDocuments({email:'z@x.com'}))")
LIVE_C=$(C "print(db.getSiblingDB(process.env.TC_DB).ttxf.countDocuments({email:'z@x.com',_tcDeletedAt:{\$exists:false}}))")
chk "one live holder of the key across both tiers"      "1" "$((LIVE_H + LIVE_C))"

echo "== the check covers every document, not the first batch of them =="
# The post-image read asks for one batch and no cursor, which is right, but a
# find with singleBatch and no batchSize returns the server's default 101
# documents and closes. An update touching more than that had the rest of its
# post-images never computed, so a duplicate created by the 130th document was
# not found. Only one document here moves onto the archived key, and it is
# deliberately past that boundary.
fresh ttxi '{email:1}'
R "db.getSiblingDB(process.env.TC_DB).ttxi.insertOne({_id:'owner',email:'far@x.com'})" >/dev/null
waitMirrored ttxi 1
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).ttxi.deleteMany({})"
R "const c=db.getSiblingDB(process.env.TC_DB).ttxi; const d=[];
   for(let i=0;i<150;i++) d.push({_id:i, n:i, grp:'g', email:'u'+i+'@x.com'});
   c.insertMany(d);" >/dev/null
chk "the fast tier holds all 150"                      "150" "$(H 'print(db.getSiblingDB(process.env.TC_DB).ttxi.countDocuments({grp:"g"}))')"
# The sentinel is not decoration. The first command of a transaction carries
# startTransaction, and the router deliberately guards that one before the write
# instead of after, because a transaction cannot have written anything yet. Only
# a later statement takes the read-back path this section is about, and without
# something ahead of it this check passed against a build with the bug in it.
chk "a duplicate past the first batch is refused"      "11000" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxi;
  s.startTransaction();
  try {
    c.insertOne({_id:'sentinel',grp:'x',email:'sentinel@x.com'});
    c.updateMany({grp:'g'},[{\$set:{email:{\$cond:[{\$eq:['\$n',130]},'far@x.com','\$email']}}}]);
    s.commitTransaction(); print('COMMITTED');
  } catch(e) { try{s.abortTransaction()}catch(_){} print(e.code||e.message.substring(0,60)); }")"
LIVE_H2=$(H "print(db.getSiblingDB(process.env.TC_DB).ttxi.countDocuments({email:'far@x.com'}))")
LIVE_C2=$(C "print(db.getSiblingDB(process.env.TC_DB).ttxi.countDocuments({email:'far@x.com',_tcDeletedAt:{\$exists:false}}))")
chk "one live holder of that key across both tiers"    "1" "$((LIVE_H2 + LIVE_C2))"

stopDaemons
echo
echo "== a transaction whose read carries its own collation =="
# A transaction cannot read the archive, so the router copies the documents it
# matches up to the fast tier and asks the driver to replay. That copy-up runs
# the caller's filter against the archive, and a filter means different
# documents in a different collation. Every other copy-up path passes the
# caller's collation through. This one did not, so the copy-up compared by bytes
# while the replayed read compared by the collation the caller asked for.
#
# When it matches nothing the router reports completeness, which is the answer
# that makes this a wrong answer rather than a refusal: the transaction then
# reads the fast tier alone and is short.
#
# A collection default would not show it, because the server applies a default
# to a query that names no collation. It takes a query carrying its own.
#
# Its own daemons, because the section above this one ends with stopDaemons. The
# first version of this did not start them: the mirroring wait timed out, every
# read came back ECONNREFUSED, and both checks read as product failures.
startFeeder
startRouter
chk "the router is up for this section"                 "1" \
    "$(R "print(db.adminCommand({ping:1}).ok)")"
fresh ttxk
R "db.getSiblingDB(process.env.TC_DB).ttxk.insertOne({_id:1,name:'Bob'})" >/dev/null
waitMirrored ttxk 1
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).ttxk.deleteMany({})"
chk "the archive holds it and the fast tier does not"   "1/0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).ttxk.countDocuments({_tcDeletedAt:{\$exists:false}}))")/$(H "print(db.getSiblingDB(process.env.TC_DB).ttxk.countDocuments({}))")"
# withTransaction, because a copy-up lands outside the caller's snapshot and the
# router asks for a replay to make it visible. A hand-rolled startTransaction
# never replays: written that way first, this came back WriteConflict from the
# fixed build and 0 from the broken one, which reads as the fix being worse.
TXOUT=$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase(process.env.TC_DB).ttxk;
  let tries=0, n=-1;
  s.withTransaction(() => { tries++;
    n=c.find({name:'bob'}).collation({locale:'en',strength:2}).toArray().length; });
  print(n+' '+(tries>1));")
chk "a collated read inside a txn finds the archived document" "1" \
    "$(echo "$TXOUT" | awk '{print $1}')"
chk "  and a replay is what made it possible"            "true" \
    "$(echo "$TXOUT" | awk '{print $2}')"
# The control, because a check that a read finds one document passes just as
# well when the collation is doing nothing. Same data, untiered, at mongod.
H "db.getSiblingDB(process.env.TC_DB).ttxkctl.drop(); db.getSiblingDB(process.env.TC_DB).ttxkctl.insertOne({_id:1,name:'Bob'})" >/dev/null
chk "  and mongod answers the same read the same way"   "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).ttxkctl.find({name:'bob'}).collation({locale:'en',strength:2}).toArray().length)")"
chk "  while without the collation it finds nothing"    "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).ttxkctl.find({name:'bob'}).toArray().length)")"

echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
