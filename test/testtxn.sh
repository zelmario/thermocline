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
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
stopFeeder() {
  pkill -f 'cline feeder' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/txn_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/txn_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}
waitMirrored() {
  local n=""
  for _ in $(seq 1 60); do
    n=$(C "print(db.getSiblingDB('shop').$1.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$2" ] && return 0
    sleep 0.5
  done
  echo "  WARN  shop.$1 never reached $2 mirrored documents, last saw ${n:-none}"
}
# One collection per section: the router caches a namespace's unique indexes for
# a minute, so a name reused with a different index is still enforced under the
# old one.
fresh() {
  H "db.getSiblingDB('shop').$1.drop()" >/dev/null
  C "db.getSiblingDB('shop').$1.drop()" >/dev/null
  H "db.getSiblingDB('_thermocline').keys.deleteMany({ns:'shop.$1'})" >/dev/null
  [ -n "$2" ] && H "db.getSiblingDB('shop').$1.createIndex($2,{unique:true})" >/dev/null
  return 0
}
# Remove documents from the fast tier the way an eviction does, with the feeder
# blind to it, so the archive keeps a live copy.
evictBehindTheFeeder() {
  stopFeeder
  H "$1" >/dev/null
  C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder
}

echo "== setup =="
stopDaemons
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"}); db.getSiblingDB("_thermocline").watermarks.deleteMany({}); db.getSiblingDB("_thermocline").residency.deleteMany({}); db.getSiblingDB("_thermocline").warming.deleteMany({}); db.getSiblingDB("_thermocline").markers.deleteOne({_id:"warmerActive"})' >/dev/null
startFeeder
startRouter

echo "== the cross-tier guard still holds inside a transaction =="
# The owner of the key is in the archive only. The fast tier's own unique index
# cannot see it, so the registry is the only thing standing between this and two
# live documents sharing a key.
fresh ttxa '{email:1}'
R "db.getSiblingDB('shop').ttxa.insertOne({_id:1,email:'a@x.com'})" >/dev/null
waitMirrored ttxa 1
evictBehindTheFeeder "db.getSiblingDB('shop').ttxa.deleteMany({})"
chk "duplicate of an archived key refused inside a txn" "11000" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase('shop').ttxa;
  s.startTransaction();
  try { c.insertOne({_id:2,email:'a@x.com'}); s.commitTransaction(); print('COMMITTED'); }
  catch(e) { try{s.abortTransaction()}catch(_){} print(e.code||e.codeName||e.message.substring(0,40)); }")"
chk "and the duplicate did not land"                    "0"     "$(H "print(db.getSiblingDB('shop').ttxa.countDocuments({email:'a@x.com'}))")"

echo "== an aborted transaction leaves no false duplicate =="
# The claim is written by the router's own client, so an abort cannot roll it
# back. What must not happen is the orphaned claim rejecting the next honest
# insert of that value: nobody holds it, so nobody can be duplicating it.
fresh ttxb '{email:1}'
R "
  const s=db.getMongo().startSession(); const c=s.getDatabase('shop').ttxb;
  s.startTransaction(); c.insertOne({_id:1,email:'b@x.com'}); s.abortTransaction();" >/dev/null
chk "the aborted insert is gone"                        "0"  "$(H "print(db.getSiblingDB('shop').ttxb.countDocuments({}))")"
chk "an honest insert of that value still succeeds"     "ok" "$(R "
  try { db.getSiblingDB('shop').ttxb.insertOne({_id:2,email:'b@x.com'}); print('ok'); }
  catch(e) { print(e.code||e.message.substring(0,60)); }")"

echo "== a transaction reads its own writes =="
fresh ttxc
R "db.getSiblingDB('shop').ttxc.insertMany([{_id:1,v:'a'},{_id:2,v:'b'}])" >/dev/null
waitMirrored ttxc 2
chk "a txn sees its own uncommitted insert"             "z" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase('shop').ttxc;
  s.startTransaction();
  c.insertOne({_id:99,v:'z'});
  const d=c.findOne({_id:99});
  const v=d?d.v:'MISSING';
  s.abortTransaction();
  print(v);")"
chk "and the abort took it away"                        "MISSING" "$(R "
  const d=db.getSiblingDB('shop').ttxc.findOne({_id:99}); print(d?d.v:'MISSING')")"

echo "== a read miss inside a transaction is replayed, not lost =="
# The document is in the archive only. The router cannot copy it into the
# caller's snapshot, so it copies it up outside and asks for the whole
# transaction to be replayed. withTransaction is what makes that invisible.
fresh ttxd
R "db.getSiblingDB('shop').ttxd.insertOne({_id:1,v:'deep'})" >/dev/null
waitMirrored ttxd 1
evictBehindTheFeeder "db.getSiblingDB('shop').ttxd.deleteMany({})"
OUT=$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase('shop').ttxd;
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
R "db.getSiblingDB('shop').ttxe.insertMany(Array.from({length:10},(_,i)=>({_id:i,v:i})))" >/dev/null
waitMirrored ttxe 10
evictBehindTheFeeder "db.getSiblingDB('shop').ttxe.deleteMany({})"
OUT5=$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase('shop').ttxe;
  let tries=0, n=-1;
  try { s.withTransaction(() => { tries++; n=c.find({}).toArray().length; }); print(n+' '+(tries>1)); }
  catch(e) { print('REFUSED '+(tries>1)); }")
chk "a txn range read is answered in full"              "10"   "$(echo "$OUT5" | awk '{print $1}')"
chk "and a replay is what made it possible"            "true" "$(echo "$OUT5" | awk '{print $2}')"
chk "the documents are on the fast tier now"           "10"   "$(H 'print(db.getSiblingDB("shop").ttxe.countDocuments({}))')"

echo "== a multi-document write inside a txn reaches every document =="
# The failure this prevents is the quiet one: updateMany reporting three
# modified when thirteen matched, because ten of them were in the archive.
fresh ttxg
R "db.getSiblingDB('shop').ttxg.insertMany(Array.from({length:12},(_,i)=>({_id:i,grp:'g',n:i})))" >/dev/null
waitMirrored ttxg 12
evictBehindTheFeeder "db.getSiblingDB('shop').ttxg.deleteMany({_id:{\$lt:9}})"
OUT6=$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase('shop').ttxg;
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
R "db.getSiblingDB('shop').ttxh.insertOne({_id:1,email:'h@x.com',v:0})" >/dev/null
waitMirrored ttxh 1
evictBehindTheFeeder "db.getSiblingDB('shop').ttxh.deleteMany({})"
OUT7=$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase('shop').ttxh;
  let n=-1;
  try { s.withTransaction(() => { n=c.updateOne({_id:1},{\$set:{v:9}}).matchedCount; }); print(n); }
  catch(e) { print('ERR '+e.message.substring(0,40)); }")
chk "the update matched the archived document"           "1" "$(echo "$OUT7" | awk '{print $1}')"
chk "and the new value is on the fast tier"              "9" "$(H "const d=db.getSiblingDB('shop').ttxh.findOne({_id:1}); print(d?d.v:'MISSING')")"

echo "== a write inside a txn the guard cannot evaluate =="
# The sharp edge. The router works out the key an update will produce by having
# the fast tier apply it in a transaction of the router's own, which cannot see
# the caller's uncommitted writes. Here the document being updated was inserted
# earlier in the same transaction, so the evaluation runs against a fast tier
# where it does not exist, while an archived document already holds the value
# being moved onto.
fresh ttxf '{email:1}'
R "db.getSiblingDB('shop').ttxf.insertOne({_id:1,email:'z@x.com'})" >/dev/null
waitMirrored ttxf 1
evictBehindTheFeeder "db.getSiblingDB('shop').ttxf.deleteMany({})"
chk "a duplicate built inside a txn is refused"         "REFUSED" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase('shop').ttxf;
  s.startTransaction();
  try {
    c.insertOne({_id:2,email:'tmp@x.com'});
    c.updateOne({_id:2},[{\$set:{email:'z@x.com'}}]);
    s.commitTransaction(); print('COMMITTED');
  } catch(e) { try{s.abortTransaction()}catch(_){} print('REFUSED'); }")"
# Whatever the router decided, this is the invariant that matters: one live
# document per unique value across both tiers.
LIVE_H=$(H "print(db.getSiblingDB('shop').ttxf.countDocuments({email:'z@x.com'}))")
LIVE_C=$(C "print(db.getSiblingDB('shop').ttxf.countDocuments({email:'z@x.com',_tcDeletedAt:{\$exists:false}}))")
chk "one live holder of the key across both tiers"      "1" "$((LIVE_H + LIVE_C))"

echo "== the check covers every document, not the first batch of them =="
# The post-image read asks for one batch and no cursor, which is right, but a
# find with singleBatch and no batchSize returns the server's default 101
# documents and closes. An update touching more than that had the rest of its
# post-images never computed, so a duplicate created by the 130th document was
# not found. Only one document here moves onto the archived key, and it is
# deliberately past that boundary.
fresh ttxi '{email:1}'
R "db.getSiblingDB('shop').ttxi.insertOne({_id:'owner',email:'far@x.com'})" >/dev/null
waitMirrored ttxi 1
evictBehindTheFeeder "db.getSiblingDB('shop').ttxi.deleteMany({})"
R "const c=db.getSiblingDB('shop').ttxi; const d=[];
   for(let i=0;i<150;i++) d.push({_id:i, n:i, grp:'g', email:'u'+i+'@x.com'});
   c.insertMany(d);" >/dev/null
chk "the fast tier holds all 150"                      "150" "$(H 'print(db.getSiblingDB("shop").ttxi.countDocuments({grp:"g"}))')"
# The sentinel is not decoration. The first command of a transaction carries
# startTransaction, and the router deliberately guards that one before the write
# instead of after, because a transaction cannot have written anything yet. Only
# a later statement takes the read-back path this section is about, and without
# something ahead of it this check passed against a build with the bug in it.
chk "a duplicate past the first batch is refused"      "11000" "$(R "
  const s=db.getMongo().startSession(); const c=s.getDatabase('shop').ttxi;
  s.startTransaction();
  try {
    c.insertOne({_id:'sentinel',grp:'x',email:'sentinel@x.com'});
    c.updateMany({grp:'g'},[{\$set:{email:{\$cond:[{\$eq:['\$n',130]},'far@x.com','\$email']}}}]);
    s.commitTransaction(); print('COMMITTED');
  } catch(e) { try{s.abortTransaction()}catch(_){} print(e.code||e.message.substring(0,60)); }")"
LIVE_H2=$(H "print(db.getSiblingDB('shop').ttxi.countDocuments({email:'far@x.com'}))")
LIVE_C2=$(C "print(db.getSiblingDB('shop').ttxi.countDocuments({email:'far@x.com',_tcDeletedAt:{\$exists:false}}))")
chk "one live holder of that key across both tiers"    "1" "$((LIVE_H2 + LIVE_C2))"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
