#!/bin/bash
# Single-document writes whose filter is not an _id.
#
# The write path resolves a miss in two places, and between them is a gap.
# interceptWrite acts on the way back, but only when the filter pins an exact
# _id. guardMultiWrite acts before the write and takes any filter, but only for
# statements that name many documents: multi:true for an update, limit:0 for a
# delete.
#
# A deleteOne or updateOne filtered on anything else falls between the two. The
# fast tier matches nothing, reports zero, and the archived document is left
# where it is. For a delete that is the worst possible answer, because an erasure
# request that reports success without erasing is the one failure nobody can
# audit their way out of afterwards.
#
# An insert is the mirror image: an _id the archive holds is not on the fast tier
# any more, so the insert succeeds where one cluster would have raised E11000,
# and the feeder then mirrors the new document over the archived one.
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
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/sing_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/sing_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}
# Archive only, which is what an evicted document looks like.
seed() { C "db.getSiblingDB(process.env.TC_DB).$1.insertOne($2)" >/dev/null; }
# Live means present in the archive and not tombstoned.
liveInCold() { C "const d=db.getSiblingDB(process.env.TC_DB).$1.findOne({_id:$2}); print(!!d && !d._tcDeletedAt)"; }

echo "== setup =="
stopDaemons
for n in tsdel tsupd tsins; do
  H "db.getSiblingDB(process.env.TC_DB).$n.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$n.drop()" >/dev/null
done
C "db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({}); db.getSiblingDB(process.env.TC_META).evicting.deleteMany({})" >/dev/null
startFeeder
startRouter

seed tsdel "{_id:1, email:'a@x', note:'by id'}"
seed tsdel "{_id:2, email:'b@x', note:'by field'}"
seed tsupd "{_id:1, email:'c@x', name:'carol', balance:100}"
chk "the fast tier holds none of them"                  "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tsdel.countDocuments({}))")"

echo "== deleteOne by _id, which is the covered path =="
chk "it deletes one"                                    "1" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tsdel.deleteOne({_id:1}).deletedCount)")"
sleep 5
chk "and the archive stops returning it"                "false" "$(liveInCold tsdel 1)"

echo "== deleteOne by a field that is not _id =="
# Same statement, same document, a filter the guards do not recognise.
chk "it deletes one"                                    "1" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tsdel.deleteOne({email:'b@x'}).deletedCount)")"
sleep 5
chk "and the archive stops returning it"                "false" "$(liveInCold tsdel 2)"
chk "the document is gone from a read as well"          "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tsdel.countDocuments({email:'b@x'}))")"

echo "== updateOne by a field that is not _id =="
UPD=$(R "print(db.getSiblingDB(process.env.TC_DB).tsupd.updateOne({email:'c@x'},{\$set:{balance:250}}).matchedCount)")
chk "it matches the archived document"                  "1" "$UPD"
chk "and the change is visible"                         "carol-250" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tsupd.findOne({email:'c@x'}); print(d?d.name+'-'+d.balance:'MISSING')")"
sleep 5
chk "and reached the archive"                           "250" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).tsupd.findOne({_id:1}); print(d?d.balance:'MISSING')")"

echo "== findOneAndDelete by a field that is not _id =="
seed tsdel "{_id:3, email:'d@x', note:'fam'}"
chk "it returns the archived document"                  "d@x" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tsdel.findOneAndDelete({email:'d@x'}); print(d?d.email:'MISSING')")"
sleep 5
chk "and the archive stops returning it"                "false" "$(liveInCold tsdel 3)"

echo "== several single-document statements in one command =="
# The reply resolves a single-document statement only when it is the command's
# only one, because n says whether that one missed. With several, n is their
# total: two deleteOnes by _id of archived documents answered deletedCount 0
# and both stayed readable. A driver's bulkWrite sends exactly this.
seed tsdel "{_id:10, email:'m1@x'}"
seed tsdel "{_id:11, email:'m2@x'}"
seed tsupd "{_id:10, email:'u1@x', balance:1}"
seed tsupd "{_id:11, email:'u2@x', balance:1}"
chk "two deleteOnes of archived documents delete both"  "2" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tsdel.bulkWrite([{deleteOne:{filter:{_id:10}}},{deleteOne:{filter:{email:'m2@x'}}}]).deletedCount)")"
chk "  and neither is readable after"                   "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tsdel.countDocuments({_id:{\$in:[10,11]}}))")"
chk "two updateOnes of archived documents modify both"  "2" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tsupd.bulkWrite([{updateOne:{filter:{_id:10},update:{\$inc:{balance:1}}}},{updateOne:{filter:{email:'u2@x'},update:{\$inc:{balance:1}}}}]).modifiedCount)")"
chk "  each once"                                       "2,2" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tsupd.find({_id:{\$in:[10,11]}}).sort({_id:1}).toArray().map(d=>d.balance).join(','))")"

echo "== insert of an _id the archive still holds =="
# One cluster answers E11000 here. Anything else both hides the collision and
# lets the feeder mirror the new document over the archived one.
seed tsins "{_id:9, name:'dave', tier:'gold', balance:100}"
INS=$(R "try { db.getSiblingDB(process.env.TC_DB).tsins.insertOne({_id:9, balance:5}); print('ACCEPTED'); }
         catch(e) { print(/E11000|duplicate key/i.test(e.message)?'DUPLICATE':'other: '+e.message.substring(0,50)); }")
chk "it is refused as a duplicate"                      "DUPLICATE" "$INS"
sleep 5
chk "and the archived document is intact"               "dave-gold-100" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).tsins.findOne({_id:9}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"

echo "== an _id the fast tier holds is mongod's to refuse, not this router's =="
# The guard above exists for an _id that lives only in the archive, which is the
# one mongod cannot see. An _id the fast tier already holds is a different
# question and mongod is the authority on it: its own index answers, and if the
# insert is a driver's automatic retry of a write that already applied, mongod
# answers it with the recorded result instead of an error.
#
# Answering it here took that away. The client sees the same E11000 either way,
# so the difference is only visible in who counted it: duplicate_keys moves when
# this router refuses and stays still when mongod does.
DUPNS=tsdup
H "db.getSiblingDB(process.env.TC_DB).$DUPNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DUPNS.drop()" >/dev/null
# Both tiers hold _id 1; only the archive holds _id 2.
C "db.getSiblingDB(process.env.TC_DB).$DUPNS.insertMany([{_id:1,n:'both'},{_id:2,n:'archive only'}])" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$DUPNS.insertOne({_id:1,n:'both'})" >/dev/null
# The counter the router keeps of refusals it made itself, read from its own
# dashboard the way testdaemonmetrics.sh reads it.
dups() { curl -s http://127.0.0.1:$PDASH/metrics 2>/dev/null | awk '/^thermocline_duplicate_keys_total /{print $2}'; }
BEFORE=$(dups)
INS1=$(R "try { db.getSiblingDB(process.env.TC_DB).$DUPNS.insertOne({_id:1,n:'again'}); print('ACCEPTED'); }
          catch(e) { print(/E11000|duplicate key/i.test(e.message)?'DUPLICATE':'other'); }")
MID=$(dups)
chk "an _id both tiers hold is still refused"           "DUPLICATE" "$INS1"
chk "and this router did not count it as its own"       "true" \
    "$([ -n "$BEFORE" ] && [ "$BEFORE" = "$MID" ] && echo true || echo "false ($BEFORE then $MID)")"
INS2=$(R "try { db.getSiblingDB(process.env.TC_DB).$DUPNS.insertOne({_id:2,n:'again'}); print('ACCEPTED'); }
          catch(e) { print(/E11000|duplicate key/i.test(e.message)?'DUPLICATE':'other'); }")
AFTER=$(dups)
chk "an _id only the archive holds is refused too"      "DUPLICATE" "$INS2"
# The control that makes the check above mean something: the counter does move
# when the refusal is this router's, so a counter that never moves is not what
# the first check is reading.
chk "and that one this router did count"                "true" \
    "$([ -n "$AFTER" ] && [ "$AFTER" -gt "$MID" ] 2>/dev/null && echo true || echo "false ($MID then $AFTER)")"
chk "the archived document is untouched"                "archive only" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).$DUPNS.findOne({_id:2}); print(d?d.n:'MISSING')")"
H "db.getSiblingDB(process.env.TC_DB).$DUPNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DUPNS.drop()" >/dev/null

echo "== and an insert of an id nobody holds still works =="
chk "a fresh id is accepted"                            "1" \
    "$(R "db.getSiblingDB(process.env.TC_DB).tsins.insertOne({_id:10, name:'erin'}); print(db.getSiblingDB(process.env.TC_DB).tsins.countDocuments({_id:10}))")"

echo "== a sort says which of several matches the caller means =="
# The two paths above repair a write that matched nothing: the fast tier applied
# nothing, so the statement can be run again once the document is here. A sorted
# findAndModify that matched *something* reaches neither. The fast tier answers
# with the first of the part it happens to hold, and that is a different
# document from the one the caller asked for.
#
# Measured: an archive holding seq 1, 2 and 3 with only 2 and 3 on the fast
# tier. findOneAndUpdate({}, ..., {sort:{seq:1}}) modified seq 2 and reported
# it, and findOneAndDelete with the same sort deleted seq 2. One document
# matched, one was modified, and the count was right.
SNS=tssort; export TC_SNS=$SNS
H "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$SNS.insertMany([{_id:1,seq:1,who:'oldest'},{_id:2,seq:2,who:'middle'},{_id:3,seq:3,who:'newest'}])" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$SNS.insertMany([{_id:2,seq:2,who:'middle'},{_id:3,seq:3,who:'newest'}])" >/dev/null
chk "the fast tier holds the recent two only"          "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$SNS.countDocuments({}))")"
# The control, and the whole standard: whatever the archive would pick on its
# own is the right answer, because the archive holds the collection.
chk "the archive would pick the oldest"                "oldest" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$SNS.find({}).sort({seq:1}).limit(1).toArray()[0].who)")"
chk "and so does a sorted findOneAndUpdate"            "oldest" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$SNS.findOneAndUpdate({},{\$set:{marked:true}},{sort:{seq:1}}); print(d?d.who:'null')")"
chk "the document it marked is that one"               "oldest" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).$SNS.findOne({marked:true}); print(d?d.who:'none')")"
chk "and only one was marked"                          "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$SNS.countDocuments({marked:true}))")"
chk "a sorted findOneAndDelete takes the oldest too"   "oldest" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$SNS.findOneAndDelete({},{sort:{seq:1}}); print(d?d.who:'null')")"
sleep 5
chk "and the archive recorded that deletion"           "true" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).$SNS.findOne({who:'oldest'}); print(!!(d && d._tcDeletedAt))")"
# The other direction of the same sort, which the fast tier would have got right
# on its own. It must still be right, or the fix is just a different wrong one.
chk "sorting the other way takes the newest"           "newest" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$SNS.findOneAndUpdate({},{\$set:{last:true}},{sort:{seq:-1}}); print(d?d.who:'null')")"
# And a findAndModify with no sort is untouched: MongoDB promises any match, so
# the fast tier's answer is as good as the archive's and no archive lookup is
# owed for it.
chk "an unsorted one still answers from the fast tier" "true" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$SNS.findOneAndUpdate({seq:{\$gte:0}},{\$set:{any:true}}); print(d!==null)")"
# And inside a transaction, which is the awkward half. The copy-up is an
# ordinary write outside the transaction, so this transaction's snapshot was
# taken before it landed and repeating the statement would still miss the
# document. The whole transaction is replayed instead, which is the contract
# every driver already implements for TransientTransactionError.
H "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$SNS.insertMany([{_id:1,seq:1,who:'oldest'},{_id:2,seq:2,who:'middle'},{_id:3,seq:3,who:'newest'}])" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$SNS.insertMany([{_id:2,seq:2,who:'middle'},{_id:3,seq:3,who:'newest'}])" >/dev/null
TXNPICK=$(mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval '
const s = db.getMongo().startSession();
let picked = "none", tries = 0;
try {
  s.withTransaction(function () {
    tries++;
    const d = s.getDatabase(process.env.TC_DB)[process.env.TC_SNS]
      .findOneAndUpdate({}, {$set:{txnmark:true}}, {sort:{seq:1}});
    picked = d ? d.who : "null";
  });
  print(picked + ":" + tries);
} catch (e) { print("ERROR " + e.message.substring(0,60)); }
s.endSession();' 2>&1 | tail -1)
chk "a transaction picks it too, after one replay" "oldest:2" "$TXNPICK"
chk "and that is the document it marked"           "oldest" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).$SNS.findOne({txnmark:true}); print(d?d.who:'none')")"
H "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null

echo "== a queue taken from the archive one job after another =="
# What a warehouse does all day: take the oldest pending job, mark it, take the
# next. The router used to wait for mirroring before choosing which archived job
# the sort means. Now the fast tier's oplog says which of the archive's best few
# changed since the feeder's position, and the first one nothing touched is the
# one. See archivedMatchUnwaited. Taken back to back, the archive still shows the
# job just taken as pending, because its change has not been mirrored yet: that
# job has to be passed over, or the same job is offered twice and the queue
# stalls. One job is deleted through the router first, so the case where a
# candidate was deleted, and the read has to wait, is in it too.
H "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$SNS.insertMany(Array.from({length:20},(_,k)=>({_id:k+1,seq:k+1,status:'pending'})))" >/dev/null
TAKEN=$(R "
  const c = db.getSiblingDB(process.env.TC_DB)[process.env.TC_SNS];
  c.deleteOne({_id:3});
  const got = [];
  for (let i = 0; i < 19; i++) {
    const d = c.findOneAndUpdate({status:'pending'}, {\$set:{status:'done'}}, {sort:{seq:1}});
    got.push(d ? d._id : 'null');
  }
  print(got.join(','));")
chk "every job is taken once, oldest first" "1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20" "$TAKEN"
chk "  and none is left pending"            "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB)[process.env.TC_SNS].countDocuments({status:'pending'}))")"
H "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null

echo "== a queue whose old jobs only the archive holds, under a claim =="
# The adoption's shape: jobs from before the claim live only in the archive, and
# newer ones in both tiers. A job the archive names inside the fast tier's claim
# is there and stays there while mongod runs the command, so it is neither held
# nor copied up (guardSortedFindAndModify). One older than the claim still has
# to be copied up first, or the fast tier's own sort takes the oldest job it
# holds, a newer one. Without the claim this path never ran, and a router that
# skipped the copy-up for every job passed the queue above.
QNS=tmenag2
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$QNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$QNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteOne({_id:'$DB.$QNS'})" >/dev/null
OLDJ="Array.from({length:5},(_,k)=>({_id:k+1, createdAt:new Date(Date.UTC(2024,k,1)), status:'pending'}))"
NEWJ="Array.from({length:3},(_,k)=>({_id:101+k, createdAt:new Date(Date.UTC(2025,6+k,1)), status:'pending'}))"
C "db.getSiblingDB(process.env.TC_DB).$QNS.insertMany($OLDJ.concat($NEWJ))" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$QNS.insertMany($NEWJ)" >/dev/null
cline watermark --config $CFG --ns $DB.$QNS --complete-from 2025-06-01T00:00:00Z >/dev/null 2>&1
startFeeder
startRouter
famClaimed() { curl -s http://127.0.0.1:$PDASH/api/snapshot | grep -o '"fam_claimed"[: ]*[0-9]*' | grep -o '[0-9]*$'; }
F0=$(famClaimed)
QTAKEN=$(R "
  const c = db.getSiblingDB(process.env.TC_DB).$QNS;
  const got = [];
  for (let i = 0; i < 8; i++) {
    const d = c.findOneAndUpdate({status:'pending'}, {\$set:{status:'done'}}, {sort:{createdAt:1}});
    got.push(d ? d._id : 'null');
  }
  print(got.join(','));")
chk "the archive's old jobs first, then the claimed ones" "1,2,3,4,5,101,102,103" "$QTAKEN"
# How many skipped the hold and the copy-up, said rather than checked: it is an
# optimisation, and a claim that cannot be read in time under a full sweep takes
# the slower path, which is still right. The order above is what catches a
# router skipping them outside the claim.
echo "      $(( $(famClaimed) - ${F0:-0} )) of the claimed jobs were taken without a hold or a copy-up"
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$QNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$QNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteOne({_id:'$DB.$QNS'})" >/dev/null

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
