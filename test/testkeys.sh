#!/bin/bash
# Unique keys across the two tiers, on every path that can write one.
#
# A unique index only constrains what it can see, and it cannot see the archive.
# The insert path was guarded first; this suite covers the rest of the ways a
# document comes to hold a value, and the bookkeeping that has to keep up when
# one moves off a value it used to hold.
#
# Every section uses its own collection on purpose. The router caches a
# namespace's unique indexes for a minute, so a name reused with a different
# index is still enforced under the old one, and checks then pass and fail for
# reasons that have nothing to do with what they claim to test. Both compound
# checks below did exactly that before the collections were split up.
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
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-48s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-48s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

# fresh COLLECTION [INDEXSPEC]: empty both tiers and the registry, then index.
fresh() {
  H "db.getSiblingDB(process.env.TC_DB).$1.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$1.drop()" >/dev/null
  H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:'$DB.$1'})" >/dev/null
  [ -n "$2" ] && H "db.getSiblingDB(process.env.TC_DB).$1.createIndex($2,{unique:true})" >/dev/null
}

# stopDaemons waits for them to actually be gone. Running this suite twice back
# to back left the previous router still holding port $PORT, the new one failed
# to bind, and every check through the proxy failed until a later section
# happened to restart it. Twenty-two failures that looked like product bugs.
stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do
    pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0
    sleep 0.5
  done
}

# startRouter refuses to continue if the router is not answering. A suite that
# reports failures when its own fixture never came up is worse than no suite.
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/keys_r.log & RP=$!
  for _ in $(seq 1 30); do
    if mongosh --port $PORT -u dba -p secret --authenticationDatabase admin \
         --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT, so nothing below would mean anything"
  tail -5 $LOGDIR/keys_r.log
  exit 2
}

# waitMirrored COLLECTION COUNT: block until the archive holds COUNT live
# documents. A fixed sleep here is the single largest source of false failures
# in this suite: if mirroring has not landed, the evicted document has no live
# archive copy, its key claim looks stale, and the next write is handed the key
# it should have been refused. That reads as a product bug and is not one.
waitMirrored() {
  local n=""
  for _ in $(seq 1 60); do
    n=$(C "print(db.getSiblingDB(process.env.TC_DB).$1.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$2" ] && return 0
    sleep 0.5
  done
  echo "  WARN  $DB.$1 never reached $2 mirrored documents, last saw ${n:-none}"
}

# Remove documents from the fast tier the way an eviction does, with the feeder
# blind to it, so the archive keeps a live copy. Deleting through the router
# instead would tombstone the archive copy, which frees the key correctly and
# tests the opposite thing.
# startFeeder waits for the checkpoint to reappear, which is the feeder saying
# it has connected and is watching. Sleeping a fixed few seconds instead means
# the writes that follow can land before it is listening, and they are then
# never mirrored.
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/keys_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}

evictBehindTheFeeder() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 20); do
    pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break
    sleep 0.5
  done
  H "$1" >/dev/null
  C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder
}

echo "== setup =="
stopDaemons
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
startRouter

echo "== an update must not take a key held by an evicted document =="
fresh tkupd '{email:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkupd.insertMany([{_id:1,email:"held@x.com"},{_id:2,email:"free@x.com"}])' >/dev/null
waitMirrored tkupd 2
chk "both mirrored" "2" "$(C 'print(db.getSiblingDB(process.env.TC_DB).tkupd.countDocuments({_tcDeletedAt:{$exists:false}}))')"
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkupd.deleteOne({_id:1})'
chk "the archive still holds the evicted one" "true" "$(C 'const d=db.getSiblingDB(process.env.TC_DB).tkupd.findOne({_id:1}); print(!!d && !d._tcDeletedAt)')"
chk "updateOne onto the evicted key rejected"  "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkupd.updateOne({_id:2},{$set:{email:"held@x.com"}});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "findOneAndUpdate rejected"                "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkupd.findOneAndUpdate({_id:2},{$set:{email:"held@x.com"}});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "replaceOne rejected"                      "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkupd.replaceOne({_id:2},{_id:2,email:"held@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
# Both halves of "no duplicate": the fast tier never took the value, and the
# archive still holds exactly the one document that owns it.
chk "  the fast tier kept the old value"       "free@x.com" "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tkupd.findOne({_id:2}); print(d?d.email:"MISSING")')"
chk "  no fast tier document holds it"         "0" "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkupd.countDocuments({email:"held@x.com"}))')"
chk "  exactly one archived document holds it" "1" "$(C 'print(db.getSiblingDB(process.env.TC_DB).tkupd.countDocuments({email:"held@x.com",_tcDeletedAt:{$exists:false}}))')"

echo "== an upsert creating a document must claim its key too =="
fresh tkups '{email:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkups.insertOne({_id:1,email:"taken@x.com"})' >/dev/null
waitMirrored tkups 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkups.deleteOne({_id:1})'
chk "the fast tier is empty"                 "0" "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkups.countDocuments({}))')"
chk "upsert onto the evicted key rejected"   "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkups.updateOne({_id:50},{$set:{email:"taken@x.com"}},{upsert:true});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and it created nothing"               "0" "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkups.countDocuments({_id:50}))')"
chk "upsert onto a free key still works"     "1" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tkups.updateOne({_id:51},{$set:{email:"spare@x.com"}},{upsert:true}).upsertedCount)')"

echo "== a key a document moves off must become reusable =="
# Nothing is evicted here. One document takes a value and then leaves it, so the
# value is unused and the next document must be allowed to have it. A claim that
# only checked its owner still existed would reject this, which is a duplicate
# key error for a duplicate that does not exist.
fresh tkfree '{email:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkfree.insertOne({_id:1,email:"a@x.com"})' >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tkfree.updateOne({_id:1},{$set:{email:"b@x.com"}})' >/dev/null
chk "the fast tier moved it"          "b@x.com" "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tkfree.findOne({_id:1}); print(d?d.email:"MISSING")')"
chk "the freed value can be taken"    "ok"      "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkfree.insertOne({_id:2,email:"a@x.com"});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "the value it moved to is now held" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkfree.insertOne({_id:3,email:"b@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "rewriting a field to its own value works" "1" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tkfree.updateOne({_id:1},{$set:{email:"b@x.com",seen:1}}).modifiedCount)')"

echo "== updates the guard must leave alone =="
chk "a non-indexed field"             "1" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tkfree.updateOne({_id:2},{$set:{note:"hello"}}).modifiedCount)')"
chk "an indexed field, value free"    "1" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tkfree.updateOne({_id:2},{$set:{email:"other@x.com"}}).modifiedCount)')"
chk "unsetting an indexed field"      "1" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tkfree.updateOne({_id:2},{$unset:{email:""}}).modifiedCount)')"

echo "== compound unique index across tiers =="
fresh tkcomp '{a:1,b:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkcomp.insertOne({_id:1,a:1,b:1})' >/dev/null
waitMirrored tkcomp 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkcomp.deleteOne({_id:1})'
chk "compound duplicate of an evicted doc rejected" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcomp.insertOne({_id:2,a:1,b:1});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "a different compound value is accepted"        "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcomp.insertOne({_id:3,a:1,b:2});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
# Only half the key is written, so the other half has to come from the document
# being updated, not from the command.
chk "half-written compound key rejected"            "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcomp.updateOne({_id:3},{$set:{b:1}});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and the document was not changed"            "2"     "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tkcomp.findOne({_id:3}); print(d?d.b:"MISSING")')"

echo "== a missing field is a null key =="
fresh tknull '{email:1}'
R 'db.getSiblingDB(process.env.TC_DB).tknull.insertOne({_id:1,other:1})' >/dev/null
waitMirrored tknull 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tknull.deleteOne({_id:1})'
chk "a second missing-field document rejected" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tknull.insertOne({_id:2,other:2});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo "== a unique index created after the documents were evicted =="
fresh tklate
R 'db.getSiblingDB(process.env.TC_DB).tklate.insertOne({_id:1,email:"late@x.com"})' >/dev/null
waitMirrored tklate 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tklate.deleteOne({_id:1})'
H 'db.getSiblingDB(process.env.TC_DB).tklate.createIndex({email:1},{unique:true})' >/dev/null
cline keys --config $CFG --rebuild >/dev/null 2>&1
# The router caches "this namespace has no unique indexes" for a minute, so a
# newly created one is not enforced across tiers until that expires. Restarting
# it here is the test saying so out loud rather than sleeping through it.
kill $RP 2>/dev/null; wait $RP 2>/dev/null
startRouter
chk "rebuild claims the archived key" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tklate.insertOne({_id:2,email:"late@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo "== sparse unique index: only the documents it indexes =="
# A sparse index does not index a document missing the field, so any number of
# them may exist. Claiming a null key for each would reject the second one, which
# is a duplicate key error for a document the database is happy to accept.
fresh tkspar
H 'db.getSiblingDB(process.env.TC_DB).tkspar.createIndex({email:1},{unique:true,sparse:true})' >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tkspar.insertOne({_id:1,email:"s@x.com"})' >/dev/null
chk "a document with no indexed field is accepted"  "ok" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkspar.insertOne({_id:2,other:1});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "and so is a second one"                        "ok" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkspar.insertOne({_id:3,other:2});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
waitMirrored tkspar 3
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkspar.deleteOne({_id:1})'
chk "a duplicate of the evicted value is rejected"  "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkspar.insertOne({_id:4,email:"s@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "a document outside the index is still accepted" "ok" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkspar.insertOne({_id:5,other:3});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo "== partial unique index: only the documents matching its filter =="
fresh tkpart
H 'db.getSiblingDB(process.env.TC_DB).tkpart.createIndex({email:1},{unique:true,partialFilterExpression:{active:true}})' >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tkpart.insertOne({_id:1,email:"p@x.com",active:true})' >/dev/null
waitMirrored tkpart 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkpart.deleteOne({_id:1})'
chk "a duplicate inside the filter is rejected"  "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkpart.insertOne({_id:2,email:"p@x.com",active:true});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "the same value outside the filter is fine" "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkpart.insertOne({_id:3,email:"p@x.com",active:false});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
# Updating a document into the filter is how it becomes constrained, and the key
# has to be claimed at that moment rather than when it was inserted.
chk "an update moving it into the filter rejected" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkpart.updateOne({_id:3},{$set:{active:true}});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and the document was not changed"           "false" "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tkpart.findOne({_id:3}); print(d?d.active:"MISSING")')"

echo "== updates whose result has to be computed, not read =="
# $inc, $rename and pipeline updates do not say what the field will hold, they
# say how to work it out. The router has the fast tier apply the statement in a
# transaction it then rolls back, so the real value is known before the write is
# allowed. Guessing here would mean either duplicates or refusing valid writes.
fresh tkeval '{sku:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkeval.insertMany([{_id:1,sku:100},{_id:2,sku:5}])' >/dev/null
waitMirrored tkeval 2
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkeval.deleteOne({_id:1})'
chk "\$inc onto an evicted key rejected"   "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkeval.updateOne({_id:2},{$inc:{sku:95}});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and the document was not changed"   "5"     "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tkeval.findOne({_id:2}); print(d?d.sku:"MISSING")')"
chk "\$inc onto a free value still works"  "6"     "$(R 'db.getSiblingDB(process.env.TC_DB).tkeval.updateOne({_id:2},{$inc:{sku:1}}); print(db.getSiblingDB(process.env.TC_DB).tkeval.findOne({_id:2}).sku)')"

fresh tkeval2 '{email:1}'
# Every document carries an email: under a plain unique index only one document
# may be missing the field, so seeding two without it silently loses one.
R 'db.getSiblingDB(process.env.TC_DB).tkeval2.insertMany([{_id:1,email:"r@x.com"},{_id:2,addr:"r@x.com",email:"two@x.com"},{_id:3,email:"three@x.com"}])' >/dev/null
waitMirrored tkeval2 3
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkeval2.deleteOne({_id:1})'
chk "\$rename onto an evicted key rejected" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkeval2.updateOne({_id:2},{$rename:{addr:"email"}});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and the field was not renamed"       "r@x.com" "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tkeval2.findOne({_id:2}); print(d&&d.addr?d.addr:"RENAMED")')"
chk "a pipeline update onto an evicted key rejected" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkeval2.updateOne({_id:3},[{$set:{email:"r@x.com"}}]);print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "a pipeline update to a free value works" "ok"  "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkeval2.updateOne({_id:3},[{$set:{email:"fresh@x.com"}}]);print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and the dry run left nothing behind"  "1"    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkeval2.countDocuments({email:"fresh@x.com"}))')"

echo "== a unique index created on a running router is enforced at once =="
# The cache that remembers a namespace's unique indexes has to be dropped when
# the indexes change, or the constraint goes unenforced across tiers until it
# expires, which is exactly the window in which somebody has just decided the
# field must be unique.
fresh tkddl
R 'db.getSiblingDB(process.env.TC_DB).tkddl.insertOne({_id:1,email:"d@x.com"})' >/dev/null
waitMirrored tkddl 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkddl.deleteOne({_id:1})'
R 'db.getSiblingDB(process.env.TC_DB).tkddl.createIndex({email:1},{unique:true})' >/dev/null
cline keys --config $CFG --rebuild >/dev/null 2>&1
chk "enforced without restarting the router" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkddl.insertOne({_id:2,email:"d@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"

pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/"
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
