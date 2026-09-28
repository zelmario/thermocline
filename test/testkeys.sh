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
  # Name the cause the timeout usually has. A fixture whose namespace is not in
  # feeder.namespaces mirrors nothing, and every check below it then reads as
  # the product letting something through: shop.tkcollmod cost two sessions that
  # way, once diagnosed as a missing feeder and once as a broken collMod guard.
  if ! grep -q "\"$DB.$1\"" "$CFG"; then
    echo "  FATAL $DB.$1 is not in feeder.namespaces in $CFG, so nothing mirrors it"
    echo "        every check in this section is about an archive that stayed empty"
    exit 1
  fi
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

# The upsert above names the key in its update document. This one does not: it
# names it in the filter, which is where mongod takes it from when it creates
# the document. The guard read the update document alone to decide whether a
# statement could touch a unique index, so this shape was never checked at all.
# Measured on the pilot against a live archived comment's key: accepted, the
# archive then refused to mirror it with E11000, and a read of the key answered
# 1 where two documents existed.
chk "upsert taking the key from its filter rejected" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkups.updateOne({email:"taken@x.com",tag:"filter"},{$setOnInsert:{n:1}},{upsert:true});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and it created nothing"               "0" "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkups.countDocuments({tag:"filter"}))')"

# An insert that lets the server name the document has no _id to record as the
# claim's owner. bson cannot marshal that at all, and the guard's answer to a
# registry it cannot write is to let the write through, so the one case with no
# owner was the one case nothing was enforced.
chk "insert with no _id onto the evicted key rejected" "11000" "$(R 'const r=db.getSiblingDB(process.env.TC_DB).runCommand({insert:"tkups",documents:[{email:"taken@x.com",tag:"noid"}]}); print(r.writeErrors?r.writeErrors[0].code:"ACCEPTED")')"
chk "  and it created nothing"               "0" "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkups.countDocuments({tag:"noid"}))')"
chk "insert with no _id onto a free key works" "1" "$(R 'const r=db.getSiblingDB(process.env.TC_DB).runCommand({insert:"tkups",documents:[{email:"noid@x.com",tag:"noid2"}]}); print(r.writeErrors?r.writeErrors[0].code:r.n)')"

# A claim that records no owner must still be resolvable, or the value it holds
# is blocked for ever: nothing could prove it free again.
R 'db.getSiblingDB(process.env.TC_DB).tkups.updateOne({email:"reuse@x.com",tag:"reuse"},{$setOnInsert:{n:1}},{upsert:true})' >/dev/null
chk "the same upsert again is not refused by its own claim" "0" "$(R 'try{print(db.getSiblingDB(process.env.TC_DB).tkups.updateOne({email:"reuse@x.com",tag:"reuse"},{$set:{n:2}},{upsert:true}).upsertedCount||0)}catch(e){print(e.code||e.message.substring(0,40))}')"
R 'db.getSiblingDB(process.env.TC_DB).tkups.deleteMany({tag:"reuse"})' >/dev/null
chk "and the key it gave up is usable again" "ok" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkups.insertOne({_id:60,email:"reuse@x.com"});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo "== what a unique index the archive lacks costs the write path =="
# The router already said such an index costs reads. The half it did not say is
# that it costs writes: establishing that a key is free is a question about every
# live document, so a write whose key the fast tier does not hold asks the
# archive, and with no index there that is a collection scan of the tier built to
# be the big one. Measured at 200,003 archived documents, for a key nobody holds:
# 38-40ms against 1-10ms with an index over the same field.
#
# The answer is right either way, which is why nothing had noticed. What is
# checked here is that the router says so at the one moment an operator can act
# on it, and names the unique index rather than lumping it in with the plain ones.
# Through the router, not with fresh(), which builds the index straight at the
# fast tier's mongod where no router ever sees the command.
H "db.getSiblingDB(process.env.TC_DB).tkucost.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tkucost.drop()" >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tkucost.createIndex({email:1},{unique:true})' >/dev/null
sleep 2
chk "the router names the write cost of a unique index" "true" \
    "$(grep -q 'costs writes as well as reads' $LOGDIR/keys_r.log && echo true || echo false)"
chk "and names the index it means"                      "true" \
    "$(grep -A0 'costs writes as well as reads' $LOGDIR/keys_r.log | grep -q 'email' && echo true || echo false)"
# Creation time is once, to whoever was reading the log that minute. An operator
# who built the index months ago and wants to know what it costs has to be able
# to ask, so the same thing is reported by every router start and by preflight,
# which is the command the documents tell them to run.
PRE=$(cline preflight --config $CFG 2>&1)
chk "preflight says it too, without starting anything" "true" \
    "$(grep -q 'no index over the fields of a unique index' <<<"$PRE" && echo true || echo false)"
chk "and names the namespace"                          "true" \
    "$(grep -q 'tkucost' <<<"$PRE" && echo true || echo false)"

echo "== a \$merge writes documents too, so it claims their keys =="
# The target guard brings archived documents up so the merge matches them, which
# settles the fields it matches *on*. It settles nothing about any other unique
# index on the target: the archive holds that key, the fast tier's index cannot
# see it, and $merge inserts. Measured before this was checked: MongoDB refused
# the statement with E11000, the router accepted it, the archive then refused to
# mirror the document, and a count of the key answered 1 where two existed.
fresh tkmrg '{email:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkmrg.insertOne({_id:1,email:"taken@x.com",n:1})' >/dev/null
waitMirrored tkmrg 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkmrg.deleteOne({_id:1})'
chk "the fast tier is empty"                  "0" "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkmrg.countDocuments({}))')"

# MRG DOCUMENT MERGESPEC: run one $merge through the router and say what it did.
MRG() {
  R "try{db.getSiblingDB(process.env.TC_DB).aggregate([{\$documents:[$1]},{\$merge:$2}]).toArray();print('ACCEPTED')}catch(e){print(e.code||e.message.substring(0,40))}"
}
BY_ID="{into:'tkmrg',on:'_id',whenMatched:'merge',whenNotMatched:'insert'}"
chk "an insert holding the archived key rejected" "11000" "$(MRG '{_id:2,email:"taken@x.com",n:2}' "$BY_ID")"
chk "  and it created nothing"                "0" "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkmrg.countDocuments({_id:2}))')"
chk "whenMatched replace onto that key rejected" "11000" "$(MRG '{_id:3,email:"taken@x.com",n:3}' "{into:'tkmrg',on:'_id',whenMatched:'replace',whenNotMatched:'insert'}")"
chk "a free key still merges in"              "ACCEPTED" "$(MRG '{_id:4,email:"free@x.com",n:4}' "$BY_ID")"
chk "the archived document is still updatable" "ACCEPTED" "$(MRG '{_id:1,email:"taken@x.com",n:9}' "$BY_ID")"
chk "  and it did not become a duplicate"     "1" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tkmrg.countDocuments({email:"taken@x.com"}))')"
chk "matching on the unique field is allowed" "ACCEPTED" "$(MRG '{email:"taken@x.com",n:8}' "{into:'tkmrg',on:'email',whenMatched:'merge',whenNotMatched:'insert'}")"
chk "whenNotMatched discard writes nothing"   "ACCEPTED" "$(MRG '{_id:5,email:"taken@x.com",n:5}' "{into:'tkmrg',on:'_id',whenMatched:'merge',whenNotMatched:'discard'}")"
chk "  so nothing was created for it"         "0" "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkmrg.countDocuments({_id:5}))')"

# mongod matches a number against the same number written another way, so a
# $merge whose `on` value is a double updates the document stored with a long.
# The guard identified the documents it had read by their raw bytes, which calls
# those two different, so the merge was read as creating a document that takes a
# key another document already holds: refused with E11000 where MongoDB merges.
fresh tkmrg2 '{acct:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkmrg2.insertOne({_id:1,acct:NumberLong(7),v:"first"})' >/dev/null
waitMirrored tkmrg2 1
chk "a numeric key written another way still matches" "ACCEPTED" "$(MRG '{acct:Double(7),v:"merged"}' "{into:'tkmrg2',on:'acct',whenMatched:'merge',whenNotMatched:'insert'}")"
chk "  and it merged rather than duplicating"        "1" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tkmrg2.countDocuments({}))')"
chk "  onto the document that was there"            "merged" "$(R 'const d=db.getSiblingDB(process.env.TC_DB).tkmrg2.findOne({_id:1}); print(d?d.v:"MISSING")')"

echo "== a \$out writes documents too, so it claims their keys =="
# $out clears both tiers first, so nothing it writes can duplicate what was
# there. The claims are for afterwards: a document written by $out and later
# evicted lives in the archive alone, and without a claim the registry has
# nothing to enforce. Measured before this: a $out left no rows in the registry
# at all, and once the evictor had taken those documents off the fast tier an
# insert of one of their keys was accepted, leaving one live copy in the archive
# and another on the fast tier that the archive refused to mirror.
fresh tkmrg '{email:1}'
chk "\$out into a tiered collection accepted" "ok" "$(R 'try{db.getSiblingDB(process.env.TC_DB).aggregate([{$documents:[{_id:1,email:"o1@x.com"},{_id:2,email:"o2@x.com"}]},{$out:"tkmrg"}]).toArray();print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and it claimed both keys"             "2" "$(H 'print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+".tkmrg"}))')"
waitMirrored tkmrg 2
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkmrg.deleteMany({})'
chk "  the fast tier is empty again"         "0" "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkmrg.countDocuments({}))')"
chk "  and a duplicate of one is refused"    "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkmrg.insertOne({_id:9,email:"o1@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo "== the registry has to stop growing, without forgetting what matters =="
# A claim is permanent on purpose: evicting a document takes its index entry off
# the fast tier, and the claim is what remembers the value was taken. Nothing
# removed one whose document had been deleted from both tiers, and the registry
# lives on the fast tier, which is the small expensive one. Measured on 500
# inserts and 500 deletes: nought documents on either tier, 500 claims still
# there, which is history-shaped growth on the wrong tier.
#
# What makes pruning safe is that it asks the question a collision asks: does any
# live document still hold this value. The claim that must survive is the evicted
# one, which is the only reason the registry exists.
fresh tkprune '{email:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkprune.insertMany([{_id:1,email:"pev@x.com"},{_id:2,email:"pdel@x.com"},{_id:3,email:"plive@x.com"}])' >/dev/null
waitMirrored tkprune 3
R 'db.getSiblingDB(process.env.TC_DB).tkprune.deleteOne({_id:2})' >/dev/null
waitMirrored tkprune 2
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkprune.deleteOne({_id:1})'
chk "three keys are claimed to begin with"     "3" \
    "$(H 'print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+".tkprune"}))')"
chk "the fast tier holds one of the three"     "1" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkprune.countDocuments({}))')"
PRUNED=$(cline keys --config $CFG --prune 2>&1 | tail -2 | tr '\n' ' ')
chk "pruning removes the deleted one only"     "2" \
    "$(H 'print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+".tkprune"}))')"
chk "and says what it removed"                 "true" \
    "$(grep -q 'removed 1 claim' <<<"$PRUNED" && echo true || echo false)"
# The two that matter, after the prune: the evicted document's key is still
# defended, and the deleted document's key is free.
chk "a duplicate of the evicted key is refused" "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkprune.insertOne({_id:9,email:"pev@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "and the deleted key can be taken again"    "ok" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkprune.insertOne({_id:10,email:"pdel@x.com"});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
# Twice in a row removes nothing more: it is the state that decides, not the run.
cline keys --config $CFG --prune >/dev/null 2>&1
chk "a second prune removes nothing"            "3" \
    "$(H 'print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+".tkprune"}))')"

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

echo "== a key a deleted document held must become reusable =="
# The other way a value stops being held, and the one an application reaches by
# cancelling something. The helper at the top of this file says a delete through
# the router tombstones the archive copy "which frees the key correctly", and
# avoids that path on purpose so the eviction case can be tested. Nothing then
# checked the sentence.
#
# It matters because a claim is permanent by design: evicting a document takes
# its index entry off the fast tier and the claim is what remembers the value
# was taken. A delete has to be told apart from that, or a cancelled order's
# reference is refused for ever and the registry is the only thing that knows.
fresh tkdel '{ref:1}'
R "db.getSiblingDB(process.env.TC_DB).tkdel.insertOne({_id:1,ref:'Ref000001'})" >/dev/null
waitMirrored tkdel 1
# Archive-only, so the fast tier's own index cannot see the holder and the
# registry is what refuses the next one.
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).tkdel.deleteOne({_id:1})"
chk "the archive holds it and the fast tier does not" "1/0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tkdel.countDocuments({ref:'Ref000001',_tcDeletedAt:{\$exists:false}}))")/$(H "print(db.getSiblingDB(process.env.TC_DB).tkdel.countDocuments({ref:'Ref000001'}))")"
chk "a second document cannot take it while it is held" "11000" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).tkdel.insertOne({_id:2,ref:'Ref000001'});print('ACCEPTED')}catch(e){print(e.code||e.message.substring(0,40))}")"
# The application cancels it. This goes through the router, which marks the
# archive's copy deleted rather than removing it.
R "db.getSiblingDB(process.env.TC_DB).tkdel.deleteOne({_id:1})" >/dev/null
sleep 2
chk "  and afterwards no live copy holds it"      "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tkdel.countDocuments({ref:'Ref000001',_tcDeletedAt:{\$exists:false}}))")"
chk "  while the claim row is still there, on purpose" "true" \
    "$(H "print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+'.tkdel'}) > 0)")"
chk "the reference can be issued again"           "ok" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).tkdel.insertOne({_id:3,ref:'Ref000001'});print('ok')}catch(e){print(e.code||e.message.substring(0,40))}")"
# And it is still a unique index afterwards, so this is not a claim that was
# simply forgotten.
chk "  and a fourth document is refused it"       "11000" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).tkdel.insertOne({_id:4,ref:'Ref000001'});print('ACCEPTED')}catch(e){print(e.code||e.message.substring(0,40))}")"

echo "== a statement whose collation is why its filter matches =="
# The registry claims the key every document a statement will end up with, and
# to know which documents those are it reads them from the fast tier with the
# statement's own filter. mongod runs the statement under the collation the
# statement carries. That collation was never parsed: updateStatement declares a
# field for it, with a comment saying anything asking the same filter of the
# other tier has to ask it the same way, and the parser for update and
# findAndModify filled in q, upsert, multi, sort and arrayFilters and not that.
#
# So the read matched nothing, no key was claimed, and the update moved a
# document onto an address an archived document already holds. Measured before
# the fix: accepted by the router, E11000 from an untiered collection carrying
# the same unique index, and taken@x.com then live on two documents at once.
fresh tkupdcoll '{email:1}'
R "db.getSiblingDB(process.env.TC_DB).tkupdcoll.insertMany([
     {_id:1,name:'ALICE',email:'a@x.com'},{_id:2,name:'zzz',email:'taken@x.com'}])" >/dev/null
waitMirrored tkupdcoll 2
# _id 2 leaves the fast tier, so only the archive holds the contested address
# and the fast tier's own unique index cannot see it.
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).tkupdcoll.deleteOne({_id:2})"
chk "the archive holds the address and the fast tier does not" "1/0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tkupdcoll.countDocuments({email:'taken@x.com',_tcDeletedAt:{\$exists:false}}))")/$(H "print(db.getSiblingDB(process.env.TC_DB).tkupdcoll.countDocuments({email:'taken@x.com'}))")"
# The filter matches _id 1 only because the collation says ALICE is alice.
chk "an update matching only under its collation is refused" "11000" \
    "$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({update:'tkupdcoll',updates:[
           {q:{name:'alice'},u:{\$set:{email:'taken@x.com'}},collation:{locale:'en',strength:2}}]});
         print(r.writeErrors?r.writeErrors[0].code:'ACCEPTED')")"
chk "  and the address is still held by one document" "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tkupdcoll.countDocuments({email:'taken@x.com',_tcDeletedAt:{\$exists:false}}))")"
# The control, because a refusal proves nothing if everything is being refused.
# The same statement moving onto an address nobody holds has to go through.
chk "the same update onto a free address still works"      "ok" \
    "$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({update:'tkupdcoll',updates:[
           {q:{name:'alice'},u:{\$set:{email:'free@x.com'}},collation:{locale:'en',strength:2}}]});
         print(r.writeErrors?r.writeErrors[0].code:'ok')")"
chk "  and it reached the document the collation names"    "free@x.com" \
    "$(H "const d=db.getSiblingDB(process.env.TC_DB).tkupdcoll.findOne({_id:1}); print(d?d.email:'MISSING')")"

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

# Every row above names its owner, so the question "is this key still held" was
# always answered by re-reading that document. A row that names nobody is
# answered a different way -- by asking the tiers whether any live document
# holds the value -- and that path did not know the index was partial.
#
# A row with no owner is a real state: a key claimed for a write whose document
# id was not knowable yet leaves one. It is made here by taking the owner off a
# row, because the alternative is computing a key id, which nothing outside the
# router can do.
#
# The value below is held by a document outside the filter, so mongod indexes
# nothing for it and accepts a second, indexed holder. The router refused it:
# E11000 naming an index whose filter the other document does not match. An
# untiered copy of the same collection with the same index is what says so.
echo "== a claim row with no owner still knows the filter =="
H 'db.getSiblingDB(process.env.TC_DB).tkplain.drop(); db.getSiblingDB(process.env.TC_DB).tkplain.createIndex({email:1},{unique:true,partialFilterExpression:{active:true}})' >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tkpart.insertOne({_id:10,email:"q@x.com",active:true})' >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tkpart.deleteOne({_id:10})' >/dev/null
# A deleted document leaves its row behind on purpose, which is what makes the
# rest of this possible. Asserted rather than assumed.
QROW=$(H "const r=db.getSiblingDB(process.env.TC_META).keys.findOne({ns:'$DB.tkpart',doc:10}); print(r?r._id:'NONE')")
chk "  the row outlived the document it was for" "true" \
    "$([ "$QROW" != "NONE" ] && echo true || echo false)"
[ "$QROW" != "NONE" ] && H "db.getSiblingDB(process.env.TC_META).keys.updateOne({_id:'$QROW'},{\$unset:{doc:''}})" >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tkpart.insertOne({_id:11,email:"q@x.com",active:false})' >/dev/null
H 'db.getSiblingDB(process.env.TC_DB).tkplain.insertOne({_id:11,email:"q@x.com",active:false})' >/dev/null
chk "  mongod accepts an indexed holder beside it" "ok" \
    "$(H 'try{db.getSiblingDB(process.env.TC_DB).tkplain.insertOne({_id:12,email:"q@x.com",active:true});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and so does the router"                     "ok" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkpart.insertOne({_id:12,email:"q@x.com",active:true});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
# And the enforcement that narrowing could have thrown away. The only holder is
# in the archive, where the fast tier's index cannot see it, and it is inside
# the filter, so it does hold the key: refusing is right here even though a lone
# mongod would accept. Its row loses its owner too, so the same ownerless path
# is the one answering.
# Waited for by name rather than by count. waitMirrored counts the whole
# collection, which makes the number depend on every section above this one.
for _ in $(seq 1 60); do
  [ "$(C 'print(db.getSiblingDB(process.env.TC_DB).tkpart.countDocuments({_id:12,_tcDeletedAt:{$exists:false}}))')" = "1" ] && break
  sleep 0.5
done
chk "  the archive has the document to be evicted" "1" \
    "$(C 'print(db.getSiblingDB(process.env.TC_DB).tkpart.countDocuments({_id:12,_tcDeletedAt:{$exists:false}}))')"
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkpart.deleteOne({_id:12})'
EROW=$(H "const r=db.getSiblingDB(process.env.TC_META).keys.findOne({ns:'$DB.tkpart',doc:12}); print(r?r._id:'NONE')")
[ "$EROW" != "NONE" ] && H "db.getSiblingDB(process.env.TC_META).keys.updateOne({_id:'$EROW'},{\$unset:{doc:''}})" >/dev/null
chk "  an archived indexed holder is still enforced" "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkpart.insertOne({_id:13,email:"q@x.com",active:true});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo "== the same number written two ways is one key =="
# A unique index makes one key out of int, long, double and decimal: a
# collection holding NumberLong(7) rejects an insert of 7. The registry encodes
# the value as BSON, where those four have different bytes, so it made four keys
# where mongod makes one.
#
# Only the cross tier case reaches it. Inside the fast tier mongod's own index
# still refuses the duplicate, which is why the first of these is the check that
# matters and the rest are there to say the key did not become too broad.
fresh tknum '{n:1}'
R 'db.getSiblingDB(process.env.TC_DB).tknum.insertOne({_id:1,n:NumberLong("7")})' >/dev/null
waitMirrored tknum 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tknum.deleteOne({_id:1})'
chk "a double against an archived long is rejected" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tknum.insertOne({_id:2,n:7});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "and an int32 against it as well"               "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tknum.insertOne({_id:3,n:NumberInt(7)});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "and a decimal that reads the same"             "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tknum.insertOne({_id:4,n:NumberDecimal("7.00")});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
# And a key that merged more than mongod does would refuse writes mongod takes,
# which is a visible failure caused by bookkeeping.
chk "a different number is still accepted"          "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tknum.insertOne({_id:5,n:8});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "so is one that is not a whole number"          "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tknum.insertOne({_id:6,n:7.5});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
# A string that reads as a number is not a number, and a unique index does not
# treat it as one.
chk "and the string \"7\" is not the number 7"       "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tknum.insertOne({_id:7,n:"7"});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo "== a collation decides which strings are one key =="
# Under en/strength 2, Bob@x.com and bob@x.com are one value and a unique index
# rejects the second. The registry derives its id from the value's bytes, and
# those two spellings have different bytes, so it made two keys where mongod
# makes one. An index inherits its collection's default collation, so the spec
# says so whichever way it was set.
fresh tkcoll
H 'db.getSiblingDB(process.env.TC_DB).tkcoll.createIndex({email:1},{unique:true,collation:{locale:"en",strength:2}})' >/dev/null
# The comparison has to be made in the index's own collation, which means asking
# the archive, and asking without an index there would scan the whole collection
# on every insert. An adopted archive is the original cluster and carries it; one
# the feeder built has only _id_, so the test creates it as the warning says to.
C 'db.getSiblingDB(process.env.TC_DB).tkcoll.createIndex({email:1},{collation:{locale:"en",strength:2}})' >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tkcoll.insertOne({_id:1,email:"Bob@x.com"})' >/dev/null
waitMirrored tkcoll 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkcoll.deleteOne({_id:1})'
chk "the other spelling of an archived key rejected" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll.insertOne({_id:2,email:"bob@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "and the exact spelling too"                     "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll.insertOne({_id:3,email:"Bob@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
# And it must not become a blanket refusal: a value nobody holds is still free,
# and a document is still allowed to keep the value it already has.
chk "a value nobody holds is still accepted"         "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll.insertOne({_id:4,email:"eve@x.com"});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "and rewriting a value to itself is allowed"     "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll.updateOne({_id:4},{$set:{email:"eve@x.com"}});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "an update onto the archived key is rejected"    "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll.updateOne({_id:4},{$set:{email:"BOB@x.com"}});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and the document was not changed"             "eve@x.com" "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tkcoll.findOne({_id:4}); print(d?d.email:"MISSING")')"
# The archive is behind the fast tier by however long mirroring takes, so a
# document that has just moved off a value still looks like it holds it there.
# Refusing on that would refuse a legitimate write for the length of the lag.
R 'db.getSiblingDB(process.env.TC_DB).tkcoll.insertOne({_id:5,email:"moves@x.com"})' >/dev/null
waitMirrored tkcoll 3
R 'db.getSiblingDB(process.env.TC_DB).tkcoll.updateOne({_id:5},{$set:{email:"moved@x.com"}})' >/dev/null
chk "the value it just moved off is free at once"    "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll.insertOne({_id:6,email:"MOVES@x.com"});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo "== a collation and a partial filter, which is where the two rules meet =="
# A partial or sparse unique index constrains only some documents, and one
# outside it does not hold the key however plainly it holds the value. That rule
# is indexScope, and its own comment says the two places that need it must not
# drift. The collated archive lookup is a third place that needs it and did not
# have it, so an archived document the index does not index refused a write.
#
# The enforcement case goes first, because it leaves the archive's live count
# where the next step expects it whether or not the write below is refused.
fresh tkcollpart
H 'db.getSiblingDB(process.env.TC_DB).tkcollpart.createIndex({email:1},{unique:true,collation:{locale:"en",strength:2},partialFilterExpression:{active:true}})' >/dev/null
# The archive needs an index in the same collation or the router cannot ask it,
# exactly as the section above this one explains.
C 'db.getSiblingDB(process.env.TC_DB).tkcollpart.createIndex({email:1},{collation:{locale:"en",strength:2}})' >/dev/null

R 'db.getSiblingDB(process.env.TC_DB).tkcollpart.insertOne({_id:3,email:"Carol@x.com",active:true})' >/dev/null
waitMirrored tkcollpart 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkcollpart.deleteOne({_id:3})'
chk "an archived document the index does index still refuses it" "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcollpart.insertOne({_id:4,email:"carol@x.com",active:true});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"

# active:false, so this one holds the value and is not in the index at all.
R 'db.getSiblingDB(process.env.TC_DB).tkcollpart.insertOne({_id:1,email:"Bob@x.com",active:false})' >/dev/null
waitMirrored tkcollpart 2
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkcollpart.deleteOne({_id:1})'
chk "an archived document it does not index is not a clash"      "ok"    \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcollpart.insertOne({_id:2,email:"bob@x.com",active:true});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"

# The control, because a check that a write is accepted passes just as well when
# nothing is being enforced at all. Same index, untiered, straight at mongod.
H 'db.getSiblingDB(process.env.TC_DB).tkcpctl.drop()' >/dev/null
H 'db.getSiblingDB(process.env.TC_DB).tkcpctl.createIndex({email:1},{unique:true,collation:{locale:"en",strength:2},partialFilterExpression:{active:true}})' >/dev/null
H 'db.getSiblingDB(process.env.TC_DB).tkcpctl.insertOne({_id:1,email:"Bob@x.com",active:false})' >/dev/null
chk "  and mongod takes the same write on the same index"        "ok"    \
    "$(H 'try{db.getSiblingDB(process.env.TC_DB).tkcpctl.insertOne({_id:2,email:"bob@x.com",active:true});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and refuses it when the document is in the index"         "11000" \
    "$(H 'try{db.getSiblingDB(process.env.TC_DB).tkcpctl.insertOne({_id:3,email:"BOB@x.com",active:true});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo "== the remedy for a collated index the archive cannot answer =="
# When the archive has no index in the same collation, the router cannot compare
# the value the way the index does, so it under-enforces and says so, with an
# action: create the same index on the archive. This is that action, taken while
# the router runs, which is how an operator would take it.
#
# The answer used to be remembered for the life of the process, so doing what
# the log asked changed nothing until somebody restarted the router, and nothing
# said so. See collatedRecheckNo.
fresh tkcoll2
H "db.getSiblingDB(process.env.TC_DB).tkcoll2.createIndex({email:1},{unique:true,collation:{locale:'en',strength:2}})" >/dev/null
# Counted as a delta over the whole section, and taken before the first insert.
# Two reasons: the router appends to this log across restarts, so a total also
# counts a line an earlier run of this suite wrote; and the guard runs on every
# insert into a collection with a unique index, so the first one is already
# where the warning happens.
WAS=$(grep -c 'not enforcing a collated unique index' $LOGDIR/keys_r.log 2>/dev/null || true)
R 'db.getSiblingDB(process.env.TC_DB).tkcoll2.insertOne({_id:1,email:"Ann@x.com"})' >/dev/null
waitMirrored tkcoll2 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkcoll2.deleteOne({_id:1})'
chk "with no index on the archive, another spelling lands" "ACCEPTED" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll2.insertOne({_id:2,email:"ann@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
NOW=$(grep -c 'not enforcing a collated unique index' $LOGDIR/keys_r.log 2>/dev/null || true)
chk "and the router said so, once and not twice"            "1"        "$((NOW-WAS))"
# The remedy, on the archive, without restarting anything.
C "db.getSiblingDB(process.env.TC_DB).tkcoll2.createIndex({email:1},{collation:{locale:'en',strength:2}})" >/dev/null
# The duplicate that was accepted has to go, or the fast tier's own index
# refuses the next spelling and this passes for the wrong reason.
R 'db.getSiblingDB(process.env.TC_DB).tkcoll2.deleteOne({_id:2})' >/dev/null
chk "the fast tier holds no email now"                     "0"        "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkcoll2.countDocuments({email:{$exists:true}}))')"
# Long enough for the negative answer to lapse. Fixed rather than polled,
# because there is nothing to poll: the router asks again on the next insert.
sleep 12
chk "once the index is there, the third spelling is refused" "11000"   "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll2.insertOne({_id:3,email:"ANN@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and the archive still holds one live copy"          "1"        "$(C 'print(db.getSiblingDB(process.env.TC_DB).tkcoll2.countDocuments({email:{$exists:true},_tcDeletedAt:{$exists:false}}))')"

echo "== a collection's collation decides which _ids are one =="
# The _id index takes the collection's default collation, so in a collection
# created with en/strength 2 "A" and "a" are one _id. The insert guard found
# the archived "A" with its $in, which ran under that collation, and then placed
# the answer back into the batch by comparing bytes, where "A" is not "a", so
# it let the insert through. Measured before the fix: "a" beside an archived
# "A", and {k:"x"} beside {k:"X"}, both accepted, two documents with one _id.
fresh tkcoll2
H "db.getSiblingDB(process.env.TC_DB).createCollection('tkcoll2', {collation:{locale:'en', strength:2}})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).createCollection('tkcoll2', {collation:{locale:'en', strength:2}})" >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tkcoll2.insertMany([{_id:"A"},{_id:{k:"X"}}])' >/dev/null
waitMirrored tkcoll2 2
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkcoll2.deleteMany({})'
chk "an _id the collation calls the archived one is refused" "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll2.insertOne({_id:"a"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "and so is one inside a compound _id"                     "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll2.insertOne({_id:{k:"x"}});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "an _id the collation keeps apart is still accepted"      "ok" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll2.insertOne({_id:"b"});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo "== one batch naming a new unique value twice =="
# mongod keeps the first and refuses the second. The router sent the second to
# the registry, where the first one's fresh claim looked stale because no tier
# held its document yet, took it over, then released it when the fast tier
# refused the document: the one that landed had no claim at all. Measured before
# the fix, ordered and unordered alike: no row left, and once the first was
# archived a third insert of the value was accepted beside it.
fresh tkcoll2 '{email:1}'
R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll2.insertMany([{_id:1,email:"x@x"},{_id:2,email:"x@x"}],{ordered:false})}catch(e){}' >/dev/null
chk "the first of the two landed and the second did not" "1" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkcoll2.countDocuments({email:"x@x"}))')"
chk "and the registry kept a claim for the one that did" "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:'$DB.tkcoll2'}))")"
waitMirrored tkcoll2 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkcoll2.deleteMany({})'
chk "a third with the value, once the first is archived, is refused" "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkcoll2.insertOne({_id:3,email:"x@x"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"

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

echo "== a unique index on a field that holds an array =="
# Such an index is multikey: mongod makes one entry per element, so no two
# documents may share *any* element. The registry computed one key from one
# document's value of the field, and an array is one value to it, so two
# documents sharing an element had different keys as far as it was concerned.
#
# While both are on the fast tier mongod catches the duplicate itself. Once one
# has been evicted, nothing does. Measured before this: the router accepted the
# second document, the fast tier accepted it, and the archive refused the
# mirrored insert with E11000 dup key { tags: "y" }. The feeder writes such a
# document off rather than retrying a write that can never succeed, so what is
# left is a document on the fast tier that is not in the archive and that the
# evictor will not remove, because the archive cannot confirm it.
fresh tkarr '{tags:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkarr.insertOne({_id:1,tags:["x","y"]})' >/dev/null
waitMirrored tkarr 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkarr.deleteOne({_id:1})'
chk "the archive holds the evicted document"   "true" \
    "$(C 'const d=db.getSiblingDB(process.env.TC_DB).tkarr.findOne({_id:1}); print(!!d && !d._tcDeletedAt)')"
chk "and the fast tier does not"               "0" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkarr.countDocuments({}))')"
chk "a document sharing one element rejected"  "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkarr.insertOne({_id:2,tags:["y","z"]});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and the fast tier did not take it"      "0" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkarr.countDocuments({_id:2}))')"
# The other half, or this is a guard that refuses everything: a document whose
# elements are all free has to be allowed.
chk "a document sharing no element is allowed" "ok" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkarr.insertOne({_id:3,tags:["p","q"]});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and it reaches the archive"             "1" \
    "$(waitMirrored tkarr 2 >/dev/null; C 'print(db.getSiblingDB(process.env.TC_DB).tkarr.countDocuments({_id:3,_tcDeletedAt:{$exists:false}}))')"
# One document repeating a value is one index entry to mongod, so it is one
# claim here. Claiming it twice would have the second look like somebody else
# holding it and the document would refuse itself.
chk "a document repeating an element is allowed" "ok" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkarr.insertOne({_id:4,tags:["w","w","w"]});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
# And two documents in one batch sharing an element, which mongod refuses and
# which nothing archived is needed for.
chk "two in one batch sharing an element rejected" "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkarr.insertMany([{_id:5,tags:["m"]},{_id:6,tags:["m"]}]);print("ACCEPTED")}catch(e){print(e.code||(e.writeErrors&&e.writeErrors[0]&&e.writeErrors[0].code)||e.message.substring(0,40))}')"
chk "  and the first of the two is there"        "1" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkarr.countDocuments({_id:5}))')"
chk "  and the second is not"                    "0" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkarr.countDocuments({_id:6}))')"

echo "== a unique index on a path that crosses an array =="
# The quieter half of the same mistake, and it points the other way. The driver
# answers "element not found" for a path that crosses an array, so every such
# document was keyed as null and the first one to arrive claimed that key for
# the whole namespace. Every insert after it was refused as a duplicate of a
# document it has nothing in common with.
#
# Nothing has to be archived to show it, which is why it went unnoticed: it is a
# refusal rather than a wrong answer, and a refusal reads as the guard working.
fresh tkarrp '{"items.sku":1}'
chk "the first document is allowed"            "ok" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkarrp.insertOne({_id:1,items:[{sku:"a"}]});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "and a second with a different sku too"    "ok" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkarrp.insertOne({_id:2,items:[{sku:"b"}]});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "while one that shares a sku is refused"   "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkarrp.insertOne({_id:3,items:[{sku:"a"}]});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"

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

echo "== and by the other routers, without restarting them =="
# The one this suite could not check before, because there was nothing to check.
# No change stream carries index DDL, so a router that did not take the command
# had only its own listing, kept for indexRefresh, a minute. In that window it
# claims no keys for the new index and lets through a duplicate of an archived
# document, which mongod's own index cannot see because the document is not
# there.
#
# The claim document is read on the hot path anyway, once per five seconds per
# namespace, so the count of index changes rides on it. This waits past that
# five seconds and well inside the minute: before, the second router accepted
# the duplicate here.
fresh tkddl2
# Truncated, because a check below greps this file and a previous run's line
# would answer for this one.
rm -f $LOGDIR/keys_r2.log
cline router --config $CFG --listen 0.0.0.0:$P2 --log-file $LOGDIR/keys_r2.log & RP2=$!
for _ in $(seq 1 30); do
  mongosh --port $P2 -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
R2() { mongosh --port $P2 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
R 'db.getSiblingDB(process.env.TC_DB).tkddl2.insertOne({_id:1,email:"other@x.com"})' >/dev/null
waitMirrored tkddl2 1
# Two things the second router needs before the index is created, and the first
# version of this check had only one, so it passed against a binary with none of
# this in it.
#
# It has to have read the claim, or there is no earlier generation to notice a
# change against. And it has to have *written*, because that is what makes it
# list the unique indexes and cache them: a router with no cached listing lists
# them fresh on its first write and sees the new index anyway, which is not the
# window this is about.
R2 'print(db.getSiblingDB(process.env.TC_DB).tkddl2.countDocuments({}))' >/dev/null
R2 'db.getSiblingDB(process.env.TC_DB).tkddl2.insertOne({_id:9,email:"warm@x.com"})' >/dev/null
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkddl2.deleteOne({_id:1})'
R 'db.getSiblingDB(process.env.TC_DB).tkddl2.createIndex({email:1},{unique:true})' >/dev/null
cline keys --config $CFG --rebuild >/dev/null 2>&1
sleep 8
chk "the other router refuses the duplicate too" "11000" \
    "$(R2 'try{db.getSiblingDB(process.env.TC_DB).tkddl2.insertOne({_id:2,email:"other@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and nothing of it reached the fast tier"  "0" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkddl2.countDocuments({_id:2}))')"
chk "  while a free value still goes through"    "ok" \
    "$(R2 'try{db.getSiblingDB(process.env.TC_DB).tkddl2.insertOne({_id:3,email:"free@x.com"});print("ok")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and it said why it re-read them"          "true" \
    "$(grep -q 'another router changed the unique indexes here' $LOGDIR/keys_r2.log && echo true || echo false)"
kill $RP2 2>/dev/null; wait $RP2 2>/dev/null

echo
echo "== a unique index that no longer exists must not still be enforced =="
#
# The router reads the fast tier's unique indexes and remembers the listing for
# a minute. Two ways it goes stale without the router being told, and both used
# to end in a duplicate key error naming an index that exists on neither tier:
# an insert plain mongod accepts, refused with 11000.
#
# Out of band on purpose. A dropIndex or a drop forwarded through the router is
# already handled where it is forwarded; the ones worth testing are the ones
# nothing forwards, which is what an operator on the mongod does and what
# resetting an environment does.
fresh tkgone '{email:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkgone.insertOne({_id:1,email:"held@x.com"})' >/dev/null
chk "with the index there, a duplicate is refused" "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkgone.insertOne({_id:2,email:"held@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
# Counted rather than grepped: the router appends to this log across restarts,
# so a total also counts a line an earlier run of this suite wrote, and the
# check would then pass against a binary that says nothing.
GONEWAS=$(grep -c 'is gone from the fast tier, so it is no longer enforced' $LOGDIR/keys_r.log 2>/dev/null || true)
H 'db.getSiblingDB(process.env.TC_DB).tkgone.dropIndex("email_1")' >/dev/null
sleep 3
chk "  the index is gone from the fast tier"      "_id_" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkgone.getIndexes().map(i=>i.name).join(","))')"
chk "  so the same insert is accepted, as mongod would" "ok" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkgone.insertOne({_id:3,email:"held@x.com"});print("ok")}catch(e){print(e.code||e.message.substring(0,60))}')"
GONENOW=$(grep -c 'is gone from the fast tier, so it is no longer enforced' $LOGDIR/keys_r.log 2>/dev/null || true)
chk "  and it said the constraint had gone"       "1" "$((GONENOW-GONEWAS))"

# The other half: the whole collection dropped and rebuilt without the index,
# which is what the fuzzer's own reseed does and what found this.
fresh tkgone '{email:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkgone.insertOne({_id:1,email:"held@x.com"})' >/dev/null
H 'const c=db.getSiblingDB(process.env.TC_DB).tkgone; c.drop(); c.insertOne({_id:1,email:"held@x.com"})' >/dev/null
C 'const c=db.getSiblingDB(process.env.TC_DB).tkgone; c.drop(); c.insertOne({_id:1,email:"held@x.com"})' >/dev/null
sleep 3
chk "a collection rebuilt without the index does not keep it" "ok" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkgone.insertOne({_id:4,email:"held@x.com"});print("ok")}catch(e){print(e.code||e.message.substring(0,60))}')"

echo
echo "== a new unique index has to cover what is already in the archive =="
#
# The documents in the archive are the only ones the constraint needs help with:
# the fast tier's own index enforces itself over what the fast tier holds. None
# of them was ever written through a router that knew about this index, so until
# now none of their keys was claimed, and the rollout list saying to run
# cline keys --rebuild afterwards was the whole of the protection.
#
# The index is created after the document is archived, which is the order that
# matters. The other way round is the ordinary write path and was never in doubt.
fresh tkgone ''
R 'db.getSiblingDB(process.env.TC_DB).tkgone.insertOne({_id:1,email:"held@x.com"})' >/dev/null
waitMirrored tkgone 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkgone.deleteOne({_id:1})'
chk "the document is in the archive and not on the fast tier" "0/1" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkgone.countDocuments({}))')/$(C 'print(db.getSiblingDB(process.env.TC_DB).tkgone.countDocuments({_tcDeletedAt:{$exists:false}}))')"
# Emptied by hand rather than waited for. A document archived before the index
# existed has no claim, and that is the state this section is about; leaving it
# to the previous section'"'"'s leftovers and to whether a change stream event has
# landed yet would make the precondition a race rather than a statement.
H 'db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+".tkgone"})' >/dev/null
chk "  and nothing has claimed its key"                  "0" \
    "$(H 'print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+".tkgone"}))')"
# The walk runs before the build, not after it, so the key is claimed by the
# time the command has been answered. Before is the only order in which the
# answer can be a refusal, which is the other half of this: see the duplicate
# section below.
R 'db.getSiblingDB(process.env.TC_DB).tkgone.createIndex({email:1},{unique:true,name:"u_email"})' >/dev/null
chk "creating the unique index claims its key"           "1" \
    "$(H 'print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+".tkgone"}))')"
chk "  so a duplicate of the archived document is refused" "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkgone.insertOne({_id:2,email:"held@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"
chk "  and nothing of it reached the fast tier"          "0" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkgone.countDocuments({_id:2}))')"

# An ordinary index build must not walk the archive. A driver that re-declares
# its indexes on every start would otherwise scan the whole collection on every
# start, for a constraint it is not creating.
WALKWAS=$(grep -c 'checking the documents already there and claiming their keys' $LOGDIR/keys_r.log 2>/dev/null || true)
R 'db.getSiblingDB(process.env.TC_DB).tkgone.createIndex({grp:1},{name:"plain_grp"})' >/dev/null
WALKNOW=$(grep -c 'checking the documents already there and claiming their keys' $LOGDIR/keys_r.log 2>/dev/null || true)
chk "an index that is not unique starts no walk"         "0" "$((WALKNOW-WALKWAS))"

echo
echo "== dropping a unique index releases the claims it made =="
#
# Claims are permanent on purpose, and that reasoning is about documents: a
# deleted document leaves its row behind and the row is resolved when something
# collides with it. For an index that has been dropped the row is a claim about
# a constraint that no longer exists.
#
# Leaving them is worse than useless because index names are reused. Create the
# same index again and the rows are already there, so the walk that follows can
# insert none of them and the documents that really hold those keys never get a
# row. Their keys then read as free. The fuzzer needed nine rounds of a hundred
# and twenty operations to build that state.
fresh tkgone '{email:1}'
R 'db.getSiblingDB(process.env.TC_DB).tkgone.insertOne({_id:1,email:"held@x.com"})' >/dev/null
waitMirrored tkgone 1
evictBehindTheFeeder 'db.getSiblingDB(process.env.TC_DB).tkgone.deleteOne({_id:1})'
claimsFor() { H 'print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+".tkgone",idx:"email_1"}))'; }
chk "the archived document holds a claim"                "1" "$(claimsFor)"
R 'db.getSiblingDB(process.env.TC_DB).tkgone.dropIndex("email_1")' >/dev/null
# The release is in the background, off the path of the command that caused it.
for _ in $(seq 1 40); do [ "$(claimsFor)" = "0" ] && break; sleep 0.5; done
chk "  dropping the index releases them"                 "0" "$(claimsFor)"
R 'db.getSiblingDB(process.env.TC_DB).tkgone.createIndex({email:1},{unique:true,name:"email_1"})' >/dev/null
for _ in $(seq 1 60); do [ "$(claimsFor)" != "0" ] && break; sleep 0.5; done
chk "  and creating it again claims the key afresh"      "1" "$(claimsFor)"
chk "  so a duplicate of the archived document is refused" "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkgone.insertOne({_id:2,email:"held@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,40))}')"

echo
echo "== a unique index over documents that already break it is refused =="
#
# mongod refuses createIndex with unique when two live documents share a key.
# Split the pair across the tiers and the fast tier'"'"'s build sees only one of
# each, so it succeeded: the application was told a constraint holds that does
# not, and nothing would ever tell it otherwise.
#
# The control is the same two documents in one untiered collection, so the
# comparison is against what mongod itself says rather than against what this
# suite believes mongod says.
fresh tkgone ''
H 'db.getSiblingDB(process.env.TC_DB).tkgone.insertOne({_id:1,sku:"x"})' >/dev/null
# Upserts, not insertMany: the feeder may have mirrored _id 1 already, and an
# ordered insert then abandons the rest on the first duplicate, leaving the
# archive with one document and this section testing nothing. The fuzzer'"'"'s
# reseed learned the same thing.
C 'db.getSiblingDB(process.env.TC_DB).tkgone.bulkWrite(
     [{_id:1,sku:"x"},{_id:2,sku:"x"}].map(d=>({replaceOne:{filter:{_id:d._id},replacement:d,upsert:true}})),
     {ordered:false})' >/dev/null
chk "one on the fast tier, both in the archive"          "1/2" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkgone.countDocuments({}))')/$(C 'print(db.getSiblingDB(process.env.TC_DB).tkgone.countDocuments({}))')"
chk "mongod refuses the same pair in one collection"     "11000" \
    "$(H 'const c=db.getSiblingDB(process.env.TC_DB).tkgone_ref; c.drop(); c.insertMany([{_id:1,sku:"x"},{_id:2,sku:"x"}]);
         try{c.createIndex({sku:1},{unique:true,name:"u_sku"});print("ACCEPTED")}catch(e){print(e.code)}')"
chk "  and so does the router"                           "11000" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkgone.createIndex({sku:1},{unique:true,name:"u_sku"});print("ACCEPTED")}catch(e){print(e.code)}')"
chk "  leaving no index behind"                          "_id_" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tkgone.getIndexes().map(i=>i.name).join(","))')"
chk "  and no claims for an index that was not built"    "0" \
    "$(H 'print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+".tkgone",idx:"u_sku"}))')"
# The same index with the duplicate removed goes through, so the refusal is
# about the data and not about the shape of the command.
C 'db.getSiblingDB(process.env.TC_DB).tkgone.deleteOne({_id:2})' >/dev/null
chk "  with the duplicate gone it is built"              "ok" \
    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tkgone.createIndex({sku:1},{unique:true,name:"u_sku"});print("ok")}catch(e){print(e.code||e.message.substring(0,60))}')"

echo "== an adopted archive whose keys nothing has claimed =="
# The shape every adoption starts in: the archive arrives with its documents and
# its unique index already on it, so no createIndexes ever went through the
# router and nothing walked the tiers to claim the values already there.
#
# Until something does, the constraint is not enforced across the tiers. The
# fast tier's own index cannot see a document only the archive holds, and the
# registry has no row to collide with -- a key whose row inserts cleanly is
# settled there and then, with no question asked of the archive. So a duplicate
# of an archived value goes straight in, which this section measures rather than
# asserts.
#
# The remedy was written down as the answer to "a cluster adopted with data
# already in it" and never as a consequence of skipping it, and a documented
# step is not a guarantee. The router says so at startup now.
ANS=tkadopt
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$ANS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ANS.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:'$DB.$ANS'})" >/dev/null
# On the archive only, which is what makes it an adoption rather than a create
# through the router.
C "const d=db.getSiblingDB(process.env.TC_DB).$ANS;
   d.insertMany([{_id:1,sku:'x'},{_id:2,sku:'y'}]); d.createIndex({sku:1},{unique:true,name:'u_sku'})" >/dev/null
# Its own log, so the count is this router's and not every router the suite has
# started. The warning is a startup one, so it takes a fresh router either way.
rm -f $LOGDIR/keys_adopt1.log
cline router --config $CFG --log-file $LOGDIR/keys_adopt1.log & RPA=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
# Scoped to this namespace rather than counted across the file. Every other
# section leaves collections with unique indexes behind, and a bare count of the
# message would be a number that depends on all of them: the same mistake as
# counting the whole key registry instead of one collection's rows.
chk "it warns that nothing has claimed this index"       "1" \
    "$(grep 'has no claims in the registry' $LOGDIR/keys_adopt1.log 2>/dev/null | grep -c "ns=$DB.$ANS" || true)"
chk "  and names the command that fixes it"              "true" \
    "$(grep 'has no claims in the registry' $LOGDIR/keys_adopt1.log 2>/dev/null | grep -q 'keys --rebuild' && echo true || echo false)"
# The thing the warning is about. Not enforced means not enforced.
chk "  a duplicate of an archived value gets in"         "ok" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$ANS.insertOne({_id:99,sku:'x'});print('ok')}catch(e){print(e.code||e.message.substring(0,40))}")"
R "db.getSiblingDB(process.env.TC_DB).$ANS.deleteOne({_id:99})" >/dev/null
kill $RPA 2>/dev/null; sleep 2

cline keys --config $CFG --rebuild >/dev/null 2>&1
chk "the rebuild claimed the archive's keys"             "true" \
    "$([ "$(H "print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:'$DB.$ANS'}))")" -ge 2 ] && echo true || echo false)"
rm -f $LOGDIR/keys_adopt2.log
cline router --config $CFG --log-file $LOGDIR/keys_adopt2.log & RPB=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "and then it stops warning about it"                 "0" \
    "$(grep 'has no claims in the registry' $LOGDIR/keys_adopt2.log 2>/dev/null | grep -c "ns=$DB.$ANS" || true)"
chk "  while the duplicate is now refused"               "11000" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$ANS.insertOne({_id:98,sku:'x'});print('ACCEPTED')}catch(e){print(e.code||e.message.substring(0,40))}")"
kill $RPB 2>/dev/null

echo "== the registry's size, which nothing reported =="
# A claim is permanent on purpose: evicting a document takes its index entry off
# the fast tier and the claim is what remembers the value was taken. Nothing
# removes one whose document has been deleted from both tiers except
# `cline keys --prune`, and that is an operator's obligation with nothing asking
# for it. It grows with history, on the expensive tier.
#
# Measured: 300 inserts into a collection with one unique index, then deleting
# all 300, leaves nought documents on either tier and 300 claims on the fast
# tier. Reported rather than alerted on, because no number is wrong on its own:
# what matters is this climbing while the data it constrains does not.
GNS=tkregistry
pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null; sleep 1
H "db.getSiblingDB(process.env.TC_DB).$GNS.drop();
   db.getSiblingDB(process.env.TC_DB).$GNS.createIndex({code:1},{name:'code_u',unique:true})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$GNS.drop();
   db.getSiblingDB(process.env.TC_DB).$GNS.createIndex({code:1},{name:'code_u',unique:true})" >/dev/null
BEFORE=$(H "print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:'$DB.$GNS'}))")
rm -f $LOGDIR/reg_r.log
cline router --config $CFG --log-file $LOGDIR/reg_r.log & GRP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
R "for (let i=0;i<40;i++) db.getSiblingDB(process.env.TC_DB).$GNS.insertOne({_id:i,code:'k'+i})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).$GNS.deleteMany({})" >/dev/null
sleep 1
chk "claims outlive the documents that made them"      "40" \
    "$(H "print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:'$DB.$GNS'}) - $BEFORE)")"
# The number an operator can watch. The inventory refreshes on a timer, so this
# waits for it rather than sampling once.
REG=""
for _ in $(seq 1 40); do
  REG=$(curl -s "http://127.0.0.1:$PDASH/metrics" 2>/dev/null | awk '/^thermocline_key_registry_rows /{print $2}')
  [ -n "$REG" ] && [ "${REG%%.*}" -gt 0 ] 2>/dev/null && break
  sleep 2
done
chk "and the registry's size is reported"              "true" \
    "$(awk -v n="${REG:-0}" 'BEGIN{print (n+0 >= 40) ? "true" : "false (" n ")"}')"
kill $GRP 2>/dev/null; wait $GRP 2>/dev/null
cline keys --config $CFG --prune >/dev/null 2>&1
chk "and pruning removes the ones nothing holds"       "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:'$DB.$GNS'}))")"
H "db.getSiblingDB(process.env.TC_DB).$GNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$GNS.drop()" >/dev/null

echo "== a write sent as a document sequence, which is how a driver sends one =="
# Every guard reads its statements through wire.AllOf, which takes both the
# inline array and the kind 1 section a driver actually sends, and there is a
# unit test for the parsing. What that does not say is whether a guard fires for
# the sequenced form end to end.
#
# mongosh cannot be relied on to produce it, so this uses a small program built
# against the official Go driver, which batches an insertMany into a document
# sequence. Measured: refused through the router with a duplicate key, accepted
# straight at the fast tier, which is what makes the check able to see a
# difference at all.
SNS=tseq
if [ ! -x /root/seqwrite ]; then
  echo "      SKIP  /root/seqwrite is not built; make install builds and copies it (2 checks)"
else
  H "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$SNS.insertOne({_id:'archived',v:'precious'})" >/dev/null
  rm -f $LOGDIR/seq_r.log
  cline router --config $CFG --log-file $LOGDIR/seq_r.log & SRP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
    sleep 1
  done
  SEQOUT=$(/root/seqwrite "mongodb://dba:secret@127.0.0.1:$PORT/?authSource=admin" "$DB" "$SNS" 2>&1 | tail -1)
  kill $SRP 2>/dev/null; wait $SRP 2>/dev/null
  chk "a sequenced insert of an archived _id is refused" "true" \
      "$(grep -q 'REFUSED with a duplicate key' <<<"$SEQOUT" && echo true || echo "false ($SEQOUT)")"
  chk "and the archived document is untouched"           "precious" \
      "$(C "const d=db.getSiblingDB(process.env.TC_DB).$SNS.findOne({_id:'archived'}); print(d?d.v:'MISSING')")"
  H "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$SNS.drop()" >/dev/null
fi

echo "== a document the archive refused, and what reads do afterwards =="
# The feeder gives up on a document the archive's unique index refuses, because
# no retry of it can succeed, and the evictor then never removes it. That is the
# half that works: the document is not lost.
#
# What reads do afterwards is easy to miss. The archive is the superset by
# assumption, so a read the fast tier cannot claim is answered there, and there
# is the one place this document is not. Measured: the archive refused one of
# two documents sharing a unique value, a sweep kept it and removed the others,
# and a client counted two where three exist. A lookup by _id still found it,
# because a point read asks both tiers, so the collection answers differently
# depending on how it is asked.
FNS=tkrefused
pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null; sleep 1
H "db.getSiblingDB(process.env.TC_DB).$FNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$FNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$FNS.createIndex({code:1},{name:'code_u',unique:true})" >/dev/null
rm -f $LOGDIR/refused_f.log
cline feeder --config $CFG --log-file $LOGDIR/refused_f.log & FRP=$!
sleep 3
# Straight at the fast tier, which has no such index and takes both.
H "const c=db.getSiblingDB(process.env.TC_DB).$FNS;
   c.insertOne({_id:1,code:'A',what:'the first'});
   c.insertOne({_id:2,code:'A',what:'the duplicate'});
   c.insertOne({_id:3,code:'B',what:'an ordinary one'});" >/dev/null
for _ in $(seq 1 30); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({}))")" = "2" ] && break
  sleep 1
done
kill $FRP 2>/dev/null; wait $FRP 2>/dev/null
chk "the archive refused one of them"                  "2" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({}))")"
chk "and said so at ERROR"                             "true" \
    "$(grep -q 'the archive refuses a document the fast tier accepted' $LOGDIR/refused_f.log && echo true || echo false)"
# The line has to name what reads will do, because that is the part an operator
# would otherwise find out from an application. It used to say those reads come
# back short, which was true until the router started refusing them: a message
# describing behaviour the product no longer has is worse than no message, and
# this check pinned the old wording in place.
chk "naming what reads that fall through will do"      "true" \
    "$(grep -q 'is refused while this stands' $LOGDIR/refused_f.log && echo true || echo false)"
chk "the fast tier still holds all three"              "3" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({}))")"

# And now the half this section used to stop before: what a client reads.
#
# The row the feeder writes is what makes this answerable at all. A log line
# cannot be read by a router and a counter cannot name a namespace, so until
# this existed the router had no way to know the archive was not a superset and
# answered from it anyway: measured, a count of 2 where three documents exist,
# a find returning two of three, and findOne by _id returning the third.
chk "the feeder recorded which document"               "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).unmirrored.countDocuments({ns:process.env.TC_DB+'.$FNS'}))")"
startRouter
chk "a count that falls through is refused"            "refused" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({}))}catch(e){print(/not a superset|does not hold one document/.test(e.message)?'refused':'other: '+e.message.substring(0,50))}")"
chk "and so is a find"                                 "refused" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$FNS.find().toArray().length)}catch(e){print(/does not hold one document/.test(e.message)?'refused':'other: '+e.message.substring(0,50))}")"
chk "and a distinct"                                   "refused" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$FNS.distinct('code').length)}catch(e){print(/does not hold one document/.test(e.message)?'refused':'other: '+e.message.substring(0,50))}")"
# A point read asks both tiers, so it was always right and must stay allowed.
# Refusing it would take away the one way an application can still reach the
# document while somebody resolves the duplicate.
chk "but a lookup by _id still answers"                "the duplicate" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$FNS.findOne({_id:2}); print(d?d.what:'missing')")"
# A write is not refused, and measuring that is the point: the document is on
# the fast tier, and a multi-document write is applied there after the archive's
# matches are copied up, so it reaches every document including this one.
chk "an updateMany still reaches all three"            "3" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$FNS.updateMany({},{\$set:{seen:1}}).modifiedCount)}catch(e){print('refused: '+e.message.substring(0,40))}")"

# The refusal has to lift by itself, or it is a deployment that never recovers.
# Whoever resolves the duplicate does not run a command here afterwards, so the
# write that finally reaches the archive is what clears the row.
cline feeder --config $CFG --log-file $LOGDIR/refused_f2.log & FRP2=$!
sleep 3
H "db.getSiblingDB(process.env.TC_DB).$FNS.updateOne({_id:2},{\$set:{code:'C'}})" >/dev/null
for _ in $(seq 1 30); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_META).unmirrored.countDocuments({ns:process.env.TC_DB+'.$FNS'}))")" = "0" ] && break
  sleep 1
done
kill $FRP2 2>/dev/null; wait $FRP2 2>/dev/null
chk "resolving the duplicate clears the record"        "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).unmirrored.countDocuments({ns:process.env.TC_DB+'.$FNS'}))")"
chk "and the archive holds all three"                  "3" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
# The router caches its reading for supersetTTL, so this waits rather than
# asking once: a check that races a cache reports a product defect that is a
# five second wait.
for _ in $(seq 1 20); do
  [ "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({}))}catch(e){print('refused')}")" = "3" ] && break
  sleep 1
done
chk "and reads answer in full again"                   "3" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$FNS.countDocuments({}))}catch(e){print('still refused')}")"

# The other way a duplicate is resolved, which nothing checked. Above, the
# offending document is edited until the archive takes it. An operator may
# instead decide the document should not exist and delete it, and that has to
# lift the refusal too: a namespace whose reads stay refused for ever, with the
# document that caused it already gone, is a deployment that never recovers.
#
# The record is cleared by the write that lands rather than by a command, so the
# question is whether a delete counts as one. Its own namespace, built the same
# way: the unique index on the archive alone, so the fast tier takes both.
FNS2=tkrefdel
pkill -f "cline feeder --config $WDIR/" 2>/dev/null; sleep 1
H "db.getSiblingDB(process.env.TC_DB).$FNS2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$FNS2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$FNS2.createIndex({code:1},{name:'code_u',unique:true})" >/dev/null
cline feeder --config $CFG --log-file $LOGDIR/refused_f3.log & FRP3=$!
sleep 3
H "const c=db.getSiblingDB(process.env.TC_DB).$FNS2;
   c.insertOne({_id:1,code:'A',what:'the first'});
   c.insertOne({_id:2,code:'A',what:'the one to delete'});" >/dev/null
for _ in $(seq 1 40); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_META).unmirrored.countDocuments({ns:process.env.TC_DB+'.$FNS2'}))")" = "1" ] && break
  sleep 1
done
chk "a duplicate the archive will not take, recorded"  "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).unmirrored.countDocuments({ns:process.env.TC_DB+'.$FNS2'}))")"
startRouter
chk "and reads of it are refused"                      "refused" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$FNS2.countDocuments({}))}catch(e){print(/not a superset|does not hold one document/.test(e.message)?'refused':'other: '+e.message.substring(0,50))}")"
# The resolution nobody had tried: delete it rather than edit it.
R "db.getSiblingDB(process.env.TC_DB).$FNS2.deleteOne({_id:2})" >/dev/null 2>&1
for _ in $(seq 1 40); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_META).unmirrored.countDocuments({ns:process.env.TC_DB+'.$FNS2'}))")" = "0" ] && break
  sleep 1
done
chk "deleting it clears the record too"                "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).unmirrored.countDocuments({ns:process.env.TC_DB+'.$FNS2'}))")"
for _ in $(seq 1 25); do
  [ "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$FNS2.countDocuments({}))}catch(e){print('refused')}")" != "refused" ] && break
  sleep 1
done
chk "and reads answer again"                           "1" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$FNS2.countDocuments({}))}catch(e){print('still refused')}")"
kill $FRP3 2>/dev/null; wait $FRP3 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$FNS2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$FNS2.drop()" >/dev/null

H "db.getSiblingDB(process.env.TC_DB).$FNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$FNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).unmirrored.deleteMany({})" >/dev/null

echo "== this system's own marks, written by an application =="
# _tcDeletedAt is how an archived document is marked as one the application
# deleted, and every read of the archive excludes the documents carrying it.
# Nothing stopped an application writing a field of that name, and mongod takes
# it like any other.
#
# Measured before this was refused: an insert of {_id:"x", n:5000,
# _tcDeletedAt:<a date>} into a tiered collection was taken by both tiers, and
# countDocuments({n:5000}) answered 0 through the router where the same document
# in an untiered collection answered 1, ok on both. A lookup by _id still found
# it, so one collection answered two ways about one document. The read is the
# smaller half: a fast tier rebuilt from that archive, which is the documented
# recovery, does not have the document at all.
RNS=tkreserved
fresh $RNS
R "db.getSiblingDB(process.env.TC_DB).$RNS.insertOne({_id:1,v:'ordinary'})" >/dev/null
res() { case "$1" in *"refusing this write"*) echo REFUSED ;; *) echo "$1" ;; esac; }
chk "an insert carrying the mark is refused"           "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.insertOne({_id:2,_tcDeletedAt:new Date()});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "and a set of it"                                  "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},{\$set:{_tcDeletedAt:new Date()}});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "and a replacement document carrying it"           "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.replaceOne({_id:1},{_tcDeletedAt:new Date(),v:'x'});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
# A rename names its target in the value rather than the key, which is the one
# operator that writes a field the keys never mention.
chk "and a rename onto it"                             "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},{\$rename:{'v':'_tcDeletedAt'}});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "and a pipeline update setting it"                 "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},[{\$set:{_tcDeletedAt:'\$v'}}]);print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
# A replacing stage says what the whole document becomes, as an expression
# rather than as a list of field names, and $replaceRoot wraps that expression
# in newRoot where $replaceWith takes it directly. Both were read at the same
# depth, so the documented spelling of the stage went straight through while the
# name sat in the guard's list looking covered.
chk "and a pipeline \$replaceWith of it"                "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},[{\$replaceWith:{_id:'\$_id',_tcDeletedAt:'\$v'}}]);print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "and the \$replaceRoot spelling of the same"        "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},[{\$replaceRoot:{newRoot:{_id:'\$_id',_tcDeletedAt:'\$v'}}}]);print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
# $mergeObjects is how a pipeline adds one field and keeps the rest, which is
# the shape an application would actually write.
chk "and a \$mergeObjects adding it to the document"    "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},[{\$replaceWith:{\$mergeObjects:['\$\$ROOT',{_tcDeletedAt:'\$v'}]}}]);print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
# An operator's keys are paths, not names: this creates a top level
# _tcDeletedAt holding a subdocument, and the predicate that hides a tombstone
# is {$exists: false}, which a subdocument satisfies as well as a date does.
chk "and a dotted path under the mark"                 "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},{\$set:{'_tcDeletedAt.x':1}});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
# And the control for that, because a prefix test that matched here would refuse
# every field whose name happens to start the same way.
chk "while a field merely starting the same is fine"   "ACCEPTED" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},{\$set:{_tcDeletedAtX:1}});print('ACCEPTED')}catch(e){print(e.message.substring(0,60))}")"
chk "and findAndModify"                                "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.findOneAndUpdate({_id:1},{\$set:{_tcDeletedAt:new Date()}});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
# An upsert's filter is a write path. mongod builds the document it creates from
# the filter's equality fields and then applies the update, so a filter alone
# writes the mark with no update operator naming it. Measured before this was
# refused: three such statements created three documents on the fast tier and
# countDocuments through the router then answered 1 of 3.
chk "and an upsert taking the mark from its filter"    "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_tcDeletedAt:new Date('2020-01-01')},{\$set:{a:1}},{upsert:true});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "and the \$eq spelling of that filter"              "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_tcDeletedAt:{\$eq:new Date('2020-01-01')}},{\$set:{a:1}},{upsert:true});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "and findAndModify upserting from its query"       "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.findAndModify({query:{_tcDeletedAt:new Date('2021-01-01'),k:9},update:{\$set:{b:2}},upsert:true});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "and the bulkWrite upsert form"                    "REFUSED" \
    "$(res "$(R "try{db.adminCommand({bulkWrite:1, ops:[{update:0, filter:{_tcBodyDropped:true,k:5}, updateMods:{\$set:{a:1}}, upsert:true}], nsInfo:[{ns:process.env.TC_DB+'.$RNS'}]});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
# Equality is the whole rule, so a range predicate on the same field has to go
# through: mongod does not carry it into the document it creates, and refusing
# it would be a guard asserting more than the thing it guards against.
chk "while the same filter without upsert is fine"     "ACCEPTED" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_tcDeletedAt:new Date('2020-01-01')},{\$set:{a:1}});print('ACCEPTED')}catch(e){print(e.message.substring(0,60))}")"
chk "and a range predicate on it, which writes nothing" "ACCEPTED" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_tcDeletedAt:{\$gt:new Date('2020-01-01')},k:3},{\$set:{a:1}},{upsert:true});print('ACCEPTED')}catch(e){print(e.message.substring(0,60))}")"
chk "and the document it created carries no mark"      "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({k:3,_tcDeletedAt:{\$exists:true}}))")"

# The controls matter as much as the refusals. A guard that turned down ordinary
# writes, or writes to collections this system does not tier, would be this
# proxy inventing a rule about data it does not touch.
# The 8.0 bulkWrite command, spelled out rather than through mongosh's
# bulkWrite() helper, which sends ordinary insert and update commands. This is
# the command that once reopened every write protection through a side door, so
# it gets the same rule asked of it directly.
chk "and the bulkWrite command"                        "REFUSED" \
    "$(res "$(R "try{db.adminCommand({bulkWrite:1, ops:[{insert:0, document:{_id:90, _tcDeletedAt:new Date()}}], nsInfo:[{ns:process.env.TC_DB+'.$RNS'}]});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "while an ordinary bulkWrite still works"          "1" \
    "$(R "db.adminCommand({bulkWrite:1, ops:[{insert:0, document:{_id:91, n:1}}], nsInfo:[{ns:process.env.TC_DB+'.$RNS'}]})" >/dev/null; H "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({_id:91}))")"

# Every operator that names the field, not a list of the likely ones. The
# predicate that hides a document is {$exists: false}, so a field of any type
# hides it: measured against the first version of this guard, $currentDate and
# $min each wrote the mark and were accepted.
chk "and \$currentDate onto it"                         "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},{\$currentDate:{_tcDeletedAt:true}});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "and \$min onto it"                                 "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},{\$min:{_tcDeletedAt:new Date()}});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
# $unset is the one that must stay allowed: it is how an operator takes the
# field off a document that has one.
chk "but \$unset of it is allowed"                      "ACCEPTED" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.updateOne({_id:1},{\$unset:{_tcDeletedAt:''}});print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")"

# A pipeline writes documents too, and the two paths are read differently: a
# tiered $out is performed by the router, which reads each document as it
# passes, while a $merge the fast tier can answer alone is forwarded for mongod
# to perform, where the stages are all there is to read. Both were accepted
# before this, and two live documents on the fast tier ended up wearing the mark.
#
# Waited for, and this is the whole reason these two checks were wrong once. The
# namespace carries no completeness claim, so a read of it falls through to the
# archive, and until mirroring catches up the archive answers nothing. A $out
# whose input is empty writes no documents, so no document carries the mark and
# accepting it is correct: the check was measuring an empty pipeline and calling
# it a product defect.
# With a feeder, because the section before this one stops it and nothing here
# had started another. Without one the archive never receives these documents,
# a read of a namespace with no completeness claim falls through to it and
# answers nothing, and the $out below runs over an empty input: it writes no
# documents, so none carries the mark, and accepting it is correct. The check
# was measuring an empty pipeline and reporting a product defect.
cline feeder --config $CFG --log-file $LOGDIR/reserved_f.log & RFP=$!
sleep 2
# Three by now: the document this section opened with, the one the ordinary
# bulkWrite control wrote, and the one the range-predicate upsert created. That
# third one arrived with the upsert filter checks and the count here was never
# moved, so every run of this suite waited the full thirty seconds and warned,
# which is how a warning stops being read.
waitMirrored $RNS 3
chk "and a \$out into a tiered collection"              "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.aggregate([{\$set:{_tcDeletedAt:new Date()}},{\$out:'$RNS'}]).toArray();print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "and a \$merge into one"                            "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.aggregate([{\$set:{_tcDeletedAt:new Date()}},{\$merge:{into:'$RNS'}}]).toArray();print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
# $merge carries a pipeline of its own: whenMatched can be a list of stages
# instead of one of the named modes, and those stages run against the document
# already in the target. Nothing in the outer pipeline mentions the field, so
# reading the outer stages alone missed it. Measured before this was refused: a
# live document in a tiered collection came back carrying the tombstone mark.
chk "and a \$merge whose whenMatched sets it"           "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.aggregate([{\$match:{_id:1}},{\$merge:{into:'$RNS',on:'_id',whenMatched:[{\$set:{_tcDeletedAt:'\$\$NOW'}}]}}]).toArray();print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
chk "and the \$addFields spelling of it"                "REFUSED" \
    "$(res "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.aggregate([{\$match:{_id:1}},{\$merge:{into:'$RNS',on:'_id',whenMatched:[{\$addFields:{_tcBodyDropped:true}}]}}]).toArray();print('ACCEPTED')}catch(e){print(/refusing this write|refusing this bulkWrite/.test(e.message)?'REFUSED':e.message.substring(0,60))}")")"
# whenMatched is a string in every ordinary merge, and reading it as a pipeline
# would panic on that. This control is here for the type as much as the rule.
chk "while an ordinary whenMatched pipeline works"      "ACCEPTED" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).$RNS.aggregate([{\$match:{_id:1}},{\$merge:{into:'$RNS',on:'_id',whenMatched:[{\$set:{tkseen:true}}]}}]).toArray();print('ACCEPTED')}catch(e){print(e.message.substring(0,60))}")"
chk "and the collection took no mark from any of it"   "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({\$or:[{_tcDeletedAt:{\$exists:true}},{_tcBodyDropped:{\$exists:true}}]}))")"

# Unacknowledged, which is the shape that has caught this project out before:
# everything the router repairs on the reply is absent for a w:0 message, and
# single-document update, delete and findAndModify were once guarded nowhere
# because of it. This guard runs before the command is forwarded rather than on
# the way back, so it holds here, and the control beside it is what makes that
# mean something: an ordinary w:0 write still lands.
R "db.getSiblingDB(process.env.TC_DB).$RNS.insertOne({_id:70,_tcDeletedAt:new Date()},{writeConcern:{w:0}})" >/dev/null 2>&1
R "db.getSiblingDB(process.env.TC_DB).$RNS.insertOne({_id:71,n:1},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 2
chk "an unacknowledged write carrying it never lands"  "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({_id:70}))")"
chk "while an ordinary unacknowledged one does"        "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({_id:71}))")"

# Counted on the fast tier, which is where the write went. Counting through the
# router asks the archive for a namespace with no completeness claim, so these
# read 0 for as long as mirroring takes and fail for a reason that has nothing
# to do with the guard.
chk "an ordinary write is untouched"                   "1" \
    "$(R "db.getSiblingDB(process.env.TC_DB).$RNS.insertOne({_id:3,n:7})" >/dev/null; H "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({_id:3}))")"
chk "a nested field of that name is fine"              "1" \
    "$(R "db.getSiblingDB(process.env.TC_DB).$RNS.insertOne({_id:4,inner:{_tcDeletedAt:1}})" >/dev/null; H "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({_id:4}))")"
chk "and so is the field on an untiered collection"    "1" \
    "$(R "db.getSiblingDB(process.env.TC_DB2).tkfree.drop(); db.getSiblingDB(process.env.TC_DB2).tkfree.insertOne({_id:1,_tcDeletedAt:new Date()}); print(db.getSiblingDB(process.env.TC_DB2).tkfree.countDocuments({}))")"
chk "and the collection never took a mark"             "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
chk "and the archive did not either"                   "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
kill $RFP 2>/dev/null; wait $RFP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$RNS.drop(); db.getSiblingDB(process.env.TC_DB2).tkfree.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$RNS.drop()" >/dev/null

echo "== the other door into a unique index =="
# createIndexes with unique: true on a tiered collection is walked and claimed,
# because the fast tier's own build sees only the half of the data that is here.
# collMod reaches the same state by a different route, and reached none of that:
#
#   collMod: NS, index: {keyPattern: {email: 1}, prepareUnique: true}
#   collMod: NS, index: {keyPattern: {email: 1}, unique: true}
#
# Measured before this was guarded, with one live document in the archive
# duplicating a value the fast tier holds: createIndexes was refused with E11000
# and both collMods were accepted, leaving a unique index over a value held by
# two live documents and zero rows in the key registry. Every later insert of
# that value was accepted too, because the registry is what remembers a value
# the fast tier cannot see.
MNS=tkcollmod
# Its own feeder. The section above this one kills the one it started, so the
# fixture below mirrored nothing and the archive never held the duplicate the
# whole section is about: every check read as the guard letting a conversion
# through when there was nothing to refuse.
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
fresh $MNS
R "db.getSiblingDB(process.env.TC_DB).$MNS.insertMany([{_id:1,email:'a@x'},{_id:2,email:'b@x'}])" >/dev/null
waitMirrored $MNS 2
# The archive keeps a live copy of a@x, which is what an eviction leaves.
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).$MNS.deleteOne({_id:1})"
chk "the archive still holds the evicted one"        "true" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).$MNS.findOne({_id:1}); print(!!d && !d._tcDeletedAt)")"
# A second live a@x on the fast tier. No unique index yet, so nothing refuses it,
# and the two halves of the duplicate are now one per tier.
R "db.getSiblingDB(process.env.TC_DB).$MNS.insertOne({_id:3,email:'a@x'})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).runCommand({createIndexes:'$MNS',indexes:[{key:{email:1},name:'tkem_1'}]})" >/dev/null
cm() { case "$1" in *E11000*) echo "REFUSED" ;; *) echo "$1" ;; esac; }
chk "prepareUnique on its own is allowed"            "ACCEPTED" \
    "$(cm "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({collMod:'$MNS',index:{keyPattern:{email:1},prepareUnique:true}});print('ACCEPTED')}catch(e){print(e.message.substring(0,60))}")")"
chk "collMod unique by key pattern is refused"       "REFUSED" \
    "$(cm "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({collMod:'$MNS',index:{keyPattern:{email:1},unique:true}});print('ACCEPTED')}catch(e){print(e.message.substring(0,60))}")")"
chk "and by index name"                              "REFUSED" \
    "$(cm "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({collMod:'$MNS',index:{name:'tkem_1',unique:true}});print('ACCEPTED')}catch(e){print(e.message.substring(0,60))}")")"
chk "the index is still not unique"                  "false" \
    "$(H "print(!!db.getSiblingDB(process.env.TC_DB).$MNS.getIndexes().filter(i=>i.name==='tkem_1')[0].unique)")"
# The claims the walk wrote are given back, or a later create of the same index
# would find its own keys already taken.
chk "and the refusal left no claims behind"          "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+'.$MNS'}))")"
# The controls. A guard that refused every collMod would be worse than the hole.
C "db.getSiblingDB(process.env.TC_DB).$MNS.deleteOne({_id:1})" >/dev/null
chk "with the duplicate gone it goes through"        "ACCEPTED" \
    "$(cm "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({collMod:'$MNS',index:{keyPattern:{email:1},unique:true}});print('ACCEPTED')}catch(e){print(e.message.substring(0,60))}")")"
chk "and the index is unique now"                    "true" \
    "$(H "print(!!db.getSiblingDB(process.env.TC_DB).$MNS.getIndexes().filter(i=>i.name==='tkem_1')[0].unique)")"
# And it claimed what was already there, which is the whole point of walking.
# Two live documents, two keys, exactly as createIndexes would have left it.
chk "and it claimed the keys already there"          "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+'.$MNS'}))")"
# Issuing it again must change nothing. The walk inserts claim rows, and a
# second walk that read its own rows as somebody else's would refuse a no-op.
chk "and issuing the same collMod again is fine"     "ACCEPTED" \
    "$(cm "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({collMod:'$MNS',index:{keyPattern:{email:1},unique:true}});print('ACCEPTED')}catch(e){print(e.message.substring(0,60))}")")"
chk "and the claims are still the same two"          "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_META).keys.countDocuments({ns:process.env.TC_DB+'.$MNS'}))")"
chk "a collMod that touches no index still works"    "ACCEPTED" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({collMod:'$MNS',validationLevel:'off'});print('ACCEPTED')}catch(e){print(e.message.substring(0,60))}")"
chk "and one naming an index that is not there"      "true" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({collMod:'$MNS',index:{name:'tknosuch',unique:true}});print('false')}catch(e){print(/cannot find index|not found/i.test(e.message))}")"
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$MNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$MNS.drop()" >/dev/null

pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/"
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
