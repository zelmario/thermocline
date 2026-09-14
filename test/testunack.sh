#!/bin/bash
# Writes that ask for no acknowledgement.
#
# An unacknowledged write, writeConcern {w:0}, is sent with the wire protocol's
# moreToCome flag set and the client never waits for a reply. The router has
# nothing to answer, so every guard it runs was skipped for those messages: the
# copy-up that lets a multi-document write reach archived documents, the unique
# key claim that stops an insert duplicating one the fast tier can no longer
# see, and the upsert judgement that stops a stub overwriting an archived
# record.
#
# That is the same shape as the compression finding. A flag the client sets for
# reasons of its own turns off every protection in the system, invisibly,
# because nothing about the write looks unusual from the outside. It is worse
# here in one way: w:0 is chosen by ordinary applications for ordinary reasons,
# in metrics writers and log ingesters, and it needs no unusual driver options.
#
# There is no differential reference for these, because the whole question is
# what happens to documents an untiered collection would not have. What a single
# cluster does with each statement is not in doubt, and it is written beside each
# check.
#
# Refusing has nowhere to go, so where the guard would have refused the right
# thing is to not perform the write. A dropped write is what w:0 already means to
# an application: it asked not to be told.
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
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-56s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-56s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() { pkill -9 -f "cline feeder --config $WDIR/" 2>/dev/null; pkill -9 -f "cline router --config $WDIR/" 2>/dev/null; }
trap cleanup EXIT

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/unack_f.log &
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/unack_r.log &
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}
stopFeeder() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
# Remove documents from the fast tier the way an eviction does, with the feeder
# blind to it, so the archive keeps a live copy. Doing it with the feeder running
# is a delete, which tombstones the archive's copies, and the collection then has
# nothing archived to lose: the suite would pass while testing nothing.
evictBehindTheFeeder() {
  stopFeeder
  H "$1" >/dev/null
  C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder
}
# The feeder has to have caught up before a check reads the archive, and it is
# always some way behind. Waiting on a sentinel beats sleeping for a guess.
waitFeeder() {
  H "db.getSiblingDB(process.env.TC_DB).tunku.insertOne({_id:'__sentinel'})" >/dev/null
  for _ in $(seq 1 120); do
    [ "$(C "print(db.getSiblingDB(process.env.TC_DB).tunku.countDocuments({_id:'__sentinel'}))")" = "1" ] && break
    sleep 0.5
  done
  H "db.getSiblingDB(process.env.TC_DB).tunku.deleteOne({_id:'__sentinel'})" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).tunku.deleteOne({_id:'__sentinel'})" >/dev/null
}

echo "== setup =="
stopDaemons
for t in tunku tunki tunkm tunk1 tunk2 tunk3 tunkx tunkxfree tunkc tunkb tunkcfree; do
  H "db.getSiblingDB(process.env.TC_DB).$t.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$t.drop()" >/dev/null
done
C "db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'});
   db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({});
   db.getSiblingDB(process.env.TC_META).residency.deleteMany({});
   db.getSiblingDB(process.env.TC_META).evicting.deleteMany({})" >/dev/null
startFeeder
startRouter

echo "== an unacknowledged upsert over a document only the archive holds =="
# On one cluster this updates the existing document and nothing is lost. Here the
# fast tier cannot see it, so an unjudged upsert creates a stub in its place and
# the feeder mirrors that over the archived record.
C "db.getSiblingDB(process.env.TC_DB).tunku.insertOne({_id:5, name:'alice', tier:'gold', balance:100})" >/dev/null
chk "the fast tier has never held it"                      "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tunku.countDocuments({_id:5}))")"
R "db.getSiblingDB(process.env.TC_DB).tunku.updateOne({_id:5},{\$set:{balance:200}},{upsert:true,writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "the fields it could not see survived"                 "alice-gold-200" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tunku.findOne({_id:5}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"
chk "and the archive was not overwritten by a stub"        "alice-gold-200" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).tunku.findOne({_id:5}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"

echo "== an unacknowledged insert of an _id the archive still holds =="
# On one cluster this is a duplicate key error and the existing document is
# untouched. The client asked not to be told, but it did not ask for the
# archived document to be replaced.
C "db.getSiblingDB(process.env.TC_DB).tunki.insertOne({_id:9, name:'carol', tier:'silver', balance:70})" >/dev/null
chk "the archive holds it and the fast tier does not"      "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tunki.countDocuments({_id:9}))")"
R "db.getSiblingDB(process.env.TC_DB).tunki.insertOne({_id:9, v:'overwrite'},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "the archived document is still whole"                 "carol-silver-70" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).tunki.findOne({_id:9}); print(d?d.name+'-'+d.tier+'-'+d.balance:'MISSING')")"

echo "== an unacknowledged deleteMany that reaches archived documents =="
# On one cluster this removes all twelve. Here nine of them are only in the
# archive, and without the copy-up the delete never sees them: they stay
# readable through the router afterwards, which is an erasure that did not erase.
H "db.getSiblingDB(process.env.TC_DB).tunkm.insertMany(Array.from({length:12},(_,i)=>({_id:i+1,grp:'g',n:i})))" >/dev/null
waitFeeder
# Take nine away from the fast tier only, which is what eviction leaves behind.
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).tunkm.deleteMany({_id:{\$gt:3}})"
chk "the fast tier holds three of twelve"                  "3" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tunkm.countDocuments({}))")"
chk "the archive still holds all twelve"                   "12" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tunkm.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
R "db.getSiblingDB(process.env.TC_DB).tunkm.deleteMany({grp:'g'},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 4
waitFeeder
chk "nothing matching is left to read"                     "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tunkm.countDocuments({grp:'g'}))")"

echo "== an unacknowledged single-document update over an archived document =="
# On one cluster this sets status on the one matching document and reports
# matched 1. Here the document is only in the archive, so the fast tier matches
# nothing.
#
# guardMultiWrite skips any statement that is not multi, and says why: a
# single-document write that matched nothing is put right on the reply, by
# interceptWrite, which resolves the filter against the archive, copies the
# document up and runs the statement again. An unacknowledged write has no
# reply. Nothing is intercepted, so the update lands nowhere and says nothing.
#
# Both filters are checked. interceptWrite resolves an _id-only filter on the
# reply as well, so naming the document outright is the same door.
H "db.getSiblingDB(process.env.TC_DB).tunk1.insertMany([
     {_id:1, ref:'a1', status:'new', tier:'gold'},
     {_id:2, ref:'a2', status:'new', tier:'gold'},
     {_id:3, ref:'a3', status:'new'}])" >/dev/null
waitFeeder
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).tunk1.deleteMany({_id:{\$lt:3}})"
chk "the fast tier holds one of three"                     "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tunk1.countDocuments({}))")"
R "db.getSiblingDB(process.env.TC_DB).tunk1.updateOne({ref:'a1'},{\$set:{status:'shipped'}},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "an update by another field reached the document"      "shipped" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tunk1.findOne({ref:'a1'}); print(d?d.status:'MISSING')")"
chk "and the fields it could not see survived"             "gold" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).tunk1.findOne({_id:1}); print(d?d.tier:'MISSING')")"
R "db.getSiblingDB(process.env.TC_DB).tunk1.updateOne({_id:2},{\$set:{status:'shipped'}},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "an update naming the _id outright reached it too"     "shipped" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tunk1.findOne({ref:'a2'}); print(d?d.status:'MISSING')")"

echo "== an unacknowledged single-document delete over an archived document =="
# On one cluster this removes the document. Here it is only in the archive, so
# without the copy-up the delete matches nothing and the document stays
# readable: an erasure that did not erase, the same failure the deleteMany
# section pins for the multi case.
H "db.getSiblingDB(process.env.TC_DB).tunk2.insertMany([
     {_id:1, ref:'b1'}, {_id:2, ref:'b2'}, {_id:3, ref:'b3'}])" >/dev/null
waitFeeder
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).tunk2.deleteMany({_id:{\$lt:3}})"
chk "the fast tier holds one of three here too"            "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tunk2.countDocuments({}))")"
R "db.getSiblingDB(process.env.TC_DB).tunk2.deleteOne({ref:'b1'},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "a delete by another field erased it"                  "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tunk2.countDocuments({ref:'b1'}))")"
R "db.getSiblingDB(process.env.TC_DB).tunk2.deleteOne({_id:2},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "and one naming the _id outright erased it too"        "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tunk2.countDocuments({_id:2}))")"

echo "== an unacknowledged findAndModify over archived documents =="
# findAndModify is the third dispatch, with a repair of its own
# (interceptFindAndModify) and a guard of its own for the sort
# (guardSortedFindAndModify). Both were reachable only through a reply.
#
# Three shapes, because they take three different routes through that guard:
# no sort at all, a sort naming a document the fast tier cannot see, and a
# filter that is nothing but an _id.
H "db.getSiblingDB(process.env.TC_DB).tunk3.insertMany([
     {_id:1, seq:1, ref:'c1', status:'new'},
     {_id:2, seq:2, ref:'c2', status:'new'},
     {_id:3, seq:3, ref:'c3', status:'new'},
     {_id:9, seq:9, ref:'c9', status:'new'}])" >/dev/null
waitFeeder
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).tunk3.deleteMany({_id:{\$lt:9}})"
chk "the fast tier holds one of four"                      "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tunk3.countDocuments({}))")"
R "db.getSiblingDB(process.env.TC_DB).runCommand({findAndModify:'tunk3',
     query:{ref:'c1'}, update:{\$set:{status:'shipped'}}, writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "an unsorted findAndModify reached the document"       "shipped" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tunk3.findOne({ref:'c1'}); print(d?d.status:'MISSING')")"
R "db.getSiblingDB(process.env.TC_DB).runCommand({findAndModify:'tunk3',
     query:{_id:2}, update:{\$set:{status:'shipped'}}, writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "one naming the _id outright reached it too"           "shipped" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tunk3.findOne({ref:'c2'}); print(d?d.status:'MISSING')")"
# The sorted case. seq 3 is the oldest document left unshipped and the fast
# tier cannot see it; seq 9 is the oldest it can. Removing by sort must take
# the one the sort names, not the oldest of the part that happens to be here.
R "db.getSiblingDB(process.env.TC_DB).runCommand({findAndModify:'tunk3',
     query:{status:'new'}, sort:{seq:1}, remove:true, writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
waitFeeder
chk "a sorted remove took the document the sort names"     "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tunk3.countDocuments({seq:3}))")"
chk "and left the one the fast tier could see"             "1" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tunk3.countDocuments({seq:9}))")"

echo "== an unacknowledged dropIndexes, which is refused rather than guarded =="
# Not every guard can be moved in front of the write. A unique index dropped
# through the router has one to drop behind it on the archive, or the archive
# goes on refusing values the router now accepts, and both halves of that live
# on the reply: which indexes are going has to be read before the fast tier
# removes them, and the archive's copy is dropped only once the reply says the
# fast tier dropped its own. Doing it before forwarding would drop the archive's
# index for a command the fast tier might turn down.
#
# So it is refused, which is the applyOps reasoning: a refusal is a limitation
# and a wrong answer is a bug. Measured before, with the archive carrying the
# index as an adoption leaves it: the fast tier ended with _id_ and the archive
# kept code_1, and of two documents the application was now entitled to write,
# one reached the archive and one did not, with nothing said anywhere.
H "db.getSiblingDB(process.env.TC_DB).tunkx.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tunkx.drop()" >/dev/null
# Seeded on the fast tier and mirrored down, rather than inserted on both. The
# feeder is running, so inserting the same _ids on the archive by hand is a
# duplicate key error, and an uncaught error in mongosh --eval ends the script:
# the createIndex on the next line would never run and the check below would
# fail for the fixture rather than for the router.
H "db.getSiblingDB(process.env.TC_DB).tunkx.insertMany([{_id:1,code:'A'},{_id:2,code:'B'}])" >/dev/null
waitFeeder
# The index on both tiers is the adoption shape: the archive arrives carrying
# its own, and the indexer puts the same one on the fast tier.
C "db.getSiblingDB(process.env.TC_DB).tunkx.createIndex({code:1},{unique:true,name:'code_1'})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).tunkx.createIndex({code:1},{unique:true,name:'code_1'})" >/dev/null
chk "both tiers carry the unique index"                    "_id_,code_1/_id_,code_1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tunkx.getIndexes().map(i=>i.name).join(','))")/$(C "print(db.getSiblingDB(process.env.TC_DB).tunkx.getIndexes().map(i=>i.name).join(','))")"
R "db.getSiblingDB(process.env.TC_DB).runCommand({dropIndexes:'tunkx', index:'code_1', writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
chk "an unacknowledged dropIndexes changes neither tier"    "_id_,code_1/_id_,code_1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tunkx.getIndexes().map(i=>i.name).join(','))")/$(C "print(db.getSiblingDB(process.env.TC_DB).tunkx.getIndexes().map(i=>i.name).join(','))")"
# And the acknowledged one still works, or the refusal is a ban on the command.
chk "an acknowledged one still drops it on both"           "_id_/_id_" \
    "$(R "db.getSiblingDB(process.env.TC_DB).runCommand({dropIndexes:'tunkx', index:'code_1'})" >/dev/null 2>&1; sleep 4;
       echo "$(H "print(db.getSiblingDB(process.env.TC_DB).tunkx.getIndexes().map(i=>i.name).join(','))")/$(C "print(db.getSiblingDB(process.env.TC_DB).tunkx.getIndexes().map(i=>i.name).join(','))")")"
# An untiered collection is none of this system's business whatever flag is set.
H "db.getSiblingDB(process.env.TC_DB).tunkxfree.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).tunkxfree.insertOne({_id:1,code:'A'});
   db.getSiblingDB(process.env.TC_DB).tunkxfree.createIndex({code:1},{name:'code_1'})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).runCommand({dropIndexes:'tunkxfree', index:'code_1', writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
chk "an untiered collection's index still goes"            "_id_" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tunkxfree.getIndexes().map(i=>i.name).join(','))")"

# What collation a tier's copy of a collection was created with, as its own
# catalog reports it. "no collection" and "none" are different answers and the
# checks below depend on telling them apart.
collationOf() { # collationOf H|C COLL
  $1 "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$2'})[0];
      if(!c){print('no collection')}
      else if(c.options && c.options.collation){print(c.options.collation.locale+'/'+c.options.collation.strength)}
      else {print('none')}"
}

echo "== an unacknowledged create carrying options the archive needs =="
# A collection's options are carried to the archive by MirrorCollection, which
# runs on the reply to the create. Unacknowledged there is no reply, so the
# archive's copy would be left to the feeder's first mirrored insert, and that
# carries no options at all.
#
# Measured before this was refused, with a control: acknowledged, both tiers end
# en/2 and find({name:'ALICE'}) is 1 on both; unacknowledged, the fast tier ends
# en/2 and the archive none, and the same find is 1 on the fast tier and 0 on
# the archive. A collation is fixed at creation and cannot be added to a
# collection that already holds documents, so unlike the rest of this suite that
# divergence never heals.
H "db.getSiblingDB(process.env.TC_DB).tunkc.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tunkc.drop()" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).runCommand({create:'tunkc',
     collation:{locale:'en', strength:2}, writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
chk "an unacknowledged create with a collation is refused"  "no collection/no collection" \
    "$(collationOf H tunkc)/$(collationOf C tunkc)"
# The acknowledged control, or the refusal is a ban on the command.
R "db.getSiblingDB(process.env.TC_DB).runCommand({create:'tunkc', collation:{locale:'en', strength:2}})" >/dev/null 2>&1
sleep 3
chk "an acknowledged one reaches both tiers with it"        "en/2/en/2" \
    "$(collationOf H tunkc)/$(collationOf C tunkc)"
R "db.getSiblingDB(process.env.TC_DB).tunkc.insertOne({_id:1, name:'alice'})" >/dev/null 2>&1
waitFeeder
# The collation is the whole point: at strength 2 'ALICE' matches 'alice', and
# a tier created without it answers a different question.
chk "and the same query means the same on both tiers"       "1/1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tunkc.countDocuments({name:'ALICE'}))")/$(C "print(db.getSiblingDB(process.env.TC_DB).tunkc.countDocuments({name:'ALICE'}))")"
# A create with nothing to carry is not refused: the collection the feeder's
# first insert makes is the same collection.
H "db.getSiblingDB(process.env.TC_DB).tunkb.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tunkb.drop()" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).runCommand({create:'tunkb', writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
chk "a bare unacknowledged create still goes through"       "none" "$(collationOf H tunkb)"
# And an untiered collection is none of this system's business whatever flag is
# set, or the refusal is about the write concern rather than about the archive.
H "db.getSiblingDB(process.env.TC_DB).tunkcfree.drop()" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).runCommand({create:'tunkcfree',
     collation:{locale:'en', strength:2}, writeConcern:{w:0}})" >/dev/null 2>&1
sleep 3
chk "an untiered one is created with its collation"         "en/2" "$(collationOf H tunkcfree)"

echo "== and an ordinary unacknowledged write still lands =="
# The fix must not turn every w:0 write into a dropped one.
R "db.getSiblingDB(process.env.TC_DB).tunku.insertOne({_id:50, v:'plain'},{writeConcern:{w:0}})" >/dev/null 2>&1
sleep 2
chk "a write with nothing to judge is performed"           "plain" \
    "$(H "const d=db.getSiblingDB(process.env.TC_DB).tunku.findOne({_id:50}); print(d?d.v:'MISSING')")"
chk "and it reaches the archive too"                       "plain" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).tunku.findOne({_id:50}); print(d?d.v:'MISSING')")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
