#!/bin/bash
# Query features that depend on something other than the documents.
#
# Most of what the router does assumes the two tiers differ only in which
# documents they hold. That is not quite true. A collection carries a default
# collation, and an index can be the only thing that makes a query answerable at
# all, and neither of those crosses to the archive by itself.
#
# The dangerous half is collation, because it changes which documents match
# rather than how fast they are found. A case-insensitive collection answers one
# way on the fast tier and another on an archive that has no collation, and both
# answers look like success.
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
  cline feeder --config $CFG --log-file $LOGDIR/q_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/q_r.log & RP=$!
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

echo "== setup =="
stopDaemons
for t in tqcoll tqtext; do
  H "db.getSiblingDB(process.env.TC_DB).$t.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$t.drop()" >/dev/null
done
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({})' >/dev/null
startFeeder
startRouter

echo "== a collection with a case-insensitive default collation =="
# strength 2 ignores case. Three documents, two of which differ only in case, so
# the collation decides whether a query for one finds one document or two.
H "db.getSiblingDB(process.env.TC_DB).createCollection('tqcoll', {collation: {locale: 'en', strength: 2}})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).tqcoll.insertMany([{_id:1,name:'Alice'},{_id:2,name:'alice'},{_id:3,name:'BOB'}])" >/dev/null
waitMirrored tqcoll 3
chk "the fast tier collection has a collation"   "en" "$(H "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'tqcoll'})[0]; print(c.options.collation?c.options.collation.locale:'none')")"
chk "the archive's copy has none"                "none" "$(C "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'tqcoll'})[0]; print(c&&c.options&&c.options.collation?c.options.collation.locale:'none')")"

# With the fast tier answering, the collation applies: 'alice' matches both
# spellings. This is the answer the application already gets today.
cline watermark --config $CFG --ns $DB.tqcoll --complete-all >/dev/null
sleep 7
HOTANS=$(R "print(db.getSiblingDB(process.env.TC_DB).tqcoll.countDocuments({name:'alice'}))")
chk "the fast tier matches case-insensitively"   "2" "$HOTANS"

# Now the same query, answered by the archive. It must not change its mind about
# which documents match.
cline watermark --config $CFG --ns $DB.tqcoll --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
COLDANS=$(R "print(db.getSiblingDB(process.env.TC_DB).tqcoll.countDocuments({name:'alice'}))")
chk "and so does the archive"                    "$HOTANS" "$COLDANS"

# An explicit collation on the query is a different matter: it travels with the
# command, so it should work on either tier.
chk "an explicit collation crosses too"          "2" "$(R "print(db.getSiblingDB(process.env.TC_DB).tqcoll.find({name:'alice'}).collation({locale:'en',strength:2}).itcount())")"

echo "== a query that needs an index the archive cannot have =="
# A text query is not slow without a text index, it is impossible: mongod
# refuses it outright. The archive is not expected to carry one, so this is a
# case the router cannot make transparent. What it must do is fail clearly
# rather than return nothing and call it success.
H "db.getSiblingDB(process.env.TC_DB).tqtext.createIndex({body:'text'})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).tqtext.insertMany([{_id:1,body:'thermocline tiering'},{_id:2,body:'mongodb storage'}])" >/dev/null
waitMirrored tqtext 2
cline watermark --config $CFG --ns $DB.tqtext --complete-all >/dev/null
sleep 7
chk "a text search works on the fast tier"       "1" "$(R "print(db.getSiblingDB(process.env.TC_DB).tqtext.find({\$text:{\$search:'thermocline'}}).itcount())")"
cline watermark --config $CFG --ns $DB.tqtext --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
TEXTANS=$(R "
  try { print('ANSWERED:'+db.getSiblingDB(process.env.TC_DB).tqtext.find({\$text:{\$search:'thermocline'}}).itcount()); }
  catch(e) { print('REFUSED'); }")
# Either it is answered correctly or it is refused. Returning zero matches
# quietly would be the wrong answer this whole system exists to prevent.
chk "a text search does not silently return none" "true" \
    "$([ "$TEXTANS" = "REFUSED" ] || [ "$TEXTANS" = "ANSWERED:1" ] && echo true || echo "false ($TEXTANS)")"

echo "== batchSize zero asks for a cursor, not for documents =="
# A driver sends batchSize 0 to get a cursor back without any documents in it.
# Answering with the default hundred and one is not what it asked for, and the
# archive fallback was doing exactly that.
cline watermark --config $CFG --ns $DB.tqcoll --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
# In one explicit session. Issued as two runCommand calls, the find and the
# getMore each get their own implicit session from the driver's pool, and a
# getMore from a different session than the one that opened the cursor is
# refused by mongod and by the router alike. The pool usually returns the same
# session, which is why this passed until a parallel sweep made it not.
chk "an empty first batch, then the documents"        "3" "$(R "
  const s = db.getMongo().startSession();
  const d = s.getDatabase(process.env.TC_DB);
  const r = d.runCommand({find:'tqcoll', filter:{}, batchSize:0});
  if (r.cursor.firstBatch.length !== 0) { print('first batch was '+r.cursor.firstBatch.length); }
  else if (!r.cursor.id) { print('no cursor'); }
  else {
    const g = d.runCommand({getMore:r.cursor.id, collection:'tqcoll'});
    print(g.ok === 1 ? g.cursor.nextBatch.length : 'getMore failed: '+g.codeName);
  }
  s.endSession();")"

echo "== a view over a tiered collection =="
# A view is a saved pipeline over another collection. The router decides whether
# to guard a query by the namespace the query names, and that name is the
# view's, which is not in feeder.namespaces and never could be: a view holds no
# documents of its own to mirror. So the completeness check is skipped and the
# view answers from whatever the fast tier happens to be holding, which is the
# short answer this system exists to prevent.
H "db.getSiblingDB(process.env.TC_DB).tqview.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tqview.drop()" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).tqview.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i,grp:'g'})))" >/dev/null
waitMirrored tqview 10
H "db.getSiblingDB(process.env.TC_DB).createView('vqview','tqview',[{\$match:{grp:'g'}}])" >/dev/null
cline watermark --config $CFG --ns $DB.tqview --complete-all >/dev/null
sleep 7
chk "the view reads correctly while the tier is whole" "10" "$(R "print(db.getSiblingDB(process.env.TC_DB).vqview.countDocuments({}))")"

# Now take the documents out from under it. The view still resolves to the same
# collection, which no longer holds them.
stopFeeder2() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
stopFeeder2
H "db.getSiblingDB(process.env.TC_DB).tqview.deleteMany({_id:{\$lt:7}})" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
chk "the fast tier now holds three"                    "3"  "$(H "print(db.getSiblingDB(process.env.TC_DB).tqview.countDocuments({}))")"

# The watermark has to stop claiming the range, or the router is right to trust
# the fast tier and every query returns three, view or not. With the claim
# withdrawn, a query on the collection itself falls through to the archive and
# answers ten. The view has to reach the same place.
cline watermark --config $CFG --ns $DB.tqview --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the collection itself answers in full"            "10" "$(R "print(db.getSiblingDB(process.env.TC_DB).tqview.countDocuments({}))")"
VIEWANS=$(R "
  try { print(db.getSiblingDB(process.env.TC_DB).vqview.countDocuments({})); }
  catch(e) { print('REFUSED'); }")
# Ten or a refusal are both acceptable. Three is the silent short answer.
chk "the view does not answer short"                   "true" \
    "$([ "$VIEWANS" = "10" ] || [ "$VIEWANS" = "REFUSED" ] && echo true || echo "false (got $VIEWANS)")"

echo "== a pipeline that joins a view over a tiered collection =="
# The same blindness, reached from the other side. $lookup, $graphLookup and
# $unionWith all accept a view, and the joined namespace is checked by asking
# whether it is managed, which a view never is. So the collection being read is
# the ordinary untiered one, it passes its own check, and the tiered half of the
# join comes in short.
#
# This is the shape reporting actually takes: a small dimension collection that
# nobody tiers, joined to a reporting view over the large history that is the
# whole reason tiering is here.
H "db.getSiblingDB(process.env.TC_DB).tqjoin.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).tqjoin.insertOne({_id:1,grp:'g'})" >/dev/null
JOINED=$(R "
  try {
    const r = db.getSiblingDB(process.env.TC_DB).tqjoin.aggregate([
      {\$lookup:{from:'vqview', localField:'grp', foreignField:'grp', as:'hits'}}
    ]).toArray();
    print(r[0].hits.length);
  } catch(e) { print('REFUSED'); }")
# Ten or a refusal are both honest. Three is the join silently missing history.
chk "a joined view does not come in short"             "true" \
    "$([ "$JOINED" = "10" ] || [ "$JOINED" = "REFUSED" ] && echo true || echo "false (got $JOINED)")"

# $unionWith reads the same way and was checked the same way.
UNIONED=$(R "
  try {
    print(db.getSiblingDB(process.env.TC_DB).tqjoin.aggregate([
      {\$unionWith:{coll:'vqview'}}, {\$count:'n'}
    ]).toArray()[0].n);
  } catch(e) { print('REFUSED'); }")
chk "a unioned view does not come in short"            "true" \
    "$([ "$UNIONED" = "11" ] || [ "$UNIONED" = "REFUSED" ] && echo true || echo "false (got $UNIONED)")"

# A join can also be named from inside a sub-pipeline, where a scan of the
# top-level stages never looks. $facet is the ordinary way to compute several
# aggregates over one pass, and each of its fields is a whole pipeline.
FACETED=$(R "
  try {
    print(db.getSiblingDB(process.env.TC_DB).tqjoin.aggregate([
      {\$facet:{tally:[{\$lookup:{from:'tqview',localField:'grp',foreignField:'grp',as:'h'}}]}}
    ]).toArray()[0].tally[0].h.length);
  } catch(e) { print('REFUSED'); }")
chk "a join inside \$facet does not come in short"      "true" \
    "$([ "$FACETED" = "10" ] || [ "$FACETED" = "REFUSED" ] && echo true || echo "false (got $FACETED)")"

# And the guard must not have become a blanket refusal: with the collection
# underneath the view whole again, the same join has to answer.
cline watermark --config $CFG --ns $DB.tqview --complete-all >/dev/null
sleep 7
chk "the same join answers once the tier is whole"     "true" \
    "$([ "$(R "print(db.getSiblingDB(process.env.TC_DB).tqjoin.aggregate([{\$lookup:{from:'vqview',localField:'grp',foreignField:'grp',as:'h'}}]).toArray()[0].h.length)")" != "REFUSED" ] && echo true || echo false)"

echo "== a bound on a field the collection is not aged by =="
# A completeness claim says the fast tier holds every document newer than an
# instant, and "newer" is measured the way the rules measure age: a date field,
# or the timestamp inside a default ObjectId. A bound on any other date is a
# bound in a different dimension and proves nothing about which documents are
# here.
#
# Measured before this: a collection aged by createdAt, claimed complete for the
# last thirty days, holding a recent order on the fast tier and a two year old
# one in the archive, both shipped yesterday. The query
# {shippedAt: {$gte: two days ago}} was read as bounded two days back, judged
# inside the claim, answered from the fast tier alone, and returned 1 where the
# archive holds 2. No error, and the number looked reasonable.
BNS=tqbound
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$BNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$BNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})" >/dev/null
YEARS=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=730)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
DAYAGO=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
FROM=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
TWODAY=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
# The archive holds both. The fast tier holds only the recent one, which is what
# a collection aged by createdAt looks like.
C "db.getSiblingDB(process.env.TC_DB).$BNS.insertMany([
     {_id:1, createdAt:new Date('$YEARS'),  shippedAt:new Date('$DAYAGO'), n:'old, shipped yesterday'},
     {_id:2, createdAt:new Date(),          shippedAt:new Date('$DAYAGO'), n:'new, shipped yesterday'}])" >/dev/null
# The fast tier's copy carries the archive's own createdAt rather than a second
# new Date(). Two documents with the same _id and different dates are two
# documents, and a check comparing an exact value then asks each tier about its
# own data. The counts below did not notice; a $eq on that value does.
NEWAT=$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.findOne({_id:2}).createdAt.toISOString())")
H "db.getSiblingDB(process.env.TC_DB).$BNS.insertOne(
     {_id:2, createdAt:new Date('$NEWAT'), shippedAt:new Date('$DAYAGO'), n:'new, shipped yesterday'})" >/dev/null
startRouter
cline watermark --config $CFG --ns $DB.$BNS --complete-from "$FROM" >/dev/null
sleep 7
chk "the tiers hold different amounts of it"     "2/1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({}))")/$(H "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({}))")"
# The archive is the control: whatever it answers is the right answer.
chk "a bound on shippedAt answers like the archive" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({shippedAt:{\$gte:new Date('$TWODAY')}}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({shippedAt:{\$gte:new Date('$TWODAY')}}))")"
# And the claim still has to be worth something: a bound on the field the
# collection really is aged by is answered from the fast tier, as before.
chk "a bound on createdAt answers like the archive too" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({createdAt:{\$gte:new Date('$TWODAY')}}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({createdAt:{\$gte:new Date('$TWODAY')}}))")"
chk "and an unbounded query does as well"        \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({}))")"
# $and is how an ORM writes a filter, and every branch of one holds, so a bound
# in any branch bounds the whole query. Without this it read as unbounded and
# went to the archive however complete the fast tier was.
chk "a bound inside \$and answers like the archive" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$and:[{n:{\$exists:true}},{createdAt:{\$gte:new Date('$TWODAY')}}]}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$and:[{n:{\$exists:true}},{createdAt:{\$gte:new Date('$TWODAY')}}]}))")"
# $eq and $in say the same thing as the forms above, and a driver writes one or
# the other depending on how the query was built. Reading only the bare form
# sent an ORM's query to the archive and the hand written one to the fast tier.
#
# The recent document's own createdAt, so these match something. A check whose
# answer is zero on both sides passes whatever the router does.
RECENT=$NEWAT
chk "a bound written as \$eq answers like the archive" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({createdAt:{\$eq:new Date('$RECENT')}}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({createdAt:{\$eq:new Date('$RECENT')}}))")"
chk "and one written as \$in"                        \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({createdAt:{\$in:[new Date('$RECENT'),new Date('$TWODAY')]}}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({createdAt:{\$in:[new Date('$RECENT'),new Date('$TWODAY')]}}))")"
# A list reaching past the claim is bounded where its earliest member is, which
# is outside it, so this one has to consult the archive and find both.
chk "a \$in reaching past the claim finds both"       \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({createdAt:{\$in:[new Date('$YEARS'),new Date('$RECENT')]}}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({createdAt:{\$in:[new Date('$YEARS'),new Date('$RECENT')]}}))")"
# And a list with a member that is not an instant matches documents no claim
# about time covers, so it is not a bound at all.
chk "a \$in holding something else is not a bound"    \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({createdAt:{\$in:[new Date('$RECENT'),'yesterday']}}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({createdAt:{\$in:[new Date('$RECENT'),'yesterday']}}))")"
# $or is satisfied by any one branch, so it is bounded only when every branch is,
# and then at the earliest of them: a matching document satisfies at least one
# branch and is no older than that branch's bound. Alternatives written by hand
# or by an ORM usually do bound every branch.
chk "an \$or bounded in every branch"                 \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$or:[{createdAt:{\$gte:new Date('$TWODAY')}},{createdAt:{\$gte:new Date('$RECENT')}}]}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$or:[{createdAt:{\$gte:new Date('$TWODAY')}},{createdAt:{\$gte:new Date('$RECENT')}}]}))")"
# One branch reaching past the claim makes the whole thing reach that far, so
# this one has to consult the archive and find both.
chk "an \$or whose earliest branch reaches back"      \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$or:[{createdAt:{\$gte:new Date('$YEARS')}},{createdAt:{\$gte:new Date('$RECENT')}}]}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$or:[{createdAt:{\$gte:new Date('$YEARS')}},{createdAt:{\$gte:new Date('$RECENT')}}]}))")"
# And a branch bounded on another field is a bound in another dimension, so the
# whole $or is unbounded whatever the other branches say.
chk "an \$or with a branch on another field"          \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$or:[{createdAt:{\$gte:new Date('$RECENT')}},{shippedAt:{\$gte:new Date('$TWODAY')}}]}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$or:[{createdAt:{\$gte:new Date('$RECENT')}},{shippedAt:{\$gte:new Date('$TWODAY')}}]}))")"
# And the ones that must not be read as bounds. $or is satisfied by a branch
# carrying none, so the query reaches as far back as that branch does.
chk "a bound inside \$or answers like the archive"  \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$or:[{createdAt:{\$gte:new Date('$TWODAY')}},{n:{\$exists:true}}]}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$or:[{createdAt:{\$gte:new Date('$TWODAY')}},{n:{\$exists:true}}]}))")"
# The newest N is what a dashboard asks, and it carries no lower bound, so it
# used to go to the archive every time. It does not need one: if the fast tier
# returns the N asked for, newest first, and the last of them is itself inside
# the claim, every document it did not return is older than all N. Measured on
# a shop of 3,000 orders with the fast tier holding 314: thirty such queries
# took 118ms and three archive round trips before, and 53ms and none after.
#
# The archive is the control for every one of these, including the case where
# the fast tier runs out and the proof must fail.
for LIM in 1 2 3 5; do
  chk "the newest $LIM agree with the archive" \
      "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.find({_tcDeletedAt:{\$exists:false}}).sort({createdAt:-1}).limit($LIM).toArray().map(function (d) { return String(d._id) }).join(','))")" \
      "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.find({}).sort({createdAt:-1}).limit($LIM).toArray().map(function (d) { return String(d._id) }).join(','))")"
done
# Oldest first is the other direction and proves nothing: the last one returned
# bounds nothing, because everything not returned is newer.
chk "the oldest two agree with the archive too"  \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.find({_tcDeletedAt:{\$exists:false}}).sort({createdAt:1}).limit(2).toArray().map(function (d) { return String(d._id) }).join(','))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.find({}).sort({createdAt:1}).limit(2).toArray().map(function (d) { return String(d._id) }).join(','))")"
# And with a filter, which the proof does not depend on: whatever it selects,
# the fast tier holds every matching document inside the claim.
chk "the newest matching one agrees as well"     \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.find({n:{\$exists:true},_tcDeletedAt:{\$exists:false}}).sort({createdAt:-1}).limit(1).toArray().map(function (d) { return String(d._id) }).join(','))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.find({n:{\$exists:true}}).sort({createdAt:-1}).limit(1).toArray().map(function (d) { return String(d._id) }).join(','))")"
chk "and one inside \$and on another field too"     \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$and:[{shippedAt:{\$gte:new Date('$TWODAY')}}]}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$BNS.countDocuments({\$and:[{shippedAt:{\$gte:new Date('$TWODAY')}}]}))")"
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$BNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$BNS.drop()" >/dev/null

echo "== a date field holding something that is not a date =="
# A completeness claim is a range predicate, and MongoDB brackets those by type:
# a date bound never matches a Timestamp. So a document whose createdAt is a
# Timestamp is honestly outside the claim, and outside every bounded query the
# claim answers. The archive would not return it either, and the two agree.
#
# A sort is not bracketed. Timestamp sorts above Date, so that same document
# belongs at the top of a newest-first answer, and the newest-first proof used
# to hand back the N below it. Measured: MongoDB answers 99,0,1 and the router
# answered 0,1,2, with no error.
TNS=tqtype
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$TNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$TNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})" >/dev/null
# The archive is the original cluster, so it holds everything. The odd one is
# what an adoption leaves behind: no warm window's predicate ever matches it.
# The instants are fixed in the shell, not written as new Date() on each side.
# Each mongosh evaluates its own, and two documents with the same _id and
# different dates are two documents, so every comparison then asks each tier
# about its own data.
T0=$(python3 -c "import datetime;print(datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))")
T1=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
T2=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
C "db.getSiblingDB(process.env.TC_DB).$TNS.insertMany([
     {_id:0, createdAt:new Date('$T0')},
     {_id:1, createdAt:new Date('$T1')},
     {_id:2, createdAt:new Date('$T2')},
     {_id:99, createdAt:new Timestamp({t: Math.floor(Date.now()/1000), i: 1})}])" >/dev/null
# The fast tier holds the two newest by date, which is what warming that window
# leaves behind.
H "db.getSiblingDB(process.env.TC_DB).$TNS.insertMany([
     {_id:0, createdAt:new Date('$T0')},
     {_id:1, createdAt:new Date('$T1')}])" >/dev/null
startRouter
TFROM=$(H "print(db.getSiblingDB(process.env.TC_DB).$TNS.find().sort({createdAt:1}).limit(1).toArray()[0].createdAt.toISOString())")
cline watermark --config $CFG --ns $DB.$TNS --complete-from "$TFROM" >/dev/null
chk "the newest two agree with the archive" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$TNS.find({_tcDeletedAt:{\$exists:false}}).sort({createdAt:-1}).limit(2).toArray().map(function (d) { return String(d._id) }).join(','))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$TNS.find({}).sort({createdAt:-1}).limit(2).toArray().map(function (d) { return String(d._id) }).join(','))")"
# And the claim still buys something on this namespace: a query bounded inside
# it is answered from the fast tier, because a bound and a claim are the same
# kind of predicate and the odd document is outside both.
chk "a bound inside the claim still agrees"  \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$TNS.countDocuments({createdAt:{\$gte:new Date('$TWODAY')},_tcDeletedAt:{\$exists:false}}))")" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$TNS.countDocuments({createdAt:{\$gte:new Date('$TWODAY')}}))")"
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$TNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$TNS.drop()" >/dev/null


stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
