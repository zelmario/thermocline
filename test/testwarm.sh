#!/bin/bash
# The warmer's completeness claim, and what it takes to withdraw one.
#
# A completeness claim is the router's licence to answer a range query from the
# fast tier alone. If the fast tier quietly loses documents inside a claimed
# range, every such query returns a short answer and reports success. Nothing
# else in the system notices, which is why the warmer has to.
#
# The hard part is telling a hole from a deletion. The archive is upsert only,
# so a document the application deleted is tombstoned there rather than removed:
# what is live in the archive is exactly what the fast tier should hold. Both
# directions are tested here, because a check that refilled deleted documents
# would be worse than no check at all.
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
# router.freshness: permissive for the whole suite. Its fixtures plant
# documents in the archive with no feeder running, and nothing mirroring can
# never be shown fresh, so the default since 2026-10-12, strict, refuses the
# reads this suite is here to check. testfresh tests strict itself.
CFG_STRICT=$CFG
CFG=$WDIR/$(basename "$0" .sh)_permissive.yaml
sed 's/^router:$/router:\n  freshness: permissive/' "$CFG_STRICT" > "$CFG"
grep -q 'freshness: permissive' "$CFG" || { echo "  FATAL  no router: block in $CFG_STRICT"; exit 2; }
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
NS=orders
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-48s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-48s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do
    pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0
    sleep 0.5
  done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/warm_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/warm_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin \
      --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}
waitMirrored() {
  local n=""
  for _ in $(seq 1 60); do
    n=$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$1" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the archive never reached $1 live documents, last saw ${n:-none}"
}
# Remove documents from the fast tier without the feeder seeing it. This is what
# a restore from an old backup, or a botched repair, looks like: the archive
# still holds them live, so they are missing rather than deleted.
loseFromFastTier() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
  H "$1" >/dev/null
  C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder
}

echo "== setup =="
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})" >/dev/null
startFeeder
startRouter

# Real ObjectId ids, because this rule reads a document's age from the timestamp
# inside its _id. Integer ids sort before every ObjectId in BSON, so a window
# filter of _id >= ObjectId(...) matches none of them and every count comes back
# zero, which looks exactly like a fast tier with nothing missing.
R "const c=db.getSiblingDB(process.env.TC_DB).$NS; const d=[];
   for(let i=0;i<20;i++) d.push({_id:new ObjectId(),n:i,tag:'warm'});
   c.insertMany(d);" >/dev/null
waitMirrored 20
chk "seeded and mirrored" "20" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== a full fast tier earns a claim =="
cline warmer --config $CFG >/dev/null 2>&1
chk "the watermark is claimed" "true" "$(cline watermark --config $CFG --ns $DB.$NS 2>&1 | grep -qE 'newer than|all of history' && echo true || echo false)"
chk "the fast tier still holds everything" "20" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

echo "== a warm that covered everything says so, and it is worth saying =="
# A watermark says "complete for everything newer than T". A query carrying no
# lower time bound reaches arbitrarily far back, so no such watermark can cover
# it: it falls through to the archive however full the fast tier is. Only a
# claim on all of history covers it, which watermark.Mark.CoversAll has always
# said.
#
# That is every adoption. The warm copies the whole collection up and the claim
# it leaves still refuses the query an application asks most, give me this
# customer's orders. Measured on a 50,000 order shop whose orders had all just
# been warmed: 41% of the run still answered from the archive at 284 ops/s and
# 62.74ms, against 100% from the fast tier at 1,384 ops/s and 2.65ms once the
# same tier was declared complete for all of history.
#
# The claim is checked here, and then the thing the claim is for: a filter with
# no time bound in it must not reach the archive.
chk "the claim covers all of history" "true" \
    "$(cline watermark --config $CFG --ns $DB.$NS 2>&1 | grep -qE 'all of history' && echo true || echo false)"
# Read through the router's own counter of archive reads. Checked for being
# readable first, and deliberately: an unreachable dashboard makes both samples
# empty, and a delta of nothing minus nothing is zero, which is the answer this
# check wants. A check that cannot run must not look like one that passed.
# The snapshot is pretty printed, so the colon is followed by a space. A pattern
# without it matches nothing, and nothing is what made this check pass before it
# was ever right.
arch() { curl -s http://127.0.0.1:$PDASH/api/snapshot | grep -oE '"from_archive":[[:space:]]*[0-9]+' | grep -oE '[0-9]+'; }
# The router caches each namespace's claim for watermark.ClaimCacheTTL, five
# seconds, and it may have read this one before the warm made it. Answering from
# the archive until then is the safe side and the contract. This check used to
# get that wait for free: the warmer spent five seconds waiting for the feeder
# to notice its heartbeat before it copied anything, and that heartbeat is gone.
# On 2026-09-25 one read in the first second went to the archive.
sleep 6
BEFORE=$(arch)
chk "the router's archive counter is readable" "true" "$([ -n "$BEFORE" ] && echo true || echo false)"
chk "an unbounded query still finds them all"  "20" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({tag:'warm'}))")"
AFTER=$(arch)
chk "and no part of it went to the archive"    "0" "$(( ${AFTER:-0} - ${BEFORE:-1} ))"

echo "== the warm stops before it fills the disk =="
# The one failure that ends a deployment rather than slowing it. The warmer
# fills the fast tier, which is smaller than the archive by design, and a warm
# window is bounded by time and not by size: a window chosen from the age of the
# data rather than the size of the tier fills the disk. On the first day of an
# adoption the evictor cannot make room either, because a new replica set's
# oplog window is its own age.
#
# The limit is set above what is actually free, which is what a tier about to
# fill looks like from here.
# Scoped to the warmer block: `residency:` appears under router: too, and an
# unscoped substitution put the limit in both, where the router does not read it.
# The disk floor has to be the only limit that can fire. Under four workers the
# fast tier's replication runs seconds behind, and the warmer pauses on that
# instead: measured, "replication is 14s behind, limit 10s", with the disk never
# reached inside the timeout. That is the warmer working, and it is not what
# this section is asking about, so the replication limit is put out of range in
# the same derived config.
sed -e '/^warmer:/,/^router:/{s/^  residency: .*/&\n  min_free_disk: 0.99/}' \
    -e '/^warmer:/,/^router:/{s/^    max_replication_lag: .*/    max_replication_lag: 600s/}' \
    $CFG > $WDIR/nodisk.yaml
chk "the derived config sets a free disk floor"    "1" \
    "$(grep -c 'min_free_disk: 0.99' $WDIR/nodisk.yaml)"
chk "and puts the replication limit out of reach"  "1" \
    "$(grep -c 'max_replication_lag: 600s' $WDIR/nodisk.yaml)"
# There has to be something to fill, or the warmer reports "already complete"
# and never reaches the loop the limit is checked in. An earlier section leaves
# a claim on this namespace, so withdraw it first.
cline watermark --config $CFG --ns $DB.$NS --clear >/dev/null 2>&1
rm -f $LOGDIR/warm_disk.log
timeout 40 cline warmer --config $WDIR/nodisk.yaml --log-file $LOGDIR/warm_disk.log >/dev/null 2>&1
chk "the warmer pauses and says how full it is"    "true" \
    "$(grep -qE "filesystem is .* free, limit" $LOGDIR/warm_disk.log && echo true || echo false)"
# And the evictor must not be held back by the same thing: it is what makes room.
rm -f $LOGDIR/evict_disk.log
cline evictor --config $WDIR/nodisk.yaml --once --log-file $LOGDIR/evict_disk.log >/dev/null 2>&1
chk "the evictor is not paused by a full disk"     "0" \
    "$(grep -c "filesystem is" $LOGDIR/evict_disk.log || true)"

echo "== a rule that cannot address the collection it names =="
# With no date_field a rule reads each document's age from the timestamp inside
# its _id, which only an ObjectId has. MongoDB brackets range predicates by BSON
# type, so against integer, string or UUID _ids the range matches nothing at
# all. Sorting uses the canonical type order; queries do not.
#
# Obeying such a rule is silent in both directions. Measured before this was
# refused, on ten integer-keyed documents held only by the archive: the warmer
# reported "warm complete, scanned 0" and then claimed the collection complete
# for all of history, and the router answered countDocuments({}) with 0 where
# the original cluster answers 10. The collection had disappeared and every part
# of the system reported success.
H "db.getSiblingDB(process.env.TC_DB).tintid.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tintid.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tintid.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i})))" >/dev/null
chk "the archive holds them, the fast tier does not" "10/0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tintid.countDocuments({}))")/$(H "print(db.getSiblingDB(process.env.TC_DB).tintid.countDocuments({}))")"
rm -f $LOGDIR/warm_intid.log
cline warmer --config $CFG --log-file $LOGDIR/warm_intid.log >/dev/null 2>&1
chk "the warmer refuses the rule and says why"      "true" \
    "$(grep -q 'cannot address' $LOGDIR/warm_intid.log && echo true || echo false)"
chk "and it claims nothing for that namespace"      "true" \
    "$(cline watermark --config $CFG --ns $DB.tintid 2>&1 | grep -q 'claims nothing' && echo true || echo false)"
# The consequence, which is the whole point: with no claim the read falls
# through to the archive and the application still sees its collection.
chk "the application still sees every document"     "10" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tintid.countDocuments({}))")"
H "db.getSiblingDB(process.env.TC_DB).tintid.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tintid.drop()" >/dev/null

echo "== a hole inside the claimed range must withdraw the claim =="
# Eight documents vanish from the fast tier. The archive still has them live, so
# this is loss, not deletion, and the claim is now a lie.
loseFromFastTier "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({n:{\$lt:8}})"
chk "the fast tier is short"           "12" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "the archive still holds them all" "20" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
rm -f $LOGDIR/warm_verify.log
cline warmer --config $CFG --log-file $LOGDIR/warm_verify.log >/dev/null 2>&1
chk "the warmer said the claim was unsupportable" "true" \
    "$(grep -qE 'discarding a completeness claim' $LOGDIR/warm_verify.log && echo true || echo false)"
chk "and reported how many were missing"          "true" \
    "$(grep -qE 'missing=8' $LOGDIR/warm_verify.log && echo true || echo false)"
chk "the hole was refilled"                       "20" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "and the claim is back"                       "true" \
    "$(cline watermark --config $CFG --ns $DB.$NS 2>&1 | grep -qE 'newer than|all of history' && echo true || echo false)"

echo "== a deletion is not a hole =="
# The opposite case, and the dangerous one. Five documents are deleted through
# the router, so the feeder tombstones them in the archive. The fast tier now
# legitimately holds fewer than the archive stores, and a warmer that treated
# that as loss would bring deleted records back to life.
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({n:{\$gte:15}})" >/dev/null
waitMirrored 15
chk "the fast tier holds the survivors" "15" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "the archive tombstoned the rest"   "5"  "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
rm -f $LOGDIR/warm_del.log
cline warmer --config $CFG --log-file $LOGDIR/warm_del.log >/dev/null 2>&1
chk "the claim survives a deletion"     "false" \
    "$(grep -qE 'discarding a completeness claim' $LOGDIR/warm_del.log && echo true || echo false)"
chk "and nothing was resurrected"       "15" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "the deleted documents stay gone"   "0"  "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({n:{\$gte:15}}))")"

echo "== widening the window must copy what it then claims =="
# The scan has to run in the order the watermark is expressed in. A date_field
# rule tiers by that field and the watermark records a date, but the walk ran
# newest _id first and resumed by turning the date watermark into an _id bound.
# Those two orders agree only while a collection's _id order tracks its date
# field, which is the usual case and not the interesting one.
#
# Found on MongoDB's sample datasets. sample_mflix.comments has ObjectIds minted
# when the dataset was published in 2018 and comments dated 1970 to 2017, so
# resuming at the ten year boundary asked for `_id < ObjectId(2016-08-23)`,
# which no document in the collection matches. The scan returned nothing, the
# loop read that as "the window is done", and the claim moved five years. The
# archive held 5,233 documents newer than the new boundary and the fast tier
# held 889 of them, so a range query inside those five years would have been
# answered from the fast tier alone and returned a sixth of the rows, as a
# success. The warmer is one-shot, so nothing would have looked again.
#
# tdated reproduces that shape deliberately: every _id is minted now and the
# dates run backwards, so _id order says nothing about date order.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).tdated.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tdated.drop()" >/dev/null
C 'const d=[]; for (let i=0;i<60;i++) d.push({_id:new ObjectId(), n:i, at:new Date(Date.now()-i*60*1000)});
   db.getSiblingDB(process.env.TC_DB).tdated.insertMany(d)' >/dev/null
cline watermark --config $CFG --ns $DB.tdated --clear >/dev/null 2>&1
# Two configs, a narrow window and a wide one, so the second run is the
# incremental step that used to scan nothing.
for span in 20 50; do
  sed '/^warmer:/,/^router:/ s|^  rules:|  rules:\n    - ns: "'"$DB.tdated"'"\n      date_field: "at"\n      back: '"$span"'m|' \
      $CFG > $WDIR/warm_dated_$span.yaml
done
chk "the derived configs name the collection" "2" \
    "$(grep -lc "ns: \"$DB.tdated\"" $WDIR/warm_dated_20.yaml $WDIR/warm_dated_50.yaml | wc -l)"
cline warmer --without-feeder --config $WDIR/warm_dated_20.yaml >/dev/null 2>&1
NARROW=$(H "print(db.getSiblingDB(process.env.TC_DB).tdated.countDocuments({}))")
chk "the narrow warm filled its window"       "true" \
    "$(awk -v n="$NARROW" 'BEGIN{print (n+0 > 0 && n+0 < 60) ? "true" : "false"}')"
rm -f $LOGDIR/warm_wide.log
cline warmer --without-feeder --config $WDIR/warm_dated_50.yaml --log-file $LOGDIR/warm_wide.log >/dev/null 2>&1
chk "widening it scanned the newly exposed slice" "true" \
    "$(grep -qE 'warm complete scanned=0 ' $LOGDIR/warm_wide.log && echo false || echo true)"
# The invariant, and the only one that matters: everything the claim covers is
# actually there. Asked of both tiers with the claim's own boundary.
WM=$(cline watermark --config $CFG --ns $DB.tdated 2>&1 | sed 's/.*newer than //')
chk "the fast tier holds all the claim covers" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tdated.countDocuments({at:{\$gte:new Date('$WM')},_tcDeletedAt:{\$exists:false}}))")" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tdated.countDocuments({at:{\$gte:new Date('$WM')}}))")"
H "db.getSiblingDB(process.env.TC_DB).tdated.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tdated.drop()" >/dev/null

echo "== never claim a window the fast tier no longer holds =="
# A claim describes the fast tier as it is now, not as it was when the warm
# started copying. An evictor working on the same namespace takes documents away
# while the warm fills, and their windows overlap whenever the hot window
# reaches further back than the eviction cutoff.
#
# Measured on the pilot with a warm reaching back fifteen years against an
# evictor removing anything over a day old: the warm finished its scan, claimed
# all of history, and by then the fast tier held 4,400 of the 10,000 documents
# the archive holds. A client counting the collection was answered 4,400, as a
# success.
#
# Here the removal is done by hand, during the warm, which is what the evictor
# does and is the only part of it that matters. The warm is slowed to a crawl so
# the removal lands inside it rather than racing it.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).tshrink.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tshrink.drop()" >/dev/null
cline watermark --config $CFG --ns $DB.tshrink --clear >/dev/null 2>&1
C 'const d=[]; for (let i=0;i<120;i++) d.push({_id:new ObjectId(), n:i, at:new Date(Date.now()-i*60*1000)});
   db.getSiblingDB(process.env.TC_DB).tshrink.insertMany(d)' >/dev/null
sed -e '/^warmer:/,/^router:/ s|^  rules:|  rules:\n    - ns: "'"$DB.tshrink"'"\n      date_field: "at"\n      back: 24h|' \
    -e '/^warmer:/,/^router:/ s|^  rate: .*|  rate: 12|' \
    -e '/^warmer:/,/^router:/ s|^  batch_size: .*|  batch_size: 6|' $CFG > $WDIR/warm_shrink.yaml
chk "the derived config warms this collection slowly" "true" \
    "$(grep -q "ns: \"$DB.tshrink\"" $WDIR/warm_shrink.yaml && grep -q "rate: 12" $WDIR/warm_shrink.yaml && echo true || echo false)"

# Truncated first. --log-file appends, so a line left by an earlier run makes
# the "it said why" check pass against a binary that never said it.
rm -f $LOGDIR/warm_shrink.log
cline warmer --without-feeder --config $WDIR/warm_shrink.yaml --log-file $LOGDIR/warm_shrink.log >/dev/null 2>&1 &
WP=$!
# Wait until it has copied something, then take documents away underneath it,
# which is exactly what a sweep does.
for _ in $(seq 1 40); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).tshrink.countDocuments({}))")" -gt 20 ] 2>/dev/null && break
  sleep 0.5
done
H "db.getSiblingDB(process.env.TC_DB).tshrink.deleteMany({n:{\$lt:15}})" >/dev/null
wait $WP 2>/dev/null

HOTN=$(H "print(db.getSiblingDB(process.env.TC_DB).tshrink.countDocuments({}))")
COLDN=$(C "print(db.getSiblingDB(process.env.TC_DB).tshrink.countDocuments({_tcDeletedAt:{\$exists:false}}))")
echo "  the fast tier holds $HOTN of the archive's $COLDN"
chk "the removal actually happened during the warm" "true" \
    "$(awk -v h="$HOTN" -v c="$COLDN" 'BEGIN{print (h+0 < c+0) ? "true" : "false"}')"
# The invariant. A claim covering the whole collection is only honest when the
# fast tier holds the whole collection.
CLAIM=$(cline watermark --config $CFG --ns $DB.tshrink 2>&1)
chk "no claim on all of history while it is short" "false" \
    "$(grep -q "all of history" <<<"$CLAIM" && echo true || echo false)"
chk "and it said why"                              "true" \
    "$(grep -q "not claiming a window the fast tier no longer holds" $LOGDIR/warm_shrink.log && echo true || echo false)"
H "db.getSiblingDB(process.env.TC_DB).tshrink.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tshrink.drop()" >/dev/null

stopDaemons
echo "== documents the rule cannot address, which never resolves =="
# A rule ages a collection by a date field, and a document written before that
# field existed has none. No window selects it, so no warm will ever copy it up,
# and the claim it blocks is never going to be made.
#
# The guard above already refuses the claim, which is right and is why the
# router keeps answering from the archive and keeps answering correctly. What it
# said was wrong: "documents left the fast tier while this warm was filling it,
# which is what an evictor whose cutoff falls inside the warm window does". That
# sends somebody looking for an evictor that is not there, for a condition that
# will still be true tomorrow.
UNS=twunaged
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$UNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$UNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({})" >/dev/null
# Three shapes a window cannot select, because MongoDB brackets a range
# predicate by type: the field absent, the field holding a string date, and the
# field explicitly null. All three are equally never warmed, and counting only
# the absent one would report a third of the problem.
C "db.getSiblingDB(process.env.TC_DB).$UNS.insertMany([
     {_id:1, at:new Date(), n:'has the field'},
     {_id:2, at:new Date(), n:'has it too'},
     {_id:3,                n:'written before the field existed'},
     {_id:4, at:'2026-01-01', n:'a string, not a date'},
     {_id:5, at:null,       n:'explicitly nothing'}])" >/dev/null
chk "three of them cannot be aged by the rule"   "3" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$UNS.countDocuments({at:{\$not:{\$type:'date'}}}))")"
# Its own config: one rule, on this namespace, aged by a date field. Warming the
# shared config's namespaces here would take minutes and report somebody else's
# state in the same log.
# From $CFG, the suite's permissive config. This read $WDIR/cline.yaml, the
# strict one, so its daemons refused what the suite expects answered: on
# 2026-10-13 the evclaim router refused a read with "position has stopped
# moving quiet_for=7.38s" once the evictor ran slow under four workers.
DB=$DB WDIR=$WDIR CFG=$CFG python3 - <<'PY'
import os
db, wdir = os.environ['DB'], os.environ['WDIR']
s = open(os.environ['CFG']).read()
head = s[:s.index('warmer:')]
tail = s[s.index('router:'):]
open(wdir + '/unaged.yaml', 'w').write(head + """warmer:
  residency: 30m
  rules:
    - ns: "%s.twunaged"
      back: 40m
      date_field: "at"
  backoff:
    max_replication_lag: 60s
    max_cache_dirty: 0.9
    min_write_tickets_free: 0.01
    min_oplog_window: 1m
""" % db + tail)
PY
chk "the derived config warms only this one"     "1" \
    "$(grep -c "ns: \"$DB.$UNS\"" $WDIR/unaged.yaml)"
rm -f $LOGDIR/warm_unaged.log
cline warmer --without-feeder --config $WDIR/unaged.yaml --log-file $LOGDIR/warm_unaged.log >/dev/null 2>&1
chk "the warm copied only what it could address" "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$UNS.countDocuments({}))")"
chk "it refuses the claim"                       "true" \
    "$(grep -q 'not claiming' $LOGDIR/warm_unaged.log && echo true || echo false)"
chk "and says the rule cannot address them"      "true" \
    "$(grep -q 'this rule cannot address' $LOGDIR/warm_unaged.log && echo true || echo false)"
chk "counting all three, not just the absent one" "true" \
    "$(grep -q 'not_dated_by_at=3' $LOGDIR/warm_unaged.log && echo true || echo false)"
# The part that distinguishes it from an evictor fighting the warm: this one is
# permanent, and the message has to say so rather than describing a race.
chk "and that it is never going to be made"      "true" \
    "$(grep -q 'never going to be made' $LOGDIR/warm_unaged.log && echo true || echo false)"
# Asked as "the log exists and does not say it", because a missing log makes
# every grep answer false and the check would pass on a warm that never ran.
chk "and does not blame an evictor"              "true" \
    "$([ -s $LOGDIR/warm_unaged.log ] && ! grep -q 'documents left the fast tier' $LOGDIR/warm_unaged.log && echo true || echo false)"
# The boundary claim is withheld too, but a boundary claim is not the danger and
# one may legitimately be recorded: it says the fast tier holds everything newer
# than an instant, and the two documents that have the field are newer than it.
# The document without the field is not newer than anything, so no bounded query
# reaches it.
#
# The claim that must never be made is the one on all of history, because that
# is the one an unbounded query is answered from, and this collection can never
# earn it. A zero completeFrom is how that is recorded.
chk "no claim on all of history"                 "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).watermarks.countDocuments({_id:process.env.TC_DB+'.$UNS', completeFrom:new Date(0)}))")"
chk "nor one dated before this collection existed" "0" \
    "$(C "const w=db.getSiblingDB(process.env.TC_META).watermarks.findOne({_id:process.env.TC_DB+'.$UNS'});
          print(w && w.completeFrom && w.completeFrom.getFullYear() < 2020 ? 1 : 0)")"
# And the answers, which is the whole point: the router must still see all three.
startRouter
chk "the router still sees every document"       "5" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$UNS.countDocuments({}))")"
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$UNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$UNS.drop()" >/dev/null

echo "== a warmed document is not an application's insert =="
# The warmer's whole job is moving documents the archive already holds onto the
# fast tier, in bulk. Every one of those is an insert on the fast tier, and a
# client watching the collection sees it.
#
# The router recognises this system's own copy-ups by the residency record the
# copy-up writes. The warmer wrote a hold and nothing else -- no promotedAt, no
# echo -- so there was nothing to recognise, and nothing in the router ever read
# the warming announcements either. Measured before the fix: five documents
# warmed, five inserts delivered, identical to what the fast tier's own
# unfiltered stream delivered. An application would have been told five records
# the archive may have held for years had just been created.
#
# Read from a position before the warm, because that is the consumer this is
# about: one that stores its resume token and comes back to it. The control is
# the same question put to the fast tier directly, where nothing is hidden, so
# an empty answer cannot pass for a correct one.
WSNS=twstream
sed '/^warmer:/,/^router:/ s|^  rules:|  rules:\n    - ns: "'"$DB.$WSNS"'"\n      back: 40m|' $CFG > $WDIR/warm_stream.yaml
H "db.getSiblingDB(process.env.TC_DB).$WSNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$WSNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_META).residency.deleteMany({});
   db.getSiblingDB(process.env.TC_META).warming.deleteMany({})" >/dev/null
startFeeder; startRouter
# Created here rather than by the warm, so no drop or create lands in the window:
# a drop delivers an invalidate, which closes the cursor before the events being
# asked about.
H "db.getSiblingDB(process.env.TC_DB).createCollection('$WSNS')" >/dev/null 2>&1
# Archive only, with recent ObjectIds so a rule reaching back 40m addresses them.
C "db.getSiblingDB(process.env.TC_DB).$WSNS.insertMany([1,2,3,4,5].map(i=>({_id:new ObjectId(), n:i, v:'archived'})))" >/dev/null
sleep 3
WSTS=$(( $(date +%s) - 1 ))
sleep 1
cline warmer --config $WDIR/warm_stream.yaml --log-file $LOGDIR/warm_stream.log >/dev/null 2>&1
sleep 8
chk "the warmer moved them onto the fast tier"    "5"     "$(H "print(db.getSiblingDB(process.env.TC_DB).$WSNS.countDocuments({}))")"
chk "and each one is recorded as a promotion"     "5"     "$(C "print(db.getSiblingDB(process.env.TC_META).residency.countDocuments({ns:process.env.TC_DB+'.$WSNS', promotedAt:{\$exists:true}}))")"
WSDRAIN="const r = db.getSiblingDB(process.env.TC_DB).runCommand({
     aggregate: '$WSNS',
     pipeline: [{\$changeStream: {startAtOperationTime: Timestamp({t: $WSTS, i: 1})}}],
     cursor: {}});
   print(r.ok ? r.cursor.firstBatch.filter(e => e.operationType === 'insert').length : 'aggregate failed');"
WSGOT=$(R "$WSDRAIN")
WSCTRL=$(H "$WSDRAIN")
echo "  the client saw $WSGOT inserts, the fast tier's own stream $WSCTRL"
chk "the warm is hidden from the application"     "0" "$WSGOT"
chk "and the fast tier's own stream did carry it" "5" "$WSCTRL"

# And a second pass over documents the fast tier already holds, with their
# residency rows gone.
#
# Residency is recorded before the documents are moved, and a row that is new or
# has ended announces an echo. The documents produce no insert, because the fast
# tier already has them, and an announcement left standing is spent on whatever
# write reaches that document next -- dropping that mirror, so the archive keeps
# the older version with nothing saying so.
#
# The rows are removed here rather than waited out, which is what makes this the
# ordinary case rather than a contrived one: rows expire an hour after their
# hold, a warmer rule reaching back 40m keeps selecting the same documents, and
# a long-running warmer therefore meets its own warmed documents with no row
# beside them all the time. Warming twice in quick succession proves nothing,
# because the row is still there and still says the promotion is live, so no
# echo is announced and the check passes without exercising anything.
# The claim goes with them, or the warmer looks at its own watermark, sees the
# window is filled and selects nothing at all -- which is a second way for this
# check to pass while exercising nothing.
C "db.getSiblingDB(process.env.TC_META).residency.deleteMany({ns:process.env.TC_DB+'.$WSNS'});
   db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({_id:process.env.TC_DB+'.$WSNS'})" >/dev/null
cline warmer --config $WDIR/warm_stream.yaml --log-file $LOGDIR/warm_stream2.log >/dev/null 2>&1
sleep 6
chk "the second pass moved nothing"                   "5" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$WSNS.countDocuments({}))")"
chk "a warm that moved nothing leaves no echo behind" "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).residency.countDocuments({ns:process.env.TC_DB+'.$WSNS', echoPending:true}))")"
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$WSNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$WSNS.drop()" >/dev/null 2>&1

echo
echo "================================"
echo "== a claim read in a field it was not made in =="
# A claim says the fast tier holds every live document newer than an instant,
# and newer is measured in the field the rules tier by. The row used to record
# the instant alone, so changing a rule's date_field re-read every existing
# claim in a dimension it was never made in.
#
# Measured on twenty orders whose shippedAt is thirty days after their
# createdAt, claimed complete from 15 January in createdAt: with date_field
# changed to shippedAt the router answered 6 where the archive holds 20, and
# reported success. One line in a config file.
DNS=tevdim
JAN15='new Date(Date.UTC(2025,0,15))'
pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null; sleep 1
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "const c=db.getSiblingDB(process.env.TC_DB).$DNS; const day=24*60*60*1000;
   for (let i=1;i<=20;i++) c.insertOne({_id:i,createdAt:new Date(Date.UTC(2025,0,i)),shippedAt:new Date(Date.UTC(2025,0,i)+30*day)});" >/dev/null
H "const c=db.getSiblingDB(process.env.TC_DB).$DNS; const day=24*60*60*1000;
   for (let i=15;i<=20;i++) c.insertOne({_id:i,createdAt:new Date(Date.UTC(2025,0,i)),shippedAt:new Date(Date.UTC(2025,0,i)+30*day)});" >/dev/null
# The claim the warmer would have written, stamped with the field it was made in.
C "db.getSiblingDB(process.env.TC_META).watermarks.replaceOne({_id:'$DB.$DNS'},
    {_id:'$DB.$DNS',completeFrom:$JAN15,dimension:'createdAt'},{upsert:true})" >/dev/null
# Appended to the evictor's existing rules rather than starting a second
# "rules:" key, which yaml refuses and which is how this section first failed.
mkdim() {
  awk -v f="$1" -v ns="$DB.$DNS" '
    /^evictor:/ { inev = 1 }
    /^[a-z]/ && !/^evictor:/ { inev = 0 }
    { print }
    inev && /^  rules:/ && !done { print "    - ns: \"" ns "\""; print "      older_than: 40m"; print "      date_field: \"" f "\""; done = 1 }
  ' $CFG > $WDIR/dim_$1.yaml
}
mkdim createdAt; mkdim shippedAt
askdim() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({$1:{\$gte:$JAN15}}))" 2>&1 | tail -1; }

rm -f $LOGDIR/dim_r.log
cline router --config $WDIR/dim_createdAt.yaml --log-file $LOGDIR/dim_r.log & DRP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "in its own field the claim is used"               "6" "$(askdim createdAt)"
kill $DRP 2>/dev/null; wait $DRP 2>/dev/null

rm -f $LOGDIR/dim2_r.log
cline router --config $WDIR/dim_shippedAt.yaml --log-file $LOGDIR/dim2_r.log & DRP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
# The archive's own answer, which is what the client must get.
chk "in another field the archive answers"             "20" "$(askdim shippedAt)"
chk "and the router said the claim was withdrawn"      "true" \
    "$(grep -q 'measured in a field this namespace no longer ages by' $LOGDIR/dim2_r.log && echo true || echo false)"
chk "naming both fields"                               "true" \
    "$(grep 'no longer ages by' $LOGDIR/dim2_r.log | grep -q 'createdAt' && grep 'no longer ages by' $LOGDIR/dim2_r.log | grep -q 'shippedAt' && echo true || echo false)"
kill $DRP 2>/dev/null; wait $DRP 2>/dev/null

# A row written before the field was recorded has to work exactly as it did, or
# every deployment loses its claims on upgrade.
C "db.getSiblingDB(process.env.TC_META).watermarks.replaceOne({_id:'$DB.$DNS'},
    {_id:'$DB.$DNS',completeFrom:$JAN15},{upsert:true})" >/dev/null
rm -f $LOGDIR/dim3_r.log
cline router --config $WDIR/dim_createdAt.yaml --log-file $LOGDIR/dim3_r.log & DRP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "a claim with no field recorded is still used"     "6" "$(askdim createdAt)"
chk "and nothing is withdrawn for it"                  "0" \
    "$(grep -c 'no longer ages by' $LOGDIR/dim3_r.log)"
kill $DRP 2>/dev/null; wait $DRP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteOne({_id:'$DB.$DNS'})" >/dev/null


echo "== the order a dated warm reads the archive in =="
# A rule that ages by a date field sorts the archive {field: -1, _id: -1}, and
# nothing copies an index there. Without one mongod answers every batch by
# reading the whole collection and sorting it: measured on 200,003 archived
# documents with batch_size 200, 200,000 examined per batch, against 100 with
# {createdAt: -1, _id: -1}. Per batch, so a pass costs the square of the
# collection rather than the size of the window.
#
# The middle case is why the warning names the index rather than the field. An
# index on the field alone is the obvious fix and still sorts, at 19,039 per
# batch, so the check below has to see it warn with that one present.
DNS2=tqbound
C "db.getSiblingDB(process.env.TC_DB).$DNS2.dropIndex('createdAt_-1__id_-1')" >/dev/null 2>&1
WARMOUT=$(cline warmer --config $CFG --dry-run 2>&1)
chk "it warns when the archive cannot sort it"      "true" \
    "$(grep -q 'no index for the order this warm reads it in' <<<"$WARMOUT" && echo true || echo false)"
chk "and names the index to build"                  "true" \
    "$(grep -q "build {createdAt: -1, _id: -1}" <<<"$WARMOUT" && echo true || echo false)"
C "db.getSiblingDB(process.env.TC_DB).$DNS2.createIndex({createdAt:1})" >/dev/null 2>&1
WARMOUT=$(cline warmer --config $CFG --dry-run 2>&1)
chk "the field on its own is not enough"            "true" \
    "$(grep -q 'no index for the order this warm reads it in' <<<"$WARMOUT" && echo true || echo false)"
C "db.getSiblingDB(process.env.TC_DB).$DNS2.createIndex({createdAt:-1,_id:-1})" >/dev/null 2>&1
WARMOUT=$(cline warmer --config $CFG --dry-run 2>&1)
chk "and quiet once the archive can walk it"        "false" \
    "$(grep -q "ns=$DB.$DNS2" <<<"$(grep 'no index for the order this warm reads it in' <<<"$WARMOUT")" && echo true || echo false)"
C "db.getSiblingDB(process.env.TC_DB).$DNS2.dropIndex('createdAt_1')" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$DNS2.dropIndex('createdAt_-1__id_-1')" >/dev/null 2>&1

echo "== a warm does not undo a delete mirroring has not reached yet =="
# The warmer copies up what the archive holds and the fast tier does not, and a
# document the application has just deleted is exactly that until its tombstone
# lands. It used to skip the mirroring check on the grounds that it only adds
# to the fast tier, and a warm then copied back every document deleted while
# the feeder was behind: 25 of 25 on 2026-09-25, all of them answered by the
# router afterwards. The feeder is held back here so the window stays open.
# The sections above that warm with no feeder at all pass --without-feeder,
# because they empty the fast tier straight on purpose and a running feeder
# would tombstone what they delete.
stopDaemons
startFeeder
startRouter
R "const c=db.getSiblingDB(process.env.TC_DB).$NS; for (let i=0;i<20;i++) c.insertOne({_id:new ObjectId(), wd:i});" >/dev/null
for _ in $(seq 1 60); do [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({wd:{\$exists:true}}))")" = 20 ] && break; sleep 0.5; done
chk "the archive holds the twenty before the delete"  "20" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({wd:{\$exists:true}, _tcDeletedAt:{\$exists:false}}))")"
pkill -f "cline feeder --config $WDIR/" 2>/dev/null; sleep 1
chk "ten deleted through the router"                  "10" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({wd:{\$lt:10}}).deletedCount)")"
timeout 45 cline warmer --config $CFG --log-file $LOGDIR/warm_behind.log >/dev/null 2>&1
# On the fast tier, which is the only place a warm writes. Through the router
# the count reaches the archive, which has no tombstones while the feeder is
# held back, and answers ten whatever the warm did.
chk "none of them came back"                          "0"  "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({wd:{\$lt:10}}))")"
chk "and the warmer said why it waited"               "true" "$(grep -q 'not warming while mirroring is behind' $LOGDIR/warm_behind.log && echo true || echo false)"
startFeeder
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({wd:{\$exists:true}})" >/dev/null
stopDaemons

echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
