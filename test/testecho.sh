#!/bin/bash
# The router's promotions must not be mirrored back into the archive they came
# from, and suppressing them must not lose a genuine write.
#
# A copy-up inserts an archived document into the fast tier. That is a change
# event like any other, so the feeder mirrored it straight back down, writing the
# archive's own document into the archive. Identical content, so nothing was ever
# wrong with the answer; what it cost was a cold tier write for every document
# the fast tier had to fetch.
#
# Measuring it is the interesting part, because a suppressed echo and a completed
# echo leave exactly the same document behind. So the archive copy is marked
# directly, after the promotion and before the feeder runs, with a field the fast
# tier's copy does not have. A mirror is a whole-document replace, so it wipes
# that field; suppression leaves it. The first section proves the marker can see
# a mirror at all, because a measurement whose control does not come back
# positive proves nothing.
#
# The last section is the one that matters most. Suppressing every mirror for a
# recently promoted document would also swallow a genuine insert, if the
# application deleted that document and created it again in the window, and the
# archive would keep the tombstone while the new document never arrived. So the
# suppression is one-shot, and this checks that the shot is spent on the
# promotion and not on the write that follows it.
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
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
NS=techo
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() { pkill -9 -f "cline feeder --config $WDIR/" 2>/dev/null; pkill -9 -f "cline router --config $WDIR/" 2>/dev/null; }
trap cleanup EXIT

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/echo_f.log &
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/echo_r.log &
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}
# The archive copy's marker, or ABSENT. This is the whole measurement.
marker() { C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:$1}); print(d&&d.marker?d.marker:'ABSENT')"; }
archived() { C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:$1}); print(!!d && !d._tcDeletedAt)"; }

echo "== setup =="
stopDaemons
C "db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'});
   db.getSiblingDB(process.env.TC_META).residency.deleteMany({});
   db.getSiblingDB(process.env.TC_META).markers.deleteMany({})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
startFeeder
startRouter

echo "== control: a mirror does wipe the marker =="
# Without this, everything below could pass because nothing was ever written.
H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:1,v:'a'})" >/dev/null
for _ in $(seq 1 40); do [ "$(archived 1)" = "true" ] && break; sleep 0.5; done
chk "the document reached the archive"               "true" "$(archived 1)"
stopDaemons
C "db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:1},{\$set:{marker:'control'}})" >/dev/null
chk "the archive copy carries a marker"              "control" "$(marker 1)"
H "db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:1},{\$set:{v:'b'}})" >/dev/null
startFeeder
for _ in $(seq 1 40); do [ "$(marker 1)" = "ABSENT" ] && break; sleep 0.5; done
chk "a mirrored write replaces the archive copy"     "ABSENT" "$(marker 1)"

echo "== a promotion is not mirrored back =="
stopDaemons
# Archive only, which is what an evicted document looks like.
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:2})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:2})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:2,v:'archived'})" >/dev/null
startRouter
# A point read by _id is the only thing that promotes.
chk "the archived document is served"                "archived" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:2}); print(d?d.v:'MISSING')")"
for _ in $(seq 1 40); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:2}))")" = "1" ] && break
  sleep 0.5
done
chk "and is now on the fast tier"                    "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:2}))")"
# Mark the archive copy before the feeder ever sees the promotion.
C "db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:2},{\$set:{marker:'kept'}})" >/dev/null
startFeeder
sleep 8
chk "the promotion did not overwrite the archive"    "kept" "$(marker 2)"
chk "and the archived document is still live"        "true" "$(archived 2)"

echo "== the suppression is spent on the promotion, not on the next write =="
# The hazard: delete the promoted document and create a different one with the
# same id before the feeder has caught up. The insert of the new document must
# reach the archive, or it is lost there while the application believes it is
# stored.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:3})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:3})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:3,v:'old'})" >/dev/null
startRouter
chk "the second archived document is served"         "old" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:3}); print(d?d.v:'MISSING')")"
for _ in $(seq 1 40); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:3}))")" = "1" ] && break
  sleep 0.5
done
# Now, with the feeder still stopped, replace it on the fast tier.
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:3})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:3,v:'new'})" >/dev/null
startFeeder
sleep 10
chk "the replacement reached the archive"            "new" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:3}); print(d?d.v:'MISSING')")"
chk "and it is live there, not tombstoned"           "true" "$(archived 3)"

echo "== a copy-up that inserts nothing must not eat the next write =="
# The regression this section exists for. A multi-document write copies up
# everything its filter matches in the archive, and announces an echo for each,
# but only the documents the fast tier was actually missing produce an insert.
# The notices left standing for the rest were then spent on the mirrors of the
# write itself, so the archive kept the old version of those documents while the
# application had been told all of them were updated.
#
# Measured in testconform as bulkWrite updateMany reporting 10 matched and 5
# updated in the archive.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.insertMany([{_id:10,grp:'b',v:0},{_id:11,grp:'b',v:0}])" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:10,grp:'b',v:0})" >/dev/null
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null 2>&1
startFeeder
startRouter
# _id 10 is on both tiers and _id 11 only in the archive, so the copy-up inserts
# one document and skips the other.
chk "the multi-document write matches both"          "2" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.updateMany({grp:'b'},{\$set:{v:1}}).matchedCount)")"
for _ in $(seq 1 40); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({v:1}))")" = "2" ] && break
  sleep 0.5
done
chk "and both updates reached the archive"           "2" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({v:1}))")"
# A notice can legitimately be left standing: the feeder caches its view of
# whether anything is being promoted, so an echo arriving inside a stale "no" is
# mirrored rather than suppressed, which costs one redundant write and leaves the
# notice unconsumed. What matters is that a leftover cannot reach a later write,
# which the two checks above measure directly, and that it expires.
chk "any notice left behind is one the router wrote"  "true" \
    "$(C "const n=db.getSiblingDB(process.env.TC_META).residency.countDocuments({echoPending:true, promotedAt:{\$exists:false}}); print(n===0)")"

echo "== a warm must not swallow the application's own write =="
# The warmer announces every document it is about to copy up, so the feeder does
# not mirror that copy straight back into the archive it came from. The notice
# matched any mirror for the document rather than only the copy-up's insert, and
# it was never spent, so while a warm was running the feeder dropped the
# application's own writes to warmed documents.
#
# Everything the promotion path above had to learn, and this had learned none of
# it. Found by adopting MongoDB's sample datasets rather than by a fixture: warm
# a collection, then update a year of it through the router. 868 documents
# modified, 868 marked on the fast tier, 548 in the archive, and it stayed at
# 548. The router answered 548 as well, because a range straddling the watermark
# is read from the archive, so the write was acknowledged, held on one tier,
# invisible through the proxy and missing from the durable one. An eviction
# would have finished the job.
#
# The rule lives in a derived config rather than the shared one, which is what
# the suites needing a different config already do, so nothing else inherits a
# warm rule for this namespace.
# date_field, because these _ids are integers and a rule without one takes a
# document's age from the timestamp inside an ObjectId. Both components refuse
# such a rule rather than obeying it, so the warm would copy nothing and every
# check below would pass while measuring an idle warmer.
sed '/^warmer:/,/^router:/ s|^  rules:|  rules:\n    - ns: "'"$DB.$NS"'"\n      date_field: "at"\n      back: 40m|' $CFG > $WDIR/echo_warm.yaml
chk "the derived config warms this namespace"        "1" \
    "$(grep -c "ns: \"$DB.$NS\"" $WDIR/echo_warm.yaml)"
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).warming.deleteMany({})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.insertMany([{_id:30,v:'old',at:new Date()},{_id:31,v:'old',at:new Date()}])" >/dev/null
startFeeder
startRouter
cline warmer --config $WDIR/echo_warm.yaml >/dev/null 2>&1
chk "the warm copied both up"                        "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:{\$in:[30,31]}}))")"
# Immediately, while the warmer's marker is still fresh. That window is the only
# time the suppression is active, so a check run after it has gone stale would
# pass whatever the feeder does.
chk "the update is applied"                          "1" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:30},{\$set:{v:'new'}}).modifiedCount)")"
for _ in $(seq 1 40); do
  [ "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:30}); print(d?d.v:'MISSING')")" = "new" ] && break
  sleep 0.5
done
chk "and it reached the archive"                     "new" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:30}); print(d?d.v:'MISSING')")"
chk "the one nobody wrote to is untouched"           "old" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:31}); print(d?d.v:'MISSING')")"
# And the notice is spent rather than left waiting for a later write to the same
# document, which is what made the loss above possible.
chk "the warm's notices were spent"                  "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).warming.countDocuments({_id:{\$regex:'^'+process.env.TC_DB+'\\.$NS'}}))")"

echo "== a warm is not mirrored back =="
# The counterpart of "a promotion is not mirrored back" above, which the warm
# path did not have. What it had was the section before this one: that a warm
# must not *swallow* the application's own write, which is over-suppression.
# Nothing checked under-suppression, so a feeder that suppressed no warm echo at
# all passed every check in this suite but one, and that one noticed only
# indirectly, by the notices going unspent.
#
# Found by breaking warmingActive to return false and running the suites against
# it: go test passed, testwarm passed all 57, testchange passed all 49, and
# testecho failed on "the warm's notices were spent". A guard worth having is
# worth a check that names what it prevents.
#
# The mechanism, which is the same trick the promotion section uses: mark the
# archive's own copy after the warm has copied it up and before the feeder ever
# sees that copy-up. Suppressed, the feeder drops the echo and the mark stays.
# Mirrored back, the fast tier's copy overwrites the archive's and takes the
# mark with it.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_META).warming.deleteMany({})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.insertMany([{_id:40,v:'archived',at:new Date()},{_id:41,v:'archived',at:new Date()}])" >/dev/null
# The feeder's log goes with the daemons, while nothing holds it open. The
# section before this one already ran a warmer and started a feeder, so the line
# the control below greps for is in this file already, and without removing it
# the control would pass on that one and say nothing about this section. That is
# the same fault found in testfresh on 2026-09-18, in a suite written long
# before. Removing it with a feeder running would be worse: the feeder keeps
# writing to the unlinked file and the grep then finds nothing whatever happens.
rm -f $LOGDIR/echo_f.log
# No feeder and no router while the warm runs, so the only writer the fast tier
# sees is the warmer itself.
# --without-feeder: this section keeps the feeder away on purpose, and the
# warmer otherwise waits for mirroring before it copies anything up.
cline warmer --without-feeder --config $WDIR/echo_warm.yaml >/dev/null 2>&1
chk "the warm copied both up"                        "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:{\$in:[40,41]}}))")"
# The mark goes on the archive's copy, which the fast tier's copy does not have.
C "db.getSiblingDB(process.env.TC_DB).$NS.updateMany({_id:{\$in:[40,41]}},{\$set:{marker:'kept'}})" >/dev/null
# Two, or the zero asked for below would be notices never written rather than
# notices spent.
chk "the warm left the feeder a notice for each"      "2" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).warming.countDocuments({_id:{\$regex:'^'+process.env.TC_DB+'\\.$NS'}}))")"
startFeeder
sleep 8
chk "the warm did not overwrite the archive"         "kept" "$(marker 40)"
chk "nor the second document"                        "kept" "$(marker 41)"
chk "and both are still live in the archive"         "true" \
    "$(C "const a=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:40}); const b=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:41}); print(!!a && !a._tcDeletedAt && !!b && !b._tcDeletedAt)")"
# The control for the control. If the feeder never looked for the warm's echoes
# then it suppressed nothing and the marks above survived for the wrong reason.
# It used to grep for the line the feeder printed when it noticed a warmer's
# heartbeat, and that gate is gone: the feeder asks on every flush that holds an
# insert, because a cached "no" let copy-ups through (test/echorace). What
# suppression leaves behind is spent notices, and nothing else spends them.
chk "and the feeder suppressed rather than missed it" "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).warming.countDocuments({_id:{\$regex:'^'+process.env.TC_DB+'\\.$NS'}}))")"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
