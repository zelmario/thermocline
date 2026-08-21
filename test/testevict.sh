#!/bin/bash
# The evictor's documented precondition is a running feeder. Test it both ways.
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
# The tier endpoints, defaulted the same way every other suite defaults them.
# Without a default these were ${HOT:?}, which reads as strict and is not: a
# sweep does not export them, so the two checks that use them exited before
# querying anything and reported got= want=1. A check that cannot run looks
# exactly like a check that failed, and this pair guards the evictor destroying
# a live record.
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
ok() {
  local name="$1" want="$2" pat="$3"; shift 3; [ "$1" = "--" ] && shift
  local out rc; out=$("$@" 2>&1); rc=$?
  local good=1
  [ -n "$want" ] && [ "$rc" != "$want" ] && good=0
  [ -n "$pat" ] && ! grep -qE "$pat" <<<"$out" && good=0
  if [ $good = 1 ]; then PASS=$((PASS+1)); printf '  PASS  %s\n' "$name"
  else FAIL=$((FAIL+1)); FAILED+=("$name"); printf '  FAIL  %s (exit=%s want=%s)\n    %s\n' "$name" "$rc" "$want" "$(tail -2 <<<"$out"|tr '\n' ' ')"; fi
}

echo "== fail-closed once mirroring goes stale (the safety guard) =="
# Killing the feeder does not make lag stale instantly: the archive is current
# until enough time passes. Wait past max_feeder_lag so the guard has something
# to trip on, which is the condition it exists for.
pkill -f "cline feeder --config $WDIR/"; sleep 35
ok "refuses when mirroring lag is unknown/stale" 1 "not healthy enough to evict" \
   -- cline evictor --config $CFG --once --health-timeout 5s

echo "== with the feeder running, as documented =="
cline feeder --config $CFG --log-file $LOGDIR/t_ev_feeder.log &
FP=$!
# The evictor's precondition is mirroring lag inside max_feeder_lag. After the
# feeder has been down it has to catch up first, so wait for that rather than
# racing it.
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
for i in $(seq 1 60); do
  LAG=$(mongosh "$COLD" --quiet --eval 'const c=db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}); print(c?Math.round((Date.now()-c.eventAt.getTime())/1000):9999)' 2>/dev/null | tail -1)
  [ "$LAG" -lt 20 ] 2>/dev/null && { echo "  (feeder caught up, lag ${LAG}s)"; break; }
  sleep 2
done
ok "--once --dry-run"          0 "sweep complete" -- cline evictor --config $CFG --once --dry-run
ok "--once"                    0 "sweep complete" -- cline evictor --config $CFG --once
ok "--once -v"                 0 "sweep complete" -- cline evictor --config $CFG --once -v
ok "--once --health-timeout"   0 "sweep complete" -- cline evictor --config $CFG --once --health-timeout 30s

rm -f $LOGDIR/t_ev.log
cline evictor --config $CFG --every 2s --log-file $LOGDIR/t_ev.log &
EP=$!; sleep 9
N=$(grep -c "sweep complete" $LOGDIR/t_ev.log 2>/dev/null); N=${N:-0}
if [ "$N" -ge 2 ]; then PASS=$((PASS+1)); echo "  PASS  --every loops ($N sweeps)"
else FAIL=$((FAIL+1)); FAILED+=("--every"); echo "  FAIL  --every loops (saw $N)"; fi
kill $EP 2>/dev/null; wait $EP 2>/dev/null

ok "dry run never blocks on health" 0 "dry run" -- cline evictor --config $CFG --once --dry-run

echo "== a tombstone is not a copy of the document =="
# The evictor removes a document once the archive is confirmed to hold it, and
# a tombstoned copy is not that. It is a record that the application deleted the
# document; reads filter it out, so counting it would confirm the archive holds
# something no read will ever return.
#
# The state is ordinary: an application deletes a record and writes the same _id
# again, which is what a natural key or a reprocessed record looks like. Until
# the feeder mirrors the second write the fast tier holds the only live copy and
# the archive holds a tombstone. Measured before the fix: one sweep and the
# document was on neither tier, and the router answered GONE for a document the
# application had just written.
#
# Set up directly rather than raced, because the evictor's decision depends on
# the state it finds and not on how the state arose.
chkv() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT"  --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
# The feeder is stopped for this, so it cannot mirror the fast tier write and
# clear the tombstone underneath the sweep. Briefly, though: the evictor fails
# closed once mirroring looks stale, and a sweep that refuses to run would pass
# this check while proving nothing. A few seconds is far inside max_feeder_lag.
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
TOID=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 4242)
H "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$TOID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$TOID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).residency.deleteMany({})" >/dev/null
# The live copy is put on the fast tier while the feeder is stopped further up,
# so the archive keeps its tombstone rather than being caught up over it.
H "db.getSiblingDB(process.env.TC_DB).orders.insertOne({_id:ObjectId('$TOID'), n:'written again'})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.insertOne({_id:ObjectId('$TOID'), n:'the older one', _tcDeletedAt:new Date()})" >/dev/null
chkv "the fast tier holds the only live copy" "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$TOID')}))")"
chkv "and the archive holds none"             "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$TOID'),_tcDeletedAt:{\$exists:false}}))")"
# Make the health gate deterministic rather than racing it. The feeder is
# stopped for this setup, so mirroring lag grows while the fixture is written,
# and inside a four-worker sweep that took longer than max_feeder_lag: the sweep
# refused on health and the check below would have been meaningless. The control
# caught it, which is what it is for. Lag is what the first section of this
# suite tests; this one is about what counts as the archive holding a document,
# so the position is stamped fresh immediately before the sweep.
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null
EVOUT=$(cline evictor --config $CFG --once 2>&1)
# The control. A sweep that refused to run for health reasons would leave the
# document alone too, and prove nothing at all about tombstones.
chkv "the sweep actually ran"                  "true" \
    "$(grep -q "not healthy enough to evict" <<<"$EVOUT" && echo false || echo true)"
chkv "the sweep left the only live copy alone" "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$TOID')}))")"
H "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$TOID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$TOID')})" >/dev/null

echo "== a copy-up must not turn an eviction into a deletion =="
# The evictor announces a document before removing it, and the feeder reads that
# announcement to tell an eviction from an application delete. A copy-up used to
# delete the announcement, on the grounds that a document back in the fast tier
# is not leaving it. That is true of the next eviction and false of the one
# already in flight: with the feeder yet to apply the delete event, the notice
# it needs is gone by the time it looks, so it reads a real eviction as an
# application delete and tombstones a record nothing deleted.
#
# The warm's own insert does not put that right, because the feeder suppresses
# it as an echo of the copy-up. The archive is left holding a tombstone over a
# live document the fast tier has already dropped, and the router hides it from
# every answer.
#
# Found on the pilot with a warm and an evictor fighting over the same
# collections while a client read them: 66 batches of "deletes not announced by
# the evictor", and an archive left holding 200 tombstoned trips and 45
# tombstoned sales that nothing had ever deleted.
#
# The ordering is forced rather than raced. The feeder polls on a fifteen second
# await, so the eviction and the copy-up both land inside one of its batches:
# each of those is a whole process start, and at five seconds they fell into
# different polls and the check passed against a binary that had the bug;
# it has already seen a warmer, so the copy-up's insert is suppressed; and the
# warm window is widened past the eviction cutoff so both address the same
# document, which is what "the hot window is too small" means.
stopFeeder() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
}
stopFeeder
sed -e 's|^  max_await: .*|  max_await: 15s|' $CFG > $WDIR/ev_slow.yaml
sed '/^warmer:/,/^router:/ s|^      back: 40m|      back: 24h|' $CFG > $WDIR/ev_wide.yaml
chkv "the derived config warms past the eviction cutoff" "true" \
    "$(grep -q 'back: 24h' $WDIR/ev_wide.yaml && echo true || echo false)"
EVID=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 5150)
NOTICE="$DB.orders|07$EVID"
H "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$EVID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$EVID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).residency.deleteMany({});
   db.getSiblingDB(process.env.TC_META).evicting.deleteMany({});
   db.getSiblingDB(process.env.TC_META).warming.deleteMany({})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).orders.insertOne({_id:ObjectId('$EVID'), n:'live'})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.insertOne({_id:ObjectId('$EVID'), n:'live'})" >/dev/null
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null
# The feeder has to be suppressing warm echoes for this to reproduce, because
# what stops the copy-up's insert repairing the tombstone is that suppression.
#
# The marker is held directly rather than by running warms in a loop. A real
# warm puts a residency hold on everything it copies and the evictor honours it,
# so the loop that made the feeder suppress also stopped the eviction the
# section is about. This is the same record a warmer writes, and what is under
# test here is what the feeder does with it.
( for _ in $(seq 1 60); do
    C "db.getSiblingDB(process.env.TC_META).markers.updateOne({_id:'warmerActive'},{\$set:{seen:new Date()}},{upsert:true})" >/dev/null
    sleep 2
  done ) &
WLOOP=$!
sleep 1
cline feeder --config $WDIR/ev_slow.yaml --log-file $LOGDIR/ev_notice.log & FP2=$!
# Something to flush, so the feeder consults the marker rather than sitting idle
# with an empty batch. On a loaded machine with nothing being written it never
# looks, and then the section passes while measuring nothing.
for _ in $(seq 1 40); do
  H "db.getSiblingDB(process.env.TC_DB).orders.updateOne({_id:ObjectId('$EVID')},{\$set:{poke:Math.random()}})" >/dev/null
  grep -q 'warmer detected' $LOGDIR/ev_notice.log && break
  sleep 1
done
chkv "the feeder is suppressing warm echoes"     "true" \
    "$(grep -q 'warmer detected' $LOGDIR/ev_notice.log && echo true || echo false)"
# The warm above would have put a residency hold on this document, and the
# evictor honours one, so the sweep would hold it instead of evicting it and
# every check below would pass while measuring nothing. That happened.
C "db.getSiblingDB(process.env.TC_META).residency.deleteMany({})" >/dev/null
EVOUT2=$(cline evictor --config $CFG --once 2>&1)
chkv "the sweep actually ran"                    "true" \
    "$(grep -q "not healthy enough to evict" <<<"$EVOUT2" && echo false || echo true)"
chkv "the evictor removed it from the fast tier" "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$EVID')}))")"
# The copy-up, inside the same poll, before the feeder has applied the delete.
# Whether the announcement is still there at any given instant is a race with
# the feeder's own poll and not worth asserting; what the announcement is for is
# asserted below, where it cannot be timed wrong.
cline warmer --config $WDIR/ev_wide.yaml >/dev/null 2>&1
chkv "the warm put it back"                      "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$EVID')}))")"
sleep 14
chkv "the archive did not tombstone what nobody deleted" "false" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).orders.findOne({_id:ObjectId('$EVID')}); print(!!(d && d._tcDeletedAt))")"
chkv "and the announcement was spent, not left"  "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_META).evicting.countDocuments({_id:'$NOTICE'}))")"
kill $FP2 2>/dev/null; wait $FP2 2>/dev/null
kill $WLOOP 2>/dev/null; wait $WLOOP 2>/dev/null
pkill -f "cline warmer --config $WDIR/" 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$EVID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$EVID')})" >/dev/null

echo "== a sweep narrows a claim and does not invent one =="
# The state every adoption starts in: the archive holds the collection, the fast
# tier has never held any of it, no warm has run, so nothing has claimed
# anything. One sweep used to leave a claim behind saying the fast tier holds
# everything since the eviction boundary, about a tier holding nothing, and a
# sweep that scanned zero documents and evicted zero was enough to do it.
#
# Measured through the router on 4,000 archived orders and an empty fast tier:
# 240 in the last thirty days before the sweep, 0 after it, reported as success.
# The router already running kept the claim it had cached, so this only showed
# itself on the next restart, which is what made it worth a check here.
#
# The claim belongs to whatever put the documents there: the warmer, which knows
# how far back it filled, or an operator who has checked.
WMWAS=$(cline watermark --config $CFG --ns $DB.orders 2>&1 | tail -1)
cline watermark --config $CFG --ns $DB.orders --clear >/dev/null 2>&1
chkv "the claim starts withdrawn"              "true" \
    "$(cline watermark --config $CFG --ns $DB.orders 2>&1 | grep -qc 'claims nothing' && echo true || echo false)"
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null
WMOUT=$(cline evictor --config $CFG --once 2>&1)
# The control again. A sweep that refused on health would leave the claim
# withdrawn too, and prove nothing.
chkv "the sweep actually ran"                  "true" \
    "$(grep -q "not healthy enough to evict" <<<"$WMOUT" && echo false || echo true)"
chkv "and it claimed nothing on its own"       "true" \
    "$(cline watermark --config $CFG --ns $DB.orders 2>&1 | grep -qc 'claims nothing' && echo true || echo false)"
# Put back exactly what was there, so nothing after this suite inherits a
# withdrawn claim.
case "$WMWAS" in
  *"newer than "*) cline watermark --config $CFG --ns $DB.orders \
      --complete-from "${WMWAS##*newer than }" >/dev/null 2>&1 ;;
  *"all of history"*) cline watermark --config $CFG --ns $DB.orders --complete-all >/dev/null 2>&1 ;;
esac

kill $FP 2>/dev/null; wait $FP 2>/dev/null
echo
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
