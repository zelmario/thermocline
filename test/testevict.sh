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

kill $FP 2>/dev/null; wait $FP 2>/dev/null
echo
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
