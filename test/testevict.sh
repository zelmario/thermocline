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
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
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

echo "== a claim narrowed and acted on in the same breath =="
# The evictor raises the completeness watermark before removing, so an
# interrupted sweep never leaves a claim wider than the truth. That protects a
# reader who reads the claim fresh. The router does not: it caches one, because
# reading it per query would put an archive round trip on the hot path.
#
# So for a few seconds after the raise there are two copies of the claim and the
# router's is the older, wider one. Removing in that window means it answers
# from a fast tier that no longer holds what it believes it holds.
#
# Measured before the wait existed, with a fast tier claiming all of history and
# a sweep removing 905 of 2,000 documents: the router answered countDocuments
# with 1,095 where the archive holds 2,000, and answered 2,000 again seven
# seconds later. No error either time, which is this system's worst failure.
CNS=tevclaim
stopFeeder 2>/dev/null || true
H "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
OLDD=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=400)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
for U in "$HOT" "$COLD"; do
  mongosh "$U" --quiet --eval "db.getSiblingDB('$DB').$CNS.insertMany([
    {_id:1, at:new Date('$OLDD')}, {_id:2, at:new Date('$OLDD')},
    {_id:3, at:new Date()},        {_id:4, at:new Date()}])" >/dev/null
done
DB=$DB WDIR=$WDIR python3 - <<'PY'
import os
db, wdir = os.environ['DB'], os.environ['WDIR']
s = open(wdir + '/cline.yaml').read()
head, tail = s[:s.index('evictor:')], s[s.index('warmer:'):]
open(wdir + '/evclaim.yaml', 'w').write(head + """evictor:
  feeder_id: "test"
  rate: 500
  batch_size: 100
  max_feeder_lag: 30s
  rules:
    - ns: "%s.tevclaim"
      older_than: 40m
      date_field: "at"
  backoff:
    max_replication_lag: 60s
    max_cache_dirty: 0.9
    min_write_tickets_free: 0.01
    min_oplog_window: 1m
""" % db + tail)
PY
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS), everyMS:1000}},{upsert:true});
   db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null
cline watermark --config $WDIR/evclaim.yaml --ns $DB.$CNS --complete-all >/dev/null 2>&1
cline router --config $WDIR/evclaim.yaml --log-file $LOGDIR/evclaim_r.log >/dev/null 2>&1 & CRP=$!
for _ in $(seq 1 30); do R 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break; sleep 1; done
# The router reads once, which is what puts the claim in its cache.
chkv "the router sees all four"                  "4" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({}))")"
cline evictor --config $WDIR/evclaim.yaml --once >/dev/null 2>&1
# Immediately, which is the whole point: no sleep between the sweep and this.
chkv "and still sees all four straight afterwards" "4" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({}))")"
# The control: the sweep really did remove something, or this proves nothing.
chkv "the sweep did remove the old ones"          "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({}))")"
chkv "and the archive still holds them all"       "4" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
kill $CRP 2>/dev/null; wait $CRP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null

echo "== documents the rule can never remove =="
# The whole-namespace version of this is already refused: a rule with no
# date_field against non-ObjectId _ids matches nothing at all. The partial
# version is quieter and reads as success. MongoDB brackets a range predicate by
# type, so a cutoff on a date selects only documents whose field holds a date,
# and one whose field is missing, null or a string is invisible to every sweep.
#
# Measured on a fast tier of three documents, one properly dated and two not:
# "sweep complete, scanned 1, evicted 1", and the two that cannot be aged stayed
# where they were with nothing mentioning them. That is a fast tier growing
# without bound while every sweep reports success.
ENS=tevunaged
OLDDATE=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=400)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
H "db.getSiblingDB(process.env.TC_DB).$ENS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ENS.drop()" >/dev/null
for U in "$HOT" "$COLD"; do
  mongosh "$U" --quiet --eval "db.getSiblingDB('$DB').$ENS.insertMany([
    {_id:1, at:new Date('$OLDDATE'), n:'old and evictable'},
    {_id:2, at:'$OLDDATE',           n:'old, but a string'},
    {_id:3,                          n:'no field at all'}])" >/dev/null
done
DB=$DB WDIR=$WDIR python3 - <<'PY'
import os
db, wdir = os.environ['DB'], os.environ['WDIR']
s = open(wdir + '/cline.yaml').read()
head, tail = s[:s.index('evictor:')], s[s.index('warmer:'):]
open(wdir + '/evunaged.yaml', 'w').write(head + """evictor:
  feeder_id: "test"
  rate: 500
  batch_size: 100
  max_feeder_lag: 30s
  rules:
    - ns: "%s.tevunaged"
      older_than: 40m
      date_field: "at"
  backoff:
    max_replication_lag: 60s
    max_cache_dirty: 0.9
    min_write_tickets_free: 0.01
    min_oplog_window: 1m
""" % db + tail)
PY
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS), everyMS:1000}},{upsert:true});
   db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null
rm -f $LOGDIR/ev_unaged.log
cline evictor --config $WDIR/evunaged.yaml --once --log-file $LOGDIR/ev_unaged.log >/dev/null 2>&1
chkv "the sweep removed the one it could"        "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$ENS.countDocuments({}))")"
chkv "and says the other two can never go"       "true" \
    "$(grep -q 'this rule can never remove' $LOGDIR/ev_unaged.log && echo true || echo false)"
chkv "counting them by the field they lack a date in" "true" \
    "$(grep -q 'not_dated_by_at=2' $LOGDIR/ev_unaged.log && echo true || echo false)"
chkv "and says the fast tier grows by that much" "true" \
    "$(grep -q 'grows by that much' $LOGDIR/ev_unaged.log && echo true || echo false)"
# The control: a namespace whose documents are all properly dated must stay
# quiet, or the warning is noise on every start and nobody reads it.
H "db.getSiblingDB(process.env.TC_DB).$ENS.deleteMany({at:{\$not:{\$type:'date'}}})" >/dev/null
rm -f $LOGDIR/ev_unaged2.log
cline evictor --config $WDIR/evunaged.yaml --once --log-file $LOGDIR/ev_unaged2.log >/dev/null 2>&1
chkv "and says nothing when they are all dated"  "0" \
    "$(grep -c 'this rule can never remove' $LOGDIR/ev_unaged2.log || true)"
H "db.getSiblingDB(process.env.TC_DB).$ENS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ENS.drop()" >/dev/null

kill $FP 2>/dev/null; wait $FP 2>/dev/null
echo
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
